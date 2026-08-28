#!/usr/bin/env python3

import asyncio
from collections import deque
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit
from urllib.request import urlopen

try:
    from playwright.async_api import async_playwright
except ImportError as error:
    raise SystemExit(
        "Playwright for Python is required: python -m pip install playwright"
    ) from error


ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = ROOT / "apps" / "terminal-cloud"
HOST_CLI = ROOT / "apps" / "host-daemon" / "dist" / "cli.mjs"
VP = ROOT / "node_modules" / "vite-plus" / "bin" / "vp"
DEV_VARS = APP_ROOT / ".dev.vars"
GENERATED_PATHS = (
    APP_ROOT / ".wrangler",
    HOST_CLI.parent,
    ROOT / "vendor" / "wterm" / "packages" / "@wterm" / "core" / "dist",
    ROOT / "vendor" / "wterm" / "packages" / "@wterm" / "ghostty" / "dist",
    ROOT / "vendor" / "wterm" / "packages" / "@wterm" / "dom" / "dist",
)
BOOTSTRAP_TOKEN = "e2e-bootstrap-token-with-at-least-32-bytes"
CAPABILITY_KEY = "e2e-capability-key-with-at-least-32-bytes"
TIMEOUT_SECONDS = int(os.environ.get("ZHONGDUAN_E2E_TIMEOUT_SECONDS", "30"))


class ManagedProcess:
    def __init__(self, name: str, command: list[str], cwd: Path, env=None) -> None:
        self.name = name
        self.output = deque(maxlen=80)
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )
        self.reader = threading.Thread(target=self._read_output, daemon=True)
        self.reader.start()

    def _read_output(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.output.append(line.rstrip())

    def assert_running(self) -> None:
        status = self.process.poll()
        if status is not None:
            output = "\n".join(self.output)
            raise RuntimeError(f"{self.name} exited {status}\n{output}")

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except (AttributeError, ProcessLookupError):
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except (AttributeError, ProcessLookupError):
                self.process.kill()
            self.process.wait(timeout=5)


def free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


def wait_for_http(url: str, process: ManagedProcess) -> None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        process.assert_running()
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise TimeoutError(f"server did not become ready at {url}")


def wait_for_json(path: Path, process: ManagedProcess) -> dict:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        process.assert_running()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.1)
    raise TimeoutError(f"Host did not write {path}")


async def wait_for_marker(path: Path) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size >= 1:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError("Ctrl-C did not reach the Host PTY during cold restore")


async def wait_for_socket_events(events, expected: set[str]) -> None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if expected.issubset(events):
            return
        await asyncio.sleep(0.1)
    missing = sorted(expected.difference(events))
    raise TimeoutError(f"browser WebSocket events did not arrive: {missing}")


def chromium_executable() -> str | None:
    configured = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE")
    if configured:
        return configured
    for command in ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable"):
        executable = shutil.which(command)
        if executable:
            return executable
    return None


async def verify_browser(base_url: str, session: dict, marker: Path) -> None:
    page_errors: list[str] = []
    console_errors: list[str] = []
    socket_events = deque(maxlen=80)
    api_events = deque(maxlen=40)
    async with async_playwright() as playwright:
        executable = chromium_executable()
        launch_options = {"headless": True}
        if executable is not None:
            launch_options["executable_path"] = executable
        browser = await playwright.chromium.launch(**launch_options)
        page = await browser.new_page(viewport={"width": 1280, "height": 800})
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.on(
            "console",
            lambda message: console_errors.append(message.text)
            if message.type == "error"
            else None,
        )

        def frame_label(payload) -> str:
            if isinstance(payload, bytes):
                return f"binary:{len(payload)}"
            try:
                decoded = json.loads(payload)
                return str(decoded.get("type", "json"))
            except (AttributeError, json.JSONDecodeError):
                return payload[:32]

        def observe_socket(web_socket) -> None:
            if "/api/v1/sessions/" not in web_socket.url:
                return
            channel = "control" if "/ws/control" in web_socket.url else "data"
            socket_events.append(f"{channel}:open")
            web_socket.on(
                "framesent",
                lambda payload: socket_events.append(f"{channel}:sent:{frame_label(payload)}"),
            )
            web_socket.on(
                "framereceived",
                lambda payload: socket_events.append(
                    f"{channel}:received:{frame_label(payload)}"
                ),
            )
            web_socket.on("socketerror", lambda error: socket_events.append(f"{channel}:error:{error}"))
            web_socket.on("close", lambda _: socket_events.append(f"{channel}:close"))

        page.on("websocket", observe_socket)

        def api_path(url: str) -> str:
            return urlsplit(url).path

        page.on(
            "response",
            lambda response: api_events.append(
                f"response:{response.status}:{api_path(response.url)}"
            )
            if "/api/" in response.url
            else None,
        )
        page.on(
            "requestfailed",
            lambda request: api_events.append(
                f"failed:{api_path(request.url)}:{request.failure}"
            )
            if "/api/" in request.url
            else None,
        )

        target = (
            f"{base_url}/sessions/{session['sessionId']}"
            f"#capability={session['writerCapability']}"
        )
        try:
            await page.goto(target, wait_until="domcontentloaded", timeout=TIMEOUT_SECONDS * 1000)
            surface = page.get_by_test_id("wterm-surface")
            await surface.wait_for(state="visible", timeout=TIMEOUT_SECONDS * 1000)
            bounds = await surface.bounding_box()
            if bounds is None or bounds["width"] < 100 or bounds["height"] < 100:
                raise AssertionError("terminal first screen has no stable visible surface")
            if "capability=" in page.url or "#" in page.url:
                raise AssertionError("browser capability was not scrubbed from the address bar")

            attaching = page.locator("main[data-phase='attaching']")
            await attaching.wait_for(
                timeout=TIMEOUT_SECONDS * 1000
            )
            await wait_for_socket_events(
                socket_events,
                {"control:open", "data:open", "control:received:welcome"},
            )
            if await page.locator(".ownership-button").inner_text() != "已控制":
                raise AssertionError("writer ownership was not granted")
            await surface.click()
            await page.keyboard.press("Control+C")
            await wait_for_marker(marker)
            await wait_for_socket_events(
                socket_events,
                {
                    "control:sent:key",
                    "control:received:input-ack",
                    "control:received:writer-lease-status",
                    "control:received:pong",
                    "data:received:pong",
                },
            )
            if await page.locator(".connection-status").inner_text() != "同步终端":
                raise AssertionError("browser left cold recovery unexpectedly")
            if page_errors:
                raise AssertionError(f"browser page errors: {page_errors}")
        except Exception as error:
            phase = await page.locator("main").get_attribute("data-phase")
            status = await page.locator(".connection-status").inner_text()
            raise RuntimeError(
                f"browser smoke failed in phase={phase!r}, status={status!r}, "
                f"page_errors={page_errors!r}, console_errors={console_errors!r}, "
                f"socket_events={list(socket_events)!r}, api_events={list(api_events)!r}"
            ) from error
        finally:
            await browser.close()


async def main() -> None:
    if DEV_VARS.exists():
        raise RuntimeError(f"refusing to overwrite existing local bindings: {DEV_VARS}")
    if not VP.exists():
        raise RuntimeError("dependencies are not installed")

    node = shutil.which("node")
    if node is None:
        raise RuntimeError("Node.js is not installed")
    preexisting_generated_paths = {
        path for path in GENERATED_PATHS if path.exists()
    }
    temporary = Path(tempfile.mkdtemp(prefix="zhongduan-browser-e2e-"))
    session_info = temporary / "session.json"
    marker = temporary / "interrupts"
    pty_probe = temporary / "pty-probe.py"
    server = None
    host = None
    try:
        DEV_VARS.write_text(
            f'BOOTSTRAP_TOKEN="{BOOTSTRAP_TOKEN}"\n'
            f'CAPABILITY_SIGNING_KEY="{CAPABILITY_KEY}"\n',
            encoding="utf-8",
        )
        DEV_VARS.chmod(0o600)
        pty_probe.write_text(
            "import os\n"
            "import sys\n"
            "import tty\n"
            "tty.setraw(sys.stdin.fileno())\n"
            "os.write(sys.stdout.fileno(), b'\\x1b[2J\\x1b[HZhongduan E2E ready\\r\\n')\n"
            "while True:\n"
            "    data = os.read(sys.stdin.fileno(), 1)\n"
            "    with open(os.environ['ZHONGDUAN_E2E_INTERRUPT_MARKER'], 'ab', buffering=0) as output:\n"
            "        output.write(data)\n",
            encoding="utf-8",
        )
        subprocess.run([node, str(VP), "run", "build"], cwd=ROOT, check=True)

        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        server = ManagedProcess(
            "Vite workerd",
            [node, str(VP), "dev", "--host", "127.0.0.1", "--port", str(port), "--strictPort"],
            APP_ROOT,
        )
        wait_for_http(base_url, server)

        host_env = os.environ.copy()
        host_env["ZHONGDUAN_BOOTSTRAP_TOKEN"] = BOOTSTRAP_TOKEN
        host_env["ZHONGDUAN_E2E_INTERRUPT_MARKER"] = str(marker)
        host = ManagedProcess(
            "Host relay",
            [
                node,
                str(HOST_CLI),
                "cloud",
                "--url",
                base_url,
                "--session-info-file",
                str(session_info),
                "--",
                sys.executable,
                str(pty_probe),
            ],
            ROOT,
            host_env,
        )
        session = wait_for_json(session_info, host)
        await verify_browser(base_url, session, marker)
        host.assert_running()
        server.assert_running()
        print("real workerd, Host, and browser cold-recovery smoke passed")
    except Exception:
        if host is not None:
            print("Host relay output:\n" + "\n".join(host.output), file=sys.stderr)
        if server is not None:
            print("Vite workerd output:\n" + "\n".join(server.output), file=sys.stderr)
        raise
    finally:
        if host is not None:
            host.stop()
        if server is not None:
            server.stop()
        DEV_VARS.unlink(missing_ok=True)
        shutil.rmtree(temporary, ignore_errors=True)
        for path in GENERATED_PATHS:
            if path not in preexisting_generated_paths:
                shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
