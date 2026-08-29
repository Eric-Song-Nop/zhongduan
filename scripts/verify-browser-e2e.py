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
from urllib.request import urlopen

from browser_e2e_contract import (
    INPUT_CAPTURE_ENV,
    INTERRUPT_INPUT,
    INTERRUPT_SENTINEL,
    PROBE_INPUT,
    READY_SENTINEL,
    RESULT_SENTINEL,
    validate_synthetic_capture,
)

try:
    from playwright.async_api import async_playwright
except ImportError:
    raise SystemExit(
        "Playwright for Python is required: "
        "python3 -m pip install -r scripts/requirements-browser-e2e.txt; "
        "python3 -m playwright install chromium"
    ) from None


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
# This is a local smoke timeout, not a service recovery SLO.
COLD_RECOVERY_TIMEOUT_SECONDS = int(
    os.environ.get("ZHONGDUAN_E2E_COLD_RECOVERY_TIMEOUT_SECONDS", "30")
)


class ManagedProcess:
    def __init__(self, name: str, command: list[str], cwd: Path, env=None) -> None:
        self.name = name
        self.output = deque(maxlen=80)
        try:
            self.process = subprocess.Popen(
                command,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                text=True,
            )
        except Exception:
            raise RuntimeError(f"{self.name} did not start") from None
        self.reader = threading.Thread(target=self._read_output, daemon=True)
        self.reader.start()

    def _read_output(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.output.append(line.rstrip())

    def assert_running(self) -> None:
        status = self.process.poll()
        if status is not None:
            raise RuntimeError(
                f"{self.name} exited {status}; captured process output was withheld"
            )

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
    raise TimeoutError("local app server did not become ready")


def wait_for_json(path: Path, process: ManagedProcess) -> dict:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        process.assert_running()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.1)
    raise TimeoutError("Host did not write session bootstrap metadata")


async def wait_for_interrupt(path: Path) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if path.exists():
            capture = path.read_bytes()
            if capture == INTERRUPT_INPUT:
                return
            if len(capture) >= len(INTERRUPT_INPUT):
                raise RuntimeError(
                    "synthetic PTY capture diverged before the fixed probe"
                )
        await asyncio.sleep(0.1)
    raise TimeoutError("Ctrl-C did not reach the Host PTY during cold restore")


async def wait_for_capture(path: Path) -> bytes:
    expected_length = len(INTERRUPT_INPUT) + len(PROBE_INPUT)
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if path.exists():
            capture = path.read_bytes()
            if len(capture) >= expected_length:
                validate_synthetic_capture(capture)
                return capture
        await asyncio.sleep(0.05)
    raise TimeoutError(
        "synthetic PTY did not receive the complete fixed input sequence"
    )


def capture_state_label(path: Path) -> str:
    try:
        capture = path.read_bytes()
    except OSError:
        return "missing"
    if capture == INTERRUPT_INPUT:
        return "interrupt-only"
    if capture == INTERRUPT_INPUT + PROBE_INPUT:
        return "complete"
    if capture.startswith(INTERRUPT_INPUT) and PROBE_INPUT.startswith(
        capture[len(INTERRUPT_INPUT) :]
    ):
        return "partial-probe"
    return "unexpected"


def capture_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


async def wait_for_grid_text(
    page, sentinel: str, label: str, timeout_seconds: int = TIMEOUT_SECONDS
) -> None:
    try:
        await page.wait_for_function(
            """({ sentinel }) => {
                const grid = document.querySelector('.term-grid');
                return grid !== null && (grid.textContent ?? '').includes(sentinel);
            }""",
            arg={"sentinel": sentinel},
            timeout=timeout_seconds * 1000,
        )
    except Exception:
        raise TimeoutError(
            f"terminal grid did not expose the {label} sentinel"
        ) from None


async def grid_text_occurrences(page, sentinel: str) -> int:
    value = await page.locator(".term-grid").evaluate(
        "(grid, text) => (grid.textContent ?? '').split(text).length - 1", sentinel
    )
    return int(value)


async def grid_sentinels_are_ordered(page) -> bool:
    value = await page.locator(".term-grid").evaluate(
        """(grid, sentinels) => {
            const text = grid.textContent ?? '';
            const indexes = sentinels.map((sentinel) => text.indexOf(sentinel));
            return indexes.every((index) => index >= 0)
                && indexes[0] < indexes[1]
                && indexes[1] < indexes[2];
        }""",
        [READY_SENTINEL, INTERRUPT_SENTINEL, RESULT_SENTINEL],
    )
    return bool(value)


def chromium_executable() -> str | None:
    return os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE") or None


async def install_interrupt_phase_capture(page) -> None:
    await page.add_init_script(
        """(() => {
            const state = { captured: false, phase: null };
            const onKeydown = (event) => {
                if (
                    !event.isTrusted ||
                    event.code !== 'KeyC' ||
                    event.key !== 'c' ||
                    !event.ctrlKey ||
                    event.altKey ||
                    event.metaKey ||
                    event.shiftKey
                ) {
                    return;
                }
                state.captured = true;
                state.phase = document.querySelector('main')?.dataset.phase ?? null;
                document.removeEventListener('keydown', onKeydown, true);
            };
            document.addEventListener('keydown', onKeydown, true);
            Object.defineProperty(window, '__zhongduanBrowserRecoverySmokeInterruptV1', {
                configurable: true,
                value: state,
            });
        })();"""
    )


async def safe_page_label(page, selector: str, attribute: str | None = None) -> str:
    try:
        locator = page.locator(selector)
        value = (
            await locator.get_attribute(attribute)
            if attribute is not None
            else await locator.inner_text()
        )
        return value if isinstance(value, str) else "unavailable"
    except Exception:
        return "unavailable"


async def run_browser_recovery_smoke(
    base_url: str, session: dict, capture_path: Path
) -> dict[str, str]:
    async with async_playwright() as playwright:
        executable = chromium_executable()
        launch_options = {"headless": True}
        if executable is not None:
            launch_options["executable_path"] = executable
        browser = await playwright.chromium.launch(**launch_options)
        # Trace, video, and screenshots are intentionally disabled for this local smoke.
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()
        await install_interrupt_phase_capture(page)

        snapshot_get_held = asyncio.Event()
        release_snapshot_get = asyncio.Event()

        async def hold_snapshot_get(route, request) -> None:
            if request.method != "GET":
                await route.continue_()
                return
            snapshot_get_held.set()
            await release_snapshot_get.wait()
            await route.continue_()

        # Hold the exact snapshot download so pre-live input has no timing race.
        await page.route(
            f"{base_url}/api/v1/sessions/*/snapshots/*",
            hold_snapshot_get,
        )

        target = (
            f"{base_url}/sessions/{session['sessionId']}"
            f"?browserDiagnostics=off#capability={session['writerCapability']}"
        )
        stage = "navigation"
        try:
            await page.goto(
                target,
                wait_until="commit",
                timeout=TIMEOUT_SECONDS * 1000,
            )
            stage = "snapshot-get-hold"
            try:
                await asyncio.wait_for(
                    snapshot_get_held.wait(),
                    timeout=COLD_RECOVERY_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                raise TimeoutError(
                    "browser did not request the cold snapshot"
                ) from None

            stage = "cold-input-readiness"
            try:
                await page.wait_for_function(
                    """() => {
                        const textarea = document.querySelector(
                            '[data-testid="wterm-surface"] textarea'
                        );
                        const main = document.querySelector('main');
                        const ownership = document.querySelector('.ownership-button');
                        return textarea instanceof HTMLTextAreaElement
                            && ownership?.getAttribute('data-owned') === 'true'
                            && (main?.dataset.phase === 'attaching'
                                || main?.dataset.phase === 'restoring');
                    }""",
                    polling="raf",
                    timeout=TIMEOUT_SECONDS * 1000,
                )
            except Exception:
                raise TimeoutError(
                    "terminal input did not become writable during pre-live cold recovery"
                ) from None
            terminal_input = page.locator('[data-testid="wterm-surface"] textarea')
            await terminal_input.focus()
            if not await terminal_input.evaluate(
                "textarea => document.activeElement === textarea"
            ):
                raise AssertionError("terminal input did not retain focus")
            stage = "cold-interrupt"
            await page.keyboard.press("Control+KeyC")
            interrupt_phase = await page.evaluate(
                "() => window.__zhongduanBrowserRecoverySmokeInterruptV1"
            )
            if not isinstance(interrupt_phase, dict) or not interrupt_phase.get(
                "captured"
            ):
                raise AssertionError(
                    "Ctrl-C keydown was not captured in the terminal page"
                )
            if interrupt_phase.get("phase") not in ("attaching", "restoring"):
                raise AssertionError(
                    "Ctrl-C was not triggered during pre-live cold recovery"
                )
            await wait_for_interrupt(capture_path)
            release_snapshot_get.set()

            stage = "ready-dom"
            await wait_for_grid_text(
                page,
                READY_SENTINEL,
                "ready",
                timeout_seconds=COLD_RECOVERY_TIMEOUT_SECONDS,
            )
            await wait_for_grid_text(page, INTERRUPT_SENTINEL, "interrupt")
            await page.locator("main[data-phase='live']").wait_for(
                timeout=TIMEOUT_SECONDS * 1000
            )
            if await grid_text_occurrences(page, READY_SENTINEL) != 1:
                raise AssertionError(
                    "ready sentinel was missing or duplicated in the terminal grid"
                )
            if await grid_text_occurrences(page, INTERRUPT_SENTINEL) != 1:
                raise AssertionError(
                    "interrupt sentinel was missing or duplicated in the terminal grid"
                )
            if await grid_text_occurrences(page, RESULT_SENTINEL) != 0:
                raise AssertionError("result sentinel appeared before the fixed probe")

            stage = "fixed-probe"
            try:
                probe_text = PROBE_INPUT[:-1].decode("ascii")
            except UnicodeDecodeError:
                raise RuntimeError("fixed recovery smoke probe must be ASCII") from None
            await page.keyboard.type(probe_text)
            await page.keyboard.press("Enter")

            stage = "result-dom"
            await wait_for_grid_text(page, RESULT_SENTINEL, "result")
            await wait_for_capture(capture_path)

            stage = "final-invariants"
            if await grid_text_occurrences(page, RESULT_SENTINEL) != 1:
                raise AssertionError(
                    "result sentinel was missing or duplicated in the terminal grid"
                )
            if await grid_text_occurrences(page, READY_SENTINEL) != 1:
                raise AssertionError("ready sentinel changed during the recovery smoke")
            if await grid_text_occurrences(page, INTERRUPT_SENTINEL) != 1:
                raise AssertionError(
                    "interrupt sentinel changed during the recovery smoke"
                )
            if not await grid_sentinels_are_ordered(page):
                raise AssertionError(
                    "terminal sentinels were not preserved in canonical order"
                )
            if await page.locator("main").get_attribute("data-phase") != "live":
                raise AssertionError(
                    "browser did not finish the recovery smoke in live phase"
                )
            try:
                final_capture = capture_path.read_bytes()
            except OSError:
                raise AssertionError(
                    "synthetic PTY capture was unavailable at final validation"
                ) from None
            validate_synthetic_capture(final_capture)

            return {
                "schemaVersion": "browser-recovery-smoke-v1",
                "outcome": "passed",
            }
        except Exception:
            phase = await safe_page_label(page, "main", "data-phase")
            status = await safe_page_label(page, ".connection-status")
            raise RuntimeError(
                f"browser recovery smoke failed at stage={stage!r}, phase={phase!r}, "
                f"status={status!r}, "
                f"capture_state={capture_state_label(capture_path)!r}, "
                f"capture_bytes={capture_size(capture_path)}"
            ) from None
        finally:
            release_snapshot_get.set()
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass


async def main() -> None:
    if DEV_VARS.exists():
        raise RuntimeError(f"refusing to overwrite existing local bindings: {DEV_VARS}")
    if not VP.exists():
        raise RuntimeError("dependencies are not installed")

    node = shutil.which("node")
    if node is None:
        raise RuntimeError("Node.js is not installed")
    preexisting_generated_paths = {path for path in GENERATED_PATHS if path.exists()}
    temporary = Path(tempfile.mkdtemp(prefix="zhongduan-browser-recovery-smoke-"))
    session_info = temporary / "session.json"
    capture_path = temporary / "input-capture"
    server = None
    host = None
    try:
        DEV_VARS.write_text(
            f'BOOTSTRAP_TOKEN="{BOOTSTRAP_TOKEN}"\n'
            f'CAPABILITY_SIGNING_KEY="{CAPABILITY_KEY}"\n',
            encoding="utf-8",
        )
        DEV_VARS.chmod(0o600)
        subprocess.run([node, str(VP), "run", "build"], cwd=ROOT, check=True)

        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        server_env = os.environ.copy()
        server_env["ZHONGDUAN_E2E_CLOUDFLARE_STATE_PATH"] = str(
            temporary / "cloudflare-state"
        )
        server = ManagedProcess(
            "Vite workerd",
            [
                node,
                str(VP),
                "dev",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--strictPort",
                "--mode",
                "browser-e2e",
            ],
            APP_ROOT,
            server_env,
        )
        wait_for_http(base_url, server)

        host_env = os.environ.copy()
        host_env["ZHONGDUAN_BOOTSTRAP_TOKEN"] = BOOTSTRAP_TOKEN
        host_env[INPUT_CAPTURE_ENV] = str(capture_path)
        host_env["PYTHONDONTWRITEBYTECODE"] = "1"
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
                str(ROOT / "scripts" / "browser_e2e_contract.py"),
            ],
            ROOT,
            host_env,
        )
        session = wait_for_json(session_info, host)
        result = await run_browser_recovery_smoke(base_url, session, capture_path)
        host.assert_running()
        server.assert_running()
        print(json.dumps(result, separators=(",", ":")))
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
