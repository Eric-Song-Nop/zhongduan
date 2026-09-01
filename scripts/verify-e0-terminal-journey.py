#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict, deque
from contextlib import suppress
import json
import os
from pathlib import Path
import platform
import random
import re
import shutil
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
from urllib.request import urlopen

try:
    from aiohttp import ClientSession, WSMsgType, web
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright
except ImportError:
    ClientSession = None  # type: ignore[assignment,misc]
    WSMsgType = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]
    async_playwright = None
    PlaywrightTimeoutError = TimeoutError  # type: ignore[misc,assignment]

from e0_terminal_fixture import (
    EVENT_LOG_ENV,
    FLOOD_CHUNK_BYTES_ENV,
    FLOOD_COMMAND,
    FLOOD_MAX_BYTES_ENV,
    FLOOD_MAX_DURATION_MS_ENV,
    FLOOD_PREFIX,
    INPUT_CAPTURE_ENV,
    INTERRUPT_ARM_PREFIX,
    INTERRUPT_PREFIX,
    PROBE_PREFIX,
    QUIET_PREFIX,
    READY_SENTINEL,
    RESULT_PREFIX,
    SECURE_PREFIX,
)
from e0_terminal_journey import (
    BASELINE_PATH,
    CONTRACT_PATH,
    EXPECTED_VARIANTS,
    assemble_candidate_report,
    assemble_current_report,
    build_e4b_decision,
    canonical_sha256,
    load_json,
    load_report_bundle,
    matrix_cells,
    validate_contract,
    validate_scenario_report,
    write_report_bundle,
)


ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = ROOT / "apps" / "terminal-cloud"
HOST_CLI = ROOT / "apps" / "host-daemon" / "dist" / "cli.mjs"
VP = ROOT / "node_modules" / "vite-plus" / "bin" / "vp"
DEV_VARS = APP_ROOT / ".dev.vars"
BOOTSTRAP_TOKEN = "e0-bootstrap-token-with-at-least-32-bytes"
CAPABILITY_KEY = "e0-capability-key-with-at-least-32-bytes"
INPUT_FRAME_TYPES = {"key", "text", "paste", "focus", "mouse", "resize-request"}
CONTROL_MODIFIER = 1 << 1
E4_EVIDENCE_ENV = "ZHONGDUAN_E4_EVIDENCE_JSONL"
E0_DISABLE_SNAPSHOT_REFRESH_ENV = "ZHONGDUAN_E0_DISABLE_SNAPSHOT_REFRESH"
# Keep the second half of snapshot-enabled long enough to overlap both CURRENT's
# 30-second checkpoint expiry and E4a's 15-second background refresh cadence.
SNAPSHOT_CHECKPOINT_TTL_MS = 30_000
SNAPSHOT_CHECKPOINT_EXPIRY_CUSHION_MS = 2_000
SNAPSHOT_OVERLAP_SAMPLE_INTERVAL_MS = 3_000
SNAPSHOT_ATTACH_REMAINING_SAMPLES = 3
PROBE_PATTERN = re.compile(r"ZHONGDUAN_E0_PROBE:([A-Za-z0-9_-]+)\r")
SECURE_PATTERN = re.compile(r"ZHONGDUAN_E0_SECURE:([A-Za-z0-9_-]+)\r")
FLOOD_PATTERN = re.compile(r"ZHONGDUAN_E0_FLOOD:([A-Za-z0-9_-]+)\r")
INTERRUPT_ARM_PATTERN = re.compile(
    r"ZHONGDUAN_E0_INTERRUPT_ARM:[A-Za-z0-9_-]+:([A-Za-z0-9_-]+)\r"
)
HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


class JourneyError(RuntimeError):
    pass


class ManagedProcess:
    def __init__(self, name: str, command: list[str], cwd: Path, env: dict[str, str]) -> None:
        self.name = name
        self.output: deque[str] = deque(maxlen=120)
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
            sanitized = self.sanitized_tail()
            raise JourneyError(
                f"{self.name} exited with status {status}; sanitized tail:\n"
                + "\n".join(sanitized[-20:])
            )

    def sanitized_tail(self) -> list[str]:
        sanitized = []
        for line in self.output:
            value = line.replace(BOOTSTRAP_TOKEN, "[redacted-bootstrap]").replace(
                CAPABILITY_KEY, "[redacted-signing-key]"
            )
            value = re.sub(r"Bearer\s+\S+", "Bearer [redacted]", value, flags=re.IGNORECASE)
            value = re.sub(r"([?&](?:ticket|capability)=)[^&\s]+", r"\1[redacted]", value)
            sanitized.append(value)
        return sanitized

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        with suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGTERM)
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=5)


class TraceStore:
    def __init__(self, variant: str) -> None:
        self.variant = variant
        self.events: list[dict[str, Any]] = []
        self.identity_samples: dict[str, str] = {}
        self.browser_send_counts: Counter[str] = Counter()
        self.host_receive_counts: Counter[str] = Counter()
        self.sample_identities: dict[str, str] = {}
        self.sample_browser_identities: dict[str, set[str]] = defaultdict(set)
        self.sample_wire_identities: dict[str, set[str]] = defaultdict(set)
        self.held: dict[str, asyncio.Event] = {}
        self.releases: dict[str, asyncio.Event] = {}
        self.disconnect_sample: str | None = None
        self.disconnect_applied = False
        self.finalized_snapshot_id: str | None = None
        self.snapshot_finalized = asyncio.Event()
        self.snapshot_finalizations: list[dict[str, Any]] = []
        self.hold_next_snapshot_attach = False
        self.snapshot_attach_held = asyncio.Event()
        self.snapshot_attach_release = asyncio.Event()
        self.pending_ctrl_sample: str | None = None
        self.host_ready_acknowledged = asyncio.Event()

    def event(self, name: str, sample_id: str, at_ns: int | None = None, **fields: Any) -> None:
        self.events.append(
            {
                "name": name,
                "sampleId": sample_id,
                "variant": self.variant,
                "atUnixNs": time.time_ns() if at_ns is None else at_ns,
                **fields,
            }
        )

    @staticmethod
    def browser_identity(frame: dict[str, Any]) -> str | None:
        epoch = frame.get("inputEpoch")
        sequence = frame.get("clientInputSeq")
        if isinstance(epoch, str) and isinstance(sequence, str):
            return f"{epoch}/{sequence}"
        return None

    @classmethod
    def identity(cls, frame: dict[str, Any]) -> str | None:
        fence = frame.get("writerFence")
        browser_identity = cls.browser_identity(frame)
        if isinstance(fence, str) and browser_identity is not None:
            return f"{fence}/{browser_identity}"
        return None

    def sample(self, frame: dict[str, Any]) -> str | None:
        if (
            frame.get("type") == "key"
            and frame.get("action") == "press"
            and frame.get("code") == "KeyC"
            and frame.get("key") == "c"
            and isinstance(frame.get("modifiers"), int)
            and frame["modifiers"] & CONTROL_MODIFIER
        ):
            sample = self.pending_ctrl_sample
            self.pending_ctrl_sample = None
            return sample or "ctrl-c-pending"
        payload = frame.get("data") if frame.get("type") in {"text", "paste"} else frame.get("text")
        if isinstance(payload, str):
            probe = PROBE_PATTERN.search(payload)
            if probe is not None:
                return probe.group(1)
            secure = SECURE_PATTERN.search(payload)
            if secure is not None:
                return secure.group(1)
            flood = FLOOD_PATTERN.search(payload)
            if flood is not None:
                return flood.group(1)
            interrupt_arm = INTERRUPT_ARM_PATTERN.search(payload)
            if interrupt_arm is not None:
                return interrupt_arm.group(1)
            if payload == "\x03":
                sample = self.pending_ctrl_sample
                self.pending_ctrl_sample = None
                return sample or "ctrl-c-pending"
            if payload == FLOOD_COMMAND.decode("ascii") + "\r":
                return "flood-command-legacy"
        return None

    def browser_frame(self, frame: dict[str, Any], at_ns: int) -> tuple[str | None, str | None]:
        identity = self.browser_identity(frame)
        if identity is None or frame.get("type") not in INPUT_FRAME_TYPES:
            return None, None
        retained_sample = self.identity_samples.get(identity)
        sample = self.sample(frame)
        if sample == "ctrl-c-pending":
            if retained_sample is not None:
                sample = retained_sample
            else:
                ctrl_index = sum(
                    1 for value in self.sample_identities if value.startswith("ctrl-c-")
                )
                sample = f"ctrl-c-{ctrl_index:03d}"
        if sample is None:
            sample = retained_sample
        if sample is None:
            return identity, None
        self.identity_samples[identity] = sample
        self.sample_browser_identities[sample].add(identity)
        self.browser_send_counts[identity] += 1
        self.event(
            "cloud.browser-receive-attempt",
            sample,
            at_ns,
            browserIdentity=identity,
            attempt=self.browser_send_counts[identity],
        )
        if self.browser_send_counts[identity] == 1:
            self.event("cloud.browser-receive", sample, at_ns, browserIdentity=identity)
        return identity, sample

    def host_frame(self, frame: dict[str, Any], send_ns: int, receive_ns: int) -> None:
        if frame.get("type") == "host-ready-ack":
            self.host_ready_acknowledged.set()
        identity = self.identity(frame)
        browser_identity = self.browser_identity(frame)
        if (
            identity is None
            or browser_identity is None
            or frame.get("type") not in INPUT_FRAME_TYPES
        ):
            return
        sample = self.identity_samples.get(browser_identity)
        if sample is None:
            return
        self.sample_identities.setdefault(sample, identity)
        self.sample_wire_identities[sample].add(identity)
        self.host_receive_counts[identity] += 1
        if self.host_receive_counts[identity] == 1:
            self.event("cloud.host-send", sample, send_ns, wireIdentity=identity)
            self.event("host.receive", sample, receive_ns, wireIdentity=identity)

    def hold(self, sample_id: str) -> tuple[asyncio.Event, asyncio.Event]:
        held = self.held.setdefault(sample_id, asyncio.Event())
        release = self.releases.setdefault(sample_id, asyncio.Event())
        return held, release


class LinkProxy:
    def __init__(
        self,
        role: str,
        upstream: str,
        rtt_ms: int,
        jitter_ms: int,
        trace: TraceStore,
        seed: int,
    ) -> None:
        self.role = role
        self.upstream = upstream.rstrip("/")
        self.rtt_ms = rtt_ms
        self.jitter_ms = jitter_ms
        self.trace = trace
        self.random = random.Random(seed)
        self.client: ClientSession | None = None
        self.runner: web.AppRunner | None = None
        self.origin: str | None = None

    async def start(self) -> None:
        self.client = ClientSession(auto_decompress=False)
        application = web.Application(client_max_size=16 * 1024 * 1024)
        application.router.add_route("*", "/{tail:.*}", self._handle)
        self.runner = web.AppRunner(application, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        sockets = site._server.sockets if site._server is not None else []
        if len(sockets) != 1:
            raise JourneyError(f"{self.role} proxy did not bind exactly one socket")
        self.origin = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
        if self.client is not None:
            await self.client.close()

    async def _delay(self) -> None:
        base = self.rtt_ms / 2
        jitter = self.random.uniform(-self.jitter_ms, self.jitter_ms) if self.jitter_ms else 0
        await asyncio.sleep(max(0, base + jitter) / 1000)

    @staticmethod
    def _headers(headers: Any, websocket: bool = False) -> dict[str, str]:
        excluded = HOP_HEADERS | {"host", "content-length"}
        if websocket:
            excluded |= {"sec-websocket-accept", "sec-websocket-extensions", "sec-websocket-key"}
        return {name: value for name, value in headers.items() if name.lower() not in excluded}

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await self._websocket(request)
        assert self.client is not None
        target = f"{self.upstream}{request.rel_url}"
        body = await request.read()
        await self._delay()
        async with self.client.request(
            request.method,
            target,
            headers=self._headers(request.headers),
            data=body or None,
            allow_redirects=False,
        ) as upstream:
            response_body = await upstream.read()
            snapshot_match = re.fullmatch(
                r"/api/v1/sessions/[^/]+/snapshots/([^/]+)", request.path
            )
            if (
                self.role == "host"
                and request.method == "PUT"
                and snapshot_match is not None
                and upstream.status in {200, 201}
            ):
                self.trace.finalized_snapshot_id = snapshot_match.group(1)
                self.trace.snapshot_finalizations.append(
                    {
                        "snapshotId": snapshot_match.group(1),
                        "atUnixNs": time.time_ns(),
                    }
                )
                self.trace.event(
                    "host.snapshot-finalized",
                    f"snapshot-{snapshot_match.group(1)}",
                    self.trace.snapshot_finalizations[-1]["atUnixNs"],
                    snapshotId=snapshot_match.group(1),
                )
                self.trace.snapshot_finalized.set()
            await self._delay()
            return web.Response(
                status=upstream.status,
                reason=upstream.reason,
                body=response_body,
                headers=self._headers(upstream.headers),
            )

    async def _websocket(self, request: web.Request) -> web.WebSocketResponse:
        assert self.client is not None
        target = f"{self.upstream}{request.rel_url}"
        target = target.replace("http://", "ws://", 1).replace("https://", "wss://", 1)
        protocols = [
            item.strip()
            for item in request.headers.get("sec-websocket-protocol", "").split(",")
            if item.strip()
        ]
        upstream = await self.client.ws_connect(
            target,
            headers=self._headers(request.headers, websocket=True),
            protocols=protocols,
            autoping=False,
            autoclose=False,
            compress=0,
            max_msg_size=16 * 1024 * 1024,
        )
        downstream = web.WebSocketResponse(
            protocols=protocols,
            autoping=False,
            autoclose=False,
            compress=False,
            max_msg_size=16 * 1024 * 1024,
        )
        await downstream.prepare(request)

        async def forward(source: Any, destination: Any, to_cloud: bool) -> None:
            async for message in source:
                frame: dict[str, Any] | None = None
                identity: str | None = None
                sample: str | None = None
                if message.type == WSMsgType.TEXT:
                    with suppress(json.JSONDecodeError):
                        decoded = json.loads(message.data)
                        if isinstance(decoded, dict):
                            frame = decoded
                cloud_send_ns = time.time_ns()
                await self._delay()
                host_receive_ns = time.time_ns()
                if self.role == "browser" and to_cloud and frame is not None:
                    # This is the verifiable Cloud-ingress boundary: the configured
                    # Browser/Cloud link delay has completed, but Worker dispatch has not begun.
                    identity, sample = self.trace.browser_frame(frame, host_receive_ns)
                    if sample in self.trace.held:
                        self.trace.held[sample].set()
                        await self.trace.releases[sample].wait()
                if self.role == "host" and not to_cloud and frame is not None:
                    self.trace.host_frame(frame, cloud_send_ns, host_receive_ns)
                    if (
                        frame.get("type") == "attach-request"
                        and self.trace.hold_next_snapshot_attach
                    ):
                        self.trace.hold_next_snapshot_attach = False
                        self.trace.snapshot_attach_held.set()
                        await self.trace.snapshot_attach_release.wait()

                try:
                    if message.type == WSMsgType.TEXT:
                        await destination.send_str(message.data)
                    elif message.type == WSMsgType.BINARY:
                        await destination.send_bytes(message.data)
                    elif message.type == WSMsgType.PING:
                        await destination.ping(message.data)
                    elif message.type == WSMsgType.PONG:
                        await destination.pong(message.data)
                    elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                        break
                except ConnectionResetError:
                    break

                if (
                    self.role == "browser"
                    and to_cloud
                    and sample is not None
                    and sample == self.trace.disconnect_sample
                    and not self.trace.disconnect_applied
                ):
                    self.trace.disconnect_applied = True
                    await destination.close(code=4001, message=b"E0 acceptance uncertainty")
                    await source.close(code=4001, message=b"E0 acceptance uncertainty")
                    break

        tasks = [
            asyncio.create_task(forward(downstream, upstream, True)),
            asyncio.create_task(forward(upstream, downstream, False)),
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
        finally:
            await upstream.close()
            await downstream.close()
        return downstream


def free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def wait_for_http(url: str, process: ManagedProcess, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        process.assert_running()
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise TimeoutError("local Workerd/Vite server did not become ready")


def wait_for_json(path: Path, process: ManagedProcess, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        process.assert_running()
        with suppress(FileNotFoundError, json.JSONDecodeError):
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        time.sleep(0.1)
    raise TimeoutError("Host did not write session bootstrap metadata")


def read_raw_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        if not isinstance(value, dict):
            raise JourneyError("fixture event log contains a non-object record")
        events.append(value)
    return events


def read_jsonl(path: Path, variant: str) -> list[dict[str, Any]]:
    return [{**event, "variant": variant} for event in read_raw_jsonl(path)]


async def wait_for_fixture_event(
    path: Path, name: str, sample_id: str, timeout_seconds: int
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        for event in read_jsonl(path, "pending"):
            if event.get("name") == name and event.get("sampleId") == sample_id:
                return
        await asyncio.sleep(0.05)
    raise JourneyError(f"fixture did not record {name} for {sample_id}")


def run_authority_oracle(samples: int) -> dict[str, Any]:
    env = os.environ.copy()
    env["ZHONGDUAN_E0_SAMPLES"] = str(samples)
    completed = subprocess.run(
        [
            "node",
            str(ROOT / "scripts" / "e0_authority_oracle.mjs"),
            "--allow-failure",
        ],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    value = json.loads(completed.stdout)
    if not isinstance(value, dict):
        raise JourneyError("authority oracle returned a non-object result")
    return value


def check_node_pty_spawn_helper() -> None:
    """Surface pnpm installations that lost node-pty's executable mode."""

    if sys.platform != "darwin":
        return
    architecture = "arm64" if platform.machine() in {"arm64", "aarch64"} else "x64"
    helpers = sorted(
        (ROOT / "node_modules" / ".pnpm").glob(
            f"node-pty@*/node_modules/node-pty/prebuilds/darwin-{architecture}/spawn-helper"
        )
    )
    blocked = [path for path in helpers if not os.access(path, os.X_OK)]
    if not blocked:
        return
    relative = blocked[0].relative_to(ROOT)
    raise JourneyError(
        "node-pty spawn-helper is not executable; repair the local dependency and rerun: "
        f"chmod u+x {shlex.quote(str(relative))}"
    )


INIT_SCRIPT = r"""
(() => {
  const state = {
    pageId: crypto.randomUUID(),
    events: [],
    currentSample: null,
    duplicateSample: null,
    nextLocalIntent: 1,
    intents: [],
    intentsBySample: Object.create(null),
    pendingByBrowserIdentity: Object.create(null),
    installedSockets: new WeakSet(),
  };
  const now = () => Math.round((performance.timeOrigin + performance.now()) * 1_000_000);
  const record = (name, sampleId) => state.events.push({
    name,
    sampleId,
    atUnixNs: now(),
  });
  const consume = (sampleId) => {
    if (state.intentsBySample[sampleId] !== undefined) {
      throw new Error(`duplicate E0 UI consumption for ${sampleId}`);
    }
    const intent = {
      sampleId,
      localIntentId: `e0-${state.pageId}-${state.nextLocalIntent++}`,
      consumedAtUnixNs: now(),
      sends: [],
      terminal: null,
    };
    state.intents.push(intent);
    state.intentsBySample[sampleId] = intent;
    return intent.localIntentId;
  };
  const browserIdentity = (frame) =>
    typeof frame?.inputEpoch === 'string' && typeof frame?.clientInputSeq === 'string'
      ? `${frame.inputEpoch}/${frame.clientInputSeq}`
      : null;
  const sampleFromFrame = (frame) => {
    if (state.currentSample !== null) return state.currentSample;
    const identity = browserIdentity(frame);
    if (identity !== null && state.pendingByBrowserIdentity[identity] !== undefined) {
      return state.pendingByBrowserIdentity[identity].sampleId;
    }
    const payload = frame?.type === 'paste' || frame?.type === 'text' ? frame.data : frame?.text;
    if (typeof payload !== 'string') return null;
    for (const pattern of [
      /ZHONGDUAN_E0_PROBE:([A-Za-z0-9_-]+)\r/u,
      /ZHONGDUAN_E0_SECURE:([A-Za-z0-9_-]+)\r/u,
      /ZHONGDUAN_E0_FLOOD:([A-Za-z0-9_-]+)\r/u,
      /ZHONGDUAN_E0_INTERRUPT_ARM:[A-Za-z0-9_-]+:([A-Za-z0-9_-]+)\r/u,
    ]) {
      const match = pattern.exec(payload);
      if (match !== null) return match[1];
    }
    return null;
  };
  const finish = (
    intent,
    outcome,
    reason,
    identity,
    deterministicResult = null,
    source = 'passive-wire-observation',
    productLocalIntentId = null,
  ) => {
    if (intent.terminal !== null) return;
    intent.terminal = {
      sampleId: intent.sampleId,
      localIntentId: intent.localIntentId,
      outcome,
      identity,
      reason,
      deterministicResult,
      source,
      productLocalIntentId,
      observedAtUnixNs: now(),
    };
  };
  const acceptAck = (frame) => {
    const identity = browserIdentity(frame);
    if (identity === null) return;
    const intent = state.pendingByBrowserIdentity[identity];
    if (intent === undefined) return;
    const fullIdentity = typeof frame.writerFence === 'string' ? {
      writerFence: frame.writerFence,
      inputEpoch: frame.inputEpoch,
      clientInputSeq: frame.clientInputSeq,
    } : null;
    if (frame.status === 'uncertain') {
      finish(intent, 'uncertain', 'input-ack-uncertain', fullIdentity);
    } else {
      finish(intent, 'deterministic', 'input-ack', fullIdentity, frame.status);
    }
  };
  const installSocketObservers = (socket) => {
    if (state.installedSockets.has(socket)) return;
    state.installedSockets.add(socket);
    socket.addEventListener('message', (event) => {
      if (typeof event.data !== 'string' || !event.data.startsWith('{')) return;
      let frame;
      try { frame = JSON.parse(event.data); } catch { return; }
      if (frame.type === 'input-ack') acceptAck(frame);
      if (frame.type === 'input-epoch-ack' && Array.isArray(frame.results)) {
        for (const result of frame.results) acceptAck(result);
      }
    });
    socket.addEventListener('close', () => {
      for (const intent of state.intents) {
        if (intent.terminal === null && intent.sends.some((send) => send.socket === socket)) {
          const last = intent.sends.at(-1);
          finish(intent, 'uncertain', 'socket-closed-after-send', last?.identity ?? null);
        }
      }
    });
  };
  window.addEventListener('zhongduan:input-intent-result', (event) => {
    const result = event.detail;
    if (result === null || typeof result !== 'object') return;
    const identity = browserIdentity(result.identity);
    const intent = identity === null
      ? state.intentsBySample[state.currentSample]
      : state.pendingByBrowserIdentity[identity];
    if (intent === undefined) return;
    finish(
      intent,
      result.outcome,
      result.reason,
      result.identity === null ? null : { ...result.identity },
      result.outcome === 'deterministic' ? result.result : null,
      'product-intent-result-event',
      result.localIntentId ?? null,
    );
  });
  const originalSend = WebSocket.prototype.send;
  WebSocket.prototype.send = function(data) {
    let frame = null;
    if (typeof data === 'string' && data.startsWith('{')) {
      try { frame = JSON.parse(data); } catch {}
    }
    const semantic = frame !== null && [
      'key', 'text', 'paste', 'focus', 'mouse', 'resize-request'
    ].includes(frame.type);
    const sampleId = semantic ? sampleFromFrame(frame) : null;
    if (sampleId !== null) {
      const intent = state.intentsBySample[sampleId];
      const identity = browserIdentity(frame);
      if (intent !== undefined && identity !== null) {
        installSocketObservers(this);
        intent.sends.push({ identity, socket: this, observedAtUnixNs: now() });
        state.pendingByBrowserIdentity[identity] = intent;
        if (intent.sends.length === 1) {
          setTimeout(() => {
            if (intent.terminal === null) {
              finish(intent, 'uncertain', 'observation-deadline-after-send', null);
            }
          }, 35_000);
        }
      }
      record('browser.send-decision', sampleId);
    }
    originalSend.call(this, data);
    if (sampleId !== null && state.duplicateSample === sampleId) {
      state.duplicateSample = null;
      const intent = state.intentsBySample[sampleId];
      const identity = browserIdentity(frame);
      if (intent !== undefined && identity !== null) {
        intent.sends.push({ identity, socket: this, observedAtUnixNs: now() });
      }
      originalSend.call(this, data);
    }
  };
  document.addEventListener('keydown', (event) => {
    if (
      event.isTrusted && event.code === 'KeyC' && event.key === 'c' && event.ctrlKey &&
      !event.altKey && !event.metaKey && !event.shiftKey && state.currentSample !== null
    ) {
      consume(state.currentSample);
      record('browser.keydown', state.currentSample);
      record('browser.ctrl-c', state.currentSample);
      record('browser.input-consumed', state.currentSample);
    }
  }, true);
  Object.defineProperty(window, '__zhongduanE0', {
    configurable: false,
    value: Object.assign(state, { consume }),
  });
})();
"""


async def browser_now(page: Any) -> int:
    return int(
        await page.evaluate(
            "() => Math.round((performance.timeOrigin + performance.now()) * 1000000)"
        )
    )


async def set_sample(page: Any, sample_id: str | None, duplicate: bool = False) -> None:
    await page.evaluate(
        "({ sampleId, duplicate }) => { window.__zhongduanE0.currentSample = sampleId; "
        "window.__zhongduanE0.duplicateSample = duplicate ? sampleId : null; }",
        {"sampleId": sample_id, "duplicate": duplicate},
    )


async def append_browser_event(page: Any, name: str, sample_id: str) -> None:
    await page.evaluate(
        "({ name, sampleId }) => window.__zhongduanE0.events.push({ "
        "name, sampleId, atUnixNs: Math.round((performance.timeOrigin + performance.now()) * 1000000) })",
        {"name": name, "sampleId": sample_id},
    )


async def dispatch_paste(
    page: Any, payload: str, sample_id: str, *, duplicate: bool = False
) -> None:
    await set_sample(page, sample_id, duplicate=duplicate)
    await page.evaluate("sampleId => window.__zhongduanE0.consume(sampleId)", sample_id)
    await append_browser_event(page, "browser.input-consumed", sample_id)
    dispatched = await page.locator('[data-testid="wterm-surface"] textarea').evaluate(
        """(textarea, payload) => {
          const event = new Event('paste', { bubbles: true, cancelable: true });
          Object.defineProperty(event, 'clipboardData', {
            value: {
              getData: (format) => format === 'text' || format === 'text/plain' ? payload : '',
            },
          });
          return textarea.dispatchEvent(event);
        }""",
        payload,
    )
    if dispatched is not False:
        raise JourneyError("WTerm did not consume the deterministic paste event")
    await page.wait_for_function(
        "sampleId => window.__zhongduanE0.events.some(event => "
        "event.name === 'browser.send-decision' && event.sampleId === sampleId)",
        arg=sample_id,
    )
    await set_sample(page, None)


async def wait_for_grid(page: Any, sentinel: str, timeout_ms: int) -> None:
    await page.wait_for_function(
        "sentinel => { const grid = document.querySelector('.term-grid'); "
        "return grid !== null && (grid.textContent || '').includes(sentinel); }",
        arg=sentinel,
        timeout=timeout_ms,
    )


async def mark_render(page: Any, sample_id: str, matching: bool = True) -> None:
    await page.evaluate(
        "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
    )
    if matching:
        await append_browser_event(page, "browser.matching-render", sample_id)
    await append_browser_event(page, "browser.useful-render", sample_id)


async def intent_observation(page: Any, sample_id: str, timeout_ms: int) -> dict[str, Any]:
    with suppress(Exception):
        await page.wait_for_function(
            "sampleId => window.__zhongduanE0.intentsBySample[sampleId]?.terminal !== null",
            arg=sample_id,
            timeout=min(timeout_ms, 5_000),
        )
    intent = await page.evaluate(
        """sampleId => {
          const intent = window.__zhongduanE0.intentsBySample[sampleId];
          if (intent === undefined) return null;
          return {
            sampleId: intent.sampleId,
            localIntentId: intent.localIntentId,
            terminal: intent.terminal === null ? null : { ...intent.terminal },
            browserIdentities: [...new Set(intent.sends.map((send) => send.identity))].sort(),
            sendAttemptCount: intent.sends.length,
          };
        }""",
        sample_id,
    )
    if not isinstance(intent, dict) or not isinstance(intent.get("localIntentId"), str):
        raise JourneyError(f"sample {sample_id} was not recorded at UI consumption")
    terminal = intent.get("terminal")
    records = [terminal] if isinstance(terminal, dict) else []
    return {
        "sampleId": sample_id,
        "localIntentId": intent["localIntentId"],
        "consumed": True,
        "terminalOutcomes": [record["outcome"] for record in records],
        "terminalRecords": records,
        "passiveBrowserIdentities": intent.get("browserIdentities", []),
        "passiveSendAttemptCount": intent.get("sendAttemptCount", 0),
    }


async def run_probe(
    page: Any,
    sample_id: str,
    timeout_ms: int,
    *,
    retain_observation: bool = True,
) -> dict[str, Any] | None:
    payload = f"{PROBE_PREFIX.decode('ascii')}{sample_id}\r"
    await dispatch_paste(page, payload, sample_id)
    await wait_for_grid(page, f"{RESULT_PREFIX}{sample_id}", timeout_ms)
    await mark_render(page, sample_id)
    observation = await intent_observation(page, sample_id, timeout_ms)
    return observation if retain_observation else None


async def run_fault_command(
    page: Any,
    payload: str,
    sample_id: str,
    timeout_ms: int,
) -> dict[str, Any]:
    """Observe a fault input without requiring an effect from CURRENT."""

    await dispatch_paste(page, payload, sample_id)
    observation = await intent_observation(page, sample_id, timeout_ms)
    rendered = False
    try:
        await wait_for_grid(
            page,
            f"{RESULT_PREFIX}{sample_id}",
            min(timeout_ms, 5_000),
        )
        await mark_render(page, sample_id)
        rendered = True
    except PlaywrightTimeoutError:
        # Deterministic rejection and acceptance uncertainty legitimately have no output.
        pass
    observation["matchingOutputObserved"] = rendered
    return observation


async def run_ctrl_c(
    page: Any,
    sample_id: str,
    timeout_ms: int,
    *,
    trace: TraceStore,
    output_flood: bool,
    duplicate: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    setup_intents: list[dict[str, Any]] = []
    if output_flood:
        flood_sample = f"flood-command-{sample_id}"
        await dispatch_paste(
            page,
            f"{FLOOD_PREFIX.decode('ascii')}{flood_sample}\r",
            flood_sample,
        )
        await wait_for_grid(page, f"E0-FLOOD-{flood_sample}-", timeout_ms)
        setup_intents.append(await intent_observation(page, flood_sample, timeout_ms))
    arm_sample = f"arm-{sample_id}"
    await dispatch_paste(
        page,
        f"{INTERRUPT_ARM_PREFIX.decode('ascii')}{sample_id}:{arm_sample}\r",
        arm_sample,
    )
    setup_intents.append(await intent_observation(page, arm_sample, timeout_ms))
    trace.pending_ctrl_sample = sample_id
    await page.keyboard.down("Control")
    await set_sample(page, sample_id, duplicate=duplicate)
    await page.keyboard.down("KeyC")
    await set_sample(page, None)
    await page.keyboard.up("KeyC")
    await page.keyboard.up("Control")
    await wait_for_grid(page, f"{QUIET_PREFIX}{sample_id}", timeout_ms)
    await page.evaluate(
        "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
    )
    await append_browser_event(page, "browser.application-quiet", sample_id)
    await mark_render(page, sample_id)
    intent = await intent_observation(page, sample_id, timeout_ms)
    return [*setup_intents, intent], {"sampleId": sample_id, "outputFlood": output_flood}


async def wait_live_writer(page: Any, timeout_ms: int) -> None:
    await page.locator("main[data-phase='live']").wait_for(timeout=timeout_ms)
    await page.locator(".ownership-button[data-owned='true']").wait_for(timeout=timeout_ms)
    textarea = page.locator('[data-testid="wterm-surface"] textarea')
    await textarea.wait_for(timeout=timeout_ms)
    await textarea.focus()


async def wait_live_observer(page: Any, timeout_ms: int) -> None:
    await page.locator("main[data-phase='live']").wait_for(timeout=timeout_ms)
    await page.locator('[data-testid="wterm-surface"] textarea').wait_for(timeout=timeout_ms)


async def wait_for_snapshot_count(
    trace: TraceStore, minimum: int, timeout_ms: int
) -> None:
    deadline = time.monotonic() + timeout_ms / 1_000
    while len(trace.snapshot_finalizations) < minimum:
        if time.monotonic() >= deadline:
            raise JourneyError(
                f"snapshot-enabled workload finalized fewer than {minimum} snapshots"
            )
        await asyncio.sleep(0.05)


async def wait_for_initial_snapshot_expiry(trace: TraceStore, timeout_ms: int) -> None:
    if not trace.snapshot_finalizations:
        raise JourneyError("snapshot-enabled workload lacks an initial checkpoint")
    finalized_at_ns = trace.snapshot_finalizations[0]["atUnixNs"]
    expires_after_ms = (
        SNAPSHOT_CHECKPOINT_TTL_MS + SNAPSHOT_CHECKPOINT_EXPIRY_CUSHION_MS
    )
    remaining_ms = expires_after_ms - (time.time_ns() - finalized_at_ns) / 1_000_000
    if remaining_ms <= 0:
        return
    if remaining_ms >= timeout_ms:
        raise JourneyError("snapshot checkpoint expiry exceeds the scenario deadline")
    await asyncio.sleep(remaining_ms / 1_000)


async def browser_journey(
    browser_origin: str,
    session: dict[str, Any],
    trace: TraceStore,
    samples: int,
    warmups: int,
    variant: str,
    bulk_backlog_bytes: int,
    timeout_ms: int,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any],
]:
    async with async_playwright() as playwright:
        launch: dict[str, Any] = {"headless": True}
        executable = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE")
        if executable:
            launch["executable_path"] = executable
        browser = await playwright.chromium.launch(**launch)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()
        browser_messages: list[str] = []
        page.on(
            "console",
            lambda message: browser_messages.append(
                f"console:{message.type}:{message.text}"[:2_000]
            ),
        )
        page.on(
            "pageerror",
            lambda error: browser_messages.append(f"pageerror:{error}"[:2_000]),
        )
        await page.add_init_script(INIT_SCRIPT)
        journey_pages = [page]
        snapshot_requested = asyncio.Event()
        release_snapshot = asyncio.Event()

        async def hold_snapshot(route: Any, request: Any) -> None:
            if request.method != "GET":
                await route.continue_()
                return
            snapshot_requested.set()
            await release_snapshot.wait()
            await route.continue_()

        await page.route(f"{browser_origin}/api/v1/sessions/*/snapshots/*", hold_snapshot)
        target = (
            f"{browser_origin}/sessions/{session['sessionId']}"
            f"#capability={session['writerCapability']}"
        )
        second_page = None
        observer_page = None
        observer_navigation: asyncio.Task[Any] | None = None
        active_page = page
        stage = "navigate"
        try:
            await page.goto(target, wait_until="commit", timeout=timeout_ms)
            stage = "await-snapshot-or-live"
            snapshot_wait = asyncio.create_task(snapshot_requested.wait())
            live_wait = asyncio.create_task(wait_for_grid(page, READY_SENTINEL, timeout_ms))
            done, pending = await asyncio.wait(
                (snapshot_wait, live_wait),
                timeout=timeout_ms / 1_000,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                for task in pending:
                    task.cancel()
                raise JourneyError("Browser neither requested a snapshot nor reached live output")
            cold_snapshot_exercised = snapshot_wait in done
            if cold_snapshot_exercised:
                premature = await page.locator(".term-grid").evaluate(
                    "(grid, sentinel) => (grid.textContent || '').includes(sentinel)",
                    READY_SENTINEL,
                )
            else:
                premature = None
            release_snapshot.set()
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            stage = "await-ready-after-snapshot"
            await wait_for_grid(page, READY_SENTINEL, timeout_ms)
            stage = "await-live-writer"
            await wait_live_writer(page, timeout_ms)
            if variant in {"snapshot-disabled", "snapshot-enabled"}:
                await wait_for_snapshot_count(trace, 1, timeout_ms)

            observations = {
                "intents": [],
                "ctrlC": [],
                "writerTransfers": [],
                "coldCandidates": [],
                "secureInput": [],
            }
            warmup_observations = {"intents": [], "ctrlC": []}
            acceptance_reconnect_observed = False
            primary_sample_ids: list[str] = []
            warmup_primary_sample_ids: list[str] = []
            warmup_sample_ids: set[str] = set()
            snapshot_count_before_measurement = len(trace.snapshot_finalizations)

            async def execute_sample(index: int, warmup: bool) -> None:
                prefix = "warmup" if warmup else "measured"
                primary = f"{prefix}-{variant}-{index:03d}"
                if warmup:
                    warmup_sample_ids.add(primary)
                    warmup_primary_sample_ids.append(primary)
                else:
                    primary_sample_ids.append(primary)
                intent_target = (
                    warmup_observations["intents"] if warmup else observations["intents"]
                )
                ctrl_target = (
                    warmup_observations["ctrlC"] if warmup else observations["ctrlC"]
                )

                if variant in {"steady", "snapshot-disabled", "snapshot-enabled", "correctness-faults"}:
                    probe_sample = f"probe-{primary}"
                    if warmup:
                        warmup_sample_ids.add(probe_sample)
                    probe = await run_probe(page, probe_sample, timeout_ms)
                    assert probe is not None
                    intent_target.append(probe)
                elif variant.startswith("bulk-backlog-"):
                    if bulk_backlog_bytes > 0:
                        flood_sample = f"flood-command-{primary}"
                        if warmup:
                            warmup_sample_ids.add(flood_sample)
                        await dispatch_paste(
                            page,
                            f"{FLOOD_PREFIX.decode('ascii')}{flood_sample}\r",
                            flood_sample,
                        )
                        await wait_for_grid(page, f"E0-FLOOD-{flood_sample}-", timeout_ms)
                        intent_target.append(
                            await intent_observation(page, flood_sample, timeout_ms)
                        )
                    probe_sample = f"probe-{primary}"
                    if warmup:
                        warmup_sample_ids.add(probe_sample)
                    probe = await run_probe(page, probe_sample, timeout_ms)
                    assert probe is not None
                    intent_target.append(probe)
                elif variant == "output-flood":
                    ctrl_sample = f"ctrl-c-{primary}"
                    intent_items, ctrl_item = await run_ctrl_c(
                        page,
                        ctrl_sample,
                        timeout_ms,
                        trace=trace,
                        output_flood=True,
                    )
                    if warmup:
                        warmup_sample_ids.update(
                            item["sampleId"] for item in intent_items
                        )
                        warmup_sample_ids.add(ctrl_sample)
                    intent_target.extend(intent_items)
                    ctrl_target.append(ctrl_item)
                else:
                    raise JourneyError(f"unsupported E0 workload variant {variant}")

                if variant == "steady":
                    ctrl_sample = f"ctrl-c-{primary}"
                    intent_items, ctrl_item = await run_ctrl_c(
                        page,
                        ctrl_sample,
                        timeout_ms,
                        trace=trace,
                        output_flood=False,
                    )
                    if warmup:
                        warmup_sample_ids.update(
                            item["sampleId"] for item in intent_items
                        )
                        warmup_sample_ids.add(ctrl_sample)
                    intent_target.extend(intent_items)
                    ctrl_target.append(ctrl_item)

            for index in range(warmups):
                await execute_sample(index, True)

            snapshot_count_before_measurement = len(trace.snapshot_finalizations)
            measurement_started_at_unix_ms = time.time_ns() // 1_000_000
            snapshot_post_midpoint_sample_ids: set[str] = set()
            snapshot_input_overlap: dict[str, Any] | None = None
            midpoint = samples // 2
            attach_index = samples - SNAPSHOT_ATTACH_REMAINING_SAMPLES
            for index in range(samples):
                if variant == "snapshot-enabled" and index >= midpoint:
                    probe_sample = f"probe-measured-{variant}-{index:03d}"
                    snapshot_post_midpoint_sample_ids.add(probe_sample)
                    if index > midpoint:
                        stage = "pace-snapshot-overlap-inputs"
                        await asyncio.sleep(SNAPSHOT_OVERLAP_SAMPLE_INTERVAL_MS / 1_000)

                if variant == "snapshot-enabled" and index == attach_index:
                    stage = "await-expired-snapshot-checkpoint"
                    await wait_for_initial_snapshot_expiry(trace, timeout_ms)
                    stage = "coordinate-snapshot-attach-with-input"
                    observer_page = await context.new_page()
                    journey_pages.append(observer_page)
                    await observer_page.add_init_script(INIT_SCRIPT)
                    observer_target = (
                        f"{browser_origin}/sessions/{session['sessionId']}"
                        f"#capability={session['observerCapability']}"
                    )
                    trace.snapshot_attach_held.clear()
                    trace.snapshot_attach_release.clear()
                    trace.hold_next_snapshot_attach = True
                    observer_navigation = asyncio.create_task(
                        observer_page.goto(
                            observer_target, wait_until="commit", timeout=timeout_ms
                        )
                    )
                    await asyncio.wait_for(
                        trace.snapshot_attach_held.wait(), timeout=timeout_ms / 1_000
                    )

                    held, release = trace.hold(probe_sample)
                    sample_task = asyncio.create_task(execute_sample(index, False))
                    try:
                        await asyncio.wait_for(held.wait(), timeout=timeout_ms / 1_000)
                        trace.snapshot_attach_release.set()
                        release.set()
                        await sample_task
                    except BaseException:
                        trace.snapshot_attach_release.set()
                        release.set()
                        if not sample_task.done():
                            sample_task.cancel()
                        await asyncio.gather(sample_task, return_exceptions=True)
                        raise
                    continue

                await execute_sample(index, False)

            if variant == "snapshot-enabled":
                assert observer_page is not None
                assert observer_navigation is not None
                await observer_navigation
                await wait_live_observer(observer_page, timeout_ms)
                await wait_for_snapshot_count(
                    trace, snapshot_count_before_measurement + 1, timeout_ms
                )
                post_midpoint_receives = sorted(
                    (
                        event
                        for event in trace.events
                        if event.get("name") == "host.receive"
                        and event.get("sampleId") in snapshot_post_midpoint_sample_ids
                        and isinstance(event.get("atUnixNs"), int)
                    ),
                    key=lambda event: event["atUnixNs"],
                )
                new_finalizations = trace.snapshot_finalizations[
                    snapshot_count_before_measurement:
                ]
                if len(post_midpoint_receives) < 2:
                    raise JourneyError("snapshot overlap lacks post-midpoint Host inputs")
                first_receive = post_midpoint_receives[0]
                last_receive = post_midpoint_receives[-1]
                overlapped = next(
                    (
                        item
                        for item in new_finalizations
                        if first_receive["atUnixNs"]
                        < item["atUnixNs"]
                        < last_receive["atUnixNs"]
                    ),
                    None,
                )
                if overlapped is None:
                    raise JourneyError(
                        "snapshot finalization did not overlap post-midpoint Host inputs"
                    )
                snapshot_input_overlap = {
                    "firstHostReceiveAtUnixNs": first_receive["atUnixNs"],
                    "firstSampleId": first_receive["sampleId"],
                    "snapshotFinalizationAtUnixNs": overlapped["atUnixNs"],
                    "snapshotId": overlapped["snapshotId"],
                    "lastHostReceiveAtUnixNs": last_receive["atUnixNs"],
                    "lastSampleId": last_receive["sampleId"],
                }

            if variant == "correctness-faults":
                observations["coldCandidates"].append(
                    {
                        "sampleId": "cold-attach-000",
                        "visibleBeforeValidation": premature,
                    }
                )

                duplicate_sample = "duplicate-000"
                await dispatch_paste(
                    page,
                    f"{PROBE_PREFIX.decode('ascii')}{duplicate_sample}\r",
                    duplicate_sample,
                    duplicate=True,
                )
                await wait_for_grid(page, f"{RESULT_PREFIX}{duplicate_sample}", timeout_ms)
                await mark_render(page, duplicate_sample)
                observations["intents"].append(
                    await intent_observation(page, duplicate_sample, timeout_ms)
                )

                uncertainty_sample = "uncertain-000"
                trace.disconnect_sample = uncertainty_sample
                await dispatch_paste(
                    page,
                    f"{PROBE_PREFIX.decode('ascii')}{uncertainty_sample}\r",
                    uncertainty_sample,
                )
                stage = "await-uncertain-reconnect"
                await page.locator("main[data-phase='reconnecting']").wait_for(
                    timeout=timeout_ms
                )
                await wait_live_writer(page, timeout_ms)
                acceptance_reconnect_observed = True
                # Acceptance uncertainty permits either a PTY effect or no effect. Waiting for
                # matching output would therefore reject truthful CURRENT evidence when the
                # disconnect wins the race. Once the original connection is live again, retain a
                # short quiescence window so any automatic retry is visible to the passive proxy.
                stage = "observe-uncertain-no-retry"
                await page.wait_for_timeout(1_000)
                uncertainty = await intent_observation(
                    page, uncertainty_sample, timeout_ms
                )
                uncertainty["acceptanceUncertaintyInjected"] = True
                observations["intents"].append(uncertainty)

                stage = "exercise-writer-transfer"
                old_sample = "old-writer-000"
                held, release = trace.hold(old_sample)
                paste_task = asyncio.create_task(
                    dispatch_paste(
                        page,
                        f"{PROBE_PREFIX.decode('ascii')}{old_sample}\r",
                        old_sample,
                    )
                )
                await asyncio.wait_for(held.wait(), timeout=timeout_ms / 1000)
                storage = await page.evaluate(
                    "() => Object.fromEntries(Object.entries(sessionStorage))"
                )
                second_page = await context.new_page()
                journey_pages.append(second_page)
                await second_page.add_init_script(INIT_SCRIPT)
                await second_page.add_init_script(
                    "(() => { const entries = "
                    + json.dumps(storage, separators=(",", ":"))
                    + "; for (const [key, value] of Object.entries(entries)) "
                    "sessionStorage.setItem(key, value); })();"
                )
                await second_page.goto(target, wait_until="commit", timeout=timeout_ms)
                await wait_live_writer(second_page, timeout_ms)
                release.set()
                await paste_task
                observations["intents"].append(
                    await intent_observation(page, old_sample, timeout_ms)
                )
                observations["writerTransfers"].append(
                    {
                        "sampleId": "writer-transfer-000",
                        "oldWriterSuccessfulEffects": 0,
                    }
                )
                active_page = second_page

                new_sample = "new-writer-000"
                stage = "observe-new-writer-result"
                observations["intents"].append(
                    await run_fault_command(
                        active_page,
                        f"{PROBE_PREFIX.decode('ascii')}{new_sample}\r",
                        new_sample,
                        timeout_ms,
                    )
                )

                secure_sample = "secure-000"
                stage = "observe-secure-input-result"
                observations["intents"].append(
                    await run_fault_command(
                        active_page,
                        f"{SECURE_PREFIX.decode('ascii')}{secure_sample}\r",
                        secure_sample,
                        timeout_ms,
                    )
                )
                speculative_count = await active_page.locator(
                    "[data-presentation='speculative'], [data-speculative='true'], "
                    ".speculative-presentation"
                ).count()
                observations["secureInput"].append(
                    {
                        "sampleId": secure_sample,
                        "speculativePresentationCount": speculative_count,
                    }
                )

            measurement_ended_at_unix_ms = time.time_ns() // 1_000_000

            browser_events: list[dict[str, Any]] = []
            for source_page in journey_pages:
                browser_events.extend(
                    await source_page.evaluate("() => window.__zhongduanE0.events")
                )
            browser_events = [
                event
                for event in browser_events
                if isinstance(event, dict)
                and event.get("sampleId") not in warmup_sample_ids
            ]
            snapshot_delta = (
                len(trace.snapshot_finalizations) - snapshot_count_before_measurement
            )
            workload_evidence = {
                "primarySampleIds": primary_sample_ids,
                "warmupPrimarySampleIds": warmup_primary_sample_ids,
                "warmupCount": warmups,
                "configuredBulkBacklogBytes": bulk_backlog_bytes,
                "snapshotFinalizationsDuringMeasurement": snapshot_delta,
                "snapshotInputOverlap": snapshot_input_overlap,
                "measurementStartedAtUnixMs": measurement_started_at_unix_ms,
                "measurementEndedAtUnixMs": measurement_ended_at_unix_ms,
                "outputFlood": variant == "output-flood"
                or (variant.startswith("bulk-backlog-") and bulk_backlog_bytes > 0),
                "acceptanceDisconnect": trace.disconnect_applied,
                "acceptanceReconnectObserved": acceptance_reconnect_observed,
                "writerTransfer": bool(observations["writerTransfers"]),
                "coldAttachValidation": bool(observations["coldCandidates"])
                and cold_snapshot_exercised,
                "coldSnapshotRequested": cold_snapshot_exercised,
            }
            return (
                observations,
                warmup_observations,
                browser_events,
                workload_evidence,
            )
        except Exception as error:
            browser_state: list[object] = []
            for diagnostic_page in journey_pages:
                try:
                    browser_state.append(
                        await diagnostic_page.evaluate(
                            """() => ({
                      mainPhase: document.querySelector('main')?.getAttribute('data-phase') ?? null,
                      gridText: (document.querySelector('.term-grid')?.textContent ?? '').slice(-4096),
                      ownership: document.querySelector('.ownership-button')?.getAttribute('data-owned') ?? null,
                      inputSurfaceTextareaCount: document.querySelectorAll('[data-testid="wterm-surface"] textarea').length,
                      activeElement: document.activeElement?.tagName ?? null,
                      e0Events: (window.__zhongduanE0?.events ?? []).slice(-50),
                      e0Intents: (window.__zhongduanE0?.intents ?? []).slice(-10),
                    })"""
                        )
                    )
                except Exception as state_error:
                    browser_state.append({"captureError": str(state_error)})
            raise JourneyError(
                f"browser stage {stage} failed: {error}; "
                f"state={json.dumps(browser_state, sort_keys=True)}; "
                f"messages={json.dumps(browser_messages[-20:])}"
            ) from error
        finally:
            release_snapshot.set()
            trace.snapshot_attach_release.set()
            if observer_navigation is not None and not observer_navigation.done():
                observer_navigation.cancel()
                await asyncio.gather(observer_navigation, return_exceptions=True)
            with suppress(Exception):
                await context.close()
            with suppress(Exception):
                await browser.close()


def git_output(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def update_observation_effects(
    observations: dict[str, Any], trace: TraceStore, fixture_events: list[dict[str, Any]]
) -> None:
    pty_effects = Counter(
        event["sampleId"]
        for event in fixture_events
        if event.get("name") == "host.pty-write" and isinstance(event.get("sampleId"), str)
    )
    for item in observations.get("intents", []):
        sample = item.get("sampleId", item["localIntentId"])
        browser_identities = sorted(trace.sample_browser_identities.get(sample, set()))
        identities = sorted(trace.sample_wire_identities.get(sample, set()))
        item["ptyEffectCount"] = pty_effects[sample]
        if browser_identities:
            item["browserIdentity"] = next(
                identity
                for identity, retained_sample in trace.identity_samples.items()
                if retained_sample == sample
            )
            item["browserIdentities"] = browser_identities
        if identities:
            item["wireIdentity"] = trace.sample_identities[sample]
            item["wireIdentities"] = identities
        if item.get("acceptanceUncertaintyInjected") is True:
            total_sends = sum(
                trace.browser_send_counts[identity] for identity in browser_identities
            )
            item["automaticRetryCount"] = max(0, total_sends - 1)
            item["identityChanged"] = len(browser_identities) > 1
    for item in observations.get("ctrlC", []):
        sample = item["sampleId"]
        identity = trace.sample_identities.get(sample)
        if identity is not None:
            item["wireIdentity"] = identity
        item["ptyEffectCount"] = pty_effects[sample]
    if observations.get("writerTransfers"):
        old_effects = pty_effects["old-writer-000"]
        observations["writerTransfers"][0]["oldWriterSuccessfulEffects"] = old_effects


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if ClientSession is None or web is None or async_playwright is None:
        raise JourneyError(
            "E0 journey dependencies are required: "
            "python3 -m pip install -r scripts/requirements-e0-terminal-journey.txt; "
            "python3 -m playwright install chromium"
        )
    contract = load_json(CONTRACT_PATH)
    validate_contract(contract)
    source_revision = git_output("rev-parse", "HEAD")
    source_tree_git_oid = git_output("rev-parse", "HEAD^{tree}")
    source_tree_dirty = bool(git_output("status", "--porcelain=v1", "--untracked-files=all"))
    if source_tree_dirty and not args.allow_dirty_development:
        raise JourneyError(
            "E0 scenarios must run from a clean committed tree; "
            "--allow-dirty-development may only produce an unmergeable development artifact"
        )
    if DEV_VARS.exists():
        raise JourneyError(f"refusing to overwrite existing local bindings: {DEV_VARS}")
    if not VP.exists():
        raise JourneyError("dependencies are not installed")
    check_node_pty_spawn_helper()
    temporary = Path(tempfile.mkdtemp(prefix="zhongduan-e0-journey-"))
    workerd: ManagedProcess | None = None
    host: ManagedProcess | None = None
    browser_proxy: LinkProxy | None = None
    host_proxy: LinkProxy | None = None
    session_info = temporary / "session.json"
    capture_path = temporary / "input-capture.bin"
    fixture_log = temporary / "fixture-events.jsonl"
    host_measurements_log = temporary / "host-measurements.jsonl"
    trace = TraceStore(args.variant)
    generated_paths = [APP_ROOT / ".wrangler", HOST_CLI.parent]
    preexisting = {path for path in generated_paths if path.exists()}
    dev_vars_created = False
    try:
        with DEV_VARS.open("x", encoding="utf-8") as bindings:
            dev_vars_created = True
            bindings.write(
                f'BOOTSTRAP_TOKEN="{BOOTSTRAP_TOKEN}"\n'
                f'CAPABILITY_SIGNING_KEY="{CAPABILITY_KEY}"\n'
            )
        DEV_VARS.chmod(0o600)
        subprocess.run(["node", str(VP), "run", "build"], cwd=ROOT, check=True)
        workerd_port = free_port()
        workerd_origin = f"http://127.0.0.1:{workerd_port}"
        workerd_env = os.environ.copy()
        workerd_env["ZHONGDUAN_E0_CLOUDFLARE_STATE_PATH"] = str(temporary / "workerd-state")
        workerd = ManagedProcess(
            "Vite Workerd",
            [
                "node",
                str(VP),
                "dev",
                "--host",
                "127.0.0.1",
                "--port",
                str(workerd_port),
                "--strictPort",
                "--mode",
                "e0-journey",
            ],
            APP_ROOT,
            workerd_env,
        )
        await asyncio.to_thread(wait_for_http, workerd_origin, workerd, args.timeout_seconds)
        browser_proxy = LinkProxy(
            "browser",
            workerd_origin,
            args.browser_cloud_rtt_ms,
            args.jitter_ms,
            trace,
            args.seed,
        )
        host_proxy = LinkProxy(
            "host",
            workerd_origin,
            args.cloud_host_rtt_ms,
            args.jitter_ms,
            trace,
            args.seed + 1,
        )
        await browser_proxy.start()
        await host_proxy.start()
        assert browser_proxy.origin is not None and host_proxy.origin is not None

        host_env = os.environ.copy()
        host_env["ZHONGDUAN_BOOTSTRAP_TOKEN"] = BOOTSTRAP_TOKEN
        host_env[INPUT_CAPTURE_ENV] = str(capture_path)
        host_env[EVENT_LOG_ENV] = str(fixture_log)
        host_env[E4_EVIDENCE_ENV] = str(host_measurements_log)
        if args.variant == "snapshot-disabled":
            host_env[E0_DISABLE_SNAPSHOT_REFRESH_ENV] = "1"
        host_env[FLOOD_CHUNK_BYTES_ENV] = str(contract["workload"]["outputFlood"]["chunkBytes"])
        bulk_backlog_bytes = (
            int(args.variant.removeprefix("bulk-backlog-"))
            if args.variant.startswith("bulk-backlog-")
            else 0
        )
        maximum_flood_bytes = (
            bulk_backlog_bytes
            if bulk_backlog_bytes > 0
            else contract["workload"]["outputFlood"]["maximumBytes"]
        )
        host_env[FLOOD_MAX_BYTES_ENV] = str(maximum_flood_bytes)
        host_env[FLOOD_MAX_DURATION_MS_ENV] = str(
            contract["workload"]["outputFlood"]["maximumDurationMs"]
        )
        host_env["PYTHONDONTWRITEBYTECODE"] = "1"
        fixture_python = shutil.which("python3")
        if fixture_python is None:
            raise JourneyError("python3 is required for the deterministic PTY fixture")
        host = ManagedProcess(
            "Host relay",
            [
                "node",
                str(HOST_CLI),
                "cloud",
                "--url",
                host_proxy.origin,
                "--session-info-file",
                str(session_info),
                "--",
                "/bin/sh",
                "-c",
                "exec "
                + shlex.quote(fixture_python)
                + " "
                + shlex.quote(str(ROOT / "scripts" / "e0_terminal_fixture.py")),
            ],
            ROOT,
            host_env,
        )
        session = await asyncio.to_thread(
            wait_for_json, session_info, host, args.timeout_seconds
        )
        try:
            await asyncio.wait_for(
                trace.host_ready_acknowledged.wait(), timeout=args.timeout_seconds
            )
        except TimeoutError as error:
            raise JourneyError(
                "Host did not receive host-ready-ack before the Browser journey"
            ) from error
        host.assert_running()
        (
            observations,
            warmup_observations,
            browser_events,
            workload_evidence,
        ) = await browser_journey(
            browser_proxy.origin,
            session,
            trace,
            args.samples,
            args.warmups,
            args.variant,
            bulk_backlog_bytes,
            args.timeout_seconds * 1000,
        )
        if bulk_backlog_bytes > 0:
            final_flood_sample = (
                f"flood-command-measured-{args.variant}-{args.samples - 1:03d}"
            )
            await wait_for_fixture_event(
                fixture_log,
                "fixture.flood-stopped",
                final_flood_sample,
                args.timeout_seconds,
            )
        host.assert_running()
        workerd.assert_running()
        fixture_events = read_jsonl(fixture_log, args.variant)
        warmup_sample_ids = {
            item["sampleId"]
            for item in warmup_observations["intents"]
            if isinstance(item, dict) and isinstance(item.get("sampleId"), str)
        }
        warmup_sample_ids.update(
            item["sampleId"]
            for item in warmup_observations["ctrlC"]
            if isinstance(item, dict) and isinstance(item.get("sampleId"), str)
        )
        fixture_events = [
            event
            for event in fixture_events
            if event.get("sampleId") not in warmup_sample_ids
        ]
        trace.events = [
            event
            for event in trace.events
            if event.get("sampleId") not in warmup_sample_ids
        ]
        update_observation_effects(observations, trace, fixture_events)
        all_fixture_events = read_jsonl(fixture_log, args.variant)
        update_observation_effects(warmup_observations, trace, all_fixture_events)

        events = [*trace.events, *fixture_events]
        for event in browser_events:
            if isinstance(event, dict):
                events.append({**event, "variant": args.variant})
        environment = {
            "executionTier": "local-workerd",
            "platform": platform.platform(),
            "python": platform.python_version(),
            "node": subprocess.run(
                ["node", "--version"], capture_output=True, check=True, text=True
            ).stdout.strip(),
            "wtermGitlink": git_output("rev-parse", "HEAD:vendor/wterm"),
        }
        report = {
            "schemaVersion": "zhongduan-terminal-journey-scenario-v1",
            "contractSha256": canonical_sha256(contract),
            "status": "measured",
            "sourceRevision": source_revision,
            "sourceTreeGitOid": source_tree_git_oid,
            "sourceTreeDirty": source_tree_dirty,
            "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "environment": environment,
            "variant": args.variant,
            "samples": args.samples,
            "warmups": args.warmups,
            "appliedProfile": {
                "browserCloudRttMs": args.browser_cloud_rtt_ms,
                "cloudHostRttMs": args.cloud_host_rtt_ms,
                "jitterMs": args.jitter_ms,
                "networkFault": args.network_fault,
                "seed": args.seed,
                "networkImplementation": "two independent aiohttp userspace proxies",
            },
            "cloudSpanBoundary": {
                "start": "browser proxy receive after configured Browser/Cloud link delay",
                "end": "host proxy send before configured Cloud/Host link delay",
                "includesBrowserLink": False,
                "includesHostLink": False,
            },
            "workloadEvidence": workload_evidence,
            "observations": observations,
            "warmupObservations": warmup_observations,
            "hostMeasurements": read_raw_jsonl(host_measurements_log),
            "events": events,
            "rawEventCount": len(events),
            "deadlineMs": args.timeout_seconds * 1000,
            "deadlineIsSlo": False,
        }
        validate_scenario_report(
            report, contract, require_clean=not args.allow_dirty_development
        )
        return report
    except Exception as error:
        diagnostic_path = Path(tempfile.gettempdir()) / "zhongduan-e0-last-failure.json"
        fixture_diagnostics: list[dict[str, Any]] = []
        host_measurement_diagnostics: list[dict[str, Any]] = []
        with suppress(Exception):
            fixture_diagnostics = read_raw_jsonl(fixture_log)
        with suppress(Exception):
            host_measurement_diagnostics = read_raw_jsonl(host_measurements_log)
        diagnostic = {
            "schemaVersion": "zhongduan-e0-failure-diagnostic-v1",
            "errorType": type(error).__name__,
            "error": str(error),
            "variant": args.variant,
            "sessionMetadataCreated": session_info.exists(),
            "hostReadyAcknowledged": trace.host_ready_acknowledged.is_set(),
            "traceEvents": trace.events,
            "snapshotFinalizations": trace.snapshot_finalizations,
            "fixtureEvents": fixture_diagnostics,
            "hostMeasurements": host_measurement_diagnostics,
            "hostTail": [] if host is None else host.sanitized_tail(),
            "workerdTail": [] if workerd is None else workerd.sanitized_tail(),
        }
        diagnostic_path.write_text(
            json.dumps(diagnostic, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        raise JourneyError(f"{error}; diagnostics: {diagnostic_path}") from error
    finally:
        if host is not None:
            host.stop()
        if browser_proxy is not None:
            await browser_proxy.close()
        if host_proxy is not None:
            await host_proxy.close()
        if workerd is not None:
            workerd.stop()
        if dev_vars_created:
            DEV_VARS.unlink(missing_ok=True)
        shutil.rmtree(temporary, ignore_errors=True)
        for path in generated_paths:
            if path not in preexisting:
                shutil.rmtree(path, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the reproducible E0 terminal journey")
    parser.add_argument("--browser-cloud-rtt-ms", type=int, default=20)
    parser.add_argument("--cloud-host-rtt-ms", type=int, default=20)
    parser.add_argument("--jitter-ms", type=int, default=0)
    parser.add_argument("--samples", type=int, default=24)
    parser.add_argument("--warmups", type=int, default=4)
    parser.add_argument("--seed", type=int, default=450)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--variant", choices=sorted(EXPECTED_VARIANTS), default="steady")
    parser.add_argument("--network-fault", choices=("none", "jitter"), default="none")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(tempfile.gettempdir()) / "zhongduan-e0-scenario.json",
    )
    parser.add_argument("--merge-scenarios", type=Path, nargs="+")
    parser.add_argument(
        "--artifact-kind", choices=("current", "candidate"), default="current"
    )
    parser.add_argument("--current-report", type=Path)
    parser.add_argument("--e4b-decision", type=Path)
    parser.add_argument("--allow-dirty-development", action="store_true")
    parser.add_argument("--matrix-plan", action="store_true")
    args = parser.parse_args()
    for name in (
        "browser_cloud_rtt_ms",
        "cloud_host_rtt_ms",
        "jitter_ms",
        "samples",
        "warmups",
        "timeout_seconds",
    ):
        if getattr(args, name) < (1 if name in {"samples", "warmups", "timeout_seconds"} else 0):
            parser.error(f"--{name.replace('_', '-')} is outside the supported range")
    if args.samples != 24 or args.warmups != 4:
        parser.error("checked E0 evidence requires exactly --samples 24 --warmups 4")
    if args.network_fault == "jitter" and args.jitter_ms == 0:
        parser.error("--network-fault jitter requires a positive --jitter-ms")
    if args.network_fault == "none" and args.jitter_ms != 0:
        parser.error("a non-zero --jitter-ms requires --network-fault jitter")
    if args.merge_scenarios is None and args.report.resolve() == BASELINE_PATH.resolve():
        parser.error("single scenarios cannot overwrite CURRENT; use --merge-scenarios")
    if args.artifact_kind == "candidate" and (
        args.merge_scenarios is None or args.current_report is None
    ):
        parser.error("candidate merge requires --merge-scenarios and --current-report")
    if args.artifact_kind == "current" and args.current_report is not None:
        parser.error("--current-report is only valid with --artifact-kind candidate")
    if args.e4b_decision is not None and args.artifact_kind != "candidate":
        parser.error("--e4b-decision is only valid with --artifact-kind candidate")
    return args


def merge_scenario_files(args: argparse.Namespace, contract: dict[str, Any]) -> dict[str, Any]:
    scenarios = [load_json(path) for path in args.merge_scenarios]
    if not scenarios:
        raise JourneyError("at least one scenario report is required")
    for scenario in scenarios:
        validate_scenario_report(scenario, contract)
    first = scenarios[0]
    source_revision = first["sourceRevision"]
    source_tree_git_oid = first["sourceTreeGitOid"]
    environment = first.get("environment")
    if not isinstance(environment, dict):
        raise JourneyError("scenario environment is missing")
    for scenario in scenarios[1:]:
        if (
            scenario.get("sourceRevision") != source_revision
            or scenario.get("sourceTreeGitOid") != source_tree_git_oid
        ):
            raise JourneyError("all merged scenarios must come from the same E0 commit")
        if scenario.get("environment") != environment:
            raise JourneyError("all merged scenarios must use the same reproducible environment")

    authority = run_authority_oracle(contract["workload"]["samplesPerVariant"])
    environment = {
        **environment,
        "ghosttyEngineId": authority.get("engineId"),
        "ghosttyArtifactVerified": authority.get("artifactVerified"),
    }
    evidence_boundary = {
        "realCloudflareEdge": False,
        "realComponents": [
            "Chromium",
            "WTerm/Ghostty WASM",
            "Browser application",
            "Vite Workerd/Miniflare Durable Object and R2",
            "Host daemon",
            "node-pty",
            "deterministic raw PTY child",
        ],
        "simulated": [
            "link RTT",
            "link jitter",
            "acceptance-uncertainty disconnect",
        ],
        "notClaimed": [
            "Cloudflare edge latency",
            "real Durable Object hibernation",
            "real Host relay process replacement",
        ],
        "cloudSpanBoundary": (
            "after Browser/Cloud proxy delay through before Cloud/Host proxy delay"
        ),
    }
    common = {
        "environment": environment,
        "evidence_boundary": evidence_boundary,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_revision": source_revision,
        "source_tree_git_oid": source_tree_git_oid,
        "deadline_ms": contract["deadlinePolicy"]["scenarioDeadlineMs"],
    }
    if args.artifact_kind == "current":
        return assemble_current_report(
            contract, scenarios, authority, **common
        )
    assert args.current_report is not None
    current_report = load_report_bundle(args.current_report)
    host_measurements = [
        {
            **measurement,
            "scenarioVariant": scenario.get("variant"),
            "measurementStartedAtUnixMs": scenario.get("workloadEvidence", {}).get(
                "measurementStartedAtUnixMs"
            ),
            "measurementEndedAtUnixMs": scenario.get("workloadEvidence", {}).get(
                "measurementEndedAtUnixMs"
            ),
        }
        for scenario in scenarios
        for measurement in scenario.get("hostMeasurements", [])
        if isinstance(measurement, dict)
    ]
    return assemble_candidate_report(
        contract,
        scenarios,
        authority,
        current_report,
        snapshot_phase_measurements=host_measurements,
        **common,
    )


def main() -> None:
    args = parse_args()
    contract = load_json(CONTRACT_PATH)
    validate_contract(contract)
    if args.matrix_plan:
        print(json.dumps(matrix_cells(contract), indent=2, sort_keys=True))
        return
    try:
        report = (
            merge_scenario_files(args, contract)
            if args.merge_scenarios is not None
            else asyncio.run(run(args))
        )
    except (JourneyError, ValueError) as error:
        raise SystemExit(f"E0 journey failed: {error}") from None
    if args.merge_scenarios is None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written_schema = report["schemaVersion"]
    else:
        written_schema = write_report_bundle(args.report, report)["schemaVersion"]
    if args.e4b_decision is not None:
        assert args.current_report is not None
        decision = build_e4b_decision(
            load_report_bundle(args.current_report), report, contract
        )
        args.e4b_decision.parent.mkdir(parents=True, exist_ok=True)
        args.e4b_decision.write_text(
            json.dumps(decision, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(
        json.dumps(
            {
                "report": str(args.report),
                "schemaVersion": written_schema,
                "variant": report.get("variant"),
                "oracles": Counter(
                    item["status"] for item in report.get("oracleResults", {}).values()
                ),
                "latencySamples": len(report.get("latencySamples", [])),
            },
            default=dict,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
