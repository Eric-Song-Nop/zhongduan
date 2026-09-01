#!/usr/bin/env python3

"""Deterministic raw PTY child used by the E0 real journey."""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import sys
import threading
import time
import tty


READY_SENTINEL = "ZHONGDUAN_E0_READY"
PROBE_PREFIX = b"ZHONGDUAN_E0_PROBE:"
FLOOD_COMMAND = b"ZHONGDUAN_E0_FLOOD"
FLOOD_PREFIX = b"ZHONGDUAN_E0_FLOOD:"
INTERRUPT_ARM_PREFIX = b"ZHONGDUAN_E0_INTERRUPT_ARM:"
RESULT_PREFIX = "ZHONGDUAN_E0_RESULT:"
INTERRUPT_PREFIX = "ZHONGDUAN_E0_INTERRUPT:"
QUIET_PREFIX = "ZHONGDUAN_E0_QUIET:"
SECURE_PREFIX = b"ZHONGDUAN_E0_SECURE:"
INPUT_CAPTURE_ENV = "ZHONGDUAN_E0_INPUT_CAPTURE"
EVENT_LOG_ENV = "ZHONGDUAN_E0_EVENT_LOG"
FLOOD_CHUNK_BYTES_ENV = "ZHONGDUAN_E0_FLOOD_CHUNK_BYTES"
FLOOD_MAX_BYTES_ENV = "ZHONGDUAN_E0_FLOOD_MAX_BYTES"
FLOOD_MAX_DURATION_MS_ENV = "ZHONGDUAN_E0_FLOOD_MAX_DURATION_MS"


class FixtureProtocolError(ValueError):
    pass


class FixtureState:
    def __init__(self) -> None:
        self.pending = bytearray()
        self.effect_counts: Counter[str] = Counter()
        self.interrupt_count = 0
        self.flood_requested = False
        self.next_interrupt_sample: str | None = None

    def accept(self, payload: bytes) -> list[tuple[str, str]]:
        if not isinstance(payload, bytes):
            raise FixtureProtocolError("fixture input must be bytes")
        self.pending.extend(payload)
        events: list[tuple[str, str]] = []
        while self.pending:
            if self.pending[0] == 0x03:
                del self.pending[0]
                self.interrupt_count += 1
                sample_id = self.next_interrupt_sample
                self.next_interrupt_sample = None
                events.append(
                    (
                        "interrupt",
                        sample_id or f"ctrl-c-{self.interrupt_count - 1:03d}",
                    )
                )
                continue
            carriage = self.pending.find(b"\r")
            if carriage < 0:
                if len(self.pending) > 65536:
                    raise FixtureProtocolError("unterminated fixture command exceeded 64 KiB")
                break
            command = bytes(self.pending[:carriage])
            del self.pending[: carriage + 1]
            if command.startswith(PROBE_PREFIX):
                sample_id = command[len(PROBE_PREFIX) :].decode("ascii")
                if not sample_id or len(sample_id) > 128:
                    raise FixtureProtocolError("invalid probe sample id")
                self.effect_counts[sample_id] += 1
                events.append(("probe", sample_id))
                continue
            if command == FLOOD_COMMAND:
                self.flood_requested = True
                events.append(("flood", "output-flood"))
                continue
            if command.startswith(FLOOD_PREFIX):
                sample_id = command[len(FLOOD_PREFIX) :].decode("ascii")
                if not sample_id or len(sample_id) > 128:
                    raise FixtureProtocolError("invalid flood sample id")
                self.flood_requested = True
                events.append(("flood", sample_id))
                continue
            if command.startswith(SECURE_PREFIX):
                sample_id = command[len(SECURE_PREFIX) :].decode("ascii")
                if not sample_id or len(sample_id) > 128:
                    raise FixtureProtocolError("invalid secure-input sample id")
                self.effect_counts[sample_id] += 1
                events.append(("secure", sample_id))
                continue
            if command.startswith(INTERRUPT_ARM_PREFIX):
                fields = command[len(INTERRUPT_ARM_PREFIX) :].decode("ascii").split(":")
                if len(fields) != 2 or any(
                    not field or len(field) > 128 for field in fields
                ):
                    raise FixtureProtocolError("invalid interrupt sample id")
                interrupt_sample, command_sample = fields
                self.next_interrupt_sample = interrupt_sample
                self.effect_counts[command_sample] += 1
                events.append(("arm-interrupt", command_sample))
                continue
            raise FixtureProtocolError("fixture received an unexpected command")
        return events


class JsonlRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def record(self, name: str, sample_id: str, **fields: object) -> None:
        value = {
            "name": name,
            "sampleId": sample_id,
            "atUnixNs": time.time_ns(),
            **fields,
        }
        encoded = json.dumps(value, separators=(",", ":"), sort_keys=True)
        with self.lock, self.path.open("a", encoding="utf-8") as output:
            output.write(encoded + "\n")


def _positive_integer(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value <= 0:
        raise FixtureProtocolError(f"{name} must be positive")
    return value


def run_fixture() -> None:
    capture_value = os.environ.get(INPUT_CAPTURE_ENV)
    event_value = os.environ.get(EVENT_LOG_ENV)
    if not capture_value or not event_value:
        raise FixtureProtocolError(f"{INPUT_CAPTURE_ENV} and {EVENT_LOG_ENV} are required")
    capture_path = Path(capture_value)
    recorder = JsonlRecorder(Path(event_value))
    state = FixtureState()
    output_lock = threading.Lock()
    flood_stop: threading.Event | None = None
    flood_thread: threading.Thread | None = None
    chunk_bytes = _positive_integer(FLOOD_CHUNK_BYTES_ENV, 4096)
    maximum_flood_bytes = _positive_integer(FLOOD_MAX_BYTES_ENV, 4 * 1024 * 1024)
    maximum_flood_duration_ms = _positive_integer(FLOOD_MAX_DURATION_MS_ENV, 5_000)

    def emit(payload: bytes, sample_id: str) -> None:
        with output_lock:
            os.write(sys.stdout.fileno(), payload)
        recorder.record("pty.output", sample_id, bytes=len(payload))

    def flood(stop: threading.Event, sample_id: str) -> None:
        emitted = 0
        sequence = 0
        deadline = time.monotonic() + maximum_flood_duration_ms / 1_000
        while (
            not stop.is_set()
            and emitted < maximum_flood_bytes
            and time.monotonic() < deadline
        ):
            prefix = f"E0-FLOOD-{sample_id}-{sequence:08d} ".encode("ascii")
            payload = (
                prefix + b"x" * max(1, chunk_bytes - len(prefix) - 2) + b"\r\n"
            )[:chunk_bytes]
            emit(payload, f"flood-{sample_id}")
            emitted += len(payload)
            sequence += 1
        recorder.record("fixture.flood-stopped", sample_id, bytes=emitted)

    def stop_flood() -> None:
        nonlocal flood_stop, flood_thread
        if flood_stop is not None:
            flood_stop.set()
        if flood_thread is not None:
            flood_thread.join(timeout=1)
        flood_stop = None
        flood_thread = None

    def start_flood(sample_id: str) -> None:
        nonlocal flood_stop, flood_thread
        stop_flood()
        flood_stop = threading.Event()
        flood_thread = threading.Thread(
            target=flood, args=(flood_stop, sample_id), daemon=True
        )
        flood_thread.start()

    tty.setraw(sys.stdin.fileno())
    emit(f"\x1b[2J\x1b[H{READY_SENTINEL}\r\n".encode("ascii"), "ready")
    with capture_path.open("ab", buffering=0) as capture:
        while True:
            payload = os.read(sys.stdin.fileno(), 4096)
            if not payload:
                stop_flood()
                return
            capture.write(payload)
            for kind, sample_id in state.accept(payload):
                recorder.record("host.pty-write", sample_id, kind=kind)
                if kind == "probe":
                    emit(f"{RESULT_PREFIX}{sample_id}\r\n".encode("ascii"), sample_id)
                elif kind == "secure":
                    emit(f"{RESULT_PREFIX}{sample_id}\r\n".encode("ascii"), sample_id)
                elif kind == "flood":
                    start_flood(sample_id)
                elif kind == "arm-interrupt":
                    pass
                elif kind == "interrupt":
                    stop_flood()
                    emit(f"{INTERRUPT_PREFIX}{sample_id}\r\n".encode("ascii"), sample_id)
                    emit(f"{QUIET_PREFIX}{sample_id}\r\n".encode("ascii"), sample_id)


if __name__ == "__main__":
    run_fixture()
