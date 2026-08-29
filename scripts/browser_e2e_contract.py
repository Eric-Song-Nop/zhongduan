#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path
import sys
import tty
from typing import Literal


READY_SENTINEL = "ZHONGDUAN_E2E_READY"
PROBE_INPUT = b"zhongduan-e2e-probe\r"
RESULT_SENTINEL = "ZHONGDUAN_E2E_RESULT"
INTERRUPT_INPUT = b"\x03"
INTERRUPT_SENTINEL = "ZHONGDUAN_E2E_INTERRUPT"
INPUT_CAPTURE_ENV = "ZHONGDUAN_E2E_INPUT_CAPTURE"


class BrowserRecoverySmokeContractError(ValueError):
    pass


class SyntheticPtyState:
    """Deterministic byte protocol used by the real Host child process."""

    def __init__(self) -> None:
        self._started = False
        self._pending = bytearray()
        self.probe_input_count = 0
        self.result_count = 0
        self.interrupt_count = 0

    @property
    def phase(
        self,
    ) -> Literal["created", "awaiting-interrupt", "awaiting-probe", "complete"]:
        if not self._started:
            return "created"
        if self.interrupt_count == 0:
            return "awaiting-interrupt"
        if self.result_count == 0:
            return "awaiting-probe"
        return "complete"

    def start(self) -> bytes:
        if self._started:
            raise BrowserRecoverySmokeContractError(
                "synthetic PTY was started more than once"
            )
        self._started = True
        return f"\x1b[2J\x1b[H{READY_SENTINEL}\r\n".encode("ascii")

    def accept(self, payload: bytes) -> tuple[bytes, ...]:
        if not self._started:
            raise BrowserRecoverySmokeContractError(
                "synthetic PTY received input before ready"
            )
        if not isinstance(payload, bytes):
            raise BrowserRecoverySmokeContractError("synthetic PTY input must be bytes")

        self._pending.extend(payload)
        output: list[bytes] = []
        while self._pending:
            pending = bytes(self._pending)
            if pending.startswith(PROBE_INPUT):
                del self._pending[: len(PROBE_INPUT)]
                self.probe_input_count += 1
                if self.interrupt_count != 1:
                    raise BrowserRecoverySmokeContractError(
                        "fixed probe arrived before exactly one cold-restore interrupt"
                    )
                if self.result_count == 0:
                    self.result_count = 1
                    output.append(f"{RESULT_SENTINEL}\r\n".encode("ascii"))
                continue
            if PROBE_INPUT.startswith(pending):
                break
            if pending.startswith(INTERRUPT_INPUT):
                del self._pending[: len(INTERRUPT_INPUT)]
                self.interrupt_count += 1
                if self.interrupt_count == 1:
                    output.append(f"{INTERRUPT_SENTINEL}\r\n".encode("ascii"))
                continue
            raise BrowserRecoverySmokeContractError(
                "synthetic PTY received unexpected input bytes"
            )
        return tuple(output)


def validate_synthetic_capture(capture: bytes) -> None:
    expected = INTERRUPT_INPUT + PROBE_INPUT
    if capture != expected:
        raise BrowserRecoverySmokeContractError(
            "PTY capture must contain one cold-restore interrupt followed by one fixed probe"
        )


def run_synthetic_pty() -> None:
    capture_value = os.environ.get(INPUT_CAPTURE_ENV)
    if not capture_value:
        raise BrowserRecoverySmokeContractError(f"{INPUT_CAPTURE_ENV} is required")
    capture_path = Path(capture_value)
    state = SyntheticPtyState()
    tty.setraw(sys.stdin.fileno())
    os.write(sys.stdout.fileno(), state.start())
    with capture_path.open("ab", buffering=0) as capture:
        while True:
            payload = os.read(sys.stdin.fileno(), 1024)
            if not payload:
                return
            capture.write(payload)
            for output in state.accept(payload):
                os.write(sys.stdout.fileno(), output)


if __name__ == "__main__":
    run_synthetic_pty()
