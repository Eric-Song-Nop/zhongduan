#!/usr/bin/env python3

import unittest

from scripts.browser_e2e_contract import (
    BrowserRecoverySmokeContractError,
    INTERRUPT_INPUT,
    INTERRUPT_SENTINEL,
    PROBE_INPUT,
    READY_SENTINEL,
    RESULT_SENTINEL,
    SyntheticPtyState,
    validate_synthetic_capture,
)


class SyntheticPtyStateTest(unittest.TestCase):
    def test_emits_ready_interrupt_and_result_in_deterministic_order(self) -> None:
        state = SyntheticPtyState()
        self.assertIn(READY_SENTINEL.encode("ascii"), state.start())
        self.assertEqual(state.phase, "awaiting-interrupt")

        interrupt = state.accept(INTERRUPT_INPUT)
        self.assertEqual(interrupt, (f"{INTERRUPT_SENTINEL}\r\n".encode("ascii"),))
        self.assertEqual(state.phase, "awaiting-probe")

        split = len(PROBE_INPUT) // 2
        self.assertEqual(state.accept(PROBE_INPUT[:split]), ())
        result = state.accept(PROBE_INPUT[split:])
        self.assertEqual(result, (f"{RESULT_SENTINEL}\r\n".encode("ascii"),))
        self.assertEqual(state.phase, "complete")
        validate_synthetic_capture(INTERRUPT_INPUT + PROBE_INPUT)

    def test_duplicate_probe_never_emits_a_second_result_and_fails_capture_validation(
        self,
    ) -> None:
        state = SyntheticPtyState()
        state.start()
        interrupt_result = state.accept(INTERRUPT_INPUT)
        first_result = state.accept(PROBE_INPUT)
        duplicate_result = state.accept(PROBE_INPUT)

        self.assertEqual(len(interrupt_result), 1)
        self.assertEqual(len(first_result), 1)
        self.assertEqual(duplicate_result, ())
        self.assertEqual(state.probe_input_count, 2)
        self.assertEqual(state.result_count, 1)
        with self.assertRaisesRegex(
            BrowserRecoverySmokeContractError, "one cold-restore interrupt"
        ):
            validate_synthetic_capture(INTERRUPT_INPUT + PROBE_INPUT + PROBE_INPUT)

    def test_rejects_probe_before_interrupt_and_unexpected_input(self) -> None:
        state = SyntheticPtyState()
        state.start()
        with self.assertRaisesRegex(
            BrowserRecoverySmokeContractError, "before exactly one"
        ):
            state.accept(PROBE_INPUT)

        other = SyntheticPtyState()
        other.start()
        with self.assertRaisesRegex(
            BrowserRecoverySmokeContractError, "unexpected input"
        ):
            other.accept(b"not-the-fixed-probe")


if __name__ == "__main__":
    unittest.main()
