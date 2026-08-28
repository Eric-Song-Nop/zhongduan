import { describe, expect, it, vi } from "vitest";

import { runRecoveryAction } from "./terminal-app";

describe("terminal recovery action", () => {
  it("reloads compatible assets after an engine mismatch without reconnecting", () => {
    const reconnect = vi.fn();
    const reload = vi.fn();

    runRecoveryAction(true, reconnect, reload);

    expect(reconnect).not.toHaveBeenCalled();
    expect(reload).toHaveBeenCalledOnce();
  });

  it("uses the live session reconnect path for recoverable failures", () => {
    const reconnect = vi.fn();
    const reload = vi.fn();

    runRecoveryAction(false, reconnect, reload);

    expect(reconnect).toHaveBeenCalledOnce();
    expect(reload).not.toHaveBeenCalled();
  });
});
