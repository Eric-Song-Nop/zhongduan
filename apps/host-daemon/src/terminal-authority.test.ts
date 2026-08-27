import { describe, expect, it } from "vitest";

import { validateSemanticMouse, type SemanticMouse } from "./terminal-authority";

const geometry = { widthPx: 800, heightPx: 480 };
const press: SemanticMouse = {
  action: "press",
  altGraph: false,
  button: 0,
  buttons: 1,
  modifiers: 0,
  surface: { x: 10, y: 20 },
};

describe("validateSemanticMouse", () => {
  it("accepts every action-specific valid shape", () => {
    expect(() => validateSemanticMouse(press, geometry)).not.toThrow();
    expect(() =>
      validateSemanticMouse({ ...press, action: "release", buttons: 0 }, geometry),
    ).not.toThrow();
    expect(() =>
      validateSemanticMouse({ ...press, action: "move", button: null }, geometry),
    ).not.toThrow();
    expect(() =>
      validateSemanticMouse(
        {
          ...press,
          action: "wheel",
          button: null,
          buttons: 0,
          deltaMode: "line",
          deltaY: -1,
        },
        geometry,
      ),
    ).not.toThrow();
  });

  it.each([
    ["missing geometry", press, { widthPx: 0, heightPx: 480 }],
    ["button", { ...press, button: 5 }, geometry],
    ["buttons", { ...press, buttons: 32 }, geometry],
    ["modifier bits", { ...press, modifiers: 0x40 }, geometry],
    ["surface integer", { ...press, surface: { x: 0.5, y: 0 } }, geometry],
    ["surface bound", { ...press, surface: { x: 1_000_001, y: 0 } }, geometry],
    ["move button", { ...press, action: "move", button: 0 }, geometry],
    ["non-wheel delta", { ...press, deltaY: 1 }, geometry],
    [
      "wheel mode",
      { ...press, action: "wheel", button: null, deltaMode: "invalid", deltaY: 1 },
      geometry,
    ],
    [
      "wheel finite delta",
      { ...press, action: "wheel", button: null, deltaMode: "pixel", deltaY: Number.NaN },
      geometry,
    ],
    [
      "wheel delta bound",
      { ...press, action: "wheel", button: null, deltaMode: "page", deltaY: 1_000_001 },
      geometry,
    ],
    [
      "wheel nonzero delta",
      { ...press, action: "wheel", button: null, deltaMode: "line", deltaX: 0, deltaY: 0 },
      geometry,
    ],
  ])("rejects invalid %s before encoding", (_name, mouse, dimensions) => {
    expect(() =>
      validateSemanticMouse(mouse as SemanticMouse, dimensions as typeof geometry),
    ).toThrow();
  });
});
