import { createRoot } from "react-dom/client";
import "@wterm/dom/css";

import { consumeBrowserCapability, type BrowserCapabilityBootstrap } from "./capability";
import { TerminalApp } from "./terminal-app";
import "./styles.css";

let capability: BrowserCapabilityBootstrap | undefined;
let capabilityError = false;
try {
  capability = consumeBrowserCapability(window.location, window.history, window.sessionStorage);
} catch {
  capabilityError = true;
}

const root = document.getElementById("root");
if (!root) throw new Error("root element not found");

createRoot(root).render(
  <TerminalApp
    {...(capability === undefined ? {} : { capability })}
    capabilityError={capabilityError}
  />,
);
