import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

function App() {
  return <main className="terminal-surface" aria-label="Remote terminal" />;
}

const root = document.querySelector("#root");
if (!(root instanceof HTMLElement)) {
  throw new Error("missing application root");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
