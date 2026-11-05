// A small Chrome DevTools Protocol driver for the Markas+ verification scripts. It starts a local
// Chromium-based browser headless on loopback, so the checks need no npm dependency beyond axe-core,
// which the frontend already has through vitest-axe.
//
// Environment:
//   MARKAS_BROWSER  path to a Chromium-based browser (default: Brave, then Google Chrome on macOS)
//   MARKAS_AXE      path to axe.min.js (default: frontend/node_modules/axe-core/axe.min.js)
import { spawn } from "node:child_process";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "../../..");

const BROWSERS = [
  "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
];

export const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function browserPath() {
  if (process.env.MARKAS_BROWSER) return process.env.MARKAS_BROWSER;
  const found = BROWSERS.find((candidate) => existsSync(candidate));
  if (!found) throw new Error("No Chromium-based browser found; set MARKAS_BROWSER");
  return found;
}

async function fetchJson(url, init) {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    try {
      return await (await fetch(url, init)).json();
    } catch {
      await sleep(250);
    }
  }
  throw new Error(`No DevTools endpoint at ${url}`);
}

export async function launch({ port, profileDir }) {
  rmSync(profileDir, { recursive: true, force: true });
  const browser = spawn(
    browserPath(),
    [
      "--headless=new",
      `--remote-debugging-port=${port}`,
      "--remote-debugging-address=127.0.0.1",
      `--user-data-dir=${profileDir}`,
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-extensions",
      "--hide-scrollbars",
      "about:blank",
    ],
    { stdio: "ignore" },
  );
  const target = await fetchJson(`http://127.0.0.1:${port}/json/new?about:blank`, { method: "PUT" });
  const socket = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve) => socket.addEventListener("open", resolve, { once: true }));

  let nextId = 1;
  const pending = new Map();
  const consoleErrors = [];
  const inflight = new Map();
  let lastNetwork = Date.now();

  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) reject(new Error(JSON.stringify(message.error)));
      else resolve(message.result);
      return;
    }
    const { method, params } = message;
    if (method === "Runtime.exceptionThrown") {
      const details = params.exceptionDetails;
      consoleErrors.push(`exception: ${details.exception?.description ?? details.text}`.slice(0, 300));
    } else if (method === "Runtime.consoleAPICalled" && params.type === "error") {
      const text = params.args.map((arg) => arg.value ?? arg.description).join(" ");
      consoleErrors.push(`console.error: ${text}`.slice(0, 300));
    } else if (method === "Log.entryAdded" && params.entry.level === "error") {
      consoleErrors.push(`log: ${params.entry.text} ${params.entry.url ?? ""}`.slice(0, 300));
    } else if (method === "Network.requestWillBeSent" && ["Fetch", "XHR", "Document"].includes(params.type)) {
      inflight.set(params.requestId, params.request.url);
      lastNetwork = Date.now();
    } else if (method === "Network.loadingFinished" || method === "Network.loadingFailed") {
      if (inflight.delete(params.requestId)) lastNetwork = Date.now();
    }
  });

  const send = (method, params = {}) =>
    new Promise((resolve, reject) => {
      const id = nextId;
      nextId += 1;
      pending.set(id, { resolve, reject });
      socket.send(JSON.stringify({ id, method, params }));
    });

  const evaluate = async (expression) => {
    const result = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (result.exceptionDetails) {
      throw new Error(result.exceptionDetails.exception?.description ?? "evaluate failed");
    }
    return result.result.value;
  };

  const waitFor = async (expression, timeout = 20000) => {
    const started = Date.now();
    while (Date.now() - started < timeout) {
      if (await evaluate(expression).catch(() => false)) return true;
      await sleep(120);
    }
    return false;
  };

  const networkIdle = async (quiet = 700, timeout = 20000) => {
    const started = Date.now();
    while (Date.now() - started < timeout) {
      if (inflight.size === 0 && Date.now() - lastNetwork >= quiet) return true;
      await sleep(100);
    }
    return false;
  };

  await send("Page.enable");
  await send("Runtime.enable");
  await send("Log.enable");
  await send("Network.enable");

  const navigate = async (url) => {
    inflight.clear();
    lastNetwork = Date.now();
    await send("Page.navigate", { url });
    await waitFor("document.readyState === 'complete'");
  };

  // Waits for the document, the network and any skeleton or busy region to finish.
  const settle = async () => {
    await waitFor("document.readyState === 'complete'");
    await networkIdle();
    await waitFor(
      "![...document.querySelectorAll('[aria-busy=\"true\"], .ws-skeleton, .animate-pulse')]" +
        ".some((element) => element.getBoundingClientRect().width > 16)",
      10000,
    );
    await waitFor("document.fonts.status === 'loaded'", 5000);
    await sleep(500);
  };

  const setCell = async ({ width, height, mobile, theme, reducedMotion = false }) => {
    await send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: Boolean(mobile) });
    await send("Emulation.setTouchEmulationEnabled", { enabled: Boolean(mobile), maxTouchPoints: mobile ? 5 : 1 });
    await send("Emulation.setEmulatedMedia", {
      features: [
        { name: "prefers-color-scheme", value: theme === "dark" ? "dark" : "light" },
        { name: "prefers-reduced-motion", value: reducedMotion ? "reduce" : "no-preference" },
      ],
    });
  };

  const shot = async (file, { full = false, clip } = {}) => {
    const params = { format: "png", captureBeyondViewport: full };
    if (clip) params.clip = { ...clip, scale: 1 };
    const result = await send("Page.captureScreenshot", params);
    writeFileSync(file, Buffer.from(result.data, "base64"));
  };

  const key = async (name, code, keyCode, modifiers = 0, text) => {
    let extra = {};
    if (text) extra = { text, unmodifiedText: text };
    else if (name === "Enter") extra = { text: "\r", unmodifiedText: "\r" };
    await send("Input.dispatchKeyEvent", { type: "keyDown", key: name, code, windowsVirtualKeyCode: keyCode, modifiers, ...extra });
    await send("Input.dispatchKeyEvent", { type: "keyUp", key: name, code, windowsVirtualKeyCode: keyCode, modifiers });
  };

  const axePath = process.env.MARKAS_AXE ?? path.join(repoRoot, "frontend/node_modules/axe-core/axe.min.js");
  const axeSource = readFileSync(axePath, "utf8");
  const axe = async () => {
    await evaluate(axeSource);
    return evaluate(`axe.run(document, {
      runOnly: { type: "tag", values: ${JSON.stringify(AXE_TAGS)} },
      resultTypes: ["violations"],
    }).then((result) => result.violations.map((violation) => ({
      id: violation.id,
      impact: violation.impact,
      nodes: violation.nodes.length,
      targets: violation.nodes.slice(0, 4).map((node) => node.target.join(" ")),
    })))`);
  };

  const close = () => {
    try {
      socket.close();
    } catch {
      // The socket may already be closed when the browser exits first.
    }
    browser.kill();
  };

  return { send, evaluate, waitFor, networkIdle, navigate, settle, setCell, shot, key, axe, close, consoleErrors };
}
