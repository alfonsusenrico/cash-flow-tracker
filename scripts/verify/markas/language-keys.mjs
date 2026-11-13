// Keyboard flows in English (add-language-system task 6.4) on a running app signed in as an English account: the
// skip link, the tab order, Rincian open and close with focus return, and the Language radio group with the
// arrow keys, which switches the interface in place and keeps focus on the chosen option. Writes language-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node language-keys.mjs --origin http://127.0.0.1:3522 --out <dir> [--port 9711]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9711), profileDir: path.join(args.out, ".profile-language-keys") });
const results = [];
const record = (check, pass, detail) => {
  results.push({ check, pass, detail });
  console.log(pass ? "PASS" : "FAIL", check, detail ?? "");
};
const active = () =>
  browser.evaluate(`(() => { const a = document.activeElement; return a ? (a.getAttribute("aria-label") || a.id || (a.labels && a.labels[0] ? a.labels[0].textContent.trim() : "") || a.textContent.trim().slice(0, 40)) : null; })()`);
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const LEFT = ["ArrowLeft", "ArrowLeft", 37];
const lang = () => browser.evaluate("document.documentElement.lang");

async function open(url) {
  await browser.navigate(args.origin + url);
  await browser.settle();
  await sleep(500);
}

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "contoh-sandi-123" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  await browser.setCell({ width: 1440, height: 900, theme: "light" });

  await open("/");
  record("the page language is English", (await lang()) === "en", await lang());
  await browser.key(...TAB);
  const skip = await active();
  await browser.key(...ENTER);
  await sleep(200);
  record("skip link is the first stop, named in English, and moves focus to main", skip === "Skip to content" && (await active()) === "main", `${skip} → ${await active()}`);

  const order = [];
  for (let index = 0; index < 14; index += 1) {
    await browser.key(...TAB);
    order.push(await active());
  }
  const expectedStart = ["Safe to spend breakdown", "Open Review · 4"];
  record("tab order after the skip link follows the reading order", expectedStart.every((name, index) => order[index] === name), order.join(" › "));

  await open("/");
  await browser.evaluate(`[...document.querySelectorAll("button")].find((b) => b.getAttribute("aria-label") === "Safe to spend breakdown").focus()`);
  await browser.key(...ENTER);
  await sleep(600);
  const dialog = await browser.evaluate(`(() => { const d = document.querySelector("dialog[open]"); return d ? d.querySelector("h2").textContent + " | focus inside: " + d.contains(document.activeElement) : null; })()`);
  record("Breakdown opens the dialog with focus inside", /^Safe to spend \| focus inside: true$/.test(dialog ?? ""), dialog);
  await browser.key(...ESC);
  await sleep(500);
  record("Escape closes the Breakdown and returns focus to its button", (await active()) === "Safe to spend breakdown", await active());

  await open("/settings/tampilan");
  await browser.evaluate(`document.querySelector("input[name=interface-language]:checked").focus()`);
  const before = await active();
  await browser.key(...RIGHT);
  await sleep(2500);
  const afterSwitch = { lang: await lang(), focus: await active(), url: await browser.evaluate("location.pathname + location.search"), sidebar: await browser.evaluate(`document.querySelector(".sb-nav .nav-item .lbl")?.textContent`) };
  const saved = await browser.evaluate(`fetch("/api/auth/me", { credentials: "include" }).then((r) => r.json()).then((j) => j.user.language)`);
  record(
    "→ in the Language group switches to Bahasa Indonesia in place, saved, focus kept",
    before === "English" && afterSwitch.lang === "id" && afterSwitch.focus === "Bahasa Indonesia" && afterSwitch.url === "/settings/tampilan" && afterSwitch.sidebar === "Beranda" && saved === "id",
    JSON.stringify({ before, ...afterSwitch, saved }),
  );
  await browser.key(...LEFT);
  await sleep(2500);
  const back = { lang: await lang(), focus: await active(), sidebar: await browser.evaluate(`document.querySelector(".sb-nav .nav-item .lbl")?.textContent`) };
  const savedBack = await browser.evaluate(`fetch("/api/auth/me", { credentials: "include" }).then((r) => r.json()).then((j) => j.user.language)`);
  record("← switches back to English the same way", back.lang === "en" && back.focus === "English" && back.sidebar === "Home" && savedBack === "en", JSON.stringify({ ...back, savedBack }));
  const cookie = await browser.evaluate("document.cookie.split('; ').find((part) => part.startsWith('cft_language='))");
  record("the browser remembers the account's language", cookie === "cft_language=en", cookie);
  // The cookie's stored attributes as the browser keeps them; Secure is only set over HTTPS (D2).
  const { cookies } = await browser.send("Network.getAllCookies");
  const stored = cookies.find((item) => item.name === "cft_language");
  const attributes = stored && { path: stored.path, sameSite: stored.sameSite, secure: stored.secure, httpOnly: stored.httpOnly, days: Math.round((stored.expires * 1000 - Date.now()) / 86_400_000) };
  record(
    "cft_language is Path=/, SameSite=Lax, a year long, readable by the page and not Secure over plain-HTTP loopback",
    Boolean(attributes) && attributes.path === "/" && attributes.sameSite === "Lax" && attributes.httpOnly === false && attributes.secure === false && attributes.days >= 364,
    JSON.stringify(attributes),
  );
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "language-keys.json"), JSON.stringify({ results, passed: results.filter((item) => item.pass).length, failed: results.filter((item) => !item.pass).length }, null, 1));
