// Keyboard flows of Pengaturan (apply-markas-pengaturan D20, D24) on a running app at 1440 and 390, in the account's
// language. A fetch recorder logs the method, path and body of every write; the flows send only the requests the
// section sent before (language, profile, budget, pairing code, sign-out) and the expected bodies are checked.
// Writes pengaturan-keys.json. Run it on a disposable owner: flow 3 switches the language and back, flow 2 signs out.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node pengaturan-keys.mjs --origin <url> --out <dir> [--port 9792] [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 rail: Enter on a section link opens it and moves focus to the section title",
  "2 Keluar: the last rail item sends POST /api/auth/logout once and opens the sign-in page",
  "3 Bahasa: an arrow key chooses the other language (PATCH {language}), focus stays on the segment; back again",
  "4 Tema: arrow keys move and choose in the radio group; no request",
  "5 Pintasan satu tombol: Space toggles aria-checked; Space again restores; no request",
  "6 Profil: Enter in a field saves {name, name_aliases} only",
  "7 Mata uang & anggaran: 40 months is refused with an alert and no request; a valid save sends the four fields",
  "8 Pemetaan: Enter on a trash button asks in the row with focus on Batal; Esc cancels and returns focus; no request",
  "9 Hubungkan ponsel: Enter opens the dialog with focus inside, Tab stays inside, Esc closes and focus returns; one pairing-code request; Cabut opens its dialog and Esc returns focus to Cabut",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9792), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const SPACE = [" ", "Space", 32, 0, " "];
const LEFT = ["ArrowLeft", "ArrowLeft", 37];
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const key = (spec, modifiers = 0) => browser.key(spec[0], spec[1], spec[2], modifiers || spec[3] || 0, spec[4]);
const active = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e || e === document.body) return null; const r = e.getBoundingClientRect(); return { tag: e.tagName, label: (e.getAttribute("aria-label") || e.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 70), inDialog: Boolean(e.closest("dialog[open]")), top: Math.round(r.top), bottom: Math.round(r.bottom), checked: e.getAttribute("aria-checked") }; })()`);
const dialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].filter((d) => d.getBoundingClientRect().width > 0).map((d) => (d.querySelector("h2")?.textContent ?? "").trim())`);
const focusOn = (expression) => browser.evaluate(`(() => { const e = ${expression}; if (!e) return false; e.scrollIntoView({ block: "center" }); e.focus(); return document.activeElement === e; })()`);
const byText = (selector, pattern) => `[...document.querySelectorAll(${JSON.stringify(selector)})].find((e) => e.getBoundingClientRect().width > 0 && new RegExp(${JSON.stringify(pattern)}).test((e.getAttribute("aria-label") || e.textContent || "").replace(/\\s+/g, " ").trim()))`;
// Every write's method, path and JSON body; reads are left out.
const recordWrites = () => browser.evaluate(`(() => { if (window.__writes) return; window.__writes = []; const original = window.fetch; window.fetch = (input, init) => { const method = (init?.method ?? "GET").toUpperCase(); if (method !== "GET") { let body = init?.body ?? null; try { body = body ? JSON.parse(body) : null; } catch {} window.__writes.push({ method, path: String(input).replace(location.origin, ""), body }); } return original(input, init); }; })()`);
const writes = () => browser.evaluate("window.__writes ?? []");
const requests = [];
async function open(target) {
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(1000);
  await recordWrites();
}
async function login() {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
}
const keep = async (flow, cell) => {
  for (const write of await writes()) requests.push({ flow, cell, ...write });
};

try {
  await login();
  const me = await browser.evaluate(`fetch("/api/auth/me", { credentials: "include" }).then((r) => r.json())`);
  const language = me.user.language;
  const other = language === "en" ? "id" : "en";
  for (const [width, height, mobile] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}-${language}`;
    await browser.setCell({ width, height, theme: "light", mobile });
    await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("hideBalances")`);

    // 1
    await open("/settings/perangkat");
    await focusOn(byText(".set-rail a", "^(Kebiasaan|Habits)$"));
    await key(ENTER);
    await sleep(1500);
    const url1 = await browser.evaluate("location.pathname");
    const focus1 = await active();
    record(1, "Enter opens the section and focuses its title", cell, url1 === "/settings/kebiasaan" && focus1?.tag === "H2" && /Kebiasaan|Habits/.test(focus1.label), `${url1} ${JSON.stringify(focus1)}`);
    if (mobile) record(1, "the title is in view below the 56 px header", cell, focus1 && focus1.top >= 56 && focus1.bottom <= 844 - 64, `top ${focus1?.top}`);

    // 3
    await open("/settings/tampilan");
    await focusOn(`document.querySelector('[role=radiogroup] [role=radio][aria-checked=true][lang]')`);
    await key(language === "en" ? RIGHT : LEFT);
    await sleep(2500);
    const after3 = await active();
    const writes3 = await writes();
    await keep(3, cell);
    record(3, "one PATCH /api/auth/settings {language}", cell, writes3.length === 1 && writes3[0].path === "/api/auth/settings" && JSON.stringify(writes3[0].body) === JSON.stringify({ language: other }), JSON.stringify(writes3));
    record(3, "focus stays on the chosen language", cell, after3?.checked === "true" && after3.label === (other === "en" ? "English" : "Bahasa Indonesia"), JSON.stringify(after3));
    await open("/settings/tampilan");
    await focusOn(`document.querySelector('[role=radiogroup] [role=radio][aria-checked=true][lang]')`);
    await key(language === "en" ? LEFT : RIGHT);
    await sleep(2500);
    const back3 = await writes();
    await keep(3, cell);
    record(3, "switching back sends the account's language", cell, back3.length === 1 && back3[0].body?.language === language, JSON.stringify(back3));

    // 4
    await open("/settings/tampilan");
    await focusOn(`[...document.querySelectorAll('[role=radiogroup]')][1].querySelector('[aria-checked=true]')`);
    const before4 = await active();
    await key(RIGHT);
    await sleep(500);
    const after4 = await active();
    const theme4 = await browser.evaluate(`localStorage.getItem("theme")`);
    record(4, "an arrow key moves and chooses the next theme", cell, after4?.checked === "true" && after4.label !== before4?.label && theme4 !== "light", `${before4?.label} → ${after4?.label} (${theme4})`);
    await key(LEFT);
    await sleep(500);
    record(4, "no request", cell, (await writes()).length === 0, JSON.stringify(await writes()));

    // 5
    await focusOn(`document.querySelector('[role=switch]')`);
    const before5 = (await active())?.checked;
    await key(SPACE);
    await sleep(400);
    const after5 = (await active())?.checked;
    await key(SPACE);
    await sleep(400);
    const back5 = (await active())?.checked;
    record(5, "Space toggles the switch and back", cell, before5 !== after5 && back5 === before5, `${before5} → ${after5} → ${back5}`);
    record(5, "no request", cell, (await writes()).length === 0, "");

    // 6
    await open("/settings/profil");
    await focusOn(`document.getElementById("settings-display-name")`);
    await key(ENTER);
    await sleep(1500);
    const writes6 = await writes();
    await keep(6, cell);
    record(6, "one PATCH with name and name_aliases only", cell, writes6.length === 1 && JSON.stringify(Object.keys(writes6[0].body ?? {})) === JSON.stringify(["name", "name_aliases"]), JSON.stringify(writes6));

    // 7
    await open("/settings/anggaran");
    await browser.evaluate(`(() => { const input = document.getElementById("settings-emergency-multiplier"); const set = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set; set.call(input, "40"); input.dispatchEvent(new Event("input", { bubbles: true })); })()`);
    await focusOn(`document.getElementById("settings-emergency-multiplier")`);
    await key(ENTER);
    await sleep(800);
    const alert7 = await browser.evaluate(`[...document.querySelectorAll("[role=alert]")].map((e) => e.textContent.trim()).join(" | ")`);
    record(7, "40 months is refused with an alert and no request", cell, /36/.test(alert7) && (await writes()).length === 0, alert7);
    await open("/settings/anggaran");
    await focusOn(`document.getElementById("settings-emergency-multiplier")`);
    await key(ENTER);
    await sleep(1500);
    const writes7 = await writes();
    await keep(7, cell);
    record(7, "a valid save sends currency, region, months and the limit", cell, writes7.length === 1 && JSON.stringify(Object.keys(writes7[0].body ?? {}).sort()) === JSON.stringify(["currency", "emergency_fund_multiplier", "monthly_spending_budget", "region"]), JSON.stringify(writes7));

    // 8
    await open("/settings/pemetaan");
    await focusOn(`document.querySelector(".set-list .ws-icon-btn")`);
    const trigger8 = (await active())?.label;
    await key(ENTER);
    await sleep(400);
    const cancel8 = await active();
    await key(ESC);
    await sleep(400);
    const back8 = await active();
    record(8, "Enter asks in the row with focus on Batal", cell, /^(Batal|Cancel)$/.test(cancel8?.label ?? ""), JSON.stringify(cancel8));
    record(8, "Esc cancels and returns focus to the trash button; no request", cell, back8?.label === trigger8 && (await writes()).length === 0, `${trigger8} / ${back8?.label}`);

    // 9
    await open("/settings/perangkat");
    await focusOn(byText("button", "^(Hubungkan ponsel|Connect a phone)$"));
    await key(ENTER);
    await sleep(2500);
    const open9 = await dialogs();
    const inside = [];
    for (let index = 0; index < 14; index += 1) {
      await key(TAB);
      await sleep(80);
      inside.push((await active())?.inDialog ?? false);
    }
    record(9, "the pairing dialog opens with focus inside and Tab stays inside", cell, open9.length === 1 && inside.every(Boolean), `${JSON.stringify(open9)} ${inside.filter(Boolean).length}/14`);
    await key(ESC);
    await sleep(700);
    const back9 = await active();
    record(9, "Esc closes it and focus returns to Hubungkan ponsel", cell, (await dialogs()).length === 0 && /^(Hubungkan ponsel|Connect a phone)$/.test(back9?.label ?? ""), JSON.stringify(back9));
    const writes9 = await writes();
    await keep(9, cell);
    record(9, "one POST /api/devices/pairing-codes", cell, writes9.length === 1 && writes9[0].method === "POST" && writes9[0].path === "/api/devices/pairing-codes", JSON.stringify(writes9));
    await recordWrites();
    await browser.evaluate("window.__writes.length = 0");
    await focusOn(byText("button", "^(Cabut|Revoke) Ponsel Cadangan$"));
    await key(ENTER);
    await sleep(700);
    const revoke9 = await dialogs();
    await key(ESC);
    await sleep(500);
    const back9b = await active();
    record(9, "Cabut opens its dialog; Esc returns focus to Cabut; no DELETE", cell, revoke9.length === 1 && /Ponsel Cadangan/.test(back9b?.label ?? "") && (await writes()).length === 0, `${JSON.stringify(revoke9)} ${back9b?.label}`);

    // 2 (last: it signs out)
    await open("/settings/tampilan");
    const last = await browser.evaluate(`(() => { const items = [...document.querySelectorAll(".set-rail .rail-item")]; return (items.at(-1)?.textContent ?? "").trim(); })()`);
    await focusOn(`[...document.querySelectorAll(".set-rail .rail-item")].at(-1)`);
    await key(ENTER);
    await sleep(2000);
    const writes2 = await writes().catch(() => []);
    const url2 = await browser.evaluate("location.pathname");
    record(2, "Keluar is the last rail item and signs out", cell, /^(Keluar|Sign out)$/.test(last) && url2 === "/auth/login", `${last} → ${url2} ${JSON.stringify(writes2)}`);
    requests.push({ flow: 2, cell, method: "POST", path: url2 === "/auth/login" ? "/api/auth/logout (page left)" : "none" });
    await login();
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "pengaturan-keys.json"), JSON.stringify({ origin: args.origin, at: new Date().toISOString(), pass: results.filter((r) => r.pass).length, fail: results.filter((r) => !r.pass).length, results, requests }, null, 1));
console.log(JSON.stringify({ pass: results.filter((r) => r.pass).length, fail: results.filter((r) => !r.pass).length }));
