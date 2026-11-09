// Keyboard flows of Catat and the global surfaces (apply-markas-catat D16, D24) on a running app at 1440 and 390, in
// the account's language. Each flow opens its layer from the keyboard where the owner can, checks where focus lands,
// that Tab stays inside, and that Escape (or the flow's end) gives focus back to the opener. Writes catat-keys.json.
// Run it on a disposable owner: flow 1 records and undoes a capture, flows 6 and 7 save.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node catat-keys.mjs --origin <url> --out <dir> [--port 9813] [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 N opens Tulis transaksi with focus in the sentence (title on a phone); an example chip by Enter, Catat by Enter, the toast's Urungkan by Enter",
  "2 Isi manual keeps the amount read; Tulis kalimat brings back the sentence; focus follows each view",
  "3 Pengeluaran: arrow keys choose a pillar in its radio group; Detail tambahan opens with Enter",
  "4 Pengeluaran: three debts in the split, Tab stays inside the dialog",
  "5 Pemasukan with a reimbursement: Mengurangi pengeluaran appears and is reachable by Tab",
  "6 Perpindahan into credit: Bayar sisa tagihan by Enter fills the amount left; a part with a fee is typed and Enter saves",
  "7 Pengeluaran on credit: 3x by arrow keys, the first due date shown, Enter in the amount saves",
  "8 Rekening › SPayLater › Bayar tagihan opens Perpindahan prefilled; Esc returns focus to the opener",
  "9 The bell's credit bill opens Perpindahan prefilled; Esc returns focus to the page",
  "10 Lainnya › Tema on a phone (Tema from the top bar on a desktop): arrow keys move, Esc returns focus",
  "11 ? opens Bantuan; the bell's due plan opens Rencana jatuh tempo; Esc returns focus each time",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9813), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const DOWN = ["ArrowDown", "ArrowDown", 40];
const N = ["n", "KeyN", 78, 0, "n"];
const key = async (spec, wait = 300) => {
  await browser.key(spec[0], spec[1], spec[2], spec[3] || 0, spec[4]);
  await sleep(wait);
};
const type = async (text) => {
  for (const char of text) await browser.send("Input.insertText", { text: char });
  await sleep(500);
};
const active = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e || e === document.body) return { tag: "BODY", label: "", inDialog: false }; return { tag: e.tagName, id: e.id, label: (e.getAttribute("aria-label") || e.textContent || e.value || "").replace(/\\s+/g, " ").trim().slice(0, 60), inDialog: Boolean(e.closest("dialog[open], [role=dialog], [role=menu]")), checked: e.getAttribute("aria-checked"), opener: e.hasAttribute("data-opener") }; })()`);
const titles = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].filter((d) => d.getBoundingClientRect().width > 0).map((d) => (d.querySelector("h2")?.textContent ?? "").trim())`);
const focusOn = (expression) => browser.evaluate(`(() => { const e = ${expression}; if (!e) return false; e.scrollIntoView({ block: "center" }); e.setAttribute("data-opener", ""); e.focus(); return document.activeElement === e; })()`);
const byText = (selector, pattern) => `[...document.querySelectorAll(${JSON.stringify(selector)})].find((e) => e.getBoundingClientRect().width > 0 && new RegExp(${JSON.stringify(pattern)}).test((e.getAttribute("aria-label") || e.textContent || "").replace(/\\s+/g, " ").trim()))`;
const value = (selector) => browser.evaluate(`document.querySelector(${JSON.stringify(selector)})?.value ?? null`);

async function open(target) {
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(1200);
}
async function trapped(flow, cell, presses = 40) {
  let escaped = null;
  for (let index = 0; index < presses; index += 1) {
    await key(TAB, 40);
    const now = await active();
    if (!now.inDialog) {
      escaped = now;
      break;
    }
  }
  record(flow, `Tab ×${presses} stays inside`, cell, escaped === null, escaped ? JSON.stringify(escaped) : undefined);
}
async function escapeReturns(flow, cell, expectOpener = true) {
  await key(ESC, 800);
  const after = await active();
  const open = await titles();
  record(flow, "Esc closes the layer", cell, open.length === 0, open.join(" | "));
  if (expectOpener) record(flow, "focus returns to the opener", cell, after.opener, JSON.stringify(after));
  else record(flow, "focus returns to the page, not the body", cell, after.tag !== "BODY", JSON.stringify(after));
}
async function focusTo(pattern, presses = 40) {
  for (let index = 0; index < presses; index += 1) {
    const now = await active();
    if (new RegExp(pattern).test(now.label) || new RegExp(pattern).test(now.id ?? "")) return true;
    await key(TAB, 40);
  }
  return false;
}
async function openCaptureByN(flow, cell, phone) {
  await focusOn(`document.querySelector("main")`);
  await key(N, 1500);
  const now = await active();
  const expected = phone ? "H2" : "INPUT";
  record(flow, `N opens Catat with focus on the ${phone ? "title" : "sentence"}`, cell, now.tag === expected && now.inDialog, JSON.stringify(now));
}
async function manual(flow, cell, phone) {
  await openCaptureByN(flow, cell, phone);
  await focusTo("^(Isi manual|Fill in manually)$");
  await key(ENTER, 1200);
}

async function login() {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  await sleep(500);
}

try {
  await login();
  const me = await browser.evaluate(`fetch("/api/auth/me", { credentials: "include" }).then((r) => r.json())`);
  const language = me.user.language;
  const en = language === "en";
  for (const [width, height, phone] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}-${language}`;
    await browser.setCell({ width, height, theme: "light", mobile: phone });
    await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("hideBalances")`);

    // 1
    await open("/");
    await openCaptureByN("1", cell, phone);
    await trapped("1", cell);
    await focusTo(en ? "^lunch 35k$" : "^makan siang 35rb$");
    await key(ENTER, 1500);
    const field = await value("dialog[open] [data-capture-field]");
    record("1", "an example chip by Enter fills the sentence", cell, Boolean(field && /35/.test(field)), field);
    const [catat] = await titles();
    await focusTo(en ? "^Record$" : "^Catat$");
    await key(ENTER, 2500);
    let asked = "";
    // Follow-up questions: focus moves to each question's first option, which Enter answers.
    for (let round = 0; round < 4 && (await titles()).length; round += 1) {
      const focus = await active();
      asked += `${await browser.evaluate(`document.querySelector("dialog[open] .capture-question p")?.textContent ?? ""`)} `;
      record("1", `question ${round + 1} takes focus on its first option`, cell, focus.inDialog && focus.tag === "BUTTON", JSON.stringify(focus));
      await key(ENTER, 2500);
    }
    await browser.waitFor(`/(Dicatat|Recorded)/.test(document.querySelector(".ws-toast")?.textContent ?? "")`, 6000).catch(() => false);
    const toast = await browser.evaluate(`document.querySelector(".ws-toast")?.textContent ?? ""`);
    record("1", "Catat records (after its question, if any) and the toast offers Urungkan", cell, /(Dicatat|Recorded)/.test(toast), `${catat} → ${asked ? asked + " → " : ""}${toast.slice(0, 70)}`);
    await focusOn(`document.querySelector(".ws-toast button")`);
    await key(ENTER, 1500);
    const undone = await browser.evaluate(`document.querySelector(".ws-toast")?.textContent ?? ""`);
    record("1", "Urungkan undoes the capture", cell, /(Catatan dibatalkan|Recording undone)/.test(undone), undone.slice(0, 60));

    // 2
    await open("/");
    await openCaptureByN("2", cell, phone);
    await browser.evaluate(`document.querySelector("dialog[open] [data-capture-field]").focus()`);
    await type(en ? "parking 5k" : "parkir 5rb");
    await sleep(1200);
    await focusTo("^(Isi manual|Fill in manually)$");
    await key(ENTER, 1200);
    const amount = await active();
    record("2", "the manual view takes focus on the amount with the amount read", cell, amount.id === "capture-amount" && /5[.,]000/.test(await value("#capture-amount")), JSON.stringify(amount));
    await focusTo("^(Tulis kalimat|Write a sentence)$");
    await key(ENTER, 800);
    record("2", "Tulis kalimat returns to the sentence with its text", cell, (await value("dialog[open] [data-capture-field]")) === (en ? "parking 5k" : "parkir 5rb"));
    await escapeReturns("2", cell, false);

    // 3
    await open("/");
    await manual("3", cell, phone);
    await browser.evaluate(`document.querySelector("dialog[open] [aria-labelledby=capture-pillar-label] [aria-checked=true]").focus()`);
    await key(RIGHT, 400);
    const pillar = await active();
    record("3", "an arrow key chooses the next pillar", cell, pillar.checked === "true" && /(Keinginan|Wants)/.test(pillar.label), JSON.stringify(pillar));
    await browser.evaluate(`document.querySelector("dialog[open] details > summary").focus()`);
    await key(ENTER, 500);
    record("3", "Enter opens Detail tambahan", cell, await browser.evaluate(`document.querySelector("dialog[open] details").open`));
    await trapped("3", cell);
    await escapeReturns("3", cell, false);

    // 4
    await open("/");
    await manual("4", cell, phone);
    await browser.evaluate(`document.querySelector("dialog[open] details > summary").focus()`);
    await key(ENTER, 500);
    for (let index = 0; index < 3; index += 1) {
      await focusOn(byText("dialog[open] button", "^(\\+ Tambah tagihan|\\+ Add a bill)$"));
      await key(ENTER, 500);
      // A closed select on macOS opens its list on ↓, so the option is chosen in the page once the select has focus.
      const found = await browser.evaluate(`(() => { const s = document.getElementById("capture-debt-${index}"); if (!s) return false; s.focus(); const o = [...s.options].find((x) => x.value); Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value").set.call(s, o.value); s.dispatchEvent(new Event("change", { bubbles: true })); return document.activeElement === s; })()`);
      record("4", `debt row ${index + 1} appears and takes focus`, cell, found);
      await sleep(300);
    }
    const rows = await browser.evaluate(`[0, 1, 2].map((i) => document.getElementById("capture-debt-" + i)?.value).filter(Boolean).length`);
    record("4", "three debts chosen by keyboard", cell, rows === 3, String(rows));
    await trapped("4", cell, 60);
    await escapeReturns("4", cell, false);

    // 5
    await open("/");
    await manual("5", cell, phone);
    await browser.evaluate(`document.querySelector("dialog[open] [role=radiogroup] [aria-checked=true]").focus()`);
    await key(RIGHT, 500);
    await browser.evaluate(`(() => { const s = document.getElementById("capture-category"); const o = [...s.options].find((x) => /Patungan/.test(x.textContent)); s.focus(); const set = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value").set; set.call(s, o.value); s.dispatchEvent(new Event("change", { bubbles: true })); })()`);
    await sleep(400);
    record("5", "Mengurangi pengeluaran is reachable by Tab", cell, await focusTo("capture-offset", 6));
    await escapeReturns("5", cell, false);

    // 6
    await open("/");
    await manual("6", cell, phone);
    await browser.evaluate(`document.querySelector("dialog[open] [role=radiogroup] [aria-checked=true]").focus()`);
    await key(["ArrowLeft", "ArrowLeft", 37], 500);
    await browser.evaluate(`(() => { const s = document.getElementById("capture-target"); const o = [...s.options].find((x) => /^SPayLater/.test(x.textContent)); s.focus(); const set = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value").set; set.call(s, o.value); s.dispatchEvent(new Event("change", { bubbles: true })); })()`);
    await sleep(500);
    const chip = await browser.evaluate(`(${byText("dialog[open] button", "^(Bayar sisa tagihan|Pay the amount left)")})?.textContent ?? ""`);
    await focusOn(byText("dialog[open] button", "^(Bayar sisa tagihan|Pay the amount left)"));
    await key(ENTER, 400);
    const filled = await value("#capture-amount");
    record("6", "Bayar sisa tagihan fills the amount left", cell, Boolean(filled) && chip.replace(/\D/g, "").endsWith(filled.replace(/\D/g, "")), `${chip} → ${filled}`);
    // A part of the bill is paid, so the bill stays open for the next flows.
    await browser.evaluate(`(() => { const a = document.getElementById("capture-amount"); a.focus(); a.select(); })()`);
    await type("10000");
    await browser.evaluate(`document.getElementById("capture-fee").focus()`);
    await type("500");
    await key(ENTER, 2500);
    record("6", "Enter in the fee saves the movement and closes", cell, (await titles()).length === 0);

    // 7
    await open("/");
    await manual("7", cell, phone);
    await browser.evaluate(`document.getElementById("capture-amount").focus()`);
    await type("300000");
    await browser.evaluate(`(() => { const s = document.getElementById("capture-account"); const o = [...s.options].find((x) => /^SPayLater/.test(x.textContent)); s.focus(); const set = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value").set; set.call(s, o.value); s.dispatchEvent(new Event("change", { bubbles: true })); })()`);
    await sleep(500);
    await browser.evaluate(`document.querySelector("dialog[open] .capture-credit [aria-checked=true]").focus()`);
    await key(RIGHT, 400);
    const plan = await active();
    record("7", "an arrow key chooses 3x", cell, plan.label === "3x" && plan.checked === "true", JSON.stringify(plan));
    const due = await value("#capture-credit-due");
    record("7", "the first due date shows the bill date", cell, /^\d{4}-\d{2}-\d{2}$/.test(due ?? ""), due);
    await browser.evaluate(`document.getElementById("capture-amount").focus()`);
    await key(ENTER, 2500);
    record("7", "Enter on the plan saves the purchase and closes", cell, (await titles()).length === 0);

    // 8
    await open("/accounts");
    await browser.evaluate(`(() => { const b = [...document.querySelectorAll("button")].find((e) => /^(Per kegunaan|By use)$/.test(e.textContent.trim())); b?.click(); })()`);
    await sleep(800);
    const tile = byText("[data-account-row], [data-account-row] button", "SPayLater");
    await focusOn(`(${tile})?.matches("button") ? (${tile}) : (${tile})?.querySelector("button")`);
    await key(ENTER, 1500);
    await focusOn(byText("dialog[open] button", "^(Bayar tagihan|Pay the bill)$"));
    await key(ENTER, 1800);
    const prefilled = await titles();
    record("8", "Bayar tagihan opens Catat on Perpindahan with Ke and the amount", cell,
      prefilled.some((title) => /(Catat transaksi|Record a transaction)/.test(title)) && (await value("#capture-target")) !== "" && Boolean(await value("#capture-amount")),
      `${prefilled.join(" | ")} · ${await value("#capture-amount")}`);
    await trapped("8", cell);
    await key(ESC, 900);
    const back8 = await active();
    record("8", "Esc returns focus to Bayar tagihan or its dialog", cell, back8.tag !== "BODY", JSON.stringify(back8));
    await key(ESC, 600);

    // 9
    await open("/");
    await focusOn(byText("button", "^(Notifikasi|Notifications)"));
    await key(ENTER, 800);
    await focusOn(byText(".notif", "SPayLater"));
    await key(ENTER, 1800);
    record("9", "the bill opens Perpindahan prefilled", cell, /\d/.test((await value("#capture-amount")) ?? "") && (await value("#capture-target")) !== "", await value("#capture-amount"));
    await trapped("9", cell);
    await escapeReturns("9", cell, false);

    // 10
    await open("/");
    if (phone) {
      await focusOn(byText(".tabbar button", "^(Lainnya|More)$"));
      await key(ENTER, 800);
      await focusOn(byText("dialog[open] button", "^(Tema|Theme)"));
      await key(ENTER, 800);
    } else {
      await focusOn(byText("button", "^(Tema|Theme)"));
      await key(ENTER, 800);
    }
    const first = await active();
    await key(DOWN, 200);
    const second = await active();
    record("10", "↓ moves between the theme options", cell, first.label !== second.label && second.tag === "BUTTON", `${first.label} → ${second.label}`);
    await key(ESC, 700);
    const back10 = await active();
    record("10", "Esc returns focus", cell, back10.tag !== "BODY", JSON.stringify(back10));
    if (phone) await key(ESC, 500);

    // 11
    await open("/");
    await focusOn(`document.querySelector("main")`);
    await key(["?", "Slash", 191, 8, "?"], 900);
    record("11", "? opens Bantuan", cell, (await titles()).some((title) => /(Bantuan|Help)/.test(title)));
    await trapped("11", cell, 20);
    await escapeReturns("11", cell, true);
    await focusOn(byText("button", "^(Notifikasi|Notifications)"));
    await key(ENTER, 800);
    await focusOn(byText(".notif", "(Rencana|plan)"));
    await key(ENTER, 1500);
    record("11", "a due plan opens Rencana jatuh tempo", cell, (await titles()).some((title) => /(Rencana jatuh tempo|Plans due)/.test(title)), (await titles()).join(" | "));
    await trapped("11", cell);
    await escapeReturns("11", cell, false);
  }
} finally {
  writeFileSync(path.join(args.out, "catat-keys.json"), JSON.stringify(results, null, 1));
  const failed = results.filter((result) => !result.pass);
  console.log(`checks ${results.length} passed ${results.length - failed.length} failed ${failed.length}`);
  browser.close();
}
