// Keyboard flows of Laporan (apply-markas-laporan D17, D23) on a running app, in the account's language. The note
// flow writes a note on the running month and deletes it again; nothing else is written. Writes laporan-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node laporan-keys.mjs --origin http://127.0.0.1:3733 --out <dir> [--port 9735] [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 the basis segment and the stepper: Enter on the previous chevron opens the earlier report, the reset returns",
  "2 a past report row: Enter opens that report in place",
  "3 a table row: Enter on its link opens the ledger for that category and window",
  "4 a top-place row: Enter opens the ledger searched for that place",
  "5 the disclosure: Enter opens and closes it; an unexpected item opens the transaction dialog, Esc returns focus to it",
  "6 Atur anggaran: Enter opens the budget dialog, Esc returns focus to the button",
  "7 Lengkapi klasifikasi: Enter opens the dialog, Esc returns focus to the button",
  "8 the note: write opens the form with focus in it, save moves focus to Ubah, Ubah reopens it, delete returns to Tulis",
  "9 the chart/table toggle: Enter swaps, focus stays, aria-pressed follows",
  "10 Buka Tinjau: Enter opens Tinjau",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9735), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const SPACE = [" ", "Space", 32];
const press = async (key, wait = 600) => {
  await browser.key(...key);
  await sleep(wait);
};
const url = () => browser.evaluate("location.pathname + location.search");
const focused = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e) return null; return { tag: e.tagName, cls: String(e.className ?? ""), row: e.closest("[data-row]")?.dataset.row ?? null, label: (e.getAttribute("aria-label") || e.textContent || "").trim().slice(0, 60), inDialog: Boolean(e.closest("dialog[open]")), pressed: e.getAttribute("aria-pressed"), field: e.tagName === "TEXTAREA" }; })()`);
const openDialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].length`);
const focusOn = (expression) => browser.evaluate(`(() => { const e = ${expression}; e?.focus(); return Boolean(e); })()`);
const byText = (selector, pattern) => `[...document.querySelectorAll(${JSON.stringify(selector)})].find((e) => new RegExp(${JSON.stringify(pattern)}).test(e.textContent.trim()) && e.getBoundingClientRect().width > 0)`;
async function open(target) {
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(1500);
}

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER, password: process.env.MARKAS_PASSWORD };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  const running = await browser.evaluate(`fetch("/api/reports/monthly/periods?basis=month").then((r) => r.json()).then((d) => d.periods.find((p) => !p.complete)?.start_date)`);

  for (const [width, height, mobile] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}`;
    await browser.setCell({ width, height, theme: "light", mobile });
    await browser.evaluate(`localStorage.setItem("theme", "light")`);

    // 1: the stepper.
    await open("/laporan");
    const startUrl = await url();
    await focusOn(`document.querySelector(".stepper .ws-icon-btn")`);
    await press(ENTER, 1800);
    record(1, "Enter on the previous chevron opens the earlier report", cell, (await url()) !== startUrl && (await url()).includes("start="), await url());
    await focusOn(`document.querySelector(".stepper .ws-btn")`);
    await press(ENTER, 1800);
    record(1, "the reset opens the running report", cell, (await url()).includes(`start=${running}`), await url());
    await focusOn(`document.querySelector(".ws-seg button[aria-pressed='false']")`);
    await press(ENTER, 1800);
    record(1, "Enter on the other basis switches it", cell, (await url()).includes("basis=cycle"), await url());

    // 2: a past report row.
    await open("/laporan");
    await focusOn(`[...document.querySelectorAll(".lp-past-row")].find((row) => !row.getAttribute("aria-current"))`);
    const target = await browser.evaluate(`document.activeElement.getAttribute("href")`);
    await press(ENTER, 1800);
    record(2, "Enter opens the listed report in place", cell, (await url()) === target, `${await url()} ${target}`);
    record(2, "the opened report is marked current", cell, await browser.evaluate(`document.querySelector(".lp-past-row[aria-current='page']")?.getAttribute("href") === ${JSON.stringify(target)}`), "");

    // 3: a table row.
    await open("/laporan");
    await focusOn(`document.querySelector(".lp-tbl .lp-cat .lp-rowlink")`);
    await press(ENTER, 2500);
    let now = await url();
    record(3, "Enter on a category row opens its ledger rows", cell, now.startsWith("/ledger") && now.includes("category="), now);

    // 4: a top place.
    await open("/laporan");
    await focusOn(`document.querySelector("[aria-labelledby=lp-places] .row")`);
    await press(ENTER, 2500);
    now = await url();
    record(4, "Enter on a top place opens its ledger rows", cell, now.startsWith("/ledger") && now.includes("q="), now);

    // 5: the disclosure and an unexpected item.
    await open("/laporan");
    await focusOn(`document.querySelector(".lp-details > summary")`);
    await press(ENTER);
    record(5, "Enter opens the disclosure", cell, await browser.evaluate(`document.querySelector(".lp-details").open`), "");
    await press(SPACE);
    record(5, "Space closes it again", cell, !(await browser.evaluate(`document.querySelector(".lp-details").open`)), "");
    await press(ENTER);
    await focusOn(`document.querySelector(".lp-details-body button.row")`);
    const item = (await focused())?.row;
    await press(ENTER, 2000);
    record(5, "Enter on an unexpected item opens the transaction dialog", cell, (await openDialogs()) === 1, "");
    await press(ESC, 1000);
    let state = await focused();
    record(5, "Esc closes it with focus on the item", cell, (await openDialogs()) === 0 && state?.row === item, `${state?.row} ${item}`);

    // 6: Atur anggaran.
    await focusOn(`document.querySelector("#lp-budget")?.closest(".sec-h")?.querySelector("button")`);
    await press(ENTER, 1500);
    state = await focused();
    record(6, "Enter opens the budget dialog with focus inside", cell, (await openDialogs()) === 1 && state?.inDialog, state?.label);
    await press(ESC, 1000);
    state = await focused();
    record(6, "Esc returns focus to Atur anggaran", cell, (await openDialogs()) === 0 && /Atur anggaran|Set budgets/.test(state?.label ?? ""), state?.label);

    // 7: Lengkapi klasifikasi, on a report with an expense without a pillar.
    const withGap = await browser.evaluate(`fetch("/api/reports/monthly/periods?basis=month").then((r) => r.json()).then(async (d) => { for (const p of d.periods) { const r = await fetch("/api/reports/monthly?basis=month&start_date=" + p.start_date).then((x) => x.json()); if (r.data_quality?.unclassified?.count > 0) return p.start_date; } return null; })`);
    if (withGap) {
      await open(`/laporan?basis=month&start=${withGap}`);
      await focusOn(byText(".lp-fixes button", "^(Lengkapi klasifikasi|Complete classification)$"));
      await press(ENTER, 1500);
      state = await focused();
      record(7, "Enter opens the classification dialog with focus inside", cell, (await openDialogs()) === 1 && state?.inDialog, state?.label);
      await press(ESC, 1000);
      state = await focused();
      record(7, "Esc returns focus to Lengkapi klasifikasi", cell, (await openDialogs()) === 0 && /Lengkapi klasifikasi|Complete classification/.test(state?.label ?? ""), state?.label);
    } else record(7, "a report with an expense without a pillar exists", cell, false, "none in the seed");

    // 8: the note, on the running month, removed again at the end.
    await open(`/laporan?basis=month&start=${running}`);
    await focusOn(`document.querySelector(".lp-note-write")`);
    await press(ENTER, 800);
    state = await focused();
    record(8, "Tulis catatan opens the form with focus in the field", cell, state?.field, state?.tag);
    await browser.evaluate(`(() => { const field = document.activeElement; const set = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value").set; set.call(field, "Catatan uji keyboard"); field.dispatchEvent(new Event("input", { bubbles: true })); })()`);
    await sleep(300);
    await focusOn(`document.querySelector(".lp-note-actions button[type=submit]")`);
    await press(ENTER, 2000);
    state = await focused();
    record(8, "saving moves focus to Ubah", cell, /^(Ubah|Edit)$/.test(state?.label ?? ""), state?.label);
    await press(ENTER, 800);
    state = await focused();
    record(8, "Ubah reopens the form with the note", cell, state?.field && (await browser.evaluate("document.activeElement.value")) === "Catatan uji keyboard", state?.tag);
    await focusOn(byText(".lp-note-actions button", "^(Hapus catatan|Delete note)$"));
    await press(ENTER, 2000);
    state = await focused();
    record(8, "deleting returns focus to Tulis catatan", cell, /^(Tulis catatan|Write a note)/.test(state?.label ?? ""), state?.label);

    // 9: the chart/table toggle.
    await open("/laporan");
    await focusOn(`document.querySelector("#lp-history")?.closest(".sec-h")?.querySelector("button")`);
    await press(ENTER, 800);
    state = await focused();
    record(9, "Enter shows the table and keeps focus with aria-pressed", cell, state?.pressed === "true" && (await browser.evaluate(`Boolean(document.querySelector("[aria-labelledby=lp-history] table"))`)), state?.label);
    await press(ENTER, 800);
    state = await focused();
    record(9, "Enter again shows the chart", cell, state?.pressed === "false" && (await browser.evaluate(`Boolean(document.querySelector(".bars-v"))`)), state?.label);

    // 10: Buka Tinjau, on a report with rows waiting.
    const waiting = await browser.evaluate(`fetch("/api/reports/monthly/periods?basis=month").then((r) => r.json()).then(async (d) => { for (const p of d.periods) { const r = await fetch("/api/reports/monthly?basis=month&start_date=" + p.start_date).then((x) => x.json()); if (r.data_quality?.pending_review > 0) return p.start_date; } return null; })`);
    if (waiting) {
      await open(`/laporan?basis=month&start=${waiting}`);
      await focusOn(byText(".lp-fixes a", "^(Buka Tinjau|Open Review)$"));
      await press(ENTER, 2500);
      record(10, "Enter on Buka Tinjau opens Tinjau", cell, (await url()).startsWith("/review"), await url());
    } else record(10, "a report with rows waiting exists", cell, false, "none in the seed");
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "laporan-keys.json"), JSON.stringify(results, null, 1));
console.log(`passed ${results.filter((entry) => entry.pass).length} of ${results.length}`);
