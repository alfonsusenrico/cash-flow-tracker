// Keyboard flows of Target & Tagihan and its H9 parts on Beranda and the bell (apply-markas-target D21, D26) on a
// running app, in the account's language. Read only: every write request is caught by a fetch guard and none is
// expected (flow 6 stops at the confirmation step, flows 11 and 12 close Perpindahan without saving).
// Writes target-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node target-keys.mjs --origin <url> --out <dir> [--port 9774] [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 Tab order: the actions, …, Rincian, Tagihan (Kelola, rows, toggle), goal rows, debt rows, credit rows, Sudah lunas",
  "2 Rincian: Enter opens, Tab stays inside, Esc returns focus to Rincian",
  "3 \"dan N tagihan lain\" toggles with Enter, focus stays on it",
  "4 a plan row opens Rencana & Pengingat with that plan chosen; Esc returns focus to the row",
  "5 a goal row → dialog → Arsipkan → Batal keeps focus on Arsipkan → Esc closes → focus on the row",
  "6 Sumber progres by arrows, a pocket by Space, Simpan shows the confirmation step; nothing is sent",
  "7 a debt row → Kaitkan atau catat pembayaran → Esc → focus back, Esc → focus on the row",
  "8 Sudah lunas · N toggles aria-expanded with Enter",
  "9 a credit row opens the account dialog, Esc returns focus to the row",
  "10 … by keyboard: ArrowDown through Tambah utang and the emergency item, Esc returns focus",
  "11 Beranda: a credit bill in Perlu tindakan opens Perpindahan with Ke and the amount filled; Esc closes, no request",
  "12 the bell: a credit bill item by keyboard opens the same",
  "13 phone: a sheet opens and closes with focus return; the focused row is not under the top bar or the tab bar",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9774), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const SPACE = [" ", "Space", 32, 0, " "];
const DOWN = ["ArrowDown", "ArrowDown", 40];
const LEFT = ["ArrowLeft", "ArrowLeft", 37];
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const key = (spec, modifiers = 0) => browser.key(spec[0], spec[1], spec[2], modifiers || spec[3] || 0, spec[4]);
const active = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e || e === document.body) return null; const r = e.getBoundingClientRect(); return { tag: e.tagName, row: e.getAttribute("data-row"), label: (e.getAttribute("aria-label") || e.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 70), inDialog: Boolean(e.closest("dialog[open], [role=dialog]")), menu: Boolean(e.closest("[role=menu]")), top: Math.round(r.top), bottom: Math.round(r.bottom), expanded: e.getAttribute("aria-expanded"), checked: e.getAttribute("aria-checked") }; })()`);
const dialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open], [role=dialog]")].filter((d) => d.getBoundingClientRect().width > 0).map((d) => (d.querySelector("h2")?.textContent ?? d.getAttribute("aria-label") ?? "").trim())`);
const focusOn = (expression) => browser.evaluate(`(() => { const e = ${expression}; if (!e) return false; e.scrollIntoView({ block: "center" }); e.focus(); return document.activeElement === e; })()`);
const byText = (selector, pattern, scope = "document") => `[...${scope}.querySelectorAll(${JSON.stringify(selector)})].find((e) => e.getBoundingClientRect().width > 0 && new RegExp(${JSON.stringify(pattern)}).test((e.getAttribute("aria-label") || e.textContent || "").replace(/\\s+/g, " ").trim()))`;
const guardWrites = () => browser.evaluate(`(() => { if (window.__writes) return; window.__writes = []; const original = window.fetch; window.fetch = (input, init) => { const method = (init?.method ?? "GET").toUpperCase(); if (method !== "GET") window.__writes.push(method + " " + String(input)); return original(input, init); }; })()`);
const writes = () => browser.evaluate("window.__writes ?? []");
async function open(target) {
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(1000);
  await guardWrites();
}
const api = (url) => browser.evaluate(`fetch("/api${url}", { credentials: "include" }).then((r) => r.json())`);
// The capture form's state: the visible selects with their chosen option, and the amount field.
const captureState = () => browser.evaluate(`(() => {
  const dialog = [...document.querySelectorAll("dialog[open], [role=dialog]")].filter((d) => d.getBoundingClientRect().width > 0).pop();
  if (!dialog) return null;
  const selects = [...dialog.querySelectorAll("select")].filter((s) => s.getBoundingClientRect().width > 0).map((s) => ({ label: (s.labels?.[0]?.textContent ?? s.getAttribute("aria-label") ?? "").trim(), value: s.value, text: s.selectedOptions[0]?.textContent?.trim() ?? "" }));
  const amount = [...dialog.querySelectorAll("input")].find((i) => i.inputMode === "numeric" || /nominal|amount/i.test(i.name + " " + (i.labels?.[0]?.textContent ?? "")));
  const pressed = [...dialog.querySelectorAll("[aria-pressed=true], [aria-selected=true], [aria-checked=true]")].map((e) => e.textContent.trim()).join(" | ");
  return { title: dialog.querySelector("h2")?.textContent?.trim() ?? "", selects, amount: amount?.value ?? null, mode: pressed };
})()`);
const digits = (value) => String(value ?? "").replace(/[^\d]/g, "");

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "target-contoh-123" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  const goals = (await api("/goals")).goals.filter((goal) => !goal.is_archived);
  const manualGoal = goals.find((goal) => !(goal.account_ids ?? []).length && goal.remaining_amount > 0 && !goal.is_emergency);
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" }).format(new Date());
  const twoDays = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" }).format(new Date(Date.now() + 2 * 86_400_000));
  const bills = (await api(`/credit/bills?to=${twoDays}&status=open,upcoming,due_today,overdue`)).bills.filter((bill) => bill.left_to_pay > 0);

  for (const [width, height, mobile] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}`;
    await browser.setCell({ width, height, theme: "light", mobile });
    await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("hideBalances")`);

    // 1
    if (!mobile) {
      await open("/goals");
      await browser.evaluate("document.activeElement?.blur(); window.scrollTo(0, 0)");
      const stops = [];
      for (let index = 0; index < 60; index += 1) {
        await key(TAB);
        stops.push(await browser.evaluate(`(() => { const e = document.activeElement; if (!e) return "none"; if (e.closest(".tb-actions")) return e.getAttribute("aria-haspopup") === "menu" ? "more" : "action"; if (e.closest(".tg-page section.hero")) return "rincian"; if (e.closest("[aria-labelledby=tg-bills]")) return "bills"; if (e.matches(".goal-row")) return "goal"; const row = e.getAttribute("data-row") ?? ""; if (row.startsWith("debt-")) return "debt"; if (row.startsWith("credit-")) return "credit"; if (/^(Sudah lunas|Paid off)/.test(e.textContent.trim())) return "paid"; return "other"; })()`));
      }
      const order = ["action", "more", "rincian", "bills", "goal", "debt", "credit", "paid"].map((name) => stops.indexOf(name));
      record(1, "actions, …, Rincian, Tagihan, goals, debts, credit rows, Sudah lunas in order", cell, order.every((index, position) => index >= 0 && (position === 0 || index > order[position - 1])), JSON.stringify(order));
      record(1, "Sudah lunas follows the last credit row", cell, stops.lastIndexOf("credit") < stops.indexOf("paid"), "");

      // 2
      await focusOn(byText(".tg-page section.hero button", "^(Rincian|Breakdown)$"));
      await key(ENTER);
      await sleep(500);
      const opened = await dialogs();
      let inside = true;
      for (let index = 0; index < 8; index += 1) {
        await key(TAB);
        inside &&= Boolean((await active())?.inDialog);
      }
      await key(ESC);
      await sleep(400);
      const back = await active();
      record(2, "Rincian opens, Tab stays inside, Esc returns focus", cell, opened.length === 1 && inside && /^(Rincian|Breakdown)$/.test(back?.label ?? ""), `${opened} → ${back?.label}`);

      // 3
      await focusOn(byText("[aria-labelledby=tg-bills] button[aria-expanded]", "."));
      const before = await active();
      await key(ENTER);
      await sleep(300);
      const after = await active();
      record(3, "the toggle expands with Enter and keeps focus", cell, before?.expanded === "false" && after?.expanded === "true", `${before?.label} → ${after?.label}`);
      await key(ENTER);
      await sleep(300);

      // 4
      await focusOn(`document.querySelector('[aria-labelledby=tg-bills] .row[data-row^="plan-"]')`);
      const planRow = await active();
      await key(ENTER);
      await sleep(1500);
      const chosen = await browser.evaluate(`(() => { const d = [...document.querySelectorAll("dialog[open]")].pop(); const current = d?.querySelector("[aria-current=true]"); return { title: d?.querySelector("h2")?.textContent ?? "", current: current?.textContent?.replace(/\\s+/g, " ").trim().slice(0, 60) ?? null }; })()`);
      const planName = planRow?.label.replace(/^\d+ \w+ ?/, "").split(/terlewat|jatuh tempo|terjadwal|overdue|due today|scheduled/)[0].trim();
      await key(ESC);
      await sleep(500);
      const planBack = await active();
      record(4, "a plan row opens the plans dialog with that plan chosen", cell, Boolean(chosen.current) && chosen.current.includes(planName ?? "?"), `${planName} → ${chosen.title} / ${chosen.current}`);
      record(4, "Esc returns focus to the plan row", cell, planBack?.row === planRow?.row, planBack?.row ?? "");
    }

    // 5
    await open("/goals");
    await focusOn(`document.querySelector(".goal-row")`);
    const goalRow = await active();
    await key(ENTER);
    await sleep(700);
    await focusOn(byText("dialog[open] .dlg-f button", "^(Arsipkan|Archive)$"));
    await key(ENTER);
    await sleep(300);
    const confirmFocus = await active();
    await key(ENTER);
    await sleep(400);
    const archiveFocus = await active();
    await key(ESC);
    await sleep(500);
    const goalBack = await active();
    record(5, "Arsipkan asks in the footer with focus on Batal", cell, /^(Batal|Cancel)$/.test(confirmFocus?.label ?? ""), confirmFocus?.label);
    record(5, "Batal returns focus to Arsipkan", cell, /^(Arsipkan|Archive)$/.test(archiveFocus?.label ?? ""), archiveFocus?.label);
    record(5, "Esc closes and focus returns to the goal row", cell, (await dialogs()).length === 0 && goalBack?.row === goalRow?.row, goalBack?.row ?? goalBack?.label);

    // 6
    if (manualGoal) {
      await open(`/goals?target=${manualGoal.id}`);
      await focusOn(`document.querySelector("dialog[open] [role=radio][aria-checked=true]")`);
      const start = await active();
      await key(start?.label?.match(/Manual/) ? LEFT : RIGHT);
      await sleep(300);
      const moved = await active();
      await focusOn(`document.querySelector("dialog[open] .tg-pockets input[type=checkbox], dialog[open] .tg-picker input[type=checkbox]")`);
      await key(SPACE);
      await sleep(200);
      await focusOn(byText("dialog[open] .dlg-f button", "^(Simpan|Save)$"));
      await key(ENTER);
      await sleep(500);
      const step = await browser.evaluate(`Boolean([...document.querySelectorAll("dialog[open] [role=alert]")].find((e) => /Konfirmasi|Confirm/.test(e.textContent))) && Boolean(${byText("dialog[open] .dlg-f button", "^(Konfirmasi|Confirm)")})`);
      record(6, "Sumber progres moves with the arrows", cell, start?.checked === "true" && moved?.checked === "true" && moved?.label !== start?.label, `${start?.label} → ${moved?.label}`);
      record(6, "Simpan shows the confirmation step and sends nothing", cell, step && (await writes()).length === 0, JSON.stringify(await writes()));
      await key(ESC);
      await sleep(400);
    }

    // 7
    await open("/goals");
    await focusOn(`document.querySelector('.panel .row[data-row^="debt-"]')`);
    const debtRow = await active();
    await key(ENTER);
    await sleep(700);
    await focusOn(byText("dialog[open] button", "^(Kaitkan atau catat pembayaran|Link or record a payment)$"));
    await key(ENTER);
    await sleep(900);
    const payOpen = await dialogs();
    await key(ESC);
    await sleep(500);
    const payBack = await active();
    await key(ESC);
    await sleep(500);
    const debtBack = await active();
    record(7, "the payment dialog opens over the debt dialog", cell, payOpen.length === 2, JSON.stringify(payOpen));
    record(7, "Esc returns focus to the payment button, then to the debt row", cell, /Kaitkan|Link or record/.test(payBack?.label ?? "") && debtBack?.row === debtRow?.row, `${payBack?.label} / ${debtBack?.row}`);

    // 8
    await focusOn(byText("[aria-labelledby=tg-debts] button[aria-expanded]", "^(Sudah lunas|Paid off)"));
    const paid = await active();
    await key(ENTER);
    await sleep(300);
    const paidAfter = await active();
    record(8, "Sudah lunas toggles aria-expanded with Enter", cell, paid?.expanded === "false" && paidAfter?.expanded === "true", `${paid?.label}`);

    // 9
    await focusOn(`document.querySelector('.panel .row[data-row^="credit-"]')`);
    const creditRow = await active();
    await key(ENTER);
    await sleep(1200);
    const creditDialog = await dialogs();
    await key(ESC);
    await sleep(500);
    const creditBack = await active();
    record(9, "a credit row opens its account and Esc returns focus to it", cell, creditDialog.length === 1 && creditBack?.row === creditRow?.row, `${creditDialog} / ${creditBack?.row}`);

    // 10
    await focusOn(`document.querySelector('.tb-actions [aria-haspopup=menu]')`);
    const more = await active();
    await key(ENTER);
    await sleep(400);
    const items = [];
    for (let index = 0; index < 3; index += 1) {
      items.push((await active())?.label);
      await key(DOWN);
      await sleep(150);
    }
    await key(ESC);
    await sleep(300);
    const moreBack = await active();
    record(10, "… lists Tambah utang and the emergency item by arrows; Esc returns", cell,
      items.some((label) => /^(Tambah utang|Add debt)$/.test(label ?? "")) && items.some((label) => /dana darurat|emergency fund/i.test(label ?? "")) && moreBack?.label === more?.label,
      JSON.stringify(items));

    // 11
    const bill = bills[0];
    if (bill) {
      await open("/");
      await focusOn(byText("[aria-labelledby=beranda-todo] .row", bill.account_name));
      await key(ENTER);
      await sleep(1200);
      const capture = await captureState();
      const into = capture?.selects.find((select) => select.value === bill.account_id);
      record(11, "Perlu tindakan opens Perpindahan into the bill's account with the amount left", cell,
        Boolean(into) && digits(capture?.amount) === String(bill.left_to_pay), `${bill.account_name} ${bill.left_to_pay} → ${JSON.stringify(capture)}`.slice(0, 300));
      await key(ESC);
      await sleep(600);
      record(11, "Esc closes without a request", cell, (await dialogs()).length === 0 && (await writes()).length === 0, JSON.stringify(await writes()));

      // 12
      await focusOn(`[...document.querySelectorAll("button.bell")].find((button) => button.getBoundingClientRect().width > 0)`);
      await key(ENTER);
      await sleep(600);
      let found = null;
      for (let index = 0; index < 20 && !found; index += 1) {
        const now = await active();
        if (now?.label?.includes(bill.account_name)) found = now;
        else await key(TAB);
      }
      if (found) await key(ENTER);
      await sleep(1200);
      const fromBell = await captureState();
      const intoBell = fromBell?.selects.find((select) => select.value === bill.account_id);
      record(12, "the bell's credit bill opens the same Perpindahan", cell, Boolean(found) && Boolean(intoBell) && digits(fromBell?.amount) === String(bill.left_to_pay), `${found?.label} → ${JSON.stringify(fromBell)}`.slice(0, 300));
      await key(ESC);
      await sleep(600);
      record(12, "no request after closing", cell, (await writes()).length === 0, JSON.stringify(await writes()));
    }

    // 13
    if (mobile) {
      await open("/goals");
      await focusOn(`[...document.querySelectorAll(".goal-row")].pop()`);
      const row = await active();
      await key(ENTER);
      await sleep(800);
      const sheet = await browser.evaluate(`(() => { const d = [...document.querySelectorAll("dialog[open]")].pop(); const r = d?.getBoundingClientRect(); return r ? { width: Math.round(r.width), bottom: Math.round(r.bottom) } : null; })()`);
      await key(ESC);
      await sleep(600);
      const rowBack = await active();
      const tabBar = await browser.evaluate(`(() => { const bar = [...document.querySelectorAll("nav")].find((n) => getComputedStyle(n).position === "fixed" && n.getBoundingClientRect().top > innerHeight / 2); return bar ? Math.round(bar.getBoundingClientRect().top) : innerHeight; })()`);
      record(13, "the sheet spans the phone and focus returns to the row", cell, sheet?.width === width && rowBack?.row === row?.row, `${JSON.stringify(sheet)} ${rowBack?.row}`);
      record(13, "the focused row is not under the top bar or the tab bar", cell, rowBack && rowBack.top >= 56 && rowBack.bottom <= tabBar, `top ${rowBack?.top} bottom ${rowBack?.bottom} tab bar ${tabBar}`);
    }
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "target-keys.json"), JSON.stringify({ origin: args.origin, at: new Date().toISOString(), pass: results.filter((r) => r.pass).length, fail: results.filter((r) => !r.pass).length, results }, null, 1));
console.log(JSON.stringify({ pass: results.filter((r) => r.pass).length, fail: results.filter((r) => !r.pass).length }));
