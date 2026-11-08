// Keyboard flows of Transaksi (apply-markas-transaksi D24) on a running app, in the account's language. The only
// write is "Tandai sudah dicek", which flow 10 undoes with Z. Writes transaksi-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node transaksi-keys.mjs --origin http://127.0.0.1:3721 --out <dir> [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 Tab order: skip link, sidebar, top bar, stepper, …, rail, toolbar, the first row",
  "2 J/K across a day heading, Enter opens, Esc closes with focus on the row",
  "3 j typed in the search field",
  "4 the rail and the type control by keyboard",
  "5 … with arrows and Esc",
  "6 Filter: Tab stays inside, a segment by arrows, Terapkan, focus back on Filter",
  "7 removing a chip by keyboard",
  "8 merge by keyboard: … › Gabungkan, Space on two checkboxes, Batal gabungkan",
  "9 detail → Ubah → Esc (focus on Ubah) → Ubah again",
  "10 Tandai sudah dicek, then Z",
  "11 delete: Hapus, Batal focused, Esc",
  "12 the phone sheet: open, back closes with focus on the row",
  "13 the return=review deep link",
  "14 the single-key switch off",
  "15 no focused row under the top bar or a day heading",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9738), profileDir: path.join(args.out, ".profile-keys") });
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
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const char = (value) => [value, `Key${value.toUpperCase()}`, value.toUpperCase().charCodeAt(0), 0, value];
const active = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e) return null; return { tag: e.tagName, row: e.closest("[data-row]")?.dataset.row ?? null, label: (e.getAttribute("aria-label") || e.textContent || "").trim().slice(0, 60), inDialog: Boolean(e.closest("dialog[open]")), rail: Boolean(e.closest(".tx-rail")), menu: Boolean(e.closest("[role=menu]")) }; })()`);
const url = () => browser.evaluate("location.pathname + location.search");
const dialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].map((dialog) => dialog.querySelector("h2")?.textContent ?? "")`);
const focusSelector = (selector) => browser.evaluate(`(() => { const e = document.querySelector(${JSON.stringify(selector)}); e?.focus(); return Boolean(e); })()`);
async function open(target) {
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(900);
}

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "contoh-sandi-123" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);

  for (const [width, height, mobile] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}`;
    await browser.setCell({ width, height, theme: "light", mobile });
    await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("shortcuts")`);

    if (!mobile) {
      // 1: the Tab order from the start of the page.
      await open("/ledger?period=cycle&waiting=1");
      await browser.evaluate("document.activeElement?.blur(); window.scrollTo(0, 0)");
      const stops = [];
      for (let index = 0; index < 45; index += 1) {
        await browser.key(...TAB);
        stops.push(await browser.evaluate(`(() => { const e = document.activeElement; return e.closest(".skip, [href='#main']") ? "skip" : e.closest("nav.sb, .sidebar, aside") && !e.closest("main") ? "sidebar" : e.closest(".topbar, header.tb, .tb") && !e.closest("main") ? "topbar" : e.closest(".stepper") ? "stepper" : e.closest(".tb-actions") ? "more" : e.closest(".tx-rail") ? "rail" : e.closest(".toolbar") ? "toolbar" : e.closest(".chips") ? "chips" : e.closest("[data-row]") ? "row" : "other:" + e.tagName; })()`));
      }
      const order = ["stepper", "more", "rail", "toolbar", "chips", "row"].map((name) => stops.indexOf(name));
      record(1, "stepper, …, rail, toolbar, chips and the first row come in order", cell, order.every((index, position) => index >= 0 && (position === 0 || index > order[position - 1])), JSON.stringify([...new Set(stops)]));

      // 2: J/K and Enter/Esc.
      await open("/ledger?period=cycle");
      await focusSelector(".tx-list [data-row]");
      const first = (await active()).row;
      await browser.key(...char("j"));
      await sleep(200);
      const second = (await active()).row;
      await browser.key(...char("k"));
      await sleep(200);
      record(2, "J moves to the next row and K back", cell, Boolean(second) && second !== first && (await active()).row === first, `${first} → ${second}`);
      for (let index = 0; index < 6; index += 1) await browser.key(...char("j"));
      await sleep(200);
      const deeper = (await active()).row;
      const days = await browser.evaluate(`(() => { const rows = [...document.querySelectorAll(".tx-list [data-row]")]; const index = rows.findIndex((row) => row.dataset.row === ${JSON.stringify(deeper)}); return [...new Set(rows.slice(0, index + 1).map((row) => row.closest(".day-group")))].length; })()`);
      record(2, "J walks across a day heading", cell, days >= 2, `${days} day groups`);
      await browser.key(...ENTER);
      await sleep(1200);
      const opened = await dialogs();
      await browser.key(...ESC);
      await sleep(900);
      record(2, "Enter opens the dialog and Esc returns focus to its row", cell, opened.length === 1 && (await active()).row === deeper, `${opened[0]} → ${(await active()).row}`);

      // 3: typing in search.
      await focusSelector("#ledger-search");
      await browser.key(...char("j"));
      await sleep(300);
      record(3, "j types into the search field", cell, (await browser.evaluate(`document.getElementById("ledger-search").value`)) === "j");

      // 4: the rail by keyboard.
      await open("/ledger?period=cycle");
      await browser.evaluate(`[...document.querySelectorAll(".tx-rail .rail-item")][1].focus()`);
      await browser.key(...ENTER);
      await sleep(900);
      record(4, "Enter on a rail type writes the URL and keeps focus on it", cell, (await url()).includes("type=expense") && (await active()).rail, await url());

      // 5: the … menu.
      await focusSelector(".tb-actions button[aria-haspopup]");
      await browser.key(...ENTER);
      await sleep(400);
      await browser.key(...DOWN);
      const inMenu = (await active()).menu;
      await browser.key(...ESC);
      await sleep(300);
      record(5, "… opens with Enter, arrows move inside it, Esc returns to …", cell, inMenu && (await browser.evaluate(`document.activeElement?.matches(".tb-actions button[aria-haspopup]")`)));

      // 6: the Filter dialog.
      await open("/ledger?period=cycle");
      await focusSelector(".toolbar .ws-btn");
      await browser.key(...ENTER);
      await sleep(800);
      let inside = true;
      for (let index = 0; index < 30; index += 1) {
        await browser.key(...TAB);
        inside = inside && (await active()).inDialog;
      }
      await browser.evaluate(`document.querySelector('dialog[open] [role=radiogroup] [role=radio][aria-checked=true]')?.focus()`);
      await browser.key(...RIGHT);
      await sleep(200);
      const checked = await browser.evaluate(`document.querySelector('dialog[open] [role=radiogroup] [role=radio][aria-checked=true]')?.textContent`);
      await browser.evaluate(`[...document.querySelectorAll("dialog[open] .dlg-f button")].at(-1).focus()`);
      await browser.key(...ENTER);
      await sleep(900);
      record(6, "Tab stays inside, an arrow picks a segment, Terapkan writes the URL and focus returns to Filter", cell,
        inside && (await url()).includes("review=needs_review") && (await browser.evaluate(`document.activeElement?.closest(".toolbar") !== null`)), `${checked} ${await url()}`);

      // 7: removing a chip.
      await open("/ledger?period=cycle&waiting=1&kakeibo=want");
      await focusSelector(".chips .chip button");
      await browser.key(...ENTER);
      await sleep(900);
      record(7, "Enter on a chip's remove button clears it and moves focus to the next chip", cell,
        !(await url()).includes("kakeibo") && (await browser.evaluate(`document.activeElement?.closest(".chips") !== null`)), await url());

      // 8: merge by keyboard.
      await open("/ledger?period=cycle");
      await focusSelector(".tb-actions button[aria-haspopup]");
      await browser.key(...ENTER);
      await sleep(400);
      await browser.key(...ENTER);
      await sleep(600);
      const boxes = await browser.evaluate(`[...document.querySelectorAll(".tx-list input[type=checkbox]:not(:disabled)")].length`);
      await focusSelector(".tx-list input[type=checkbox]:not(:disabled)");
      await browser.key(...SPACE);
      await browser.key(...char("j"));
      await browser.key(...SPACE);
      await sleep(400);
      const count = await browser.evaluate(`document.querySelector(".sel-bar")?.textContent ?? ""`);
      await browser.evaluate(`document.querySelector(".sel-bar .ws-btn-quiet")?.focus()`);
      await browser.key(...ENTER);
      await sleep(400);
      record(8, "… › Gabungkan starts selection, Space checks two rows, Batal gabungkan ends it", cell,
        boxes > 1 && /2 (dari|of) 2/.test(count) && (await browser.evaluate(`document.querySelector(".sel-bar") === null`)), count);

      // 9: detail → Ubah → Esc.
      await open("/ledger?period=cycle&type=expense");
      await focusSelector(".tx-list [data-row]");
      await browser.key(...ENTER);
      await sleep(1200);
      await browser.evaluate(`[...document.querySelectorAll("dialog[open] .dlg-f button")].at(-1).focus()`);
      await browser.key(...ENTER);
      await sleep(1200);
      const editing = await dialogs();
      await browser.key(...ESC);
      await sleep(900);
      const back = await dialogs();
      const onEdit = await browser.evaluate(`document.activeElement === [...document.querySelectorAll("dialog[open] .dlg-f button")].at(-1)`);
      record(9, "Ubah opens the edit in place of the detail; Esc returns to the detail with focus on Ubah", cell, editing.length === 1 && back.length === 1 && editing[0] !== back[0] && onEdit, `${editing[0]} → ${back[0]}`);
      await browser.key(...ESC);
      await sleep(600);

      // 10: Tandai sudah dicek, then Z.
      const auto = await browser.evaluate(`fetch("/api/transactions?review_status=auto&limit=1").then((r) => r.json()).then((d) => d.transactions[0]?.id ?? null)`);
      if (auto) {
        await open(`/ledger?edit=${auto}`);
        await sleep(600);
        const marked = await browser.evaluate(`(() => { const link = [...document.querySelectorAll("dialog[open] .dl .ws-linkbtn")].find((b) => /Tandai|Mark/.test(b.textContent)); link?.focus(); return Boolean(link); })()`);
        await browser.key(...ENTER);
        await sleep(1500);
        const toast = await browser.evaluate(`document.querySelector(".ws-toast")?.textContent ?? ""`);
        await browser.key(...ESC);
        await sleep(900);
        await focusSelector(".tx-list [data-row]");
        await browser.key(...char("z"));
        await sleep(1800);
        const status = await browser.evaluate(`fetch("/api/transactions/${auto}").then((r) => r.json()).then((d) => d.transaction.review_status)`);
        record(10, "Tandai sudah dicek shows its toast and Z restores the row to not checked", cell, marked && /dicek|checked/i.test(toast) && status === "auto", `${toast.slice(0, 40)} → ${status}`);
      } else {
        record(10, "an automatic row to check", cell, false, "none in the seed");
      }

      // 11: delete asks first.
      await open("/ledger?period=cycle&type=expense");
      await focusSelector(".tx-list [data-row]");
      await browser.key(...ENTER);
      await sleep(1200);
      await browser.evaluate(`document.querySelector("dialog[open] .dlg-f button")?.focus()`);
      await browser.key(...ENTER);
      await sleep(500);
      const cancelFocused = await browser.evaluate(`/^(Batal|Cancel)$/.test(document.activeElement?.textContent?.trim() ?? "")`);
      await browser.key(...ESC);
      await sleep(800);
      record(11, "Hapus asks first with Batal focused, and Esc closes without deleting", cell, cancelFocused && (await dialogs()).length === 0);

      // 13: the return=review deep link.
      const waiting = await browser.evaluate(`fetch("/api/transactions?waiting=true&limit=1").then((r) => r.json()).then((d) => d.transactions[0]?.id ?? null)`);
      await open(`/ledger?edit=${waiting}&return=review&folder=jawab`);
      // Tinjau may move the item to its own folder once it loads, so the navigation itself is what is checked.
      await browser.evaluate(`window.__pushed = []; const push = history.pushState.bind(history); history.pushState = (state, title, target) => { window.__pushed.push(String(target)); return push(state, title, target); }`);
      await browser.key(...ESC);
      await sleep(1500);
      const pushed = await browser.evaluate("window.__pushed ?? []");
      record(13, "Esc on a dialog opened from Tinjau returns to the same item and folder", cell, pushed.some((target) => target.endsWith(`/review?item=${waiting}&folder=jawab`)), JSON.stringify(pushed));

      // 14: the single-key switch.
      await open("/ledger?period=cycle");
      await browser.evaluate(`localStorage.setItem("shortcuts", "off")`);
      await open("/ledger?period=cycle");
      await focusSelector(".tx-list [data-row]");
      const still = (await active()).row;
      await browser.key(...char("j"));
      await sleep(300);
      record(14, "J does nothing with single keys off", cell, (await active()).row === still, still);
      await browser.evaluate(`localStorage.removeItem("shortcuts")`);

      // 15: nothing focused under the top bar or a sticky day heading.
      await open("/ledger?period=cycle");
      await focusSelector(".tx-list [data-row]");
      let covered = 0;
      for (let index = 0; index < 20; index += 1) {
        await browser.key(...char("j"));
        await sleep(120);
        covered += await browser.evaluate(`(() => { const row = document.activeElement.closest("[data-row]"); const top = row.getBoundingClientRect().top; const heading = [...document.querySelectorAll(".day-h")].map((h) => h.getBoundingClientRect()).filter((r) => r.top <= 120 && r.bottom > 0).map((r) => r.bottom); const limit = Math.max(56, ...heading); return top < limit - 1 ? 1 : 0; })()`);
      }
      record(15, "no focused row is covered by the top bar or a day heading", cell, covered === 0, `${covered} covered`);
    } else {
      // 12: the phone sheet.
      await open("/ledger?period=cycle");
      await focusSelector(".tx-list [data-row]");
      const row = (await active()).row;
      await browser.key(...ENTER);
      await sleep(1200);
      const pushed = await browser.evaluate(`Boolean(history.state?.ledgerSheet)`);
      const sheet = await browser.evaluate(`(() => { const d = document.querySelector("dialog[open]"); return d ? Math.round(d.getBoundingClientRect().width) : 0; })()`);
      await browser.evaluate("history.back()");
      await sleep(1200);
      record(12, "a row opens a bottom sheet with a history entry; back closes it with focus on the row", cell,
        pushed && sheet === width && (await dialogs()).length === 0 && (await active())?.row === row, `${row} sheet ${sheet}`);
      // 2 on the phone: J/K still walk the rows.
      await focusSelector(".tx-list [data-row]");
      await browser.key(...char("j"));
      await sleep(200);
      record(2, "J moves to the next row on the phone", cell, (await active())?.row !== row, (await active())?.row);
    }
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "transaksi-keys.json"), JSON.stringify(results, null, 1));
console.log(`${results.filter((result) => result.pass).length}/${results.length} passed`);
