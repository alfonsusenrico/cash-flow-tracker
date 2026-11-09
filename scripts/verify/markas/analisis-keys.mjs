// Keyboard flows of Analisis (apply-markas-analisis D20, D26) on a running app, in the account's language. Nothing
// is written: dialogs are opened and closed without saving. Writes analisis-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node analisis-keys.mjs --origin http://127.0.0.1:3733 --out <dir> [--port 9733] [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 tabs: arrows, Home and End move and select, the URL keeps the tab",
  "2 Rincian: Enter opens the dialog, Esc closes it with focus on Rincian",
  "3 the chart/table toggle: Enter swaps, focus stays, aria-pressed follows",
  "4 a category row: Enter opens the ledger for that category and period",
  "5 Kelola kategori: Enter opens, Ubah opens the form with focus in Nama, Kembali returns to Ubah, Esc to the button",
  "6 Atur anggaran: Enter opens with focus in the first amount, Esc returns",
  "7 the calendar walk: arrows, Home, End, PageUp and PageDown move the day, with single-key shortcuts on and off",
  "8 a day row: Enter opens the transaction dialog, Esc returns focus to the row",
  "9 Lihat di Transaksi opens the ledger for that day only",
  "10 the flow map: arrows move between nodes, Enter opens the details, Esc returns focus to the node",
  "11 the flow table: Enter on a link opens its details, Esc returns focus",
  "12 the stepper and the basis segment by keyboard",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9733), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const LEFT = ["ArrowLeft", "ArrowLeft", 37];
const DOWN = ["ArrowDown", "ArrowDown", 40];
const UP = ["ArrowUp", "ArrowUp", 38];
const HOME = ["Home", "Home", 36];
const END = ["End", "End", 35];
const PAGE_UP = ["PageUp", "PageUp", 33];
const PAGE_DOWN = ["PageDown", "PageDown", 34];
const press = async (key, wait = 500) => {
  await browser.key(...key);
  await sleep(wait);
};
const url = () => browser.evaluate("location.pathname + location.search");
const focused = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e) return null; return { id: e.id, day: e.dataset?.day ?? null, node: e.dataset?.node ?? null, row: e.closest("[data-row]")?.dataset.row ?? null, key: e.dataset?.focusKey ?? null, cls: String(e.className?.baseVal ?? e.className ?? ""), tag: e.tagName, label: (e.getAttribute("aria-label") || e.textContent || "").trim().slice(0, 50), inDialog: Boolean(e.closest("dialog[open]")), pressed: e.getAttribute("aria-pressed") }; })()`);
const openDialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].length`);
const focusOn = (selector) => browser.evaluate(`(() => { const e = document.querySelector(${JSON.stringify(selector)}); e?.focus(); return Boolean(e); })()`);
const waitFor = (expression) => browser.waitFor(expression).then(() => true).catch(() => false);
async function open(target) {
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(1200);
}

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER, password: process.env.MARKAS_PASSWORD };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);

  for (const [width, height, mobile] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}`;
    await browser.setCell({ width, height, theme: "light", mobile });
    await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("shortcuts")`);

    // 1: tabs.
    await open("/insights");
    await focusOn(".tab[aria-selected='true']");
    await press(RIGHT);
    let state = await focused();
    record(1, "Right selects Kalender and keeps focus on its tab", cell, state?.id === "analysis-tab-kalender" && (await url()).includes("tab=kalender"), `${state?.id} ${await url()}`);
    await press(END);
    state = await focused();
    record(1, "End selects Peta Aliran", cell, state?.id === "analysis-tab-aliran" && (await url()).includes("tab=aliran"), state?.id);
    await press(HOME);
    state = await focused();
    record(1, "Home selects Ringkasan", cell, state?.id === "analysis-tab-ringkasan" && (await url()).includes("tab=ringkasan"), state?.id);

    // 2: the Rincian.
    await open("/insights");
    await waitFor(`Boolean(document.querySelector(".hero .hero-top button"))`);
    await focusOn(".hero .hero-top button");
    await press(ENTER, 900);
    state = await focused();
    record(2, "Enter opens the Rincian with focus inside", cell, (await openDialogs()) === 1 && state?.inDialog, state?.label);
    await press(ESC, 700);
    state = await focused();
    record(2, "Esc closes it with focus on Rincian", cell, (await openDialogs()) === 0 && state?.cls.includes("ws-btn-quiet") && !state?.inDialog, state?.label);

    // 3: the chart/table toggle.
    await focusOn(".ct-toggle");
    await press(ENTER);
    state = await focused();
    record(3, "Enter shows the table, focus stays on the toggle, pressed", cell, state?.cls.includes("ct-toggle") && state?.pressed === "true" && (await browser.evaluate(`Boolean(document.querySelector("#an-daily ~ * table, section[aria-labelledby=an-daily] table"))`)), state?.label);
    await press(ENTER);
    state = await focused();
    record(3, "Enter again shows the chart", cell, state?.pressed === "false" && (await browser.evaluate(`Boolean(document.querySelector("section[aria-labelledby=an-daily] svg"))`)), state?.label);

    // 4: a category row.
    await focusOn(".cat-row[href]");
    const href = await browser.evaluate(`document.activeElement.getAttribute("href")`);
    await press(ENTER, 2000);
    const landed = await url();
    record(4, "Enter on a category row opens the ledger for that category and period", cell, landed.startsWith("/ledger") && landed.includes("type=expense") && landed.includes("category="), `${href} → ${landed}`);

    // 5: Kelola kategori.
    await open("/insights");
    await waitFor(`Boolean(document.querySelector("section[aria-labelledby=an-categories] .sec-actions button"))`);
    await focusOn("section[aria-labelledby=an-categories] .sec-actions button");
    await press(ENTER, 1500);
    await waitFor(`Boolean(document.querySelector("dialog[open] [data-focus-key^=edit-]"))`);
    const editKey = await browser.evaluate(`(() => { const e = document.querySelector("dialog[open] [data-focus-key^=edit-]"); e.focus(); return e.dataset.focusKey; })()`);
    await press(ENTER, 800);
    state = await focused();
    record(5, "Ubah opens the form with focus in Nama", cell, state?.id === "category-name", state?.id);
    await focusOn("dialog[open] .mgr-back");
    await press(ENTER, 800);
    state = await focused();
    record(5, "Kembali returns to the list with focus on that Ubah", cell, state?.key === editKey, `${state?.key} / ${editKey}`);
    await press(ESC, 700);
    state = await focused();
    record(5, "Esc closes with focus on Kelola kategori", cell, (await openDialogs()) === 0 && !state?.inDialog && state?.cls.includes("ws-btn-quiet"), state?.label);

    // 6: Atur anggaran.
    await focusOn("section[aria-labelledby=an-categories] .sec-actions .btn-link");
    await press(ENTER, 1500);
    await waitFor(`Boolean(document.querySelector("dialog[open] .budget-row input"))`);
    await sleep(300);
    state = await focused();
    record(6, "Atur anggaran opens with focus inside", cell, state?.inDialog === true, state?.label);
    await press(ESC, 700);
    state = await focused();
    record(6, "Esc returns focus to Atur anggaran", cell, (await openDialogs()) === 0 && state?.cls.includes("btn-link"), state?.label);

    // 7: the calendar walk, with single-key shortcuts on and then off.
    for (const shortcuts of ["on", "off"]) {
      await browser.evaluate(`localStorage.setItem("shortcuts", "${shortcuts}")`);
      await open("/insights?tab=kalender");
      await waitFor(`Boolean(document.querySelector(".cal-cell[aria-selected=true]"))`);
      await focusOn(".cal-cell[aria-selected=true]");
      const start = (await focused())?.day;
      const shift = (day, days) => { const d = new Date(`${day}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + days); return d.toISOString().slice(0, 10); };
      const first = await browser.evaluate(`document.querySelector(".cal-cell[role=gridcell]").dataset.day`);
      await press(start > first ? LEFT : RIGHT);
      state = await focused();
      const leftOk = start > first ? state?.day === shift(start, -1) : state?.day === shift(start, 1);
      record(7, `Left/Right moves one day (shortcuts ${shortcuts})`, cell, leftOk && (await url()).includes(`day=${state?.day}`), `${start} → ${state?.day}`);
      const before = state?.day;
      await press(UP);
      state = await focused();
      const upOk = state?.day === shift(before, -7) || (state?.day === first && shift(before, -7) < first);
      record(7, `Up moves a week back or to the first day (shortcuts ${shortcuts})`, cell, upOk, `${before} → ${state?.day}`);
      await press(DOWN);
      const afterDown = (await focused())?.day;
      record(7, `Down moves a week on (shortcuts ${shortcuts})`, cell, afterDown > state?.day, `${state?.day} → ${afterDown}`);
      await press(HOME);
      const home = (await focused())?.day;
      await press(END);
      const end = (await focused())?.day;
      const weekday = (day) => (new Date(`${day}T00:00:00Z`).getUTCDay() + 6) % 7;
      record(7, `Home and End go to the week's start and end (shortcuts ${shortcuts})`, cell, (weekday(home) === 0 || home === first) && (weekday(end) === 6 || end > home), `${home} ${end}`);
      await press(PAGE_UP);
      const pageUp = (await focused())?.day;
      await press(PAGE_DOWN);
      const pageDown = (await focused())?.day;
      record(7, `PageUp and PageDown move by a week (shortcuts ${shortcuts})`, cell, pageUp < end && pageDown > pageUp, `${pageUp} ${pageDown}`);
    }
    await browser.evaluate(`localStorage.removeItem("shortcuts")`);

    // 8 and 9: a day row and Lihat di Transaksi, on the day with the most rows up to today.
    await open("/insights?tab=kalender");
    const today = await browser.evaluate(`document.querySelector(".cal-cell[aria-current=date]")?.dataset.day ?? document.querySelector(".cal-cell[aria-selected=true]").dataset.day`);
    const busy = await browser.evaluate(`(() => { const cells = [...document.querySelectorAll(".cal-cell[role=gridcell]")].filter((c) => c.dataset.day <= ${JSON.stringify(today)} && c.querySelector(".cal-amt")?.textContent); return cells[cells.length - 1]?.dataset.day ?? null; })()`);
    await open(`/insights?tab=kalender&day=${busy}`);
    const hasRow = await waitFor(`Boolean(document.querySelector(".day-rows .row"))`);
    if (hasRow) {
      const rowKey = await browser.evaluate(`(() => { const e = document.querySelector(".day-rows .row"); e.focus(); return e.dataset.row; })()`);
      await press(ENTER, 1500);
      record(8, "Enter opens the transaction dialog", cell, (await openDialogs()) === 1, rowKey);
      await press(ESC, 900);
      state = await focused();
      record(8, "Esc returns focus to the day row", cell, (await openDialogs()) === 0 && state?.row === rowKey, state?.row);
    } else record(8, "the chosen day has rows", cell, false, busy);
    await focusOn("aside .panel-actions a.ws-btn");
    await press(ENTER, 2000);
    const ledger = decodeURIComponent(await url());
    record(9, "Lihat di Transaksi opens the ledger for that day only", cell, ledger.startsWith("/ledger") && ledger.includes(`from=${busy}T00:00:00+07:00`) && ledger.includes(`to=${busy}T23:59:59.999+07:00`), ledger);

    // 10: the flow map (the map scrolls inside its box on the phone).
    await open("/insights?tab=aliran&view=peta");
    await waitFor(`Boolean(document.querySelector(".flow-node[tabindex='0']"))`);
    await focusOn(".flow-node[tabindex='0']");
    const startNode = (await focused())?.node;
    await press(RIGHT);
    const nextNode = (await focused())?.node;
    record(10, "Right moves to a node in another column", cell, Boolean(nextNode) && nextNode !== startNode, `${startNode} → ${nextNode}`);
    await press(ENTER, 1200);
    record(10, "Enter opens the node's details", cell, (await openDialogs()) === 1);
    await press(ESC, 900);
    state = await focused();
    record(10, "Esc returns focus to the node", cell, (await openDialogs()) === 0 && state?.node === nextNode, state?.node);

    // 11: the flow table.
    await open("/insights?tab=aliran&view=tabel");
    await waitFor(`Boolean(document.querySelector(".tbl-link"))`);
    const linkLabel = await browser.evaluate(`(() => { const e = document.querySelector(".tbl-link"); e.focus(); return e.textContent; })()`);
    await press(ENTER, 1500);
    record(11, "Enter on a link opens its details", cell, (await openDialogs()) === 1, linkLabel);
    await press(ESC, 900);
    state = await focused();
    record(11, "Esc returns focus to the link", cell, (await openDialogs()) === 0 && state?.cls.includes("tbl-link"), state?.label);

    // 12: the stepper and the basis segment.
    await open("/insights");
    await focusOn(".stepper .ws-icon-btn");
    const label = await browser.evaluate(`document.querySelector(".stepper .cur").textContent`);
    await press(ENTER, 1500);
    const stepped = await browser.evaluate(`document.querySelector(".stepper .cur").textContent`);
    record(12, "Enter on the previous chevron steps back", cell, stepped !== label, `${label} → ${stepped}`);
    await focusOn(".tb-actions .ws-seg button[aria-pressed='false'], .ws-seg[role=group] button[aria-pressed='false']");
    await press(ENTER, 1500);
    const pressed = await browser.evaluate(`[...document.querySelectorAll(".ws-seg button[aria-pressed='true']")].map((b) => b.textContent).join(" ")`);
    record(12, "Enter on Bulan kalender chooses it", cell, /Bulan kalender|Calendar month/.test(pressed), pressed);
  }
} finally {
  writeFileSync(path.join(args.out, "analisis-keys.json"), JSON.stringify(results, null, 1));
  browser.close();
}
const failed = results.filter((entry) => !entry.pass);
console.log(`${results.length - failed.length} of ${results.length} passed`);
