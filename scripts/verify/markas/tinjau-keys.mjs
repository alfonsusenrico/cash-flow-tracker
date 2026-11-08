// Keyboard flows of Tinjau (apply-markas-tinjau D22) on a running app, in the account's language: the Tab order,
// J/K, an answer by 1 with its toast and Z, E and T, the "…" menu, the category dialog, typing in "Jawab sendiri",
// the phone sheet, a deep link, the single-key switch and focus under the top bar. Every answer it gives is undone
// by Z before the next flow. Writes tinjau-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node tinjau-keys.mjs --origin http://127.0.0.1:3621 --out <dir> [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 Tab order: skip link, sidebar, top bar, …, rail, list, item",
  "2 J/K from the list and from the item, focus and URL",
  "3 1 on a group, the toast, Z, the restored focus",
  "4 E and T",
  "5 the … menu with arrows and Esc",
  "6 the category dialog: open, Esc with focus return, save",
  "7 Jawab sendiri with 1 kopi typed and no answer pressed",
  "8 the phone sheet: open, 1 inside, back with focus on the row",
  "9 a deep link ?item=&folder=pindah",
  "10 the single-key switch off",
  "11 no focused element under the top bar",
];

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9668), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const DOWN = ["ArrowDown", "ArrowDown", 40];
const char = (value) => [value, value >= "0" && value <= "9" ? `Digit${value}` : `Key${value.toUpperCase()}`, value.toUpperCase().charCodeAt(0), 0, value];
const type = async (text) => {
  for (const value of text) await browser.key(...(value === " " ? [" ", "Space", 32, 0, " "] : char(value)));
};

const heading = () => browser.evaluate(`document.getElementById("triage-item-heading")?.textContent ?? null`);
const headingFocused = () => browser.evaluate(`document.activeElement?.id === "triage-item-heading"`);
const toast = () => browser.evaluate(`document.querySelector(".ws-toast")?.textContent ?? ""`);
const focusHeading = () => browser.evaluate(`document.getElementById("triage-item-heading")?.focus()`);
const openDialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].map((dialog) => dialog.className)`);
async function open(url) {
  await browser.navigate(args.origin + url);
  await browser.settle();
  await sleep(600);
}
async function undo() {
  await focusHeading();
  await browser.key(...char("z"));
  await sleep(1500);
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
      // 1 and 11: the Tab order from the start of the page, and every stop clear of the sticky top bar.
      await open("/review");
      await browser.evaluate(`document.activeElement?.blur(); window.scrollTo(0, 0)`);
      const stops = [];
      let covered = 0;
      for (let step = 0; step < 70; step += 1) {
        await browser.key(...TAB);
        const stop = await browser.evaluate(`(() => {
          const element = document.activeElement;
          if (!element || element === document.body) return null;
          const top = element.getBoundingClientRect().top;
          const bar = document.querySelector(".topbar")?.getBoundingClientRect().bottom ?? 0;
          const area = element.closest(".skip") ? "skip" : element.closest(".sb") ? "sidebar" : element.closest(".topbar") ? "topbar"
            : element.closest(".tb-actions") ? "more" : element.closest(".rail") ? "rail" : element.closest(".tj-list") ? "list"
            : element.closest(".tj-detail") ? "item" : "other";
          return { area, covered: top < bar - 0.5 && !element.closest(".topbar, .sb, .skip") };
        })()`);
        if (!stop) continue;
        if (stop.covered) covered += 1;
        if (stops[stops.length - 1] !== stop.area) stops.push(stop.area);
        if (stop.area === "item") break;
      }
      const expected = ["skip", "sidebar", "topbar", "more", "rail", "list", "item"];
      record(1, "Tab order", cell, JSON.stringify(stops.filter((area) => area !== "other")) === JSON.stringify(expected), stops.join(" → "));
      record(11, "no focused element under the top bar", cell, covered === 0, `${covered} covered`);

      // 5: the "…" menu.
      await browser.evaluate(`document.querySelector(".tb-actions button[aria-haspopup=menu]").focus()`);
      await browser.key(...ENTER);
      await sleep(400);
      const first = await browser.evaluate(`document.activeElement?.getAttribute("role")`);
      await browser.key(...DOWN);
      const items = await browser.evaluate(`[...document.querySelectorAll("[role=menu] [role=menuitem]")].indexOf(document.activeElement)`);
      await browser.key(...ESC);
      await sleep(300);
      const back = await browser.evaluate(`document.activeElement?.getAttribute("aria-haspopup") === "menu" && !document.querySelector("[role=menu]")`);
      record(5, "Enter opens the menu, ↓ moves, Esc closes it back on …", cell, first === "menuitem" && items === 1 && back, `${first} ${items} ${back}`);
    }

    // 2: J/K from the list and from the item.
    await open("/review");
    await browser.evaluate(`document.querySelector(".tj-list [data-row]").focus()`);
    const before = await browser.evaluate(`location.search`);
    await browser.key(...char("j"));
    await sleep(500);
    const rowFocus = await browser.evaluate(`[...document.querySelectorAll(".tj-list [data-row]")].indexOf(document.activeElement)`);
    const after = await browser.evaluate(`location.search`);
    record(2, "J from the list focuses the next row", cell, rowFocus === 1 && (mobile || after !== before), `row ${rowFocus}, ${before} → ${after}`);
    if (!mobile) {
      await focusHeading();
      const was = await browser.evaluate(`location.search`);
      await browser.key(...char("k"));
      await sleep(500);
      const now = await browser.evaluate(`location.search`);
      record(2, "K from the item moves to the previous item with its heading focused", cell, now !== was && (await headingFocused()), `${was} → ${now}`);
    }

    if (!mobile) {
      // 3: 1 on a group, the toast, then Z.
      await open("/review?folder=jawab");
      await focusHeading();
      const group = await heading();
      await browser.key(...char("1"));
      await sleep(1500);
      const saved = await toast();
      record(3, "1 accepts the group and the toast offers Urungkan", cell, saved.length > 0 && (await heading()) !== group, saved.slice(0, 80));
      await undo();
      record(3, "Z restores the group with its heading focused", cell, (await heading()) === group && (await headingFocused()), await heading());

      // 4: E on an item without a suggested answer names it; T on the learned top-up converts, then Z.
      await open("/review?folder=pindah");
      await browser.evaluate(`[...document.querySelectorAll(".tj-list [data-row]")].find((row) => /750/.test(row.textContent) && /Utama/.test(row.textContent))?.click()`);
      await sleep(600);
      const itemOf = () => browser.evaluate(`new URLSearchParams(location.search).get("item")`);
      const topup = await itemOf();
      await focusHeading();
      await browser.key(...char("t"));
      await sleep(1500);
      record(4, "T converts the learned top-up", cell, (await toast()).length > 0 && (await itemOf()) !== topup, (await toast()).slice(0, 80));
      await undo();
      record(4, "Z brings the debit back", cell, (await itemOf()) === topup, topup);
      await browser.evaluate(`[...document.querySelectorAll(".tj-list [data-row]")].find((row) => /Dana Darurat/.test(row.textContent))?.click()`);
      await sleep(600);
      await focusHeading();
      await browser.key(...char("e"));
      await sleep(400);
      const notice = await browser.evaluate(`document.querySelector(".tj-status")?.textContent ?? ""`);
      record(4, "E without a suggested answer says so", cell, notice.length > 0, notice);

      // 6: the category dialog.
      await open("/review?folder=jawab");
      await focusHeading();
      await browser.key(...char("2"));
      await sleep(600);
      const inside = await browser.evaluate(`Boolean(document.querySelector("dialog[open]")?.contains(document.activeElement))`);
      await browser.key(...ESC);
      await sleep(400);
      const returned = await browser.evaluate(`document.activeElement?.dataset.choice === "2" && !document.querySelector("dialog[open]")`);
      record(6, "2 opens the dialog with focus inside; Esc returns focus to the answer", cell, inside && returned, `${inside} ${returned}`);
      const group6 = await heading();
      await browser.key(...ENTER);
      await sleep(600);
      await browser.evaluate(`(() => {
        const select = document.querySelector("dialog[open] select");
        select.value = select.options[1].value;
        select.dispatchEvent(new Event("change", { bubbles: true }));
        document.querySelector("dialog[open] button[type=submit]").focus();
      })()`);
      await browser.key(...ENTER);
      await sleep(1500);
      record(6, "Save sends it and closes the dialog", cell, (await openDialogs()).length === 0 && (await toast()).length > 0, (await toast()).slice(0, 80));
      await undo();
      record(6, "Z undoes the saved category", cell, (await heading()) === group6, await heading());

      // 7: typing an answer presses nothing.
      await open("/review?folder=apa");
      const question = await heading();
      await browser.evaluate(`document.querySelector(".tj-after button").focus()`);
      await browser.key(...ENTER);
      await sleep(600);
      await type("1 kopi");
      await sleep(300);
      const value = await browser.evaluate(`document.querySelector("dialog[open] input")?.value`);
      record(7, "\"1 kopi\" stays in the field and presses no answer", cell, value === "1 kopi" && (await heading()) === question && (await toast()) === "", value);
      await browser.key(...ESC);
      await sleep(300);

      // 9: a deep link.
      const id = await browser.evaluate(`fetch("/api/review/queue").then((r) => r.json()).then((queue) => queue.awaiting[0].id)`);
      await open(`/review?item=${id}&folder=pindah`);
      await sleep(400);
      const current = await browser.evaluate(`document.querySelector(".rail-item[aria-current=true] .lbl")?.textContent`);
      record(9, "a deep link selects its item in its folder with the heading focused", cell, (await headingFocused()) && Boolean(current), `${current}: ${await heading()}`);

      // 10: the single-key switch.
      await browser.evaluate(`localStorage.setItem("shortcuts", "off")`);
      await open("/review");
      await focusHeading();
      const still = await heading();
      await browser.key(...char("j"));
      await sleep(400);
      record(10, "J does nothing with single keys off", cell, (await heading()) === still, still);
      await browser.evaluate(`localStorage.removeItem("shortcuts")`);
    } else {
      // 8: the phone sheet.
      await open("/review?folder=jawab");
      const name = await browser.evaluate(`(() => { const row = document.querySelector(".tj-list [data-row]"); row.click(); return row.dataset.row; })()`);
      await sleep(800);
      const sheet = await browser.evaluate(`document.querySelector("dialog.is-full[open]") !== null`);
      record(8, "a row opens the full-height sheet with the heading focused", cell, sheet && (await headingFocused()), name);
      const was = await heading();
      await browser.key(...char("1"));
      await sleep(1500);
      record(8, "1 inside the sheet answers and shows the next item", cell, (await heading()) !== was, await heading());
      await undo();
      await browser.evaluate(`history.back()`);
      await sleep(800);
      const focus = await browser.evaluate(`document.activeElement?.dataset.row ?? null`);
      record(8, "back closes the sheet with focus on a row", cell, (await openDialogs()).length === 0 && Boolean(focus), focus);
    }
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "tinjau-keys.json"), JSON.stringify(results, null, 1));
console.log(`${results.filter((result) => result.pass).length}/${results.length} passed`);
