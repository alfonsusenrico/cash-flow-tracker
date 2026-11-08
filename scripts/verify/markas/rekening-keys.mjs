// Keyboard flows of Rekening & Kantong (apply-markas-rekening D22) on a running app, in the account's language.
// Writes: the saved order (flows 7 and 8), restored to the layouts read at the start. "Periksa" only reads.
// Writes rekening-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node rekening-keys.mjs --origin <url> --out <dir> [--port 9760] [--dry-run 1]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const FLOWS = [
  "1 Tab order: the two actions, …, Rincian, the two segments, the first tile",
  "2 Rincian: Enter opens, Esc closes, focus back on Rincian",
  "3 a tile: Enter opens the detail, Tab stays inside, Lainnya by ArrowDown and Esc, Esc closes with focus on the tile",
  "4 a credit tile: a bill expands with Enter, Lihat di Transaksi reachable, Bayar tagihan opens Perpindahan with the amount",
  "5 Kartu → Grafik by keyboard, a legend use by Space, a product bar opens its detail",
  "6 Per kegunaan → Per rekening and back",
  "7 Atur urutan: … › Atur urutan, Alt + ArrowRight on a tile, Alt + ArrowDown on a handle, the live message, Selesai, reload keeps the order",
  "8 the phone reorder with move buttons at 390",
  "9 the account form: Jenis by arrows, the credit days, day 32 refused under the field, Esc",
  "10 Jual: the over-sale error on the units field, Batal returns focus",
  "11 Cocokkan saldo: type, Periksa, the result announced, Sesuaikan saldo opens with the amount",
  "12 Perbarui harga and its toast announced",
  "13 the Tagihan row reaches /goals",
  "14 no focused element under the top bar or the reorder bar",
];

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (args["dry-run"]) {
  FLOWS.forEach((flow) => console.log(flow));
  process.exit(0);
}
mkdirSync(args.out, { recursive: true });
const browser = await launch({ port: Number(args.port ?? 9760), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (flow, check, cell, pass, detail) => {
  results.push({ flow, check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, `[${flow}]`, check, detail ?? "");
};
const ALT = 1;
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const SPACE = [" ", "Space", 32, 0, " "];
const DOWN = ["ArrowDown", "ArrowDown", 40];
const RIGHT = ["ArrowRight", "ArrowRight", 39];
const LEFT = ["ArrowLeft", "ArrowLeft", 37];
const key = (spec, modifiers = 0) => browser.key(spec[0], spec[1], spec[2], modifiers || spec[3] || 0, spec[4]);
const typeText = async (text) => { for (const character of text) await browser.key(character, `Digit${character}`, character.charCodeAt(0), 0, character); };
const active = () => browser.evaluate(`(() => { const e = document.activeElement; if (!e) return null; const r = e.getBoundingClientRect(); return { id: e.id, tag: e.tagName, label: (e.getAttribute("aria-label") || e.textContent || "").trim().slice(0, 70), inDialog: Boolean(e.closest("dialog[open]")), menu: Boolean(e.closest("[role=menu]")), top: Math.round(r.top), expanded: e.getAttribute("aria-expanded"), pressed: e.getAttribute("aria-pressed") }; })()`);
const dialogs = () => browser.evaluate(`[...document.querySelectorAll("dialog[open]")].map((dialog) => dialog.querySelector("h2")?.textContent ?? "")`);
const focusOn = (expression) => browser.evaluate(`(() => { const e = ${expression}; if (!e) return false; e.scrollIntoView({ block: "center" }); e.focus(); return document.activeElement === e; })()`);
const byText = (selector, pattern) => `[...document.querySelectorAll(${JSON.stringify(selector)})].find((e) => new RegExp(${JSON.stringify(pattern)}).test((e.getAttribute("aria-label") || e.textContent || "").trim()))`;
const tileOf = (name) => `(() => { const row = [...document.querySelectorAll("[data-account-row]")].find((r) => r.querySelector(".tl-name")?.textContent === ${JSON.stringify(name)}); return row && (row.matches("button") ? row : row.querySelector(".tl-open")); })()`;
const tileNames = (group) => browser.evaluate(`[...document.querySelectorAll('section[data-group="${group}"] [data-account-row] .tl-name')].map((e) => e.textContent)`);
const groupOrder = () => browser.evaluate(`[...document.querySelectorAll("section[data-group]")].map((s) => s.dataset.group)`);
const status = () => browser.evaluate(`[...document.querySelectorAll("[role=status]")].map((e) => e.textContent).join(" | ")`);
async function open(target, grouping = "use") {
  await browser.evaluate(`localStorage.setItem("rekening-grouping", "${grouping}")`);
  await browser.navigate(args.origin + target);
  await browser.settle();
  await sleep(1200);
}
const api = (method, url, body) => browser.evaluate(`fetch("/api${url}", { method: "${method}", credentials: "include", headers: { "Content-Type": "application/json" }${body ? `, body: JSON.stringify(${JSON.stringify(body)})` : ""} }).then((r) => r.json())`);

let saved = {};
try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "rekening-contoh-123" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  saved = { use: await api("GET", "/accounts/layout?grouping=use"), rekening: await api("GET", "/accounts/layout?grouping=rekening") };

  for (const [width, height, mobile] of [[1440, 900, false], [390, 844, true]]) {
    const cell = `${width}`;
    await browser.setCell({ width, height, theme: "light", mobile });
    await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("hideBalances")`);

    if (!mobile) {
      // 1
      await open("/accounts");
      await browser.evaluate("document.activeElement?.blur(); window.scrollTo(0, 0)");
      const stops = [];
      for (let index = 0; index < 40; index += 1) {
        await key(TAB);
        stops.push(await browser.evaluate(`(() => { const e = document.activeElement; if (e.closest(".tb-actions")) return e.getAttribute("aria-haspopup") ? "more" : "action"; if (e.closest(".hero")) return "rincian"; if (e.closest(".rk-toolbar")) return "segment"; if (e.closest("[data-account-row]")) return "tile"; return "other"; })()`));
      }
      const order = ["action", "more", "rincian", "segment", "tile"].map((name) => stops.indexOf(name));
      record(1, "actions, …, Rincian, segments and the first tile come in order", cell, order.every((index, position) => index >= 0 && (position === 0 || index > order[position - 1])), JSON.stringify(order));
      record(1, "two visible actions and four segments are tab stops", cell, stops.filter((stop) => stop === "action").length === 2 && stops.filter((stop) => stop === "segment").length === 4, JSON.stringify(stops.slice(0, 16)));

      // 2
      await focusOn(byText(".hero button", "^(Rincian|Breakdown)$"));
      await key(ENTER);
      await sleep(600);
      const rincianOpen = (await dialogs()).length === 1;
      await key(ESC);
      await sleep(500);
      const back = await active();
      record(2, "Rincian opens and Esc returns focus to it", cell, rincianOpen && /^(Rincian|Breakdown)$/.test(back?.label ?? "") && (await dialogs()).length === 0, JSON.stringify(back));

      // 3
      await focusOn(tileOf("GoPay"));
      await key(ENTER);
      await sleep(900);
      const detail = await dialogs();
      let inside = true;
      for (let index = 0; index < 25; index += 1) {
        await key(TAB);
        if (!(await active())?.inDialog) inside = false;
      }
      record(3, "Enter opens the detail and Tab stays inside", cell, detail.includes("GoPay") && inside, JSON.stringify(detail));
      await focusOn(`document.querySelector("dialog[open] .dlg-f [aria-haspopup=menu]")`);
      await key(DOWN);
      await sleep(400);
      const inMenu = await active();
      await key(DOWN);
      await key(ESC);
      await sleep(300);
      const afterMenu = await active();
      record(3, "Lainnya opens by ArrowDown and Esc returns to it", cell, Boolean(inMenu?.menu) && /^(Lainnya|More)/.test(afterMenu?.label ?? ""), `${inMenu?.label} → ${afterMenu?.label}`);
      await key(ESC);
      await sleep(600);
      const onTile = await active();
      record(3, "Esc closes the detail with focus on the tile", cell, (await dialogs()).length === 0 && /GoPay/.test(onTile?.label ?? "") , JSON.stringify(onTile));

      // 4
      await focusOn(tileOf("SPayLater"));
      await key(ENTER);
      await sleep(1500);
      await focusOn(`document.querySelector("dialog[open] .row.is-line")`);
      await key(ENTER);
      await sleep(300);
      const bill = await active();
      const ledgerLink = await browser.evaluate(`Boolean(document.querySelector("dialog[open] a[href^='/ledger?account=']"))`);
      record(4, "a bill expands with Enter and Lihat di Transaksi is a link", cell, bill?.expanded === "true" && ledgerLink, JSON.stringify(bill));
      await focusOn(byText("dialog[open] .dlg-f button", "^(Bayar tagihan|Pay bill)$"));
      await key(ENTER);
      await sleep(1500);
      const capture = await browser.evaluate(`(() => { const d = [...document.querySelectorAll("[role=dialog], dialog[open]")].find((x) => x.getBoundingClientRect().width > 0 && /Catat|Record/.test(x.textContent)); const amount = d?.querySelector("input[inputmode=numeric], input[name=amount]")?.value ?? ""; const pressed = d ? [...d.querySelectorAll("[aria-pressed=true]")].map((b) => b.textContent.trim()) : []; return { open: Boolean(d), amount, pressed }; })()`);
      record(4, "Bayar tagihan opens Perpindahan with the amount", cell, capture.open && capture.amount.replace(/\D/g, "") === "434000", JSON.stringify(capture));
      await key(ESC);
      await sleep(600);

      // 5
      await open("/accounts");
      await focusOn(byText(".rk-toolbar button", "^(Grafik|Chart)$"));
      await key(ENTER);
      await sleep(900);
      const onChart = await browser.evaluate("location.search");
      await focusOn(byText(".rk-legend button", "^(Tabungan|Savings)"));
      await key(SPACE);
      await sleep(400);
      const pressed = await active();
      await focusOn(byText(".rk-legend button", "^(Investasi|Investments)"));
      await key(SPACE);
      await sleep(400);
      await focusOn(`document.querySelector("button.hbar")`);
      await key(ENTER);
      await sleep(900);
      record(5, "Grafik by keyboard, Space chooses a use, a bar opens its detail", cell, onChart === "?tampilan=grafik" && pressed?.pressed === "true" && (await dialogs()).length === 1, `${onChart} ${pressed?.pressed} ${JSON.stringify(await dialogs())}`);
      await key(ESC);
      await sleep(500);

      // 6
      await open("/accounts");
      await focusOn(byText(".rk-toolbar button", "^(Per rekening|By account)$"));
      await key(ENTER);
      await sleep(600);
      const perAccount = await browser.evaluate(`document.querySelectorAll("section.parent-block").length`);
      await focusOn(byText(".rk-toolbar button", "^(Per kegunaan|By use)$"));
      await key(ENTER);
      await sleep(600);
      record(6, "Per rekening and back by Enter", cell, perAccount > 3 && (await groupOrder())[0] !== undefined && (await browser.evaluate(`document.querySelectorAll("section.parent-block").length`)) === 0, `${perAccount}`);

      // 7 and 14
      await open("/accounts");
      await focusOn(`document.querySelector(".tb-actions [aria-haspopup=menu]")`);
      await key(ENTER);
      await sleep(400);
      await focusOn(byText("[role=menu] [role=menuitem]", "^(Atur urutan|Arrange)$"));
      await key(ENTER);
      await sleep(600);
      const before = await tileNames("harian");
      await focusOn(`document.getElementById([...document.querySelectorAll('section[data-group="harian"] [data-account-row]')][1]?.querySelector(".tl-open")?.id)`);
      const moving = (await active())?.label;
      await key(RIGHT, ALT);
      await sleep(900);
      const afterMove = await tileNames("harian");
      const stayed = await active();
      const message = await status();
      record(7, "Alt + ArrowRight moves the tile, keeps focus and announces it", cell, afterMove[2] === before[1] && stayed?.label === moving && /2|3/.test(message), `${moving}: ${JSON.stringify(afterMove.slice(0, 4))} ${message}`);
      record(14, "the focused tile is below the top bar and the reorder bar", cell, (stayed?.top ?? 0) >= 104, JSON.stringify(stayed));
      const groupsBefore = await groupOrder();
      await focusOn(`document.querySelector("#rk-handle-${groupsBefore[0]}")`);
      await key(DOWN, ALT);
      await sleep(900);
      const groupsAfter = await groupOrder();
      record(7, "Alt + ArrowDown on a handle moves the row", cell, groupsAfter[1] === groupsBefore[0], JSON.stringify(groupsAfter));
      await focusOn(byText(".rk-sel-bar button", "^(Selesai|Done)$"));
      await key(ENTER);
      await sleep(400);
      await open("/accounts");
      record(7, "Selesai leaves the mode and a reload keeps the order", cell, JSON.stringify(await groupOrder()) === JSON.stringify(groupsAfter) && JSON.stringify(await tileNames("harian")) === JSON.stringify(afterMove) && !(await browser.evaluate(`Boolean(document.querySelector(".rk-sel-bar"))`)), "");
      await api("PUT", "/accounts/layout?grouping=use", saved.use);

      // 9
      await open("/accounts");
      await focusOn(byText(".tb-actions button", "^(Tambah rekening|Add account)$"));
      await key(ENTER);
      await sleep(800);
      await focusOn(`document.querySelector("dialog[open] [role=radiogroup] [role=radio][aria-checked=true]")`);
      for (let index = 0; index < 4; index += 1) await key(RIGHT);
      await sleep(400);
      const credit = await active();
      const days = await browser.evaluate(`[...document.querySelectorAll("dialog[open] label")].map((l) => l.textContent).filter((t) => /tiap tgl|on day/.test(t)).length`);
      const dayField = await browser.evaluate(`(() => { const l = [...document.querySelectorAll("dialog[open] label")].find((x) => /Jatuh tempo tiap tgl|Due on day/.test(x.textContent)); return l?.htmlFor || null; })()`);
      if (dayField) {
        await focusOn(`document.getElementById(${JSON.stringify(dayField)})`);
        await typeText("32");
        await focusOn(`document.querySelector("dialog[open] [name=account-name], dialog[open] #account-name")`);
        await typeText("9");
        await focusOn(`[...document.querySelectorAll("dialog[open] .dlg-f button")].pop()`);
        await key(ENTER);
        await sleep(800);
      }
      const refused = await browser.evaluate(`(() => { const f = document.getElementById(${JSON.stringify(dayField)}); return { invalid: f?.getAttribute("aria-invalid"), error: f ? document.getElementById((f.getAttribute("aria-describedby") || "").split(" ").find((id) => /err/.test(id)) || "")?.textContent ?? [...(f.closest(".field")?.querySelectorAll(".err") ?? [])].map((e) => e.textContent).join(" ") : null, open: Boolean(document.querySelector("dialog[open]")) }; })()`);
      record(9, "Jenis by arrows reaches Kredit with both days, and 32 is refused under the field", cell, /Kredit|Credit/.test(credit?.label ?? "") && days === 2 && refused.invalid === "true" && refused.open, `${credit?.label} ${days} ${JSON.stringify(refused)}`);
      await key(ESC);
      await sleep(600);
      record(9, "Esc closes the form", cell, (await dialogs()).length === 0, "");

      // 10
      await open("/accounts");
      await focusOn(tileOf("Saham Bank Nusantara"));
      await key(ENTER);
      await sleep(1000);
      await focusOn(byText("dialog[open] .dlg-f button", "^(Jual|Sell)$"));
      await key(ENTER);
      await sleep(900);
      const units = await browser.evaluate(`(() => { const d = [...document.querySelectorAll("dialog[open]")].pop(); const input = d?.querySelector("input[inputmode=decimal], input[inputmode=numeric]"); input?.focus(); return input?.id ?? null; })()`);
      await typeText("99999");
      await focusOn(`[...[...document.querySelectorAll("dialog[open]")].pop().querySelectorAll(".dlg-f button")].pop()`);
      await key(ENTER);
      await sleep(900);
      const overSale = await browser.evaluate(`(() => { const f = document.getElementById(${JSON.stringify(units)}); return { invalid: f?.getAttribute("aria-invalid"), text: f?.closest(".field")?.querySelector(".err")?.textContent ?? null }; })()`);
      record(10, "the over-sale error sits on the units field", cell, overSale.invalid === "true" && Boolean(overSale.text), JSON.stringify(overSale));
      await focusOn(byText("dialog[open] .dlg-f button", "^(Batal|Cancel)$"));
      await key(ENTER);
      await sleep(700);
      const afterCancel = await active();
      record(10, "Batal returns focus to Jual", cell, /^(Jual|Sell)$/.test(afterCancel?.label ?? ""), JSON.stringify(afterCancel));
      await key(ESC);
      await sleep(500);

      // 11
      await open("/accounts");
      await focusOn(tileOf("GoPay"));
      await key(ENTER);
      await sleep(1000);
      await focusOn(byText("dialog[open] .dlg-f button", "^(Cocokkan saldo|Check balance)$"));
      await key(ENTER);
      await sleep(900);
      await browser.evaluate(`(() => { const d = [...document.querySelectorAll("dialog[open]")].pop(); d?.querySelector("input")?.focus(); })()`);
      await typeText("600000");
      await focusOn(byText("dialog[open] button", "^(Periksa|Check)$"));
      await key(ENTER);
      await sleep(1500);
      const result = await browser.evaluate(`(() => { const d = [...document.querySelectorAll("dialog[open]")].pop(); return { live: [...d.querySelectorAll("[role=status], [aria-live]")].map((e) => e.textContent).join(" ").slice(0, 120), adjust: Boolean([...d.querySelectorAll("button")].find((b) => /^(Sesuaikan saldo|Adjust balance)$/.test(b.textContent.trim()))) }; })()`);
      record(11, "Periksa announces the result and offers Sesuaikan saldo", cell, result.live.length > 0 && result.adjust, JSON.stringify(result));
      await focusOn(byText("dialog[open] .dlg-f button", "^(Sesuaikan saldo|Adjust balance)$"));
      await key(ENTER);
      await sleep(1000);
      const adjustValue = await browser.evaluate(`(() => { const d = [...document.querySelectorAll("dialog[open]")].pop(); return { title: d?.querySelector("h2")?.textContent, value: d?.querySelector("input")?.value }; })()`);
      record(11, "Sesuaikan saldo opens with the typed amount", cell, /Sesuaikan|Adjust/.test(adjustValue.title ?? "") && (adjustValue.value ?? "").replace(/\D/g, "") === "600000", JSON.stringify(adjustValue));
      for (let index = 0; index < 3; index += 1) { await key(ESC); await sleep(400); }

      // 12
      await open("/accounts");
      await focusOn(byText(".tb-actions button", "^(Perbarui harga|Update prices)$"));
      await key(ENTER);
      await sleep(3000);
      const toast = await browser.evaluate(`[...document.querySelectorAll("[role=status]")].map((e) => e.textContent).join(" | ")`);
      record(12, "Perbarui harga reports through a toast", cell, /Harga|Prices/.test(toast), toast.slice(0, 160));

      // 13
      await open("/accounts");
      await focusOn(`document.querySelector("a[href='/goals'].row")`);
      await key(ENTER);
      await sleep(2000);
      record(13, "the Tagihan row reaches /goals", cell, (await browser.evaluate("location.pathname")) === "/goals", "");
    } else {
      // 8
      await open("/accounts");
      await focusOn(`document.querySelector(".tb-actions [aria-haspopup=menu]")`);
      await key(ENTER);
      await sleep(500);
      await focusOn(byText("[role=menu] [role=menuitem]", "^(Atur urutan|Arrange)$"));
      await key(ENTER);
      await sleep(700);
      const disabled = await browser.evaluate(`document.querySelector('section[data-group="harian"] .rk-moves button')?.disabled`);
      const sizes = await browser.evaluate(`[...document.querySelectorAll(".rk-moves button")].slice(0, 4).map((b) => Math.round(b.getBoundingClientRect().height))`);
      const before = await tileNames("harian");
      await focusOn(`document.querySelectorAll('section[data-group="harian"] [data-account-row]')[0].querySelectorAll(".rk-moves button")[1]`);
      await key(ENTER);
      await sleep(900);
      const after = await tileNames("harian");
      const focusKept = await active();
      record(8, "move buttons are 44 px, the first is disabled, a move keeps focus", cell, disabled === true && sizes.every((size) => size >= 44) && after[1] === before[0] && /Geser|Move/.test(focusKept?.label ?? ""), `${sizes} ${JSON.stringify(after.slice(0, 3))} ${focusKept?.label}`);
      await api("PUT", "/accounts/layout?grouping=use", saved.use);
    }
  }
} finally {
  if (saved.use) await api("PUT", "/accounts/layout?grouping=use", saved.use).catch(() => {});
  if (saved.rekening) await api("PUT", "/accounts/layout?grouping=rekening", saved.rekening).catch(() => {});
  writeFileSync(path.join(args.out, "rekening-keys.json"), JSON.stringify({ origin: args.origin, results }, null, 1));
  const failed = results.filter((result) => !result.pass).length;
  console.log(`rekening keys: ${results.length - failed} passed, ${failed} failed`);
  browser.close();
}
