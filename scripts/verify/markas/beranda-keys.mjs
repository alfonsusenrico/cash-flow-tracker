// Keyboard flows of Beranda (apply-markas-beranda D14) on a running app: the Tab order from the hero through the
// panel, Rincian opened with Enter and closed with Esc and its close button with focus returned, a line opened
// with Enter and Space, a row opening the transaction dialog with focus returned, and no focused row under the
// sticky top bar or day heading. Writes beranda-keys.json.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node beranda-keys.mjs --origin http://127.0.0.1:3496 --out <dir>
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
const browser = await launch({ port: Number(args.port ?? 9658), profileDir: path.join(args.out, ".profile-keys") });
const results = [];
const record = (check, cell, pass, detail) => {
  results.push({ check, cell, pass, detail });
  console.log(pass ? "PASS" : "FAIL", cell, check, detail ?? "");
};
const TAB = ["Tab", "Tab", 9];
const ESC = ["Escape", "Escape", 27];
const ENTER = ["Enter", "Enter", 13];
const SPACE = [" ", "Space", 32, 0, " "];

// Where the focused element is: its region on Beranda and a short name.
const focused = () =>
  browser.evaluate(`(() => {
    const element = document.activeElement;
    if (!element || element === document.body) return null;
    const section = element.closest("section[aria-labelledby], aside[aria-label], dialog, [role=dialog]");
    const region = section ? (section.getAttribute("aria-label") || document.getElementById(section.getAttribute("aria-labelledby"))?.textContent || section.tagName) : "";
    const name = (element.getAttribute("aria-label") || element.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 48);
    return { region: region.trim(), name, row: element.classList.contains("row") };
  })()`);

async function open(url) {
  await browser.navigate(args.origin + url);
  await browser.settle();
  await sleep(400);
}

try {
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "raka_contoh", password: process.env.MARKAS_PASSWORD ?? "contoh-sandi-123" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);

  for (const [width, height, theme, mobile] of [[1440, 900, "light", false], [1440, 900, "dark", false], [390, 844, "light", true]]) {
    const cell = `${width}-${theme}`;
    await browser.setCell({ width, height, theme, mobile });
    await browser.evaluate(`localStorage.setItem("theme", "${theme}")`);
    await open("/");

    // Tab order: from main, through the hero, Perlu tindakan, Aktivitas and the panel.
    await browser.evaluate(`document.getElementById("main").focus()`);
    const stops = [];
    for (let step = 0; step < 40; step += 1) {
      await browser.key(...TAB);
      const stop = await focused();
      if (!stop || !stop.region) break;
      stops.push(stop);
      const covered = await browser.evaluate(`(() => {
        const element = document.activeElement;
        const box = element.getBoundingClientRect();
        const header = document.querySelector(${mobile ? '".ph-header"' : '".topbar"'}).getBoundingClientRect();
        const day = [...document.querySelectorAll(".day-h")].map((heading) => heading.getBoundingClientRect()).find((heading) => heading.top <= header.bottom + 1 && heading.bottom > header.bottom);
        const top = day ? day.bottom : header.bottom;
        return element.closest(".day-group") ? box.top < top - 0.5 : box.top < header.bottom - 0.5;
      })()`);
      if (covered) record("focused control under the sticky top bar or day heading", cell, false, stop.name);
    }
    const regions = stops.map((stop) => stop.region).filter((region, index, all) => all.indexOf(region) === index);
    record("Tab order hero → Perlu tindakan → Aktivitas terbaru → panel", cell,
      JSON.stringify(regions) === JSON.stringify(["Aman dipakai", "Perlu tindakan", "Aktivitas terbaru", "Ringkasan simpanan"]), regions.join(" → "));
    record("Rincian is the hero's first stop", cell, stops[0]?.name === "Rincian aman dipakai", stops[0]?.name);
    const names = stops.map((stop) => stop.name);
    // The header link precedes its rows, as in the prototype's DOM (sec-h, then the list).
    const buka = names.findIndex((name) => name.startsWith("Buka Tinjau"));
    const firstTodo = stops.findIndex((stop) => stop.region === "Perlu tindakan" && stop.row);
    record("Buka Tinjau, then the Perlu tindakan rows", cell, buka >= 0 && firstTodo > buka, `${buka} / ${firstTodo}`);
    const semua = names.indexOf("Semua transaksi");
    const lastActivity = stops.map((stop, index) => (stop.region === "Aktivitas terbaru" && stop.row ? index : -1)).reduce((a, b) => Math.max(a, b), -1);
    record("Semua transaksi follows the activity rows", cell, semua > lastActivity && lastActivity > 0, `${lastActivity} / ${semua}`);
    record("no focused control hides under a sticky bar", cell, !results.some((result) => result.cell === cell && result.check.startsWith("focused control under")), `${stops.length} stops`);

    // Rincian with Enter, a line with Enter and Space, Esc, then the close button.
    await open("/");
    await browser.evaluate(`document.querySelector('[aria-label="Rincian aman dipakai"]').focus()`);
    await browser.key(...ENTER);
    await sleep(500);
    const inside = await browser.evaluate(`Boolean(document.querySelector("dialog[open]")?.contains(document.activeElement))`);
    record("Enter opens Rincian with focus inside", cell, inside, (await focused())?.name);
    await browser.evaluate(`[...document.querySelectorAll("dialog[open] .row.is-line")].find((row) => row.textContent.startsWith("Tagihan kredit")).focus()`);
    await browser.key(...ENTER);
    await sleep(300);
    const expanded = await browser.evaluate(`document.activeElement.getAttribute("aria-expanded")`);
    await browser.key(...SPACE);
    await sleep(300);
    const collapsed = await browser.evaluate(`document.activeElement.getAttribute("aria-expanded")`);
    record("a line opens with Enter and closes with Space", cell, expanded === "true" && collapsed === "false", `${expanded} → ${collapsed}`);
    let trapped = true;
    for (let step = 0; step < 14; step += 1) {
      await browser.key(...TAB);
      trapped &&= await browser.evaluate(`Boolean(document.querySelector("dialog[open]")?.contains(document.activeElement))`);
    }
    record("Tab stays inside Rincian", cell, trapped);
    await browser.key(...ESC);
    await sleep(400);
    record("Esc closes Rincian and returns focus to it", cell,
      (await browser.evaluate(`!document.querySelector("dialog[open]")`)) && (await focused())?.name === "Rincian aman dipakai", (await focused())?.name);
    await browser.key(...ENTER);
    await sleep(500);
    await browser.evaluate(`document.querySelector('dialog[open] [aria-label="Tutup rincian"]').focus()`);
    await browser.key(...ENTER);
    await sleep(400);
    record("the close button closes Rincian and returns focus to it", cell, (await focused())?.name === "Rincian aman dipakai", (await focused())?.name);

    // A row opens the transaction dialog; closing returns focus to the row.
    const rowName = await browser.evaluate(`(() => { const row = document.querySelector('section[aria-labelledby="beranda-activity"] .row'); row.focus(); return row.textContent.trim().slice(0, 40); })()`);
    await browser.key(...ENTER);
    await sleep(1200);
    // The transaction dialog is a native modal <dialog>, which carries no role attribute.
    const drawer = await browser.evaluate(`Boolean(document.querySelector('dialog[open], [role="dialog"][aria-modal="true"]'))`);
    await browser.key(...ESC);
    await sleep(600);
    const back = await browser.evaluate(`document.activeElement.classList.contains("row") && document.activeElement.textContent.trim().slice(0, 40)`);
    record("a row opens the transaction dialog and focus returns to it", cell, drawer && back === rowName, `${rowName} → ${back}`);
  }
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "beranda-keys.json"), JSON.stringify(results, null, 1));
console.log(`${results.filter((result) => result.pass).length}/${results.length} passed`);
