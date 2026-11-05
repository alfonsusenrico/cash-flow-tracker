// Figure parity: every money figure, count and date on each page, read from the DOM in document order, must
// be the same before and after a presentation change (the plan's common gate, item 4).
//
// Usage: node parity.mjs --before http://127.0.0.1:3474 --after http://127.0.0.1:3471 --out <dir>
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { launch } from "./cdp.mjs";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
const here = path.dirname(fileURLToPath(import.meta.url));
mkdirSync(args.out, { recursive: true });
// Every page route once, in its default view (dialog scenarios open from these same pages).
const pages = JSON.parse(readFileSync(path.join(here, "scenarios-app.json"), "utf8"))
  .filter((scenario) => !scenario.name.startsWith("dlg-") && !scenario.componentSheet)
  .filter((scenario, index, all) => all.findIndex((other) => other.path === scenario.path) === index);

// Amounts, percentages, counts with a unit, dates and times as the page writes them.
const FIGURES = String.raw`/[−+-]?Rp\s?[\d.]+(?:,\d+)?|\d+(?:,\d+)?\s?%|\b\d{1,2} (?:Jan|Feb|Mar|Apr|Mei|Jun|Jul|Agu|Sep|Okt|Nov|Des)[a-z]*\b|\b\d{1,2}[.:]\d{2}\b|\b\d+(?:,\d+)? (?:bulan|hari|transaksi|kantong|rekening)\b/g`;

// The seed owner: the audit seed's by default, the parity seed's through MARKAS_USER (apply-markas-shell D13).
const credentials = { username: process.env.MARKAS_USER ?? "raka_contoh", password: process.env.MARKAS_PASSWORD ?? "contoh-sandi-123" };

async function figuresOf(origin) {
  const browser = await launch({ port: Number(args.port ?? 9634), profileDir: path.join(args.out, `.profile-${new URL(origin).port}`) });
  const result = {};
  try {
    await browser.navigate(`${origin}/auth/login`);
    await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include",
      headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
    await browser.setCell({ width: 1440, height: 900, theme: "light" });
    for (const page of pages) {
      await browser.navigate(origin + page.path);
      await browser.settle();
      result[page.name] = await browser.evaluate(`(document.querySelector("main") || document.body).innerText.match(${FIGURES}) || []`);
    }
  } finally {
    browser.close();
  }
  return result;
}

const before = await figuresOf(args.before);
const after = await figuresOf(args.after);
const report = {};
for (const page of pages) {
  const same = JSON.stringify(before[page.name]) === JSON.stringify(after[page.name]);
  report[page.name] = { same, figures: after[page.name].length };
  if (!same) report[page.name].diff = { before: before[page.name].slice(0, 40), after: after[page.name].slice(0, 40) };
  console.log(same ? "SAME" : "DIFF", page.name, after[page.name].length);
}
writeFileSync(path.join(args.out, "parity.json"), JSON.stringify(report, null, 1));
