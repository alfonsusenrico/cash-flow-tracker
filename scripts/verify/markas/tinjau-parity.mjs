// Tinjau's figure parity (apply-markas-tinjau task 5.5): for every item of the queue, the item pane's amounts, dates,
// times and counts before and after the change, the folder counts and the hero; and the same after the change in an
// Indonesian and an English account on the same data. Item panes changed layout, so figures are compared as values:
// a value shown before and not after (or the reverse) is a change; a value shown fewer times is a removed duplicate.
//
// Usage: node tinjau-parity.mjs --before http://127.0.0.1:3610 --after http://127.0.0.1:3621 --en http://127.0.0.1:3622 --out <dir>
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
const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "contoh-sandi-123" };

// Amounts, dates as day-month (month by number, so both languages compare), clock times and counts with a unit.
const EXTRACT = (selector) => String.raw`(() => {
  const months = { jan: 1, feb: 2, mar: 3, apr: 4, mei: 5, may: 5, jun: 6, jul: 7, agu: 8, aug: 8, sep: 9, okt: 10, oct: 10, nov: 11, des: 12, dec: 12 };
  const scope = document.querySelector(${JSON.stringify(selector)});
  if (!scope) return null;
  const text = scope.innerText.replace(/ /g, " ");
  const found = [];
  const pattern = /([−+-]?Rp\s?[\d.]+)|\b(\d{1,2}) (Jan|Feb|Mar|Apr|Mei|May|Jun|Jul|Agu|Aug|Sep|Okt|Oct|Nov|Des|Dec)[a-z]*\b|\b(\d{1,2}[.:]\d{2})\b|\b(\d+) (?:transaksi|transactions?|kemungkinan|possible)\b/g;
  for (const match of text.matchAll(pattern)) {
    if (match[1]) found.push(match[1].replace(/\s/, " ").replace(/^-/, "−"));
    else if (match[2]) found.push(match[2] + "-" + months[match[3].toLowerCase()]);
    else if (match[4]) found.push(match[4]);
    else found.push("n" + match[5]);
  }
  return found;
})()`;

async function session(origin, port) {
  const browser = await launch({ port, profileDir: path.join(args.out, `.profile-${new URL(origin).port}`) });
  await browser.navigate(`${origin}/auth/login`);
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include",
    headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  await browser.setCell({ width: 1440, height: 900, theme: "light" });
  return browser;
}

async function links(browser) {
  return browser.evaluate(`Promise.all([fetch("/api/review/queue").then((r) => r.json()), fetch("/api/review/suggestions").then((r) => r.json())])
    .then(([queue, suggestions]) => [
      ...queue.questions.map((entry) => ["apa", entry.transaction.id]),
      ...queue.groups.map((group) => [group.needs_review_count > 0 ? "jawab" : "otomatis", group.transactions[0].id]),
      ...queue.awaiting.map((item) => ["pindah", item.id]),
      ...queue.ambiguous.map((item) => ["pindah", item.id]),
      ...(queue.seen_twice || []).filter((item) => item.kept).map((item) => ["dua", item.kept.transaction_id]),
      ...(queue.skipped || []).filter((item) => item.kept).map((item) => ["lewat", item.kept.transaction_id]),
      ...suggestions.groups.map((group) => ["saran", group.suggestions[0].transaction_id]),
    ])`);
}

async function read(browser, origin, pane, list) {
  const result = {};
  for (const [folder, id] of list) {
    await browser.navigate(`${origin}/review?item=${id}&folder=${folder}`);
    await browser.settle();
    await sleep(500);
    result[`${folder}:${id}`] = await browser.evaluate(EXTRACT(pane));
  }
  return result;
}

function compare(before, after) {
  const count = (values) => values.reduce((map, value) => map.set(value, (map.get(value) ?? 0) + 1), new Map());
  const b = count(before ?? []);
  const a = count(after ?? []);
  const changed = [...new Set([...b.keys(), ...a.keys()])].filter((value) => !b.has(value) || !a.has(value));
  const fewer = [...b.keys()].filter((value) => a.has(value) && a.get(value) < b.get(value)).map((value) => `${value} ×${b.get(value)}→${a.get(value)}`);
  return { changed, fewer };
}

const before = await session(args.before, 9681);
const after = await session(args.after, 9682);
const english = await session(args.en, 9683);
const report = { items: {}, counts: {}, languages: {} };
try {
  const list = await links(after);
  const beforeItems = await read(before, args.before, ".ws-tri-item", list);
  const afterItems = await read(after, args.after, ".tj-detail", list);
  const englishItems = await read(english, args.en, ".tj-detail", list);
  for (const key of Object.keys(afterItems)) {
    report.items[key] = { before: beforeItems[key], after: afterItems[key], ...compare(beforeItems[key], afterItems[key]) };
    const same = JSON.stringify(afterItems[key]) === JSON.stringify(englishItems[key]);
    report.languages[key] = same ? "identical" : { id: afterItems[key], en: englishItems[key] };
  }
  await before.navigate(`${args.before}/review`);
  await before.settle();
  await after.navigate(`${args.after}/review`);
  await after.settle();
  await english.navigate(`${args.en}/review`);
  await english.settle();
  report.counts.before = await before.evaluate(`({ hero: document.querySelector("#triage-list-heading [aria-hidden]")?.textContent,
    folders: [...document.querySelectorAll(".ws-tri-folders .ws-chip, .ws-tri-fold summary")].map((node) => node.textContent.replace(/\\s+/g, " ").trim()) })`);
  report.counts.after = await after.evaluate(`({ hero: document.querySelector(".t-hero")?.textContent,
    folders: [...document.querySelectorAll(".rail-item")].map((node) => node.querySelector(".lbl").textContent + " " + node.querySelector(".count").textContent) })`);
  report.counts.english = await english.evaluate(`({ hero: document.querySelector(".t-hero")?.textContent,
    folders: [...document.querySelectorAll(".rail-item .count")].map((node) => node.textContent) })`);
} finally {
  before.close();
  after.close();
  english.close();
}
const changed = Object.entries(report.items).filter(([, item]) => item.changed.length);
const languageDiffs = Object.values(report.languages).filter((entry) => entry !== "identical").length;
report.summary = { items: Object.keys(report.items).length, itemsWithChangedValues: changed.length, languageDiffs };
writeFileSync(path.join(args.out, "tinjau-parity.json"), JSON.stringify(report, null, 1));
console.log(JSON.stringify(report.summary), JSON.stringify(report.counts));
for (const [key, item] of changed) console.log("CHANGED", key, JSON.stringify(item.changed));
