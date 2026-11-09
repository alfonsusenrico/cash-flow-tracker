// Figure parity of Laporan (apply-markas-laporan D23, task 4.5) on running apps with the same data: the page before
// this change against after it, and the English page against the Indonesian one. Nothing is written.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node laporan-parity.mjs --before http://127.0.0.1:3735 --after http://127.0.0.1:3733 \
//   --english http://127.0.0.1:3734 --out <file.json> [--starts 2026-10-01,2026-09-01] [--port 9740]
import { writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
const starts = (args.starts ?? "2026-10-01,2026-09-01,2026-08-01,2026-11-01").split(",");
const browser = await launch({ port: Number(args.port ?? 9740), profileDir: path.join(path.dirname(args.out), ".profile-parity") });

// The old page (before): KPI cards, the pillar table, income sources, merchants, the history list and Selisih.
const BEFORE = `(() => {
  const text = (element) => (element?.textContent ?? "").replace(/\\s+/g, " ").trim();
  const kpis = Object.fromEntries([...document.querySelectorAll(".ws-kpi")].map((kpi) => [text(kpi.querySelector(".ws-kpi-label")), text(kpi.querySelector(".ws-kpi-value"))]));
  const table = {};
  for (const row of document.querySelectorAll("table tbody tr")) {
    const cells = [...row.children];
    table[text(cells[0])] = [text(cells[1]), text(cells[2])];
  }
  const list = (id) => Object.fromEntries([...document.querySelectorAll("#" + id + "-title")].flatMap((heading) => [...heading.closest("section").querySelectorAll("li")]).map((item) => {
    const label = text(item.querySelector(".truncate"));
    const amount = text(item.querySelector("a, span.shrink-0"));
    return [label, amount];
  }));
  const history = [...document.querySelectorAll("section[aria-labelledby=report-history] ul li")].map(text);
  return {
    spending: kpis["Pengeluaran"] ?? kpis["Pengeluaran sejauh ini"], income: kpis["Masuk"], saved: kpis["Ditabung & investasi"],
    difference: kpis["Selisih"] ?? null, table, sources: list("report-income"), merchants: list("report-merchants"), history,
  };
})()`;

// The new page (after): the hero, the question figures, the table, income sources, top places, the chart and Selisih.
const AFTER = `(() => {
  const text = (element) => (element?.textContent ?? "").replace(/\\s+/g, " ").trim();
  const table = {};
  for (const row of document.querySelectorAll(".lp-tbl tbody tr")) {
    const cells = [...row.children];
    table[text(cells[0])] = [text(cells[1]), text(cells[2])];
  }
  const rows = (selector) => Object.fromEntries([...document.querySelectorAll(selector)].map((row) => [text(row.querySelector(".r-title")), text(row.querySelector(".r-amt")).replace(/^[+−]/, "")]));
  const panel = document.querySelector("aside.panel");
  const difference = [...(panel?.querySelectorAll(".t-fig") ?? [])].map(text)[0] ?? null;
  return {
    spending: text(document.querySelector(".t-hero")), income: text(document.querySelector("#lp-q1 ~ .q-body .t-fig")),
    saved: text(document.querySelector("#lp-q2 ~ .q-body .t-fig")), difference: difference ? difference.replace(/^\\+/, "") : null,
    table, sources: rows("[aria-labelledby=lp-income-sources] .row"), merchants: rows("[aria-labelledby=lp-places] .row"),
    history: document.querySelector(".bars-v")?.getAttribute("aria-label") ?? "",
    figures: (document.querySelector("main")?.textContent ?? "").match(/Rp\\s?\\d{1,3}(?:[.,]\\d{3})*|\\d+(?:[.,]\\d+)?\\s?%|\\$\\d{1,3}(?:,\\d{3})*(?:\\.\\d+)?/g) ?? [],
  };
})()`;

async function login(origin) {
  await browser.navigate(`${origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER, password: process.env.MARKAS_PASSWORD };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) }).then((r) => r.status)`);
}

async function read(origin, start, source) {
  await browser.navigate(`${origin}/laporan?basis=month&start=${start}`);
  await browser.settle();
  await sleep(2500);
  return browser.evaluate(source);
}

const result = { starts, before: {}, after: {}, english: {}, differences: [], languageDifferences: [] };
try {
  await browser.setCell({ width: 1440, height: 900, theme: "light", mobile: false });
  for (const [name, origin, source] of [["before", args.before, BEFORE], ["after", args.after, AFTER], ["english", args.english, AFTER]]) {
    await login(origin);
    for (const start of starts) result[name][start] = await read(origin, start, source);
  }
  const strip = (value) => (value ?? "").replace(/^[+−-]/, "");
  for (const start of starts) {
    const before = result.before[start];
    const after = result.after[start];
    const compare = (figure, left, right) => {
      if (strip(left) !== strip(right)) result.differences.push({ start, figure, before: left, after: right });
    };
    compare("Pengeluaran", before.spending, after.spending);
    compare("Masuk", before.income, after.income);
    compare("Ditabung & investasi", before.saved, after.saved);
    if (before.difference !== null) compare("Selisih", before.difference, after.difference);
    for (const [row, [amount, share]] of Object.entries(before.table)) {
      const match = after.table[row.toUpperCase()] ?? after.table[row] ?? Object.entries(after.table).find(([key]) => key.toLowerCase() === row.toLowerCase())?.[1];
      if (!match) result.differences.push({ start, figure: `table ${row}`, before: amount, after: "missing" });
      else {
        compare(`table ${row} amount`, amount, match[0]);
        compare(`table ${row} share`, share, match[1]);
      }
    }
    for (const [source, amount] of Object.entries(before.sources)) compare(`income ${source}`, amount, after.sources[source] ?? "missing");
    for (const item of before.history) {
      const [label, amount] = item.split(": ");
      const plain = label.replace(" (berjalan)", "");
      if (!after.history.includes(`${plain}`) || !after.history.includes(amount)) result.differences.push({ start, figure: `history ${label}`, before: amount, after: after.history });
    }
    // D29: the top places are on the Pengeluaran basis now; listed apart, as an intended difference.
    result.merchantsBefore = result.merchantsBefore ?? {};
    result.merchantsBefore[start] = { before: before.merchants, after: after.merchants };
    const english = result.english[start].figures.join(" | ");
    const indonesian = after.figures.join(" | ");
    if (english !== indonesian) result.languageDifferences.push({ start, english, indonesian });
  }
} finally {
  browser.close();
}
writeFileSync(args.out, JSON.stringify(result, null, 1));
console.log(`starts ${starts.length}; before/after differences ${result.differences.length}; English/Indonesian differences ${result.languageDifferences.length}`);
for (const difference of result.differences) console.log("DIFF", JSON.stringify(difference).slice(0, 300));
for (const difference of result.languageDifferences) console.log("LANG", difference.start, difference.english.slice(0, 200), "||", difference.indonesian.slice(0, 200));
