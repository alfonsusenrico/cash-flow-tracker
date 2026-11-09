// Figure parity for Analisis (apply-markas-analisis task 8.5 and owner decision D31). For the running cycle, the
// running month and the last whole month it reads:
// - every "Pengeluaran" the page shows: Ringkasan's hero, the sum of the daily table, the sum of the category rows,
//   Kalender's hero and Peta Aliran's Pengeluaran; all must equal the comparison's `totals.spending`;
// - Masuk and Ditabung & investasi on the hero line, against the former page's KPI row (before);
// - every amount on each tab, English against Indonesian (region ID), which must be byte-identical.
//
// Usage: node analisis-parity.mjs --before <origin> --after <origin> --english <origin> --out <file>
// Each origin is a loopback web whose API serves the same seeded owner (MARKAS_USER, MARKAS_PASSWORD).
import { writeFileSync } from "node:fs";
import { launch, sleep } from "./cdp.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
const credentials = { username: process.env.MARKAS_USER, password: process.env.MARKAS_PASSWORD };
const PERIODS = [
  { name: "cycle 0", steps: [] },
  { name: "month 0", steps: ["^(Bulan kalender|Calendar month)$"] },
  { name: "month -1", steps: ["^(Bulan kalender|Calendar month)$", "previous"] },
];

const money = (text) => Number(String(text ?? "").replace(/[^\d]/g, "")) || 0;
const ALL_MONEY = `[...(document.querySelector("main") ?? document.body).innerText.matchAll(/[−+-]?Rp ?[\\d.]+/g)].map((m) => m[0])`;

async function session(origin, port) {
  const browser = await launch({ port, profileDir: `${args.out}.profile-${port}` });
  await browser.setCell({ name: "parity", width: 1440, height: 900, theme: "light" });
  await browser.navigate(`${origin}/auth/login`);
  const status = await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) }).then((r) => r.status)`);
  if (status !== 200) throw new Error(`${origin} login ${status}`);
  return browser;
}

async function go(browser, origin, path, steps) {
  await browser.navigate(origin + path);
  await browser.settle();
  await sleep(1200);
  for (const step of steps) {
    if (step === "previous") {
      await browser.evaluate(`document.querySelector(".stepper .ws-icon-btn")?.click()`);
    } else {
      await browser.evaluate(`(() => { const re = new RegExp(${JSON.stringify(step)}); [...document.querySelectorAll("button")].find((b) => re.test(b.textContent.trim()))?.click(); })()`);
    }
    await sleep(1800);
  }
  await browser.settle();
  await sleep(800);
}

async function readAfter(browser, origin, steps) {
  await go(browser, origin, "/insights", steps);
  const ringkasan = await browser.evaluate(`(async () => {
    const hero = document.querySelector(".hero .hero-fig")?.textContent ?? null;
    const line = [...document.querySelectorAll(".hero .hero-sub .t-amt")].map((e) => e.textContent);
    document.querySelector(".ct-toggle")?.click();
    await new Promise((r) => setTimeout(r, 500));
    const daily = [...document.querySelectorAll("section[aria-labelledby=an-daily] tbody td.num")].map((e) => e.textContent);
    const zero = document.querySelector(".an-more button");
    if (zero && zero.getAttribute("aria-expanded") === "false") zero.click();
    await new Promise((r) => setTimeout(r, 500));
    const categories = [...document.querySelectorAll(".cat-row .r-amt")].map((e) => e.textContent);
    return { hero, line, daily, categories, all: ${ALL_MONEY} };
  })()`);
  await browser.evaluate(`(() => { const t = document.getElementById("analysis-tab-kalender"); t?.click(); })()`);
  await sleep(2500);
  const kalender = await browser.evaluate(`({ hero: document.querySelector(".hero .hero-fig")?.textContent ?? null, all: ${ALL_MONEY} })`);
  await browser.evaluate(`document.getElementById("analysis-tab-aliran")?.click()`);
  await sleep(2500);
  const aliran = await browser.evaluate(`({ spending: document.querySelector(".flow-supports .fig-block .t-fig")?.textContent ?? null, all: ${ALL_MONEY} })`);
  return { ringkasan, kalender, aliran };
}

async function readBefore(browser, origin, steps) {
  await go(browser, origin, "/insights", steps);
  return browser.evaluate(`(() => {
    const kpis = Object.fromEntries([...document.querySelectorAll(".ws-kpi")].map((k) => [k.querySelector(".ws-kpi-label")?.textContent?.trim() ?? k.textContent.slice(0, 20), (k.querySelector(".ws-kpi-value")?.textContent ?? "").match(/Rp ?[\\d.]+/)?.[0] ?? null]));
    return { kpis };
  })()`);
}

const report = { periods: {} };
const before = await session(args.before, 9744);
const after = await session(args.after, 9745);
const english = await session(args.english, 9746);
try {
  for (const period of PERIODS) {
    const now = await readAfter(after, args.after, period.steps);
    const en = await readAfter(english, args.english, period.steps);
    const old = await readBefore(before, args.before, period.steps);
    const basis = period.name.startsWith("cycle") ? "cycle" : "month";
    const periods = await after.evaluate(`fetch("/api/reports/monthly/periods?basis=${basis}").then((r) => r.json())`);
    const ordered = [...periods.periods].sort((a, b) => b.start_date.localeCompare(a.start_date));
    const viewed = ordered[period.name.endsWith("-1") ? 1 : 0];
    const comparison = await after.evaluate(`fetch("/api/reports/monthly/comparison?basis=${basis}&start_date=${viewed.start_date}").then((r) => r.json())`);
    const server = comparison.comparison.totals;
    const pengeluaran = {
      server: server.spending,
      ringkasanHero: money(now.ringkasan.hero),
      dailyTableSum: now.ringkasan.daily.reduce((sum, text) => sum + money(text), 0),
      categorySum: now.ringkasan.categories.reduce((sum, text) => sum + money(text), 0),
      kalenderHero: money(now.kalender.hero),
      petaAliran: money(now.aliran.spending),
    };
    const allEqual = new Set(Object.values(pengeluaran)).size === 1;
    const oldKpi = (name) => old.kpis[Object.keys(old.kpis).find((key) => key.startsWith(name)) ?? ""] ?? null;
    const beforeAfter = {
      masuk: { before: oldKpi("Masuk"), after: now.ringkasan.line[0] ?? null },
      pengeluaran: { before: oldKpi("Pengeluaran"), after: now.ringkasan.hero },
      ditabung: { before: oldKpi("Ditabung"), after: now.ringkasan.line[1] ?? null },
    };
    const changed = Object.entries(beforeAfter).filter(([, pair]) => pair.before !== pair.after).map(([name]) => name);
    const languages = {
      ringkasan: JSON.stringify(now.ringkasan.all) === JSON.stringify(en.ringkasan.all),
      kalender: JSON.stringify(now.kalender.all) === JSON.stringify(en.kalender.all),
      aliran: JSON.stringify(now.aliran.all) === JSON.stringify(en.aliran.all),
      values: now.ringkasan.all.length + now.kalender.all.length + now.aliran.all.length,
    };
    report.periods[period.name] = { pengeluaran, allEqual, beforeAfter, changed, languages };
  }
} finally {
  before.close();
  after.close();
  english.close();
}
report.totals = {
  everyPengeluaranEqual: Object.values(report.periods).every((entry) => entry.allEqual),
  changedValues: Object.values(report.periods).reduce((sum, entry) => sum + entry.changed.length, 0),
  languagesIdentical: Object.values(report.periods).every((entry) => entry.languages.ringkasan && entry.languages.kalender && entry.languages.aliran),
  languageValues: Object.values(report.periods).reduce((sum, entry) => sum + entry.languages.values, 0),
};
writeFileSync(args.out, JSON.stringify(report, null, 1));
console.log(JSON.stringify(report.totals));
