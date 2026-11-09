// Figure parity for Transaksi (apply-markas-transaksi task 6.5): every amount, time and count the page shows, before
// against after on the same data, and English against Indonesian; plus the savings hero against Beranda's figure.
//
// Usage: node transaksi-parity.mjs --before <origin> --after <origin> --english <origin> --out <file>
// Each origin is a loopback web whose API serves the same seeded owner (MARKAS_USER, MARKAS_PASSWORD).
import { writeFileSync } from "node:fs";
import { launch, sleep } from "./cdp.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
const username = process.env.MARKAS_USER ?? "dimas_contoh";
const password = process.env.MARKAS_PASSWORD ?? "contoh-sandi-123";
const PATHS = ["/ledger?period=cycle", "/ledger?period=month", "/ledger", "/ledger?period=cycle&type=expense", "/ledger?period=cycle&type=movement", "/ledger?period=cycle&type=income"];

// Money with its sign, times of day, and plain counts, in document order; the page's own text only.
const EXTRACT = `(() => {
  const main = document.querySelector("main") ?? document.body;
  const text = main.innerText;
  return {
    money: [...text.matchAll(/[−+-]?Rp ?[\\d.]+/g)].map((match) => match[0].replace("Rp ", "Rp ")),
    times: [...text.matchAll(/\\b\\d{2}[.:]\\d{2}\\b/g)].map((match) => match[0]),
  };
})()`;

async function session(origin, port) {
  const browser = await launch({ port, profileDir: `${args.out}.profile-${port}` });
  await browser.setCell({ name: "parity", width: 1440, height: 900, theme: "light" });
  await browser.navigate(`${origin}/auth/login`);
  const status = await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify({ username, password })}) }).then((r) => r.status)`);
  if (status !== 200) throw new Error(`${origin} login ${status}`);
  return browser;
}

async function read(browser, origin, path) {
  await browser.navigate(origin + path);
  await browser.settle();
  await sleep(1500);
  return browser.evaluate(EXTRACT);
}

const count = (values) => values.reduce((map, value) => map.set(value, (map.get(value) ?? 0) + 1), new Map());
const unsigned = (value) => value.replace(/^[−+-]/, "");
function difference(first, second) {
  const a = count(first);
  const b = count(second);
  const keys = new Set([...a.keys(), ...b.keys()]);
  return [...keys].filter((key) => (a.get(key) ?? 0) !== (b.get(key) ?? 0)).map((key) => ({ value: key, before: a.get(key) ?? 0, after: b.get(key) ?? 0 }));
}

const report = { pages: {}, languages: {}, savings: {} };
const before = await session(args.before, 9741);
const after = await session(args.after, 9742);
const english = await session(args.english, 9743);
try {
  for (const path of PATHS) {
    const old = await read(before, args.before, path);
    const now = await read(after, args.after, path);
    const en = await read(english, args.english, path);
    // Before against after: the same amounts once their sign is set aside (the "+" on Masuk and Bersih and the
    // signs of the old KPI cards are presentation), and the same times of day.
    report.pages[path] = {
      before: old.money.length,
      after: now.money.length,
      changedAmounts: difference(old.money.map(unsigned), now.money.map(unsigned)),
      signChanges: difference(old.money, now.money).filter((entry) => !difference(old.money.map(unsigned), now.money.map(unsigned)).some((other) => other.value === unsigned(entry.value))),
      changedTimes: difference(old.times, now.times),
    };
    report.languages[path] = {
      values: now.money.length + now.times.length,
      identicalMoney: JSON.stringify(now.money) === JSON.stringify(en.money),
      identicalTimes: JSON.stringify(now.times) === JSON.stringify(en.times),
    };
  }
  // Owner feedback 4: the savings hero for the open cycle equals Beranda's "Ditabung & investasi".
  const summary = await after.evaluate(`fetch("/api/dashboard/cycle-summary").then((r) => r.json())`);
  const ledger = await after.evaluate(`fetch("/api/transactions?type=saving&period=cycle&logical_movements=true&include=type_counts&limit=200").then((r) => r.json())`);
  await after.navigate(`${args.after}/ledger?period=cycle&type=saving`);
  await after.settle();
  await sleep(1500);
  const hero = await after.evaluate(`document.querySelector(".hero .t-hero")?.textContent ?? null`);
  await after.navigate(`${args.after}/`);
  await after.settle();
  await sleep(2000);
  const beranda = await after.evaluate(`(() => { const label = [...document.querySelectorAll(".fig-block")].find((block) => /Ditabung & investasi/.test(block.textContent)); return label?.querySelector(".t-fig")?.textContent ?? null; })()`);
  report.savings = {
    cycleSummaryTotal: summary.tabungan?.total ?? null,
    ledgerSavingNet: ledger.summary?.period?.saving_net ?? null,
    equalApi: summary.tabungan?.total === ledger.summary?.period?.saving_net,
    savingCount: ledger.type_counts?.saving ?? null,
    listed: ledger.total,
    heroText: hero,
    berandaText: beranda,
    equalPage: hero !== null && hero === beranda,
  };
} finally {
  before.close();
  after.close();
  english.close();
}
const changed = Object.values(report.pages).reduce((sum, page) => sum + page.changedAmounts.length + page.changedTimes.length, 0);
report.totals = {
  changedValues: changed,
  languagesIdentical: Object.values(report.languages).every((page) => page.identicalMoney && page.identicalTimes),
};
writeFileSync(args.out, JSON.stringify(report, null, 1));
console.log(JSON.stringify(report.totals), JSON.stringify(report.savings));
