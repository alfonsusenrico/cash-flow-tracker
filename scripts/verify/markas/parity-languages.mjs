// Figure identity across languages (add-language-system D16): an English and an Indonesian account on the same
// parity seed, region Indonesia, must show every amount, percentage, time, count and date number identically on
// Beranda and in its Rincian. Money strings are compared byte for byte; month names are compared by month.
//
// Usage: node parity-languages.mjs --en http://127.0.0.1:3522 --id http://127.0.0.1:3521 --out <dir> [--port 9709]
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

// Read in the page: amounts, percentages, clock times, dates as day-month, and counts as numbers.
const EXTRACT = String.raw`(() => {
  const months = { jan: 1, feb: 2, mar: 3, apr: 4, mei: 5, may: 5, jun: 6, jul: 7, agu: 8, aug: 8, sep: 9, sept: 9, okt: 10, oct: 10, nov: 11, des: 12, dec: 12 };
  const scope = [...document.querySelectorAll("[role=dialog], dialog[open]")].filter((d) => d.getBoundingClientRect().width > 0).pop() || document.querySelector("main") || document.body;
  const text = scope.innerText;
  const pattern = /([−+-]?Rp\s?[\d.,]+)|(\d+(?:[.,]\d+)?\s?%)|\b(\d{1,2}[.:]\d{2})\b|\b(\d{1,2}) (Jan|Feb|Mar|Apr|Mei|May|Jun|Jul|Agu|Aug|Sept?|Okt|Oct|Nov|Des|Dec)[a-z]*\b|\b(\d+(?:[.,]\d+)?)[×x]? ?(?:hari|days?|transaksi|transactions?|bulan|months?|hal|item|items|periode|periods?)\b/g;
  const figures = [];
  for (const match of text.matchAll(pattern)) {
    if (match[1]) figures.push(match[1]);
    else if (match[2]) figures.push(match[2].replace(/\s/g, ""));
    else if (match[3]) figures.push("time " + match[3]);
    else if (match[4]) figures.push("date " + match[4] + "-" + months[match[5].toLowerCase()]);
    else if (match[6]) figures.push("count " + match[6]);
  }
  return figures;
})()`;

const SCENES = [
  { name: "beranda", steps: [] },
  { name: "rincian", steps: ["^(Rincian aman dipakai sampai gajian|Breakdown of safe to spend until payday)$"] },
  {
    name: "rincian-lines-open",
    steps: ["^(Rincian aman dipakai sampai gajian|Breakdown of safe to spend until payday)$", "^(Masuk|Income)", "^(Pengeluaran|Spending)",
      "^(Ditabung & investasi|Saved & invested|Tabungan berkurang|Savings went down)", "^(Terjadwal|Scheduled)", "^(Menunggu jawaban|Waiting for an answer)"],
  },
];

async function click(browser, pattern) {
  return browser.evaluate(`(() => {
    const re = new RegExp(${JSON.stringify(pattern)});
    const dialogs = [...document.querySelectorAll('[role=dialog], dialog[open]')].filter((d) => d.getBoundingClientRect().width > 0);
    const scope = dialogs.length ? dialogs[dialogs.length - 1] : document;
    const target = [...scope.querySelectorAll("button, a")].find((element) => {
      const label = ((element.getAttribute("aria-label") || "") + " " + element.textContent).trim();
      return element.getBoundingClientRect().width > 0 && (re.test((element.getAttribute("aria-label") || "").trim()) || re.test(element.textContent.trim()) || re.test(label));
    });
    if (!target) return "NOT FOUND";
    target.click();
    return "clicked";
  })()`);
}

async function figuresOf(origin, port) {
  const browser = await launch({ port, profileDir: path.join(args.out, `.profile-${new URL(origin).port}`) });
  const result = {};
  try {
    await browser.navigate(`${origin}/auth/login`);
    const status = await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include",
      headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) }).then((response) => response.status)`);
    if (status !== 200) throw new Error(`login at ${origin} returned ${status}`);
    await browser.setCell({ width: 1440, height: 900, theme: "light" });
    for (const scene of SCENES) {
      await browser.navigate(origin + "/");
      await browser.settle();
      const log = [];
      for (const step of scene.steps) {
        log.push(await click(browser, step));
        await sleep(700);
      }
      result[scene.name] = { lang: await browser.evaluate("document.documentElement.lang"), log, figures: await browser.evaluate(EXTRACT) };
    }
  } finally {
    browser.close();
  }
  return result;
}

const port = Number(args.port ?? 9709);
const english = await figuresOf(args.en, port);
const indonesian = await figuresOf(args.id, port + 1);
const report = {};
let differing = 0;
for (const scene of SCENES) {
  const en = english[scene.name];
  const id = indonesian[scene.name];
  const same = JSON.stringify(en.figures) === JSON.stringify(id.figures);
  const money = (list) => list.filter((item) => item.includes("Rp"));
  const moneySame = JSON.stringify(money(en.figures)) === JSON.stringify(money(id.figures));
  if (!same) differing += 1;
  report[scene.name] = {
    lang: { en: en.lang, id: id.lang },
    steps: { en: en.log, id: id.log },
    figures: en.figures.length,
    money: money(en.figures).length,
    same,
    moneyByteIdentical: moneySame,
    ...(same ? {} : { en: en.figures, id: id.figures }),
  };
  console.log(same ? "SAME" : "DIFF", scene.name, en.figures.length, `money ${money(en.figures).length}`, `lang ${en.lang}/${id.lang}`);
}
writeFileSync(path.join(args.out, "parity-languages.json"), JSON.stringify({ differing, scenes: report }, null, 1));
