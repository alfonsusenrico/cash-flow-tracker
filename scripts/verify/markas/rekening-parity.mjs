// Rekening figure parity (apply-markas-rekening task 5.5): reads every tile's amount and gain, every group's
// figures and the hero from the DOM of two origins, Per kegunaan and Per rekening, and lists the differences.
// It reads the old page (`data-rail` rows, cards named by `aria-label`) and the new one (`data-group`, `.tl-name`).
//
// Usage: node rekening-parity.mjs --a <origin> --b <origin> --out <file.json> [--port 9755]
// Environment: MARKAS_USER, MARKAS_PASSWORD (the seed owner of both origins' databases).
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, all) => (index % 2 ? pairs : [...pairs, [all[index].replace(/^--/, ""), all[index + 1]]]), []));
const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "rekening-contoh-123" };

const EXTRACT = String.raw`(() => {
  const money = /[−+-]?(?:Rp|\$)\s?[\d.,]+/g;
  const percent = /[−+-]?\d+(?:[.,]\d+)?%/g;
  const figures = (text) => ({ amounts: text.match(money) ?? [], percents: text.match(percent) ?? [] });
  const tiles = {};
  for (const row of document.querySelectorAll("[data-account-row]")) {
    if (!row.getBoundingClientRect().width) continue;
    const name = row.querySelector(".tl-name")?.textContent ?? (row.querySelector("button[aria-label]")?.getAttribute("aria-label") ?? "").split(",")[0];
    const group = row.closest("[data-group], [data-rail]");
    const key = (group?.getAttribute("data-group") ?? group?.getAttribute("data-rail") ?? "?") + " / " + name;
    tiles[key] = figures(row.innerText.replace(/\n/g, " "));
  }
  const groups = {};
  for (const section of document.querySelectorAll("section[data-group], section[data-rail]")) {
    const head = section.querySelector(".grp-h") ?? section.firstElementChild;
    groups[section.getAttribute("data-group") ?? section.getAttribute("data-rail")] = figures(head.innerText.replace(/\n/g, " "));
  }
  const hero = document.querySelector(".hero") ?? document.querySelector("main section");
  return { hero: figures(hero ? hero.innerText.replace(/\n/g, " ") : ""), groups, tiles };
})()`;

async function read(origin, port) {
  const browser = await launch({ port, profileDir: path.join(path.dirname(args.out), `.profile-${port}`) });
  try {
    await browser.setCell({ name: "1440-light", width: 1440, height: 900, theme: "light" });
    await browser.navigate(`${origin}/auth/login`);
    const status = await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) }).then((r) => r.status)`);
    if (status !== 200) throw new Error(`login ${status}`);
    const result = {};
    for (const grouping of ["use", "rekening"]) {
      await browser.evaluate(`localStorage.setItem("rekening-grouping", "${grouping}"); localStorage.removeItem("hideBalances")`);
      await browser.navigate(`${origin}/accounts`);
      await browser.settle();
      await sleep(1500);
      result[grouping] = await browser.evaluate(EXTRACT);
    }
    return result;
  } finally {
    await browser.close?.();
  }
}

function compare(a, b) {
  const differences = [];
  for (const grouping of Object.keys(a)) {
    for (const part of ["tiles", "groups"]) {
      const keys = new Set([...Object.keys(a[grouping][part]), ...Object.keys(b[grouping][part])]);
      for (const key of keys) {
        const left = JSON.stringify(a[grouping][part][key] ?? null);
        const right = JSON.stringify(b[grouping][part][key] ?? null);
        if (left !== right) differences.push({ grouping, part, key, a: a[grouping][part][key] ?? null, b: b[grouping][part][key] ?? null });
      }
    }
    if (JSON.stringify(a[grouping].hero) !== JSON.stringify(b[grouping].hero)) differences.push({ grouping, part: "hero", a: a[grouping].hero, b: b[grouping].hero });
  }
  return differences;
}

const port = Number(args.port ?? 9755);
const first = await read(args.a, port);
const second = await read(args.b, port + 1);
const differences = compare(first, second);
mkdirSync(path.dirname(args.out), { recursive: true });
writeFileSync(args.out, JSON.stringify({ a: args.a, b: args.b, differences, first, second }, null, 1));
const tiles = Object.keys(first.use.tiles).length;
console.log(`tiles ${tiles} / ${Object.keys(second.use.tiles).length}, groups ${Object.keys(first.use.groups).length}, differences ${differences.length}`);
for (const difference of differences.slice(0, 40)) console.log(JSON.stringify(difference).slice(0, 300));
