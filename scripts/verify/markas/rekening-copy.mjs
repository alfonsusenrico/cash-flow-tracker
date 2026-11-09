// Indonesian copy check (apply-markas-rekening task 3.4): the visible words of Rekening & Kantong, Per kegunaan and
// Per rekening, on two origins; prints the strings only one side shows (figures and user-named accounts left out).
//
// Usage: node rekening-copy.mjs --a <before origin> --b <after origin> --out <file.json> [--port 9758]
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, all) => (index % 2 ? pairs : [...pairs, [all[index].replace(/^--/, ""), all[index + 1]]]), []));
const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "rekening-contoh-123" };
const TEXT = String.raw`(() => {
  const names = new Set();
  const out = new Set();
  const walker = document.createTreeWalker(document.querySelector("main") ?? document.body, NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    const element = node.parentElement;
    if (!element || !element.getBoundingClientRect().width) continue;
    const text = node.textContent.replace(/\s+/g, " ").trim();
    if (!text || /\d/.test(text) || text.length < 3) continue;
    out.add(text);
  }
  for (const element of document.querySelectorAll("main [aria-label]")) { const label = element.getAttribute("aria-label"); if (!/\d/.test(label)) out.add("[aria] " + label); }
  return [...out];
})()`;

async function read(origin, port) {
  const browser = await launch({ port, profileDir: path.join(path.dirname(args.out), `.profile-copy-${port}`) });
  await browser.setCell({ name: "1440-light", width: 1440, height: 900, theme: "light" });
  await browser.navigate(`${origin}/auth/login`);
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  const seen = new Set();
  for (const grouping of ["use", "rekening"]) {
    await browser.evaluate(`localStorage.setItem("rekening-grouping", "${grouping}")`);
    await browser.navigate(`${origin}/accounts`);
    await browser.settle();
    await sleep(1500);
    for (const text of await browser.evaluate(TEXT)) seen.add(text);
  }
  browser.close();
  return seen;
}

const port = Number(args.port ?? 9758);
const before = await read(args.a, port);
const after = await read(args.b, port + 1);
const accounts = await (async () => new Set())();
const onlyBefore = [...before].filter((text) => !after.has(text)).sort();
const onlyAfter = [...after].filter((text) => !before.has(text)).sort();
mkdirSync(path.dirname(args.out), { recursive: true });
writeFileSync(args.out, JSON.stringify({ a: args.a, b: args.b, onlyBefore, onlyAfter, shared: [...before].filter((text) => after.has(text)).sort(), accounts: [...accounts] }, null, 1));
console.log(`before ${before.size}, after ${after.size}, only before ${onlyBefore.length}, only after ${onlyAfter.length}`);
console.log("ONLY BEFORE:", onlyBefore.join(" ¦ "));
console.log("ONLY AFTER:", onlyAfter.join(" ¦ "));
