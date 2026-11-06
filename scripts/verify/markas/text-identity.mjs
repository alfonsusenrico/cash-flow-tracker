// Indonesian copy byte for byte (add-language-system task 4.1): the rendered text of the shell, Beranda with its
// Rincian and the sign-in and sign-up page, read from the DOM of the baseline build and of the change's build for
// the same Indonesian account and data, must be identical, accessible names included.
//
// Usage: node text-identity.mjs --before http://127.0.0.1:3510 --after http://127.0.0.1:3521 --out <dir> [--port 9713]
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

// Visible text and every accessible name, in document order.
const READ = `(() => {
  const names = [...document.querySelectorAll("[aria-label], [placeholder], [title]")]
    .map((element) => [element.getAttribute("aria-label"), element.getAttribute("placeholder"), element.getAttribute("title")].filter(Boolean).join(" | "));
  return { text: document.body.innerText.split("\\n").map((line) => line.trim()).filter(Boolean), names };
})()`;

const SCENES = [
  { name: "beranda", url: "/", steps: [] },
  { name: "rincian", url: "/", steps: ["Rincian aman dipakai sampai gajian"] },
  { name: "rincian-lines", url: "/", steps: ["Rincian aman dipakai sampai gajian", "^Masuk", "^Pengeluaran", "^Ditabung", "^Terjadwal", "^Menunggu jawaban"] },
  { name: "notifikasi", url: "/", steps: ["^Notifikasi"] },
  { name: "akun", url: "/", steps: ["^Akun "] },
  { name: "bantuan", url: "/", keys: true },
  { name: "lainnya-ponsel", url: "/", phone: true, steps: ["^Lainnya$"] },
  { name: "beranda-ponsel", url: "/", phone: true, steps: [] },
];
const SIGNED_OUT = [
  { name: "masuk", url: "/auth/login", steps: [] },
  { name: "daftar", url: "/auth/login", steps: ["^Daftar sekarang$"] },
];

async function click(browser, pattern) {
  return browser.evaluate(`(() => {
    const re = new RegExp(${JSON.stringify(pattern)});
    const dialogs = [...document.querySelectorAll('[role=dialog], dialog[open]')].filter((d) => d.getBoundingClientRect().width > 0);
    const scope = dialogs.length ? dialogs[dialogs.length - 1] : document;
    const target = [...scope.querySelectorAll("button, a")].filter((element) => element.getBoundingClientRect().width > 0)
      .find((element) => re.test((element.getAttribute("aria-label") || "").trim()) || re.test(element.textContent.trim()));
    if (!target) return "NOT FOUND";
    target.click();
    return "clicked";
  })()`);
}

async function read(origin, port, signedIn) {
  const browser = await launch({ port, profileDir: path.join(args.out, `.profile-${new URL(origin).port}-${signedIn ? "in" : "out"}`) });
  const result = {};
  try {
    await browser.navigate(`${origin}/auth/login`);
    if (signedIn) {
      await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
    } else {
      await browser.evaluate(`document.cookie = "cft_language=id; Path=/"`);
    }
    for (const scene of signedIn ? SCENES : SIGNED_OUT) {
      await browser.setCell(scene.phone ? { width: 390, height: 844, theme: "light", mobile: true } : { width: 1440, height: 900, theme: "light" });
      await browser.evaluate(`localStorage.setItem("theme", "light"); localStorage.removeItem("notificationsSeen")`).catch(() => {});
      await browser.navigate(origin + scene.url);
      await browser.settle();
      const log = [];
      if (scene.keys) {
        await browser.key("?", "Slash", 191, 8, "?");
        await sleep(700);
      }
      for (const step of scene.steps ?? []) {
        log.push(await click(browser, step));
        await sleep(700);
      }
      result[scene.name] = { log, ...(await browser.evaluate(READ)) };
    }
  } finally {
    browser.close();
  }
  return result;
}

const port = Number(args.port ?? 9713);
const before = { ...(await read(args.before, port, true)), ...(await read(args.before, port, false)) };
const after = { ...(await read(args.after, port + 1, true)), ...(await read(args.after, port + 1, false)) };
const report = {};
let differing = 0;
for (const name of Object.keys(before)) {
  const textSame = JSON.stringify(before[name].text) === JSON.stringify(after[name].text);
  const namesSame = JSON.stringify(before[name].names) === JSON.stringify(after[name].names);
  if (!textSame || !namesSame) differing += 1;
  const onlyBefore = before[name].text.filter((line) => !after[name].text.includes(line));
  const onlyAfter = after[name].text.filter((line) => !before[name].text.includes(line));
  const namesOnlyBefore = before[name].names.filter((line) => !after[name].names.includes(line));
  const namesOnlyAfter = after[name].names.filter((line) => !before[name].names.includes(line));
  report[name] = {
    lines: after[name].text.length, names: after[name].names.length, textSame, namesSame,
    steps: { before: before[name].log, after: after[name].log },
    ...(textSame ? {} : { onlyBefore, onlyAfter }),
    ...(namesSame ? {} : { namesOnlyBefore, namesOnlyAfter }),
  };
  console.log(textSame && namesSame ? "SAME" : "DIFF", name, after[name].text.length, after[name].names.length);
}
writeFileSync(path.join(args.out, "text-identity.json"), JSON.stringify({ differing, scenes: report }, null, 1));
