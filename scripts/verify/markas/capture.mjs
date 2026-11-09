// Captures app or prototype routes and dialogs at 1440 × 900 and 390 × 844 in both themes, and writes
// checklist.json with the in-page measurements (measure.js), axe-core violations, horizontal overflow
// and console errors for every capture.
//
// Usage:
//   node capture.mjs --target app --origin http://127.0.0.1:3471 --out <dir> [--only <regex>] [--skip <regex>] [--cells 1440-light,390-dark]
//   node capture.mjs --target prototype --origin http://127.0.0.1:3481 --out <dir> [--only <regex>]
// The app target logs in as the synthetic seed owner (MARKAS_USER, MARKAS_PASSWORD; defaults below). With
// `--signed-out 1` it stays signed out and runs only the scenarios marked `signedOut` (the sign-in page), with
// `--language en|id` as the browser's remembered language (add-language-system D16). Every capture records the
// document language and every element that declares another one.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { launch, sleep } from "./cdp.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
if (!args.target || !args.origin || !args.out) {
  console.error("Usage: node capture.mjs --target app|prototype --origin <url> --out <dir> [--only <regex>] [--cells <list>]");
  process.exit(2);
}

const CELLS = [
  { name: "1440-light", width: 1440, height: 900, theme: "light" },
  { name: "1440-dark", width: 1440, height: 900, theme: "dark" },
  { name: "390-light", width: 390, height: 844, theme: "light", mobile: true },
  { name: "390-dark", width: 390, height: 844, theme: "dark", mobile: true },
];
// Extra widths for the shell's breakpoints (apply-markas-shell D2) and Beranda's one-column range
// (apply-markas-beranda D1), only when named in --cells.
const EXTRA_WIDTHS = [1920, 1400, 1380, 1280, 1200, 1199, 1100, 1024, 1023, 768, 767, 320];
const EXTRA_CELLS = EXTRA_WIDTHS.flatMap((width) =>
  ["light", "dark"].map((theme) => ({ name: `${width}-${theme}`, width, height: width < 768 ? 844 : width === 1024 ? 768 : 900, theme, mobile: width < 768 })),
);
const cells = args.cells ? [...CELLS, ...EXTRA_CELLS].filter((cell) => args.cells.split(",").includes(cell.name)) : CELLS;
const scenarios = JSON.parse(readFileSync(path.join(here, `scenarios-${args.target}.json`), "utf8"));
const measureSource = readFileSync(path.join(here, "measure.js"), "utf8");
const shellSource = readFileSync(path.join(here, "shell-checklist.js"), "utf8");
const berandaSource = readFileSync(path.join(here, "beranda-checklist.js"), "utf8");
const tinjauSource = readFileSync(path.join(here, "tinjau-checklist.js"), "utf8");
const transaksiSource = readFileSync(path.join(here, "transaksi-checklist.js"), "utf8");
const analisisSource = readFileSync(path.join(here, "analisis-checklist.js"), "utf8");
const rekeningSource = readFileSync(path.join(here, "rekening-checklist.js"), "utf8");
const laporanSource = readFileSync(path.join(here, "laporan-checklist.js"), "utf8");
const targetSource = readFileSync(path.join(here, "target-checklist.js"), "utf8");
const pengaturanSource = readFileSync(path.join(here, "pengaturan-checklist.js"), "utf8");
const catatSource = readFileSync(path.join(here, "catat-checklist.js"), "utf8");
// Settings a scenario may set (`storage`); every capture starts without them, so no scenario leaks into the next.
const STORAGE_KEYS = { app: ["hideBalances", "sidebarCollapsed", "rekening-grouping"], prototype: ["markas6-hide"] };
mkdirSync(args.out, { recursive: true });
const reportFile = path.join(args.out, "checklist.json");

const browser = await launch({ port: Number(args.port ?? 9631), profileDir: path.join(args.out, ".profile") });

// Clicks the smallest visible element whose label or text matches, inside the open dialog when there is one.
const clickMatching = (pattern) =>
  browser.evaluate(`(async () => {
    const re = new RegExp(${JSON.stringify(pattern)}, "i");
    const dialogs = [...document.querySelectorAll('[role=dialog]')].filter((d) => d.getBoundingClientRect().width > 0);
    const scope = dialogs.length ? dialogs[dialogs.length - 1] : document;
    const pool = [...scope.querySelectorAll("button, a, [role=button], [role=tab], [role=option], li, tr, [tabindex]")];
    const matches = pool.filter((element) => {
      const box = element.getBoundingClientRect();
      const label = ((element.getAttribute("aria-label") || "") + " " + element.textContent.replace(/\\s+/g, " ")).trim();
      return box.width > 0 && box.height > 0 && re.test(label);
    });
    if (!matches.length) return "NOT FOUND";
    matches.sort((a, b) => {
      const first = a.getBoundingClientRect();
      const second = b.getBoundingClientRect();
      return first.width * first.height - second.width * second.height;
    });
    const element = matches[0];
    const box = element.getBoundingClientRect();
    if (box.top < 60 || box.bottom > innerHeight - 60) {
      element.scrollIntoView({ block: "center" });
      await new Promise((resolve) => setTimeout(resolve, 400));
    }
    element.click();
    return "clicked " + (element.getAttribute("aria-label") || element.textContent.trim()).slice(0, 50);
  })()`);

async function runSteps(steps, log) {
  for (const step of steps) {
    if (step.wait) {
      await sleep(step.wait);
    } else if (step.key) {
      await browser.key(...step.key);
      await sleep(800);
      log.push(`key ${step.key[0]}`);
    } else if (step.eval) {
      log.push(String(await browser.evaluate(step.eval)));
      await sleep(step.after ?? 600);
    } else if (step.click) {
      let result = "NOT FOUND";
      for (let attempt = 0; attempt < 20 && result === "NOT FOUND"; attempt += 1) {
        result = await clickMatching(step.click);
        if (result === "NOT FOUND") await sleep(400);
      }
      log.push(result);
      await sleep(step.after ?? 900);
    }
  }
}

async function login() {
  await browser.navigate(`${args.origin}/auth/login`);
  const username = process.env.MARKAS_USER ?? "raka_contoh";
  const password = process.env.MARKAS_PASSWORD ?? "contoh-sandi-123";
  const status = await browser.evaluate(`fetch("/api/auth/login", {
    method: "POST", credentials: "include", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(${JSON.stringify({ username, password })}),
  }).then((response) => response.status)`);
  if (status !== 200) throw new Error(`login returned ${status}`);
}

function urlFor(scenario, cell) {
  if (args.target === "prototype") {
    const word = cell.theme === "dark" ? "gelap" : "terang";
    return `${args.origin}/index.html#${scenario.path}${scenario.path.includes("?") ? "&" : "?"}tema=${word}`;
  }
  return args.origin + scenario.path;
}

const report = {};
try {
  if (args.target === "app" && !args["signed-out"]) await login();
  // Signed out, storage and the language cookie need the app's origin before the first scenario.
  if (args.target === "app" && args["signed-out"]) await browser.navigate(`${args.origin}/auth/login`);
  for (const cell of cells) {
    await browser.setCell(cell);
    for (const scenario of scenarios) {
      if (args.only && !new RegExp(args.only).test(scenario.name)) continue;
      if (args.target === "app" && Boolean(scenario.signedOut) !== Boolean(args["signed-out"])) continue;
      if (args.skip && new RegExp(args.skip).test(scenario.name)) continue;
      if (scenario.cells && !scenario.cells.includes(cell.name)) continue;
      const key = `${scenario.name}-${cell.width}-${cell.theme}`;
      browser.consoleErrors.length = 0;
      const storage = `${JSON.stringify(STORAGE_KEYS[args.target])}.forEach((name) => localStorage.removeItem(name));
        Object.entries(${JSON.stringify(scenario.storage ?? {})}).forEach(([name, value]) => localStorage.setItem(name, value))`;
      if (args.target === "app") {
        // Every capture starts with unseen notifications, so the bell's count does not depend on the order.
        await browser.evaluate(`localStorage.setItem("theme", "${cell.theme}"); localStorage.removeItem("notificationsSeen"); ${storage}`).catch(() => {});
        if (args.language) await browser.evaluate(`document.cookie = "cft_language=${args.language}; Path=/; SameSite=Lax"`).catch(() => {});
      } else {
        await browser.navigate(`${args.origin}/styles.css`);
        await browser.evaluate(storage).catch(() => {});
        await browser.navigate("about:blank");
      }
      await browser.navigate(urlFor(scenario, cell));
      await browser.settle();
      const log = [];
      if (scenario.steps) await runSteps(scenario.steps, log);
      await sleep(300);
      const hasDialog = await browser.evaluate(
        "Boolean([...document.querySelectorAll('[role=dialog], [role=alertdialog], dialog[open]')].find((d) => d.getBoundingClientRect().width > 0))",
      );
      const fullPage = !scenario.steps || scenario.full;
      if (fullPage) {
        await browser.evaluate("window.scrollTo(0, 0)");
        await sleep(200);
        await browser.shot(path.join(args.out, `${key}.png`), { full: true });
      } else {
        await browser.shot(path.join(args.out, `${key}.png`));
      }
      await browser.evaluate(`window.__measureRoot = ${JSON.stringify(scenario.measureRoot ?? null)}`);
      const measure = await browser.evaluate(measureSource).catch((error) => ({ error: String(error).slice(0, 200) }));
      const shell = scenario.shell ? await browser.evaluate(shellSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const beranda = scenario.beranda ? await browser.evaluate(berandaSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const tinjau = scenario.tinjau ? await browser.evaluate(tinjauSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const transaksi = scenario.transaksi ? await browser.evaluate(transaksiSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const analisis = scenario.analisis ? await browser.evaluate(analisisSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const rekening = scenario.rekening ? await browser.evaluate(rekeningSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const laporan = scenario.laporan ? await browser.evaluate(laporanSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const target = scenario.target ? await browser.evaluate(targetSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const pengaturan = scenario.pengaturan ? await browser.evaluate(pengaturanSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      const catat = scenario.catat ? await browser.evaluate(catatSource).catch((error) => ({ error: String(error).slice(0, 200) })) : undefined;
      // The document language and every interface part that declares its own (WCAG 3.1.1 and 3.1.2).
      const lang = await browser.evaluate(`({
        root: document.documentElement.lang,
        parts: [...document.querySelectorAll("body [lang]")].filter((element) => element.getBoundingClientRect().width > 0)
          .map((element) => element.tagName.toLowerCase() + (element.id ? "#" + element.id : "") + "[" + element.lang + "] " + (element.textContent || "").trim().slice(0, 40)),
        notice: Boolean([...document.querySelectorAll(".untranslated-note")].find((element) => element.getBoundingClientRect().width > 0)),
        overflowX: document.documentElement.scrollWidth > innerWidth,
      })`).catch((error) => ({ error: String(error).slice(0, 120) }));
      const violations = await browser.axe().catch((error) => [{ id: "axe-error", impact: "critical", targets: [String(error).slice(0, 120)] }]);
      report[key] = {
        path: scenario.path,
        log,
        hasDialog,
        consoleErrors: browser.consoleErrors.slice(0, 5),
        axeSeriousOrCritical: violations
          .filter((violation) => ["serious", "critical"].includes(violation.impact))
          .map((violation) => `${violation.id} (${violation.nodes}) ${violation.targets.join(" | ")}`.slice(0, 300)),
        axeOther: violations.filter((violation) => !["serious", "critical"].includes(violation.impact)).map((violation) => violation.id),
        measure,
        shell,
        beranda,
        tinjau,
        transaksi,
        analisis,
        rekening,
        laporan,
        target,
        pengaturan,
        catat,
        lang,
      };
      const flags = report[key].axeSeriousOrCritical.length + report[key].consoleErrors.length;
      console.log(key, hasDialog ? "dialog" : "", JSON.stringify(log), flags ? `flags ${flags}` : "");
      writeFileSync(reportFile, JSON.stringify(report, null, 1));
      // A scenario that leaves state behind (an open test session) undoes it, so the next capture starts clean.
      if (scenario.cleanup) console.log(key, "cleanup", await browser.evaluate(scenario.cleanup).catch((error) => String(error).slice(0, 80)));
    }
  }
} finally {
  browser.close();
}
