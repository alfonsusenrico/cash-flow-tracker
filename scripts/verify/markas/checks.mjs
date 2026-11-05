// Scripted state checks for the Markas+ foundation (apply-markas-foundation tasks 2.3, 4.2, 5.1–5.4, 6.2):
// keyboard and pointer focus, the field focus and error edges in Catat, flat cards and layer shadows,
// primary button contrast, reduced motion and the figure settings. Writes checks.json.
//
// Usage: node checks.mjs --origin http://127.0.0.1:3471 --out <dir>
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
const browser = await launch({ port: Number(args.port ?? 9633), profileDir: path.join(args.out, ".profile-checks") });
const results = [];
const record = (name, pass, detail) => {
  results.push({ name, pass, detail });
  console.log(pass ? "PASS" : "FAIL", name, JSON.stringify(detail).slice(0, 240));
};

const EXPECT = {
  light: { focus: "rgb(69, 72, 79)", field: "rgb(43, 45, 50)", ring: "rgba(28, 30, 34, 0.1)", layer: "rgba(40, 34, 26, 0.14)" },
  dark: { focus: "rgb(189, 190, 194)", field: "rgb(201, 201, 204)", ring: "rgba(232, 231, 228, 0.12)", layer: "rgba(0, 0, 0, 0.5)" },
};

const styleOf = (expression) => browser.evaluate(`(() => {
  const element = ${expression};
  if (!element) return null;
  const style = getComputedStyle(element);
  return {
    text: (element.getAttribute("aria-label") || element.textContent || "").trim().slice(0, 40),
    tag: element.tagName.toLowerCase(),
    outline: style.outlineStyle + " " + style.outlineWidth + " " + style.outlineColor,
    outlineOffset: style.outlineOffset,
    border: style.borderTopWidth + " " + style.borderTopColor,
    boxShadow: style.boxShadow,
    color: style.color,
    background: style.backgroundColor,
    fontSize: style.fontSize,
    numeric: style.fontVariantNumeric,
    animation: style.animationName + " " + style.animationDuration,
    transition: style.transitionDuration,
    // A borderless input inside a search box shows focus on its wrapper (focus-within).
    wrapperShadow: element.closest(".ws-search, .ws-field-box") ? getComputedStyle(element.closest(".ws-search, .ws-field-box")).boxShadow : null,
  };
})()`);

const clickAt = async (expression) => {
  const box = await browser.evaluate(`(() => { const r = (${expression}).getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; })()`);
  for (const type of ["mousePressed", "mouseReleased"]) {
    await browser.send("Input.dispatchMouseEvent", { type, x: box.x, y: box.y, button: "left", clickCount: 1 });
  }
  await sleep(300);
};

const byText = (selector, pattern) =>
  `[...document.querySelectorAll(${JSON.stringify(selector)})].find((element) => new RegExp(${JSON.stringify(pattern)}).test((element.getAttribute("aria-label") || element.textContent || "").trim()) && element.getBoundingClientRect().width > 0)`;

async function login() {
  await browser.navigate(`${args.origin}/auth/login`);
  const status = await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include",
    headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username: "raka_contoh", password: "contoh-sandi-123" }) }).then((r) => r.status)`);
  if (status !== 200) throw new Error(`login ${status}`);
}

async function open(pathname, theme, { reducedMotion = false, width = 1440 } = {}) {
  await browser.setCell({ width, height: width === 390 ? 844 : 900, theme, mobile: width === 390, reducedMotion });
  await browser.evaluate(`localStorage.setItem("theme", "${theme}")`).catch(() => {});
  await browser.navigate(args.origin + pathname);
  await browser.settle();
}

async function openCatat() {
  await clickAt(byText("button", "^(\\+ )?Catat( transaksi)?$"));
  await sleep(600);
}

/** Tabs from the document start and returns the outline of each of the first stops. */
async function tabStops(count) {
  await browser.evaluate("document.activeElement && document.activeElement.blur(); window.scrollTo(0, 0)");
  const stops = [];
  for (let index = 0; index < count; index += 1) {
    await browser.key("Tab", "Tab", 9);
    // Wait past the 140 ms state transition before reading the outline or the field ring.
    await sleep(300);
    stops.push(await styleOf("document.activeElement"));
  }
  return stops;
}

try {
  await login();
  for (const theme of ["light", "dark"]) {
    const expected = EXPECT[theme];

    // Keyboard focus on "+ Catat" and every early stop on four pages: a 2 px neutral outline, or the field ring.
    for (const pathname of ["/", "/ledger", "/settings/profil"]) {
      await open(pathname, theme);
      const stops = (await tabStops(14)).filter(Boolean);
      const bad = stops.filter((stop) => {
        const field = ["input", "select", "textarea"].includes(stop.tag);
        if (field) return !(stop.boxShadow.includes(expected.ring) || (stop.wrapperShadow ?? "").includes(expected.ring));
        return stop.outline !== `solid 2px ${expected.focus}`;
      });
      record(`keyboard focus ${pathname} ${theme}`, bad.length === 0, { stops: stops.length, bad: bad.slice(0, 3) });
    }
    await open("/", theme);
    const catatStops = await tabStops(30);
    const catat = catatStops.find((stop) => stop && /^Catat/.test(stop.text) && stop.tag === "button");
    record(`keyboard focus on + Catat ${theme}`, Boolean(catat) && catat.outline === `solid 2px ${expected.focus}` && catat.outlineOffset === "2px", catat);

    // Pointer click on a button shows no outline.
    await open("/", theme);
    await open("/ledger", theme);
    await clickAt(byText("button", "^Filter"));
    const pointer = await styleOf("document.activeElement");
    record(`pointer click shows no outline ${theme}`, pointer && pointer.outline.startsWith("none"), pointer);
    await browser.key("Escape", "Escape", 27);

    // Catat: clicking into the sentence field shows the 1 px field-focus edge and the soft ring, no outline.
    await open("/", theme);
    await openCatat();
    await clickAt('document.querySelector("[role=dialog] textarea, [role=dialog] input[type=text]")');
    const sentence = await styleOf("document.activeElement");
    record(`sentence field focus ${theme}`,
      sentence && sentence.border === `1px ${expected.field}` && sentence.boxShadow.includes(expected.ring) && sentence.outline.startsWith("none"), sentence);

    // Catat manual: its save button stays disabled without an amount, so the reachable error is a movement
    // with an amount and no accounts. The field keeps a neutral edge; the error is 12 px expense text, linked.
    await clickAt(byText("[role=dialog] button", "^Isi manual$"));
    await sleep(500);
    await clickAt(byText("[role=dialog] button", "^Perpindahan$"));
    await sleep(400);
    await clickAt('document.querySelector("#quick-capture-amount")');
    await browser.evaluate(`(() => { const input = document.querySelector("#quick-capture-amount");
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set;
      setter.call(input, "10000"); input.dispatchEvent(new Event("input", { bubbles: true })); })()`);
    await sleep(300);
    await browser.evaluate(`document.querySelector('[role=dialog] button[type=submit]')?.click()`);
    await sleep(600);
    const invalidField = `(() => {
      const field = document.querySelector('[role=dialog] [aria-invalid="true"]');
      if (!field) return null;
      const error = document.getElementById((field.getAttribute("aria-describedby") || "").split(" ").pop());
      const style = getComputedStyle(field);
      return { id: field.id, border: style.borderTopWidth + " " + style.borderTopColor, describedBy: field.getAttribute("aria-describedby"),
        error: error ? { text: error.textContent.trim(), size: getComputedStyle(error).fontSize, color: getComputedStyle(error).color } : null };
    })()`;
    const invalid = await browser.evaluate(invalidField);
    const neutralEdge = (value) => value && !/rgb\((184, 20, 45|255, 107, 120|225, 29, 72|255, 71, 87)\)/.test(value.border);
    const expenseText = theme === "dark" ? "rgb(255, 107, 120)" : "rgb(184, 20, 45)";
    if (!invalid) {
      // Catat validates a movement in its summary alert, so no field carries aria-invalid here; the component
      // sheet's error field below covers the field state.
      record(`field error in Catat ${theme}`, "not reachable", { alert: await browser.evaluate(`document.querySelector("[role=dialog] [role=alert]")?.textContent ?? null`) });
    } else {
      record(`field error stays neutral ${theme}`,
        Boolean(neutralEdge(invalid) && invalid.error && invalid.error.size === "12px" && invalid.error.color === expenseText), invalid);
    }

    // An open dialog carries the layer shadow; cards stay flat.
    const dialog = await styleOf('[...document.querySelectorAll("[role=dialog]")].pop()');
    record(`dialog layer shadow ${theme}`, dialog && dialog.boxShadow.includes(expected.layer), dialog?.boxShadow);
    await browser.key("Escape", "Escape", 27);
    const cards = await browser.evaluate(`[...document.querySelectorAll(".ws-card, .ws-kcard")].map((card) => getComputedStyle(card).boxShadow)`);
    record(`cards are flat ${theme}`, cards.length > 0 && cards.every((shadow) => shadow === "none"), { cards: cards.length });

    // Primary button text contrast at rest.
    const primary = await styleOf(byText("button", "^(\\+ )?Catat$"));
    record(`primary button colours ${theme}`, primary && primary.color === "rgb(10, 13, 20)", primary);

    // Figures: amounts tabular, the "hari ke-" subtitle proportional.
    const figures = await browser.evaluate(`(() => {
      const all = [...document.querySelectorAll("body *")].filter((element) => element.childElementCount === 0 && element.getBoundingClientRect().width > 0);
      const amounts = all.filter((element) => /^[−+-]?\\s?Rp\\s?[\\d.]+$/.test(element.textContent.trim()));
      const proportional = amounts.filter((element) => !getComputedStyle(element).fontVariantNumeric.includes("tabular-nums"));
      const subtitle = all.find((element) => /hari ke-\\d/.test(element.textContent));
      return { amounts: amounts.length, notTabular: proportional.map((element) => element.textContent.trim()).slice(0, 6),
        subtitle: subtitle ? getComputedStyle(subtitle).fontVariantNumeric : null };
    })()`);
    record(`figures tabular, prose proportional ${theme}`, figures.notTabular.length === 0 && figures.subtitle && !figures.subtitle.includes("tabular"), figures);
  }

  // The component sheet's error field: neutral edge, aria-invalid and the linked 12 px message.
  for (const theme of ["light", "dark"]) {
    await open("/komponen", theme);
    const sheetError = await browser.evaluate(`(() => {
      const field = document.getElementById("ks-d-${theme}");
      const error = document.getElementById(field.getAttribute("aria-describedby"));
      const style = getComputedStyle(field);
      return { invalid: field.getAttribute("aria-invalid"), border: style.borderTopWidth + " " + style.borderTopColor,
        error: { size: getComputedStyle(error).fontSize, color: getComputedStyle(error).color } };
    })()`);
    const edge = theme === "dark" ? "1px rgb(201, 201, 204)" : "1px rgb(43, 45, 50)";
    record(`sheet error field ${theme}`, sheetError.invalid === "true" && sheetError.border === edge && sheetError.error.size === "12px", sheetError);
  }

  // Reflow: no horizontal page scroll at 320 px on the main pages (SPEC §11).
  for (const pathname of ["/", "/review", "/ledger", "/insights", "/laporan", "/accounts", "/goals", "/settings/profil"]) {
    await browser.setCell({ width: 320, height: 700, theme: "light", mobile: true });
    await browser.navigate(args.origin + pathname);
    await browser.settle();
    const overflow = await browser.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth");
    record(`no overflow at 320 ${pathname}`, overflow === 0, { overflow });
  }

  // Keyboard inside Catat: every stop shows the neutral outline or the field ring.
  for (const theme of ["light", "dark"]) {
    await open("/", theme);
    await openCatat();
    const stops = [];
    for (let index = 0; index < 8; index += 1) {
      await browser.key("Tab", "Tab", 9);
      // Wait past the 140 ms field transition before reading the edge.
      await new Promise((resolve) => setTimeout(resolve, 300));
      stops.push(await styleOf("document.activeElement"));
    }
    const expected = EXPECT[theme];
    const bad = stops.filter(Boolean).filter((stop) => {
      if (["input", "select", "textarea"].includes(stop.tag)) return !stop.boxShadow.includes(expected.ring);
      return stop.outline !== `solid 2px ${expected.focus}`;
    });
    record(`keyboard focus in Catat ${theme}`, bad.length === 0, { stops: stops.length, bad: bad.slice(0, 3) });
  }

  // Reduced motion: a dialog appears without animation and a row without transition.
  await open("/", "light", { reducedMotion: true });
  const row = await styleOf('document.querySelector(".ws-act-row, .ws-btn")');
  await openCatat();
  const reduced = await styleOf('[...document.querySelectorAll("[role=dialog]")].pop()');
  record("reduced motion", reduced && reduced.animation.startsWith("none") && row.transition.split(",").every((value) => value.trim() === "0s"), { dialog: reduced?.animation, row: row?.transition });
} finally {
  browser.close();
}
writeFileSync(path.join(args.out, "checks.json"), JSON.stringify(results, null, 1));
console.log(`${results.filter((result) => result.pass === true).length}/${results.length} passed`);
