// Per-primitive checklist: measures the same primitive on the prototype's #/komponen and the app's /komponen
// in both themes (height, paddings, radius, border, font, colours) and writes primitives.json with every
// property that differs.
//
// Usage: node primitives.mjs --prototype http://127.0.0.1:3481/option-6 --app http://127.0.0.1:3471 --out <dir>
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { launch } from "./cdp.mjs";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 2) args[argv[index].replace(/^--/, "")] = argv[index + 1];
  return args;
}

const args = parseArgs(process.argv.slice(2));
mkdirSync(args.out, { recursive: true });

// Primitive: [prototype selector, app selector], each the first match inside the light or the dark cell.
const PRIMITIVES = {
  "primary button": [".btn.btn-primary:not([class*=is-])", ".ws-btn.ws-btn-accent:not([class*=is])"],
  "secondary button": [".btn.btn-secondary:not([class*=is-])", ".ws-btn:not(.ws-btn-accent):not(.ws-btn-quiet):not(.ws-btn-danger):not([class*=is])"],
  "quiet button": [".btn.btn-quiet:not([class*=is-])", ".ws-btn.ws-btn-quiet:not([class*=is])"],
  "destructive button": [".btn.btn-danger:not([class*=is-])", ".ws-btn.ws-btn-danger:not([class*=is])"],
  "icon button": [".icon-btn:not([class*=is-])", ".ws-icon-btn:not([class*=is])"],
  "link button": [".btn.btn-link", ".ws-linkbtn"],
  "key cap": [".kbd", ".ws-kbd"],
  "input": ["input.input:not(.is-focus):not(.is-error):not(.input-money)", 'input[id^="ks-a-"]'],
  "input focus": ["input.input.is-focus", 'input[id^="ks-c-"]'],
  "input error": ["input.input.is-error", 'input[id^="ks-d-"]'],
  "select": ["select.select", 'select[id^="ks-e-"]'],
  "money input": ["input.input-money", 'input[id^="ks-f-"]'],
  "field label": [".field > label", 'label[for^="ks-a-"]'],
  "field error": [".field .err", '[id^="ks-d-"][id$="-error"]'],
  "switch on": ['.switch[aria-checked="true"]', '[aria-checked="true"] > .ws-switch-track'],
  "chip": [".chip", "span.ws-chip"],
  "segmented track": [".seg", ".ws-seg"],
  "segment selected": ['.seg button[aria-pressed="true"]', '.ws-seg button[aria-pressed="true"]'],
  "segment": ['.seg button[aria-pressed="false"]', '.ws-seg button[aria-pressed="false"]'],
  "badge": [".badge.warn", ".ws-pill.ws-tone-warn"],
  "toast": [".toast", ".ws-toast"],
  "dialog": [".dialog.ks-static:not([style*='390'])", ".ws-drawer:not([class*=sheet])"],
  "dialog title": [".dialog .t-dtitle", ".ws-drawer-h h2"],
  "empty title": [".empty .t-strong", ".ws-empty h2"],
  "empty line": [".empty .t-meta", ".ws-empty p"],
  "skeleton bar": [".skel", ".ws-skeleton"],
  "error box": [".error-box", ".ws-error"],
};
const PROPERTIES = ["height", "paddingTop", "paddingRight", "paddingBottom", "paddingLeft", "borderTopLeftRadius", "borderTopWidth", "borderTopColor",
  "fontFamily", "fontSize", "fontWeight", "lineHeight", "letterSpacing", "color", "backgroundColor", "boxShadow"];

async function measure(origin, route, isPrototype) {
  const browser = await launch({ port: Number(args.port ?? 9636), profileDir: path.join(args.out, `.profile-${isPrototype ? "p" : "a"}`) });
  try {
    if (!isPrototype) {
      await browser.navigate(`${origin}/auth/login`);
      await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username: "raka_contoh", password: "contoh-sandi-123" }) })`);
    }
    await browser.setCell({ width: 1440, height: 900, theme: "light" });
    await browser.navigate(origin + route);
    await browser.settle();
    const index = isPrototype ? 0 : 1;
    return await browser.evaluate(`(() => {
      const primitives = ${JSON.stringify(Object.fromEntries(Object.entries(PRIMITIVES).map(([name, pair]) => [name, pair[index]])))};
      const properties = ${JSON.stringify(PROPERTIES)};
      const cellsOf = (theme) => ${isPrototype}
        ? [...document.querySelectorAll(".ks-cell.theme-" + theme)]
        : [...document.querySelectorAll("[class*=komponen_cell]")].filter((cell) => cell.classList.contains("dark") === (theme === "dark"));
      const result = {};
      for (const [name, selector] of Object.entries(primitives)) {
        for (const theme of ["light", "dark"]) {
          const element = cellsOf(theme).map((cell) => cell.querySelector(selector)).find(Boolean);
          if (!element) { result[name + " " + theme] = null; continue; }
          const style = getComputedStyle(element);
          const box = element.getBoundingClientRect();
          const valueOf = (property) => {
            if (property === "height") return Math.round(box.height) + "px";
            if (property !== "fontFamily") return style[property];
            const first = style.fontFamily.split(",")[0].replace(/["']/g, "").trim().toLowerCase();
            if (first.startsWith("inter")) return "inter";
            if (first.startsWith("manrope")) return "manrope";
            return first;
          };
          result[name + " " + theme] = Object.fromEntries(properties.map((property) => [property, valueOf(property)]));
        }
      }
      return result;
    })()`);
  } finally {
    browser.close();
  }
}

const prototype = await measure(args.prototype, "/index.html#/komponen", true);
const app = await measure(args.app, "/komponen", false);
const report = {};
let same = 0;
let differing = 0;
for (const key of Object.keys(prototype)) {
  if (!prototype[key] || !app[key]) {
    report[key] = { missing: !prototype[key] ? "prototype" : "app" };
    continue;
  }
  // A border colour is invisible when both borders are 0 px wide.
  const noBorder = prototype[key].borderTopWidth === "0px" && app[key].borderTopWidth === "0px";
  const compared = PROPERTIES.filter((property) => !(noBorder && property === "borderTopColor"));
  const diff = Object.fromEntries(compared.filter((property) => prototype[key][property] !== app[key][property])
    .map((property) => [property, { prototype: prototype[key][property], app: app[key][property] }]));
  same += compared.length - Object.keys(diff).length;
  differing += Object.keys(diff).length;
  report[key] = Object.keys(diff).length ? diff : "match";
}
writeFileSync(path.join(args.out, "primitives.json"), JSON.stringify({ summary: { same, differing }, report }, null, 1));
console.log(JSON.stringify({ same, differing }));
for (const [key, value] of Object.entries(report)) if (value !== "match") console.log(key, JSON.stringify(value).slice(0, 400));
