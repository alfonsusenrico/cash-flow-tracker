import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

/*
 * Straight selection marks (apply-markas-foundation D10, taken over from fix/selection-bars-and-tinjau 64909ee):
 * a left border or a left inset shadow on a selection rule bends with rounded corners, so selected rows use the
 * shared `.ws-select-row` bar, navigation and rail items the `.ws-select-bar` and groups the `.ws-group-rail`,
 * each drawn as its own square-ended element.
 */

const srcDir = path.resolve(__dirname, "..");
const globals = readFileSync(path.join(__dirname, "globals.css"), "utf8");

function filesUnder(dir: string, pattern: RegExp): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return filesUnder(full, pattern);
    return pattern.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [full] : [];
  });
}

const SELECTION = /\[(aria-pressed|aria-current|aria-selected|data-selected)/;
const LEFT_BORDER = /(^|[;\s{])border-left\s*:/;
const LEFT_INSET = /inset\s+[1-9]\d*(\.\d+)?px\s+0(px)?\s+0/;

/** The rules of a stylesheet that draw a left mark able to curve: any left inset shadow, or a left border on a selection rule. */
function curvedMarks(css: string): string[] {
  const rules = [...css.replace(/\/\*[\s\S]*?\*\//g, "").matchAll(/([^{}]+)\{([^{}]*)\}/g)];
  return rules
    .filter(([, selector, body]) => LEFT_INSET.test(body) || (SELECTION.test(selector) && LEFT_BORDER.test(body)))
    .map(([, selector]) => selector.trim());
}

function ruleBody(css: string, selector: string): string {
  const start = css.indexOf(`${selector} {`);
  expect(start, `${selector} is missing`).toBeGreaterThanOrEqual(0);
  return css.slice(start, css.indexOf("}", start));
}

describe("straight selection marks", () => {
  it("finds a planted curved mark", () => {
    expect(curvedMarks(".row[aria-pressed=\"true\"] { border-radius: 12px; box-shadow: inset 3px 0 0 var(--ink); }")).toEqual([".row[aria-pressed=\"true\"]"]);
    expect(curvedMarks(".row[data-selected] { border-left: 3px solid red; }")).toEqual([".row[data-selected]"]);
    expect(curvedMarks(".day.isToday { box-shadow: inset 0 0 0 2px var(--accent-fg); }")).toEqual([]);
  });

  it("has no curved left mark in any stylesheet", () => {
    const sheets = [path.join(__dirname, "globals.css"), ...filesUnder(srcDir, /\.module\.css$/)];
    const found = sheets.flatMap((file) => curvedMarks(readFileSync(file, "utf8")).map((selector) => `${path.relative(srcDir, file)}: ${selector}`));
    expect(found).toEqual([]);
  });

  it("uses no Tailwind left-border utility or inline left inset shadow in a component", () => {
    const found = filesUnder(srcDir, /\.tsx?$/).filter((file) => {
      const source = readFileSync(file, "utf8");
      return /\bborder-l(?=[-\s"'`])/.test(source) || /boxShadow:\s*["'`]inset\s+[1-9]/.test(source);
    });
    expect(found.map((file) => path.relative(srcDir, file))).toEqual([]);
  });

  it("draws the shared bar, the navigation bar and the rail as square-ended 2 px elements in the prototype's geometry", () => {
    const row = ruleBody(globals, '.ws-select-row:is([aria-pressed="true"], [aria-current="true"], [aria-selected="true"], [data-selected])::before');
    expect(row).toMatch(/position: absolute/);
    expect(row).toMatch(/top: 0;[\s\S]*bottom: 0;[\s\S]*left: 0;/);
    expect(row).toMatch(/width: 2px/);
    expect(row).toMatch(/border-radius: 0/);
    expect(row).toMatch(/background: var\(--accent\)/);
    expect(ruleBody(globals, ".ws-select-row")).toMatch(/border-radius: 0/);

    const bar = ruleBody(globals, '.ws-select-bar:is([aria-current="page"], [aria-current="true"])::before');
    expect(bar).toMatch(/left: -8px/);
    expect(bar).toMatch(/width: 2px/);
    expect(bar).toMatch(/height: 20px/);
    expect(bar).toMatch(/border-radius: 0/);

    const rail = ruleBody(globals, ".ws-group-rail::before");
    expect(rail).toMatch(/left: 0/);
    expect(rail).toMatch(/width: 2px/);
    expect(rail).toMatch(/border-radius: 0/);
    expect(rail).toMatch(/background: var\(--rail\)/);
  });

  it("marks the goal account picker's chosen pocket, the Grafik legend, the Tinjau rows and the rail with the shared bars", () => {
    const source = (file: string) => readFileSync(path.join(srcDir, file), "utf8");
    expect(source("components/goals/GoalDialogs.tsx")).toMatch(/data-selected=\{isChildChecked \? "" : undefined\}[\s\S]{0,120}ws-select-row/);
    expect(source("components/goals/GoalDialogs.tsx")).toContain('className="ws-group-rail ml-4');
    expect(source("components/accounts/RekeningChart.tsx")).toContain('className="ws-select-row" aria-pressed=');
    expect(source("components/workspace/lists.tsx")).toMatch(/cn\("row", "ws-select-row", layout === "stacked" && "is-stacked"\)/);
    expect(source("components/review/triage/TriageList.tsx")).toMatch(/<ListRow\s+layout="stacked"[\s\S]*current=\{item\.id === selectedId\}/);
    expect(source("components/workspace/rail.tsx")).toContain('className="rail-item ws-select-bar"');
    // A rounded phone chip carries no coloured edge: its bar is removed and the fill marks it.
    expect(globals).toContain(".rail.is-chips .rail-item::before { content: none; }");
  });
});
