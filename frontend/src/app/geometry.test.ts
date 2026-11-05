import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

/*
 * Geometry guard (apply-markas-foundation D6–D8, option-6 SPEC §5): radii 0, 6, 12, 16 or a circle; every
 * border 1 px in a neutral line token; no coloured outline or ring except focus; shadows only on floating
 * layers; no focus ring utilities, because the global rules draw the neutral focus.
 */

const appDir = __dirname;
const srcDir = path.resolve(appDir, "..");

function filesUnder(dir: string, pattern: RegExp): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return filesUnder(full, pattern);
    return pattern.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [full] : [];
  });
}

const stylesheets = [path.join(appDir, "globals.css"), ...filesUnder(srcDir, /\.module\.css$/)];
const components = filesUnder(srcDir, /\.tsx?$/);

const RADII = /^(?:0|0px|50%|9999px|inherit|var\(--r-(?:control|panel|layer)\))$/;
// "Belum diklasifikasi" (--k-none) is the neutral dashed marker, so it is not a semantic colour here.
const SEMANTIC = "(?:accent|accent-fg|accent-hover|in|out|tr|inv|warn|ok|danger)(?:-(?:solid|fg|bg|dot))?|k-(?:need|want|culture|unexpected|saving)|text|ink";
// Layers that may cast the layer shadow, and the two allowed rings: the field ring and the segment hairline.
const SHADOW_OK = /^(?:none|var\(--shadow-float\)|0 0 0 3px var\(--ring\)|0 0 0 1px var\(--(?:line|text)\))$/;

function rulesOf(css: string): { selector: string; body: string }[] {
  const plain = css.replace(/\/\*[\s\S]*?\*\//g, "");
  return [...plain.matchAll(/([^{}]+)\{([^{}]*)\}/g)].map((match) => ({ selector: match[1].trim(), body: match[2] }));
}

/** Every rule of a stylesheet that breaks the geometry rules, as "selector: declaration". */
export function cssViolations(css: string): string[] {
  const found: string[] = [];
  for (const { selector, body } of rulesOf(css)) {
    if (selector.startsWith("@")) continue;
    for (const match of body.matchAll(/border-radius:\s*([^;]+)/g)) {
      const values = match[1].trim().match(/var\([^)]*\)|\S+/g) ?? [];
      if (!values.every((value) => RADII.test(value))) found.push(`${selector}: border-radius: ${match[1].trim()}`);
    }
    for (const match of body.matchAll(/(?<![\w-])border(?:-(?:top|right|bottom|left))?(?:-width)?:\s*([^;]+)/g)) {
      const width = match[1].match(/(\d+(?:\.\d+)?)px/);
      if (width && Number(width[1]) >= 2) found.push(`${selector}: border ${match[1].trim()}`);
      if (new RegExp(`var\\(--(?:${SEMANTIC})\\)`).test(match[1])) found.push(`${selector}: coloured border ${match[1].trim()}`);
    }
    for (const match of body.matchAll(/border(?:-(?:top|right|bottom|left))?-color:\s*([^;]+)/g)) {
      if (new RegExp(`var\\(--(?:${SEMANTIC})\\)`).test(match[1])) found.push(`${selector}: coloured border ${match[1].trim()}`);
    }
    for (const match of body.matchAll(/(?<![\w-])outline:\s*([^;]+)/g)) {
      const value = match[1].replace(/\s*!important$/, "").trim();
      // Real focus rules, and the component sheet's simulated focus state.
      const focusRule = /:focus|isFocus/.test(selector);
      const ok = value === "none" || value === "0" || (focusRule && value === "2px solid var(--focus)") || /^1px (?:solid|dashed) var\(--(?:line-strong|text)\)$/.test(value);
      if (!ok) found.push(`${selector}: outline: ${value}`);
    }
    for (const match of body.matchAll(/box-shadow:\s*([^;]+)/g)) {
      const value = match[1].trim();
      if (!SHADOW_OK.test(value)) found.push(`${selector}: box-shadow: ${value}`);
    }
  }
  return found;
}

const CLASS_VIOLATION = new RegExp(
  [
    "(?<![\\w-])(?:[\\w-]+:|\\[[^\\]]+\\]:)*(?:",
    "rounded(?:-[a-z]{1,2})?-\\[(?!0\\])[^\\]]+\\]",
    "|border(?:-[lrtbxy])?-(?:[2-9]|\\[\\d+(?:\\.\\d+)?px\\])",
    "|border(?:-[lrtbxy])?-(?:in|out|tr|inv|warn|ok)-(?:solid|fg|bg)(?:/\\d+)?",
    `|border(?:-[lrtbxy])?-\\[var\\(--(?:${SEMANTIC})\\)\\]`,
    "|ring(?:-[\\w[\\]()/.%-]+)?",
    "|shadow-(?!float(?![\\w-])|none(?![\\w-]))[\\w[\\]()/.%-]+",
    "|shadow(?![\\w-])",
    "|outline-(?!none(?![\\w-]))[\\w[\\]()/.%-]+",
    "|backdrop-blur(?:-[\\w-]+)?",
    ")(?![\\w-])",
  ].join(""),
  "g",
);

/** Every class token in a component source that breaks the geometry rules, as "line token". */
export function componentViolations(source: string): string[] {
  const found: string[] = [];
  source.split("\n").forEach((line, index) => {
    // Only quoted class text; prose such as "string" or "ringkas" never starts a class token.
    for (const literal of line.matchAll(/["'`]([^"'`]*)["'`]/g)) {
      for (const match of literal[1].matchAll(CLASS_VIOLATION)) found.push(`${index + 1} ${match[0]}`);
    }
  });
  return found;
}

describe("geometry, borders and elevation", () => {
  it("names a planted thick or coloured border, coloured ring, stray radius or card shadow", () => {
    expect(cssViolations(".card { border: 2px solid var(--line); border-radius: 14px; box-shadow: var(--shadow-card); }")).toHaveLength(3);
    expect(cssViolations(".field:focus { outline: 2px solid var(--accent-fg); border-color: var(--out-solid); }")).toHaveLength(2);
    expect(cssViolations(".ok { border: 1px solid var(--line); border-radius: var(--r-panel); }\n.ok:focus-visible { outline: 2px solid var(--focus); }")).toEqual([]);
    expect(componentViolations('className="rounded-[18px] border-2 border-out-solid/20 focus-visible:ring-2 shadow-sm"')).toEqual([
      "1 rounded-[18px]",
      "1 border-2",
      "1 border-out-solid/20",
      "1 focus-visible:ring-2",
      "1 shadow-sm",
    ]);
    expect(componentViolations('className="rounded-md border border-[var(--line)] shadow-float" title="string ringkas"')).toEqual([]);
  });

  it("keeps every stylesheet on the radii, 1 px neutral borders, neutral focus and layer shadows", () => {
    const found = stylesheets.flatMap((file) =>
      cssViolations(readFileSync(file, "utf8")).map((violation) => `${path.relative(srcDir, file)}: ${violation}`));
    expect(found).toEqual([]);
  });

  it("uses no thick or coloured border, ring, focus outline, stray radius, blur or card shadow in a component", () => {
    const found = components.flatMap((file) =>
      componentViolations(readFileSync(file, "utf8")).map((violation) => `${path.relative(srcDir, file)}:${violation}`));
    expect(found).toEqual([]);
  });
});
