import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

/*
 * Type scale guard (apply-markas-foundation D5, option-6 SPEC §1–§2): only Inter and Manrope, only the
 * seven sizes 12, 14, 16, 20, 24, 32 and 44 px and only the weights 400, 500 and 600, in the stylesheets,
 * the Tailwind configuration and the class strings of every component.
 */

const appDir = __dirname;
const srcDir = path.resolve(appDir, "..");
const frontendDir = path.resolve(srcDir, "..");

const SIZES = new Set(["12px", "14px", "16px", "20px", "24px", "32px", "44px"]);
const WEIGHTS = new Set(["400", "500", "600"]);

function filesUnder(dir: string, pattern: RegExp): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return filesUnder(full, pattern);
    return pattern.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [full] : [];
  });
}

const stylesheets = [path.join(appDir, "globals.css"), ...filesUnder(srcDir, /\.module\.css$/)];
const components = filesUnder(srcDir, /\.tsx?$/);

/** Every stray size, weight or family declaration in a stylesheet, as "line value". */
export function cssStrays(css: string): string[] {
  const found: string[] = [];
  css.replace(/\/\*[\s\S]*?\*\//g, (comment) => comment.replace(/[^\n]/g, " ")).split("\n").forEach((line, index) => {
    for (const match of line.matchAll(/font-size:\s*([^;}]+)/g)) {
      const value = match[1].trim();
      const scaleToken = /^var\(--fs-(12|14|16|20|24|32|44)\)$/.test(value);
      if (!scaleToken && !SIZES.has(value) && !["0", "inherit"].includes(value)) found.push(`${index + 1} font-size: ${value}`);
    }
    for (const match of line.matchAll(/font-weight:\s*([^;}]+)/g)) {
      const value = match[1].trim();
      if (!WEIGHTS.has(value) && value !== "inherit") found.push(`${index + 1} font-weight: ${value}`);
    }
    for (const match of line.matchAll(/font-family:\s*([^;}]+)/g)) {
      const value = match[1].trim();
      if (!/^var\(--font-(sans|display)\)$/.test(value) && value !== "inherit") found.push(`${index + 1} font-family: ${value}`);
    }
    for (const match of line.matchAll(/(?<![\w-])font:\s*([^;}]+)/g)) {
      if (match[1].trim() !== "inherit") found.push(`${index + 1} font: ${match[1].trim()}`);
    }
  });
  return found;
}

const STRAY_CLASS =
  /(?<![\w-])(?:[\w-]+:|\[[^\]]+\]:)*(?:text-\[\d+(?:\.\d+)?(?:px|rem|em)\]|text-(?:lg|5xl|6xl|7xl|8xl|9xl)|font-(?:thin|extralight|light|bold|extrabold|black|mono|serif|\[\d+\]))(?![\w-])/g;
const STRAY_INLINE = /\bfont(?:Size|Weight)\s*[:=]\s*\{?\s*["']?(\d+(?:\.\d+)?)(?:px)?["']?/g;

/** Every stray class or inline size and weight in a component source, as "line value". */
export function componentStrays(source: string): string[] {
  const found: string[] = [];
  source.split("\n").forEach((line, index) => {
    for (const match of line.matchAll(STRAY_CLASS)) found.push(`${index + 1} ${match[0]}`);
    for (const match of line.matchAll(STRAY_INLINE)) {
      const value = match[1];
      const isWeight = /fontWeight/.test(match[0]);
      if (isWeight ? !WEIGHTS.has(value) : !SIZES.has(`${value}px`)) found.push(`${index + 1} ${match[0]}`);
    }
  });
  return found;
}

describe("type scale", () => {
  it("names a planted stray size, weight or family", () => {
    expect(cssStrays(".x { font-size: 13.5px; font-weight: 700; }")).toEqual(["1 font-size: 13.5px", "1 font-weight: 700"]);
    expect(cssStrays(".x { font: 500 11.5px/1 var(--font-sans); font-family: Georgia; }")).toHaveLength(2);
    expect(cssStrays(".x { font-size: var(--fs-14); font-weight: 600; font-family: var(--font-display); }")).toEqual([]);
    expect(componentStrays('<p className="text-[15px] text-lg font-bold sm:font-black">')).toEqual([
      "1 text-[15px]",
      "1 text-lg",
      "1 font-bold",
      "1 sm:font-black",
    ]);
    expect(componentStrays("<text fontSize={11} style={{ fontWeight: 700 }} />")).toEqual(["1 fontSize={11", "1 fontWeight: 700"]);
    expect(componentStrays('<p className="text-xs font-semibold" style={{ fontSize: 20 }}>')).toEqual([]);
  });

  it("keeps every stylesheet on the seven sizes, three weights and two families", () => {
    const found = stylesheets.flatMap((file) =>
      cssStrays(readFileSync(file, "utf8")).map((stray) => `${path.relative(srcDir, file)}:${stray}`));
    expect(found).toEqual([]);
  });

  it("keeps every component on the seven sizes, three weights and two families", () => {
    const found = components.flatMap((file) =>
      componentStrays(readFileSync(file, "utf8")).map((stray) => `${path.relative(srcDir, file)}:${stray}`));
    expect(found).toEqual([]);
  });

  it("generates no other Tailwind size or weight", () => {
    const config = readFileSync(path.join(frontendDir, "tailwind.config.js"), "utf8");
    const sizes = [...config.matchAll(/var\(--fs-(\d+)\)/g)].map((match) => `${match[1]}px`);
    expect(new Set(sizes)).toEqual(SIZES);
    const weights = config.match(/fontWeight: \{([^}]*)\}/)![1];
    expect(new Set([...weights.matchAll(/"(\d{3})"/g)].map((match) => match[1]))).toEqual(WEIGHTS);
  });
});
