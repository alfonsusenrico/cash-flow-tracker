import { readdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import resolveConfig from "tailwindcss/resolveConfig";
import { describe, expect, it } from "vitest";

const appDir = path.resolve(__dirname);
const srcDir = path.resolve(appDir, "..");
const tokens = readFileSync(path.resolve(appDir, "../../tokens.css"), "utf8");
const globals = readFileSync(path.join(appDir, "globals.css"), "utf8");
const layout = readFileSync(path.join(appDir, "layout.tsx"), "utf8");

const declaredIn = (source: string) => new Set([...source.matchAll(/(--[\w-]+)\s*:/g)].map((match) => match[1]));
const usedIn = (source: string) => new Set([...source.matchAll(/var\(\s*(--[\w-]+)/g)].map((match) => match[1]));

function componentSources(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return componentSources(full);
    return /\.tsx?$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [readFileSync(full, "utf8")] : [];
  });
}

function block(selector: string): Record<string, string> {
  const start = tokens.indexOf(`${selector} {`);
  expect(start).toBeGreaterThanOrEqual(0);
  const body = tokens.slice(start, tokens.indexOf("\n}", start));
  return Object.fromEntries([...body.matchAll(/(--[\w-]+)\s*:\s*([^;]+);/g)].map((match) => [match[1], match[2].trim()]));
}

const themes = {
  ":root": block(":root"),
  ".dark": { ...block(":root"), ...block(".dark") },
};

function resolve(theme: Record<string, string>, name: string): string {
  const value = theme[name];
  expect(value, `${name} is not defined`).toBeDefined();
  const reference = value.match(/^var\((--[\w-]+)\)$/);
  return reference ? resolve(theme, reference[1]) : value;
}

type Rgb = [number, number, number];

function hex(value: string): Rgb {
  const channels = value.replace("#", "").match(/../g);
  expect(channels, `${value} is not a #RRGGBB colour`).toHaveLength(3);
  return channels!.map((channel) => parseInt(channel, 16)) as Rgb;
}

/** A token colour as painted: a translucent rgba() value is composited over the theme surface. */
function painted(theme: Record<string, string>, name: string): Rgb {
  const value = resolve(theme, name);
  const rgba = value.match(/^rgba\((\d+),\s*(\d+),\s*(\d+),\s*([\d.]+)\)$/);
  if (!rgba) return hex(value);
  const surface = hex(resolve(theme, "--surface"));
  const alpha = Number(rgba[4]);
  return surface.map((channel, index) => Number(rgba[index + 1]) * alpha + channel * (1 - alpha)) as Rgb;
}

function luminance(colour: Rgb): number {
  const [r, g, b] = colour.map((channel) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(first: Rgb, second: Rgb): number {
  const [light, dark] = [luminance(first), luminance(second)].sort((a, b) => b - a);
  return (light + 0.05) / (dark + 0.05);
}

// Text tokens paired with every background they are painted on.
const textPairs: [string, string[]][] = [
  ["--primary-contrast", ["--primary", "--primary-hover"]],
  ["--danger-contrast", ["--danger-solid", "--danger-solid-hover"]],
  ["--accent-text", ["--surface", "--bg", "--surface-raised"]],
  ["--expense-text", ["--surface", "--bg", "--surface-raised", "--color-expense-soft"]],
  ["--primary-text", ["--surface", "--bg", "--surface-raised", "--sidebar-hover"]],
  ["--danger-text", ["--surface", "--bg", "--surface-raised"]],
];

const tailwindTheme = resolveConfig(createRequire(import.meta.url)("../../tailwind.config.js")).theme as unknown as Record<
  string,
  Record<string, Record<string, string>>
>;

describe("design tokens", () => {
  it("defines every custom property the stylesheets and components use", () => {
    const runtimeFontVariables = [...layout.matchAll(/variable: "(--[\w-]+)"/g)].map((match) => match[1]);
    const defined = new Set([...declaredIn(tokens), ...declaredIn(globals), ...runtimeFontVariables]);
    const used = new Set([tokens, globals, ...componentSources(srcDir)].flatMap((source) => [...usedIn(source)]));

    expect([...used].filter((name) => !defined.has(name)).sort()).toEqual([]);
  });

  it("paints coral text utilities with the text inks and keeps the vivid fills", () => {
    const { textColor, backgroundColor } = tailwindTheme;
    for (const shade of ["400", "500", "600", "700"]) {
      expect(textColor.rose[shade], `text-rose-${shade}`).toBe("var(--expense-text)");
    }
    expect(textColor.expense.DEFAULT).toBe("var(--expense-text)");
    expect(textColor.primary.DEFAULT).toBe("var(--primary-text)");
    expect(textColor.primary.contrast).toBe("var(--primary-contrast)");
    expect(backgroundColor.rose["500"]).toBe("var(--color-expense)");
    expect(backgroundColor.expense.DEFAULT).toBe("var(--color-expense)");
    expect(backgroundColor.primary.DEFAULT).toBe("var(--primary)");
  });

  it("keeps the signature lime and coral fills unchanged", () => {
    expect(resolve(themes[":root"], "--accent-lime")).toBe("#66CC55");
    expect(resolve(themes[":root"], "--color-expense")).toBe("#FF334B");
    expect(resolve(themes[":root"], "--primary")).toBe("#FF334B");
  });

  describe.each(Object.entries(themes))("in %s", (selector, theme) => {
    it.each(textPairs)("keeps %s at WCAG AA on its backgrounds", (text, backgrounds) => {
      for (const background of backgrounds) {
        expect(contrast(painted(theme, text), painted(theme, background)), `${text} on ${background} in ${selector}`)
          .toBeGreaterThanOrEqual(4.5);
      }
    });
  });
});
