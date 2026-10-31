import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const appDir = path.resolve(__dirname);
const tokens = readFileSync(path.resolve(appDir, "../../tokens.css"), "utf8");
const globals = readFileSync(path.join(appDir, "globals.css"), "utf8");
const layout = readFileSync(path.join(appDir, "layout.tsx"), "utf8");

// Pre-existing gaps behind `.card-green`; this list may only shrink.
const knownUndefined = ["--color-accent", "--color-accent-2", "--color-accent-ink"];

const declaredIn = (css: string) => new Set([...css.matchAll(/(--[\w-]+)\s*:/g)].map((match) => match[1]));
const usedIn = (css: string) => new Set([...css.matchAll(/var\(\s*(--[\w-]+)/g)].map((match) => match[1]));

function block(selector: string): Record<string, string> {
  const start = tokens.indexOf(`${selector} {`);
  expect(start).toBeGreaterThanOrEqual(0);
  const body = tokens.slice(start, tokens.indexOf("\n}", start));
  return Object.fromEntries([...body.matchAll(/(--[\w-]+)\s*:\s*([^;]+);/g)].map((match) => [match[1], match[2].trim()]));
}

function resolve(theme: Record<string, string>, name: string): string {
  const value = theme[name];
  expect(value, `${name} is not defined`).toBeDefined();
  const reference = value.match(/^var\((--[\w-]+)\)$/);
  return reference ? resolve(theme, reference[1]) : value;
}

function luminance(hex: string): number {
  const channels = hex.replace("#", "").match(/../g);
  expect(channels, `${hex} is not a #RRGGBB colour`).toHaveLength(3);
  const [r, g, b] = channels!.map((channel) => {
    const value = parseInt(channel, 16) / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(first: string, second: string): number {
  const [light, dark] = [luminance(first), luminance(second)].sort((a, b) => b - a);
  return (light + 0.05) / (dark + 0.05);
}

describe("design tokens", () => {
  it("defines every custom property the stylesheets use", () => {
    const runtimeFontVariables = [...layout.matchAll(/variable: "(--[\w-]+)"/g)].map((match) => match[1]);
    const defined = new Set([...declaredIn(tokens), ...declaredIn(globals), ...runtimeFontVariables]);
    const undefinedUses = [...new Set([...usedIn(tokens), ...usedIn(globals)])].filter((name) => !defined.has(name));

    expect(undefinedUses.sort()).toEqual([...knownUndefined].sort());
  });

  it.each([":root", ".dark"])("keeps primary button text at WCAG AA on the primary colours in %s", (selector) => {
    const theme = selector === ":root" ? block(":root") : { ...block(":root"), ...block(".dark") };
    const text = resolve(theme, "--primary-contrast");

    for (const background of ["--primary", "--primary-hover"]) {
      expect(contrast(text, resolve(theme, background)), `${background} in ${selector}`).toBeGreaterThanOrEqual(4.5);
    }
  });
});
