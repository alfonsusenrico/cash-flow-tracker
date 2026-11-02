import { readdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import resolveConfig from "tailwindcss/resolveConfig";
import { describe, expect, it } from "vitest";

const appDir = path.resolve(__dirname);
const srcDir = path.resolve(appDir, "..");
const bottomNav = readFileSync(path.join(srcDir, "components/layout/BottomNav.tsx"), "utf8");
const insightsPage = readFileSync(path.join(appDir, "insights/page.tsx"), "utf8");
const ledgerPage = readFileSync(path.join(appDir, "ledger/page.tsx"), "utf8");
const goalsPage = readFileSync(path.join(appDir, "goals/page.tsx"), "utf8");
const heroWalletCard = readFileSync(path.join(srcDir, "components/dashboard/HeroWalletCard.tsx"), "utf8");
const categoryDonutChart = readFileSync(path.join(srcDir, "components/dashboard/CategoryDonutChart.tsx"), "utf8");
const berandaPage = readFileSync(path.join(appDir, "page.tsx"), "utf8");
const mobileLedgerFeed = readFileSync(path.join(srcDir, "components/ledger/MobileLedgerFeed.tsx"), "utf8");
const mobileHomeView = readFileSync(path.join(srcDir, "components/dashboard/MobileHomeView.tsx"), "utf8");
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
  ["--income-text", ["--surface", "--bg", "--surface-raised", "--color-income-soft"]],
  // Text on the emerald buttons; their hover keeps the same fill or a lighter tint of it.
  ["--income-contrast", ["--color-income"]],
  // Text on the strong emerald buttons of Target & Tagihan (bg-emerald-600, hover bg-emerald-700).
  ["--income-hover-contrast", ["--color-income-hover"]],
  ["--warning-text", ["--surface", "--bg", "--surface-raised", "--color-warning-soft"]],
  ["--transfer-text", ["--surface", "--bg", "--surface-raised"]],
  // Text and icons on the solid transfer fill (bg-transfer), such as the HeroWalletCard tile.
  ["--transfer-contrast", ["--color-transfer"]],
  // The phone dock is dark in both themes; its translucent fill is measured over the page surface.
  ["--dock-muted", ["--dock-surface"]],
  ["--dock-text", ["--dock-surface"]],
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

  it("paints emerald text utilities with the income text ink and keeps the emerald fills", () => {
    const { textColor, backgroundColor } = tailwindTheme;
    for (const shade of ["400", "500", "600", "700"]) {
      expect(textColor.emerald[shade], `text-emerald-${shade}`).toBe("var(--income-text)");
    }
    expect(textColor.income.DEFAULT).toBe("var(--income-text)");
    expect(textColor.income.contrast).toBe("var(--income-contrast)");
    expect(backgroundColor.emerald["500"]).toBe("var(--color-income)");
    expect(backgroundColor.income.DEFAULT).toBe("var(--color-income)");
  });

  it("paints amber text utilities with the warning text ink and keeps the amber fills", () => {
    const { textColor, backgroundColor } = tailwindTheme;
    for (const shade of ["400", "500", "600", "700"]) {
      expect(textColor.amber[shade], `text-amber-${shade}`).toBe("var(--warning-text)");
    }
    expect(textColor.income["hover-contrast"]).toBe("var(--income-hover-contrast)");
    expect(backgroundColor.amber["500"]).toBe("#f59e0b");
    expect(backgroundColor.emerald["600"]).toBe("var(--color-income-hover)");
  });

  it("paints sky text utilities with the transfer text ink and keeps the sky fills", () => {
    const { textColor, backgroundColor } = tailwindTheme;
    for (const shade of ["400", "500", "600", "700"]) {
      expect(textColor.sky[shade], `text-sky-${shade}`).toBe("var(--transfer-text)");
    }
    expect(textColor.transfer.DEFAULT).toBe("var(--transfer-text)");
    expect(textColor.transfer.contrast).toBe("var(--transfer-contrast)");
    expect(backgroundColor.sky["500"]).toBe("#0ea5e9");
    expect(backgroundColor.transfer.DEFAULT).toBe("var(--color-transfer)");
    expect(resolve(themes[":root"], "--color-transfer")).toBe("#0EA5E9");
    expect(resolve(themes[".dark"], "--color-transfer")).toBe("#38BDF8");
  });

  it("paints every Pindah Saldo and movement element sky, never amber or blue", () => {
    const classLists = (source: string) =>
      [...source.matchAll(/(?:isMovement|is_excluded_from_budget)\s*\?\s*"([^"]*)"/g)]
        .map((match) => match[1])
        .filter((classes) => /(^|\s)(text|bg|border)-/.test(classes));
    for (const [name, source] of [
      ["Beranda", berandaPage],
      ["Transaksi", ledgerPage],
      ["MobileLedgerFeed", mobileLedgerFeed],
      ["MobileHomeView", mobileHomeView],
    ] as const) {
      const movementClasses = classLists(source);
      expect(movementClasses.length, `${name} movement branches`).toBeGreaterThan(0);
      for (const classes of movementClasses) {
        expect(classes, `${name}: ${classes}`).toMatch(/(^|\s)text-sky-500(\s|$)/);
        expect(classes, `${name}: ${classes}`).not.toMatch(/(amber|blue)-/);
      }
    }

    const berandaButton = berandaPage.match(/onClick=\{\(\) => handleOpenTransfer\(\)\}[^>]*className="([^"]*)"[^>]*>\s*Pindah Saldo/);
    expect(berandaButton?.[1]).toMatch(/(^|\s)text-sky-500(\s|$)/);
    expect(berandaButton?.[1]).not.toMatch(/amber-/);

    const capsule = heroWalletCard.match(/className="([^"]*)"\s*>\s*<div aria-hidden="true" className="([^"]*)">\s*<Icon name="repeat"/);
    expect(capsule?.[1]).toMatch(/(^|\s)text-sky-500(\s|$)/);
    expect(capsule?.[1]).not.toMatch(/amber-/);
    expect(capsule?.[2]).toContain("bg-transfer text-transfer-contrast");
    expect(ledgerPage).toMatch(/isMovement\s*\?\s*"var\(--color-transfer\)"/);
    expect(mobileHomeView).toMatch(/<Icon name="repeat" className="[^"]*text-sky-500/);
  });

  it("keeps the signature lime, coral and emerald fills unchanged", () => {
    expect(resolve(themes[":root"], "--accent-lime")).toBe("#66CC55");
    expect(resolve(themes[":root"], "--color-expense")).toBe("#FF334B");
    expect(resolve(themes[":root"], "--primary")).toBe("#FF334B");
    expect(resolve(themes[":root"], "--color-income")).toBe("#059669");
    expect(resolve(themes[".dark"], "--color-income")).toBe("#10B981");
    expect(resolve(themes[":root"], "--color-income-hover")).toBe("#047857");
    expect(resolve(themes[".dark"], "--color-income-hover")).toBe("#34D399");
    expect(resolve(themes[":root"], "--color-warning")).toBe("#F59E0B");
    expect(resolve(themes[".dark"], "--color-warning")).toBe("#FBBF24");
  });

  it("puts the strong-emerald ink on every bg-emerald-600 button of Target & Tagihan", () => {
    const classStrings = [...goalsPage.matchAll(/"([^"]*)"/g)].map((match) => match[1]);
    const buttons = classStrings.filter((classes) => /(^|\s)bg-emerald-600(\s|$)/.test(classes) && /(^|\s)text-/.test(classes));
    expect(buttons.length).toBeGreaterThanOrEqual(3);
    for (const classes of buttons) {
      expect(classes).toContain("text-income-hover-contrast");
      expect(classes).not.toMatch(/(^|\s)(dark:)?text-(white|\[#)/);
    }
  });

  it("marks the HeroWalletCard icon tiles decorative and keeps the emerald tile above 3:1", () => {
    const tiles = [...heroWalletCard.matchAll(/<div([^>]*)className="h-5 w-5 rounded-lg ([^"]*)"/g)];
    expect(tiles).toHaveLength(3);
    for (const [, attributes] of tiles) expect(attributes).toContain('aria-hidden="true"');
    const emerald = tiles.find(([, , classes]) => classes.includes("bg-emerald-500"));
    expect(emerald?.[2]).toContain("text-income-contrast");
  });

  it("labels every donut sector, which recharts paints as an SVG role=img", () => {
    expect(categoryDonutChart).toMatch(/<Cell[^>]*aria-label=\{sectorLabel\(entry\)\}/);
    expect(categoryDonutChart).toContain('role="figure"');
  });

  it("puts no white text on the emerald buttons of Analisis and Transaksi", () => {
    const classStrings = (source: string) => [...source.matchAll(/"([^"]*)"/g)].map((match) => match[1]);
    const onEmeraldFill = (classes: string) => /(^|\s)bg-(emerald-500|income)(\s|$)/.test(classes);
    for (const [page, source] of [["insights", insightsPage], ["ledger", ledgerPage]] as const) {
      const buttons = classStrings(source).filter(onEmeraldFill);
      expect(buttons.length, `${page} emerald fills`).toBeGreaterThan(0);
      for (const classes of buttons) {
        if (!/(^|\s)text-/.test(classes)) continue;
        expect(classes, `${page}: ${classes}`).toContain("text-income-contrast");
        expect(classes, `${page}: ${classes}`).not.toMatch(/(^|\s)text-white/);
      }
    }
  });

  it("paints the phone dock and its inactive labels with the dock tokens", () => {
    const dockRule = globals.slice(globals.indexOf(".glass-dock {"), globals.indexOf("}", globals.indexOf(".glass-dock {")));
    expect(dockRule).toContain("background: var(--dock-surface);");
    expect(bottomNav).toContain("glass-dock");
    expect(bottomNav).not.toMatch(/bg-\[var\(--surface\)\]/);
    expect(bottomNav).not.toMatch(/text-\[var\(--(muted|text)\)\]/);
    expect(bottomNav.match(/text-\[var\(--dock-muted\)\] hover:text-\[var\(--dock-text\)\]/g)).toHaveLength(4);
  });

  it("generates shadow-xs from the --shadow-xs token", () => {
    const boxShadow = tailwindTheme.boxShadow as unknown as Record<string, string>;
    expect(boxShadow.xs).toBe("var(--shadow-xs)");
    expect(boxShadow.sm).toBe("var(--shadow-sm)");
  });

  describe.each(Object.entries(themes))("in %s", (selector, theme) => {
    it("keeps --shadow-xs a 1px raise lighter than --shadow-sm", () => {
      const shadow = (name: string) => {
        const match = resolve(theme, name).match(/^0 (\d+)px (\d+)px (?:-?\d+(?:px)? )?rgba\(\d+, \d+, \d+, ([\d.]+)\)$/);
        expect(match, `${name} in ${selector}`).not.toBeNull();
        return { offset: Number(match![1]), blur: Number(match![2]), alpha: Number(match![3]) };
      };
      const xs = shadow("--shadow-xs");
      const sm = shadow("--shadow-sm");
      expect(xs.offset).toBe(1);
      expect(xs.blur).toBeLessThanOrEqual(2);
      expect(xs.blur).toBeLessThan(sm.blur);
      expect(xs.alpha).toBeLessThan(sm.alpha);
    });

    it("keeps --transfer-text at WCAG AA on the sky tints of transfer chips", () => {
      // Transfer chips and capsules paint Tailwind's sky-500 at 10 to 20% over the card or the raised surface.
      const sky = hex("#0ea5e9");
      for (const surface of ["--surface", "--surface-raised"]) {
        for (const alpha of [0.1, 0.15, 0.2]) {
          const base = painted(theme, surface);
          const tint = base.map((channel, index) => sky[index] * alpha + channel * (1 - alpha)) as Rgb;
          expect(contrast(painted(theme, "--transfer-text"), tint), `${alpha} sky over ${surface} in ${selector}`)
            .toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    it.each(textPairs)("keeps %s at WCAG AA on its backgrounds", (text, backgrounds) => {
      for (const background of backgrounds) {
        expect(contrast(painted(theme, text), painted(theme, background)), `${text} on ${background} in ${selector}`)
          .toBeGreaterThanOrEqual(4.5);
      }
    });
  });
});
