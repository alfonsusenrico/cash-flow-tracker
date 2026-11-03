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

function componentFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return componentFiles(full);
    return /\.tsx?$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [full] : [];
  });
}

function componentSources(dir: string): string[] {
  return componentFiles(dir).map((file) => readFileSync(file, "utf8"));
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
const SURFACES = ["--surface", "--window", "--surface-2"];
const textPairs: [string, string[]][] = [
  ["--text", [...SURFACES, "--surface-3", "--canvas"]],
  ["--text-2", [...SURFACES, "--surface-3"]],
  // Also the inactive labels of the phone tab bar, which sits on --surface.
  ["--muted", [...SURFACES, "--surface-3"]],
  ["--ink-fg", ["--ink", "--ink-hover"]],
  ["--tile-fg", ["--tile"]],
  ["--lime-ink", ["--lime", "--lime-hover"]],
  ["--lime-text", [...SURFACES, "--lime-soft"]],
  ["--in-fg", [...SURFACES, "--in-bg"]],
  ["--out-fg", [...SURFACES, "--out-bg"]],
  ["--tr-fg", [...SURFACES, "--tr-bg"]],
  ["--inv-fg", [...SURFACES, "--inv-bg"]],
  ["--warn-fg", [...SURFACES, "--warn-bg"]],
  ["--ok-fg", [...SURFACES, "--ok-bg"]],
  ["--danger-contrast", ["--danger-solid", "--danger-solid-hover"]],
  // Text on the semantic solids (the `*-solid-fg` Tailwind keys).
  ["--in-solid-fg", ["--in-solid"]],
  ["--in-solid-hover-fg", ["--in-solid-hover"]],
  ["--tr-solid-fg", ["--tr-solid"]],
  ["--inv-solid-fg", ["--inv-solid"]],
];

/** The compatibility aliases removed in phase 5 (redesign-money-and-settings D11); none may come back. */
const REMOVED_ALIASES = [
  "--bg", "--color-paper", "--color-paper-2", "--color-surface", "--color-surface-raised", "--surface-raised", "--color-ink", "--color-ink-2",
  "--color-muted", "--color-rule", "--color-rule-strong", "--border", "--border-strong", "--text-secondary", "--primary", "--primary-hover",
  "--primary-soft", "--primary-contrast", "--primary-text", "--color-focus", "--accent-lime", "--accent-text", "--color-income",
  "--color-income-hover", "--color-income-soft", "--income-text", "--income-contrast", "--income-hover-contrast", "--color-expense",
  "--color-expense-hover", "--color-expense-soft", "--expense-text", "--danger", "--danger-hover", "--danger-text", "--color-warning",
  "--color-warning-soft", "--warning-text", "--color-transfer", "--transfer-text", "--transfer-contrast", "--color-investment",
  "--color-investment-soft", "--investment-text", "--investment-contrast", "--color-kakeibo-need", "--color-kakeibo-need-soft",
  "--color-kakeibo-want", "--color-kakeibo-want-soft", "--color-kakeibo-culture", "--color-kakeibo-culture-soft",
  "--color-kakeibo-unexpected", "--color-kakeibo-unexpected-soft", "--color-kakeibo-saving", "--color-kakeibo-saving-soft",
  "--color-kakeibo-unclassified", "--color-kakeibo-unclassified-soft", "--sidebar", "--sidebar-hover", "--sidebar-active",
  "--sidebar-text", "--sidebar-text-active",
];
const PALETTE = "rose|red|emerald|green|amber|yellow|orange|sky|blue|violet|purple|indigo|pink|fuchsia|teal|cyan|lime|slate|gray|zinc|neutral|stone";
const PALETTE_CLASS = new RegExp(`(?<![\\w-])(?:[a-z-]+:)*(?:bg|text|border|ring|fill|stroke|from|to|via|divide|outline|decoration|placeholder|accent|caret|shadow)-(?:${PALETTE})-\\d{2,3}(?:/\\d+)?(?![\\w-])`, "g");
const LEGACY_KEYS = ["bg", "surface-raised", "border", "border-strong", "text-secondary", "income", "expense", "warning", "transfer", "investment",
  "primary", "sidebar", "sidebar-hover", "sidebar-active", "sidebar-text", "sidebar-text-active", "emerald", "rose", "amber", "sky", "violet"];
const LEGACY_UTILITIES = ["btn-primary", "btn-secondary", "cockpit-card", "card-squircle", "pressable", "donut-ring"];
const sourceFiles = componentFiles(srcDir).filter((file) => !file.endsWith("tokens.test.ts"));

/** Every match of a pattern in the sources, as "file:line match", so a failure names its place. */
function findings(pattern: RegExp, sources: string[] = sourceFiles): string[] {
  return sources.flatMap((file) => readFileSync(file, "utf8").split("\n").flatMap((line, index) =>
    [...line.matchAll(pattern)].map((match) => `${path.relative(srcDir, file)}:${index + 1} ${match[0]}`)));
}

// Icons and marks need 3:1 (WCAG 1.4.11). The focus ring sits 2px outside its control, on the window or a surface.
const markPairs: [string, string[]][] = [
  ["--focus", SURFACES],
  ["--ok-dot", ["--surface", "--window"]],
  ["--lime-text", ["--lime-soft"]],
];

const PILLARS = ["need", "want", "culture", "unexpected", "saving"] as const;
/** The pillar token of a Kakeibo bucket; "Belum diklasifikasi" is `--k-none`. */
const pillarToken = (pillar: string) => `--k-${pillar === "unclassified" ? "none" : pillar}`;

/** HSL hue in degrees and saturation (0 for a grey, 1 for a pure hue) of an opaque colour. */
function hueOf(colour: Rgb): { hue: number; saturation: number } {
  const [r, g, b] = colour.map((channel) => channel / 255);
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  const chroma = max - min;
  if (chroma === 0) return { hue: 0, saturation: 0 };
  const lightness = (max + min) / 2;
  const sector = max === r ? ((g - b) / chroma + 6) % 6 : max === g ? (b - r) / chroma + 2 : (r - g) / chroma + 4;
  return { hue: sector * 60, saturation: chroma / (1 - Math.abs(2 * lightness - 1)) };
}

const hueDistance = (first: number, second: number) => {
  const difference = Math.abs(first - second) % 360;
  return Math.min(difference, 360 - difference);
};

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

  it("declares and uses no compatibility alias", () => {
    const declared = declaredIn(tokens + globals);
    expect(REMOVED_ALIASES.filter((name) => declared.has(name))).toEqual([]);
    const aliasUse = new RegExp(`var\\(\\s*(?:${REMOVED_ALIASES.map((name) => name.replace(/-/g, "\\-")).join("|")})\\s*[,)]`, "g");
    expect(findings(aliasUse)).toEqual([]);
    expect(tokens).not.toContain("Compatibility aliases");
  });

  it("maps no legacy Tailwind key or palette remap and paints with no default palette class", () => {
    const config = createRequire(import.meta.url)("../../tailwind.config.js");
    const extend = config.theme.extend as Record<string, Record<string, unknown> | undefined>;
    for (const group of ["colors", "textColor", "backgroundColor", "borderColor"]) {
      const keys = Object.keys(extend[group] ?? {});
      expect(keys.filter((key) => LEGACY_KEYS.includes(key)), group).toEqual([]);
    }
    expect(findings(PALETTE_CLASS)).toEqual([]);
  });

  it("keeps no legacy utility class in the stylesheet or the components", () => {
    for (const name of LEGACY_UTILITIES) expect(globals, name).not.toMatch(new RegExp(`\\.${name}\\b`));
    expect(findings(new RegExp(`\\b(?:${LEGACY_UTILITIES.join("|")})\\b`, "g"))).toEqual([]);
  });

  it("builds every semantic Tailwind key from a token declared in both themes", () => {
    const colors = createRequire(import.meta.url)("../../tailwind.config.js").theme.extend.colors as Record<string, string | Record<string, string>>;
    const values = Object.values(colors).flatMap((value) => (typeof value === "string" ? [value] : Object.values(value)));
    const referenced = values.map((value) => value.match(/var\((--[\w-]+)\)/)?.[1]);
    expect(referenced.filter((name) => !name)).toEqual([]);
    for (const name of referenced as string[]) {
      expect(themes[":root"][name], `${name} in :root`).toBeDefined();
      expect(themes[".dark"][name], `${name} in .dark`).toBeDefined();
    }
    for (const family of ["in", "out", "tr", "inv", "warn", "ok"]) {
      for (const shade of ["fg", "bg"]) expect((colors[family] as Record<string, string>)[shade], `${family}-${shade}`).toContain(`var(--${family}-${shade})`);
    }
  });

  it("paints Kakeibo pillars only through their tokens in kakeibo.ts", () => {
    const kakeibo = readFileSync(path.join(srcDir, "lib/kakeibo.ts"), "utf8");
    for (const pillar of PILLARS) expect(kakeibo).toContain(`var(${pillarToken(pillar)})`);
    expect(kakeibo).not.toMatch(/#[0-9a-fA-F]{6}/);
  });

  it("starts new investment accounts with a lavender tag colour", async () => {
    const { defaultAccountTagColor, INVESTMENT_ACCOUNT_TAG_COLOR } = await import("@/lib/accountColours");
    expect(defaultAccountTagColor("investment")).toBe(INVESTMENT_ACCOUNT_TAG_COLOR);
    for (const type of ["bank", "wallet", "cash"]) expect(defaultAccountTagColor(type)).not.toBe(INVESTMENT_ACCOUNT_TAG_COLOR);
  });

  it("gives every card the card shadow token and floating layers the float token", () => {
    const rule = (selector: string) => {
      const start = globals.indexOf(`${selector} {`);
      expect(start, selector).toBeGreaterThanOrEqual(0);
      return globals.slice(start, globals.indexOf("}", start));
    };
    expect(rule(".ws-card")).toContain("box-shadow: var(--shadow-card)");
    for (const floating of [".ws-menu", ".ws-explain", ".ws-toast", "dialog.ws-dialog"]) {
      expect(rule(floating), floating).toContain("box-shadow: var(--shadow-float)");
    }
  });

  it("stacks the overlay drawer and the phone sheet above the scrim that dims the page", () => {
    const zOf = (selector: string) => Number(globals.slice(globals.indexOf(`${selector} {`)).match(/z-index:\s*(\d+)/)![1]);
    const scrim = zOf(".ws-sheet-scrim");
    expect(zOf('.ws-drawer[data-mode="overlay"]')).toBeGreaterThan(scrim);
    expect(zOf('.ws-drawer[data-mode="sheet"]')).toBeGreaterThan(scrim);
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

    // Chips, notes and pills carry text on `bg-<family>-solid/5` to `/15` (income only to `/10`); transfer and warning also on `/20`.
    it.each([["in", 0.1], ["out", 0.15], ["inv", 0.15], ["tr", 0.2], ["warn", 0.2]] as const)("keeps --%s-fg at WCAG AA on the tints of its solid up to %s", (family, strongest) => {
      const solid = painted(theme, `--${family}-solid`);
      for (const surface of ["--surface", "--surface-2"]) {
        for (const alpha of [0.05, 0.1, 0.15, 0.2].filter((value) => value <= strongest)) {
          const base = painted(theme, surface);
          const tint = base.map((channel, index) => solid[index] * alpha + channel * (1 - alpha)) as Rgb;
          expect(contrast(painted(theme, `--${family}-fg`), tint), `${alpha} ${family} over ${surface} in ${selector}`)
            .toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    it.each([...PILLARS, "unclassified"])("keeps the %s pillar colour at WCAG AA as text and as a mark", (pillar) => {
      // The pillar colour is a chart mark (dots, bar segments) and may be text. Text needs 4.5:1 on every surface
      // and the pillar's soft fill; marks need 3:1 on the bar tracks (--surface-2, --surface-3 and --line).
      const colour = painted(theme, pillarToken(pillar));
      for (const background of ["--surface", "--window", "--surface-2", `${pillarToken(pillar)}-soft`]) {
        expect(contrast(colour, painted(theme, background)), `${pillar} text on ${background} in ${selector}`).toBeGreaterThanOrEqual(4.5);
      }
      for (const track of ["--surface-3", "--line", "--surface-2"]) {
        expect(contrast(colour, painted(theme, track)), `${pillar} mark on ${track} in ${selector}`).toBeGreaterThanOrEqual(3);
      }
    });

    it("gives every pillar its own hue, apart from the transfer, investment, income and expense colours", () => {
      const pillarHue = (pillar: string) => hueOf(painted(theme, pillarToken(pillar)));
      const reserved = ["--tr-solid", "--tr-fg", "--inv-solid", "--inv-fg", "--in-solid", "--in-fg", "--out-solid", "--out-fg"];
      for (const pillar of PILLARS) {
        const { hue, saturation } = pillarHue(pillar);
        expect(saturation, `${pillar} is a colour, not a grey, in ${selector}`).toBeGreaterThan(0.35);
        for (const token of reserved) {
          const distance = hueDistance(hue, hueOf(painted(theme, token)).hue);
          expect(distance, `${pillar} vs ${token} in ${selector}`).toBeGreaterThanOrEqual(25);
        }
      }
      for (const [index, first] of PILLARS.entries()) {
        for (const second of PILLARS.slice(index + 1)) {
          expect(hueDistance(pillarHue(first).hue, pillarHue(second).hue), `${first} vs ${second} in ${selector}`).toBeGreaterThanOrEqual(40);
        }
      }
      // "Belum diklasifikasi" is missing information, so it stays a neutral grey rather than a sixth hue.
      expect(pillarHue("unclassified").saturation, `unclassified in ${selector}`).toBeLessThan(0.25);
    });

    it.each(markPairs)("keeps the mark %s at 3:1 on its backgrounds", (mark, backgrounds) => {
      for (const background of backgrounds) {
        expect(contrast(painted(theme, mark), painted(theme, background)), `${mark} on ${background} in ${selector}`).toBeGreaterThanOrEqual(3);
      }
    });

    it("uses blue only for transfers: no info blue and no other blue token", () => {
      const blue = Object.entries(theme)
        .filter(([, value]) => /^#[0-9A-Fa-f]{6}$/.test(value))
        .filter(([, value]) => {
          const { hue, saturation } = hueOf(hex(value));
          return saturation > 0.3 && hue >= 190 && hue <= 245;
        })
        .map(([name]) => name);
      expect(blue.filter((name) => !name.startsWith("--tr-"))).toEqual([]);
      expect(Object.keys(theme).filter((name) => name.startsWith("--info"))).toEqual([]);
    });

    it.each(textPairs)("keeps %s at WCAG AA on its backgrounds", (text, backgrounds) => {
      for (const background of backgrounds) {
        expect(contrast(painted(theme, text), painted(theme, background)), `${text} on ${background} in ${selector}`)
          .toBeGreaterThanOrEqual(4.5);
      }
    });
  });
});
