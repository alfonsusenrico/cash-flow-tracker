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

function cssModules(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return cssModules(full);
    return entry.name.endsWith(".module.css") ? [full] : [];
  });
}

function componentSources(dir: string): string[] {
  return componentFiles(dir).map((file) => readFileSync(file, "utf8"));
}

function block(selector: string): Record<string, string> {
  const start = selector === ":root" ? tokens.indexOf(":root, .theme-light {") : tokens.indexOf(`${selector} {`);
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

// Text tokens paired with every background they are painted on: the canvas (s0), cards and panels (s1),
// the raised badge and selection fill (s2) and the dialog surface.
const SURFACES = ["--window", "--surface", "--surface-2", "--dialog"];
const textPairs: [string, string[]][] = [
  ["--text", [...SURFACES, "--canvas", "--nav-bg", "--nav-hover"]],
  ["--text-2", [...SURFACES, "--nav-bg", "--nav-hover"]],
  ["--muted", [...SURFACES, "--nav-bg", "--nav-hover"]],
  ["--tile-fg", ["--tile"]],
  ["--ink-fg", ["--ink"]],
  ["--accent-ink", ["--accent", "--accent-hover", "--accent-pressed"]],
  ["--accent-fg", SURFACES],
  ["--in-fg", [...SURFACES, "--in-bg"]],
  // Expense amounts are red wherever they carry "−", on the canvas too (apply-markas-transaksi D29).
  ["--out-fg", ["--canvas", ...SURFACES, "--out-bg"]],
  ["--tr-fg", [...SURFACES, "--tr-bg"]],
  ["--inv-fg", [...SURFACES, "--inv-bg"]],
  ["--warn-fg", [...SURFACES, "--warn-bg"]],
  ["--ok-fg", [...SURFACES, "--ok-bg"]],
  ["--danger-contrast", ["--danger-solid", "--danger-solid-hover"]],
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

// Marks need 3:1 (WCAG 1.4.11): money marks on the page and panel surfaces, the focus outline on every
// surface, and a focused field's edge against its resting edge (SPEC §3.3).
const markPairs: [string, string[]][] = [
  ["--focus", SURFACES],
  ["--field-focus", SURFACES],
  ["--in-solid", ["--window", "--surface"]],
  ["--tr-solid", ["--window", "--surface"]],
  ["--inv-solid", ["--window", "--surface"]],
  ["--warn-solid", ["--window", "--surface"]],
  ["--out-solid", ["--window", "--surface"]],
  ["--ok-dot", ["--window", "--surface"]],
  ["--line-strong", ["--window", "--surface", "--surface-2"]],
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

const tailwindTheme = resolveConfig(createRequire(import.meta.url)("../../tailwind.config.js")).theme as unknown as Record<
  string,
  Record<string, unknown>
>;

// Option 6 "Markas+" values on the app's token names (apply-markas-foundation D3), light then dark.
const MARKAS: Record<string, [string, string]> = {
  "--canvas": ["#F8F7F4", "#151517"],
  "--window": ["#F8F7F4", "#151517"],
  "--surface": ["#F1EFEB", "#1C1D20"],
  "--surface-2": ["#E6E3DD", "#26272B"],
  "--surface-3": ["#E6E3DD", "#26272B"],
  "--nav-bg": ["#F1EFEB", "#1C1D20"],
  "--nav-hover": ["#E6E3DD", "#26272B"],
  "--accent-soft": ["#E6E3DD", "#26272B"],
  "--dialog": ["#F8F7F4", "#1C1D20"],
  "--line": ["#E1DDD6", "#2F3034"],
  "--line-strong": ["#847F78", "#6E7076"],
  "--text": ["#1C1E22", "#E8E7E4"],
  "--ink": ["#1C1E22", "#E8E7E4"],
  "--muted": ["#5B5E65", "#A3A3A7"],
  "--text-2": ["#5B5E65", "#A3A3A7"],
  "--ink-hover": ["#45484F", "#BDBEC2"],
  "--ink-fg": ["#F8F7F4", "#151517"],
  "--accent": ["#FF334B", "#FF4757"],
  "--accent-hover": ["#FF4D61", "#FF6675"],
  "--accent-pressed": ["#FF6B7C", "#FF8A95"],
  "--accent-ink": ["#0A0D14", "#0A0D14"],
  "--accent-fg": ["#B8142D", "#FF6B78"],
  "--in-fg": ["#056548", "#34D399"],
  "--ok-fg": ["#056548", "#34D399"],
  "--out-fg": ["#B8142D", "#FF6B78"],
  "--tr-fg": ["#075985", "#38BDF8"],
  "--inv-fg": ["#5B21B6", "#B39DFB"],
  "--warn-fg": ["#8F4F04", "#FBBF24"],
  "--in-bg": ["#E6E3DD", "#26272B"],
  "--out-bg": ["#E6E3DD", "#26272B"],
  "--tr-bg": ["#E6E3DD", "#26272B"],
  "--inv-bg": ["#E6E3DD", "#26272B"],
  "--warn-bg": ["#E6E3DD", "#26272B"],
  "--ok-bg": ["#E6E3DD", "#26272B"],
  "--in-solid": ["#047857", "#10B981"],
  "--ok-dot": ["#047857", "#10B981"],
  "--k-saving": ["#047857", "#10B981"],
  "--flow-in": ["#047857", "#10B981"],
  "--use-tabungan": ["#047857", "#10B981"],
  "--tr-solid": ["#0284C7", "#0EA5E9"],
  "--use-dana": ["#0284C7", "#0EA5E9"],
  "--inv-solid": ["#7C3AED", "#8B5CF6"],
  "--k-need": ["#7C3AED", "#8B5CF6"],
  "--flow-inv": ["#7C3AED", "#8B5CF6"],
  "--use-investasi": ["#7C3AED", "#8B5CF6"],
  "--warn-solid": ["#B45309", "#F59E0B"],
  "--flow-warn": ["#B45309", "#F59E0B"],
  "--k-unexpected": ["#B45309", "#FBBF24"],
  "--out-solid": ["#E11D48", "#FF4757"],
  "--k-want": ["#E11D48", "#FF4757"],
  "--k-culture": ["#0369A1", "#38BDF8"],
  "--k-none": ["#64748B", "#94A3B8"],
  "--flow-left": ["#64748B", "#94A3B8"],
  "--flow-other": ["#64748B", "#94A3B8"],
  "--use-harian": ["#64748B", "#94A3B8"],
  "--danger-solid": ["#B8142D", "#E11D48"],
  "--danger-solid-hover": ["#A9122A", "#CF1B42"],
  "--danger-contrast": ["#FFFFFF", "#FFFFFF"],
  "--focus": ["#45484F", "#BDBEC2"],
  "--field-focus": ["#2B2D32", "#CACACD"],
  "--ring": ["rgba(28, 30, 34, 0.10)", "rgba(232, 231, 228, 0.12)"],
  "--scrim": ["rgba(28, 26, 22, 0.44)", "rgba(8, 8, 9, 0.62)"],
  "--shadow-float": [
    "0 16px 48px rgba(40, 34, 26, 0.14), 0 2px 6px rgba(40, 34, 26, 0.06)",
    "0 16px 48px rgba(0, 0, 0, 0.5), 0 2px 6px rgba(0, 0, 0, 0.3)",
  ],
};

/** Tokens retired with the pre-redesign styling; none may come back (apply-markas-foundation D3, D6, D7). */
const RETIRED = [
  "--font-body", "--font-mono", "--radius-window", "--radius-card", "--radius-btn", "--radius-xs", "--radius-sm", "--radius-md",
  "--radius-lg", "--radius-xl", "--radius-2xl", "--radius-3xl", "--radius-pill", "--shadow-window", "--shadow-card", "--shadow-xs",
  "--shadow-sm", "--shadow-md", "--in-solid-fg", "--in-solid-hover", "--in-solid-hover-fg", "--out-solid-hover", "--tr-solid-fg",
  "--inv-solid-fg",
];

describe("design tokens", () => {
  it.each([
    [":root", 0],
    [".dark", 1],
  ] as const)("paints %s with the Markas+ palette", (selector, index) => {
    const theme = themes[selector];
    for (const [name, values] of Object.entries(MARKAS)) expect(theme[name], name).toBe(values[index]);
  });

  it("keeps the chart tokens without a prototype counterpart until their page changes", () => {
    for (const name of ["--heat-1", "--heat-5", "--band-op", "--kind-emas", "--kind-reksa-dana", "--kind-saham"]) {
      expect(themes[":root"][name], name).toBeDefined();
      expect(themes[".dark"][name], name).toBeDefined();
    }
  });

  it("defines the type, spacing, radius and motion scales of the prototype", () => {
    const root = themes[":root"];
    expect([12, 14, 16, 20, 24, 32, 44].map((size) => root[`--fs-${size}`])).toEqual(["12px", "14px", "16px", "20px", "24px", "32px", "44px"]);
    expect([4, 8, 12, 16, 24, 32, 48, 64].map((step) => root[`--sp-${step}`])).toEqual(["4px", "8px", "12px", "16px", "24px", "32px", "48px", "64px"]);
    expect([root["--r-control"], root["--r-panel"], root["--r-layer"]]).toEqual(["6px", "12px", "16px"]);
    expect(root["--t-state"]).toBe("140ms cubic-bezier(0.2, 0, 0, 1)");
    expect(root["--t-layer"]).toBe("200ms cubic-bezier(0.2, 0, 0, 1)");
    expect(root["--features-text"]).toBe('"lnum" 1, "cv11" 1');
    expect(root["--features-num"]).toBe('"tnum" 1, "lnum" 1, "cv11" 1');
    expect(root["--font-sans"]).toMatch(/^var\(--font-inter\), "Inter"/);
    expect(root["--font-display"]).toMatch(/^var\(--font-manrope\), "Manrope"/);
  });

  it("defines every custom property the stylesheets and components use", () => {
    const runtimeFontVariables = [...layout.matchAll(/variable: "(--[\w-]+)"/g)].map((match) => match[1]);
    // Components set some properties inline (style={{ "--bc": colour }}); those count as defined too.
    const inline = componentSources(srcDir).flatMap((source) => [...source.matchAll(/["'](--[\w-]+)["'](?:\s+as\s+string)?\]?\s*:/g)].map((match) => match[1]));
    const defined = new Set([...declaredIn(tokens), ...declaredIn(globals), ...runtimeFontVariables, ...inline]);
    const modules = cssModules(srcDir).map((file) => readFileSync(file, "utf8"));
    const used = new Set([tokens, globals, ...modules, ...componentSources(srcDir)].flatMap((source) => [...usedIn(source)]));

    expect([...used].filter((name) => !defined.has(name)).sort()).toEqual([]);
  });

  it("declares and uses no retired token", () => {
    const declared = declaredIn(tokens + globals);
    expect(RETIRED.filter((name) => declared.has(name))).toEqual([]);
    const retiredUse = new RegExp(`var\\(\\s*(?:${RETIRED.map((name) => name.replace(/-/g, "\\-")).join("|")})\\s*[,)]`, "g");
    expect(findings(retiredUse, [...sourceFiles, path.join(appDir, "globals.css"), ...cssModules(srcDir)])).toEqual([]);
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

  it("builds every semantic Tailwind key from a token declared in both themes, without an opacity channel", () => {
    const colors = createRequire(import.meta.url)("../../tailwind.config.js").theme.extend.colors as Record<string, string | Record<string, string>>;
    const values = Object.values(colors).flatMap((value) => (typeof value === "string" ? [value] : Object.values(value)));
    expect(values.filter((value) => !/^var\(--[\w-]+\)$/.test(value))).toEqual([]);
    for (const value of values) {
      const name = value.slice(4, -1);
      expect(themes[":root"][name], `${name} in :root`).toBeDefined();
      expect(themes[".dark"][name], `${name} in .dark`).toBeDefined();
    }
  });

  it("paints no tinted semantic fill behind text", () => {
    // Badges carry the ink on the raised neutral fill (D7); tints of a mark colour fall below 4.5:1 on s2.
    expect(findings(/(?<![\w-])(?:[a-z-]+:)*(?:bg|border)-(?:in|out|tr|inv|warn|ok)-(?:solid|bg|fg)\/\d+/g)).toEqual([]);
    expect(findings(/color-mix\(in srgb, var\(--(?:in|out|tr|inv|warn)-/g)).toEqual([]);
  });

  it("replaces Tailwind's type, weight, radius and shadow scales with the prototype's", () => {
    expect(Object.keys(tailwindTheme.fontSize)).toEqual(["xs", "sm", "base", "xl", "2xl", "3xl", "4xl"]);
    expect(Object.values(tailwindTheme.fontSize as Record<string, [string, { lineHeight: string }]>).map(([size, { lineHeight }]) => `${size}/${lineHeight}`))
      .toEqual(["var(--fs-12)/16px", "var(--fs-14)/20px", "var(--fs-16)/24px", "var(--fs-20)/28px", "var(--fs-24)/32px", "var(--fs-32)/40px", "var(--fs-44)/52px"]);
    expect(tailwindTheme.fontWeight).toEqual({ normal: "400", medium: "500", semibold: "600" });
    expect(new Set(Object.values(tailwindTheme.borderRadius))).toEqual(new Set(["0", "var(--r-control)", "var(--r-panel)", "var(--r-layer)", "9999px"]));
    expect(tailwindTheme.boxShadow).toEqual({ none: "none", float: "var(--shadow-float)" });
    expect(Object.keys(tailwindTheme.fontFamily)).toEqual(["sans", "display"]);
  });

  it("paints Kakeibo pillars only through their tokens in kakeibo.ts", () => {
    const kakeibo = readFileSync(path.join(srcDir, "lib/kakeibo.ts"), "utf8");
    for (const pillar of PILLARS) expect(kakeibo).toContain(`var(${pillarToken(pillar)})`);
    expect(kakeibo).not.toMatch(/#[0-9a-fA-F]{6}/);
  });

  it("uses pillar colours only as marks, never as text", () => {
    // D4: dots, bars and rails carry the pillar colour; the pillar's name and amount keep the text tokens.
    const textUse = /(?:text-\[var\(--k-|(?<![\w-])color:\s*var\(--k-|style=\{\{[^}]*(?<![\w-])color:\s*[\w?.]*(?:info|bucket|pillar)\??\.color)/g;
    expect(findings(textUse, [...sourceFiles, path.join(appDir, "globals.css"), ...cssModules(srcDir)])).toEqual([]);
  });

  it("starts new investment accounts with a lavender tag colour", async () => {
    const { defaultAccountTagColor, INVESTMENT_ACCOUNT_TAG_COLOR } = await import("@/lib/accountColours");
    expect(defaultAccountTagColor("investment")).toBe(INVESTMENT_ACCOUNT_TAG_COLOR);
    for (const type of ["bank", "wallet", "cash"]) expect(defaultAccountTagColor(type)).not.toBe(INVESTMENT_ACCOUNT_TAG_COLOR);
  });

  it("gives cards no shadow and floating layers the layer shadow token", () => {
    const rule = (selector: string) => {
      const start = globals.indexOf(`${selector} {`);
      expect(start, selector).toBeGreaterThanOrEqual(0);
      return globals.slice(start, globals.indexOf("}", start));
    };
    expect(rule(".ws-card")).toContain("box-shadow: none");
    for (const floating of [".ws-menu", ".popover", ".ws-explain", ".ws-toast", "dialog.shell-dialog", '.ws-drawer[data-mode="dialog"]']) {
      expect(rule(floating), floating).toContain("box-shadow: var(--shadow-float)");
    }
  });

  it("stacks the detail dialog and the phone sheet above the scrim that dims the page", () => {
    const zOf = (selector: string) => Number(globals.slice(globals.indexOf(`${selector} {`)).match(/z-index:\s*(\d+)/)![1]);
    const scrim = zOf(".ws-sheet-scrim");
    expect(zOf('.ws-drawer[data-mode="dialog"]')).toBeGreaterThan(scrim);
    expect(zOf('.ws-drawer[data-mode="sheet"]')).toBeGreaterThan(scrim);
  });

  describe.each(Object.entries(themes))("in %s", (selector, theme) => {
    it.each(textPairs)("keeps %s at WCAG AA on its backgrounds", (text, backgrounds) => {
      for (const background of backgrounds) {
        expect(contrast(painted(theme, text), painted(theme, background)), `${text} on ${background} in ${selector}`)
          .toBeGreaterThanOrEqual(4.5);
      }
    });

    it("paints expense amounts with --out-fg at 4.5:1 on the canvas, every surface and the dialog (D29)", () => {
      for (const background of ["--canvas", ...SURFACES]) {
        expect(contrast(painted(theme, "--out-fg"), painted(theme, background)), `--out-fg on ${background} in ${selector}`).toBeGreaterThanOrEqual(4.5);
      }
      // A lighter red would fail: the check is not vacuous.
      expect(contrast(hex("#FF8A95"), painted(theme, "--surface-2")) >= 4.5 && contrast(hex("#B8142D"), painted(theme, "--surface-2")) >= 4.5).toBe(false);
    });

    it.each(markPairs)("keeps the mark %s at 3:1 on its backgrounds", (mark, backgrounds) => {
      for (const background of backgrounds) {
        expect(contrast(painted(theme, mark), painted(theme, background)), `${mark} on ${background} in ${selector}`).toBeGreaterThanOrEqual(3);
      }
    });

    it("sets a focused field's edge apart from its resting edge", () => {
      // A focused field's edge differs from its resting edge by at least 3:1 (WCAG 1.4.11). The dark value is
      // one step lighter than the prototype's so it reaches 3:1.
      expect(contrast(painted(theme, "--field-focus"), painted(theme, "--line-strong")), selector).toBeGreaterThanOrEqual(3);
    });

    it("keeps the keyboard focus outline at 7:1 on every surface", () => {
      for (const surface of SURFACES) {
        expect(contrast(painted(theme, "--focus"), painted(theme, surface)), `--focus on ${surface} in ${selector}`).toBeGreaterThanOrEqual(7);
      }
    });

    it.each([...PILLARS, "unclassified"])("keeps the %s pillar mark at 3:1 on the canvas, the surfaces and the raised fill", (pillar) => {
      const colour = painted(theme, pillarToken(pillar));
      for (const background of ["--canvas", "--window", "--surface", "--surface-2"]) {
        expect(contrast(colour, painted(theme, background)), `${pillar} mark on ${background} in ${selector}`).toBeGreaterThanOrEqual(3);
      }
    });

    it("keeps Belum diklasifikasi a neutral grey", () => {
      // "Belum diklasifikasi" is missing information, so it stays grey rather than a sixth hue.
      expect(hueOf(painted(theme, "--k-none")).saturation, `unclassified in ${selector}`).toBeLessThan(0.25);
    });

    it("uses blue only for transfers, the Budaya pillar and Dana investasi: no info blue and no other blue token", () => {
      // Chroma rather than HSL saturation: the slate neutrals read as grey although their HSL saturation is high.
      const blue = Object.entries(theme)
        .filter(([, value]) => /^#[0-9A-Fa-f]{6}$/.test(value))
        .filter(([, value]) => {
          const colour = hex(value);
          const chroma = (Math.max(...colour) - Math.min(...colour)) / 255;
          const { hue } = hueOf(colour);
          return chroma > 0.2 && hue >= 190 && hue <= 245;
        })
        .map(([name]) => name);
      const allowed = (name: string) => name.startsWith("--tr-") || name === "--k-culture" || name === "--use-dana";
      expect(blue.filter((name) => !allowed(name))).toEqual([]);
      expect(Object.keys(theme).filter((name) => name.startsWith("--info"))).toEqual([]);
    });
  });
});

describe("the expense ink (apply-markas-transaksi D29)", () => {
  it("resolves .c-expense to --out-fg, the class RowAmount gives every spending amount", () => {
    expect(globals).toMatch(/\.c-expense\s*\{\s*color:\s*var\(--out-fg\);?\s*\}/);
    const rows = readFileSync(path.join(srcDir, "components/transactions/rows.tsx"), "utf8");
    expect(rows).toMatch(/expense:\s*"t-amt c-expense"/);
  });
});
