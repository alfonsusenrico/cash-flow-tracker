import { readdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import resolveConfig from "tailwindcss/resolveConfig";
import { describe, expect, it } from "vitest";

const appDir = path.resolve(__dirname);
const srcDir = path.resolve(appDir, "..");
const insightsPage = readFileSync(path.join(appDir, "insights/page.tsx"), "utf8");
const ledgerPage = readFileSync(path.join(appDir, "ledger/page.tsx"), "utf8");
const goalsPage = readFileSync(path.join(appDir, "goals/page.tsx"), "utf8");
const berandaActivity = readFileSync(path.join(srcDir, "components/beranda/activity.tsx"), "utf8");
const mobileLedgerFeed = readFileSync(path.join(srcDir, "components/ledger/MobileLedgerFeed.tsx"), "utf8");
const accountsPage = readFileSync(path.join(appDir, "accounts/page.tsx"), "utf8");
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
  // Legacy names still painted by pages that are not redesigned yet.
  ["--primary-contrast", ["--primary", "--primary-hover"]],
  ["--accent-text", ["--surface", "--bg", "--surface-raised"]],
  ["--expense-text", ["--surface", "--bg", "--surface-raised", "--color-expense-soft"]],
  ["--primary-text", ["--surface", "--bg", "--surface-raised", "--sidebar-hover"]],
  ["--danger-text", ["--surface", "--bg", "--surface-raised"]],
  ["--income-text", ["--surface", "--bg", "--surface-raised", "--color-income-soft"]],
  ["--income-contrast", ["--color-income"]],
  ["--income-hover-contrast", ["--color-income-hover"]],
  ["--warning-text", ["--surface", "--bg", "--surface-raised", "--color-warning-soft"]],
  ["--transfer-text", ["--surface", "--bg", "--surface-raised"]],
  ["--transfer-contrast", ["--color-transfer"]],
  ["--investment-text", ["--surface", "--bg", "--surface-raised", "--color-investment-soft"]],
  ["--investment-contrast", ["--color-investment"]],
];

// Icons and marks need 3:1 (WCAG 1.4.11). The focus ring sits 2px outside its control, on the window or a surface.
const markPairs: [string, string[]][] = [
  ["--focus", SURFACES],
  ["--ok-dot", ["--surface", "--window"]],
  ["--lime-text", ["--lime-soft"]],
];

const PILLARS = ["need", "want", "culture", "unexpected", "saving"] as const;

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
    expect(resolve(themes[":root"], "--color-transfer")).toBe("#3787C8");
    expect(resolve(themes[".dark"], "--color-transfer")).toBe("#58A6E6");
  });

  it("paints every Pindah Saldo and movement element sky, never amber or blue", () => {
    const classLists = (source: string) =>
      [...source.matchAll(/(?:isMovement|is_excluded_from_budget)\s*\?\s*"([^"]*)"/g)]
        .map((match) => match[1])
        .filter((classes) => /(^|\s)(text|bg|border)-/.test(classes));
    for (const [name, source] of [
      ["Transaksi", ledgerPage],
      ["MobileLedgerFeed", mobileLedgerFeed],
    ] as const) {
      const movementClasses = classLists(source);
      expect(movementClasses.length, `${name} movement branches`).toBeGreaterThan(0);
      for (const classes of movementClasses) {
        expect(classes, `${name}: ${classes}`).toMatch(/(^|\s)text-sky-500(\s|$)/);
        expect(classes, `${name}: ${classes}`).not.toMatch(/(amber|blue)-/);
      }
    }
    expect(ledgerPage).toMatch(/isMovement\s*\?\s*"var\(--color-transfer\)"/);
    // Beranda activity: a movement's avatar and amount use the transfer pair, a top-up the investment pair.
    expect(berandaActivity).toMatch(/movement: \{ icon: "repeat", tone: "ws-tone-tr" \}/);
    expect(berandaActivity).toMatch(/topup: \{ icon: "trending-up", tone: "ws-tone-inv" \}/);
    expect(berandaActivity).toMatch(/kind === "movement"\)[\s\S]{0,80}ws-amt-tr/);
    expect(berandaActivity).toMatch(/kind === "topup"\) return <span className="ws-amt-inv">/);
  });

  it("paints violet text utilities with the investment text ink and keeps the violet fills", () => {
    const { textColor, backgroundColor } = tailwindTheme;
    for (const shade of ["400", "500", "600", "700"]) {
      expect(textColor.violet[shade], `text-violet-${shade}`).toBe("var(--investment-text)");
    }
    expect(textColor.investment.DEFAULT).toBe("var(--investment-text)");
    expect(textColor.investment.contrast).toBe("var(--investment-contrast)");
    expect(backgroundColor.violet["500"]).toBe("#8b5cf6");
    expect(backgroundColor.investment.DEFAULT).toBe("var(--color-investment)");
    expect(backgroundColor.investment.soft).toBe("var(--color-investment-soft)");
    expect(resolve(themes[":root"], "--color-investment")).toBe("#7553C7");
    expect(resolve(themes[".dark"], "--color-investment")).toBe("#A68CEE");
  });

  it("paints every investment element lavender, never amber, sky, blue, purple or indigo", () => {
    // Investment top-up rows on Transaksi: every class or colour branch keyed on the top-up is lavender.
    for (const [name, source] of [["Transaksi", ledgerPage], ["MobileLedgerFeed", mobileLedgerFeed]] as const) {
      const branches = [...source.matchAll(/investment_topup_id\s*\?\s*"([^"]*)"/g)]
        .map((match) => match[1])
        .filter((value) => /(^|\s)(text|bg|border)-|var\(/.test(value));
      expect(branches.length, `${name} top-up branches`).toBeGreaterThanOrEqual(name === "Transaksi" ? 5 : 3);
      for (const classes of branches) {
        expect(classes, `${name}: ${classes}`).toMatch(/investment/);
        expect(classes, `${name}: ${classes}`).not.toMatch(/(amber|sky|blue|purple|indigo)-|--color-transfer/);
      }
    }

    // The Rekening allocation share and the price-sync message.
    const shares = [...accountsPage.matchAll(/investPct > 0 && [^\n]*className="([^"]*)"|<span className="([^"]*)" \/>\s*Investasi:/g)]
      .map((match) => match[1] ?? match[2]);
    expect(shares).toHaveLength(2);
    for (const classes of shares) expect(classes).toContain("bg-investment");
    expect(accountsPage).not.toMatch(/bg-indigo-500(?!\/)/);
    expect(accountsPage).toMatch(/\{syncMessage && \(\s*<span className="[^"]*text-investment[^"]*">/);

    // Purple was a second lavender for top-ups; investment elements now share one family.
    const purple = componentFiles(srcDir).filter((file) => /(^|[\s"'`:])(text|bg|border)-purple-\d/.test(readFileSync(file, "utf8")));
    expect(purple.map((file) => path.relative(srcDir, file))).toEqual([]);
  });

  it("puts the investment contrast ink on every solid lavender fill", () => {
    const solid = /(^|\s)bg-(investment|violet-[5-9]00)(\s|$)/;
    const classStrings = componentSources(srcDir).flatMap((source) => [...source.matchAll(/"([^"]*)"/g)].map((match) => match[1]));
    const filled = classStrings.filter((classes) => solid.test(classes) && /(^|\s)text-/.test(classes));
    expect(filled.length).toBeGreaterThanOrEqual(4);
    for (const classes of filled) {
      expect(classes).toContain("text-investment-contrast");
      expect(classes).not.toMatch(/(^|\s)text-white(\s|$)/);
    }
  });

  it("uses sky only for transfers and neutral tokens for the Pilar Kakeibo card and information notes", () => {
    // Every sky utility must sit in a transfer branch, a movement, a Pindah Saldo action or a transfer target,
    // named on its own line or in the eight lines before it.
    const transferMarker = /isMovement|is_consolidated_transfer|is_inferred_transfer|movement_id|target_account_name|Internal Movement|selectedTx\.type === "transfer"|handleOpenTransfer|onOpenTransfer|canTransfer/;
    const outside: string[] = [];
    for (const file of componentFiles(srcDir)) {
      const source = readFileSync(file, "utf8");
      for (const match of source.matchAll(/(?:text|bg|border|ring|from|via|to|fill|stroke)-sky-\d{2,3}/g)) {
        const line = source.slice(0, match.index).split("\n").length;
        const context = source.split("\n").slice(Math.max(0, line - 9), line).join("\n");
        if (!transferMarker.test(context)) {
          outside.push(`${path.relative(srcDir, file)}:${line} ${match[0]}`);
        }
      }
    }
    expect(outside).toEqual([]);

    expect(goalsPage).toMatch(/<p className="[^"]*bg-\[var\(--surface-raised\)\][^"]*text-\[var\(--text-secondary\)\]">\s*Ini hanya memperbarui angka progres target/);
    const banner = readFileSync(path.join(srcDir, "components/recurring/PendingScheduledBanner.tsx"), "utf8");
    expect(banner).toMatch(/<span className="[^"]*bg-\[var\(--surface-raised\)\][^"]*text-\[var\(--text\)\]">\s*Ditautkan otomatis/);
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

  it("paints Kakeibo pillars only through their tokens in kakeibo.ts", () => {
    const kakeibo = readFileSync(path.join(srcDir, "lib/kakeibo.ts"), "utf8");
    for (const pillar of PILLARS) {
      expect(kakeibo).toMatch(new RegExp(`value: "${pillar}",[\\s\\S]*?color: "var\\(--color-kakeibo-${pillar}\\)"`));
    }
    expect(kakeibo).toContain('color: "var(--color-kakeibo-unclassified)"');
    // Components take the pillar colour from KAKEIBO_PILLARS or KAKEIBO_BUCKETS, never by naming a token or a hex.
    const elsewhere = componentFiles(srcDir)
      .filter((file) => !file.endsWith(path.join("lib", "kakeibo.ts")))
      .filter((file) => /--color-kakeibo-/.test(readFileSync(file, "utf8")));
    expect(elsewhere.map((file) => path.relative(srcDir, file))).toEqual([]);
  });

  it("starts new investment accounts with a lavender tag colour", async () => {
    const { INVESTMENT_ACCOUNT_TAG_COLOR, defaultAccountTagColor } = await import("@/lib/accountColours");
    const tag = hueOf(hex(INVESTMENT_ACCOUNT_TAG_COLOR)).hue;
    expect(hueDistance(tag, hueOf(painted(themes[":root"], "--inv-solid")).hue)).toBeLessThan(25);
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
    expect(rule(".cockpit-card")).toContain("box-shadow: var(--shadow-card)");
    expect(rule(".card-squircle")).toContain("box-shadow: var(--shadow-card)");
    for (const floating of [".ws-menu", ".ws-explain", ".ws-toast", "dialog.ws-dialog"]) {
      expect(rule(floating), floating).toContain("box-shadow: var(--shadow-float)");
    }
  });

  it("keeps every legacy property the old pages use inside the marked compatibility block", () => {
    const root = tokens.slice(tokens.indexOf(":root {"), tokens.indexOf("\n}", tokens.indexOf(":root {")));
    const block = root.slice(root.indexOf("Compatibility aliases"), root.indexOf("End of compatibility aliases"));
    for (const legacy of ["--bg", "--surface-raised", "--border", "--text-secondary", "--primary", "--color-income", "--color-expense",
      "--color-transfer", "--color-investment", "--color-kakeibo-need", "--accent-lime", "--sidebar"]) {
      expect(block, legacy).toMatch(new RegExp(`${legacy}:\\s*var\\(--`));
    }
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

    it("keeps --investment-text at WCAG AA on the violet tints of investment chips", () => {
      // Investment chips, pills and notes paint Tailwind's violet-500 at 10 to 25% over the card or the raised surface.
      const violet = hex("#8b5cf6");
      for (const surface of ["--surface", "--surface-raised"]) {
        for (const alpha of [0.1, 0.15, 0.2, 0.25]) {
          const base = painted(theme, surface);
          const tint = base.map((channel, index) => violet[index] * alpha + channel * (1 - alpha)) as Rgb;
          expect(contrast(painted(theme, "--investment-text"), tint), `${alpha} violet over ${surface} in ${selector}`)
            .toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    it.each([...PILLARS, "unclassified"])("keeps the %s pillar colour at WCAG AA as text and as a mark", (pillar) => {
      // The pillar colour is a chart mark (dots, bar segments) and may be text. Text needs 4.5:1 on every surface
      // and the pillar's soft fill; marks need 3:1 on the bar tracks (--surface-raised and --border over the card).
      const colour = painted(theme, `--color-kakeibo-${pillar}`);
      for (const background of ["--surface", "--window", "--surface-2", `--color-kakeibo-${pillar}-soft`]) {
        expect(contrast(colour, painted(theme, background)), `${pillar} text on ${background} in ${selector}`).toBeGreaterThanOrEqual(4.5);
      }
      for (const track of ["--surface-3", "--line", "--surface-2"]) {
        expect(contrast(colour, painted(theme, track)), `${pillar} mark on ${track} in ${selector}`).toBeGreaterThanOrEqual(3);
      }
    });

    it("gives every pillar its own hue, apart from the transfer, investment, income and expense colours", () => {
      const pillarHue = (pillar: string) => hueOf(painted(theme, `--color-kakeibo-${pillar}`));
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
