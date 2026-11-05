import type { AccountItem } from "./types";

/** The stored default of new accounts; such accounts get a distinct display hue (refine-workspace-layout D7). Nothing is written. */
const DEFAULT_COLOURS = new Set(["#3b82f6", "#2563eb"]);
// Hues apart from each other and from the semantic colours; every one passes 3:1 as a mark on the surfaces.
const ACCOUNT_HUES = ["#2E7DF6", "#F08A24", "#16A34A", "#E0A800", "#7C6CF2", "#0FB5AE", "#E5484D", "#A855F7", "#64748B", "#D9467F"];

function hueOf(hex: string): number | null {
  const match = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  if (!match) return null;
  const [r, g, b] = match.slice(1).map((part) => parseInt(part, 16) / 255);
  const max = Math.max(r, g, b);
  const chroma = max - Math.min(r, g, b);
  if (chroma < 0.15) return null;
  let sector: number;
  if (max === r) sector = ((g - b) / chroma) % 6;
  else if (max === g) sector = (b - r) / chroma + 2;
  else sector = (r - g) / chroma + 4;
  return (sector * 60 + 360) % 360;
}

/** Display colours of the top-level accounts: stored colours stay once, default or repeated ones get a hue no other account is near. */
export function displayColours(accounts: AccountItem[]): Map<string, string> {
  const colours = new Map<string, string>();
  const stored = accounts.map((account) => account.color?.toLowerCase()).filter((colour): colour is string => Boolean(colour) && !DEFAULT_COLOURS.has(colour!));
  const taken = stored.map(hueOf).filter((hue): hue is number => hue !== null);
  const near = (hue: number | null) => hue !== null && taken.some((other) => Math.min(Math.abs(hue - other), 360 - Math.abs(hue - other)) < 25);
  const free = ACCOUNT_HUES.filter((colour) => !stored.includes(colour.toLowerCase()) && !near(hueOf(colour)));
  let next = 0;
  const shown = new Set<string>();
  for (const account of accounts) {
    const own = account.color?.toLowerCase();
    // A stored colour is kept the first time; a second account with the same colour gets a free hue, so no
    // two top-level accounts look alike (the preview's rule).
    if (own && !DEFAULT_COLOURS.has(own) && !shown.has(own)) colours.set(account.id, account.color!);
    else colours.set(account.id, free.length ? free[next++ % free.length] : ACCOUNT_HUES[next++ % ACCOUNT_HUES.length]);
    shown.add(colours.get(account.id)!.toLowerCase());
  }
  return colours;
}

/** The badge ink with the higher WCAG contrast on an account colour (white or the dark text ink). */
export function badgeInk(colour: string): string {
  const match = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(colour);
  if (!match) return "#FFFFFF";
  const [r, g, b] = match.slice(1).map((part) => {
    const value = parseInt(part, 16) / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return (1.05 / (luminance + 0.05)) >= ((luminance + 0.05) / 0.0536) ? "#FFFFFF" : "#0A0D14";
}
