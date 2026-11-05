import { existsSync, readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const appDir = path.resolve(__dirname);
const read = (file: string) => readFileSync(path.join(appDir, file), "utf8");

describe("web fonts", () => {
  it("loads both families from bundled files, never from Google at build time", () => {
    const layout = read("layout.tsx");

    expect(layout).not.toContain("next/font/google");
    for (const src of layout.matchAll(/src: "\.\/(fonts\/[^"]+\.woff2)"/g)) {
      expect(existsSync(path.join(appDir, src[1]))).toBe(true);
    }
    expect([...layout.matchAll(/src: "\.\/fonts\//g)]).toHaveLength(2);
  });

  it("ships the OFL licence beside each bundled font", () => {
    expect(read("fonts/Inter-OFL.txt")).toContain("SIL Open Font License");
    expect(read("fonts/Manrope-OFL.txt")).toContain("SIL Open Font License");
  });

  it("uses Inter for text and amounts and Manrope for titles, and ships no other family", () => {
    const layout = read("layout.tsx");
    expect(layout).toContain('src: "./fonts/Inter-latin-variable.woff2"');
    expect(layout).toContain('src: "./fonts/Manrope-latin-600.woff2"');
    // Inter's variable range clamps to 400–600 and Manrope is the single 600 instance (SPEC §1).
    expect(layout).toMatch(/Inter-latin-variable\.woff2",\n\s+variable: "--font-inter",\n\s+display: "swap",\n\s+weight: "400 600"/);
    expect(layout).toMatch(/Manrope-latin-600\.woff2",\n\s+variable: "--font-manrope",\n\s+display: "swap",\n\s+weight: "600"/);
    expect(readdirSync(path.join(appDir, "fonts")).sort()).toEqual([
      "Inter-OFL.txt",
      "Inter-latin-variable.woff2",
      "Manrope-OFL.txt",
      "Manrope-latin-600.woff2",
    ]);
    expect(layout).not.toMatch(/Jakarta|Geist/);
    const tokens = readFileSync(path.resolve(appDir, "../../tokens.css"), "utf8");
    expect(tokens).toContain('--font-sans: var(--font-inter), "Inter"');
    expect(tokens).toContain('--font-display: var(--font-manrope), "Manrope"');
    expect(tokens).not.toMatch(/Jakarta|Geist|--font-mono/);
  });

  it("turns off synthetic bold so no weight above 600 is faked", () => {
    expect(read("globals.css")).toMatch(/body \{[^}]*font-synthesis-weight: none;/);
  });

  it("keeps stylesheets free of third-party font imports", () => {
    expect(read("globals.css")).not.toMatch(/@import\s+url\(\s*['"]?https?:/);
  });

  it("restricts the content security policy to same-origin fonts and styles", () => {
    const nginx = readFileSync(path.resolve(appDir, "../../../nginx/default.conf"), "utf8");

    expect(nginx).toContain("font-src 'self';");
    expect(nginx).not.toMatch(/fonts\.(googleapis|gstatic)\.com/);
  });
});
