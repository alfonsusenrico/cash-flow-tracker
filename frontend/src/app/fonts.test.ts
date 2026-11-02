import { existsSync, readFileSync } from "node:fs";
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
    expect(read("fonts/Geist-OFL.txt")).toContain("SIL Open Font License");
    expect(read("fonts/GeistMono-OFL.txt")).toContain("SIL Open Font License");
  });

  it("uses Geist for text and Geist Mono for code, and no longer ships Plus Jakarta Sans", () => {
    const layout = read("layout.tsx");
    expect(layout).toContain('src: "./fonts/Geist-latin-variable.woff2"');
    expect(layout).toContain('src: "./fonts/GeistMono-latin-variable.woff2"');
    expect(layout).not.toMatch(/Jakarta/i);
    expect(existsSync(path.join(appDir, "fonts/PlusJakartaSans-latin-variable.woff2"))).toBe(false);
    expect(readFileSync(path.resolve(appDir, "../../tokens.css"), "utf8")).toContain('--font-body: var(--font-geist), "Geist"');
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
