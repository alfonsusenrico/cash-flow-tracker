import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { PHONE_QUERY } from "@/components/workspace/layers";

/*
 * One set of shell breakpoints (apply-markas-shell D2): the phone layout below 768 px and the icon rail below
 * 1024 px. Container queries on the page frame keep their own values, so only media rules and the page's
 * viewport constants are checked.
 */
const srcDir = path.resolve(__dirname, "..");

function filesUnder(dir: string, pattern: RegExp): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return filesUnder(full, pattern);
    return pattern.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [full] : [];
  });
}

const OLD_MEDIA = /@media[^{]*\b(760|761|1180|1181)px/;
const OLD_CONSTANT = /\b(PHONE_[A-Z_]*WIDTH\s*=\s*(760|761)|useWidthAtMost\([^)]*,\s*760\)|max-width:\s*(760|1180)px)/;

describe("shell breakpoints", () => {
  it("switches the phone shell below 768 px", () => {
    expect(PHONE_QUERY).toBe("(max-width: 767px)");
  });

  it("leaves no 760, 761 or 1180 px media rule or phone constant in frontend/src", () => {
    const sheets = filesUnder(srcDir, /\.css$/).filter((file) => OLD_MEDIA.test(readFileSync(file, "utf8")));
    const sources = filesUnder(srcDir, /\.tsx?$/).filter((file) => OLD_CONSTANT.test(readFileSync(file, "utf8")));
    expect([...sheets, ...sources].map((file) => path.relative(srcDir, file))).toEqual([]);
  });

  it("finds a planted old breakpoint", () => {
    expect(OLD_MEDIA.test("@media (max-width: 760px) { .x { color: red; } }")).toBe(true);
    expect(OLD_MEDIA.test("@container main (max-width: 760px) { .x { color: red; } }")).toBe(false);
    expect(OLD_CONSTANT.test("const PHONE_WIDTH = 760;")).toBe(true);
  });
});
