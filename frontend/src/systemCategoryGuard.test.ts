import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

/**
 * The web never identifies a system category by its display name (add-category-system-keys D6, D8): it reads
 * `system_key` or `category_system_key` through `lib/systemCategories.ts`. A seed name used as copy (a tab label,
 * a role name) is fine; a seed name compared with, collected into a set or list that is searched, or held in a
 * constant is not. Tests and the development-only /komponen sheet are left out.
 */
const NAMES = ["Investasi", "Internal Movement", "Penyesuaian Saldo"];
const QUOTED = `["'\`](?:${NAMES.join("|")})["'\`]`;
const NAME_CHECKS = [
  // row.category_name === "Internal Movement", "Investasi" !== name
  new RegExp(`(?:===|!==|==|!=)\\s*${QUOTED}|${QUOTED}\\s*(?:===|!==|==|!=)`),
  // ["Internal Movement", "Investasi"].includes(name), new Set(["Investasi"])
  new RegExp(`\\[[^\\]]*${QUOTED}[^\\]]*\\]\\s*\\.(?:includes|indexOf|some|find)\\(`),
  new RegExp(`new (?:Set|Map)\\(\\s*\\[[^\\]]*${QUOTED}`),
  // const MOVEMENT_CATEGORY = "Internal Movement"
  new RegExp(`\\b(?:const|let|var)\\s+\\w+\\s*(?::[^=]+)?=\\s*${QUOTED}`),
  // switch (category.name) { case "Investasi": ... }
  new RegExp(`\\bcase\\s+${QUOTED}\\s*:`),
];

function sources(full: string): string[] {
  if (statSync(full).isFile()) return [full];
  return readdirSync(full).flatMap((name) => sources(path.join(full, name)));
}

function isScanned(file: string): boolean {
  const relative = path.relative(__dirname, file).split(path.sep).join("/");
  return /\.(ts|tsx)$/.test(relative) && !/\.test\.tsx?$/.test(relative) && !relative.startsWith("app/komponen/")
    && !relative.startsWith("test/");
}

/** `file:line: text` for every line of code under `root` that checks a category by a system name. */
function nameChecks(files: { name: string; text: string }[]): string[] {
  return files.flatMap(({ name, text }) => text.split("\n").flatMap((line, index) => {
    const code = line.replace(/\/\/.*$/, "");
    const trimmed = code.trim();
    if (!trimmed || trimmed.startsWith("*") || trimmed.startsWith("/*")) return [];
    return NAME_CHECKS.some((pattern) => pattern.test(code)) ? [`${name}:${index + 1}: ${trimmed}`] : [];
  }));
}

describe("system category guard", () => {
  it("finds no name check in the web's code", () => {
    const files = sources(__dirname).filter(isScanned)
      .map((file) => ({ name: path.relative(__dirname, file), text: readFileSync(file, "utf8") }));
    expect(files.length).toBeGreaterThan(100);
    expect(nameChecks(files)).toEqual([]);
  });

  it("names the file and line of a planted name check and leaves copy alone", () => {
    const planted = [
      'export const label = "Investasi";',
      '// row.category_name === "Internal Movement" in a comment is fine',
      'if (row.category_name === "Internal Movement") return "movement";',
      'const skip = ["Internal Movement", "Investasi"].includes(row.name);',
      'const NAMES = new Set(["Penyesuaian Saldo"]);',
      'const MOVEMENT = "Internal Movement";',
      '{ value: "investment", label: "Investasi", count: "investment" },',
    ].join("\n");
    expect(nameChecks([{ name: "components/planted.tsx", text: planted }])).toEqual([
      'components/planted.tsx:1: export const label = "Investasi";',
      'components/planted.tsx:3: if (row.category_name === "Internal Movement") return "movement";',
      'components/planted.tsx:4: const skip = ["Internal Movement", "Investasi"].includes(row.name);',
      'components/planted.tsx:5: const NAMES = new Set(["Penyesuaian Saldo"]);',
      'components/planted.tsx:6: const MOVEMENT = "Internal Movement";',
    ]);
  });
});
