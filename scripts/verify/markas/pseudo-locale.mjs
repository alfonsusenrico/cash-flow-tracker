// Dev-only pseudo-locale en-XA (add-language-system D16): every English message with accented letters and about
// 40% padding, so literal strings and tight boxes show up before real copy exists. It writes a catalogue file for
// a throwaway build and is never shipped: no app code imports it.
//
// Usage: node pseudo-locale.mjs <frontend>/src/messages/en.json <out.json>
import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [source, target] = process.argv.slice(2);
if (!source || !target) {
  console.error("Usage: node pseudo-locale.mjs <en.json> <out.json>");
  process.exit(2);
}
// The parser and printer are the frontend's own dev dependency (the version next-intl resolves).
const require = createRequire(path.resolve(path.dirname(source), "../../package.json"));
const { parse, TYPE } = await import(pathToFileURL(require.resolve("@formatjs/icu-messageformat-parser")).href);
const { printAST } = await import(pathToFileURL(require.resolve("@formatjs/icu-messageformat-parser/printer.js")).href);

const ACCENTS = { a: "á", e: "é", i: "í", o: "ö", u: "ü", c: "ç", n: "ñ", y: "ý", A: "Å", E: "É", I: "Î", O: "Ø", U: "Ü", C: "Ç", N: "Ñ", S: "Š", s: "š", z: "ž" };

function accent(text) {
  return [...text].map((char) => ACCENTS[char] ?? char).join("");
}

/** Accents every literal and lengthens the message by about 40% with a marked tail. */
function pseudo(message) {
  const ast = parse(message);
  let letters = 0;
  const walk = (elements) => {
    for (const element of elements) {
      if (element.type === TYPE.literal) {
        letters += element.value.replace(/\s/g, "").length;
        element.value = accent(element.value);
      } else if (element.type === TYPE.select || element.type === TYPE.plural) {
        Object.values(element.options).forEach((option) => walk(option.value));
      } else if (element.type === TYPE.tag) {
        walk(element.children);
      }
    }
  };
  walk(ast);
  const padding = "·".repeat(Math.max(1, Math.round(letters * 0.4)));
  return `[${printAST(ast)} ${padding}]`;
}

function transform(node) {
  return Object.fromEntries(Object.entries(node).map(([key, value]) => [key, typeof value === "string" ? pseudo(value) : transform(value)]));
}

writeFileSync(target, `${JSON.stringify(transform(JSON.parse(readFileSync(source, "utf8"))), null, 2)}\n`);
console.log(`wrote ${target}`);
