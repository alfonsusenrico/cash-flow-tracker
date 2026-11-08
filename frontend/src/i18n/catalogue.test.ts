import { parse, TYPE, type MessageFormatElement } from "@formatjs/icu-messageformat-parser";
import { describe, expect, it } from "vitest";
import en from "@/messages/en.json";
import id from "@/messages/id.json";
import { keyPaths } from "./catalogue";

/**
 * Catalogue parity (interface-language "Message Catalogue Parity", add-language-system D16): the same keys, no
 * empty values, the same ICU arguments and tags per message, and English counts with singular and plural forms.
 */
type Catalogue = { [key: string]: string | Catalogue };

function flatten(catalogue: Catalogue, prefix = ""): Map<string, string> {
  const result = new Map<string, string>();
  for (const [key, value] of Object.entries(catalogue)) {
    const keyPath = prefix ? `${prefix}.${key}` : key;
    if (typeof value === "string") result.set(keyPath, value);
    else for (const [nested, text] of flatten(value, keyPath)) result.set(nested, text);
  }
  return result;
}

/** Argument and tag names of a message, and the plural arguments with their branches. */
function argumentsOf(message: string) {
  const names = new Set<string>();
  const plurals = new Map<string, Set<string>>();
  const walk = (elements: MessageFormatElement[]) => {
    for (const element of elements) {
      if (element.type === TYPE.argument || element.type === TYPE.number || element.type === TYPE.date || element.type === TYPE.time) {
        names.add(element.value);
      } else if (element.type === TYPE.select) {
        names.add(element.value);
        Object.values(element.options).forEach((option) => walk(option.value));
      } else if (element.type === TYPE.plural) {
        names.add(element.value);
        plurals.set(element.value, new Set(Object.keys(element.options)));
        Object.values(element.options).forEach((option) => walk(option.value));
      } else if (element.type === TYPE.tag) {
        names.add(`<${element.value}>`);
        walk(element.children);
      }
    }
  };
  walk(parse(message));
  return { names: [...names].sort(), plurals };
}

/** Number formats in a message would follow the message language, not the region (D5); `#` in plurals is the count. */
function hasIcuFormat(message: string): boolean {
  let found = false;
  const walk = (elements: MessageFormatElement[]) => {
    for (const element of elements) {
      if (element.type === TYPE.number || element.type === TYPE.date || element.type === TYPE.time) found = true;
      if (element.type === TYPE.select || element.type === TYPE.plural) Object.values(element.options).forEach((option) => walk(option.value));
      if (element.type === TYPE.tag) walk(element.children);
    }
  };
  walk(parse(message));
  return found;
}

/** Arguments that hold a count, so English must say "1 day" and "2 days". */
const COUNT_ARGUMENTS = new Set(["count", "days", "months"]);

export function parityProblems(english: Catalogue, indonesian: Catalogue): string[] {
  const enMessages = flatten(english);
  const idMessages = flatten(indonesian);
  const problems: string[] = [];
  for (const key of idMessages.keys()) if (!enMessages.has(key)) problems.push(`${key}: missing in en`);
  for (const key of enMessages.keys()) if (!idMessages.has(key)) problems.push(`${key}: missing in id`);
  for (const [key, enText] of enMessages) {
    const idText = idMessages.get(key);
    if (!enText.trim()) problems.push(`${key}: empty in en`);
    if (idText === undefined) continue;
    if (!idText.trim()) problems.push(`${key}: empty in id`);
    const enArgs = argumentsOf(enText);
    const idArgs = argumentsOf(idText);
    if (enArgs.names.join(",") !== idArgs.names.join(",")) problems.push(`${key}: arguments differ (${enArgs.names} / ${idArgs.names})`);
    for (const name of enArgs.names.filter((arg) => COUNT_ARGUMENTS.has(arg))) {
      const branches = enArgs.plurals.get(name);
      if (!branches?.has("one") || !branches.has("other")) problems.push(`${key}: English {${name}} needs one and other`);
    }
    if (hasIcuFormat(enText) || hasIcuFormat(idText)) problems.push(`${key}: formats a number or date itself`);
  }
  return problems;
}

describe("message catalogues", () => {
  it("have the same keys, no empty values and the same arguments in both languages", () => {
    expect(parityProblems(en as unknown as Catalogue, id as unknown as Catalogue)).toEqual([]);
    expect(keyPaths(en).length).toBe(keyPaths(id).length);
    expect(keyPaths(en).length).toBeGreaterThan(300);
  });

  it("fail on a missing English message and name the key", () => {
    const english = { home: { needsAction: "Needs action" } };
    const indonesian = { home: { needsAction: "Perlu tindakan", openReview: "Buka Tinjau" } };
    expect(parityProblems(english, indonesian)).toEqual(["home.openReview: missing in en"]);
  });

  it("fail on an argument mismatch and name the message", () => {
    const english = { home: { daysUntilPayday: "{days, plural, one {# day} other {# days}} until payday on {date}" } };
    const indonesian = { home: { daysUntilPayday: "{n} hari lagi sampai gajian {date}" } };
    expect(parityProblems(english, indonesian)).toEqual(["home.daysUntilPayday: arguments differ (date,days / date,n)"]);
  });

  it("fail on an English count without its singular", () => {
    const english = { home: { itemCount: "{count} items" } };
    const indonesian = { home: { itemCount: "{count} item" } };
    expect(parityProblems(english, indonesian)).toEqual(["home.itemCount: English {count} needs one and other"]);
  });

  it("use Indonesian's single plural category, as Intl.PluralRules reports", () => {
    expect(new Intl.PluralRules("id").resolvedOptions().pluralCategories).toEqual(["other"]);
    expect(new Intl.PluralRules("en").resolvedOptions().pluralCategories).toEqual(expect.arrayContaining(["one", "other"]));
  });
});
