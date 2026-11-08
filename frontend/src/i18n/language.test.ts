import { describe, expect, it } from "vitest";
import { matchAcceptLanguage, parseLanguage, resolveLanguage } from "./language";

/** Language resolution before sign-in (add-language-system D2): cookie, then Accept-Language, then English. */
describe("Accept-Language matching", () => {
  it.each([
    ["id-ID,id;q=0.9,en;q=0.8", "id"],
    ["en-US,id;q=0.5", "en"],
    ["in", "id"],
    ["ms, fr", null],
    ["fr;q=1, id;q=0.4, en;q=0.3", "id"],
    ["en;q=0, id;q=0.1", "id"],
    ["id;q=2", null],
    ["*;q=0.5", null],
    [";;;", null],
    ["", null],
  ])("%s gives %s", (header, expected) => {
    expect(matchAcceptLanguage(header)).toBe(expected);
  });

  it("keeps the earlier of two equally preferred languages", () => {
    expect(matchAcceptLanguage("en, id")).toBe("en");
    expect(matchAcceptLanguage("id, en")).toBe("id");
  });
});

describe("resolveLanguage", () => {
  it("falls back to English for a missing, malformed or unsupported header", () => {
    expect(resolveLanguage(undefined, undefined)).toBe("en");
    expect(resolveLanguage(undefined, "ms, fr")).toBe("en");
    expect(resolveLanguage(undefined, "q=0.5;;")).toBe("en");
  });

  it("prefers the remembered choice over the browser", () => {
    expect(resolveLanguage("id", "en-US")).toBe("id");
    expect(resolveLanguage("en", "id-ID,id;q=0.9")).toBe("en");
  });

  it("ignores an unknown cookie value", () => {
    expect(resolveLanguage("fr", "id-ID")).toBe("id");
    expect(resolveLanguage("EN", undefined)).toBe("en");
    expect(parseLanguage("de")).toBeNull();
  });
});
