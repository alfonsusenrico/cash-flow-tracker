import { afterEach, describe, expect, it } from "vitest";
import { languageCookie, readLanguageCookie, writeLanguageCookie } from "./cookie";

afterEach(() => {
  document.cookie = "cft_language=; Path=/; Max-Age=0";
});

describe("cft_language cookie", () => {
  it("is Path=/, SameSite=Lax, a year long, Secure behind HTTPS and readable by the page", () => {
    expect(languageCookie("en", true)).toBe("cft_language=en; Path=/; Max-Age=31536000; SameSite=Lax; Secure");
    expect(languageCookie("id", false)).toBe("cft_language=id; Path=/; Max-Age=31536000; SameSite=Lax");
    expect(languageCookie("en", true)).not.toMatch(/HttpOnly/i);
  });

  it("is written and read back, and an unknown value reads as absent", () => {
    writeLanguageCookie("id");
    expect(readLanguageCookie()).toBe("id");
    document.cookie = "cft_language=fr; Path=/";
    expect(readLanguageCookie()).toBeNull();
  });
});
