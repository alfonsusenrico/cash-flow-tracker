import { renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IntlTestProvider } from "./IntlTestProvider";
import { readLanguageCookie, writeLanguageCookie } from "./cookie";
import { useAccountLanguage } from "./useAccountLanguage";

const router = vi.hoisted(() => ({ refresh: vi.fn() }));
vi.mock("next/navigation", () => ({ useRouter: () => router }));

function inLanguage(language: "en" | "id") {
  return function Catalogue({ children }: { children: ReactNode }) {
    return <IntlTestProvider language={language}>{children}</IntlTestProvider>;
  };
}

beforeEach(() => router.refresh.mockClear());
afterEach(() => {
  document.cookie = "cft_language=; Path=/; Max-Age=0";
});

/** The account wins when signed in (add-language-system D2): one refresh on a mismatch, none on a match. */
describe("useAccountLanguage", () => {
  it("adopts the account's language with exactly one refresh when the browser remembers another", () => {
    writeLanguageCookie("id");
    const { rerender } = renderHook(({ language }) => useAccountLanguage(language), {
      initialProps: { language: "en" },
      wrapper: inLanguage("id"),
    });
    expect(readLanguageCookie()).toBe("en");
    rerender({ language: "en" });
    rerender({ language: "en" });
    expect(router.refresh).toHaveBeenCalledTimes(1);
  });

  it("does not refresh when the account and the interface agree", () => {
    writeLanguageCookie("en");
    renderHook(() => useAccountLanguage("en"), { wrapper: inLanguage("en") });
    expect(router.refresh).not.toHaveBeenCalled();
    expect(readLanguageCookie()).toBe("en");
  });

  it("rewrites a stale cookie even when the page already shows the account's language", () => {
    writeLanguageCookie("en");
    renderHook(() => useAccountLanguage("id"), { wrapper: inLanguage("id") });
    expect(readLanguageCookie()).toBe("id");
    expect(router.refresh).not.toHaveBeenCalled();
  });

  it("on a shared browser, the next account's language replaces the previous one's", () => {
    writeLanguageCookie("en");
    renderHook(() => useAccountLanguage("id"), { wrapper: inLanguage("en") });
    expect(readLanguageCookie()).toBe("id");
    expect(router.refresh).toHaveBeenCalledTimes(1);
  });

  it("ignores a profile without a supported language", () => {
    writeLanguageCookie("id");
    renderHook(() => useAccountLanguage(undefined), { wrapper: inLanguage("id") });
    renderHook(() => useAccountLanguage("fr"), { wrapper: inLanguage("id") });
    expect(readLanguageCookie()).toBe("id");
    expect(router.refresh).not.toHaveBeenCalled();
  });
});
