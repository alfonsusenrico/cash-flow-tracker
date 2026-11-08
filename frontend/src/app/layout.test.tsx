import { isValidElement, type ReactElement } from "react";
import { describe, expect, it, vi } from "vitest";
import { createTranslator } from "next-intl";
import { CATALOGUES } from "@/lib/i18n/testing";

const request = vi.hoisted(() => ({ locale: "en" as "en" | "id" }));
vi.mock("next/font/local", () => ({ default: () => ({ variable: "font-var" }) }));
vi.mock("next-intl/server", () => ({
  getLocale: async () => request.locale,
  getMessages: async () => CATALOGUES[request.locale],
  getTranslations: async (namespace: string) =>
    createTranslator({ locale: request.locale, messages: CATALOGUES[request.locale], namespace: namespace as "shell" }),
}));

/** The document language and metadata follow the resolved language (add-language-system D12). */
describe("root layout", () => {
  it.each([
    ["en", "Everyday money notes: what went out, what is safe to spend, and what needs an answer."],
    ["id", "Catatan uang harian: berapa yang keluar, berapa yang aman dipakai, dan apa yang perlu dijawab."],
  ] as const)("declares lang=%s on <html> and writes the description in that language", async (locale, description) => {
    request.locale = locale;
    const { default: RootLayout, generateMetadata } = await import("./layout");
    const html = (await RootLayout({ children: null })) as ReactElement<{ lang: string }>;
    expect(isValidElement(html)).toBe(true);
    expect(html.type).toBe("html");
    expect(html.props.lang).toBe(locale);
    const metadata = await generateMetadata();
    expect(metadata.title).toBe("CashFlow");
    expect(metadata.description).toBe(description);
  });
});
