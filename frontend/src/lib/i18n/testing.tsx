import { render, type RenderOptions } from "@testing-library/react";
import type { ComponentType, ReactElement, ReactNode } from "react";
import { createTranslator } from "next-intl";
import type { Language, Region } from "@/i18n/language";
import { createFormat } from "@/lib/format";
import { CATALOGUES, IntlTestProvider } from "./IntlTestProvider";
import type { Translate, Words } from "./types";

export { CATALOGUES, IntlTestProvider } from "./IntlTestProvider";

/**
 * Renders inside the catalogue of `language` (add-language-system D16). It defaults to Indonesian until phase 12, so
 * the existing Indonesian assertions keep their meaning; English cases pass `{ language: "en" }`.
 */
export function renderWithIntl(ui: ReactElement, { language = "id", wrapper: Inner, ...options }: RenderOptions & { language?: Language } = {}) {
  const Wrapper: ComponentType<{ children: ReactNode }> = ({ children }) => (
    <IntlTestProvider language={language}>{Inner ? <Inner>{children}</Inner> : children}</IntlTestProvider>
  );
  return render(ui, { ...options, wrapper: Wrapper });
}

/** The catalogue's translator and the region's formats for testing text-building functions. */
export function testWords(language: Language = "id", region: Region = "ID"): Words {
  const t = createTranslator({ locale: language, messages: CATALOGUES[language] }) as unknown as Translate;
  return { t, format: createFormat({ language, region }) };
}
