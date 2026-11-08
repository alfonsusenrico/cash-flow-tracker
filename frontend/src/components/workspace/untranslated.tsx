"use client";

import { createContext, useContext, type ReactNode } from "react";
import { useLocale, useTranslations } from "next-intl";

/**
 * Pages not translated yet (add-language-system D10): in an English interface their content stays Indonesian,
 * marked `lang="id"`, with one muted line saying so. Dialogs read the same context through their portals.
 */
const UntranslatedContext = createContext(false);

export function UntranslatedRegion({ active, children }: { active: boolean; children: ReactNode }) {
  return <UntranslatedContext.Provider value={active}>{children}</UntranslatedContext.Provider>;
}

/** Whether Indonesian content shown here needs its own `lang="id"`: an untranslated area in another language. */
export function useUntranslated(explicit = false): boolean {
  const inRegion = useContext(UntranslatedContext);
  return useLocale() !== "id" && (explicit || inRegion);
}

/**
 * The `lang` for interface text that a shared component renders inside an untranslated area, so an English button
 * on an Indonesian page is announced in English (WCAG 3.1.2). Elsewhere it inherits the document language.
 */
export function useInterfaceLang(): string | undefined {
  const inRegion = useContext(UntranslatedContext);
  const locale = useLocale();
  return inRegion && locale !== "id" ? locale : undefined;
}

/** "This page is in Indonesian for now.", in the interface language, under an untranslated title. */
export function UntranslatedNotice({ className = "t-meta untranslated-note" }: { className?: string }) {
  const t = useTranslations("notice");
  const locale = useLocale();
  return (
    <p className={className} lang={locale}>
      {t("untranslated")}
    </p>
  );
}
