import { NextIntlClientProvider } from "next-intl";
import type { ReactNode } from "react";
import en from "@/messages/en.json";
import id from "@/messages/id.json";
import { onIntlError, messageFallback } from "@/i18n/IntlProvider";
import type { Language } from "@/i18n/language";

export const CATALOGUES: Record<Language, typeof en> = { en, id: id as unknown as typeof en };

/** The real catalogue of one language for tests; a missing message or argument throws (add-language-system D16). */
export function IntlTestProvider({ language = "id", children }: { language?: Language; children: ReactNode }) {
  return (
    <NextIntlClientProvider locale={language} messages={CATALOGUES[language]} timeZone="Asia/Jakarta" onError={onIntlError} getMessageFallback={messageFallback}>
      {children}
    </NextIntlClientProvider>
  );
}
