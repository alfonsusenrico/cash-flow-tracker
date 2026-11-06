"use client";

import { useRouter } from "next/navigation";
import { useLocale } from "next-intl";
import { useEffect, useRef } from "react";
import { parseLanguage } from "@/i18n/language";
import { readLanguageCookie, writeLanguageCookie } from "./cookie";

/**
 * The account's language wins when signed in (add-language-system D2): a browser that remembers another language,
 * because the account changed it on another device or another account used this browser, takes the account's
 * and refreshes once, without a sign-out, a reload or a URL change.
 */
export function useAccountLanguage(accountLanguage: unknown) {
  const router = useRouter();
  const locale = useLocale();
  const language = parseLanguage(accountLanguage);
  const refreshedFor = useRef<string | null>(null);

  useEffect(() => {
    if (!language) return;
    if (readLanguageCookie() !== language) writeLanguageCookie(language);
    if (language === locale || refreshedFor.current === language) return;
    refreshedFor.current = language;
    router.refresh();
  }, [language, locale, router]);
}
