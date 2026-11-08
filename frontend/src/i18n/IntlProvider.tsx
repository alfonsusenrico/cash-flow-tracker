"use client";

import { IntlErrorCode, NextIntlClientProvider, type AbstractIntlMessages, type IntlError } from "next-intl";
import type { ReactNode } from "react";
import { configureApiMessages } from "@/lib/i18n/runtime";
import type { Language } from "./language";

/** A missing message or argument is a defect: tests and development throw, production reports the key only. */
export function onIntlError(error: IntlError) {
  if (process.env.NODE_ENV !== "production") throw error;
  if (error.code === IntlErrorCode.MISSING_MESSAGE) console.error(`i18n: ${error.code}`);
}

export function messageFallback({ namespace, key }: { namespace?: string; key: string }) {
  return namespace ? `${namespace}.${key}` : key;
}

/**
 * The client side of the catalogue (D1): the server layout passes the resolved language and its messages, and
 * `router.refresh()` after a language change passes the new ones without a reload or a URL change.
 */
export function IntlProvider({ locale, messages, children }: { locale: Language; messages: AbstractIntlMessages; children: ReactNode }) {
  configureApiMessages(locale, messages);
  return (
    <NextIntlClientProvider locale={locale} messages={messages} timeZone="Asia/Jakarta" onError={onIntlError} getMessageFallback={messageFallback}>
      {children}
    </NextIntlClientProvider>
  );
}
