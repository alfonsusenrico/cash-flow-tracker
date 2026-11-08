import { createTranslator, type AbstractIntlMessages } from "next-intl";
import { DEFAULT_REGION, type Language, type Region } from "@/i18n/language";

/**
 * The interface language, catalogue and region outside React (add-language-system D8): `lib/api.ts` renders an
 * error code in the language the provider last set. Until a provider runs there is no catalogue, and an error
 * keeps the server's own message.
 */
let activeLanguage: Language | null = null;
let activeMessages: AbstractIntlMessages | null = null;
let activeRegion: Region = DEFAULT_REGION;

export function configureApiMessages(language: Language, messages: AbstractIntlMessages) {
  activeLanguage = language;
  activeMessages = messages;
}

export function configureApiRegion(region: Region) {
  activeRegion = region;
}

/** The signed-in account's region outside React, for amount inputs and error messages; Indonesia before sign-in. */
export function activeRegionSetting(): Region {
  return activeRegion;
}

function hasErrorMessage(code: string): boolean {
  const errors = activeMessages?.errors;
  return Boolean(errors && typeof errors === "object" && typeof (errors as Record<string, unknown>)[code] === "string");
}

/** The catalogue's message for an error code, or null when no catalogue is loaded or the code is unknown. */
export function errorMessage(code: string, params: Record<string, string | number> = {}): string | null {
  if (!activeLanguage || !activeMessages || !hasErrorMessage(code)) return null;
  const translate = createTranslator({ locale: activeLanguage, messages: activeMessages, namespace: "errors" });
  return (translate as unknown as (key: string, values: Record<string, string | number>) => string)(code, params);
}
