import { LANGUAGE_COOKIE, LANGUAGE_COOKIE_MAX_AGE, parseLanguage, type Language } from "@/i18n/language";

/**
 * The browser's remembered language (add-language-system D2): readable by the client that writes it, no personal
 * data, kept after sign-out so the sign-in page stays in the language last used on this device.
 */
export function languageCookie(language: Language, secure: boolean): string {
  return `${LANGUAGE_COOKIE}=${language}; Path=/; Max-Age=${LANGUAGE_COOKIE_MAX_AGE}; SameSite=Lax${secure ? "; Secure" : ""}`;
}

export function writeLanguageCookie(language: Language) {
  document.cookie = languageCookie(language, window.location.protocol === "https:");
}

/** The remembered language, or null when absent or unknown. */
export function readLanguageCookie(): Language | null {
  const entry = document.cookie.split(";").map((part) => part.trim()).find((part) => part.startsWith(`${LANGUAGE_COOKIE}=`));
  return parseLanguage(entry?.slice(LANGUAGE_COOKIE.length + 1));
}
