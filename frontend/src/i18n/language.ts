/**
 * The interface languages and how one is chosen (add-language-system D2). Signed in, the account's language wins
 * (`AppLayout` reconciles it); before sign-in the `cft_language` cookie, then `Accept-Language`, then English.
 */
export const LANGUAGES = ["en", "id"] as const;
export type Language = (typeof LANGUAGES)[number];
export const DEFAULT_LANGUAGE: Language = "en";

export const REGIONS = ["ID", "US"] as const;
export type Region = (typeof REGIONS)[number];
export const DEFAULT_REGION: Region = "ID";

export const LANGUAGE_COOKIE = "cft_language";
/** One year, in seconds. */
export const LANGUAGE_COOKIE_MAX_AGE = 365 * 24 * 60 * 60;

export function parseLanguage(value: unknown): Language | null {
  return value === "en" || value === "id" ? value : null;
}

export function parseRegion(value: unknown): Region | null {
  return value === "ID" || value === "US" ? value : null;
}

/** A primary subtag `id` (or the legacy `in`) is Indonesian, `en` is English; anything else is not supported. */
function supportedLanguage(tag: string): Language | null {
  const primary = tag.trim().toLowerCase().split("-")[0];
  if (primary === "id" || primary === "in") return "id";
  if (primary === "en") return "en";
  return null;
}

const QUALITY = /^q=(0(\.\d{0,3})?|1(\.0{0,3})?)$/i;

/**
 * The supported language the browser prefers most (RFC 9110 §12.5.4): the highest q-value wins, the earlier
 * entry on a tie, and q=0 means "not acceptable". Malformed entries are skipped; nothing usable gives null.
 */
export function matchAcceptLanguage(header: string | null | undefined): Language | null {
  if (!header) return null;
  let best: { language: Language; quality: number } | null = null;
  for (const entry of header.split(",")) {
    const [tag, ...params] = entry.split(";").map((part) => part.trim());
    if (!tag || !/^[a-z]{1,8}(-[a-z0-9]{1,8})*$/i.test(tag)) continue;
    let quality = 1;
    const qualityParam = params.find((param) => /^q=/i.test(param));
    if (qualityParam) {
      if (!QUALITY.test(qualityParam)) continue;
      quality = Number(qualityParam.slice(2));
    }
    const language = supportedLanguage(tag);
    if (!language || quality <= 0) continue;
    if (!best || quality > best.quality) best = { language, quality };
  }
  return best?.language ?? null;
}

/** Before sign-in: the remembered choice, then the browser's preference, then English. */
export function resolveLanguage(cookieValue: string | null | undefined, acceptLanguage: string | null | undefined): Language {
  return parseLanguage(cookieValue) ?? matchAcceptLanguage(acceptLanguage) ?? DEFAULT_LANGUAGE;
}
