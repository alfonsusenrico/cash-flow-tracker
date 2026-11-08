import en from "@/messages/en.json";
import id from "@/messages/id.json";
import type { Language } from "./language";

type Catalogue = Record<string, unknown>;

/** Every message key path of a catalogue, such as "home.needsAction". */
export function keyPaths(catalogue: Catalogue, prefix = ""): string[] {
  return Object.entries(catalogue).flatMap(([key, value]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    return value && typeof value === "object" ? keyPaths(value as Catalogue, path) : [path];
  });
}

/** `primary` over `fallback`, key by key, so a key missing from `primary` keeps the fallback's message. */
function withFallback(primary: Catalogue, fallback: Catalogue): Catalogue {
  const merged: Catalogue = { ...fallback };
  for (const [key, value] of Object.entries(primary)) {
    const base = fallback[key];
    merged[key] =
      value && typeof value === "object" && base && typeof base === "object"
        ? withFallback(value as Catalogue, base as Catalogue)
        : value;
  }
  return merged;
}

let reported = false;

/**
 * The catalogue for a language (D3). Tests, the type check and development fail on a missing English key; a
 * production build reports the missing key paths once, without any text, and shows the Indonesian message.
 */
export function catalogueFor(language: Language): typeof en {
  // The Indonesian catalogue has the same key tree; its message literals simply differ from the English types.
  if (language === "id") return id as unknown as typeof en;
  const known = new Set(keyPaths(en));
  const missing = keyPaths(id).filter((path) => !known.has(path));
  if (missing.length && process.env.NODE_ENV !== "production") {
    throw new Error(`English messages missing: ${missing.join(", ")}`);
  }
  if (missing.length && !reported) {
    reported = true;
    console.error(`i18n: English messages missing for ${missing.join(", ")}`);
  }
  return (missing.length ? withFallback(en, id) : en) as unknown as typeof en;
}
