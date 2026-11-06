/**
 * Light, dark and "Ikuti sistem" themes.
 *
 * The choice lives in browser storage under `theme` ("light", "dark" or "system"); an absent or unknown
 * value means "system", so values stored before the redesign keep working. The `.dark` class on <html>
 * paints the dark tokens and keeps the legacy `dark:` utilities working. Every storage access is guarded:
 * a browser that blocks storage simply follows the operating system.
 */
export type ThemeChoice = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

export const THEME_STORAGE_KEY = "theme";
const STALE_KEYS = ["theme_preset", "theme_palette"];
const DARK_QUERY = "(prefers-color-scheme: dark)";

/** The three choices in menu order; their names are `common.theme.<value>` (add-language-system D17). */
export const THEME_CHOICES: { value: ThemeChoice }[] = [{ value: "light" }, { value: "dark" }, { value: "system" }];

export function parseThemeChoice(value: unknown): ThemeChoice {
  return value === "light" || value === "dark" || value === "system" ? value : "system";
}

export function readThemeChoice(): ThemeChoice {
  try {
    return parseThemeChoice(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch {
    return "system";
  }
}

export function systemPrefersDark(): boolean {
  try {
    return window.matchMedia(DARK_QUERY).matches;
  } catch {
    return false;
  }
}

export function resolveTheme(choice: ThemeChoice, prefersDark = systemPrefersDark()): ResolvedTheme {
  if (choice === "system") return prefersDark ? "dark" : "light";
  return choice;
}

export function applyTheme(choice: ThemeChoice, prefersDark = systemPrefersDark()): ResolvedTheme {
  const resolved = resolveTheme(choice, prefersDark);
  const root = document.documentElement;
  root.classList.toggle("dark", resolved === "dark");
  root.dataset.themeChoice = choice;
  return resolved;
}

export function storeThemeChoice(choice: ThemeChoice): void {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, choice);
  } catch {
    // The choice still applies to this page; it just is not remembered.
  }
}

/** Follows operating-system changes while "Ikuti sistem" is chosen; returns the unsubscribe function. */
export function watchSystemTheme(onChange: (prefersDark: boolean) => void): () => void {
  let query: MediaQueryList;
  try {
    query = window.matchMedia(DARK_QUERY);
  } catch {
    return () => {};
  }
  if (typeof query?.addEventListener !== "function") return () => {};
  const listener = (event: MediaQueryListEvent) => onChange(event.matches);
  query.addEventListener("change", listener);
  return () => query.removeEventListener("change", listener);
}

/**
 * Inline <head> script: resolves the stored choice before the first paint so no page flashes the other
 * theme, and removes the retired preset keys. Self-contained on purpose — it runs before any bundle.
 */
export const THEME_SCRIPT = `(function () {
  var choice = "system";
  try {
    var stored = window.localStorage.getItem(${JSON.stringify(THEME_STORAGE_KEY)});
    if (stored === "light" || stored === "dark" || stored === "system") choice = stored;
    ${JSON.stringify(STALE_KEYS)}.forEach(function (key) { window.localStorage.removeItem(key); });
  } catch (error) {}
  var dark = choice === "dark";
  if (choice === "system") {
    try { dark = window.matchMedia(${JSON.stringify(DARK_QUERY)}).matches; } catch (error) {}
  }
  var root = document.documentElement;
  if (dark) root.classList.add("dark"); else root.classList.remove("dark");
  root.setAttribute("data-theme-choice", choice);
  root.removeAttribute("data-preset");
  root.removeAttribute("data-palette");
})();`;
