import { beforeEach, describe, expect, it, vi } from "vitest";
import { THEME_SCRIPT, applyTheme, parseThemeChoice, readThemeChoice, storeThemeChoice, watchSystemTheme } from "./theme";

function stubMatchMedia(dark: boolean) {
  const listeners = new Set<(event: MediaQueryListEvent) => void>();
  const list = {
    matches: dark,
    addEventListener: (_: string, listener: (event: MediaQueryListEvent) => void) => listeners.add(listener),
    removeEventListener: (_: string, listener: (event: MediaQueryListEvent) => void) => listeners.delete(listener),
  };
  Object.defineProperty(window, "matchMedia", { configurable: true, value: vi.fn().mockReturnValue(list) });
  return { change: (next: boolean) => listeners.forEach((listener) => listener({ matches: next } as MediaQueryListEvent)), listeners };
}

// jsdom here has no usable localStorage, so each test gets an in-memory one.
function memoryStorage() {
  const data = new Map<string, string>();
  return {
    getItem: vi.fn((key: string) => data.get(key) ?? null),
    setItem: vi.fn((key: string, value: string) => void data.set(key, value)),
    removeItem: vi.fn((key: string) => void data.delete(key)),
    clear: () => data.clear(),
  };
}
let storage = memoryStorage();
beforeEach(() => {
  storage = memoryStorage();
  Object.defineProperty(window, "localStorage", { configurable: true, value: storage });
});

function runScript() {
  new Function(THEME_SCRIPT)();
}

describe("pre-paint theme script", () => {
  beforeEach(() => {
    document.documentElement.className = "";
    document.documentElement.removeAttribute("data-theme-choice");
  });

  it("applies a stored dark choice", () => {
    stubMatchMedia(false);
    localStorage.setItem("theme", "dark");
    runScript();
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.dataset.themeChoice).toBe("dark");
  });

  it("keeps a stored light choice on a dark operating system", () => {
    stubMatchMedia(true);
    localStorage.setItem("theme", "light");
    runScript();
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  it("follows the operating system when nothing is stored", () => {
    stubMatchMedia(true);
    runScript();
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.dataset.themeChoice).toBe("system");
  });

  it("removes the retired preset keys", () => {
    stubMatchMedia(false);
    localStorage.setItem("theme_preset", "nordic");
    localStorage.setItem("theme_palette", "x");
    runScript();
    expect(localStorage.getItem("theme_preset")).toBeNull();
    expect(localStorage.getItem("theme_palette")).toBeNull();
  });

  it("follows the operating system without error when storage throws", () => {
    stubMatchMedia(true);
    storage.getItem.mockImplementation(() => {
      throw new Error("blocked");
    });
    expect(() => runScript()).not.toThrow();
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(readThemeChoice()).toBe("system");
  });
});

describe("theme helpers", () => {

  it("treats unknown values as the system choice", () => {
    expect(parseThemeChoice("dark")).toBe("dark");
    expect(parseThemeChoice("swiss")).toBe("system");
    expect(parseThemeChoice(null)).toBe("system");
  });

  it("stores a choice and survives a storage that throws", () => {
    storeThemeChoice("light");
    expect(localStorage.getItem("theme")).toBe("light");
    storage.setItem.mockImplementation(() => {
      throw new Error("blocked");
    });
    expect(() => storeThemeChoice("dark")).not.toThrow();
  });

  it("follows an operating-system change while on system", () => {
    const media = stubMatchMedia(false);
    applyTheme("system");
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    const stop = watchSystemTheme((prefersDark) => applyTheme("system", prefersDark));
    media.change(true);
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    stop();
    expect(media.listeners.size).toBe(0);
  });
});

describe("theme names in both languages (add-language-system D17)", () => {
  it("names the three choices from the catalogue", async () => {
    const { testWords } = await import("@/lib/i18n/testing");
    const { THEME_CHOICES } = await import("./theme");
    expect(THEME_CHOICES.map((choice) => testWords("en").t(`common.theme.${choice.value}`))).toEqual(["Light", "Dark", "Match system"]);
    expect(THEME_CHOICES.map((choice) => testWords("id").t(`common.theme.${choice.value}`))).toEqual(["Terang", "Gelap", "Ikuti sistem"]);
  });
});
