"use client";

import { useRouter } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { useEffect, useId, useState } from "react";
import { LANGUAGES, parseLanguage, type Language } from "@/i18n/language";
import { api } from "@/lib/api";
import { writeLanguageCookie } from "@/lib/i18n/cookie";

const REGISTRATION_PASSWORD_MIN_LENGTH = 8;

/** Each language named in its own words and marked with its own `lang` (add-language-system D11, D12). */
const LANGUAGE_NAMES: Record<Language, string> = { en: "English", id: "Bahasa Indonesia" };

/**
 * The language links at the card's foot: before sign-in there is no account to save to, so a choice writes the
 * browser's cookie and refreshes the page in place, without a reload or a change of address.
 */
function LanguageLinks() {
  const t = useTranslations("auth");
  const locale = useLocale();
  const router = useRouter();
  return (
    <nav aria-label={t("languageLabel")} className="mt-4 flex justify-center gap-4 text-xs">
      {LANGUAGES.map((language) => (
        <button
          key={language}
          type="button"
          lang={language}
          aria-current={language === locale ? "true" : undefined}
          onClick={() => {
            writeLanguageCookie(language);
            router.refresh();
          }}
          className={
            language === locale
              ? "min-h-11 font-semibold text-[var(--text)]"
              : "min-h-11 font-medium text-[var(--muted)] underline underline-offset-4 hover:text-[var(--text)]"
          }
        >
          {LANGUAGE_NAMES[language]}
        </button>
      ))}
    </nav>
  );
}

export default function LoginPage() {
  const t = useTranslations("auth");
  const errors = useTranslations("errors");
  const settings = useTranslations("settings.region");
  const locale = useLocale();
  const regionId = useId();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  function getSafeRedirectTarget() {
    const params = new URLSearchParams(window.location.search);
    const next = params.get("next");
    if (next && next.startsWith("/") && !next.startsWith("//") && next !== "/dashboard") {
      return next;
    }
    return "/";
  }

  useEffect(() => {
    async function redirectIfAuthenticated() {
      try {
        await api.get("/auth/me");
        window.location.replace(getSafeRedirectTarget());
      } catch {
        // Staying on the login page is correct when there is no valid session.
      }
    }

    redirectIfAuthenticated();
    window.addEventListener("pageshow", redirectIfAuthenticated);
    return () => window.removeEventListener("pageshow", redirectIfAuthenticated);
  }, []);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    setLoading(true);
    const fd = new FormData(e.currentTarget);

    try {
      if (mode === "login") {
        const response = await api.post<{ user?: { language?: string } }>("/auth/login", {
          username: fd.get("username"),
          password: fd.get("password"),
        });
        // The account's language follows it to this browser before the first signed-in page (D2).
        const language = parseLanguage(response?.user?.language);
        if (language) writeLanguageCookie(language);
        window.location.replace(getSafeRedirectTarget());
      } else {
        const password = String(fd.get("password") ?? "");
        if (password.length < REGISTRATION_PASSWORD_MIN_LENGTH) {
          throw new Error(errors("password_too_short", { min: String(REGISTRATION_PASSWORD_MIN_LENGTH) }));
        }
        await api.post("/auth/register", {
          username: fd.get("username"),
          password,
          name: fd.get("full_name") || undefined,
          invite_code: fd.get("invite_code"),
          language: locale,
          region: fd.get("region") || "ID",
        });
        writeLanguageCookie(locale);
        window.location.replace(getSafeRedirectTarget());
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : t("genericError"));
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen flex items-center justify-center bg-[var(--window)] text-[var(--text)] px-4">
      <div className="w-full max-w-sm rounded-xl border border-[var(--line)] bg-[var(--surface)] p-6 sm:p-8">
        <h1 className="text-2xl font-semibold mb-1 text-center tracking-tight">CashFlow</h1>
        <p className="text-center text-xs text-[var(--muted)] mb-6">
          {mode === "login" ? t("signInHeading") : t("signUpHeading")}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          {mode === "register" && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="full_name">
                {t("displayName")}
              </label>
              <input
                id="full_name"
                name="full_name"
                autoComplete="name"
                className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
              />
            </div>
          )}
          <div>
            <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="username">
              {t("username")}
            </label>
            <input
              id="username"
              name="username"
              autoComplete="username"
              required
              minLength={mode === "register" ? 6 : undefined}
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="password">
              {t("password")}
            </label>
            <input
              id="password"
              name="password"
              type="password"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
              required
              minLength={mode === "register" ? REGISTRATION_PASSWORD_MIN_LENGTH : undefined}
              aria-describedby={mode === "register" ? "password-help" : undefined}
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
            />
            {mode === "register" && (
              <p id="password-help" className="mt-1 text-xs text-[var(--muted)]">
                {t("passwordHelp", { min: String(REGISTRATION_PASSWORD_MIN_LENGTH) })}
              </p>
            )}
          </div>
          {mode === "register" && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="invite_code">
                {t("inviteCode")}
              </label>
              <input
                id="invite_code"
                name="invite_code"
                autoComplete="off"
                required
                className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
              />
            </div>
          )}
          {mode === "register" && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor={regionId}>
                {settings("label")}
              </label>
              <select
                id={regionId}
                name="region"
                defaultValue="ID"
                aria-describedby={`${regionId}-hint`}
                className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
              >
                <option value="ID">{settings("indonesia")}</option>
                <option value="US">{settings("unitedStates")}</option>
              </select>
              <p id={`${regionId}-hint`} className="mt-1 text-xs text-[var(--muted)]">
                {settings("hint")}
              </p>
            </div>
          )}

          {error && (
            <p role="alert" className="text-xs font-medium text-out-fg">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="ws-btn ws-btn-accent w-full"
          >
            {loading ? t("working") : mode === "login" ? t("signIn") : t("createAccount")}
          </button>
        </form>

        <p className="text-center text-xs text-[var(--muted)] mt-5">
          {mode === "login" ? (
            <>
              {t("noAccount")}{" "}
              <button
                type="button"
                onClick={() => { setMode("register"); setError(""); }}
                className="min-h-11 font-semibold text-[var(--text)] underline underline-offset-4"
              >
                {t("createOne")}
              </button>
            </>
          ) : (
            <>
              {t("haveAccount")}{" "}
              <button
                type="button"
                onClick={() => { setMode("login"); setError(""); }}
                className="min-h-11 font-semibold text-[var(--text)] underline underline-offset-4"
              >
                {t("signInHere")}
              </button>
            </>
          )}
        </p>
        <LanguageLinks />
      </div>
    </main>
  );
}
