"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { useEffect, useState } from "react";
import { LANGUAGES, type Language } from "@/i18n/language";
import { api } from "@/lib/api";
import { writeLanguageCookie } from "@/lib/i18n/cookie";
import { queryKeys } from "@/lib/queryKeys";
import { THEME_CHOICES } from "@/lib/theme";
import { useAppCtx } from "@/components/layout/AppLayout";
import { useInterfaceLang } from "@/components/workspace/untranslated";
import { useWorkspace } from "@/components/workspace/context";
import { useShortcuts } from "@/components/workspace/shortcuts";
import { SettingsCard } from "@/components/settings/SettingsCard";
import styles from "@/components/accounts/money.module.css";

/** Each language named in its own words and marked with its own `lang`, without flags (add-language-system D11). */
const LANGUAGE_NAMES: Record<Language, string> = { en: "English", id: "Bahasa Indonesia" };

type ProfileQuery = { ok: boolean; user: Record<string, unknown> } | undefined;

/**
 * The Language control (add-language-system D11): choosing saves the account, writes this browser's cookie and
 * refreshes in place, so the shell switches without a reload or a change of address and focus stays on the
 * chosen option. A refused save keeps the previous language and says why in a polite live region.
 */
function LanguageChoice() {
  const t = useTranslations("settings.language");
  const locale = useLocale();
  const lang = useInterfaceLang();
  const router = useRouter();
  const qc = useQueryClient();
  const [pending, setPending] = useState<Language | null>(null);
  const [status, setStatus] = useState<{ saved: true } | { error: string } | null>(null);
  const save = useMutation({
    mutationFn: (language: Language) => api.patch("/auth/settings", { language }),
    onMutate: (language) => {
      setPending(language);
      setStatus(null);
    },
    onSuccess: (_response, language) => {
      // The profile follows first, so the account reconciliation does not switch the interface back (D2).
      qc.setQueryData<ProfileQuery>(queryKeys.auth.me, (current) => (current ? { ...current, user: { ...current.user, language } } : current));
      writeLanguageCookie(language);
      setStatus({ saved: true });
      router.refresh();
    },
    onError: (error: Error) => {
      setPending(null);
      setStatus({ error: error.message || t("notSaved") });
    },
  });
  // The chosen option stays selected until the refreshed page arrives in that language.
  useEffect(() => {
    if (pending === locale) setPending(null);
  }, [pending, locale]);
  const shown = pending ?? locale;
  return (
    <fieldset className={styles.themePick} lang={lang}>
      <legend>{t("label")}</legend>
      {LANGUAGES.map((language) => (
        <label key={language} lang={language}>
          <input
            type="radio"
            name="interface-language"
            value={language}
            checked={shown === language}
            onChange={() => {
              if (!save.isPending) save.mutate(language);
            }}
          />
          {LANGUAGE_NAMES[language]}
        </label>
      ))}
      <p role="status" aria-live="polite" className="basis-full text-xs text-[var(--muted)]">
        {status && ("saved" in status ? t("saved") : status.error)}
      </p>
    </fieldset>
  );
}

/** Tampilan (D9): the same theme, shortcut and hidden-balance state as the top bar's Tema menu and Sembunyikan saldo button. */
export function AppearanceSettings() {
  const theme = useTranslations("common.theme");
  const { themeChoice, setThemeChoice, openHelp } = useWorkspace();
  const { singleKeysEnabled, setSingleKeysEnabled } = useShortcuts();
  const { hideBalances, setHideBalances } = useAppCtx();
  const lang = useInterfaceLang();
  return (
    <SettingsCard section="tampilan">
      <LanguageChoice />
      <fieldset className={styles.themePick} lang={lang}>
        <legend>{theme("label")}</legend>
        {THEME_CHOICES.map((choice) => (
          <label key={choice.value}>
            <input type="radio" name="theme-choice" value={choice.value} checked={themeChoice === choice.value} onChange={() => setThemeChoice(choice.value)} />
            <span className={styles.swatch} data-theme-preview={choice.value} aria-hidden="true" />
            {theme(choice.value)}
          </label>
        ))}
      </fieldset>
      <SwitchRow
        id="setting-single-keys"
        label="Pintasan satu tombol"
        caption="J, K, E dan tombol lain di Tinjau dan Transaksi. ⌘K dan Esc tetap berfungsi."
        checked={singleKeysEnabled}
        onChange={setSingleKeysEnabled}
      />
      <SwitchRow
        id="setting-hide-balances"
        label="Sembunyikan saldo"
        caption="Angka uang diganti titik di semua halaman sampai diaktifkan lagi."
        checked={hideBalances}
        onChange={setHideBalances}
      />
      {/* Help moved here from the sidebar and Lainnya (refine-workspace-layout D3, OD84); ? opens it everywhere. */}
      <div className={styles.switchRow}>
        <div>
          <b id="setting-help-label">Bantuan &amp; pintasan</b>
          <span id="setting-help-caption">Cara memakai aplikasi dan daftar tombol pintas. Tekan ? di halaman mana pun.</span>
        </div>
        <button type="button" className="ws-btn" aria-describedby="setting-help-caption" onClick={openHelp}>
          Buka bantuan
        </button>
      </div>
    </SettingsCard>
  );
}

function SwitchRow({ id, label, caption, checked, onChange }: { id: string; label: string; caption: string; checked: boolean; onChange: (value: boolean) => void }) {
  return (
    <div className={styles.switchRow}>
      <div>
        <b id={`${id}-label`}>{label}</b>
        <span id={`${id}-caption`}>{caption}</span>
      </div>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-labelledby={`${id}-label`}
        aria-describedby={`${id}-caption`}
        className="ws-switch"
        onClick={() => onChange(!checked)}
      >
        <span className="ws-switch-track" aria-hidden="true" />
      </button>
    </div>
  );
}
