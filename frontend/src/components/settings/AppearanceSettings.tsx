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
import { Switch } from "@/components/ui/Switch";
import { SectionHeader } from "@/components/workspace/lists";
import { RadioSegments } from "@/components/workspace/primitives";
import { useWorkspace } from "@/components/workspace/context";
import { useShortcuts } from "@/components/workspace/shortcuts";
import { SettingRow } from "@/components/settings/SettingRow";
import { SettingsSection } from "@/components/settings/SettingsSection";

/** Each language named in its own words and marked with its own `lang`, without flags (add-language-system D11). */
const LANGUAGE_NAMES: Record<Language, string> = { en: "English", id: "Bahasa Indonesia" };

type ProfileQuery = { ok: boolean; user: Record<string, unknown> } | undefined;

/**
 * The Language control (add-language-system D11), a segment since apply-markas-pengaturan D11: choosing saves the
 * account, writes this browser's cookie and refreshes in place, so the shell switches without a reload or a change of
 * address and focus stays on the chosen option. A refused save keeps the previous language and says why in a polite
 * live region.
 */
function LanguageChoice() {
  const t = useTranslations("settings.language");
  const locale = useLocale();
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
    <section aria-labelledby="setting-language">
      <SectionHeader id="setting-language" label={t("label")} />
      <RadioSegments
        labelledBy="setting-language"
        options={LANGUAGES.map((language) => ({ value: language, label: LANGUAGE_NAMES[language], lang: language }))}
        value={shown as Language}
        onChange={(language) => {
          if (!save.isPending && language !== shown) save.mutate(language);
        }}
      />
      <p role="status" aria-live="polite" className="t-meta set-note">
        {status && ("saved" in status ? t("saved") : status.error)}
      </p>
    </section>
  );
}

/**
 * Tampilan (apply-markas-pengaturan D11): Bahasa, the theme segment, then the shortcut and hidden-balance switches and
 * the help row. Every control uses the same state as its top-bar counterpart and applies at once.
 */
export function AppearanceSettings() {
  const t = useTranslations("settings.appearance");
  const theme = useTranslations("common.theme");
  const { themeChoice, setThemeChoice, openHelp } = useWorkspace();
  const { singleKeysEnabled, setSingleKeysEnabled } = useShortcuts();
  const { hideBalances, setHideBalances } = useAppCtx();
  return (
    <SettingsSection section="tampilan">
      <LanguageChoice />
      <section aria-labelledby="setting-theme">
        <SectionHeader id="setting-theme" label={theme("label")} />
        <RadioSegments
          labelledBy="setting-theme"
          options={THEME_CHOICES.map((choice) => ({ value: choice.value, label: theme(choice.value) }))}
          value={themeChoice}
          onChange={setThemeChoice}
        />
      </section>
      <div>
        <SettingRow
          titleId="setting-single-keys"
          metaId="setting-single-keys-meta"
          title={t("singleKeys")}
          meta={t("singleKeysMeta")}
          end={<Switch checked={singleKeysEnabled} onChange={setSingleKeysEnabled} labelledBy="setting-single-keys" describedBy="setting-single-keys-meta" />}
        />
        <SettingRow
          titleId="setting-hide-balances"
          metaId="setting-hide-balances-meta"
          title={t("hide")}
          meta={t("hideMeta")}
          end={<Switch checked={hideBalances} onChange={setHideBalances} labelledBy="setting-hide-balances" describedBy="setting-hide-balances-meta" />}
        />
        {/* Help moved here from the sidebar and Lainnya (refine-workspace-layout D3, OD84); ? opens it everywhere. */}
        <SettingRow
          titleId="setting-help"
          metaId="setting-help-meta"
          title={t("help")}
          meta={t("helpMeta")}
          end={
            <button type="button" className="ws-btn" aria-describedby="setting-help-meta" onClick={openHelp}>
              {t("openHelp")}
            </button>
          }
        />
      </div>
    </SettingsSection>
  );
}
