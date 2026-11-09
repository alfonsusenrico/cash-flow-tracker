"use client";

import type { ReactNode } from "react";
import { useTranslations } from "next-intl";
import { sectionHeadingId, type SettingsSectionSlug } from "@/components/settings/sections";

/**
 * One Pengaturan section (apply-markas-pengaturan D4): the Manrope 20 title, which takes focus after a rail choice,
 * one description line, and its parts 48 px apart (32 on a phone), without a card.
 */
export function SettingsSection({ section, children }: { section: SettingsSectionSlug; children: ReactNode }) {
  const t = useTranslations("settings.sections");
  const headingId = sectionHeadingId(section);
  return (
    <section aria-labelledby={headingId}>
      <div className="set-head">
        <h2 id={headingId} tabIndex={-1} data-settings-heading className="t-dtitle">
          {t(`${section}.title`)}
        </h2>
        <p className="t-meta">{t(`${section}.description`)}</p>
      </div>
      <div className="stack">{children}</div>
    </section>
  );
}

/** A section's failure or refusal line, announced at once. */
export function SettingsAlert({ children, id }: { children: ReactNode; id?: string }) {
  return (
    <p role="alert" id={id} className="set-err">
      {children}
    </p>
  );
}

/** Save feedback beside a section's save button; a fresh save wins until the owner edits again. */
export function SettingsSaveStatus({ dirty, saved }: { dirty: boolean; saved: boolean }) {
  const t = useTranslations("settings.common");
  return (
    <p role="status" className="t-meta">
      {saved ? t("saved") : dirty ? t("unsaved") : ""}
    </p>
  );
}
