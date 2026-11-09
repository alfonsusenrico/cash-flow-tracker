"use client";

import { usePathname } from "next/navigation";
import { useTranslations } from "next-intl";
import type { Translate } from "@/lib/i18n/types";
import { useWorkspace, type PageHeader } from "./context";
import { isActivePath } from "./navigation";
import { UntranslatedNotice, useUntranslated } from "./untranslated";
import { untranslatedPageTitle } from "./untranslatedPages";

/** The fixed title and subtitle of a page its phase translated, from the catalogue (apply-markas-tinjau D2). */
function translatedPageTitle(pathname: string, t: Translate): PageHeader | null {
  if (isActivePath(pathname, "/review")) return { title: t("nav.review"), subtitle: t("review.page.subtitle") };
  if (isActivePath(pathname, "/ledger")) return { title: t("nav.transactions"), subtitle: t("transactions.page.subtitle") };
  if (isActivePath(pathname, "/insights")) return { title: t("nav.analysis"), subtitle: t("analysis.page.subtitle") };
  if (isActivePath(pathname, "/accounts")) return { title: t("nav.accounts"), subtitle: t("accounts.page.subtitle") };
  if (isActivePath(pathname, "/laporan")) return { title: t("nav.reports"), subtitle: t("report.page.subtitle") };
  if (isActivePath(pathname, "/goals")) return { title: t("nav.goals"), subtitle: t("goals.page.subtitle") };
  // Pengaturan has no subtitle (prototype `titleBlock("Pengaturan", "")`, apply-markas-pengaturan D1).
  if (isActivePath(pathname, "/settings")) return { title: t("nav.settings"), subtitle: "" };
  return null;
}

/**
 * The page's title block (D4): Manrope 24/32 title, the one-line subtitle 4 px below, and the actions slot that
 * `PageActions` fills, on the title's bottom edge. The title comes from `usePageHeader` or the route. An
 * untranslated page keeps its Indonesian title marked `lang="id"` and gets the notice line (add-language-system D10).
 */
export function TitleBlock() {
  const pathname = usePathname();
  const t = useTranslations();
  const { header, setActionsSlot } = useWorkspace();
  const untranslated = useUntranslated();
  const meta = header ?? translatedPageTitle(pathname, t) ?? untranslatedPageTitle(pathname) ?? { title: t("nav.home"), subtitle: "" };
  return (
    <header className="title-block">
      <div className="tb-text">
        <h1 className="t-title" lang={untranslated ? "id" : undefined}>
          {meta.title}
        </h1>
        {meta.subtitle && (
          <p className="sub t-body" lang={untranslated ? "id" : undefined}>
            {meta.subtitle}
          </p>
        )}
        {untranslated && <UntranslatedNotice />}
      </div>
      <div className="tb-actions" ref={setActionsSlot} />
    </header>
  );
}
