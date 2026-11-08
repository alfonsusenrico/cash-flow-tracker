"use client";

import { usePathname } from "next/navigation";
import { useTranslations } from "next-intl";
import { useWorkspace } from "./context";
import { UntranslatedNotice, useUntranslated } from "./untranslated";
import { untranslatedPageTitle } from "./untranslatedPages";

/**
 * The page's title block (D4): Manrope 24/32 title, the one-line subtitle 4 px below, and the actions slot that
 * `PageActions` fills, on the title's bottom edge. The title comes from `usePageHeader` or the route. An
 * untranslated page keeps its Indonesian title marked `lang="id"` and gets the notice line (add-language-system D10).
 */
export function TitleBlock() {
  const pathname = usePathname();
  const t = useTranslations("nav");
  const { header, setActionsSlot } = useWorkspace();
  const untranslated = useUntranslated();
  const meta = header ?? untranslatedPageTitle(pathname) ?? { title: t("home"), subtitle: "" };
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
