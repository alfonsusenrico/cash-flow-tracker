"use client";

import { useRef, type KeyboardEvent } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import type { AnalysisTab } from "./types";
import { ANALYSIS_TABS } from "./url";

const TAB_KEYS = { ringkasan: "overview", kalender: "calendar", aliran: "flow" } as const satisfies Record<AnalysisTab, string>;

/**
 * Underline tabs (apply-markas-analisis D3): an ARIA tab list with roving focus, ←/→, Home and End, and the
 * monthly report link at their right from 768 px. Selecting a tab writes `?tab=` through the caller.
 */
export function AnalysisTabs({ value, onSelect }: { value: AnalysisTab; onSelect: (tab: AnalysisTab) => void }) {
  const t = useTranslations("analysis.tabs");
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const onKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const index = ANALYSIS_TABS.indexOf(value);
    let next = index;
    if (event.key === "ArrowRight") next = (index + 1) % ANALYSIS_TABS.length;
    else if (event.key === "ArrowLeft") next = (index - 1 + ANALYSIS_TABS.length) % ANALYSIS_TABS.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = ANALYSIS_TABS.length - 1;
    else return;
    event.preventDefault();
    onSelect(ANALYSIS_TABS[next]);
    refs.current[next]?.focus();
  };
  return (
    <div className="tabs an-tabs">
      <div role="tablist" aria-label={t("label")} className="tab-list" onKeyDown={onKey}>
        {ANALYSIS_TABS.map((tab, index) => (
          <button
            key={tab}
            ref={(element) => {
              refs.current[index] = element;
            }}
            type="button"
            role="tab"
            className="tab"
            id={`analysis-tab-${tab}`}
            aria-selected={tab === value}
            aria-controls="analysis-panel"
            tabIndex={tab === value ? 0 : -1}
            onClick={() => onSelect(tab)}
          >
            {t(TAB_KEYS[tab])}
          </button>
        ))}
      </div>
      <Link href="/laporan" className="btn-link ph-hide">
        {t("readReport")}
      </Link>
    </div>
  );
}
