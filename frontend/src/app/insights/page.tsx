"use client";

import { Suspense, useState } from "react";
import dynamic from "next/dynamic";
import { useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { AnalysisTabs } from "@/components/analysis/AnalysisTabs";
import { useAnalysisPeriods } from "@/components/analysis/period";
import { RingkasanTab } from "@/components/analysis/RingkasanTab";
import type { AnalysisTab, FlowView, PeriodBasis } from "@/components/analysis/types";
import { DATE_PATTERN, analysisHref, parseTab } from "@/components/analysis/url";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { PageActions } from "@/components/workspace/context";
import { Segmented } from "@/components/workspace/primitives";
import { replaceSearch } from "@/lib/urlState";

// The calendar and the map load with their tab, so Ringkasan's first load does not carry them.
const loading = () => <div className="skel an-skel-tab" role="status" aria-busy="true" />;
const KalenderTab = dynamic(() => import("@/components/analysis/calendar/KalenderTab").then((module) => module.KalenderTab), { ssr: false, loading });
const PetaAliranTab = dynamic(() => import("@/components/analysis/flow/PetaAliranTab").then((module) => module.PetaAliranTab), { ssr: false, loading });

export default function AnalyticsPage() {
  return (
    <Suspense fallback={null}>
      <Analysis />
    </Suspense>
  );
}

/**
 * Analisis (apply-markas-analisis): the basis segment and the period stepper in the title block, underline tabs with
 * the monthly report link, and the selected tab's panel. The shared period applies to all three tabs; every figure is
 * the server's.
 */
function Analysis() {
  const t = useTranslations();
  const search = useSearchParams();
  const { timeframe, setTimeframe, cycleOffset, setCycleOffset } = useAppCtx();
  const tab = parseTab(search.get("tab"));
  const dayParam = search.get("day");
  const day = dayParam && DATE_PATTERN.test(dayParam) ? dayParam : null;
  const viewParam = search.get("view");
  const view: FlowView | null = viewParam === "tabel" ? "tabel" : viewParam === "peta" ? "peta" : null;
  // Rolling windows are not an Analisis period; the shared value is left alone and the cycle is shown.
  const rolling = timeframe !== "cycle" && timeframe !== "month";
  const basis: PeriodBasis = rolling ? "cycle" : (timeframe as PeriodBasis);
  const offset = rolling ? 0 : cycleOffset;
  const { viewed } = useAnalysisPeriods(basis, offset);
  // Before the first confirmed salary no cycle is listed; the tab's own window label stands in.
  const [windowLabel, setWindowLabel] = useState<string | null>(null);

  const go = (next: AnalysisTab, extra: { day?: string | null; view?: FlowView | null } = {}) =>
    replaceSearch(analysisHref(next, extra, new URLSearchParams(search.toString())));

  return (
    <div className="an-page">
      <PageActions>
        <Segmented
          label={t("analysis.page.basis")}
          options={[{ value: "cycle", label: t("terms.payCycle") }, { value: "month", label: t("terms.calendarMonth") }]}
          value={basis}
          onChange={(value) => setTimeframe(value)}
        />
        <PeriodStepper
          unit={basis === "month" ? "Bulan" : "Siklus"}
          label={viewed?.label ?? windowLabel}
          period={viewed ? { start: viewed.start_date, end: viewed.complete ? viewed.end_date : null } : null}
          offset={offset}
          onChange={setCycleOffset}
        />
      </PageActions>
      {rolling && <p className="t-meta an-rolling">{t("analysis.page.rollingNote")}</p>}
      <AnalysisTabs value={tab} onSelect={(next) => go(next)} />
      <div id="analysis-panel" role="tabpanel" aria-labelledby={`analysis-tab-${tab}`}>
        {tab === "ringkasan" && <RingkasanTab basis={basis} offset={offset} onWindowLabel={setWindowLabel} />}
        {tab === "kalender" && (
          <KalenderTab basis={basis} offset={offset} day={day} onSelectDay={(next) => go("kalender", { day: next })} onWindowLabel={setWindowLabel} />
        )}
        {tab === "aliran" && (
          <PetaAliranTab basis={basis} offset={offset} view={view} onView={(next) => go("aliran", { view: next })} onWindowLabel={setWindowLabel} />
        )}
      </div>
    </div>
  );
}
