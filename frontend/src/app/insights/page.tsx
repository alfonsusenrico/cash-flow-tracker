"use client";

import { Suspense, useCallback, useRef, useState, type KeyboardEvent } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useAppCtx } from "@/components/layout/AppLayout";
import { RingkasanTab } from "@/components/analysis/RingkasanTab";
import styles from "@/components/analysis/analysis.module.css";
import type { AnalysisTab, PeriodBasis } from "@/components/analysis/types";
import { ANALYSIS_TABS as TABS, DATE_PATTERN, analysisHref, parseTab } from "@/components/analysis/url";
import type { FlowView } from "@/components/analysis/flow/PetaAliranTab";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { TopBarControls } from "@/components/workspace/context";
import { Segmented, Skeleton } from "@/components/workspace/primitives";
import { replaceSearch } from "@/lib/urlState";

// The calendar and the map load with their tab, so Ringkasan's first load does not carry them (D1).
const loading = () => <Skeleton style={{ height: 420 }} />;
const KalenderTab = dynamic(() => import("@/components/analysis/calendar/KalenderTab").then((module) => module.KalenderTab), { ssr: false, loading });
const PetaAliranTab = dynamic(() => import("@/components/analysis/flow/PetaAliranTab").then((module) => module.PetaAliranTab), { ssr: false, loading });

export default function AnalyticsPage() {
  return (
    <Suspense fallback={null}>
      <Analysis />
    </Suspense>
  );
}

function Analysis() {
  const search = useSearchParams();
  const { timeframe, setTimeframe, cycleOffset, setCycleOffset, bal } = useAppCtx();
  const tab = parseTab(search.get("tab"));
  const dayParam = search.get("day");
  const day = dayParam && DATE_PATTERN.test(dayParam) ? dayParam : null;
  const view: FlowView = search.get("view") === "tabel" ? "tabel" : "peta";
  // Rolling windows are not an Analisis period; the shared value is left alone and the cycle is shown.
  const rolling = timeframe !== "cycle" && timeframe !== "month";
  const basis: PeriodBasis = rolling ? "cycle" : (timeframe as PeriodBasis);
  const offset = rolling ? 0 : cycleOffset;
  const [label, setLabel] = useState<string | null>(null);
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const onLabel = useCallback((value: string) => setLabel(value), []);
  const money = useCallback((amount: number) => bal(amount), [bal]);

  const go = (next: AnalysisTab, extra: { day?: string | null; view?: FlowView | null } = {}) => replaceSearch(analysisHref(next, extra));
  const onTabKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const index = TABS.findIndex((entry) => entry.value === tab);
    let next = index;
    if (event.key === "ArrowRight") next = (index + 1) % TABS.length;
    else if (event.key === "ArrowLeft") next = (index - 1 + TABS.length) % TABS.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = TABS.length - 1;
    else return;
    event.preventDefault();
    go(TABS[next].value);
    tabRefs.current[next]?.focus();
  };

  return (
    <div className={styles.root}>
      <TopBarControls>
        <PeriodStepper unit={basis === "month" ? "Bulan" : "Siklus"} label={label ?? "Memuat…"} offset={offset} onChange={setCycleOffset} />
      </TopBarControls>
      <div className={styles.toolbar}>
        <div className={styles.tabs} role="tablist" aria-label="Analisis" onKeyDown={onTabKey}>
          {TABS.map((entry, index) => (
            <button
              key={entry.value}
              ref={(element) => { tabRefs.current[index] = element; }}
              type="button"
              role="tab"
              id={`analysis-tab-${entry.value}`}
              aria-selected={entry.value === tab}
              aria-controls="analysis-panel"
              tabIndex={entry.value === tab ? 0 : -1}
              onClick={() => go(entry.value)}
            >
              {entry.label}
            </button>
          ))}
        </div>
        <span className={styles.grow} />
        <Segmented
          label="Dasar periode"
          options={[{ value: "cycle", label: "Siklus gaji" }, { value: "month", label: "Bulan kalender" }]}
          value={basis}
          onChange={(value) => setTimeframe(value)}
        />
        <Link href="/laporan" className="ws-linkbtn">Baca Laporan Bulanan</Link>
      </div>
      {rolling && <p className={styles.caption}>Analisis memakai siklus gaji atau bulan kalender; yang tampil sekarang siklus gaji.</p>}
      <div id="analysis-panel" role="tabpanel" aria-labelledby={`analysis-tab-${tab}`}>
        {tab === "ringkasan" && <RingkasanTab basis={basis} offset={offset} money={money} onLabel={onLabel} />}
        {tab === "kalender" && (
          <KalenderTab basis={basis} offset={offset} money={money} day={day} onSelectDay={(next) => go("kalender", { day: next })} onLabel={onLabel} />
        )}
        {tab === "aliran" && (
          <PetaAliranTab basis={basis} offset={offset} money={money} view={view} onView={(next) => go("aliran", { view: next })} onLabel={onLabel} />
        )}
      </div>
    </div>
  );
}
