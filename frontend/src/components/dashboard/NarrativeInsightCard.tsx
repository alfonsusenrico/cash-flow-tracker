"use client";

import { useId } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import type { PeriodComparisonData } from "@/components/insights/ObservedInsights";

/** Day 1 and 2 of a period are dominated by single bills, so a comparison then misleads. */
export const MIN_COMPARISON_DAYS = 3;

type Change = PeriodComparisonData["categories"]["increases"][number];

function shortDate(isoDate: string) {
  return new Date(`${isoDate}T00:00:00`).toLocaleDateString("id-ID", { day: "numeric", month: "short" });
}

function dateRange(span: { start_date: string; end_date: string }) {
  return span.start_date === span.end_date ? shortDate(span.start_date) : `${shortDate(span.start_date)} – ${shortDate(span.end_date)}`;
}

function ChangeRows({ title, items, bal, increase }: { title: string; items: Change[]; bal: (n: number) => string; increase: boolean }) {
  if (items.length === 0) return null;
  return (
    <div className="min-w-0 space-y-1">
      <h3 className="text-[11px] font-semibold text-[var(--text-secondary)]">{title}</h3>
      <ul className="space-y-1">
        {items.map((item) => (
          <li key={item.key} className="flex min-w-0 items-baseline justify-between gap-2">
            <span className="min-w-0 break-words text-[var(--text)]">{item.name}</span>
            <span className="shrink-0 text-right tabular-nums">
              <span className={cn("font-bold", increase ? "text-rose-700 dark:text-rose-300" : "text-emerald-700 dark:text-emerald-300")}>
                {increase ? "+" : "−"}{bal(Math.abs(item.change))}
              </span>
              <span className="block text-[11px] text-[var(--muted)]">{bal(item.previous)} → {bal(item.current)}</span>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function Headline({ data, bal }: { data: PeriodComparisonData; bal: (n: number) => string }) {
  const days = data.compared_days;
  const current = data.current.outflow;
  if (data.current_truncated && days < MIN_COMPARISON_DAYS) {
    return <p>Periode lalu hanya {days} hari, terlalu pendek untuk dibandingkan. Uang keluar sejauh ini <strong className="tabular-nums">{bal(current)}</strong>.</p>;
  }
  if (days < MIN_COMPARISON_DAYS) {
    return (
      <p>
        Baru {days} hari berjalan. Perbandingan muncul setelah {MIN_COMPARISON_DAYS} hari agar tidak menyesatkan. Uang keluar sejauh
        ini <strong className="tabular-nums">{bal(current)}</strong>.
      </p>
    );
  }
  if (!data.previous_has_data || !data.previous) {
    return (
      <p>
        Periode lalu belum punya transaksi tercatat, jadi belum ada pembanding. Uang keluar sejauh ini{" "}
        <strong className="tabular-nums">{bal(current)}</strong>.
      </p>
    );
  }
  const difference = current - data.previous.outflow;
  return (
    <>
      <p>
        Dalam {days} hari pertama, uang keluar <strong className="tabular-nums">{bal(current)}</strong>
        {difference === 0 ? (
          <>, sama dengan periode lalu pada hari yang sama.</>
        ) : (
          <>
            , <strong className="tabular-nums">{bal(Math.abs(difference))}</strong> {difference > 0 ? "lebih tinggi" : "lebih rendah"} dari
            periode lalu pada hari yang sama.
          </>
        )}
      </p>
      <p className="mt-1 text-xs text-[var(--text-secondary)]">
        Pembanding: {data.previous_label ?? "periode lalu"}, <span className="tabular-nums">{dateRange(data.previous)}</span>
      </p>
    </>
  );
}

/** Beranda insight from the fair comparison: same elapsed days, amounts only, honest when data is too thin. */
export function NarrativeInsightCard() {
  const { timeframe, cycleOffset, bal } = useAppCtx();
  const titleId = useId();
  const basis = timeframe === "cycle" || timeframe === "month" ? timeframe : null;
  const { data, isPending, isError } = useQuery<PeriodComparisonData>({
    queryKey: queryKeys.dashboard.periodComparison(basis ?? "none", cycleOffset),
    queryFn: () => api.get(`/dashboard/period-comparison?timeframe=${basis}&cycle_offset=${cycleOffset}`),
    enabled: basis !== null,
  });
  const comparable = data !== undefined && data.compared_days >= MIN_COMPARISON_DAYS && data.previous_has_data;

  return (
    <section aria-labelledby={titleId} className="card-squircle space-y-3 p-4 text-xs text-[var(--text)] sm:p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <h2 id={titleId} className="text-[11px] font-bold uppercase tracking-wider text-[var(--muted)]">
          Dibanding periode lalu
        </h2>
        {data && <span className="text-[11px] text-[var(--muted)]">{data.label}</span>}
      </div>

      {basis === null ? (
        <p className="text-[var(--text-secondary)]">Perbandingan tersedia untuk siklus gajian atau bulan kalender.</p>
      ) : isError ? (
        <p className="text-[var(--text-secondary)]">Perbandingan belum dapat dimuat.</p>
      ) : isPending || !data ? (
        <p role="status" className="text-[var(--text-secondary)]">Memuat perbandingan…</p>
      ) : (
        <>
          <div className="text-sm leading-relaxed">
            <Headline data={data} bal={bal} />
          </div>
          {comparable && data.current_truncated && (
            <p className="text-[var(--text-secondary)]">Periode lalu lebih pendek, jadi periode ini juga dipotong agar adil.</p>
          )}
          {comparable && (
            data.categories.increases.length + data.categories.decreases.length === 0 ? (
              <p className="text-[var(--text-secondary)]">Tidak ada perubahan per kategori.</p>
            ) : (
              <div className="space-y-3 border-t border-[var(--border)] pt-3">
                <ChangeRows title="Naik paling banyak" items={data.categories.increases} bal={bal} increase />
                <ChangeRows title="Turun paling banyak" items={data.categories.decreases} bal={bal} increase={false} />
              </div>
            )
          )}
        </>
      )}

      <Link
        href="/insights"
        className="inline-flex min-h-11 items-center text-xs font-semibold text-[var(--text-secondary)] underline underline-offset-2 hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)]"
      >
        Lihat perbandingan lengkap di Analisis
      </Link>
    </section>
  );
}
