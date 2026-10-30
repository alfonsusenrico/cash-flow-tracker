"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import { KAKEIBO_PILLARS, type KakeiboPillar } from "@/lib/kakeibo";
import { ledgerHref, UNCATEGORIZED } from "@/lib/ledgerFilters";
import type { ExpensePlanDraft } from "@/types/recurring";

type Money = (amount: number) => string;
export type InsightBasis = "cycle" | "month";

interface PeriodWindow {
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  inflow: number;
  outflow: number;
  net: number;
}

interface Change {
  key: string;
  id: string | null;
  name: string;
  search_text?: string | null;
  is_uncategorized?: boolean;
  current: number;
  previous: number;
  change: number;
}

interface Changes {
  increases: Change[];
  decreases: Change[];
}

export interface PeriodComparisonData {
  label: string;
  compared_days: number;
  current_truncated: boolean;
  current: PeriodWindow;
  previous: PeriodWindow | null;
  previous_label: string | null;
  previous_has_data: boolean;
  pillars: { pillar: KakeiboPillar; current: number; previous: number | null; change: number | null }[];
  categories: Changes;
  merchants: Changes;
}

export interface CashFlowPeriod {
  kind: "cycle" | "month";
  label: string;
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  in_progress: boolean;
  inflow: number;
  outflow: number;
  net: number;
}

export interface RecurringCharge {
  key: string;
  merchant: string;
  search_text: string;
  cadence: "monthly" | "weekly";
  occurrences: number;
  typical_amount: number;
  last_amount: number;
  previous_amount: number;
  amount_changed: boolean;
  last_date: string;
  next_expected_date: string;
  monthly_equivalent: number;
  account_name: string;
  existing_plan: { id: string; name: string } | null;
  plan_draft: ExpensePlanDraft;
}

export interface RecurringChargesData {
  window: { start: string; end: string; start_date: string; end_date: string; months: number };
  charges: RecurringCharge[];
  monthly_equivalent_total: number;
  monthly_equivalent_without_plan: number;
}

function day(isoDate: string): string {
  return new Date(`${isoDate}T00:00:00`).toLocaleDateString("id-ID", { day: "numeric", month: "short" });
}

function range(span: { start_date: string; end_date: string }): string {
  return span.start_date === span.end_date ? day(span.start_date) : `${day(span.start_date)} – ${day(span.end_date)}`;
}

function signed(bal: Money, amount: number): string {
  if (amount === 0) return bal(0);
  return `${amount > 0 ? "+" : "−"}${bal(Math.abs(amount))}`;
}

const PILLAR_LABEL = Object.fromEntries(KAKEIBO_PILLARS.map((item) => [item.value, item.label])) as Record<KakeiboPillar, string>;

function ChangeList({ title, items, href, bal, empty }: {
  title: string;
  items: Change[];
  href: (item: Change) => string;
  bal: Money;
  empty: string;
}) {
  return (
    <div className="min-w-0 space-y-1">
      <h4 className="font-semibold text-[var(--text-secondary)]">{title}</h4>
      {items.length === 0 ? (
        <p className="text-[var(--muted)]">{empty}</p>
      ) : (
        <ul className="space-y-1">
          {items.map((item) => (
            <li key={item.key} className="flex min-w-0 items-baseline justify-between gap-2">
              <Link
                href={href(item)}
                className="min-w-0 truncate underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
              >
                {item.name}
              </Link>
              <span className="shrink-0 text-right tabular-nums">
                <span className={cn("font-bold", item.change > 0 ? "text-rose-700 dark:text-rose-300" : "text-emerald-700 dark:text-emerald-300")}>
                  {signed(bal, item.change)}
                </span>
                <span className="block text-[11px] text-[var(--muted)]">{bal(item.previous)} → {bal(item.current)}</span>
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** The selected period against the previous one, both over the same elapsed time. */
export function PeriodComparison({ basis, offset, bal }: { basis: InsightBasis; offset: number; bal: Money }) {
  const { data, isLoading, isError } = useQuery<PeriodComparisonData>({
    queryKey: queryKeys.dashboard.periodComparison(basis, offset),
    queryFn: () => api.get(`/dashboard/period-comparison?timeframe=${basis}&cycle_offset=${offset}`),
  });
  const current = data?.current;
  const previous = data?.previous_has_data ? data.previous : null;
  const scope = current ? { from: current.start, to: current.end } : {};
  return (
    <section aria-labelledby="comparison-title" className="cockpit-card space-y-4 border border-[var(--border)] p-4 text-xs shadow-xs sm:p-6">
      <div>
        <h2 id="comparison-title" className="text-base font-bold tracking-tight text-[var(--text)]">Periode ini vs periode lalu</h2>
        {data && current ? (
          <p className="mt-0.5 text-[var(--text-secondary)]">
            {data.label} ({range(current)})
            {data.previous ? ` dibanding ${data.previous_label ?? "periode lalu"} (${range(data.previous)})` : ""}, sama-sama{" "}
            {data.compared_days} hari pertama.
          </p>
        ) : (
          <p className="mt-0.5 text-[var(--text-secondary)]">{isError ? "Perbandingan belum dapat dimuat." : isLoading ? "Memuat…" : ""}</p>
        )}
      </div>
      {data && current && (
        <>
          {data.current_truncated && (
            <p className="text-[var(--text-secondary)]">Periode lalu lebih pendek, jadi periode ini juga dipotong agar adil.</p>
          )}
          {!data.previous_has_data && (
            <p className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3 text-[var(--text-secondary)]">
              Periode lalu belum punya transaksi tercatat, jadi belum ada pembanding. Angka di bawah hanya periode ini.
            </p>
          )}
          <dl className="grid grid-cols-3 gap-2">
            {([
              ["Uang masuk", current.inflow, previous?.inflow, false],
              ["Uang keluar", current.outflow, previous?.outflow, true],
              ["Selisih bersih", current.net, previous?.net, false],
            ] as const).map(([label, now, before, spending]) => (
              <div key={label} className="min-w-0 rounded-xl border border-[var(--border)] p-2.5 sm:p-3">
                <dt className="font-semibold text-[var(--muted)]">{label}</dt>
                <dd className="truncate text-sm font-black tabular-nums text-[var(--text)] sm:text-base">{bal(now)}</dd>
                {before != null && (
                  <dd className="truncate text-[11px] tabular-nums text-[var(--text-secondary)]">
                    Lalu {bal(before)} ·{" "}
                    <span className={cn(now - before !== 0 && ((now - before > 0) === spending ? "text-rose-700 dark:text-rose-300" : "text-emerald-700 dark:text-emerald-300"))}>
                      {signed(bal, now - before)}
                    </span>
                  </dd>
                )}
              </div>
            ))}
          </dl>

          <div className="space-y-1.5">
            <h3 className="font-bold text-[var(--text)]">Per pilar Kakeibo</h3>
            <ul className="divide-y divide-[var(--border)]">
              {data.pillars.map((item) => (
                <li key={item.pillar} className="flex min-w-0 items-baseline justify-between gap-2 py-1.5">
                  <Link
                    href={ledgerHref({ ...scope, type: "expense", kakeibo: item.pillar })}
                    className="min-w-0 truncate underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                  >
                    {PILLAR_LABEL[item.pillar]}
                  </Link>
                  <span className="shrink-0 text-right tabular-nums">
                    <span className="font-bold">{bal(item.current)}</span>
                    {item.change != null && item.previous != null && (
                      <span className="block text-[11px] text-[var(--muted)]">
                        lalu {bal(item.previous)} · {signed(bal, item.change)}
                      </span>
                    )}
                  </span>
                </li>
              ))}
            </ul>
          </div>

          {data.previous_has_data && (
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <h3 className="font-bold text-[var(--text)]">Kategori yang berubah</h3>
                <ChangeList
                  title="Naik" items={data.categories.increases} bal={bal} empty="Tidak ada kenaikan."
                  href={(item) => ledgerHref({ ...scope, type: "expense", category: item.is_uncategorized ? UNCATEGORIZED : item.id ?? "all" })}
                />
                <ChangeList
                  title="Turun" items={data.categories.decreases} bal={bal} empty="Tidak ada penurunan."
                  href={(item) => ledgerHref({ ...scope, type: "expense", category: item.is_uncategorized ? UNCATEGORIZED : item.id ?? "all" })}
                />
              </div>
              <div className="space-y-2">
                <h3 className="font-bold text-[var(--text)]">Toko yang berubah</h3>
                <ChangeList
                  title="Naik" items={data.merchants.increases} bal={bal} empty="Tidak ada kenaikan."
                  href={(item) => ledgerHref({ ...scope, type: "expense", q: item.search_text ?? item.name })}
                />
                <ChangeList
                  title="Turun" items={data.merchants.decreases} bal={bal} empty="Tidak ada penurunan."
                  href={(item) => ledgerHref({ ...scope, type: "expense", q: item.search_text ?? item.name })}
                />
                <p className="text-[11px] text-[var(--muted)]">Per toko memakai nilai kotor; ganti uang dihitung per kategori.</p>
              </div>
            </div>
          )}
        </>
      )}
    </section>
  );
}

/** Observed inflow, outflow and net per period; the current period is labelled as in progress. */
export function CashFlowHistory({ basis, bal }: { basis: InsightBasis; bal: Money }) {
  const { data, isLoading, isError } = useQuery<{ periods: CashFlowPeriod[]; cycle_status: string }>({
    queryKey: queryKeys.dashboard.cashFlowHistory(basis),
    queryFn: () => api.get(`/dashboard/cash-flow-history?timeframe=${basis}`),
  });
  const periods = data?.periods ?? [];
  return (
    <section aria-labelledby="history-title" className="cockpit-card space-y-3 border border-[var(--border)] p-4 text-xs shadow-xs sm:p-6">
      <div>
        <h2 id="history-title" className="text-base font-bold tracking-tight text-[var(--text)]">Arus kas per periode</h2>
        <p className="mt-0.5 text-[var(--text-secondary)]">
          Hanya yang tercatat: hingga 6 periode lengkap dan periode berjalan. Uang keluar sudah dikurangi ganti uang.
        </p>
      </div>
      {isError && <p className="text-[var(--text-secondary)]">Riwayat belum dapat dimuat.</p>}
      {!isError && !isLoading && periods.length === 0 && (
        <p className="text-[var(--text-secondary)]">Belum ada transaksi tercatat.</p>
      )}
      {periods.length > 0 && (
        <ol className="divide-y divide-[var(--border)]">
          {[...periods].reverse().map((period) => (
            <li key={period.start} className="space-y-1 py-2">
              <div className="flex min-w-0 items-center gap-2">
                <Link
                  href={ledgerHref({ from: period.start, to: period.end })}
                  className="min-w-0 truncate font-semibold underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                >
                  {period.label}
                </Link>
                {period.in_progress && (
                  <span className="shrink-0 rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 font-semibold text-amber-800 dark:text-amber-200">
                    berjalan
                  </span>
                )}
                <span className="ml-auto shrink-0 text-[11px] text-[var(--muted)]">{range(period)}</span>
              </div>
              <dl className="grid grid-cols-3 gap-2 tabular-nums">
                <div className="min-w-0"><dt className="text-[var(--muted)]">Masuk</dt><dd className="truncate font-semibold">{bal(period.inflow)}</dd></div>
                <div className="min-w-0"><dt className="text-[var(--muted)]">Keluar</dt><dd className="truncate font-semibold">{bal(period.outflow)}</dd></div>
                <div className="min-w-0">
                  <dt className="text-[var(--muted)]">Bersih</dt>
                  <dd className={cn("truncate font-bold", period.net >= 0 ? "text-emerald-700 dark:text-emerald-300" : "text-rose-700 dark:text-rose-300")}>
                    {signed(bal, period.net)}
                  </dd>
                </div>
              </dl>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

const CADENCE_LABEL: Record<RecurringCharge["cadence"], string> = { monthly: "Bulanan", weekly: "Mingguan" };

/** Charges that recurred with a steady amount; each can become a non-posting plan after confirmation. */
export function RecurringCharges({ bal, onCreatePlan }: { bal: Money; onCreatePlan: (draft: ExpensePlanDraft) => void }) {
  const { data, isLoading, isError } = useQuery<RecurringChargesData>({
    queryKey: queryKeys.dashboard.recurringCharges,
    queryFn: () => api.get("/dashboard/recurring-charges"),
  });
  const charges = data?.charges ?? [];
  return (
    <section aria-labelledby="recurring-title" className="cockpit-card space-y-3 border border-[var(--border)] p-4 text-xs shadow-xs sm:p-6">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <h2 id="recurring-title" className="text-base font-bold tracking-tight text-[var(--text)]">Tagihan & langganan terdeteksi</h2>
          <p className="mt-0.5 text-[var(--text-secondary)]">
            Dari 6 bulan terakhir: muncul minimal 3 kali, berjarak mingguan (6–8 hari) atau bulanan (25–35 hari), dengan
            nominal yang stabil (variasi ≤ 15%).
          </p>
        </div>
        {data && charges.length > 0 && (
          <p className="shrink-0 text-right tabular-nums">
            <span className="block text-[var(--muted)]">Setara per bulan</span>
            <span className="text-sm font-black text-[var(--text)]">{bal(data.monthly_equivalent_total)}</span>
          </p>
        )}
      </div>
      {isError && <p className="text-[var(--text-secondary)]">Tagihan rutin belum dapat dimuat.</p>}
      {!isError && !isLoading && charges.length === 0 && (
        <p className="text-[var(--text-secondary)]">Belum ada tagihan atau langganan yang berulang dengan pola tetap.</p>
      )}
      {charges.length > 0 && data && (
        <ul className="space-y-2">
          {charges.map((charge) => (
            <li key={charge.key} className="min-w-0 space-y-1.5 rounded-xl border border-[var(--border)] p-3">
              <div className="flex min-w-0 items-start justify-between gap-2">
                <div className="min-w-0">
                  <Link
                    href={ledgerHref({ from: data.window.start, to: data.window.end, type: "expense", q: charge.search_text })}
                    className="block truncate font-semibold text-[var(--text)] underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                  >
                    {charge.merchant}
                  </Link>
                  <p className="truncate text-[var(--muted)]">
                    {CADENCE_LABEL[charge.cadence]} · {charge.occurrences} kali · {charge.account_name}
                  </p>
                </div>
                <span className="shrink-0 text-right text-sm font-black tabular-nums">{bal(charge.typical_amount)}</span>
              </div>
              <p className="tabular-nums text-[var(--text-secondary)]">
                Terakhir {day(charge.last_date)} · berikutnya perkiraan {day(charge.next_expected_date)}
              </p>
              {charge.amount_changed && (
                <p className="tabular-nums text-amber-800 dark:text-amber-200">
                  Nominal terakhir berubah: {bal(charge.previous_amount)} → {bal(charge.last_amount)}
                </p>
              )}
              <div className="flex flex-wrap items-center gap-2">
                {charge.existing_plan ? (
                  <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 font-semibold text-emerald-800 dark:text-emerald-200">
                    Sudah ada rencana: {charge.existing_plan.name}
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={() => onCreatePlan(charge.plan_draft)}
                    aria-label={`Jadikan rencana: ${charge.merchant}`}
                    className="min-h-11 rounded-lg border border-[var(--border)] px-3 font-semibold text-[var(--text)] hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                  >
                    Jadikan rencana
                  </button>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
