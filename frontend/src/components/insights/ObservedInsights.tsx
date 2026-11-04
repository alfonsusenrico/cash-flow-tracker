"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { DisclosureSummary } from "@/components/ui/DisclosureSummary";
import { ledgerHref } from "@/lib/ledgerFilters";
import type { ExpensePlanDraft } from "@/types/recurring";

type Money = (amount: number) => string;
export type InsightBasis = "cycle" | "month";






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




const CADENCE_LABEL: Record<RecurringCharge["cadence"], string> = { monthly: "Bulanan", weekly: "Mingguan" };

/** Charges that recurred with a steady amount; each can become a non-posting plan after confirmation. */
export function RecurringCharges({ bal, onCreatePlan }: { bal: Money; onCreatePlan: (draft: ExpensePlanDraft) => void }) {
  const { data, isLoading, isError } = useQuery<RecurringChargesData>({
    queryKey: queryKeys.dashboard.recurringCharges,
    queryFn: () => api.get("/dashboard/recurring-charges"),
  });
  const charges = data?.charges ?? [];
  return (
    <section aria-labelledby="recurring-title" className="ws-card space-y-3 text-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <h2 id="recurring-title" className="text-base font-bold tracking-tight text-[var(--text)]">Tagihan & langganan terdeteksi</h2>
        {data && charges.length > 0 && (
          <p className="shrink-0 text-right tabular-nums" data-testid="recurring-headline">
            <span className="text-sm font-black text-[var(--text)]">{charges.length}</span>
            <span className="text-[var(--muted)]"> · {bal(data.monthly_equivalent_total)}/bln</span>
          </p>
        )}
      </div>
      {isError && <p className="text-[var(--text-2)]">Tagihan rutin belum dapat dimuat.</p>}
      {!isError && !isLoading && charges.length === 0 && (
        <p className="text-[var(--text-2)]">Belum ada tagihan atau langganan yang berulang dengan pola tetap.</p>
      )}
      {charges.length > 0 && data && (
        <details>
        <DisclosureSummary>Rincian</DisclosureSummary>
        <ul className="space-y-2">
          {charges.map((charge) => (
            <li key={charge.key} className="min-w-0 space-y-1.5 rounded-xl border border-[var(--line)] p-3">
              <div className="flex min-w-0 items-start justify-between gap-2">
                <div className="min-w-0">
                  <Link
                    href={ledgerHref({ from: data.window.start, to: data.window.end, type: "expense", q: charge.search_text })}
                    className="block min-h-6 truncate font-semibold leading-6 text-[var(--text)] underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--ink)]"
                  >
                    {charge.merchant}
                  </Link>
                  <p className="truncate text-[var(--muted)]">
                    {CADENCE_LABEL[charge.cadence]} · {charge.occurrences} kali · {charge.account_name}
                  </p>
                </div>
                <span className="shrink-0 text-right text-sm font-black tabular-nums">{bal(charge.typical_amount)}</span>
              </div>
              <p className="tabular-nums text-[var(--text-2)]">
                Terakhir {day(charge.last_date)} · berikutnya perkiraan {day(charge.next_expected_date)}
              </p>
              {charge.amount_changed && (
                <p className="tabular-nums text-warn-fg">
                  Nominal terakhir berubah: {bal(charge.previous_amount)} → {bal(charge.last_amount)}
                </p>
              )}
              <div className="flex flex-wrap items-center gap-2">
                {charge.existing_plan ? (
                  <span className="rounded-full border border-in-solid/30 bg-in-solid/10 px-2 py-0.5 font-semibold text-in-fg">
                    Sudah ada rencana: {charge.existing_plan.name}
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={() => onCreatePlan(charge.plan_draft)}
                    aria-label={`Jadikan rencana: ${charge.merchant}`}
                    className="min-h-11 rounded-lg border border-[var(--line)] px-3 font-semibold text-[var(--text)] hover:bg-[var(--surface-2)] focus-visible:outline-2 focus-visible:outline-[var(--ink)]"
                  >
                    Jadikan rencana
                  </button>
                )}
              </div>
            </li>
          ))}
        </ul>
        </details>
      )}
    </section>
  );
}
