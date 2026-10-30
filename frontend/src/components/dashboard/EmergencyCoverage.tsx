import Link from "next/link";
import { ledgerHref } from "@/lib/ledgerFilters";

export interface EssentialCostPeriod {
  kind: "cycle" | "month";
  label: string;
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  days: number;
  gross_need: number;
  reimbursement_offset: number;
  amount: number;
}

export interface EssentialCost {
  amount: number | null;
  living_amount: number | null;
  debt_minimum: number;
  method: "median_need_periods" | "need_budget" | "unknown";
  basis: "history" | "budget" | "unknown";
  period_count: number;
  periods_used: EssentialCostPeriod[];
  limited_history: boolean;
}

export interface EmergencyRunway {
  runway_days: number | null;
  runway_months: number | null;
  coverage_months?: number | null;
  status: "healthy" | "moderate" | "critical" | "zero" | "unknown";
  emergency_fund_balance: number;
  balance_basis?: "emergency_goals" | "all_liquid";
  emergency_goal_names?: string[];
  monthly_primary_expense: number | null;
  target_months?: number;
  essential_cost?: EssentialCost;
}

export const ALL_LIQUID_NOTICE = "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas";

/** The coverage figure, or a plain statement that it cannot be calculated yet. */
export function coverageText(runway: EmergencyRunway): string {
  const months = runway.coverage_months ?? runway.runway_months;
  if (months == null || runway.runway_days == null) return "Belum dapat dihitung";
  if (runway.status === "zero") return "0 hari";
  return `${months.toLocaleString("id-ID")}x biaya pokok · ${runway.runway_days.toLocaleString("id-ID")} hari`;
}

export function coverageTone(status: EmergencyRunway["status"]): string {
  if (status === "healthy") return "bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/20";
  if (status === "moderate") return "bg-amber-500/10 text-amber-700 dark:text-amber-300 border-amber-500/20";
  if (status === "critical") return "bg-rose-500/10 text-rose-700 dark:text-rose-300 border-rose-500/20";
  return "bg-[var(--surface-raised)] text-[var(--text-secondary)] border-[var(--border)]";
}

/** How the essential monthly cost was obtained, in one sentence. */
export function essentialBasisText(cost: EssentialCost | undefined): string {
  if (!cost || cost.method === "unknown") {
    return "Belum diketahui: belum ada periode lengkap dan belum ada anggaran kategori Kebutuhan.";
  }
  const debt = cost.debt_minimum > 0 ? " + cicilan minimum" : "";
  if (cost.method === "need_budget") return `Dari anggaran kategori Kebutuhan (belum ada periode lengkap)${debt}`;
  return `Median ${cost.period_count} periode, kebutuhan (need) saja${debt}`;
}

interface Props {
  runway: EmergencyRunway;
  bal: (amount: number) => string;
}

/** The basis of Ketahanan Dana: the reserve, the essential cost and the periods behind it. */
export function EmergencyCoverageBasis({ runway, bal }: Props) {
  const cost = runway.essential_cost;
  const periods = cost?.periods_used ?? [];
  return (
    <section aria-labelledby="coverage-basis-title" className="card-squircle space-y-3 border border-[var(--border)] p-4 text-xs shadow-xs sm:p-5">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <h2 id="coverage-basis-title" className="text-sm font-bold text-[var(--text)]">Dasar Ketahanan Dana</h2>
          <p className="mt-0.5 text-[var(--text-secondary)]">Saldo cadangan dibagi biaya hidup pokok bulanan.</p>
        </div>
        <span className={`inline-flex items-center rounded-full border px-2.5 py-1 font-bold tabular-nums ${coverageTone(runway.status)}`}>
          {coverageText(runway)}
        </span>
      </div>

      <dl className="grid gap-3 sm:grid-cols-2">
        <div className="min-w-0 space-y-0.5">
          <dt className="font-semibold text-[var(--muted)]">Saldo cadangan</dt>
          <dd className="text-base font-black tabular-nums text-[var(--text)]">{bal(runway.emergency_fund_balance)}</dd>
          <dd className="text-[var(--text-secondary)]">
            {runway.balance_basis === "all_liquid"
              ? ALL_LIQUID_NOTICE
              : `Dari target dana darurat: ${(runway.emergency_goal_names ?? []).join(", ")}`}
          </dd>
        </div>
        <div className="min-w-0 space-y-0.5">
          <dt className="font-semibold text-[var(--muted)]">Biaya hidup pokok bulanan</dt>
          <dd className="text-base font-black tabular-nums text-[var(--text)]">
            {runway.monthly_primary_expense == null ? "Belum diketahui" : `${bal(runway.monthly_primary_expense)}/bln`}
          </dd>
          <dd className="text-[var(--text-secondary)]">{essentialBasisText(cost)}</dd>
          {cost?.limited_history && (
            <dd>
              <span className="inline-flex rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 font-semibold text-amber-800 dark:text-amber-200">
                Riwayat terbatas ({cost.period_count} periode)
              </span>
            </dd>
          )}
          {cost && cost.debt_minimum > 0 && (
            <dd className="tabular-nums text-[var(--text-secondary)]">Termasuk cicilan minimum {bal(cost.debt_minimum)}</dd>
          )}
        </div>
      </dl>

      {periods.length > 0 && (
        <details className="border-t border-[var(--border)] pt-2">
          <summary className="flex min-h-11 cursor-pointer items-center font-semibold text-[var(--text)]">
            Lihat {periods.length} periode yang dipakai
          </summary>
          <p className="mb-2 text-[var(--text-secondary)]">
            Pengeluaran berpilar Kebutuhan, dikurangi ganti uang. Pindah saldo, investasi, kategori di luar anggaran dan
            pembayaran cicilan tidak dihitung; cicilan masuk lewat nilai minimumnya.
          </p>
          <ul className="space-y-1">
            {periods.map((period) => (
              <li key={period.start} className="flex min-w-0 items-center justify-between gap-2">
                <span className="min-w-0 truncate">
                  {period.label} <span className="text-[var(--muted)]">({period.days} hari)</span>
                </span>
                <Link
                  href={ledgerHref({ from: period.start, to: period.end, kakeibo: "need", type: "expense" })}
                  aria-label={`${bal(period.amount)}, lihat transaksi kebutuhan ${period.label}`}
                  className="inline-flex min-h-11 shrink-0 items-center font-bold tabular-nums text-[var(--text)] underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                >
                  {bal(period.amount)}
                </Link>
              </li>
            ))}
          </ul>
        </details>
      )}
    </section>
  );
}
