"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";

interface MetricMatrixGridProps {
  totalInflow: number;
  totalOutflow: number;
  netCashflow: number;
  savingsRate: number;
  comparison?: {
    inflow_delta_pct?: number;
    outflow_delta_pct?: number;
    net_cashflow_delta_pct?: number;
    savings_rate_delta_pts?: number;
  };
  kakeibo?: {
    need_spent: number;
    need_pct: number;
    want_spent: number;
    want_pct: number;
    saving_spent: number;
    saving_pct: number;
    kakeibo_status: string;
  };
}

export function MetricMatrixGrid({
  totalInflow,
  totalOutflow,
  netCashflow,
  savingsRate,
  comparison,
  kakeibo,
}: MetricMatrixGridProps) {
  const { bal } = useAppCtx();

  const animInflow = useAnimatedCounter(totalInflow);
  const animOutflow = useAnimatedCounter(totalOutflow);
  const animNet = useAnimatedCounter(netCashflow);

  const inflowDelta = comparison?.inflow_delta_pct ?? 0;
  const outflowDelta = comparison?.outflow_delta_pct ?? 0;
  const netDelta = comparison?.net_cashflow_delta_pct ?? 0;

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3.5 sm:gap-4">
      {/* 1. Inflow Card */}
      <div className="card-squircle p-4 sm:p-5 flex flex-col justify-between bg-income/[0.04] border-income/20 shadow-xs">
        <div className="flex items-center justify-between gap-1.5">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-xl bg-income/15 text-income flex items-center justify-center shrink-0 shadow-xs">
              <Icon name="arrow-down-left" className="h-4 w-4 stroke-[2.5]" />
            </div>
            <span className="text-xs font-bold text-[var(--muted)] truncate">Pemasukan</span>
          </div>

          {inflowDelta !== 0 && (
            <span
              className={cn(
                "text-[10px] font-bold px-2 py-0.5 rounded-full shrink-0 border",
                inflowDelta > 0
                  ? "bg-income/15 text-income border-income/25"
                  : "bg-expense/15 text-expense border-expense/25"
              )}
            >
              {inflowDelta > 0 ? `+${inflowDelta}%` : `${inflowDelta}%`}
            </span>
          )}
        </div>

        <div className="mt-3.5">
          <div className="text-lg sm:text-2xl font-black text-[var(--text)] tabular truncate select-all">
            {bal(animInflow)}
          </div>
          <div className="text-[10px] text-[var(--muted)] mt-0.5">Total kas masuk</div>
        </div>
      </div>

      {/* 2. Outflow Card */}
      <div className="card-squircle p-4 sm:p-5 flex flex-col justify-between bg-expense/[0.04] border-expense/20 shadow-xs">
        <div className="flex items-center justify-between gap-1.5">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-xl bg-expense/15 text-expense flex items-center justify-center shrink-0 shadow-xs">
              <Icon name="arrow-up-right" className="h-4 w-4 stroke-[2.5]" />
            </div>
            <span className="text-xs font-bold text-[var(--muted)] truncate">Pengeluaran</span>
          </div>

          {outflowDelta !== 0 && (
            <span
              className={cn(
                "text-[10px] font-bold px-2 py-0.5 rounded-full shrink-0 border",
                outflowDelta > 0
                  ? "bg-expense/15 text-expense border-expense/25"
                  : "bg-income/15 text-income border-income/25"
              )}
            >
              {outflowDelta > 0 ? `+${outflowDelta}%` : `${outflowDelta}%`}
            </span>
          )}
        </div>

        <div className="mt-3.5">
          <div className="text-lg sm:text-2xl font-black text-expense tabular truncate select-all">
            {bal(animOutflow)}
          </div>
          <div className="text-[10px] text-[var(--muted)] mt-0.5">Total belanja periode ini</div>
        </div>
      </div>

      {/* 3. Net Cash Flow Card */}
      <div
        className={cn(
          "card-squircle p-4 sm:p-5 flex flex-col justify-between shadow-xs",
          netCashflow >= 0
            ? "bg-income/[0.04] border-income/20"
            : "bg-expense/[0.04] border-expense/20"
        )}
      >
        <div className="flex items-center justify-between gap-1.5">
          <div className="flex items-center gap-2">
            <div
              className={cn(
                "h-8 w-8 rounded-xl flex items-center justify-center shrink-0 shadow-xs",
                netCashflow >= 0
                  ? "bg-income/15 text-income"
                  : "bg-expense/15 text-expense"
              )}
            >
              <Icon name="wallet" className="h-4 w-4 stroke-[2.5]" />
            </div>
            <span className="text-xs font-bold text-[var(--muted)] truncate">Arus Kas</span>
          </div>

          <div className="flex items-center gap-1.5">
            {netDelta !== 0 && (
              <span
                className={cn(
                  "text-[10px] font-bold px-2 py-0.5 rounded-full shrink-0 border",
                  netDelta >= 0
                    ? "bg-income/15 text-income border-income/25"
                    : "bg-expense/15 text-expense border-expense/25"
                )}
              >
                {netDelta > 0 ? `+${netDelta}%` : `${netDelta}%`}
              </span>
            )}
            <span
              className={cn(
                "text-[10px] font-extrabold px-2 py-0.5 rounded-full shrink-0 border",
                netCashflow >= 0
                  ? "bg-income/15 text-income border-income/25"
                  : "bg-expense/15 text-expense border-expense/25"
              )}
            >
              {netCashflow >= 0 ? "Surplus" : "Defisit"}
            </span>
          </div>
        </div>

        <div className="mt-3.5">
          <div
            className={cn(
              "text-lg sm:text-2xl font-black tabular truncate select-all",
              netCashflow >= 0 ? "text-income" : "text-expense"
            )}
          >
            {netCashflow >= 0 ? `+${bal(animNet)}` : `-${bal(Math.abs(animNet))}`}
          </div>
          <div className="text-[10px] text-[var(--muted)] mt-0.5">
            Tabungan: <span className="font-bold text-[var(--text)]">{savingsRate}%</span>
          </div>
        </div>
      </div>

      {/* 4. Kakeibo Ratio Card */}
      <div className="card-squircle p-4 sm:p-5 flex flex-col justify-between bg-sky-500/[0.04] border-sky-500/20 shadow-xs">
        <div className="flex items-center justify-between gap-1.5">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-xl bg-sky-500/15 text-sky-500 flex items-center justify-center shrink-0 shadow-xs">
              <Icon name="pie-chart" className="h-4 w-4 stroke-[2.5]" />
            </div>
            <span className="text-xs font-bold text-[var(--muted)] truncate">Pilar Kakeibo</span>
          </div>

          <span
            className={cn(
              "text-[10px] font-bold px-2 py-0.5 rounded-full shrink-0 border",
              kakeibo?.kakeibo_status === "warning"
                ? "bg-rose-500/15 text-rose-500 border-rose-500/25"
                : "bg-sky-500/15 text-sky-500 border-sky-500/25"
            )}
          >
            {kakeibo?.kakeibo_status === "warning" ? "Want > 30%" : "Ideal"}
          </span>
        </div>

        <div className="mt-3.5 space-y-1.5">
          {/* 3 mini bars with smooth cubic transitions */}
          <div className="flex h-2 w-full rounded-full overflow-hidden bg-[var(--surface-raised)] border border-[var(--border)] gap-0.5">
            <div
              className="transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
              style={{ width: `${kakeibo?.need_pct ?? 50}%`, backgroundColor: "var(--color-kakeibo-need)" }}
              title={`Need: ${kakeibo?.need_pct ?? 0}%`}
            />
            <div
              className="transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
              style={{ width: `${kakeibo?.want_pct ?? 30}%`, backgroundColor: "var(--color-kakeibo-want)" }}
              title={`Want: ${kakeibo?.want_pct ?? 0}%`}
            />
            <div
              className="transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
              style={{ width: `${kakeibo?.saving_pct ?? 20}%`, backgroundColor: "var(--color-kakeibo-saving)" }}
              title={`Saving: ${kakeibo?.saving_pct ?? 0}%`}
            />
          </div>

          <div className="flex items-center justify-between text-[10px] pt-0.5">
            <span className="font-bold" style={{ color: "var(--color-kakeibo-need)" }}>{kakeibo?.need_pct ?? 0}% Need</span>
            <span className="font-bold" style={{ color: "var(--color-kakeibo-want)" }}>{kakeibo?.want_pct ?? 0}% Want</span>
            <span className="font-bold" style={{ color: "var(--color-kakeibo-saving)" }}>{kakeibo?.saving_pct ?? 0}% Save</span>
          </div>
        </div>
      </div>
    </div>
  );
}
