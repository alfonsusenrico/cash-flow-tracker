"use client";

import { cn } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";

interface KpisData {
  liquid_net_worth: number;
  liquid_balance?: number;
  investment_balance?: number;
  total_balance?: number;
  total_inflow: number;
  total_outflow: number;
  net_cashflow: number;
  savings_rate: number;
  safe_to_spend_today: number;
  daily_burn_rate: number;
  runway_days: number;
  runway_months?: number;
  target_months?: number;
  target_amount?: number;
  coverage_pct?: number;
  runway_status?: "healthy" | "moderate" | "critical" | "zero";
  emergency_fund_balance?: number;
  monthly_primary_expense?: number;
  is_emergency_flagged?: boolean;
  emergency_goal_names?: string[];
  benchmark_daily: number;
  has_budget?: boolean;
  effective_budget?: number;
}

interface KpiRibbonProps {
  kpis: KpisData;
  timeframeLabel?: string;
}

export function KpiRibbon({ kpis, timeframeLabel }: KpiRibbonProps) {
  const { bal } = useAppCtx();

  const liquidBal = kpis.liquid_balance ?? kpis.liquid_net_worth;
  const isZeroBalance = liquidBal <= 0;
  const isZeroBurn = kpis.daily_burn_rate === 0;

  const monthsVal =
    kpis.runway_months !== undefined
      ? kpis.runway_months
      : Number((kpis.runway_days / 30.4).toFixed(1));
  const targetMonths = kpis.target_months ?? 6;
  const isHealthyRunway = kpis.runway_status
    ? kpis.runway_status === "healthy"
    : !isZeroBalance && (monthsVal >= targetMonths || isZeroBurn);
  const isModerateRunway = kpis.runway_status
    ? kpis.runway_status === "moderate"
    : !isZeroBalance && !isZeroBurn && monthsVal >= (targetMonths / 2) && monthsVal < targetMonths;

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 sm:gap-4">
      {/* 1. Liquid Net Worth */}
      <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 sm:p-5 flex flex-col justify-between shadow-xs">
        <div className="flex items-center justify-between text-xs text-[var(--muted)]">
          <span className="font-medium">Saldo Likuid</span>
          <span className="text-[10px] uppercase px-1.5 py-0.5 rounded bg-[var(--surface-raised)] border border-[var(--border)] font-semibold">
            Kas & Bank
          </span>
        </div>
        <div className="mt-3">
          <div className="text-xl sm:text-2xl font-bold tracking-tight text-[var(--text)] tabular select-all">
            {bal(liquidBal)}
          </div>
          <p className="text-[11px] text-[var(--muted)] mt-1 flex items-center gap-1">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-500" />
            Siap digunakan belanja harian
          </p>
        </div>
      </div>

      {/* 2. Total Inflow */}
      <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 sm:p-5 flex flex-col justify-between shadow-xs">
        <div className="flex items-center justify-between text-xs text-[var(--muted)]">
          <span className="font-medium">Uang Masuk</span>
          <div className="h-5 w-5 rounded-full bg-income/10 text-income flex items-center justify-center text-xs font-bold">
            ↓
          </div>
        </div>
        <div className="mt-3">
          <div className="text-xl sm:text-2xl font-bold tracking-tight text-income tabular select-all">
            +{bal(kpis.total_inflow)}
          </div>
          <p className="text-[11px] text-[var(--muted)] mt-1 truncate">
            {timeframeLabel || "Periode saat ini"}
          </p>
        </div>
      </div>

      {/* 3. Total Outflow */}
      <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 sm:p-5 flex flex-col justify-between shadow-xs">
        <div className="flex items-center justify-between text-xs text-[var(--muted)]">
          <span className="font-medium">Uang Keluar</span>
          <div className="h-5 w-5 rounded-full bg-expense/10 text-expense flex items-center justify-center text-xs font-bold">
            ↑
          </div>
        </div>
        <div className="mt-3">
          <div className="text-xl sm:text-2xl font-bold tracking-tight text-expense tabular select-all">
            -{bal(kpis.total_outflow)}
          </div>
          <p className="text-[11px] text-[var(--muted)] mt-1 truncate tabular font-medium">
            Rata-rata: {bal(kpis.daily_burn_rate)}/hari
          </p>
        </div>
      </div>

      {/* 4. Surplus Arus Kas (Net Cash Flow) */}
      <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 sm:p-5 flex flex-col justify-between shadow-xs">
        <div className="flex items-center justify-between text-xs text-[var(--muted)]">
          <span className="font-medium">Surplus Arus Kas</span>
          <span
            className={cn(
              "text-[10px] px-1.5 py-0.5 rounded font-bold tabular",
              kpis.net_cashflow >= 0
                ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                : "bg-rose-500/10 text-rose-600 dark:text-rose-400"
            )}
          >
            {kpis.savings_rate >= 0 ? "+" : ""}
            {kpis.savings_rate}% margin
          </span>
        </div>
        <div className="mt-3">
          <div
            className={cn(
              "text-xl sm:text-2xl font-bold tracking-tight tabular select-all",
              kpis.net_cashflow >= 0 ? "text-income" : "text-expense"
            )}
          >
            {kpis.net_cashflow >= 0 ? "+" : ""}
            {bal(kpis.net_cashflow)}
          </div>
          <p className="text-[11px] text-[var(--muted)] mt-1">
            {kpis.net_cashflow >= 0 ? "Surplus kas periode ini" : "Defisit kas periode ini"}
          </p>
        </div>
      </div>

      {/* 5. Ketahanan Dana */}
      <div className="col-span-2 md:col-span-1 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 sm:p-5 flex flex-col justify-between shadow-xs">
        <div className="flex items-center justify-between text-xs text-[var(--muted)]">
          <span className="font-medium">Ketahanan Dana</span>
          <span
            className={cn(
              "text-[10px] px-1.5 py-0.5 rounded font-bold uppercase",
              isZeroBalance || monthsVal === 0
                ? "bg-zinc-500/10 text-zinc-500 dark:text-zinc-400"
                : isHealthyRunway
                ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                : isModerateRunway
                ? "bg-amber-500/10 text-amber-600 dark:text-amber-400"
                : "bg-rose-500/10 text-rose-600 dark:text-rose-400"
            )}
          >
            {isZeroBalance || monthsVal === 0
              ? "Nol"
              : isHealthyRunway
              ? "Aman"
              : isModerateRunway
              ? "Cukup"
              : "Waspada"}
          </span>
        </div>
        <div className="mt-3">
          <div className="text-xl sm:text-2xl font-bold tracking-tight text-[var(--text)] tabular select-all">
            {isZeroBalance || monthsVal === 0
              ? "0x Biaya Pokok"
              : monthsVal >= 99
              ? "> 12x Biaya Pokok"
              : `${monthsVal}x Biaya Pokok`}
          </div>
          <p className="text-[11px] text-[var(--muted)] mt-1 tabular font-medium">
            {isZeroBalance
              ? "Belum ada dana darurat"
              : monthsVal >= 99
              ? `Sangat aman (Target ${targetMonths}x tercapai)`
              : `${kpis.coverage_pct ?? Math.round((monthsVal / targetMonths) * 100)}% target (${targetMonths} bln)`}
          </p>
        </div>
      </div>
    </div>
  );
}
