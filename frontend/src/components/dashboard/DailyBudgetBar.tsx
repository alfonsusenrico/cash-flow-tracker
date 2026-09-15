"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";

interface DailyBudgetBarProps {
  safeToSpendToday: number;
  hasBudget: boolean;
  effectiveBudget: number;
  totalOutflow: number;
  upcomingRecurring?: number;
  spendVelocityRatio?: number;
  spendVelocityStatus?: string;
  benchmarkDaily?: number;
}

export function DailyBudgetBar({
  safeToSpendToday,
  hasBudget,
  effectiveBudget,
  totalOutflow,
  upcomingRecurring = 0,
  spendVelocityRatio = 1.0,
  spendVelocityStatus = "on_track",
  benchmarkDaily = 0,
}: DailyBudgetBarProps) {
  const { bal } = useAppCtx();

  const animSafeToSpend = useAnimatedCounter(safeToSpendToday);
  const animTotalOutflow = useAnimatedCounter(totalOutflow);

  if (!hasBudget || effectiveBudget <= 0) {
    return (
      <div className="card-squircle p-5 sm:p-6 flex items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="h-10 w-10 rounded-2xl bg-[var(--surface-raised)] flex items-center justify-center text-[var(--muted)] shrink-0">
            <Icon name="zap" className="h-4 w-4" />
          </div>
          <div>
            <div className="text-xs font-bold text-[var(--text)]">Batas Anggaran Belum Diatur</div>
            <div className="text-[11px] text-[var(--muted)]">Atur anggaran bulanan di menu pengaturan akun untuk memicu kalkulasi allowance harian</div>
          </div>
        </div>
      </div>
    );
  }

  // Calculate percentage of budget used
  const spentPct = Math.min(100, Math.round((totalOutflow / effectiveBudget) * 100));
  const isOverBudget = totalOutflow > effectiveBudget;

  // Pace status badge with vibrant candy pop
  let paceBadge = {
    label: "Pace Terkendali 😎",
    color: "text-emerald-500 bg-emerald-500/15 border-emerald-500/30",
  };
  if (isOverBudget) {
    paceBadge = {
      label: "Melebihi Anggaran 🚨",
      color: "text-rose-500 bg-rose-500/15 border-rose-500/30",
    };
  } else if (spendVelocityStatus === "fast" || spentPct > 80) {
    paceBadge = {
      label: "Waspada Cepat ⚡",
      color: "text-amber-500 bg-amber-500/15 border-amber-500/30",
    };
  } else if (spendVelocityStatus === "frugal") {
    paceBadge = {
      label: "Sangat Hemat 🛡️",
      color: "text-blue-500 bg-blue-500/15 border-blue-500/30",
    };
  }

  return (
    <div
      className={cn(
        "card-squircle p-5 sm:p-6 space-y-4 transition-all duration-300",
        isOverBudget
          ? "bg-rose-500/[0.04] border-rose-500/25"
          : spentPct > 80
          ? "bg-amber-500/[0.04] border-amber-500/25"
          : "bg-emerald-500/[0.04] border-emerald-500/25"
      )}
    >
      {/* Header: Safe to spend today and status pill */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] font-bold uppercase tracking-wider text-[var(--muted)]">
              Aman Dibelanjakan Hari Ini
            </span>
            <span className={cn("px-2.5 py-0.5 rounded-full text-[10px] font-extrabold border shadow-xs", paceBadge.color)}>
              {paceBadge.label}
            </span>
          </div>
          <div className="text-3xl sm:text-4xl font-extrabold tracking-tight text-[var(--text)] mt-1.5 tabular select-all">
            {bal(animSafeToSpend)}
            <span className="text-xs font-semibold text-[var(--muted)] ml-1.5 font-sans">/ hari</span>
            {benchmarkDaily > 0 && (
              <span className="text-[10px] text-[var(--muted)] ml-2 font-normal hidden sm:inline">
                (Standar {bal(benchmarkDaily)} • {spendVelocityRatio}x)
              </span>
            )}
          </div>
        </div>

        {upcomingRecurring > 0 && (
          <div className="hidden sm:flex flex-col items-end text-right">
            <span className="text-[10px] uppercase text-[var(--muted)]">Komitmen Rutin</span>
            <span className="text-xs font-bold text-amber-500 tabular">
              -{bal(upcomingRecurring)}
            </span>
          </div>
        )}
      </div>

      {/* Linear Track with Moving Cursor */}
      <div className="space-y-2">
        <div className="relative h-2.5 w-full rounded-full bg-[var(--surface-raised)] border border-[var(--border)] overflow-visible">
          {/* Progress bar fill */}
          <div
            className={cn(
              "h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] shadow-xs",
              isOverBudget
                ? "bg-rose-500 shadow-rose-500/30"
                : spentPct > 80
                ? "bg-amber-500 shadow-amber-500/30"
                : "bg-emerald-500 shadow-emerald-500/30"
            )}
            style={{ width: `${Math.min(spentPct, 100)}%` }}
          />

          {/* Indicator Dot at current spend point */}
          <div
            className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 h-4 w-4 rounded-full bg-white border-2 border-[var(--surface)] shadow-md transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] pointer-events-none"
            style={{ left: `${Math.max(2, Math.min(spentPct, 98))}%` }}
          />
        </div>

        {/* Legend beneath the track */}
        <div className="flex items-center justify-between text-[11px] text-[var(--muted)]">
          <div className="flex items-center gap-1.5">
            <span>Terpakai:</span>
            <span className="font-bold text-[var(--text)] tabular">{bal(animTotalOutflow)}</span>
            <span className="font-semibold text-[var(--muted)]">({spentPct}%)</span>
          </div>

          <div className="flex items-center gap-1.5">
            <span>Plafon:</span>
            <span className="font-bold text-[var(--text)] tabular">{bal(effectiveBudget)}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
