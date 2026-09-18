"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";

interface KakeiboPillarCardsProps {
  kakeibo?: {
    need_spent: number;
    need_pct: number;
    need_target_pct?: number;
    want_spent: number;
    want_pct: number;
    want_target_pct?: number;
    saving_spent: number;
    saving_pct: number;
    saving_target_pct?: number;
    total_allocated: number;
    kakeibo_status: string;
    want_status?: string;
  };
}

export function KakeiboPillarCards({ kakeibo }: KakeiboPillarCardsProps) {
  const { bal } = useAppCtx();

  const animNeed = useAnimatedCounter(kakeibo?.need_spent ?? 0);
  const animWant = useAnimatedCounter(kakeibo?.want_spent ?? 0);
  const animSaving = useAnimatedCounter(kakeibo?.saving_spent ?? 0);

  const needPct = Math.round(kakeibo?.need_pct ?? 0);
  const wantPct = Math.round(kakeibo?.want_pct ?? 0);
  const savingPct = Math.round(kakeibo?.saving_pct ?? 0);

  const isWantWarning = (kakeibo?.want_status === "warning") || wantPct > 30;
  const isSavingGood = savingPct >= 20;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between px-1">
        <div className="flex items-center gap-2">
          <span className="text-base">🎯</span>
          <h2 className="text-xs sm:text-sm font-bold tracking-tight text-[var(--text)] uppercase">
            Aturan Alokasi Finansial (50 / 30 / 20)
          </h2>
        </div>
        <span className="text-[11px] text-[var(--muted)]">
          {kakeibo?.total_allocated ? `Total: ${bal(kakeibo.total_allocated)}` : "Siklus Ini"}
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3.5">
        {/* Pillar 1: Need */}
        <div className="card-squircle p-4 sm:p-5 bg-sky-500/[0.04] border-sky-500/25 hover:border-sky-500/40 transition-all flex flex-col justify-between space-y-3 shadow-xs">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="text-xl">🍞</span>
              <div>
                <div className="text-xs font-bold text-[var(--text)]">Kebutuhan</div>
                <div className="text-[10px] text-[var(--muted)]">Target: 50%</div>
              </div>
            </div>
            <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-sky-500/15 text-sky-500 border border-sky-500/25">
              {needPct}%
            </span>
          </div>

          <div>
            <div className="text-lg sm:text-xl font-black text-[var(--text)] tabular select-all">
              {bal(animNeed)}
            </div>
            <div className="text-[10px] text-[var(--muted)] mt-0.5">Makan, sewa, tagihan rutin</div>
          </div>

          <div className="space-y-1">
            <div className="h-2 w-full rounded-full bg-sky-500/15 overflow-hidden">
              <div
                className="h-full rounded-full bg-sky-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] shadow-xs"
                style={{ width: `${Math.min(needPct, 100)}%` }}
              />
            </div>
            <div className="flex justify-between text-[9px] text-[var(--muted)]">
              <span>0%</span>
              <span className="font-semibold text-sky-500">{needPct}% / 50%</span>
            </div>
          </div>
        </div>

        {/* Pillar 2: Want */}
        <div
          className={cn(
            "card-squircle p-4 sm:p-5 transition-all flex flex-col justify-between space-y-3 shadow-xs",
            isWantWarning
              ? "bg-rose-500/[0.05] border-rose-500/30 hover:border-rose-500/50"
              : "bg-purple-500/[0.04] border-purple-500/25 hover:border-purple-500/40"
          )}
        >
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="text-xl">👑</span>
              <div>
                <div className="text-xs font-bold text-[var(--text)]">Keinginan</div>
                <div className="text-[10px] text-[var(--muted)]">Maks: 30%</div>
              </div>
            </div>
            <span
              className={cn(
                "text-[10px] font-bold px-2 py-0.5 rounded-full border",
                isWantWarning
                  ? "bg-rose-500/15 text-rose-500 border-rose-500/30"
                  : "bg-purple-500/15 text-purple-500 border-purple-500/30"
              )}
            >
              {wantPct}% {isWantWarning ? "⚠️ Waspada" : "✨ Aman"}
            </span>
          </div>

          <div>
            <div
              className={cn(
                "text-lg sm:text-xl font-black tabular select-all",
                isWantWarning ? "text-rose-500" : "text-[var(--text)]"
              )}
            >
              {bal(animWant)}
            </div>
            <div className="text-[10px] text-[var(--muted)] mt-0.5">Jajan, hangout, belanja hobi</div>
          </div>

          <div className="space-y-1">
            <div className="h-2 w-full rounded-full bg-[var(--surface-raised)] border border-[var(--border)] overflow-hidden">
              <div
                className={cn(
                  "h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] shadow-xs",
                  isWantWarning ? "bg-rose-500" : "bg-purple-500"
                )}
                style={{ width: `${Math.min(wantPct, 100)}%` }}
              />
            </div>
            <div className="flex justify-between text-[9px] text-[var(--muted)]">
              <span>0%</span>
              <span className={cn("font-semibold", isWantWarning ? "text-rose-500" : "text-purple-500")}>
                {wantPct}% / 30%
              </span>
            </div>
          </div>
        </div>

        {/* Pillar 3: Saving */}
        <div className="card-squircle p-4 sm:p-5 bg-emerald-500/[0.04] border-emerald-500/25 hover:border-emerald-500/40 transition-all flex flex-col justify-between space-y-3 shadow-xs">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="text-xl">💎</span>
              <div>
                <div className="text-xs font-bold text-[var(--text)]">Tabungan</div>
                <div className="text-[10px] text-[var(--muted)]">Min: 20%</div>
              </div>
            </div>
            <span
              className={cn(
                "text-[10px] font-bold px-2 py-0.5 rounded-full border",
                isSavingGood
                  ? "bg-emerald-500/15 text-emerald-500 border-emerald-500/30"
                  : "bg-amber-500/15 text-amber-500 border-amber-500/30"
              )}
            >
              {savingPct}% {isSavingGood ? "🎉 Bagus" : "🎯 Kejar"}
            </span>
          </div>

          <div>
            <div className="text-lg sm:text-xl font-black text-emerald-500 tabular select-all">
              {bal(animSaving)}
            </div>
            <div className="text-[10px] text-[var(--muted)] mt-0.5">Dana darurat, bibit, investasi</div>
          </div>

          <div className="space-y-1">
            <div className="h-2 w-full rounded-full bg-emerald-500/15 overflow-hidden">
              <div
                className="h-full rounded-full bg-emerald-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] shadow-xs"
                style={{ width: `${Math.min(savingPct, 100)}%` }}
              />
            </div>
            <div className="flex justify-between text-[9px] text-[var(--muted)]">
              <span>0%</span>
              <span className="font-semibold text-emerald-500">{savingPct}% / 20%</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
