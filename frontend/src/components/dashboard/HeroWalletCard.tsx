"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";

interface HeroWalletCardProps {
  totalBalance: number;
  liquidBalance: number;
  investmentBalance?: number;
  accountsCount: number;
  onOpenCapture: (type?: "expense" | "income") => void;
  onOpenMovement?: () => void;
}

export function HeroWalletCard({
  totalBalance,
  liquidBalance,
  investmentBalance = 0,
  accountsCount,
  onOpenCapture,
  onOpenMovement,
}: HeroWalletCardProps) {
  const { hideBalances, setHideBalances, bal, openMovement } = useAppCtx();

  const animTotal = useAnimatedCounter(totalBalance);
  const animLiquid = useAnimatedCounter(liquidBalance);
  const animInvest = useAnimatedCounter(investmentBalance);

  const formattedTotal = bal(animTotal);
  const formattedLiquid = hideBalances ? "••••••" : bal(animLiquid);
  const formattedInvest = hideBalances ? "••••••" : bal(animInvest);

  return (
    <div className="card-squircle p-5 sm:p-7 relative overflow-hidden bg-gradient-to-br from-emerald-500/[0.08] via-[var(--surface)] to-teal-500/[0.04] border border-emerald-500/25 shadow-[0_12px_36px_rgba(0,208,156,0.08)] transition-all">
      {/* Ambient background glow orbs */}
      <div className="absolute -top-16 -right-16 w-48 h-48 rounded-full bg-emerald-500/10 blur-3xl pointer-events-none" />
      <div className="absolute -bottom-16 -left-16 w-40 h-40 rounded-full bg-indigo-500/10 blur-3xl pointer-events-none" />

      {/* Header Row: Label, Account Count, and Privacy Toggle */}
      <div className="relative z-10 flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
          <span className="text-[11px] font-extrabold uppercase tracking-widest text-[var(--muted)]">
            Total Likuiditas & Vault
          </span>
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20">
            {accountsCount} Dompet Aktif
          </span>
        </div>

        <button
          type="button"
          onClick={() => setHideBalances(!hideBalances)}
          className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[11px] font-semibold bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)] border border-[var(--border)] transition-all pressable"
          title={hideBalances ? "Tampilkan Saldo" : "Sembunyikan Saldo"}
        >
          <Icon name={hideBalances ? "eye-off" : "eye"} className="h-3.5 w-3.5" />
          <span className="text-[10px]">
            {hideBalances ? "Tampilkan" : "Privasi"}
          </span>
        </button>
      </div>

      {/* Hero Numeric Balance */}
      <div className="relative z-10 mt-4">
        <div className="text-3xl sm:text-5xl font-black tracking-tight text-[var(--text)] select-all tabular">
          {formattedTotal}
        </div>

        {/* Sub-breakdown: Liquid vs Invested */}
        <div className="flex items-center gap-3.5 mt-3 text-xs text-[var(--muted)] flex-wrap">
          <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-[var(--surface-raised)] border border-[var(--border)]">
            <span className="h-2 w-2 rounded-full bg-emerald-500 shrink-0" />
            <span>Kas & Bank:</span>
            <span className="font-bold text-[var(--text)] tabular">{formattedLiquid}</span>
          </div>
          {investmentBalance > 0 && (
            <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-blue-500/10 border border-blue-500/20 text-blue-500">
              <span className="h-2 w-2 rounded-full bg-blue-500 shrink-0" />
              <span>Investasi:</span>
              <span className="font-bold text-blue-600 dark:text-blue-400 tabular">{formattedInvest}</span>
            </div>
          )}
        </div>
      </div>

      {/* Quick Action Capsules with Tactile Spring Physics */}
      <div className="relative z-10 mt-6 pt-5 border-t border-[var(--border)]/80 flex flex-wrap items-center gap-2.5 sm:gap-3">
        <button
          type="button"
          onClick={() => onOpenCapture("expense")}
          className="flex-1 min-w-[120px] inline-flex items-center justify-center gap-2 py-3 px-4 rounded-2xl text-xs font-extrabold bg-rose-500/10 hover:bg-rose-500/20 text-rose-500 border border-rose-500/30 shadow-xs hover:shadow-rose-500/10 transition-all pressable"
        >
          <div className="h-5 w-5 rounded-lg bg-rose-500 text-white flex items-center justify-center text-[10px] shadow-2xs">
            <Icon name="arrow-up-right" className="h-3 w-3 stroke-[3]" />
          </div>
          <span>Pengeluaran</span>
        </button>

        <button
          type="button"
          onClick={() => onOpenCapture("income")}
          className="flex-1 min-w-[120px] inline-flex items-center justify-center gap-2 py-3 px-4 rounded-2xl text-xs font-extrabold bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-500 border border-emerald-500/30 shadow-xs hover:shadow-emerald-500/10 transition-all pressable"
        >
          <div className="h-5 w-5 rounded-lg bg-emerald-500 text-white flex items-center justify-center text-[10px] shadow-2xs">
            <Icon name="arrow-down-left" className="h-3 w-3 stroke-[3]" />
          </div>
          <span>Pemasukan</span>
        </button>

        <button
          type="button"
          onClick={() => (onOpenMovement ? onOpenMovement() : openMovement())}
          className="flex-1 min-w-[120px] inline-flex items-center justify-center gap-2 py-3 px-4 rounded-2xl text-xs font-extrabold bg-indigo-500/10 hover:bg-indigo-500/20 text-indigo-500 border border-indigo-500/30 shadow-xs hover:shadow-indigo-500/10 transition-all pressable"
        >
          <div className="h-5 w-5 rounded-lg bg-indigo-500 text-white flex items-center justify-center text-[10px] shadow-2xs">
            <Icon name="repeat" className="h-3 w-3 stroke-[3]" />
          </div>
          <span>Pindah Saldo</span>
        </button>
      </div>
    </div>
  );
}
