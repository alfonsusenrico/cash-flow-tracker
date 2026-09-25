"use client";

import { useState } from "react";
import Link from "next/link";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";

interface MobileHomeViewProps {
  dashboard: any;
  accounts: any[];
  canTransfer: boolean;
  onOpenCapture: (type?: "expense" | "income") => void;
  onOpenTransfer: (sourceId?: string) => void;
  onOpenPayroll: () => void;
  onOpenRecurring?: () => void;
  onSelectTx: (tx: any) => void;
  bal: (amount: number) => string;
}

export function MobileHomeView({
  dashboard,
  accounts,
  canTransfer,
  onOpenCapture,
  onOpenTransfer,
  onOpenPayroll,
  onSelectTx,
  bal,
}: MobileHomeViewProps) {
  const [balanceMode, setBalanceMode] = useState<"total" | "liquid">("total");

  const kpis = dashboard?.kpis;
  const recentTxs = dashboard?.recent_transactions ?? [];
  const kakeibo = dashboard?.kakeibo;
  const narrative = dashboard?.narrative;
  const accountsLiquidity = dashboard?.accounts_liquidity ?? accounts ?? [];

  // Balances
  const totalBalance = kpis?.total_balance ?? (kpis?.liquid_balance ?? 0);
  const liquidBalance = kpis?.liquid_balance ?? 0;
  const investmentBalance = kpis?.investment_balance ?? 0;
  const displayedBalance = balanceMode === "total" ? totalBalance : liquidBalance;

  // Safe to spend & flow
  const safeToSpend = kpis?.safe_to_spend_today ?? 0;
  const velocityStatus = kpis?.spend_velocity_status ?? "on_track";
  const totalInflow = kpis?.total_inflow ?? 0;
  const totalOutflow = kpis?.total_outflow ?? 0;

  // Kakeibo percentages
  const actualNeeds = kakeibo?.needs?.actual ?? 0;
  const actualWants = kakeibo?.wants?.actual ?? 0;
  const actualSavings = kakeibo?.savings?.actual ?? 0;
  const totalKakeibo = actualNeeds + actualWants + actualSavings || 1;
  const pctNeeds = Math.round((actualNeeds / totalKakeibo) * 100);
  const pctWants = Math.round((actualWants / totalKakeibo) * 100);
  const pctSavings = Math.max(0, 100 - pctNeeds - pctWants);

  // Group recent transactions by date for native mobile list
  const today = new Date();
  const todayStr = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = `${yesterday.getFullYear()}-${String(yesterday.getMonth() + 1).padStart(2, "0")}-${String(yesterday.getDate()).padStart(2, "0")}`;

  return (
    <div className="w-full space-y-6 -mt-1 pb-6">
      {/* ============================================================ */}
      {/* 1. SEAMLESS HERO BALANCE (App Canvas Style, No Enclosing Box) */}
      {/* ============================================================ */}
      <div className="px-1 text-center space-y-3 pt-2">
        {/* Balance Mode Pill Switcher */}
        <div className="inline-flex items-center p-0.5 rounded-full bg-[var(--surface-raised)] border border-[var(--border)] text-[11px] font-bold shadow-2xs">
          <button
            type="button"
            onClick={() => setBalanceMode("total")}
            className={cn(
              "px-3 py-1 rounded-full transition-all",
              balanceMode === "total"
                ? "bg-[var(--surface)] text-[var(--text)] shadow-xs"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            Total Kekayaan
          </button>
          <button
            type="button"
            onClick={() => setBalanceMode("liquid")}
            className={cn(
              "px-3 py-1 rounded-full transition-all",
              balanceMode === "liquid"
                ? "bg-[var(--surface)] text-[var(--text)] shadow-xs"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            Kas Likuid
          </button>
        </div>

        {/* Large Prominent Tabular Balance */}
        <div className="py-1">
          <div className="text-4xl font-black tabular tracking-tight text-[var(--text)] select-all leading-tight">
            {bal(displayedBalance)}
          </div>
          {investmentBalance > 0 && balanceMode === "total" && (
            <div className="flex items-center justify-center gap-2 mt-1.5 text-xs text-[var(--muted)] font-medium">
              <span>Kas: <strong className="text-[var(--text)] tabular">{bal(liquidBalance)}</strong></span>
              <span>•</span>
              <span>Investasi: <strong className="text-sky-500 tabular">{bal(investmentBalance)}</strong></span>
            </div>
          )}
        </div>

        {/* Monthly Flow Subtitle */}
        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-[var(--surface-raised)]/60 text-xs font-semibold tabular text-[var(--muted)]">
          <span className="text-emerald-500">+{bal(totalInflow)}</span>
          <span>•</span>
          <span className="text-rose-500">-{bal(totalOutflow)}</span>
        </div>
      </div>

      {/* ============================================================ */}
      {/* 2. CIRCULAR ACTION BAR (Guaranteed Horizontal Touch Row)     */}
      {/* ============================================================ */}
      <div className="flex flex-row items-center justify-between w-full px-1 py-1">
        {/* 1. Catat Transaksi (Primary Highlight) */}
        <button
          type="button"
          onClick={() => onOpenCapture("expense")}
          className="flex-1 flex flex-col items-center gap-1.5 group cursor-pointer active:scale-95 transition-transform"
        >
          <div className="w-14 h-14 rounded-2xl bg-[#1E201E] text-white flex items-center justify-center shadow-sm border border-white/10 group-hover:bg-[#2A2D2A]">
            <Icon name="plus" className="h-6 w-6 text-[var(--accent-lime,#66CC55)] stroke-[2.5]" />
          </div>
          <span className="text-[11px] font-bold text-[var(--text)]">Catat</span>
        </button>

        {/* 2. Pindah Saldo / Transfer */}
        <button
          type="button"
          onClick={() => onOpenTransfer()}
          disabled={!canTransfer}
          title={canTransfer ? undefined : "Perlu dua rekening kas aktif untuk transfer"}
          className="flex-1 flex flex-col items-center gap-1.5 group cursor-pointer active:scale-95 transition-transform disabled:cursor-not-allowed disabled:opacity-50"
        >
          <div className="w-14 h-14 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] text-[var(--text)] flex items-center justify-center shadow-2xs group-hover:bg-[var(--surface)]">
            <Icon name="repeat" className="h-5 w-5 text-sky-500 stroke-[2.5]" />
          </div>
          <span className="text-[11px] font-bold text-[var(--text)]">Transfer</span>
        </button>

        {/* 3. Alokasi Gaji */}
        <button
          type="button"
          onClick={onOpenPayroll}
          className="flex-1 flex flex-col items-center gap-1.5 group cursor-pointer active:scale-95 transition-transform"
        >
          <div className="w-14 h-14 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] text-[var(--text)] flex items-center justify-center shadow-2xs group-hover:bg-[var(--surface)]">
            <Icon name="allocation" className="h-5 w-5 text-amber-500 stroke-[2.5]" />
          </div>
          <span className="text-[11px] font-bold text-[var(--text)]">Alokasi</span>
        </button>

        {/* 4. Analisis / Insights */}
        <Link
          href="/insights"
          className="flex-1 flex flex-col items-center gap-1.5 group cursor-pointer active:scale-95 transition-transform"
        >
          <div className="w-14 h-14 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] text-[var(--text)] flex items-center justify-center shadow-2xs group-hover:bg-[var(--surface)]">
            <Icon name="analysis" className="h-5 w-5 text-indigo-500 stroke-[2.5]" />
          </div>
          <span className="text-[11px] font-bold text-[var(--text)]">Laporan</span>
        </Link>
      </div>

      {/* ============================================================ */}
      {/* 3. DIGITAL WALLET CARDS CAROUSEL (Apple Wallet Deck Style)   */}
      {/* ============================================================ */}
      <div className="space-y-2 pt-1">
        <div className="flex items-center justify-between px-1">
          <div className="flex items-center gap-1.5">
            <Icon name="wallet" className="h-4 w-4 text-[var(--muted)]" />
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--text)]">
              Dompet & Rekening
            </span>
          </div>
          <Link
            href="/accounts"
            className="text-xs font-semibold text-[var(--accent-lime,#66CC55)] hover:underline"
          >
            Kelola &rarr;
          </Link>
        </div>

        {/* Horizontal Snap Scroll Cards */}
        <div className="flex items-stretch gap-3 overflow-x-auto pb-2 -mx-3 px-3 scrollbar-none snap-x snap-mandatory">
          {accountsLiquidity.map((acc: any) => {
            const isInvest = acc.type === "investment" || !!acc.instrument_type;
            const isCash = acc.type === "cash";
            const isBank = acc.type === "bank";

            // Visual card styles (Apple Wallet inspired gradient skins)
            const cardBg = isInvest
              ? "bg-gradient-to-br from-[#1E1B4B] to-[#312E81] text-white border-indigo-500/30"
              : isBank
              ? "bg-gradient-to-br from-[#0F172A] to-[#1E293B] text-white border-slate-700/40"
              : isCash
              ? "bg-gradient-to-br from-[#064E3B] to-[#047857] text-white border-emerald-500/30"
              : "bg-gradient-to-br from-[#1E201E] to-[#2B2D2B] text-white border-zinc-700/40";

            return (
              <button
                type="button"
                key={acc.id}
                onClick={() => onOpenTransfer(acc.id)}
                disabled={!canTransfer || isInvest || Boolean(acc.is_archived)}
                aria-label={`Transfer dari ${acc.name}`}
                className={cn(
                  "snap-start shrink-0 w-[200px] p-3.5 rounded-2xl border shadow-sm flex flex-col justify-between h-[108px] text-left enabled:cursor-pointer enabled:active:scale-98 transition-transform relative overflow-hidden disabled:cursor-default",
                  cardBg
                )}
              >
                {/* Decorative chip & card type badge */}
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <div className="h-5 w-7 rounded-sm bg-amber-400/20 border border-amber-300/40 flex items-center justify-center">
                      <div className="h-2.5 w-3.5 rounded-xs border border-amber-300/60" />
                    </div>
                    <span className="text-[9px] font-bold uppercase tracking-widest text-white/70">
                      {isInvest ? "INVESTASI" : isCash ? "TUNAI" : isBank ? "DEBIT" : "WALLET"}
                    </span>
                  </div>

                  <span
                    className="h-2 w-2 rounded-full ring-2 ring-white/20"
                    style={{ backgroundColor: acc.color || (isInvest ? "#38bdf8" : "#34d399") }}
                  />
                </div>

                {/* Account Name & Tabular Balance */}
                <div>
                  <div className="text-[11px] font-semibold text-white/80 truncate">
                    {acc.name}
                  </div>
                  <div className="text-base font-black tabular tracking-tight text-white select-all mt-0.5">
                    {bal(acc.balance)}
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* ============================================================ */}
      {/* 4. DAILY SAFE SPEND & KAKEIBO STRIP (Compact Tactical Strip) */}
      {/* ============================================================ */}
      <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-3.5 space-y-2.5 shadow-2xs">
        {/* Top: Safe spend today pill */}
        <div className="flex items-center justify-between">
          <div className="space-y-0.5">
            <span className="text-[10px] font-bold uppercase tracking-wider text-[var(--muted)]">
              Sisa Aman Hari Ini
            </span>
            <div className="text-lg font-black tabular tracking-tight text-[var(--text)]">
              {bal(safeToSpend)}
              <span className="text-[11px] font-normal text-[var(--muted)] ml-1">/ hari</span>
            </div>
          </div>

          <span
            className={cn(
              "px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider border",
              velocityStatus === "on_track"
                ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                : velocityStatus === "warning"
                ? "bg-amber-500/10 text-amber-500 border-amber-500/20"
                : "bg-rose-500/10 text-rose-500 border-rose-500/20"
            )}
          >
            {velocityStatus === "on_track" ? "● Terkendali" : "▲ Waspada"}
          </span>
        </div>

        {/* Bottom: 50/30/20 Kakeibo segmented indicator */}
        <div className="space-y-1 pt-1.5 border-t border-[var(--border)]">
          <div className="flex items-center justify-between text-[10px] font-semibold text-[var(--muted)]">
            <span className="uppercase tracking-wider font-bold">Kakeibo 50/30/20</span>
            <div className="flex items-center gap-2">
              <span className="text-sky-500 font-bold">Need {pctNeeds}%</span>
              <span className="text-rose-400 font-bold">Want {pctWants}%</span>
              <span className="text-emerald-500 font-bold">Save {pctSavings}%</span>
            </div>
          </div>

          <div className="h-1.5 w-full rounded-full bg-[var(--surface-raised)] overflow-hidden flex">
            <div style={{ width: `${pctNeeds}%` }} className="h-full bg-sky-500 transition-all duration-300" />
            <div style={{ width: `${pctWants}%` }} className="h-full bg-rose-400 transition-all duration-300" />
            <div style={{ width: `${pctSavings}%` }} className="h-full bg-emerald-500 transition-all duration-300" />
          </div>
        </div>
      </div>

      {/* ============================================================ */}
      {/* 5. NATIVE MOBILE ACTIVITY FEED (The Core Handheld Experience) */}
      {/* ============================================================ */}
      <div className="space-y-3 pt-1">
        <div className="flex items-center justify-between px-1">
          <h2 className="text-xs font-bold uppercase tracking-wider text-[var(--text)]">
            Aktivitas Terkini
          </h2>
          <Link
            href="/ledger"
            className="text-xs font-semibold text-[var(--accent-lime,#66CC55)] hover:underline"
          >
            Lihat Semua &rarr;
          </Link>
        </div>

        {recentTxs.length === 0 ? (
          <div className="py-12 text-center rounded-3xl border border-[var(--border)] bg-[var(--surface)] text-xs text-[var(--muted)]">
            Belum ada transaksi di siklus ini. Ketuk <strong>+ Catat</strong> di atas untuk mencatat.
          </div>
        ) : (
          <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] divide-y divide-[var(--border)] overflow-hidden shadow-2xs">
            {recentTxs.slice(0, 8).map((tx: any) => {
              const isMovement =
                tx.type === "transfer" ||
                tx.category_name === "Internal Movement" ||
                !!tx.is_excluded_from_budget;
              const isIncome = tx.type === "income" && !isMovement;

              const txDate = new Date(tx.date);
              const dateStr = `${txDate.getFullYear()}-${String(txDate.getMonth() + 1).padStart(2, "0")}-${String(txDate.getDate()).padStart(2, "0")}`;
              const timeFormatted = txDate.toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit" });
              const dateBadge = dateStr === todayStr ? "Hari Ini" : dateStr === yesterdayStr ? "Kemarin" : txDate.toLocaleDateString("id-ID", { day: "numeric", month: "short" });

              return (
                <div
                  key={tx.id}
                  onClick={() => onSelectTx(tx)}
                  className="p-3.5 flex items-center justify-between hover:bg-[var(--surface-raised)]/60 active:bg-[var(--surface-raised)] transition-all cursor-pointer group"
                >
                  {/* Left: 42px Squircle Icon + Merchant & Metadata */}
                  <div className="flex items-center gap-3 min-w-0 pr-3">
                    <div
                      className={cn(
                        "h-10 w-10 rounded-2xl flex items-center justify-center text-xs font-bold shrink-0 shadow-2xs",
                        isMovement
                          ? "bg-sky-500/15 text-sky-500 border border-sky-500/20"
                          : isIncome
                          ? "bg-emerald-500/15 text-emerald-500 border border-emerald-500/20"
                          : "bg-rose-500/15 text-rose-500 border border-rose-500/20"
                      )}
                    >
                      <Icon
                        name={
                          isMovement
                            ? "repeat"
                            : isIncome
                            ? "arrow-down-left"
                            : "arrow-up-right"
                        }
                        className="h-4.5 w-4.5 stroke-[2.5]"
                      />
                    </div>

                    <div className="min-w-0">
                      <div className="text-xs font-bold text-[var(--text)] group-hover:text-emerald-500 transition-colors truncate">
                        {tx.notes || (isMovement ? "Pindah Saldo" : tx.category_name || "Pengeluaran")}
                      </div>

                      <div className="text-[11px] text-[var(--muted)] flex items-center gap-1.5 mt-0.5 truncate">
                        <span className="truncate">{tx.account_name}</span>
                        {tx.transfer_target_account_name && (
                          <>
                            <span>&rarr;</span>
                            <span className="truncate">{tx.transfer_target_account_name}</span>
                          </>
                        )}
                        <span>•</span>
                        <span className="shrink-0">{dateBadge}, {timeFormatted}</span>
                      </div>
                    </div>
                  </div>

                  {/* Right: Tabular Nominal with sign */}
                  <div className="text-right shrink-0">
                    <div
                      className={cn(
                        "text-xs font-extrabold tabular tracking-tight",
                        isMovement
                          ? "text-sky-500"
                          : isIncome
                          ? "text-emerald-500"
                          : "text-rose-500"
                      )}
                    >
                      {isMovement ? "" : isIncome ? "+" : "-"}
                      {bal(tx.amount)}
                    </div>

                    {tx.kakeibo_type && (
                      <span className="text-[9px] font-bold uppercase tracking-wider text-[var(--muted)]">
                        {tx.kakeibo_type}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ============================================================ */}
      {/* 6. COMPACT NARRATIVE SUMMARY                                 */}
      {/* ============================================================ */}
      {narrative && (typeof narrative === "string" ? narrative : narrative.summary) && (
        <div className="p-4 rounded-3xl border border-indigo-500/20 bg-indigo-500/[0.04] space-y-1.5 shadow-2xs">
          <div className="flex items-center justify-between">
            <span className="text-[10px] font-bold text-[var(--muted)] uppercase tracking-wider">
              ✨ Evaluasi Arus Kas
            </span>
            {narrative.trend_status && (
              <span className="text-[9px] font-bold px-2 py-0.5 rounded-full bg-[var(--surface)] border border-[var(--border)]">
                {narrative.trend_status}
              </span>
            )}
          </div>
          <p className="text-xs text-[var(--text)] leading-relaxed font-medium">
            {typeof narrative === "string" ? narrative : narrative.summary}
          </p>
          {narrative.top_driver && (
            <div className="text-[10px] text-[var(--muted)] pt-0.5">
              Pengeluaran Terbesar: <strong className="text-[var(--text)]">{narrative.top_driver}</strong>
              {typeof narrative.top_driver_delta_pct === "number" && (
                <span className="ml-1 text-rose-500 font-bold">
                  ({narrative.top_driver_delta_pct > 0 ? "+" : ""}{narrative.top_driver_delta_pct}%)
                </span>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
