"use client";

import { useState } from "react";
import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, formatNumberWithDots } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";

import { HeroWalletCard } from "@/components/dashboard/HeroWalletCard";
import { DailyBudgetBar } from "@/components/dashboard/DailyBudgetBar";
import { MetricMatrixGrid } from "@/components/dashboard/MetricMatrixGrid";
import { KakeiboPillarCards } from "@/components/dashboard/KakeiboPillarCards";
import { NarrativeInsightCard } from "@/components/dashboard/NarrativeInsightCard";
import { CategoryDonutChart } from "@/components/dashboard/CategoryDonutChart";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { PayrollAllocationModal } from "@/components/recurring/PayrollAllocationModal";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";

export default function OverviewPage() {
  const { timeframe, cycleOffset, openQuickAdd, user, bal } = useAppCtx();
  const qc = useQueryClient();

  const [transferModalOpen, setTransferModalOpen] = useState(false);
  const [payrollModalOpen, setPayrollModalOpen] = useState(false);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);
  const [selectedTx, setSelectedTx] = useState<any>(null);

  // Transfer state
  const [transferFrom, setTransferFrom] = useState("");
  const [transferTo, setTransferTo] = useState("");
  const [transferAmount, setTransferAmount] = useState("");
  const [transferNotes, setTransferNotes] = useState("");
  const [transferError, setTransferError] = useState("");

  const { data: dashboard, isLoading } = useQuery<any>({
    queryKey: ["dashboard-overview", timeframe, cycleOffset],
    queryFn: () =>
      api.get(
        `/dashboard/overview?timeframe=${timeframe}&cycle_offset=${cycleOffset}`
      ),
    refetchInterval: 15000,
  });

  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const accounts = accountsData?.accounts ?? [];

  const handleOpenTransfer = (sourceId?: string) => {
    const allSelectable = accounts;
    if (allSelectable.length < 2) {
      alert("Anda membutuhkan minimal 2 rekening atau kantong untuk melakukan pindah saldo.");
      return;
    }
    const from = sourceId || allSelectable[0].id;
    const to = allSelectable.find((a) => a.id !== from)?.id || allSelectable[1].id;
    setTransferFrom(from);
    setTransferTo(to);
    setTransferAmount("");
    setTransferNotes("");
    setTransferError("");
    setTransferModalOpen(true);
  };

  const transferMutation = useMutation({
    mutationFn: async () => {
      const amt = parseInt(transferAmount.replace(/[^0-9]/g, ""), 10);
      if (!amt || amt <= 0) throw new Error("Masukkan nominal uang yang valid");
      if (!transferFrom || !transferTo) throw new Error("Pilih rekening asal dan tujuan");
      if (transferFrom === transferTo) throw new Error("Rekening asal dan tujuan harus berbeda");

      return api.post("/transactions", {
        type: "transfer",
        amount: amt,
        account_id: transferFrom,
        transfer_target_account_id: transferTo,
        notes: transferNotes.trim() || null,
        date: new Date().toISOString(),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      setTransferModalOpen(false);
    },
    onError: (err: any) => {
      setTransferError(err?.message || "Pindah saldo gagal");
    },
  });

  const deleteTxMutation = useMutation({
    mutationFn: (txId: string) => api.del(`/transactions/${txId}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      setSelectedTx(null);
    },
  });

  if (isLoading) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-14 rounded-2xl bg-[var(--border)]/30" />
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-7 space-y-6">
            <div className="h-44 rounded-2xl bg-[var(--border)]/30" />
            <div className="h-28 rounded-2xl bg-[var(--border)]/30" />
            <div className="grid grid-cols-2 gap-4">
              <div className="h-28 rounded-2xl bg-[var(--border)]/30" />
              <div className="h-28 rounded-2xl bg-[var(--border)]/30" />
            </div>
            <div className="h-72 rounded-2xl bg-[var(--border)]/30" />
          </div>
          <div className="lg:col-span-5 space-y-6">
            <div className="h-32 rounded-2xl bg-[var(--border)]/30" />
            <div className="h-64 rounded-2xl bg-[var(--border)]/30" />
            <div className="h-64 rounded-2xl bg-[var(--border)]/30" />
          </div>
        </div>
      </div>
    );
  }

  const kpis = dashboard?.kpis;
  const categories = dashboard?.categories ?? [];
  const accountsLiquidity = dashboard?.accounts_liquidity ?? [];
  const goalsGlance = dashboard?.goals_glance ?? [];
  const obligationsGlance = dashboard?.obligations_glance ?? [];
  const recentTxs = dashboard?.recent_transactions ?? [];
  const timeframeLabel = dashboard?.timeframe_label;
  const comparison = dashboard?.comparison;
  const kakeibo = dashboard?.kakeibo;
  const narrative = dashboard?.narrative;

  const currentHour = new Date().getHours();
  const greetingText =
    currentHour < 11
      ? "Selamat Pagi"
      : currentHour < 15
      ? "Selamat Siang"
      : currentHour < 18
      ? "Selamat Sore"
      : "Selamat Malam";
  const greetingEmoji =
    currentHour < 11 ? "☀️" : currentHour < 15 ? "🌤️" : currentHour < 18 ? "🌇" : "🌙";
  const displayName = user?.username
    ? user.username.charAt(0).toUpperCase() + user.username.slice(1)
    : "Enrico";

  return (
    <div className="w-full space-y-6 sm:space-y-7">
      {/* 0. Pending Scheduled Due Banner */}
      <PendingScheduledBanner />

      {/* 1. Welcoming Consumer Greeting & Quick Actions */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-1">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl sm:text-2xl font-black tracking-tight text-[var(--text)]">
              {greetingText}, {displayName}! {greetingEmoji}
            </h1>
          </div>
          <p className="text-xs text-[var(--muted)] font-medium mt-0.5">
            Siklus <span className="font-bold text-[var(--text)]">{timeframeLabel}</span> • Keuanganmu aman terkendali ✨
          </p>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <button
            type="button"
            onClick={() => setPayrollModalOpen(true)}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-blue-500/25 bg-blue-500/10 hover:bg-blue-500/20 text-xs font-bold text-blue-500 pressable shadow-2xs"
          >
            <Icon name="calendar" className="h-3.5 w-3.5 text-blue-500 stroke-[2.5]" />
            <span>Alokasi Gaji</span>
          </button>

          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)] hover:bg-[var(--surface-raised)]/80 text-xs font-bold text-[var(--text)] pressable shadow-2xs"
          >
            <Icon name="clock" className="h-3.5 w-3.5 text-[var(--muted)] stroke-[2.5]" />
            <span>Aturan Rutin</span>
          </button>

          <Link
            href="/insights"
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-indigo-500/25 bg-indigo-500/10 hover:bg-indigo-500/20 text-xs font-bold text-indigo-500 pressable shadow-2xs"
          >
            <Icon name="analysis" className="h-3.5 w-3.5 text-indigo-500 stroke-[2.5]" />
            <span>Analisis Lengkap</span>
          </Link>
        </div>
      </div>

      {/* 2. Top Hero & Financial Intelligence Zone (Immediate Above-the-Fold Pulse) */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-5 sm:gap-6 items-stretch">
        {/* Left: Total Likuiditas & Vault + Action Buttons */}
        <div className="lg:col-span-7 flex flex-col justify-between">
          <HeroWalletCard
            totalBalance={kpis?.total_balance ?? (kpis?.liquid_balance ?? 0)}
            liquidBalance={kpis?.liquid_balance ?? 0}
            investmentBalance={kpis?.investment_balance ?? 0}
            accountsCount={accounts.length}
            onOpenCapture={(type) => openQuickAdd(type)}
          />
        </div>

        {/* Right: Daily Safe Fuel & Immediate Narrative Insight */}
        <div className="lg:col-span-5 flex flex-col justify-between gap-4 sm:gap-5">
          <DailyBudgetBar
            safeToSpendToday={kpis?.safe_to_spend_today ?? 0}
            hasBudget={Boolean(kpis?.benchmark_daily && kpis?.benchmark_daily > 0)}
            effectiveBudget={kpis?.effective_budget ?? 0}
            totalOutflow={kpis?.total_outflow ?? 0}
            upcomingRecurring={kpis?.upcoming_recurring_committed ?? 0}
            spendVelocityRatio={kpis?.spend_velocity_ratio ?? 1.0}
            spendVelocityStatus={kpis?.spend_velocity_status ?? "on_track"}
            benchmarkDaily={kpis?.benchmark_daily ?? 0}
          />

          <NarrativeInsightCard
            narrative={narrative}
            outflowDeltaPct={comparison?.outflow_delta_pct ?? 0}
            timeframeLabel={timeframeLabel}
          />
        </div>
      </div>

      {/* 4. Mindful 3-Pillar Kakeibo Allocation (50/30/20) */}
      <KakeiboPillarCards kakeibo={kakeibo} />

      {/* 5. 4-Pillar Financial Metrics Ribbon (Full Width) */}
      <MetricMatrixGrid
        totalInflow={kpis?.total_inflow ?? 0}
        totalOutflow={kpis?.total_outflow ?? 0}
        netCashflow={kpis?.net_cashflow ?? 0}
        savingsRate={kpis?.savings_rate ?? 0}
        comparison={comparison}
        kakeibo={kakeibo}
      />

      {/* 6. Balanced 2-Column Core: Activity & Vault (Left) vs Categories & Targets (Right) */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5 sm:gap-6 items-start pt-1">
        {/* ===================== COLUMN 1 (OPERATIONS & VAULT) ===================== */}
        <div className="space-y-5 sm:space-y-6">
          {/* A. Recent Transactions Timeline */}
          <div className="card-squircle p-5 sm:p-6 space-y-4 shadow-xs">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm sm:text-base font-bold tracking-tight text-[var(--text)]">
                  Aktivitas Terkini
                </h2>
                <p className="text-xs text-[var(--muted)] mt-0.5">
                  Arus uang keluar masuk terbaru
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => openQuickAdd("expense")}
                  className="text-xs font-bold text-emerald-500 hover:text-emerald-400 cursor-pointer pressable"
                >
                  + Catat
                </button>
                <span className="text-[var(--border)]">•</span>
                <Link
                  href="/ledger"
                  className="text-xs font-semibold text-[var(--muted)] hover:text-[var(--text)]"
                >
                  Semua &rarr;
                </Link>
              </div>
            </div>

            {recentTxs.length === 0 ? (
              <div className="py-12 text-center text-xs text-[var(--muted)]">
                Belum ada transaksi di periode ini. Tekan{" "}
                <kbd className="font-mono bg-[var(--surface-raised)] border border-[var(--border)] px-1.5 py-0.5 rounded text-[10px]">
                  N
                </kbd>{" "}
                untuk mencatat transaksi baru.
              </div>
            ) : (
              <div className="divide-y divide-[var(--border)]">
                {recentTxs.slice(0, 7).map((tx: any) => {
                  const isIncome = tx.type === "income";
                  const isTransfer = tx.type === "transfer";
                  const isExpense = tx.type === "expense";

                  return (
                    <div
                      key={tx.id}
                      onClick={() => setSelectedTx(tx)}
                      className="py-3 flex items-center justify-between hover:bg-[var(--surface-raised)]/50 px-2.5 -mx-2.5 rounded-xl transition-colors cursor-pointer group"
                    >
                      <div className="flex items-center gap-3 min-w-0 pr-3">
                        <div
                          className={cn(
                            "h-8 w-8 rounded-xl flex items-center justify-center text-xs font-bold shrink-0",
                            isIncome
                              ? "bg-emerald-500/10 text-emerald-500"
                              : isTransfer
                              ? "bg-blue-500/10 text-blue-500"
                              : "bg-rose-500/10 text-rose-500"
                          )}
                        >
                          <Icon
                            name={
                              isIncome
                                ? "arrow-down-left"
                                : isTransfer
                                ? "repeat"
                                : "arrow-up-right"
                            }
                            className="h-4 w-4"
                          />
                        </div>
                        <div className="min-w-0">
                          <div className="text-xs font-semibold text-[var(--text)] group-hover:text-emerald-400 transition-colors truncate">
                            {tx.notes ||
                              (isTransfer
                                ? "Pindah Saldo"
                                : tx.category_name || "Umum")}
                          </div>
                          <div className="text-[11px] text-[var(--muted)] flex items-center gap-1.5 mt-0.5 truncate">
                            <span className="truncate">{tx.account_name}</span>
                            {isTransfer && tx.transfer_target_account_name && (
                              <>
                                <span>&rarr;</span>
                                <span className="truncate">
                                  {tx.transfer_target_account_name}
                                </span>
                              </>
                            )}
                            {tx.kakeibo_type && (
                              <>
                                <span>•</span>
                                <span className="uppercase text-[9px] font-bold text-[var(--muted)]">
                                  {tx.kakeibo_type}
                                </span>
                              </>
                            )}
                            <span>•</span>
                            <span className="shrink-0">
                              {new Date(tx.date).toLocaleDateString("id-ID", {
                                month: "short",
                                day: "numeric",
                              })}
                            </span>
                          </div>
                        </div>
                      </div>

                      <div className="text-right shrink-0">
                        <div
                          className={cn(
                            "text-xs font-bold tabular tracking-tight",
                            isIncome
                              ? "text-emerald-500"
                              : isTransfer
                              ? "text-blue-500"
                              : "text-rose-500"
                          )}
                        >
                          {isIncome ? "+" : isExpense ? "-" : ""}
                          {bal(tx.amount)}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* B. Accounts & Pockets Breakdown */}
          <div className="card-squircle p-5 sm:p-6 space-y-4 shadow-xs">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm sm:text-base font-bold tracking-tight text-[var(--text)]">
                  Komposisi Dompet & Rekening
                </h2>
                <p className="text-xs text-[var(--muted)] mt-0.5">
                  Alokasi kas likuid & instrumen
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => handleOpenTransfer()}
                  className="text-xs font-bold text-blue-500 hover:text-blue-400 cursor-pointer pressable"
                >
                  Pindah Saldo
                </button>
                <span className="text-[var(--border)]">•</span>
                <Link
                  href="/accounts"
                  className="text-xs font-bold text-emerald-500 hover:underline pressable"
                >
                  Kelola &rarr;
                </Link>
              </div>
            </div>

            {/* Individual Accounts List */}
            <div className="space-y-2 max-h-[380px] overflow-y-auto pr-0.5">
              {accountsLiquidity.map((acc: any) => {
                const isInvest = acc.type === "investment" || !!acc.instrument_type;
                return (
                  <div
                    key={acc.id}
                    className="p-2.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/50 hover:bg-[var(--surface-raised)] transition-all space-y-1.5"
                  >
                    <div className="flex items-center justify-between text-xs">
                      <div className="flex items-center gap-2 truncate">
                        <div
                          className="h-6 w-6 rounded-lg flex items-center justify-center text-[10px] font-bold text-white shrink-0 shadow-2xs"
                          style={{
                            backgroundColor:
                              acc.color || (isInvest ? "#3b82f6" : "#10b981"),
                          }}
                        >
                          <Icon
                            name={
                              isInvest
                                ? "trending-up"
                                : acc.type === "wallet"
                                ? "wallet"
                                : acc.type === "cash"
                                ? "ledger"
                                : "credit-card"
                            }
                            className="h-3.5 w-3.5 text-white"
                          />
                        </div>
                        <span className="font-semibold text-[var(--text)] truncate">
                          {acc.name}
                        </span>
                        {isInvest && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-blue-500/10 text-blue-500 font-bold">
                            Invest
                          </span>
                        )}
                      </div>
                      <span className="font-bold tabular text-[var(--text)] ml-2 shrink-0">
                        {bal(acc.balance)}
                      </span>
                    </div>

                    {/* Proportion bar */}
                    <div className="w-full h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden">
                      <div
                        style={{
                          width: `${Math.min(acc.percentage, 100)}%`,
                          backgroundColor:
                            acc.color || (isInvest ? "#3b82f6" : "#10b981"),
                        }}
                        className="h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* ===================== COLUMN 2 (ANALYTICS & TARGETS) ===================== */}
        <div className="space-y-5 sm:space-y-6">
          {/* A. Category Donut Chart with dynamic center stat & Kakeibo chips */}
          <CategoryDonutChart
            categories={categories}
            title="Pengeluaran per Kategori"
            subtitle="Kategori belanja terbesar siklus ini"
          />

          {/* B. Goals & Obligations Glance */}
          <div className="grid grid-cols-1 gap-4">
            {/* Savings Goals */}
            <div className="card-squircle p-4 sm:p-5 space-y-3 shadow-xs">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Icon name="goals" className="h-4 w-4 text-emerald-500 stroke-[2.5]" />
                  <h3 className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
                    Target Tabungan
                  </h3>
                </div>
                <Link
                  href="/goals"
                  className="text-xs font-bold text-emerald-500 hover:underline pressable"
                >
                  Lihat &rarr;
                </Link>
              </div>

              {goalsGlance.length === 0 ? (
                <div className="py-4 text-center text-xs text-[var(--muted)]">
                  Belum ada target tabungan aktif.
                </div>
              ) : (
                <div className="space-y-2.5">
                  {goalsGlance.slice(0, 2).map((g: any) => (
                    <div
                      key={g.id}
                      className="p-2.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/50 space-y-1.5"
                    >
                      <div className="flex items-center justify-between text-xs">
                        <span className="font-semibold text-[var(--text)] truncate">
                          {g.name}
                        </span>
                        <span className="text-[11px] font-bold text-emerald-500">
                          {g.percentage}%
                        </span>
                      </div>
                      <div className="w-full h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden">
                        <div
                          style={{
                            width: `${Math.min(g.percentage, 100)}%`,
                            backgroundColor: g.color || "#10b981",
                          }}
                          className="h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
                        />
                      </div>
                      <div className="flex items-center justify-between text-[10px] text-[var(--muted)]">
                        <span>{bal(g.current_amount)} terkumpul</span>
                        <span>Target: {bal(g.target_amount)}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Obligations */}
            <div className="card-squircle p-4 sm:p-5 space-y-3 shadow-xs">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Icon name="obligations" className="h-4 w-4 text-rose-500 stroke-[2.5]" />
                  <h3 className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
                    Tagihan & Cicilan
                  </h3>
                </div>
                <Link
                  href="/goals"
                  className="text-xs font-bold text-rose-500 hover:underline pressable"
                >
                  Lihat &rarr;
                </Link>
              </div>

              {obligationsGlance.length === 0 ? (
                <div className="py-4 text-center text-xs text-[var(--muted)]">
                  Bebas dari cicilan atau tanggungan utang!
                </div>
              ) : (
                <div className="space-y-2.5">
                  {obligationsGlance.slice(0, 2).map((o: any) => (
                    <div
                      key={o.id}
                      className="p-2.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/50 space-y-1.5"
                    >
                      <div className="flex items-center justify-between text-xs">
                        <span className="font-semibold text-[var(--text)] truncate">
                          {o.name}
                        </span>
                        <span className="text-[11px] font-bold text-rose-500">
                          {bal(o.remaining_amount)}
                        </span>
                      </div>
                      <div className="w-full h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden">
                        <div
                          style={{
                            width: `${Math.min(o.payoff_percentage, 100)}%`,
                          }}
                          className="h-full rounded-full bg-rose-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
                        />
                      </div>
                      <div className="flex items-center justify-between text-[10px] text-[var(--muted)]">
                        <span>{o.payoff_percentage}% terbayar</span>
                        <span>Total: {bal(o.total_amount)}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* ===================== MODALS ===================== */}

      {/* Transfer Modal */}
      <Modal
        open={transferModalOpen}
        onClose={() => setTransferModalOpen(false)}
        title="Pindah Saldo Antar Rekening"
      >
        <div className="space-y-4 pt-2">
          {transferError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-xs text-rose-500">
              {transferError}
            </div>
          )}

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">
              Dari Rekening / Dompet
            </label>
            <select
              value={transferFrom}
              onChange={(e) => setTransferFrom(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            >
              <AccountSelectOptions
                accounts={accounts}
                formatBalance={bal}
                allowParentSelection={true}
              />
            </select>
          </div>

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">
              Ke Rekening Tujuan
            </label>
            <select
              value={transferTo}
              onChange={(e) => setTransferTo(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            >
              <AccountSelectOptions
                accounts={accounts}
                formatBalance={bal}
                allowParentSelection={true}
                excludeAccountId={transferFrom}
              />
            </select>
          </div>

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">
              Nominal Uang (IDR)
            </label>
            <input
              type="text"
              inputMode="numeric"
              value={transferAmount}
              onChange={(e) =>
                setTransferAmount(formatNumberWithDots(e.target.value))
              }
              placeholder="Contoh: 500.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
            />
          </div>

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">
              Catatan (opsional)
            </label>
            <input
              type="text"
              value={transferNotes}
              onChange={(e) => setTransferNotes(e.target.value)}
              placeholder="Contoh: Tarik tunai atau top-up e-wallet"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setTransferModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="button"
              disabled={transferMutation.isPending}
              onClick={() => transferMutation.mutate()}
              className="flex-1 rounded-xl bg-blue-500 text-white py-2 text-xs font-semibold hover:bg-blue-600 disabled:opacity-50"
            >
              {transferMutation.isPending ? "Memproses..." : "Selesaikan Transfer"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Transaction Detail & Delete Modal */}
      {selectedTx && (
        <Modal
          open={Boolean(selectedTx)}
          onClose={() => setSelectedTx(null)}
          title="Detail Transaksi"
        >
          <div className="space-y-4 pt-2 text-xs">
            <div className="p-4 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] flex items-center justify-between">
              <div>
                <div className="font-semibold text-sm text-[var(--text)]">
                  {selectedTx.notes ||
                    (selectedTx.type === "transfer"
                      ? "Pindah Saldo"
                      : selectedTx.category_name || "Umum")}
                </div>
                <div className="text-[var(--muted)] mt-0.5">
                  {new Date(selectedTx.date).toLocaleString("id-ID")}
                </div>
              </div>
              <div
                className={cn(
                  "text-base font-bold tabular",
                  selectedTx.type === "income"
                    ? "text-emerald-500"
                    : selectedTx.type === "transfer"
                    ? "text-blue-500"
                    : "text-rose-500"
                )}
              >
                {selectedTx.type === "income"
                  ? "+"
                  : selectedTx.type === "expense"
                  ? "-"
                  : ""}
                {bal(selectedTx.amount)}
              </div>
            </div>

            <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
              <span className="text-[var(--muted)]">Rekening</span>
              <span className="font-semibold text-[var(--text)]">
                {selectedTx.account_name}
              </span>
            </div>

            {selectedTx.transfer_target_account_name && (
              <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
                <span className="text-[var(--muted)]">Rekening Tujuan</span>
                <span className="font-semibold text-[var(--text)]">
                  {selectedTx.transfer_target_account_name}
                </span>
              </div>
            )}

            {selectedTx.kakeibo_type && (
              <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
                <span className="text-[var(--muted)]">Pilar Kakeibo</span>
                <span className="font-semibold uppercase text-[var(--text)]">
                  {selectedTx.kakeibo_type === "need"
                    ? "🍞 Kebutuhan (Need)"
                    : selectedTx.kakeibo_type === "want"
                    ? "👑 Keinginan (Want)"
                    : "💰 Tabungan (Saving)"}
                </span>
              </div>
            )}

            <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
              <span className="text-[var(--muted)]">Jenis Transaksi</span>
              <span className="font-semibold uppercase text-[var(--text)]">
                {selectedTx.type === "income"
                  ? "Uang Masuk"
                  : selectedTx.type === "transfer"
                  ? "Pindah Saldo"
                  : "Uang Keluar"}
              </span>
            </div>

            <div className="pt-4 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setSelectedTx(null)}
                className="px-4 py-2 rounded-xl border border-[var(--border)] text-[var(--muted)] cursor-pointer"
              >
                Tutup
              </button>
              <button
                type="button"
                onClick={() => {
                  if (confirm("Hapus transaksi ini?")) {
                    deleteTxMutation.mutate(selectedTx.id);
                  }
                }}
                disabled={deleteTxMutation.isPending}
                className="px-4 py-2 rounded-xl bg-rose-500/10 text-rose-500 border border-rose-500/20 font-semibold hover:bg-rose-500/20 cursor-pointer"
              >
                {deleteTxMutation.isPending ? "Menghapus..." : "Hapus Transaksi"}
              </button>
            </div>
          </div>
        </Modal>
      )}

      {/* Payroll Allocation Modal */}
      <PayrollAllocationModal
        open={payrollModalOpen}
        onClose={() => setPayrollModalOpen(false)}
        onOpenRulesManager={() => {
          setPayrollModalOpen(false);
          setRecurringModalOpen(true);
        }}
      />

      {/* Recurring Rules Management Modal */}
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => setRecurringModalOpen(false)}
      />
    </div>
  );
}
