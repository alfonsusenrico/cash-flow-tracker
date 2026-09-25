"use client";

import { useState } from "react";
import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { InternalMovementModal } from "@/components/ui/InternalMovementModal";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { listLiquidAccountChoices } from "@/lib/accountOptions";

import { HeroWalletCard } from "@/components/dashboard/HeroWalletCard";
import { DailyBudgetBar } from "@/components/dashboard/DailyBudgetBar";
import { MetricMatrixGrid } from "@/components/dashboard/MetricMatrixGrid";
import { KakeiboPillarCards } from "@/components/dashboard/KakeiboPillarCards";
import { NarrativeInsightCard } from "@/components/dashboard/NarrativeInsightCard";
import { CategoryDonutChart } from "@/components/dashboard/CategoryDonutChart";
import { MobileHomeView } from "@/components/dashboard/MobileHomeView";
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

  const { data: dashboard, isLoading } = useQuery<any>({
    queryKey: queryKeys.dashboard.overview(timeframe, cycleOffset),
    queryFn: () =>
      api.get(
        `/dashboard/overview?timeframe=${timeframe}&cycle_offset=${cycleOffset}`
      ),
    refetchInterval: 15000,
  });

  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
  });

  const accounts = accountsData?.accounts ?? [];
  const liquidAccounts = listLiquidAccountChoices(accounts).filter((account) => !account.is_archived);
  const canTransfer = liquidAccounts.length >= 2;

  const handleOpenTransfer = (sourceId?: string) => {
    if (!canTransfer) return;
    const from = liquidAccounts.some((account) => account.id === sourceId) ? sourceId! : liquidAccounts[0].id;
    const to = liquidAccounts.find((account) => account.id !== from)!.id;
    setTransferFrom(from);
    setTransferTo(to);
    setTransferModalOpen(true);
  };

  const deleteTxMutation = useMutation({
    mutationFn: (txId: string) => api.del(`/transactions/${txId}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
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
    <div className="w-full">
      {/* 0. Pending Scheduled Due Banner */}
      <div className="mb-4 sm:mb-6">
        <PendingScheduledBanner />
      </div>

      {/* Mobile-Native Experience (Handheld Viewports < 1024px) */}
      <div className="lg:hidden">
        <MobileHomeView
          dashboard={dashboard}
          accounts={accounts}
          onOpenCapture={(type) => openQuickAdd(type)}
          onOpenTransfer={handleOpenTransfer}
          canTransfer={canTransfer}
          onOpenPayroll={() => setPayrollModalOpen(true)}
          onOpenRecurring={() => setRecurringModalOpen(true)}
          onSelectTx={(tx) => setSelectedTx(tx)}
          bal={bal}
        />
      </div>

      {/* Desktop Command Center (>= 1024px) */}
      <div className="hidden lg:block space-y-6 sm:space-y-7">
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
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-xs font-bold text-[var(--text)] pressable shadow-2xs transition-[background-color,transform]"
            title="Alokasi Gaji Bulanan"
          >
            <Icon name="calendar" className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400 stroke-[2.5]" />
            <span>Alokasi Gaji</span>
          </button>

          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-xs font-bold text-[var(--text)] pressable shadow-2xs transition-[background-color,transform]"
            title="Kelola Transaksi Rutin"
          >
            <Icon name="clock" className="h-3.5 w-3.5 text-[var(--muted)] stroke-[2.5]" />
            <span>Aturan Rutin</span>
          </button>

          <Link
            href="/insights"
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-xs font-bold text-[var(--text)] pressable shadow-2xs transition-[background-color,transform] group"
            title="Buka Analisis Keuangan Lengkap"
          >
            <Icon name="analysis" className="h-3.5 w-3.5 text-primary stroke-[2.5]" />
            <span>Analisis Lengkap</span>
            <span className="text-[var(--muted)] text-[10px] transition-transform group-hover:translate-x-0.5">&rarr;</span>
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
            onOpenMovement={() => handleOpenTransfer()}
            canTransfer={canTransfer}
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
                  const isMovement =
                    tx.type === "transfer" ||
                    tx.category_name === "Internal Movement" ||
                    !!tx.is_excluded_from_budget;
                  const isIncome = tx.type === "income" && !isMovement;

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
                            isMovement
                              ? "bg-blue-500/10 text-blue-500"
                              : isIncome
                              ? "bg-emerald-500/10 text-emerald-500"
                              : "bg-rose-500/10 text-rose-500"
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
                            className="h-4 w-4"
                          />
                        </div>
                        <div className="min-w-0">
                          <div className="text-xs font-semibold text-[var(--text)] group-hover:text-emerald-400 transition-colors truncate">
                            {tx.notes ||
                              (isMovement
                                ? "Pindah Saldo"
                                : tx.category_name || "Umum")}
                          </div>
                          <div className="text-[11px] text-[var(--muted)] flex items-center gap-1.5 mt-0.5 truncate">
                            <span className="truncate">{tx.account_name}</span>
                            {tx.transfer_target_account_name && (
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
                            isMovement
                              ? "text-blue-500"
                              : isIncome
                              ? "text-emerald-500"
                              : "text-rose-500"
                          )}
                        >
                          {tx.type === "income" ? "+" : tx.type === "expense" ? "-" : ""}
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
                  disabled={!canTransfer}
                  title={canTransfer ? undefined : "Perlu dua rekening kas aktif untuk transfer"}
                  className="text-xs font-bold text-amber-600 dark:text-amber-400 enabled:hover:underline enabled:cursor-pointer pressable disabled:cursor-not-allowed disabled:opacity-50"
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
                    className="p-2.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/50 hover:bg-[var(--surface-raised)] transition-colors space-y-1.5"
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
                        className="h-full rounded-full transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
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
                          className="h-full rounded-full transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
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
                          className="h-full rounded-full bg-rose-500 transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
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
      </div>

      {/* ===================== MODALS ===================== */}

      <InternalMovementModal
        open={transferModalOpen}
        onClose={() => setTransferModalOpen(false)}
        defaultSourceAccountId={transferFrom}
        defaultTargetAccountId={transferTo}
      />

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
                    (selectedTx.type === "transfer" ||
                    selectedTx.category_name === "Internal Movement" ||
                    selectedTx.is_excluded_from_budget
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
                  selectedTx.type === "transfer" ||
                  selectedTx.category_name === "Internal Movement" ||
                  selectedTx.is_excluded_from_budget
                    ? "text-blue-500"
                    : selectedTx.type === "income"
                    ? "text-emerald-500"
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
                {selectedTx.type === "transfer" ||
                selectedTx.category_name === "Internal Movement" ||
                selectedTx.is_excluded_from_budget
                  ? selectedTx.type === "income"
                    ? "Pindah Saldo (Masuk)"
                    : "Pindah Saldo (Keluar)"
                  : selectedTx.type === "income"
                  ? "Uang Masuk"
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
              <ConfirmActionButton
                label="Hapus transaksi"
                confirmation="Hapus transaksi ini? Saldo akun akan diperbarui."
                onConfirm={() => deleteTxMutation.mutate(selectedTx.id)}
                disabled={deleteTxMutation.isPending}
                className="px-4 py-2 rounded-xl bg-rose-500/10 text-rose-500 border border-rose-500/20 font-semibold hover:bg-rose-500/20 cursor-pointer"
              >
                {deleteTxMutation.isPending ? "Menghapus..." : "Hapus Transaksi"}
              </ConfirmActionButton>
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
