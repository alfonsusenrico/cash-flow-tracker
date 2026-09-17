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
import { StatCard } from "@/components/ui/StatCard";
import { StatusBadge, BadgeVariant } from "@/components/ui/StatusBadge";
import { TakeawayBanner } from "@/components/ui/TakeawayBanner";
import { DetailSheet } from "@/components/ui/DetailSheet";
import { SegmentedProgressTrack, ProgressSegment } from "@/components/ui/SegmentedProgressTrack";
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

  // Transfer form state
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
  });

  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const accounts = accountsData?.accounts ?? [];

  const handleOpenTransfer = (sourceId?: string) => {
    if (accounts.length < 2) {
      alert("Anda membutuhkan minimal 2 rekening untuk melakukan pindah saldo.");
      return;
    }
    const from = sourceId || accounts[0].id;
    const to = accounts.find((a) => a.id !== from)?.id || accounts[1].id;
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

      return api.post("/movements", {
        source_account_id: transferFrom,
        target_account_id: transferTo,
        amount: amt,
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
      qc.invalidateQueries({ queryKey: ["transactions"] });
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      setSelectedTx(null);
    },
  });

  if (isLoading) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-10 w-72 rounded-xl bg-[var(--canvas-subtle)]" />
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-28 rounded-2xl bg-[var(--canvas-subtle)]" />
          ))}
        </div>
        <div className="h-16 rounded-2xl bg-[var(--canvas-subtle)]" />
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-8 h-96 rounded-2xl bg-[var(--canvas-subtle)]" />
          <div className="lg:col-span-4 h-96 rounded-2xl bg-[var(--canvas-subtle)]" />
        </div>
      </div>
    );
  }

  const kpis = dashboard?.kpis;
  const accountsLiquidity = dashboard?.accounts_liquidity ?? [];
  const goalsGlance = dashboard?.goals_glance ?? [];
  const recentTxs = dashboard?.recent_transactions ?? [];
  const timeframeLabel = dashboard?.timeframe_label || "Siklus Aktif";
  const kakeibo = dashboard?.kakeibo;
  const narrative = dashboard?.narrative;

  // Time-based greeting
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

  // Safe fuel status pill
  const safeDaily = kpis?.safe_to_spend_today ?? 0;
  const safeStatus: { text: string; variant: BadgeVariant } =
    kpis?.spend_velocity_status === "fast"
      ? { text: "Waspada Laju Cepat", variant: "warning" }
      : kpis?.spend_velocity_status === "frugal"
      ? { text: "Sangat Hemat", variant: "success" }
      : { text: "Terkendali", variant: "success" };

  // Kakeibo segments
  const kakeiboSegments: ProgressSegment[] = kakeibo
    ? [
        {
          label: "Kebutuhan (Need)",
          amount: kakeibo.need_spent,
          percentage: kakeibo.need_pct || 0,
          color: "#0284C7",
        },
        {
          label: "Keinginan (Want)",
          amount: kakeibo.want_spent,
          percentage: kakeibo.want_pct || 0,
          color: "#DC2626",
        },
        {
          label: "Tabungan (Saving)",
          amount: kakeibo.saving_spent,
          percentage: kakeibo.saving_pct || 0,
          color: "#66CC55",
        },
      ]
    : [];

  return (
    <div className="w-full space-y-5 select-none">
      {/* 0. Pending Scheduled Banner */}
      <PendingScheduledBanner />

      {/* 1. Viewport Title & Action Strip */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl sm:text-2xl font-bold tracking-tight text-[var(--text-primary)]">
              {greetingText}, {displayName} {greetingEmoji}
            </h1>
          </div>
          <div className="flex items-center gap-2 mt-1 text-xs text-[var(--text-muted)] font-medium">
            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-[var(--canvas-subtle)] border border-[var(--border-structural)] text-[11px] font-semibold text-[var(--text-secondary)]">
              <span className="h-1.5 w-1.5 rounded-full bg-[#66CC55]" />
              {timeframeLabel}
            </span>
            <span>•</span>
            <span>Arus kas harian tersinkronisasi</span>
          </div>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <button
            type="button"
            onClick={() => setPayrollModalOpen(true)}
            className="btn-secondary text-xs rounded-xl cursor-pointer"
          >
            <Icon name="calendar" className="h-3.5 w-3.5 text-sky-500" />
            <span>Alokasi Gaji</span>
          </button>

          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="btn-secondary text-xs rounded-xl cursor-pointer"
          >
            <Icon name="clock" className="h-3.5 w-3.5 text-[var(--text-muted)]" />
            <span>Aturan Rutin</span>
          </button>

          <Link
            href="/insights"
            className="btn-secondary text-xs rounded-xl text-emerald-600 dark:text-emerald-400 cursor-pointer"
          >
            <Icon name="analysis" className="h-3.5 w-3.5 text-emerald-500" />
            <span>Analisis &rarr;</span>
          </Link>
        </div>
      </div>

      {/* 2. Top 4-Card KPI Stat Strip (Ref 1 & Ref 4) */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3.5 sm:gap-4">
        {/* KPI 1: Total Likuiditas */}
        <StatCard
          title="Total Likuiditas"
          value={bal(kpis?.total_balance ?? 0)}
          subtitle={`${accounts.length} Rekening aktif • Rp ${((kpis?.investment_balance ?? 0) / 1000000).toFixed(1)}M Investasi`}
          badge={{ text: "Kas & Bank", variant: "info" }}
          icon={<Icon name="wallet" className="h-4 w-4 text-sky-600" />}
          iconBg="bg-sky-500/10"
        />

        {/* KPI 2: Batas Belanja Hari Ini */}
        <StatCard
          title="Batas Belanja Hari Ini"
          value={bal(safeDaily)}
          subtitle={
            kpis?.benchmark_daily
              ? `Benchmark harian: ${bal(kpis.benchmark_daily)}/hari`
              : "Berdasarkan anggaran siklus ini"
          }
          badge={safeStatus}
          icon={<Icon name="zap" className="h-4 w-4 text-amber-600" />}
          iconBg="bg-amber-500/10"
        />

        {/* KPI 3: Pengeluaran Siklus */}
        <StatCard
          title="Pengeluaran Siklus"
          value={bal(kpis?.total_outflow ?? 0)}
          subtitle={`Rata-rata laju: ${bal(Math.round((kpis?.total_outflow ?? 0) / 20))}/hari`}
          badge={{
            text: kpis?.effective_budget ? `${Math.round(((kpis?.total_outflow ?? 0) / kpis.effective_budget) * 100)}% Budget` : "Aktif",
            variant: (kpis?.total_outflow ?? 0) > (kpis?.effective_budget ?? 0) ? "danger" : "neutral",
          }}
          icon={<Icon name="arrow-up-right" className="h-4 w-4 text-rose-600" />}
          iconBg="bg-rose-500/10"
        />

        {/* KPI 4: Arus Kas Bersih */}
        <StatCard
          title="Arus Kas Bersih"
          value={bal(kpis?.net_cashflow ?? 0)}
          subtitle={`Total Masuk: ${bal(kpis?.total_inflow ?? 0)}`}
          badge={{
            text: `Tabungan ${kpis?.savings_rate ?? 0}%`,
            variant: (kpis?.net_cashflow ?? 0) >= 0 ? "success" : "danger",
          }}
          icon={<Icon name="trending-up" className="h-4 w-4 text-emerald-600" />}
          iconBg="bg-emerald-500/10"
        />
      </div>

      {/* 3. Analytical Takeaway Banner (Ref 1 & Ref 7) */}
      <TakeawayBanner
        title="Laporan Otomatis Siklus Ini"
        badgeText="Pemberitahuan Sistem"
        variant="sage"
      >
        {narrative?.summary ? (
          <span>{narrative.summary}</span>
        ) : (
          <span>
            Arus kas periode ini terpantau stabil. Saldo likuid siap dialokasikan untuk kebutuhan pokok dan pos tabungan.
          </span>
        )}
      </TakeawayBanner>

      {/* 4. 2-Column Split: Operations Feed (65%) vs Context Action Rail (35% / 340px) */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-5 items-start">
        {/* ===================== LEFT WORKBENCH (8 cols) ===================== */}
        <div className="lg:col-span-8 space-y-5">
          {/* A. Kakeibo Budget Allocation Track (Ref 1, 7) */}
          <div className="card-crisp p-5 space-y-3.5">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">
                  Alokasi Anggaran Kakeibo
                </h2>
                <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
                  Distribusi belanja: Pokok (50%), Keinginan (30%), Tabungan (20%)
                </p>
              </div>

              <StatusBadge
                variant={
                  kakeibo?.kakeibo_status === "warning"
                    ? "warning"
                    : kakeibo?.kakeibo_status === "elevated_needs"
                    ? "info"
                    : "success"
                }
              >
                {kakeibo?.kakeibo_status === "warning"
                  ? "Keinginan > 30%"
                  : kakeibo?.kakeibo_status === "elevated_needs"
                  ? "Pokok > 50%"
                  : "Sehat"}
              </StatusBadge>
            </div>

            <SegmentedProgressTrack
              segments={kakeiboSegments}
              formatAmount={bal}
              height={10}
            />
          </div>

          {/* B. Aktivitas Terkini (Dense Activity Feed - Ref 1, 6) */}
          <div className="card-crisp p-5 space-y-3">
            <div className="flex items-center justify-between pb-1 border-b border-[var(--border-divider)]">
              <div>
                <h2 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">
                  Aktivitas Terkini
                </h2>
                <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
                  Transaksi masuk, keluar, dan perpindahan dana terbaru
                </p>
              </div>

              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => openQuickAdd("expense")}
                  className="btn-lime px-2.5 py-1 text-[11px] rounded-lg"
                >
                  + Catat
                </button>
                <span className="text-[var(--border-structural)]">•</span>
                <Link
                  href="/ledger"
                  className="text-xs font-semibold text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
                >
                  Semua &rarr;
                </Link>
              </div>
            </div>

            {recentTxs.length === 0 ? (
              <div className="py-12 text-center text-xs text-[var(--text-muted)]">
                Belum ada transaksi di periode ini. Tekan{" "}
                <kbd className="font-mono bg-[var(--canvas-subtle)] border border-[var(--border-structural)] px-1.5 py-0.5 rounded text-[10px]">
                  N
                </kbd>{" "}
                untuk mencatat transaksi baru.
              </div>
            ) : (
              <div className="divide-y divide-[var(--border-divider)]">
                {recentTxs.slice(0, 8).map((tx: any) => {
                  const isMovement =
                    tx.type === "transfer" ||
                    tx.category_name === "Internal Movement" ||
                    tx.category_name === "Investasi" ||
                    Boolean(tx.is_excluded_from_budget);
                  const isIncome = tx.type === "income" && !isMovement;

                  return (
                    <div
                      key={tx.id}
                      onClick={() => setSelectedTx(tx)}
                      className="py-2.5 flex items-center justify-between hover:bg-[var(--canvas-subtle)] px-2.5 -mx-2.5 rounded-xl transition-colors cursor-pointer group select-none"
                    >
                      <div className="flex items-center gap-3 min-w-0 pr-3">
                        <div
                          className={cn(
                            "h-8 w-8 rounded-xl flex items-center justify-center text-xs font-bold shrink-0",
                            isMovement
                              ? "bg-sky-500/10 text-sky-600 dark:text-sky-400"
                              : isIncome
                              ? "bg-[#E8F8EA] text-[#1E7E34] dark:bg-emerald-950/40 dark:text-emerald-400"
                              : "bg-[#FDE8E8] text-[#DC2626] dark:bg-rose-950/40 dark:text-rose-400"
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
                            className="h-4 w-4 stroke-[2.2]"
                          />
                        </div>

                        <div className="min-w-0">
                          <div className="text-xs font-semibold text-[var(--text-primary)] group-hover:text-emerald-600 dark:group-hover:text-emerald-400 transition-colors truncate">
                            {tx.notes || (isMovement ? "Pindah Saldo" : tx.category_name || "Umum")}
                          </div>
                          <div className="text-[11px] text-[var(--text-muted)] flex items-center gap-1.5 mt-0.5 truncate">
                            <span className="truncate">{tx.account_name}</span>
                            {tx.category_name && !isMovement && (
                              <>
                                <span>•</span>
                                <span className="truncate">{tx.category_name}</span>
                              </>
                            )}
                            {isMovement && (
                              <>
                                <span>•</span>
                                <span className="text-sky-600 font-semibold">Pindah Saldo</span>
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
                              ? "text-sky-600 dark:text-sky-400"
                              : isIncome
                              ? "text-[#1E7E34] dark:text-emerald-400"
                              : "text-[#DC2626] dark:text-rose-400"
                          )}
                        >
                          {isIncome ? "+" : !isMovement && tx.type === "expense" ? "-" : ""}
                          {bal(tx.amount)}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        {/* ===================== RIGHT ACTION RAIL (4 cols / ~340px) ===================== */}
        <div className="lg:col-span-4 space-y-5">
          {/* A. Komposisi Rekening & Dompet (Mini Vault) */}
          <div className="card-crisp p-5 space-y-3.5">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">
                  Rekening & Dompet
                </h2>
                <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
                  Komposisi saldo kas likuid
                </p>
              </div>

              <div className="flex items-center gap-1.5">
                <button
                  type="button"
                  onClick={() => handleOpenTransfer()}
                  className="btn-secondary px-2.5 py-1 text-[11px] rounded-lg text-sky-600 cursor-pointer"
                >
                  Pindah
                </button>
                <Link
                  href="/accounts"
                  className="text-xs font-semibold text-[var(--text-muted)] hover:text-[var(--text-primary)]"
                >
                  &rarr;
                </Link>
              </div>
            </div>

            <div className="space-y-2 max-h-[320px] overflow-y-auto pr-0.5">
              {accountsLiquidity.map((acc: any) => {
                const isInvest = acc.type === "investment" || !!acc.instrument_type;
                return (
                  <div
                    key={acc.id}
                    className="p-2.5 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-app)] hover:bg-[var(--canvas-subtle)] transition-colors space-y-1.5"
                  >
                    <div className="flex items-center justify-between text-xs">
                      <div className="flex items-center gap-2 truncate">
                        <div
                          className="h-6 w-6 rounded-lg flex items-center justify-center text-[10px] font-bold text-white shrink-0 shadow-xs"
                          style={{
                            backgroundColor: acc.color || (isInvest ? "#0284C7" : "#1E7E34"),
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
                        <span className="font-semibold text-[var(--text-primary)] truncate">
                          {acc.name}
                        </span>
                        {isInvest && (
                          <span className="text-[9px] px-1.5 py-0.2 rounded-full bg-sky-500/10 text-sky-600 font-bold">
                            Invest
                          </span>
                        )}
                      </div>
                      <span className="font-bold tabular text-[var(--text-primary)] ml-2 shrink-0">
                        {bal(acc.balance)}
                      </span>
                    </div>

                    <div className="w-full h-1.5 rounded-full bg-[var(--canvas-subtle)] overflow-hidden">
                      <div
                        style={{
                          width: `${Math.min(acc.percentage, 100)}%`,
                          backgroundColor: acc.color || (isInvest ? "#0284C7" : "#1E7E34"),
                        }}
                        className="h-full rounded-full transition-all duration-300"
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* B. Target Tabungan (Mini Goals Glance) */}
          <div className="card-crisp p-5 space-y-3">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">
                  Target Tabungan
                </h2>
                <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
                  Progres dana darurat & impian
                </p>
              </div>

              <Link
                href="/goals"
                className="text-xs font-semibold text-emerald-600 dark:text-emerald-400 hover:underline"
              >
                Kelola &rarr;
              </Link>
            </div>

            {goalsGlance.length === 0 ? (
              <div className="py-6 text-center text-xs text-[var(--text-muted)]">
                Belum ada target tabungan aktif.
              </div>
            ) : (
              <div className="space-y-2">
                {goalsGlance.slice(0, 3).map((g: any) => (
                  <div
                    key={g.id}
                    className="p-2.5 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-app)] space-y-1.5"
                  >
                    <div className="flex items-center justify-between text-xs">
                      <span className="font-semibold text-[var(--text-primary)] truncate">
                        {g.name}
                      </span>
                      <span className="text-[11px] font-bold text-emerald-600 dark:text-emerald-400">
                        {g.percentage}%
                      </span>
                    </div>
                    <div className="w-full h-1.5 rounded-full bg-[var(--canvas-subtle)] overflow-hidden">
                      <div
                        style={{
                          width: `${Math.min(g.percentage, 100)}%`,
                          backgroundColor: g.color || "#1E7E34",
                        }}
                        className="h-full rounded-full transition-all duration-300"
                      />
                    </div>
                    <div className="flex items-center justify-between text-[10px] text-[var(--text-muted)]">
                      <span>{bal(g.current_amount)}</span>
                      <span>Target: {bal(g.target_amount)}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ===================== SLIDE-OUT DETAIL SHEET (Ref 6) ===================== */}
      <DetailSheet
        open={Boolean(selectedTx)}
        onClose={() => setSelectedTx(null)}
        title="Detail Transaksi"
        subtitle={
          selectedTx
            ? new Date(selectedTx.date).toLocaleString("id-ID", {
                dateStyle: "medium",
                timeStyle: "short",
              })
            : ""
        }
        footer={
          selectedTx && (
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setSelectedTx(null)}
                className="btn-secondary flex-1 py-2 rounded-xl text-xs"
              >
                Tutup
              </button>
              <button
                type="button"
                disabled={deleteTxMutation.isPending}
                onClick={() => {
                  if (confirm("Hapus transaksi ini secara permanen?")) {
                    deleteTxMutation.mutate(selectedTx.id);
                  }
                }}
                className="px-3.5 py-2 rounded-xl bg-rose-500/10 hover:bg-rose-500/20 text-rose-600 font-semibold text-xs transition-colors cursor-pointer"
              >
                {deleteTxMutation.isPending ? "Menghapus..." : "Hapus"}
              </button>
            </div>
          )
        }
      >
        {selectedTx && (
          <div className="space-y-4 text-xs">
            {/* Amount Banner */}
            <div className="p-4 rounded-2xl bg-[var(--canvas-app)] border border-[var(--border-structural)] flex items-center justify-between">
              <div>
                <span className="text-[10px] font-bold uppercase tracking-wider text-[var(--text-muted)] block">
                  Nominal Transaksi
                </span>
                <span className="text-xl font-black tabular tracking-tight text-[var(--text-primary)] mt-0.5 block">
                  {bal(selectedTx.amount)}
                </span>
              </div>
              <StatusBadge
                variant={
                  selectedTx.type === "transfer" ||
                  selectedTx.category_name === "Internal Movement" ||
                  selectedTx.category_name === "Investasi" ||
                  selectedTx.is_excluded_from_budget
                    ? "info"
                    : selectedTx.type === "income"
                    ? "success"
                    : "danger"
                }
              >
                {selectedTx.type === "transfer" ||
                selectedTx.category_name === "Internal Movement" ||
                selectedTx.category_name === "Investasi" ||
                selectedTx.is_excluded_from_budget
                  ? "Pindah Saldo"
                  : selectedTx.type === "income"
                  ? "Uang Masuk"
                  : "Uang Keluar"}
              </StatusBadge>
            </div>

            {/* Metadata List */}
            <div className="divide-y divide-[var(--border-divider)] border border-[var(--border-structural)] rounded-2xl bg-[var(--canvas-card)] px-4">
              <div className="py-2.5 flex justify-between items-center">
                <span className="text-[var(--text-muted)]">Keterangan / Catatan</span>
                <span className="font-semibold text-[var(--text-primary)] text-right">
                  {selectedTx.notes || "-"}
                </span>
              </div>
              <div className="py-2.5 flex justify-between items-center">
                <span className="text-[var(--text-muted)]">Rekening</span>
                <span className="font-semibold text-[var(--text-primary)]">
                  {selectedTx.account_name}
                </span>
              </div>
              <div className="py-2.5 flex justify-between items-center">
                <span className="text-[var(--text-muted)]">Kategori</span>
                <span className="font-semibold text-[var(--text-primary)]">
                  {selectedTx.category_name || "Umum"}
                </span>
              </div>
              {selectedTx.kakeibo_type && (
                <div className="py-2.5 flex justify-between items-center">
                  <span className="text-[var(--text-muted)]">Pilar Kakeibo</span>
                  <span className="font-bold uppercase text-[var(--text-primary)]">
                    {selectedTx.kakeibo_type === "need"
                      ? "🍞 Pokok (Need)"
                      : selectedTx.kakeibo_type === "want"
                      ? "👑 Keinginan (Want)"
                      : "💎 Tabungan (Saving)"}
                  </span>
                </div>
              )}
            </div>

            {/* Receipt Preview */}
            {selectedTx.receipt_path && (
              <div className="space-y-1.5">
                <span className="text-[11px] font-bold text-[var(--text-muted)]">Foto Struk / Bukti</span>
                <div className="rounded-2xl border border-[var(--border-structural)] overflow-hidden bg-[var(--canvas-subtle)] max-h-56 flex items-center justify-center">
                  <img
                    src={`/api/receipts/${selectedTx.receipt_path}`}
                    alt="Bukti Struk"
                    className="object-contain w-full h-full"
                  />
                </div>
              </div>
            )}
          </div>
        )}
      </DetailSheet>

      {/* ===================== TRANSFER MODAL ===================== */}
      <Modal
        open={transferModalOpen}
        onClose={() => setTransferModalOpen(false)}
        title="Pindah Saldo Antar Rekening"
      >
        <div className="space-y-3.5 pt-1 text-xs">
          {transferError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-2.5 text-rose-500">
              {transferError}
            </div>
          )}

          <div>
            <label className="text-[11px] font-medium text-[var(--text-muted)] block mb-1">
              Dari Rekening Asal
            </label>
            <select
              value={transferFrom}
              onChange={(e) => setTransferFrom(e.target.value)}
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3 py-2 text-xs font-semibold text-[var(--text-primary)]"
            >
              <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} />
            </select>
          </div>

          <div>
            <label className="text-[11px] font-medium text-[var(--text-muted)] block mb-1">
              Ke Rekening Tujuan
            </label>
            <select
              value={transferTo}
              onChange={(e) => setTransferTo(e.target.value)}
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3 py-2 text-xs font-semibold text-[var(--text-primary)]"
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
            <label className="text-[11px] font-medium text-[var(--text-muted)] block mb-1">
              Nominal Uang (IDR)
            </label>
            <input
              type="text"
              inputMode="numeric"
              value={transferAmount}
              onChange={(e) => setTransferAmount(formatNumberWithDots(e.target.value))}
              placeholder="0"
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] px-3 py-2 text-xl font-bold tabular text-[var(--text-primary)]"
            />
          </div>

          <div>
            <label className="text-[11px] font-medium text-[var(--text-muted)] block mb-1">
              Catatan (opsional)
            </label>
            <input
              type="text"
              value={transferNotes}
              onChange={(e) => setTransferNotes(e.target.value)}
              placeholder="Contoh: Tarik tunai ATM, Top up Gopay"
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3 py-2 text-xs text-[var(--text-primary)]"
            />
          </div>

          <div className="flex gap-2 pt-2 border-t border-[var(--border-structural)]">
            <button
              type="button"
              onClick={() => setTransferModalOpen(false)}
              className="btn-secondary flex-1 py-2 rounded-xl"
            >
              Batal
            </button>
            <button
              type="button"
              disabled={transferMutation.isPending}
              onClick={() => transferMutation.mutate()}
              className="btn-charcoal flex-1 py-2 rounded-xl"
            >
              {transferMutation.isPending ? "Memproses..." : "Selesaikan Pindah Saldo"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Global Modals */}
      <PayrollAllocationModal
        open={payrollModalOpen}
        onClose={() => setPayrollModalOpen(false)}
      />
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => setRecurringModalOpen(false)}
      />
    </div>
  );
}
