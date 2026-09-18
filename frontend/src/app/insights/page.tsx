"use client";

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, formatNumberWithDots } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { BurnCadenceChart } from "@/components/dashboard/BurnCadenceChart";
import { SpendingHeatmap } from "@/components/dashboard/SpendingHeatmap";
import { NarrativeInsightCard } from "@/components/dashboard/NarrativeInsightCard";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";

const CATEGORY_ICONS = [
  { id: "tag", label: "Label" },
  { id: "utensils", label: "Makanan" },
  { id: "shopping-cart", label: "Belanja" },
  { id: "shopping-bag", label: "Mall" },
  { id: "car", label: "Transportasi" },
  { id: "zap", label: "Tagihan" },
  { id: "heart", label: "Kesehatan" },
  { id: "film", label: "Hiburan" },
  { id: "wallet", label: "Dompet" },
  { id: "credit-card", label: "Kartu" },
];

const CATEGORY_COLORS = [
  "#f97316", // orange
  "#10b981", // emerald
  "#3b82f6", // blue
  "#8b5cf6", // purple
  "#ec4899", // pink
  "#ef4444", // rose
  "#eab308", // amber
  "#14b8a6", // teal
  "#64748b", // slate
];

export default function AnalyticsPage() {
  const qc = useQueryClient();
  const { timeframe, cycleOffset, bal } = useAppCtx();

  // Active view tab: summary vs compare/kakeibo
  const [activeTab, setActiveTab] = useState<"summary" | "compare">("summary");

  // Category Modal State (Add / Edit)
  const [catModalOpen, setCatModalOpen] = useState(false);
  const [editingCategory, setEditingCategory] = useState<any>(null);
  const [catName, setCatName] = useState("");
  const [catIcon, setCatIcon] = useState("tag");
  const [catColor, setCatColor] = useState("#3b82f6");
  const [catBudget, setCatBudget] = useState("");
  const [catIsPrimary, setCatIsPrimary] = useState(true);
  const [catError, setCatError] = useState("");

  const openCreateCategory = () => {
    setEditingCategory(null);
    setCatName("");
    setCatIcon("tag");
    setCatColor("#3b82f6");
    setCatBudget("");
    setCatIsPrimary(true);
    setCatError("");
    setCatModalOpen(true);
  };

  const openEditCategory = (cat: any) => {
    setEditingCategory(cat);
    setCatName(cat.name);
    setCatIcon(cat.icon || "tag");
    setCatColor(cat.color || "#3b82f6");
    setCatBudget(cat.budget ? formatNumberWithDots(cat.budget) : "");
    setCatIsPrimary(cat.is_primary !== false);
    setCatError("");
    setCatModalOpen(true);
  };

  const saveCategoryMutation = useMutation({
    mutationFn: async () => {
      const cleanBudget = catBudget.trim().replace(/[^0-9]/g, "");
      const budgetVal = cleanBudget ? parseInt(cleanBudget, 10) : null;
      if (!catName.trim()) throw new Error("Nama kategori wajib diisi");

      const payload = {
        name: catName.trim(),
        icon: catIcon,
        color: catColor,
        monthly_budget: budgetVal,
        is_primary: catIsPrimary,
      };

      if (editingCategory) {
        return api.patch(`/categories/${editingCategory.id}`, payload);
      } else {
        return api.post("/categories", {
          ...payload,
          kind: "expense",
        });
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["categories"] });
      setCatModalOpen(false);
    },
    onError: (err: any) => {
      setCatError(err?.message || "Gagal menyimpan kategori");
    },
  });

  const deleteCategoryMutation = useMutation({
    mutationFn: async (id: string) => api.del(`/categories/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["categories"] });
      setCatModalOpen(false);
    },
    onError: (err: any) => {
      setCatError(err?.message || "Gagal menghapus kategori");
    },
  });

  const { data, isLoading } = useQuery<any>({
    queryKey: ["dashboard-analytics", timeframe, cycleOffset],
    queryFn: () =>
      api.get(
        `/dashboard/analytics?timeframe=${timeframe}&cycle_offset=${cycleOffset}`
      ),
  });

  const burnRate = data?.burn_rate;
  const targetProjected =
    (timeframe === "cycle"
      ? burnRate?.projected_cycle_outflow
      : burnRate?.projected_30d_outflow) ??
    burnRate?.projected_cycle_outflow ??
    burnRate?.projected_30d_outflow ??
    0;

  const animBurn7d = useAnimatedCounter(burnRate?.daily_burn_7d ?? 0);
  const animBurn30d = useAnimatedCounter(burnRate?.daily_burn_30d ?? 0);
  const animProjected = useAnimatedCounter(targetProjected);

  if (isLoading) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-16 rounded-2xl bg-[var(--border)]/30" />
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="h-28 rounded-2xl bg-[var(--border)]/30" />
          <div className="h-28 rounded-2xl bg-[var(--border)]/30" />
          <div className="h-28 rounded-2xl bg-[var(--border)]/30" />
        </div>
        <div className="h-80 rounded-3xl bg-[var(--border)]/20" />
        <div className="h-64 rounded-3xl bg-[var(--border)]/20" />
      </div>
    );
  }

  const cadence = data?.daily_cadence ?? [];
  const heatmap = data?.day_of_week_heatmap ?? [];
  const rawCategories = data?.category_variance ?? [];
  const comparison = data?.comparison;
  const kakeibo = data?.kakeibo;
  const narrative = data?.narrative;
  const timeframeLabel = data?.timeframe_label;

  // Ensure strict filtering for expense categories only
  const categories = rawCategories.filter((c: any) => c.kind !== "income");

  const totalSpentInCategories = categories.reduce((acc: number, c: any) => acc + c.spent, 0);
  const totalBudgeted = categories.reduce((acc: number, c: any) => acc + (c.budget || 0), 0);
  const overBudgetCount = categories.filter((c: any) => c.status === "over_budget").length;

  return (
    <div className="space-y-6">
      {/* 1. Header & Segmented Tab Switch */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-[var(--border)]">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
              Analisis Keuangan
            </span>
            <span className="h-1 w-1 rounded-full bg-[var(--muted)]" />
            <span className="text-xs text-[var(--text-secondary)]">
              {timeframeLabel}
            </span>
          </div>
          <h1 className="text-2xl font-extrabold tracking-tight text-[var(--text)] mt-1">
            Analisis Pengeluaran & Anggaran
          </h1>
        </div>

        {/* Segmented Control Switch */}
        <div className="flex items-center p-1 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] self-start sm:self-auto">
          <button
            type="button"
            onClick={() => setActiveTab("summary")}
            className={cn(
              "px-4 py-2 rounded-xl text-xs font-bold transition-all pressable",
              activeTab === "summary"
                ? "bg-[var(--surface)] text-[var(--text)] shadow-xs border border-[var(--border)]"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            Ringkasan & Anggaran
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("compare")}
            className={cn(
              "px-4 py-2 rounded-xl text-xs font-bold transition-all pressable flex items-center gap-1.5",
              activeTab === "compare"
                ? "bg-[var(--surface)] text-[var(--text)] shadow-xs border border-[var(--border)]"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            <span>Bandingkan & Kakeibo</span>
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
          </button>
        </div>
      </div>

      {/* ===================== TAB 1: SUMMARY & BUDGETS ===================== */}
      {activeTab === "summary" && (
        <div className="space-y-6">
          {/* Burn Rate KPI Tiles */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div className="card-squircle bg-rose-500/[0.04] border-rose-500/20 p-5 space-y-2.5 shadow-xs">
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--muted)] font-bold">Rata-rata 7 Hari</span>
                <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-rose-500/15 text-rose-500 border border-rose-500/25">
                  7 Hari
                </span>
              </div>
              <div className="text-2xl sm:text-3xl font-black tabular tracking-tight text-rose-500 select-all">
                {bal(animBurn7d)}
                <span className="text-xs text-[var(--muted)] font-bold font-sans"> / hari</span>
              </div>
              <p className="text-[11px] text-[var(--muted)] font-medium">Laju pengeluaran harian terkini</p>
            </div>

            <div className="card-squircle bg-amber-500/[0.04] border-amber-500/20 p-5 space-y-2.5 shadow-xs">
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--muted)] font-bold">Rata-rata 30 Hari</span>
                <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-amber-500/15 text-amber-500 border border-amber-500/25">
                  30 Hari
                </span>
              </div>
              <div className="text-2xl sm:text-3xl font-black tabular tracking-tight text-amber-500 select-all">
                {bal(animBurn30d)}
                <span className="text-xs text-[var(--muted)] font-bold font-sans"> / hari</span>
              </div>
              <p className="text-[11px] text-[var(--muted)] font-medium">Rata-rata pengeluaran harian sebulan</p>
            </div>

            <div className="card-squircle bg-indigo-500/[0.04] border-indigo-500/20 p-5 space-y-2.5 shadow-xs">
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--muted)] font-bold">
                  {timeframe === "cycle"
                    ? "Proyeksi Akhir Siklus"
                    : timeframe === "90d"
                    ? "Proyeksi 90 Hari"
                    : "Proyeksi 30 Hari"}
                </span>
                <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-indigo-500/15 text-indigo-500 border border-indigo-500/25">
                  {timeframe === "cycle" ? "Siklus" : "Horizon"}
                </span>
              </div>
              <div className="text-2xl sm:text-3xl font-black tabular tracking-tight text-[var(--text)] select-all">
                {bal(animProjected)}
              </div>
              <p className="text-[11px] text-[var(--muted)] font-medium">
                {timeframe === "cycle" && burnRate?.cycle_end_date ? (
                  <>
                    Estimasi total s.d.{" "}
                    <span className="font-bold text-[var(--text)]">
                      {(() => {
                        const parts = (burnRate.cycle_end_date as string).split("-");
                        if (parts.length === 3) {
                          const m = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"];
                          return `${parseInt(parts[2], 10)} ${m[parseInt(parts[1], 10) - 1] || ""}`;
                        }
                        return burnRate.cycle_end_date;
                      })()}
                    </span>{" "}
                    <span className="text-[10px] text-[var(--muted)] tabular font-medium">
                      ({burnRate.cycle_elapsed_days ?? 0}/{burnRate.cycle_total_days ?? 0} hari)
                    </span>
                  </>
                ) : (
                  "Berdasarkan tren pengeluaran saat ini"
                )}
              </p>
            </div>
          </div>

          {/* Daily Burn Cadence Chart */}
          <BurnCadenceChart
            data={cadence}
            title="Laju Pengeluaran Harian"
            subtitle="Distribusi kronologis pengeluaran selama periode aktif"
          />

          {/* Day-of-Week Spending Heatmap */}
          <SpendingHeatmap
            data={heatmap}
            title="Peta Pola Belanja Mingguan"
            subtitle="Ketahui hari-hari dengan tingkat pengeluaran tertinggi"
          />

          {/* Category Budget Variance Table */}
          <div className="cockpit-card p-5 sm:p-6 space-y-4 border border-[var(--border)] shadow-xs">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
              <div>
                <div className="flex items-center gap-2">
                  <h2 className="text-base font-bold tracking-tight text-[var(--text)]">
                    Batas Anggaran Kategori
                  </h2>
                  {overBudgetCount > 0 && (
                    <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-rose-500/10 text-rose-500 border border-rose-500/20">
                      {overBudgetCount} melebihi batas
                    </span>
                  )}
                </div>
                <p className="text-xs text-[var(--muted)] mt-0.5">
                  Pengeluaran periode ini dibanding batas anggaran pengeluaran
                </p>
              </div>

              <div className="flex items-center gap-3 self-start sm:self-auto">
                <div className="hidden sm:flex items-center gap-2 text-xs text-[var(--muted)] tabular font-medium">
                  <span>Total Keluar: {bal(totalSpentInCategories)}</span>
                  {totalBudgeted > 0 && (
                    <>
                      <span>•</span>
                      <span>Total Anggaran: {bal(totalBudgeted)}</span>
                    </>
                  )}
                </div>
                <button
                  type="button"
                  onClick={openCreateCategory}
                  className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-white text-xs font-bold shadow-xs transition-all active:scale-95 cursor-pointer"
                >
                  <Icon name="plus" className="h-3.5 w-3.5" />
                  <span>Tambah Kategori</span>
                </button>
              </div>
            </div>

            {categories.length === 0 ? (
              <div className="py-12 text-center text-xs text-[var(--muted)]">
                Belum ada data pengeluaran kategori.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-xs text-left">
                  <thead>
                    <tr className="border-b border-[var(--border)] text-[var(--muted)] uppercase text-[10px] font-semibold">
                      <th className="pb-3 font-semibold">Kategori</th>
                      <th className="pb-3 font-semibold text-right">Terpakai</th>
                      <th className="pb-3 font-semibold text-right">Batas Anggaran</th>
                      <th className="pb-3 font-semibold text-right">Sisa Anggaran</th>
                      <th className="pb-3 font-semibold text-right">Status</th>
                      <th className="pb-3 font-semibold text-right w-16">Aksi</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[var(--border)]">
                    {categories.map((cat: any) => {
                      const hasBudget = cat.budget && cat.budget > 0;
                      const isOver = cat.status === "over_budget";
                      const isWarning = cat.status === "warning";

                      return (
                        <tr
                          key={cat.id}
                          className="hover:bg-[var(--surface-raised)]/50 transition-colors"
                        >
                          <td className="py-3.5 pr-4">
                            <div className="flex items-center gap-2.5">
                              <div
                                className="h-7 w-7 rounded-lg flex items-center justify-center text-xs text-white shrink-0 shadow-2xs"
                                style={{ backgroundColor: cat.color || "#3b82f6" }}
                              >
                                <Icon
                                  name={cat.icon || "tag"}
                                  className="h-3.5 w-3.5 text-white"
                                />
                              </div>
                              <div>
                                <div className="flex items-center gap-1.5">
                                  <span className="font-semibold text-[var(--text)]">
                                    {cat.name}
                                  </span>
                                </div>
                                {hasBudget && (
                                  <div className="w-24 sm:w-36 h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden mt-1">
                                    <div
                                      style={{
                                        width: `${Math.min(cat.percentage_used, 100)}%`,
                                      }}
                                      className={cn(
                                        "h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]",
                                        isOver
                                          ? "bg-rose-500 shadow-rose-500/30"
                                          : isWarning
                                          ? "bg-amber-500 shadow-amber-500/30"
                                          : "bg-emerald-500 shadow-emerald-500/30"
                                      )}
                                    />
                                  </div>
                                )}
                              </div>
                            </div>
                          </td>

                          <td className="py-3.5 text-right font-bold tabular text-[var(--text)]">
                            {bal(cat.spent)}
                          </td>

                          <td className="py-3.5 text-right text-[var(--muted)] tabular">
                            <button
                              type="button"
                              onClick={() => openEditCategory(cat)}
                              className="inline-flex items-center gap-1.5 px-2 py-1 rounded-lg hover:bg-[var(--surface-raised)] text-[var(--text)] font-semibold border border-transparent hover:border-[var(--border)] transition-colors group/btn cursor-pointer"
                              title="Klik untuk mengatur batas anggaran"
                            >
                              <span>{hasBudget ? bal(cat.budget) : "Atur batas"}</span>
                              <Icon
                                name="edit"
                                className="h-3 w-3 opacity-0 group-hover/btn:opacity-100 text-[var(--muted)] transition-opacity"
                              />
                            </button>
                          </td>

                          <td className="py-3.5 text-right tabular">
                            {hasBudget ? (
                              <span
                                className={cn(
                                  "font-semibold",
                                  cat.variance >= 0 ? "text-emerald-500" : "text-rose-500"
                                )}
                              >
                                {cat.variance >= 0
                                  ? `Sisa ${bal(cat.variance)}`
                                  : `Lebih ${bal(Math.abs(cat.variance))}`}
                              </span>
                            ) : (
                              <span className="text-[var(--muted)]">-</span>
                            )}
                          </td>

                          <td className="py-3.5 text-right">
                            <span
                              className={cn(
                                "px-2 py-0.5 rounded-full text-[10px] font-bold tabular",
                                isOver
                                  ? "bg-rose-500/10 text-rose-500 border border-rose-500/20"
                                  : isWarning
                                  ? "bg-amber-500/10 text-amber-500 border border-amber-500/20"
                                  : hasBudget
                                  ? "bg-emerald-500/10 text-emerald-500 border border-emerald-500/20"
                                  : "bg-[var(--surface-raised)] text-[var(--muted)]"
                              )}
                            >
                              {isOver
                                ? `${cat.percentage_used}% Lewat`
                                : isWarning
                                ? `${cat.percentage_used}% Waspada`
                                : hasBudget
                                ? `${cat.percentage_used}% Aman`
                                : "Tanpa Batas"}
                            </span>
                          </td>

                          <td className="py-3.5 text-right">
                            <div className="flex items-center justify-end gap-1">
                              <button
                                type="button"
                                onClick={() => openEditCategory(cat)}
                                className="p-1.5 rounded-lg hover:bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)] transition-colors cursor-pointer"
                                title="Edit kategori"
                              >
                                <Icon name="edit" className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                disabled={deleteCategoryMutation.isPending}
                                onClick={() => {
                                  if (
                                    confirm(
                                      `Yakin ingin menghapus atau mengarsipkan kategori "${cat.name}"?`
                                    )
                                  ) {
                                    deleteCategoryMutation.mutate(cat.id);
                                  }
                                }}
                                className="p-1.5 rounded-lg hover:bg-rose-500/10 text-[var(--muted)] hover:text-rose-500 transition-colors disabled:opacity-50 cursor-pointer"
                                title="Hapus kategori"
                              >
                                <Icon name="trash" className="h-3.5 w-3.5" />
                              </button>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ===================== TAB 2: COMPARE & KAKEIBO ===================== */}
      {activeTab === "compare" && (
        <div className="space-y-6">
          {/* 1. Conversational Financial Narrative */}
          <NarrativeInsightCard
            narrative={narrative}
            outflowDeltaPct={comparison?.outflow_delta_pct ?? 0}
            timeframeLabel={timeframeLabel}
          />

          {/* 2. Period-over-Period Delta Metric Cards ($T_0$ vs $T_{-1}$) */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-base font-bold tracking-tight text-[var(--text)]">
                  Komparasi Siklus Berjalan vs Siklus Lalu
                </h2>
                <p className="text-xs text-[var(--muted)]">
                  Membandingkan performa {timeframeLabel} dengan{" "}
                  <span className="font-semibold text-[var(--text)]">
                    {comparison?.prev_timeframe_label || "periode sebelumnya"}
                  </span>
                </p>
              </div>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              {/* Card 1: Inflow Delta */}
              <div className="cockpit-card p-4 space-y-2 border border-[var(--border)]">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-[var(--muted)]">Pemasukan</span>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-1.5 py-0.5 rounded border",
                      (comparison?.inflow_delta_pct ?? 0) >= 0
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-rose-500/10 text-rose-500 border-rose-500/20"
                    )}
                  >
                    {(comparison?.inflow_delta_pct ?? 0) >= 0 ? "+" : ""}
                    {comparison?.inflow_delta_pct ?? 0}%
                  </span>
                </div>
                <div className="text-xl font-bold text-[var(--text)] tabular">
                  {bal(comparison?.total_inflow ?? 0)}
                </div>
                <div className="text-[10px] text-[var(--muted)]">
                  Siklus lalu: {bal(comparison?.prev_inflow ?? 0)}
                </div>
              </div>

              {/* Card 2: Outflow Delta */}
              <div className="cockpit-card p-4 space-y-2 border border-[var(--border)]">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-[var(--muted)]">Pengeluaran</span>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-1.5 py-0.5 rounded border",
                      (comparison?.outflow_delta_pct ?? 0) <= 0
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-rose-500/10 text-rose-500 border-rose-500/20"
                    )}
                  >
                    {(comparison?.outflow_delta_pct ?? 0) > 0 ? "+" : ""}
                    {comparison?.outflow_delta_pct ?? 0}%
                  </span>
                </div>
                <div className="text-xl font-bold text-[var(--text)] tabular">
                  {bal(comparison?.total_outflow ?? 0)}
                </div>
                <div className="text-[10px] text-[var(--muted)]">
                  Siklus lalu: {bal(comparison?.prev_outflow ?? 0)}
                </div>
              </div>

              {/* Card 3: Net Cashflow Delta */}
              <div className="cockpit-card p-4 space-y-2 border border-[var(--border)]">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-[var(--muted)]">Arus Kas Bersih</span>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-1.5 py-0.5 rounded border",
                      (comparison?.net_cashflow ?? 0) >= 0
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-rose-500/10 text-rose-500 border-rose-500/20"
                    )}
                  >
                    {(comparison?.net_cashflow_delta_pct ?? 0) >= 0 ? "+" : ""}
                    {comparison?.net_cashflow_delta_pct ?? 0}%
                  </span>
                </div>
                <div
                  className={cn(
                    "text-xl font-bold tabular",
                    (comparison?.net_cashflow ?? 0) >= 0
                      ? "text-emerald-500"
                      : "text-rose-500"
                  )}
                >
                  {bal(comparison?.net_cashflow ?? 0)}
                </div>
                <div className="text-[10px] text-[var(--muted)]">
                  Siklus lalu: {bal(comparison?.prev_net_cashflow ?? 0)}
                </div>
              </div>

              {/* Card 4: Savings Rate Delta */}
              <div className="cockpit-card p-4 space-y-2 border border-[var(--border)]">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-[var(--muted)]">Rasio Tabungan</span>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-1.5 py-0.5 rounded border",
                      (comparison?.savings_rate_delta_pts ?? 0) >= 0
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-rose-500/10 text-rose-500 border-rose-500/20"
                    )}
                  >
                    {(comparison?.savings_rate_delta_pts ?? 0) >= 0 ? "+" : ""}
                    {comparison?.savings_rate_delta_pts ?? 0} pts
                  </span>
                </div>
                <div className="text-xl font-bold text-[var(--text)] tabular">
                  {comparison?.savings_rate ?? 0}%
                </div>
                <div className="text-[10px] text-[var(--muted)]">
                  Siklus lalu: {comparison?.prev_savings_rate ?? 0}%
                </div>
              </div>
            </div>
          </div>

          {/* 3. Kakeibo 3-Pillar Budgeting Blueprint (50 / 30 / 20) */}
          <div className="cockpit-card p-5 sm:p-6 space-y-5 border border-[var(--border)] shadow-xs">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
              <div>
                <div className="flex items-center gap-2">
                  <span className="text-base">🇯🇵</span>
                  <h2 className="text-base font-bold tracking-tight text-[var(--text)]">
                    Metode Kakeibo (50 / 30 / 20)
                  </h2>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-2 py-0.5 rounded-full border",
                      kakeibo?.kakeibo_status === "SEHAT"
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-amber-500/10 text-amber-500 border-amber-500/20"
                    )}
                  >
                    {kakeibo?.kakeibo_status === "SEHAT"
                      ? "KAKEIBO SEHAT"
                      : "PERLU PENYESUAIAN"}
                  </span>
                </div>
                <p className="text-xs text-[var(--muted)] mt-0.5">
                  Filosofi alokasi kas harian: 50% Kebutuhan Pokok, 30% Keinginan, 20% Tabungan & Investasi
                </p>
              </div>
            </div>

            {/* Visual 3-Pillar Proportion Stack Bar */}
            <div className="space-y-1.5">
              <div className="h-3.5 w-full rounded-full bg-[var(--surface-raised)] border border-[var(--border)] flex overflow-hidden">
                <div
                  style={{ width: `${Math.min(kakeibo?.need_pct ?? 0, 100)}%` }}
                  className="bg-emerald-500 h-full transition-all"
                  title={`Kebutuhan: ${kakeibo?.need_pct ?? 0}%`}
                />
                <div
                  style={{ width: `${Math.min(kakeibo?.want_pct ?? 0, 100)}%` }}
                  className="bg-blue-500 h-full transition-all"
                  title={`Keinginan: ${kakeibo?.want_pct ?? 0}%`}
                />
                <div
                  style={{ width: `${Math.min(kakeibo?.saving_pct ?? 0, 100)}%` }}
                  className="bg-purple-500 h-full transition-all"
                  title={`Tabungan: ${kakeibo?.saving_pct ?? 0}%`}
                />
              </div>
              <div className="flex items-center justify-between text-[10px] text-[var(--muted)] tabular font-medium">
                <span className="flex items-center gap-1">
                  <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                  Kebutuhan ({kakeibo?.need_pct ?? 0}%)
                </span>
                <span className="flex items-center gap-1">
                  <span className="h-1.5 w-1.5 rounded-full bg-blue-500" />
                  Keinginan ({kakeibo?.want_pct ?? 0}%)
                </span>
                <span className="flex items-center gap-1">
                  <span className="h-1.5 w-1.5 rounded-full bg-purple-500" />
                  Tabungan ({kakeibo?.saving_pct ?? 0}%)
                </span>
              </div>
            </div>

            {/* 3 Interactive Cards */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              {/* Pillar 1: Needs */}
              <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/40 space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-base">🍞</span>
                    <div>
                      <div className="text-xs font-bold text-[var(--text)]">Kebutuhan (Need)</div>
                      <div className="text-[10px] text-[var(--muted)]">Target: &le; 50%</div>
                    </div>
                  </div>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-2 py-0.5 rounded-full border",
                      (kakeibo?.need_pct ?? 0) <= 50
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-rose-500/10 text-rose-500 border-rose-500/20"
                    )}
                  >
                    {(kakeibo?.need_pct ?? 0) <= 50 ? "Aman" : "Over 50%"}
                  </span>
                </div>

                <div>
                  <div className="text-xl font-bold text-[var(--text)] tabular">
                    {bal(kakeibo?.need_spent ?? 0)}
                  </div>
                  <div className="text-[11px] text-[var(--muted)] mt-0.5">
                    {kakeibo?.need_pct ?? 0}% dari total uang masuk
                  </div>
                </div>

                <p className="text-[11px] text-[var(--muted)] leading-relaxed">
                  Belanja makanan pokok, sewa/kost, utilitas, cicilan wajib, dan operasional penting.
                </p>
              </div>

              {/* Pillar 2: Wants */}
              <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/40 space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-base">👑</span>
                    <div>
                      <div className="text-xs font-bold text-[var(--text)]">Keinginan (Want)</div>
                      <div className="text-[10px] text-[var(--muted)]">Target: &le; 30%</div>
                    </div>
                  </div>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-2 py-0.5 rounded-full border",
                      (kakeibo?.want_pct ?? 0) <= 30
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-rose-500/10 text-rose-500 border-rose-500/20"
                    )}
                  >
                    {(kakeibo?.want_pct ?? 0) <= 30 ? "Terkendali" : "Over 30%"}
                  </span>
                </div>

                <div>
                  <div className="text-xl font-bold text-[var(--text)] tabular">
                    {bal(kakeibo?.want_spent ?? 0)}
                  </div>
                  <div className="text-[11px] text-[var(--muted)] mt-0.5">
                    {kakeibo?.want_pct ?? 0}% dari total uang masuk
                  </div>
                </div>

                <p className="text-[11px] text-[var(--muted)] leading-relaxed">
                  Makan di resto, kopi, hobi, langganan hiburan, dan belanja sekunder penunjang gaya hidup.
                </p>
              </div>

              {/* Pillar 3: Savings */}
              <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/40 space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-base">💰</span>
                    <div>
                      <div className="text-xs font-bold text-[var(--text)]">Tabungan (Saving)</div>
                      <div className="text-[10px] text-[var(--muted)]">Target: &ge; 20%</div>
                    </div>
                  </div>
                  <span
                    className={cn(
                      "text-[10px] font-bold px-2 py-0.5 rounded-full border",
                      (kakeibo?.saving_pct ?? 0) >= 20
                        ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
                        : "bg-amber-500/10 text-amber-500 border-amber-500/20"
                    )}
                  >
                    {(kakeibo?.saving_pct ?? 0) >= 20 ? "Target Tercapai" : "Perlu Ditingkatkan"}
                  </span>
                </div>

                <div>
                  <div className="text-xl font-bold text-[var(--text)] tabular">
                    {bal(kakeibo?.saving_spent ?? 0)}
                  </div>
                  <div className="text-[11px] text-[var(--muted)] mt-0.5">
                    {kakeibo?.saving_pct ?? 0}% dari total uang masuk
                  </div>
                </div>

                <p className="text-[11px] text-[var(--muted)] leading-relaxed">
                  Dana darurat, tabungan target masa depan, reksadana/saham, dan sisa surplus kas aktif.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ===================== CATEGORY MODAL ===================== */}
      {catModalOpen && (
        <Modal
          open={catModalOpen}
          onClose={() => setCatModalOpen(false)}
          title={
            editingCategory
              ? `Kelola Kategori: ${editingCategory.name}`
              : "Tambah Kategori Baru"
          }
        >
          <div className="space-y-4 pt-2 text-xs">
            {catError && (
              <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {catError}
              </div>
            )}

            <div>
              <label className="text-xs font-medium text-[var(--muted)] block mb-1">
                Nama Kategori
              </label>
              <input
                type="text"
                value={catName}
                onChange={(e) => setCatName(e.target.value)}
                placeholder="Contoh: Langganan Streaming, Asuransi, dsb"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-semibold text-[var(--text)] focus:outline-hidden focus:border-emerald-500"
              />
            </div>

            {/* Icon Picker */}
            <div>
              <label className="text-xs font-medium text-[var(--muted)] block mb-1.5">
                Ikon Kategori
              </label>
              <div className="grid grid-cols-5 gap-2">
                {CATEGORY_ICONS.map((ic) => (
                  <button
                    key={ic.id}
                    type="button"
                    onClick={() => setCatIcon(ic.id)}
                    className={cn(
                      "flex flex-col items-center justify-center p-2 rounded-xl border transition-all cursor-pointer",
                      catIcon === ic.id
                        ? "border-emerald-500 bg-emerald-500/10 text-emerald-500"
                        : "border-[var(--border)] bg-[var(--surface-raised)]/60 text-[var(--muted)] hover:text-[var(--text)]"
                    )}
                  >
                    <Icon name={ic.id} className="h-4 w-4 mb-1" />
                    <span className="text-[10px] truncate font-medium">{ic.label}</span>
                  </button>
                ))}
              </div>
            </div>

            {/* Color Palette */}
            <div>
              <label className="text-xs font-medium text-[var(--muted)] block mb-1.5">
                Warna Kategori
              </label>
              <div className="flex items-center gap-2">
                {CATEGORY_COLORS.map((c) => (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setCatColor(c)}
                    style={{ backgroundColor: c }}
                    className={cn(
                      "h-7 w-7 rounded-full transition-transform cursor-pointer",
                      catColor === c
                        ? "ring-2 ring-offset-2 ring-offset-[var(--surface)] ring-emerald-500 scale-110"
                        : "opacity-80 hover:opacity-100 hover:scale-105"
                    )}
                  />
                ))}
              </div>
            </div>

            {/* Monthly Budget Input */}
            <div>
              <label className="text-xs font-medium text-[var(--muted)] block mb-1">
                Batas Anggaran Bulanan (IDR)
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={catBudget}
                onChange={(e) =>
                  setCatBudget(formatNumberWithDots(e.target.value))
                }
                placeholder="Kosongkan jika tanpa batas"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)] focus:outline-hidden focus:border-emerald-500"
              />
              <p className="text-[11px] text-[var(--muted)] mt-1">
                Batas pengeluaran bulanan untuk memantau disiplin anggaran kategori.
              </p>
            </div>

            <div className="flex items-center justify-end gap-2 pt-3 border-t border-[var(--border)]">
              <button
                type="button"
                onClick={() => setCatModalOpen(false)}
                className="px-4 py-2 rounded-xl border border-[var(--border)] text-[var(--muted)] hover:bg-[var(--surface-raised)] cursor-pointer"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={saveCategoryMutation.isPending}
                onClick={() => saveCategoryMutation.mutate()}
                className="px-4 py-2 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-white font-semibold shadow-xs disabled:opacity-50 cursor-pointer"
              >
                {saveCategoryMutation.isPending
                  ? "Menyimpan..."
                  : editingCategory
                  ? "Simpan Perubahan"
                  : "Buat Kategori"}
              </button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  );
}
