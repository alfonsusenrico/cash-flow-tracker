"use client";

import { useState } from "react";
import Link from "next/link";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { cn, formatNumberWithDots } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { FormSection } from "@/components/ui/FormField";
import { BurnCadenceChart } from "@/components/dashboard/BurnCadenceChart";
import { SpendingHeatmap } from "@/components/dashboard/SpendingHeatmap";
import { KakeiboBenchmark } from "@/components/dashboard/KakeiboBenchmark";
import { CashFlowHistory, PeriodComparison, RecurringCharges } from "@/components/insights/ObservedInsights";
import { QuestionRate } from "@/components/insights/QuestionRate";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import type { ExpensePlanDraft } from "@/types/recurring";
import { KAKEIBO_PILLARS, type KakeiboPillar, kakeiboPillarInfo, pillarOrNeed } from "@/lib/kakeibo";
import { ledgerHref, reportPeriodFilters, UNCATEGORIZED } from "@/lib/ledgerFilters";

const CATEGORY_ICONS = [
  { id: "tag", label: "Label" },
  { id: "utensils", label: "Makanan" },
  { id: "coffee", label: "Jajan" },
  { id: "shopping-cart", label: "Belanja" },
  { id: "shopping-bag", label: "Mall" },
  { id: "car", label: "Transportasi" },
  { id: "zap", label: "Tagihan" },
  { id: "heart", label: "Kesehatan" },
  { id: "film", label: "Hiburan" },
  { id: "wallet", label: "Dompet" },
  { id: "credit-card", label: "Kartu" },
  { id: "users", label: "Patungan" },
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

const CATEGORY_COLOR_NAMES: Record<string, string> = {
  "#f97316": "Oranye",
  "#10b981": "Emerald",
  "#3b82f6": "Biru",
  "#8b5cf6": "Ungu",
  "#ec4899": "Merah muda",
  "#ef4444": "Merah",
  "#eab308": "Kuning",
  "#14b8a6": "Teal",
  "#64748b": "Abu-abu",
};

export default function AnalyticsPage() {
  const qc = useQueryClient();
  const { timeframe, setTimeframe, cycleOffset, bal } = useAppCtx();
  const reportPeriod = reportPeriodFilters(timeframe, cycleOffset);
  // Drill-down: the ledger rows behind a category figure, for the same period.
  const categoryHref = (cat: { id: string | null; is_uncategorized?: boolean }) =>
    reportPeriod
      ? ledgerHref({ ...reportPeriod, type: "expense", category: cat.is_uncategorized ? UNCATEGORIZED : cat.id ?? "all" })
      : null;
  const pillarHref = (pillar: KakeiboPillar) =>
    reportPeriod ? ledgerHref({ ...reportPeriod, type: "expense", kakeibo: pillar }) : null;
  // Observed insights use salary cycles or calendar months; rolling windows have no previous period.
  const insightBasis = timeframe === "month" ? "month" : "cycle";
  const insightOffset = reportPeriod ? cycleOffset : 0;
  const [planDraft, setPlanDraft] = useState<ExpensePlanDraft | null>(null);

  // Active view tab: summary vs compare/kakeibo
  const [activeTab, setActiveTab] = useState<"summary" | "compare">("summary");

  // Category Modal State (Add / Edit)
  const [catModalOpen, setCatModalOpen] = useState(false);
  const [editingCategory, setEditingCategory] = useState<any>(null);
  const [catName, setCatName] = useState("");
  const [catIcon, setCatIcon] = useState("tag");
  const [catColor, setCatColor] = useState("#3b82f6");
  const [catBudget, setCatBudget] = useState("");
  const [catKind, setCatKind] = useState<"expense" | "income">("expense");
  const [catKakeiboType, setCatKakeiboType] = useState<KakeiboPillar>("need");
  const [catError, setCatError] = useState("");

  const openCreateCategory = (kind: "expense" | "income" = "expense") => {
    setEditingCategory(null);
    setCatName("");
    setCatIcon("tag");
    setCatColor("#3b82f6");
    setCatBudget("");
    setCatKind(kind);
    setCatKakeiboType("need");
    setCatError("");
    setCatModalOpen(true);
  };

  const openEditCategory = (cat: any) => {
    setEditingCategory(cat);
    setCatName(cat.name);
    setCatIcon(cat.icon || "tag");
    setCatColor(cat.color || "#3b82f6");
    const monthlyBudget = cat.monthly_budget ?? cat.budget;
    setCatBudget(monthlyBudget ? formatNumberWithDots(monthlyBudget) : "");
    setCatKind(cat.kind === "income" ? "income" : "expense");
    setCatKakeiboType(pillarOrNeed(cat.kakeibo_type));
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
        kind: catKind,
        monthly_budget: catKind === "expense" ? budgetVal : null,
        kakeibo_type: catKind === "expense" ? catKakeiboType : null,
      };

      if (editingCategory) {
        return api.patch(`/categories/${editingCategory.id}`, payload);
      } else {
        return api.post("/categories", {
          ...payload,
          kind: catKind,
        });
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.categories });
      setCatModalOpen(false);
    },
    onError: (err: any) => {
      setCatError(err?.message || "Gagal menyimpan kategori");
    },
  });

  const deleteCategoryMutation = useMutation({
    mutationFn: async (id: string) => api.del(`/categories/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.categories });
      setCatModalOpen(false);
    },
    onError: (err: any) => {
      setCatError(err?.message || "Gagal menghapus kategori");
    },
  });

  const restoreCategoryMutation = useMutation({
    mutationFn: async (id: string) => api.patch(`/categories/${id}`, { is_archived: false }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.categories });
    },
    onError: (err: any) => {
      setCatError(err?.message || "Gagal memulihkan kategori");
    },
  });

  const { data, isLoading } = useQuery<any>({
    queryKey: queryKeys.dashboard.analytics(timeframe, cycleOffset),
    queryFn: () =>
      api.get(
        `/dashboard/analytics?timeframe=${timeframe}&cycle_offset=${cycleOffset}`
      ),
  });

  const { data: managedCategoriesData } = useQuery<{ categories: any[] }>({
    queryKey: queryKeys.categoryManagement,
    queryFn: () => api.get("/categories?include_archived=true"),
  });
  const incomeCategories = (managedCategoriesData?.categories ?? []).filter(
    (category) => category.kind === "income" && !category.is_archived,
  );
  const archivedCategories = (managedCategoriesData?.categories ?? []).filter(
    (category) => category.is_archived,
  );

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

        {(timeframe === "cycle" || timeframe === "month") && (
          <div role="group" aria-label="Dasar periode" className="flex items-center p-1 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] self-start sm:self-auto">
            {([["cycle", "Siklus gajian"], ["month", "Bulan kalender"]] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                aria-pressed={timeframe === value}
                onClick={() => setTimeframe(value)}
                className={cn(
                  "min-h-10 px-3 rounded-xl text-xs font-bold transition-[background-color,color] focus-visible:outline-2 focus-visible:outline-[var(--primary)]",
                  timeframe === value
                    ? "bg-[var(--surface)] text-[var(--text)] shadow-xs border border-[var(--border)]"
                    : "text-[var(--text-secondary)] hover:text-[var(--text)]"
                )}
              >
                {label}
              </button>
            ))}
          </div>
        )}

        {/* Segmented Control Switch */}
        <div className="flex items-center p-1 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] self-start sm:self-auto">
          <button
            type="button"
            onClick={() => setActiveTab("summary")}
            className={cn(
              "px-4 py-2 rounded-xl text-xs font-bold transition-[background-color,color,transform] pressable",
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
              "px-4 py-2 rounded-xl text-xs font-bold transition-[background-color,color,transform] pressable flex items-center gap-1.5",
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
          <section aria-label="Arus kas tercatat" className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {[
              { label: "Uang masuk", amount: comparison?.total_inflow },
              { label: "Uang keluar", amount: comparison?.total_outflow },
              { label: "Selisih bersih", amount: comparison?.net_cashflow },
            ].map((item) => (
              <div key={item.label} className="card-squircle border border-[var(--border)] p-4 space-y-1">
                <div className="text-xs font-bold text-[var(--muted)]">{item.label}</div>
                <div className="text-xl font-black tabular text-[var(--text)]">{typeof item.amount === "number" ? bal(item.amount) : "Tidak tersedia"}</div>
              </div>
            ))}
          </section>

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
                  onClick={() => openCreateCategory()}
                  className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-white text-xs font-bold shadow-xs transition-[background-color,transform] active:scale-95 cursor-pointer"
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
              <>
                {/* 1. Desktop 6-Column Data Table (>= lg) */}
                <div className="hidden lg:block overflow-x-auto">
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
                        const managed = !cat.is_uncategorized && !cat.is_archived;
                        const drill = categoryHref(cat);

                        return (
                          <tr
                            key={cat.id ?? "uncategorized"}
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
                                    {cat.is_archived && (
                                      <span className="rounded-full border border-[var(--border)] px-1.5 text-[10px] font-semibold text-[var(--text-secondary)]">Diarsipkan</span>
                                    )}
                                  </div>
                                  {hasBudget && (
                                    <div className="w-24 sm:w-36 h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden mt-1">
                                      <div
                                        style={{
                                          width: `${Math.min(cat.percentage_used, 100)}%`,
                                        }}
                                        className={cn(
                                          "h-full rounded-full transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none",
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
                              {drill ? (
                                <Link
                                  href={drill}
                                  aria-label={`${bal(cat.spent)}, lihat transaksi ${cat.name}`}
                                  className="rounded underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                                >
                                  {bal(cat.spent)}
                                </Link>
                              ) : bal(cat.spent)}
                            </td>

                            <td className="py-3.5 text-right text-[var(--muted)] tabular">
                              {managed ? (
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
                              ) : (
                                <span>-</span>
                              )}
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
                              {managed && (
                              <div className="flex items-center justify-end gap-1">
                                <button
                                  type="button"
                                  onClick={() => openEditCategory(cat)}
                                  aria-label={`Edit kategori ${cat.name}`}
                                  className="p-1.5 rounded-lg hover:bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)] transition-colors cursor-pointer"
                                  title="Edit kategori"
                                >
                                  <Icon name="edit" className="h-3.5 w-3.5" />
                                </button>
                                <ConfirmActionButton
                                  label={`Arsipkan kategori ${cat.name}`}
                                  confirmation={`Yakin ingin mengarsipkan kategori "${cat.name}"?`}
                                  onConfirm={() => deleteCategoryMutation.mutate(cat.id)}
                                  disabled={deleteCategoryMutation.isPending}
                                  className="p-1.5 rounded-lg hover:bg-rose-500/10 text-[var(--muted)] hover:text-rose-500 transition-colors disabled:opacity-50 cursor-pointer"
                                >
                                  <Icon name="trash" className="h-3.5 w-3.5" />
                                </ConfirmActionButton>
                              </div>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                {/* 2. Mobile Native Progress Cards (< lg) */}
                <div className="lg:hidden space-y-3">
                  {categories.map((cat: any) => {
                    const hasBudget = cat.budget && cat.budget > 0;
                    const isOver = cat.status === "over_budget";
                    const isWarning = cat.status === "warning";
                    const managed = !cat.is_uncategorized && !cat.is_archived;
                    const drill = categoryHref(cat);

                    return (
                      <div
                        key={`mobile-cat-${cat.id ?? "uncategorized"}`}
                        className="p-3.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/30 hover:bg-[var(--surface-raised)]/60 transition-colors space-y-2.5"
                      >
                        {/* Row 1: Icon + Name + Terpakai */}
                        <div className="flex items-center justify-between gap-2">
                          <div className="flex items-center gap-2.5 min-w-0">
                            <div
                              className="h-8 w-8 rounded-xl flex items-center justify-center text-xs text-white shrink-0 shadow-2xs"
                              style={{ backgroundColor: cat.color || "#3b82f6" }}
                            >
                              <Icon
                                name={cat.icon || "tag"}
                                className="h-4 w-4 text-white"
                              />
                            </div>
                            <span className="font-bold text-xs text-[var(--text)] truncate">
                              {cat.name}
                            </span>
                            {cat.is_archived && (
                              <span className="shrink-0 rounded-full border border-[var(--border)] px-1.5 text-[10px] font-semibold text-[var(--text-secondary)]">Diarsipkan</span>
                            )}
                          </div>
                          <div className="text-right shrink-0">
                            {drill ? (
                              <Link
                                href={drill}
                                aria-label={`${bal(cat.spent)}, lihat transaksi ${cat.name}`}
                                className="inline-flex min-h-6 items-center text-xs font-black tabular tracking-tight text-[var(--text)] underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                              >
                                {bal(cat.spent)}
                              </Link>
                            ) : (
                              <span className="text-xs font-black tabular tracking-tight text-[var(--text)]">
                                {bal(cat.spent)}
                              </span>
                            )}
                            <div className="text-[9px] text-[var(--muted)] font-medium">Terpakai</div>
                          </div>
                        </div>

                        {/* Row 2: 6px Smooth Progress Bar (if budget set) */}
                        {hasBudget && (
                          <div className="w-full h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden">
                            <div
                              style={{
                                width: `${Math.min(cat.percentage_used, 100)}%`,
                              }}
                              className={cn(
                                "h-full rounded-full transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none",
                                isOver
                                  ? "bg-rose-500 shadow-rose-500/30"
                                  : isWarning
                                  ? "bg-amber-500 shadow-amber-500/30"
                                  : "bg-emerald-500 shadow-emerald-500/30"
                              )}
                            />
                          </div>
                        )}

                        {/* Row 3: Status Badge & Remaining / Limit Info */}
                        <div className="flex items-center justify-between gap-2 text-[11px] tabular">
                          <span
                            className={cn(
                              "px-2 py-0.5 rounded-full text-[10px] font-bold tabular",
                              isOver
                                ? "bg-rose-500/10 text-rose-500 border border-rose-500/20"
                                : isWarning
                                ? "bg-amber-500/10 text-amber-500 border border-amber-500/20"
                                : hasBudget
                                ? "bg-emerald-500/10 text-emerald-500 border border-emerald-500/20"
                                : "bg-[var(--surface-raised)] text-[var(--muted)] border border-[var(--border)]"
                            )}
                          >
                            {isOver
                              ? `✕ ${cat.percentage_used}% Lewat`
                              : isWarning
                              ? `▲ ${cat.percentage_used}% Waspada`
                              : hasBudget
                              ? `● ${cat.percentage_used}% Aman`
                              : "Tanpa Batas"}
                          </span>

                          <div className="text-right text-[11px] font-medium">
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
                                <span className="text-[10px] text-[var(--muted)] font-normal ml-1">
                                  / {bal(cat.budget)}
                                </span>
                              </span>
                            ) : (
                              <span className="text-[var(--muted)] text-[10px]">Belum ada batas</span>
                            )}
                          </div>
                        </div>

                        {/* Row 4: Subtle Touch Actions */}
                        {managed && (
                        <div className="border-t border-[var(--border)]/50 pt-2 flex items-center justify-between gap-2">
                          <button
                            type="button"
                            onClick={() => openEditCategory(cat)}
                            className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-[11px] font-semibold text-[var(--text)] border border-[var(--border)] transition-colors cursor-pointer"
                          >
                            <Icon name="edit" className="h-3 w-3 text-[var(--muted)]" />
                            <span>{hasBudget ? "Ubah Batas" : "Atur Batas"}</span>
                          </button>

                          <ConfirmActionButton
                            label={`Arsipkan kategori ${cat.name}`}
                            confirmation={`Yakin ingin mengarsipkan kategori "${cat.name}"?`}
                            onConfirm={() => deleteCategoryMutation.mutate(cat.id)}
                            disabled={deleteCategoryMutation.isPending}
                            className="p-1.5 rounded-lg text-[var(--muted)] hover:text-rose-500 hover:bg-rose-500/10 transition-colors disabled:opacity-50 cursor-pointer"
                          >
                            <Icon name="trash" className="h-3.5 w-3.5" />
                          </ConfirmActionButton>
                        </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </>
            )}
          </div>

          <section
            aria-labelledby="income-category-heading"
            className="cockpit-card space-y-4 border border-[var(--border)] p-5 shadow-xs sm:p-6"
          >
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h2 id="income-category-heading" className="text-base font-bold tracking-tight text-[var(--text)]">
                  Kategori Pemasukan
                </h2>
                <p className="mt-0.5 text-xs text-[var(--muted)]">
                  Kelola kategori yang tersedia saat mencatat pemasukan.
                </p>
              </div>
              <button
                type="button"
                onClick={() => openCreateCategory("income")}
                className="inline-flex min-h-10 items-center justify-center gap-1.5 self-start rounded-xl bg-emerald-500 px-3.5 py-1.5 text-xs font-bold text-white shadow-xs transition-colors hover:bg-emerald-400 sm:self-auto"
              >
                <Icon name="plus" className="h-3.5 w-3.5" />
                Tambah Kategori Pemasukan
              </button>
            </div>

            {incomeCategories.length === 0 ? (
              <p className="py-5 text-center text-xs text-[var(--muted)]">
                Belum ada kategori pemasukan.
              </p>
            ) : (
              <ul className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {incomeCategories.map((category) => (
                  <li
                    key={category.id}
                    className="flex min-w-0 items-center justify-between gap-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3"
                  >
                    <div className="flex min-w-0 items-center gap-2">
                      <span
                        aria-hidden="true"
                        className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg"
                        style={{ color: category.color, backgroundColor: `${category.color}18` }}
                      >
                        <Icon name={category.icon || "tag"} className="h-4 w-4" />
                      </span>
                      <span className="truncate text-sm font-semibold text-[var(--text)]">
                        {category.name}
                        {category.is_archived && (
                          <span className="ml-1 text-[10px] font-medium text-[var(--muted)]">(diarsipkan)</span>
                        )}
                      </span>
                    </div>
                    <div className="flex shrink-0 items-center gap-1">
                      <button
                        type="button"
                        aria-label={`Ubah kategori ${category.name}`}
                        onClick={() => openEditCategory(category)}
                        className="min-h-9 min-w-9 rounded-lg text-[var(--muted)] transition-colors hover:bg-[var(--surface)] hover:text-[var(--text)]"
                      >
                        <Icon name="edit" className="mx-auto h-3.5 w-3.5" />
                      </button>
                      {!category.is_archived && (
                        <ConfirmActionButton
                          label={`Arsipkan kategori ${category.name}`}
                          confirmation={`Yakin ingin mengarsipkan kategori "${category.name}"?`}
                          onConfirm={() => deleteCategoryMutation.mutate(category.id)}
                          disabled={deleteCategoryMutation.isPending}
                          className="min-h-9 min-w-9 rounded-lg text-[var(--muted)] transition-colors hover:bg-rose-500/10 hover:text-rose-500 disabled:opacity-50"
                        >
                          <Icon name="trash" className="mx-auto h-3.5 w-3.5" />
                        </ConfirmActionButton>
                      )}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>

          {archivedCategories.length > 0 && (
            <section
              aria-labelledby="archived-category-heading"
              className="cockpit-card space-y-4 border border-[var(--border)] p-5 shadow-xs sm:p-6"
            >
              <div>
                <h2 id="archived-category-heading" className="text-base font-bold tracking-tight text-[var(--text)]">
                  Kategori Diarsipkan
                </h2>
                <p className="mt-0.5 text-xs text-[var(--muted)]">
                  Pulihkan kategori agar dapat dipilih kembali pada transaksi baru.
                </p>
              </div>
              <ul className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {archivedCategories.map((category) => (
                  <li
                    key={category.id}
                    className="flex min-w-0 items-center justify-between gap-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-sm font-semibold text-[var(--text)]">{category.name}</p>
                      <p className="text-[10px] capitalize text-[var(--muted)]">
                        {category.kind === "income" ? "Pemasukan" : "Pengeluaran"}
                      </p>
                    </div>
                    <button
                      type="button"
                      aria-label={`Pulihkan kategori ${category.name}`}
                      onClick={() => restoreCategoryMutation.mutate(category.id)}
                      disabled={restoreCategoryMutation.isPending}
                      className="min-h-9 shrink-0 rounded-lg border border-[var(--border)] px-3 text-xs font-semibold text-[var(--text)] transition-colors hover:bg-[var(--surface)] disabled:opacity-50"
                    >
                      Pulihkan
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </div>
      )}

      {/* ===================== TAB 2: COMPARE & KAKEIBO ===================== */}
      {activeTab === "compare" && (
        <div className="space-y-6">
          <PeriodComparison basis={insightBasis} offset={insightOffset} bal={bal} />
          <CashFlowHistory basis={insightBasis} bal={bal} />
          <RecurringCharges bal={bal} onCreatePlan={setPlanDraft} />
          <QuestionRate />

          <KakeiboBenchmark kakeibo={kakeibo} pillarHref={pillarHref} />
        </div>
      )}

      <RecurringRulesModal open={planDraft !== null} onClose={() => setPlanDraft(null)} expenseDraft={planDraft} />

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
          <form
            className="space-y-4 pt-1 text-sm"
            onSubmit={(event) => {
              event.preventDefault();
              if (!saveCategoryMutation.isPending) saveCategoryMutation.mutate();
            }}
          >
            {catError && (
              <div role="alert" aria-live="assertive" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {catError}
              </div>
            )}

            <FormSection title="Identitas kategori" description="Jenis kategori menentukan pilihan yang muncul saat mencatat transaksi." className="border-t-0 pt-0">
            <div>
              <label htmlFor="category-name" className="text-xs font-medium text-[var(--muted)] block mb-1">
                Nama Kategori
              </label>
              <input
                id="category-name"
                name="name"
                type="text"
                required
                maxLength={100}
                value={catName}
                onChange={(e) => setCatName(e.target.value)}
                placeholder="Contoh: Langganan Streaming, Asuransi, dsb"
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-semibold text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
              />
            </div>

            <div>
              <label htmlFor="category-kind" className="text-xs font-medium text-[var(--muted)] block mb-1">Jenis Kategori</label>
              <select
                id="category-kind"
                name="kind"
                value={catKind}
                onChange={(event) => setCatKind(event.target.value as "expense" | "income")}
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              >
                <option value="expense">Pengeluaran</option>
                <option value="income">Pemasukan</option>
              </select>
            </div>

            {catKind === "expense" && (
              <div>
                <div>
                  <label htmlFor="category-kakeibo" className="text-xs font-medium text-[var(--muted)] block mb-1">Pilar Kakeibo bawaan</label>
                  <select id="category-kakeibo" name="kakeibo_type" value={catKakeiboType} onChange={(event) => setCatKakeiboType(event.target.value as typeof catKakeiboType)} className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]">
                    {KAKEIBO_PILLARS.map((pillar) => (
                      <option key={pillar.value} value={pillar.value}>{pillar.label}</option>
                    ))}
                  </select>
                  <p className="mt-1 text-[11px] text-[var(--muted)]">{kakeiboPillarInfo(catKakeiboType)?.help}</p>
                </div>
              </div>
            )}
            {catKind === "expense" && <p className="text-xs text-[var(--muted)]">Biaya hidup pokok dan Ketahanan Dana memakai pengeluaran berpilar Kebutuhan.</p>}
            </FormSection>

            <FormSection title="Tampilan kategori">
            <div>
              <p className="mb-1.5 text-xs font-medium text-[var(--muted)]">Ikon kategori</p>
              <div role="group" aria-label="Pilih ikon kategori" className="grid grid-cols-5 gap-2">
                {CATEGORY_ICONS.map((ic) => (
                  <button
                    key={ic.id}
                    type="button"
                    onClick={() => setCatIcon(ic.id)}
                    aria-pressed={catIcon === ic.id}
                    aria-label={`Ikon ${ic.label}`}
                    className={cn(
                      "flex min-h-11 flex-col items-center justify-center rounded-xl border p-2 transition-[border-color,background-color,color] cursor-pointer",
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

            <div>
              <p className="mb-1.5 text-xs font-medium text-[var(--muted)]">Warna kategori</p>
              <div role="group" aria-label="Pilih warna kategori" className="flex flex-wrap items-center gap-2">
                {CATEGORY_COLORS.map((c) => (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setCatColor(c)}
                    style={{ backgroundColor: c }}
                    aria-pressed={catColor === c}
                    aria-label={`Warna ${CATEGORY_COLOR_NAMES[c] ?? c}`}
                    className={cn(
                      "min-h-11 min-w-11 rounded-full border-4 border-[var(--surface)] transition-transform cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--text)]",
                      catColor === c
                        ? "ring-2 ring-offset-2 ring-offset-[var(--surface)] ring-emerald-500 scale-110"
                        : "opacity-80 hover:opacity-100 hover:scale-105"
                    )}
                  />
                ))}
              </div>
            </div>
            </FormSection>

            {catKind === "expense" && <FormSection title="Anggaran"><div>
              <label htmlFor="category-monthly-budget" className="text-xs font-medium text-[var(--muted)] block mb-1">
                Batas Anggaran Bulanan (IDR)
              </label>
              <input
                id="category-monthly-budget"
                name="monthly_budget"
                type="text"
                inputMode="numeric"
                value={catBudget}
                onChange={(e) =>
                  setCatBudget(formatNumberWithDots(e.target.value))
                }
                placeholder="Kosongkan jika tanpa batas"
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-semibold tabular text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
              />
              <p className="text-[11px] text-[var(--muted)] mt-1">
                Batas pengeluaran bulanan untuk memantau disiplin anggaran kategori.
              </p>
            </div></FormSection>}

            <div className="flex items-center justify-end gap-2 pt-3 border-t border-[var(--border)]">
              <button
                type="button"
                onClick={() => setCatModalOpen(false)}
                className="px-4 py-2 rounded-xl border border-[var(--border)] text-[var(--muted)] hover:bg-[var(--surface-raised)] cursor-pointer"
              >
                Batal
              </button>
              <button
                type="submit"
                disabled={saveCategoryMutation.isPending}
                className="min-h-11 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50 cursor-pointer"
              >
                {saveCategoryMutation.isPending
                  ? "Menyimpan…"
                  : editingCategory
                  ? "Simpan Perubahan"
                  : "Buat Kategori"}
              </button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  );
}
