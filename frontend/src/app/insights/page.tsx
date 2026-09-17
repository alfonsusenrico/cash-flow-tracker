"use client";

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, formatNumberWithDots } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { StatCard } from "@/components/ui/StatCard";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { TakeawayBanner } from "@/components/ui/TakeawayBanner";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from "recharts";

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
  "#f97316",
  "#10b981",
  "#3b82f6",
  "#8b5cf6",
  "#ec4899",
  "#ef4444",
  "#eab308",
  "#14b8a6",
  "#64748b",
];

export default function AnalyticsPage() {
  const qc = useQueryClient();
  const { timeframe, cycleOffset, bal } = useAppCtx();

  // Active view tab: summary vs compare/kakeibo
  const [activeTab, setActiveTab] = useState<"summary" | "kakeibo">("summary");

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

  if (isLoading) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-10 w-72 rounded-xl bg-[var(--canvas-subtle)]" />
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-28 rounded-2xl bg-[var(--canvas-subtle)]" />
          ))}
        </div>
        <div className="h-80 rounded-2xl bg-[var(--canvas-subtle)]" />
        <div className="h-64 rounded-2xl bg-[var(--canvas-subtle)]" />
      </div>
    );
  }

  const burnRate = data?.burn_rate;
  const cadence = data?.daily_cadence ?? [];
  const rawCategories = data?.category_variance ?? [];
  const kakeibo = data?.kakeibo;
  const narrative = data?.narrative;
  const timeframeLabel = data?.timeframe_label || "Siklus Aktif";

  // Filter out internal transfers and movements
  const categories = rawCategories.filter(
    (c: any) =>
      c.kind !== "income" &&
      c.name !== "Internal Movement" &&
      c.name !== "Investasi" &&
      !c.is_excluded_from_budget
  );

  const totalSpentInCategories = categories.reduce((acc: number, c: any) => acc + (c.spent || 0), 0);
  const totalBudgeted = categories.reduce((acc: number, c: any) => acc + (c.budget || 0), 0);

  // Find max peak daily expense for highlighting (Ref 4)
  const maxDayExpense = cadence.reduce(
    (max: number, d: any) => Math.max(max, d.expense || 0),
    0
  );

  // Daily target benchmark line
  const dailyBenchmark = totalBudgeted > 0 ? Math.round(totalBudgeted / 30) : 0;

  return (
    <div className="space-y-5 select-none">
      {/* 1. Header & Segmented Tab Switch */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-1">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl sm:text-2xl font-bold tracking-tight text-[var(--text-primary)]">
              Analisis Pengeluaran & Anggaran
            </h1>
            <span className="text-xs font-semibold px-2 py-0.5 rounded-full bg-[var(--canvas-subtle)] text-[var(--text-secondary)] border border-[var(--border-structural)]">
              {timeframeLabel}
            </span>
          </div>
          <p className="text-xs text-[var(--text-muted)] font-medium mt-0.5">
            Evaluasi laju belanja harian dan pemenuhan alokasi batas pos keuangan
          </p>
        </div>

        <div className="flex items-center gap-2">
          {/* Segmented Control Switch */}
          <div className="flex items-center p-1 rounded-xl bg-[var(--canvas-subtle)] border border-[var(--border-structural)]">
            <button
              type="button"
              onClick={() => setActiveTab("summary")}
              className={cn(
                "px-3.5 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer",
                activeTab === "summary"
                  ? "bg-[#1E201E] text-white shadow-xs"
                  : "text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
              )}
            >
              Laju & Kategori
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("kakeibo")}
              className={cn(
                "px-3.5 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer",
                activeTab === "kakeibo"
                  ? "bg-[#1E201E] text-white shadow-xs"
                  : "text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
              )}
            >
              Pilar Kakeibo
            </button>
          </div>

          <button
            type="button"
            onClick={openCreateCategory}
            className="btn-lime px-3 py-1.5 text-xs font-bold rounded-xl"
          >
            + Pos Kategori
          </button>
        </div>
      </div>

      {/* 2. Top Summary Stat Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3.5">
        <StatCard
          title="Rata-rata Laju Belanja (7 Hari)"
          value={bal(burnRate?.daily_burn_7d ?? 0)}
          subtitle={
            dailyBenchmark > 0
              ? `Target harian: ${bal(dailyBenchmark)}/hari`
              : "Laju pergerakan harian terkini"
          }
          badge={{
            text: (burnRate?.daily_burn_7d ?? 0) > dailyBenchmark && dailyBenchmark > 0 ? "Laju Tinggi" : "Normal",
            variant: (burnRate?.daily_burn_7d ?? 0) > dailyBenchmark && dailyBenchmark > 0 ? "warning" : "success",
          }}
          icon={<Icon name="clock" className="h-4 w-4 text-amber-600" />}
          iconBg="bg-amber-500/10"
        />

        <StatCard
          title="Total Belanja Pos Kategori"
          value={bal(totalSpentInCategories)}
          subtitle={`Total Pagu Anggaran: ${bal(totalBudgeted)}`}
          badge={{
            text: totalBudgeted > 0 ? `${Math.round((totalSpentInCategories / totalBudgeted) * 100)}% Terpakai` : "Tanpa Limit",
            variant: totalSpentInCategories > totalBudgeted && totalBudgeted > 0 ? "danger" : "neutral",
          }}
          icon={<Icon name="tag" className="h-4 w-4 text-sky-600" />}
          iconBg="bg-sky-500/10"
        />

        <StatCard
          title="Proyeksi Pengeluaran Siklus"
          value={bal(burnRate?.projected_cycle_outflow ?? totalSpentInCategories)}
          subtitle="Estimasi akhir siklus jika laju konstan"
          badge={{ text: "Forecast", variant: "info" }}
          icon={<Icon name="trending-up" className="h-4 w-4 text-emerald-600" />}
          iconBg="bg-emerald-500/10"
        />
      </div>

      {/* 3. Analytical Takeaway Banner (Ref 1 & Ref 7) */}
      <TakeawayBanner
        title="Evaluasi Keuangan Siklus Ini"
        badgeText="Rekomendasi Pintar"
        variant="sage"
      >
        {narrative?.summary ? (
          <span>{narrative.summary}</span>
        ) : (
          <span>
            Laju belanja harian terkendali. Tidak ditemukan lonjakan pengeluaran berlebih di luar kebutuhan pokok.
          </span>
        )}
      </TakeawayBanner>

      {/* 4. Tab 1: Summary & Category Benchmarks */}
      {activeTab === "summary" && (
        <div className="space-y-5">
          {/* A. Daily Spending Cadence Bar Chart (Ref 4 Aesthetic) */}
          <div className="card-crisp p-5 space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">
                  Laju Belanja Harian (Daily Spending Cadence)
                </h2>
                <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
                  Distribusi arus uang keluar setiap hari dalam siklus aktif
                </p>
              </div>

              <div className="flex items-center gap-3 text-xs">
                <div className="flex items-center gap-1.5 text-[var(--text-secondary)]">
                  <span className="h-2.5 w-2.5 rounded-sm bg-[#66CC55]" />
                  <span className="text-[11px] font-medium">Hari Puncak (Peak)</span>
                </div>
                <div className="flex items-center gap-1.5 text-[var(--text-secondary)]">
                  <span className="h-2.5 w-2.5 rounded-sm bg-[var(--border-structural)]" />
                  <span className="text-[11px] font-medium">Hari Biasa</span>
                </div>
              </div>
            </div>

            {cadence.length === 0 ? (
              <div className="py-16 text-center text-xs text-[var(--text-muted)]">
                Belum ada data belanja di siklus ini.
              </div>
            ) : (
              <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={cadence} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                    <XAxis
                      dataKey="label"
                      stroke="#8E948B"
                      fontSize={11}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis
                      stroke="#8E948B"
                      fontSize={11}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(v) => (v >= 1000000 ? `${(v / 1000000).toFixed(1)}M` : `${Math.round(v / 1000)}k`)}
                    />
                    <Tooltip
                      cursor={{ fill: "rgba(0,0,0,0.03)" }}
                      content={({ active, payload }) => {
                        if (!active || !payload?.length) return null;
                        const d = payload[0].payload;
                        return (
                          <div className="rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] p-2.5 shadow-lg text-xs">
                            <span className="font-bold text-[var(--text-primary)] block">
                              {d.label}
                            </span>
                            <span className="text-rose-600 font-bold tabular block mt-0.5">
                              Belanja: {bal(d.expense)}
                            </span>
                            {d.income > 0 && (
                              <span className="text-emerald-600 font-bold tabular block mt-0.5">
                                Masuk: {bal(d.income)}
                              </span>
                            )}
                          </div>
                        );
                      }}
                    />
                    {dailyBenchmark > 0 && (
                      <ReferenceLine
                        y={dailyBenchmark}
                        stroke="#B45309"
                        strokeDasharray="3 3"
                        label={{
                          value: `Limit: ${bal(dailyBenchmark)}`,
                          fill: "#B45309",
                          fontSize: 10,
                          position: "top",
                        }}
                      />
                    )}
                    <Bar dataKey="expense" radius={[6, 6, 0, 0]}>
                      {cadence.map((entry: any, index: number) => {
                        const isPeak = entry.expense === maxDayExpense && entry.expense > 0;
                        return (
                          <Cell
                            key={`cell-${index}`}
                            fill={isPeak ? "#66CC55" : "var(--border-strong)"}
                          />
                        );
                      })}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>

          {/* B. Bullet Benchmark Category Cards (Ref 7 Aesthetic) */}
          <div className="card-crisp p-5 space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-sm font-bold text-[var(--text-primary)] tracking-tight">
                  Evaluasi Anggaran per Kategori (Bullet Benchmarks)
                </h2>
                <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
                  Realisasi belanja dibandingkan pagu anggaran bulanan
                </p>
              </div>

              <span className="text-xs text-[var(--text-muted)] font-medium">
                {categories.length} Pos Kategori
              </span>
            </div>

            {categories.length === 0 ? (
              <div className="py-12 text-center text-xs text-[var(--text-muted)] space-y-2">
                <div>Belum ada pos kategori belanja.</div>
                <button
                  type="button"
                  onClick={openCreateCategory}
                  className="btn-lime px-3 py-1.5 text-xs rounded-xl"
                >
                  + Tambah Kategori
                </button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5">
                {categories.map((cat: any) => {
                  const spent = cat.spent || 0;
                  const budget = cat.budget || 0;
                  const pct = budget > 0 ? Math.round((spent / budget) * 100) : 0;
                  const isOver = budget > 0 && spent > budget;
                  const isWarning = budget > 0 && pct >= 80 && !isOver;

                  return (
                    <div
                      key={cat.id}
                      className="p-3.5 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-app)] space-y-2.5"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2.5 min-w-0">
                          <span
                            className="h-7 w-7 rounded-lg flex items-center justify-center text-white text-xs font-bold shrink-0 shadow-xs"
                            style={{ backgroundColor: cat.color || "#0284C7" }}
                          >
                            <Icon name={cat.icon || "tag"} className="h-3.5 w-3.5 text-white" />
                          </span>
                          <div className="min-w-0">
                            <span className="font-bold text-xs text-[var(--text-primary)] truncate block">
                              {cat.name}
                            </span>
                            <span className="text-[10px] text-[var(--text-muted)] truncate block">
                              {cat.is_primary ? "Pokok (Need)" : "Keinginan (Want)"}
                            </span>
                          </div>
                        </div>

                        <div className="flex items-center gap-1.5">
                          {budget > 0 ? (
                            <StatusBadge
                              variant={isOver ? "danger" : isWarning ? "warning" : "success"}
                            >
                              {isOver ? `Over ${pct}%` : `${pct}%`}
                            </StatusBadge>
                          ) : (
                            <StatusBadge variant="neutral">No Limit</StatusBadge>
                          )}

                          <button
                            type="button"
                            onClick={() => openEditCategory(cat)}
                            className="p-1 rounded-lg text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--canvas-subtle)] transition-colors cursor-pointer"
                            title="Ubah Anggaran"
                          >
                            <Icon name="edit" className="h-3 w-3" />
                          </button>
                        </div>
                      </div>

                      {/* Bullet Progress Track with Target Tick */}
                      <div className="space-y-1">
                        <div className="relative w-full h-2 rounded-full bg-[var(--canvas-subtle)] overflow-hidden">
                          <div
                            style={{
                              width: `${Math.min(pct, 100)}%`,
                              backgroundColor: isOver ? "#DC2626" : isWarning ? "#B45309" : cat.color || "#0284C7",
                            }}
                            className="h-full rounded-full transition-all duration-300"
                          />
                        </div>

                        <div className="flex items-center justify-between text-[11px] text-[var(--text-muted)] tabular font-medium pt-0.5">
                          <span>Realisasi: <strong className="text-[var(--text-primary)]">{bal(spent)}</strong></span>
                          <span>Budget: <strong className="text-[var(--text-primary)]">{budget > 0 ? bal(budget) : "—"}</strong></span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      )}

      {/* 5. Tab 2: Kakeibo 50/30/20 Pillar Cards */}
      {activeTab === "kakeibo" && (
        <div className="space-y-5">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Pillar 1: Need */}
            <div className="card-crisp p-5 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold uppercase tracking-wider text-sky-600">
                  🍞 Pokok (Need)
                </span>
                <StatusBadge variant={kakeibo?.need_pct > 50 ? "warning" : "success"}>
                  Target ≤ 50%
                </StatusBadge>
              </div>
              <div className="text-2xl font-bold tabular text-[var(--text-primary)]">
                {bal(kakeibo?.need_spent ?? 0)}
              </div>
              <div className="text-xs text-[var(--text-muted)] font-medium">
                Porsi Pengeluaran: <strong className="text-[var(--text-primary)]">{kakeibo?.need_pct ?? 0}%</strong>
              </div>
              <p className="text-[11px] text-[var(--text-muted)] leading-relaxed">
                Mencakup kebutuhan tempat tinggal, makanan pokok, tagihan rutin listrik/air/internet, dan transportasi harian.
              </p>
            </div>

            {/* Pillar 2: Want */}
            <div className="card-crisp p-5 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold uppercase tracking-wider text-rose-600">
                  👑 Keinginan (Want)
                </span>
                <StatusBadge variant={kakeibo?.want_pct > 30 ? "danger" : "success"}>
                  Target ≤ 30%
                </StatusBadge>
              </div>
              <div className="text-2xl font-bold tabular text-[var(--text-primary)]">
                {bal(kakeibo?.want_spent ?? 0)}
              </div>
              <div className="text-xs text-[var(--text-muted)] font-medium">
                Porsi Pengeluaran: <strong className="text-[var(--text-primary)]">{kakeibo?.want_pct ?? 0}%</strong>
              </div>
              <p className="text-[11px] text-[var(--text-muted)] leading-relaxed">
                Mencakup gaya hidup, nongkrong, belanja pakaian, langganan hiburan, dan liburan fleksibel.
              </p>
            </div>

            {/* Pillar 3: Saving */}
            <div className="card-crisp p-5 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold uppercase tracking-wider text-emerald-600">
                  💎 Tabungan (Saving)
                </span>
                <StatusBadge variant={kakeibo?.saving_pct >= 20 ? "success" : "warning"}>
                  Target ≥ 20%
                </StatusBadge>
              </div>
              <div className="text-2xl font-bold tabular text-[var(--text-primary)]">
                {bal(kakeibo?.saving_spent ?? 0)}
              </div>
              <div className="text-xs text-[var(--text-muted)] font-medium">
                Porsi Alokasi: <strong className="text-[var(--text-primary)]">{kakeibo?.saving_pct ?? 0}%</strong>
              </div>
              <p className="text-[11px] text-[var(--text-muted)] leading-relaxed">
                Dana darurat, tabungan tujuan masa depan, dan alokasi instrumen investasi saham/reksadana.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* ===================== CATEGORY ADD / EDIT MODAL ===================== */}
      <Modal
        open={catModalOpen}
        onClose={() => setCatModalOpen(false)}
        title={editingCategory ? "Ubah Pos Kategori" : "Tambah Pos Kategori Baru"}
      >
        <div className="space-y-4 pt-1 text-xs">
          {catError && (
            <div className="p-2.5 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500">
              {catError}
            </div>
          )}

          <div>
            <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
              Nama Pos Kategori
            </label>
            <input
              type="text"
              value={catName}
              onChange={(e) => setCatName(e.target.value)}
              placeholder="Contoh: Kopi & Cafe, Makanan, Gym"
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3.5 py-2 text-xs font-semibold text-[var(--text-primary)] outline-none"
            />
          </div>

          <div>
            <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
              Batas Pagu Anggaran (IDR per Bulan)
            </label>
            <input
              type="text"
              inputMode="numeric"
              value={catBudget}
              onChange={(e) => setCatBudget(formatNumberWithDots(e.target.value))}
              placeholder="0 (Kosongkan jika tanpa limit)"
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3.5 py-2 text-base font-bold tabular text-[var(--text-primary)] outline-none"
            />
          </div>

          <div>
            <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1.5">
              Pilar Kakeibo
            </label>
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setCatIsPrimary(true)}
                className={cn(
                  "py-2 rounded-xl text-xs font-bold border transition-colors cursor-pointer text-center",
                  catIsPrimary
                    ? "bg-[#1E201E] text-white border-transparent"
                    : "bg-[var(--canvas-subtle)] text-[var(--text-secondary)] border-[var(--border-structural)]"
                )}
              >
                🍞 Kebutuhan Pokok (Need)
              </button>
              <button
                type="button"
                onClick={() => setCatIsPrimary(false)}
                className={cn(
                  "py-2 rounded-xl text-xs font-bold border transition-colors cursor-pointer text-center",
                  !catIsPrimary
                    ? "bg-[#1E201E] text-white border-transparent"
                    : "bg-[var(--canvas-subtle)] text-[var(--text-secondary)] border-[var(--border-structural)]"
                )}
              >
                👑 Keinginan (Want)
              </button>
            </div>
          </div>

          <div>
            <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1.5">
              Ikon
            </label>
            <div className="flex flex-wrap gap-2">
              {CATEGORY_ICONS.map((ic) => (
                <button
                  key={ic.id}
                  type="button"
                  onClick={() => setCatIcon(ic.id)}
                  className={cn(
                    "p-2 rounded-xl border flex items-center gap-1.5 cursor-pointer transition-colors text-xs",
                    catIcon === ic.id
                      ? "bg-[#1E201E] text-white border-transparent"
                      : "bg-[var(--canvas-subtle)] text-[var(--text-secondary)] border-[var(--border-structural)]"
                  )}
                >
                  <Icon name={ic.id as any} className="h-3.5 w-3.5" />
                  <span>{ic.label}</span>
                </button>
              ))}
            </div>
          </div>

          <div>
            <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1.5">
              Warna
            </label>
            <div className="flex items-center gap-2">
              {CATEGORY_COLORS.map((clr) => (
                <button
                  key={clr}
                  type="button"
                  onClick={() => setCatColor(clr)}
                  style={{ backgroundColor: clr }}
                  className={cn(
                    "h-6 w-6 rounded-full cursor-pointer transition-transform",
                    catColor === clr ? "ring-2 ring-offset-2 ring-[#1E201E] scale-110" : "opacity-80 hover:opacity-100"
                  )}
                />
              ))}
            </div>
          </div>

          <div className="flex items-center justify-between pt-2 border-t border-[var(--border-structural)]">
            {editingCategory && (
              <button
                type="button"
                disabled={deleteCategoryMutation.isPending}
                onClick={() => {
                  if (confirm(`Hapus kategori ${editingCategory.name}?`)) {
                    deleteCategoryMutation.mutate(editingCategory.id);
                  }
                }}
                className="text-xs text-rose-600 hover:underline font-semibold cursor-pointer"
              >
                Hapus
              </button>
            )}

            <div className="flex gap-2 ml-auto">
              <button
                type="button"
                onClick={() => setCatModalOpen(false)}
                className="btn-secondary py-2 px-3.5 rounded-xl text-xs"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={saveCategoryMutation.isPending}
                onClick={() => saveCategoryMutation.mutate()}
                className="btn-charcoal py-2 px-4 rounded-xl text-xs font-semibold"
              >
                {saveCategoryMutation.isPending ? "Menyimpan..." : "Simpan Kategori"}
              </button>
            </div>
          </div>
        </div>
      </Modal>
    </div>
  );
}
