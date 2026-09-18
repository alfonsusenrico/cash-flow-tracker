"use client";

import { useState } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";
import { chartTooltipStyle } from "@/components/charts/theme";
import { cn } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";

export interface CategoryBreakdownItem {
  id: string;
  name: string;
  icon: string | null;
  color: string | null;
  spent: number;
  percentage_of_total: number;
  budget: number | null;
  percentage_used: number | null;
  is_primary?: boolean;
  kakeibo_type?: string;
}

interface CategoryDonutChartProps {
  categories: CategoryBreakdownItem[];
  title?: string;
  subtitle?: string;
}

const PALETTE = [
  "#10b981", // Emerald
  "#3b82f6", // Blue
  "#f43f5e", // Rose
  "#f59e0b", // Amber
  "#8b5cf6", // Purple
  "#06b6d4", // Cyan
  "#ec4899", // Pink
  "#64748b", // Slate
];

export function CategoryDonutChart({
  categories,
  title = "Distribusi Pengeluaran",
  subtitle = "Alokasi pengeluaran berdasarkan kategori",
}: CategoryDonutChartProps) {
  const { bal } = useAppCtx();
  const [activeIndex, setActiveIndex] = useState<number | null>(null);

  // Filter only categories with spend > 0 for the donut
  const activeCategories = categories.filter((c) => c.spent > 0);
  const totalSpent = activeCategories.reduce((acc, c) => acc + c.spent, 0);

  if (activeCategories.length === 0) {
    return (
      <div className="card-squircle p-5 sm:p-6 space-y-3 shadow-xs">
        <h2 className="text-base font-bold tracking-tight text-[var(--text)]">{title}</h2>
        <div className="py-12 text-center text-xs text-[var(--muted)]">
          Belum ada catatan pengeluaran per kategori pada periode ini.
        </div>
      </div>
    );
  }

  const dominantCategory = activeCategories.reduce(
    (max, c) => (c.spent > max.spent ? c : max),
    activeCategories[0]
  );

  const chartData = activeCategories.map((c, i) => ({
    name: c.name,
    value: c.spent,
    color: c.color || PALETTE[i % PALETTE.length],
    icon: c.icon,
    percentage: c.percentage_of_total,
    kakeibo_type: c.kakeibo_type || (c.is_primary ? "need" : "want"),
  }));

  const activeCategory = activeIndex !== null ? chartData[activeIndex] : null;

  return (
    <div className="card-squircle p-5 sm:p-6 space-y-4 shadow-xs">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-base font-bold tracking-tight text-[var(--text)]">{title}</h2>
          <p className="text-xs text-[var(--muted)] mt-0.5">{subtitle}</p>
        </div>
        <span className="text-[10px] font-medium px-2 py-0.5 rounded-full bg-[var(--surface-raised)] text-[var(--muted)] border border-[var(--border)]">
          {activeCategories.length} Kategori
        </span>
      </div>

      {/* Donut Chart with Dynamic Center Display */}
      <div className="relative h-56 sm:h-60 w-full flex items-center justify-center">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={chartData}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="50%"
              innerRadius={68}
              outerRadius={96}
              paddingAngle={2}
              onMouseEnter={(_, index) => setActiveIndex(index)}
              onMouseLeave={() => setActiveIndex(null)}
            >
              {chartData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.color}
                  stroke="var(--surface)"
                  strokeWidth={2}
                  className="transition-opacity hover:opacity-85 cursor-pointer"
                />
              ))}
            </Pie>
            <Tooltip
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const data = payload[0].payload;
                  return (
                    <div style={chartTooltipStyle} className="space-y-1">
                      <div className="text-xs font-semibold text-[var(--text)]">{data.name}</div>
                      <div className="text-xs font-bold text-rose-500 tabular">
                        {bal(data.value)}
                      </div>
                      <div className="text-[10px] text-[var(--muted)]">
                        {data.percentage}% dari total belanja
                      </div>
                    </div>
                  );
                }
                return null;
              }}
            />
          </PieChart>
        </ResponsiveContainer>

        {/* Center Label in Donut Hole: Defaults to dominant category or hover */}
        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none select-none">
          <span className="text-[10px] font-bold uppercase tracking-widest text-[var(--muted)]">
            {activeCategory ? activeCategory.name : dominantCategory ? dominantCategory.name : "Total"}
          </span>
          <span className="text-xl sm:text-2xl font-black text-[var(--text)] tabular mt-0.5">
            {activeCategory
              ? `${activeCategory.percentage}%`
              : dominantCategory
              ? `${dominantCategory.percentage_of_total}%`
              : "100%"}
          </span>
          <span className="text-[11px] text-[var(--muted)] tabular font-medium">
            {activeCategory ? bal(activeCategory.value) : bal(dominantCategory?.spent ?? totalSpent)}
          </span>
        </div>
      </div>

      {/* Dominant Highlight Pill Banner */}
      {dominantCategory && (
        <div className="px-3 py-2 rounded-xl bg-[var(--surface-raised)] border border-[var(--border)] flex items-center justify-between text-xs">
          <span className="text-[var(--muted)] text-[11px] font-medium">Terbanyak:</span>
          <div className="flex items-center gap-1.5 font-semibold text-[var(--text)]">
            <span>{dominantCategory.name}</span>
            <span className="text-rose-500 font-bold tabular">({dominantCategory.percentage_of_total}%)</span>
          </div>
        </div>
      )}

      {/* Top Categories Progress Bars */}
      <div className="space-y-2.5 pt-2 border-t border-[var(--border)] max-h-48 overflow-y-auto">
        {activeCategories.slice(0, 5).map((cat, i) => {
          const color = cat.color || PALETTE[i % PALETTE.length];
          const hasBudget = cat.budget && cat.budget > 0;
          const pctUsed = cat.percentage_used ?? 0;
          const isOver = hasBudget && pctUsed > 100;

          return (
            <div key={cat.id} className="text-xs space-y-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 min-w-0">
                  <span
                    className="h-2.5 w-2.5 rounded-full shrink-0"
                    style={{ backgroundColor: color }}
                  />
                  <span className="font-medium text-[var(--text)] truncate max-w-[150px]">
                    {cat.name}
                  </span>
                </div>
                <div className="flex items-center gap-2 text-[11px] shrink-0 tabular font-medium">
                  <span className="font-semibold text-[var(--text)] tabular">{bal(cat.spent)}</span>
                  <span className="text-[var(--muted)]">({cat.percentage_of_total}%)</span>
                </div>
              </div>

              {/* Progress bar */}
              <div className="w-full h-1.5 rounded-full bg-[var(--surface-raised)] border border-[var(--border)] overflow-hidden">
                <div
                  className={cn(
                    "h-full rounded-full transition-all",
                    isOver ? "bg-rose-500" : ""
                  )}
                  style={{
                    width: `${Math.min(hasBudget ? pctUsed : cat.percentage_of_total, 100)}%`,
                    backgroundColor: isOver ? undefined : color,
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
