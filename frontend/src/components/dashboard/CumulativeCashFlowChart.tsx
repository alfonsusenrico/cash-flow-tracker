"use client";

import { useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { chartAxisProps, chartColors, chartGridProps, chartTooltipStyle, formatAxisCurrency } from "@/components/charts/theme";
import { cn } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";

export interface TrendlinePoint {
  date: string;
  label: string;
  income: number;
  expense: number;
  cumulative_inflow: number;
  cumulative_outflow: number;
  cumulative_net: number;
}

interface CumulativeCashFlowChartProps {
  data: TrendlinePoint[];
  title?: string;
  subtitle?: string;
}

export function CumulativeCashFlowChart({
  data,
  title = "Arus Kas Kumulatif",
  subtitle = "Perbandingan uang masuk vs uang keluar sepanjang waktu",
}: CumulativeCashFlowChartProps) {
  const { bal } = useAppCtx();
  const [viewMode, setViewMode] = useState<"cumulative" | "daily">("cumulative");

  if (!data || data.length === 0) {
    return (
      <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-6 text-center text-xs text-[var(--muted)]">
        Belum ada aktivitas transaksi tercatat pada periode ini.
      </div>
    );
  }

  const latest = data[data.length - 1];
  const netSpread = latest.cumulative_inflow - latest.cumulative_outflow;


  return (
    <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs">
      {/* Header & Controls */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-base font-bold tracking-tight text-[var(--text)]">{title}</h2>
            <span
              className={cn(
                "text-[10px] px-2 py-0.5 rounded-full font-bold tabular",
                netSpread >= 0
                  ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20"
                  : "bg-rose-500/10 text-rose-600 dark:text-rose-400 border border-rose-500/20"
              )}
            >
              Selisih: {netSpread >= 0 ? "+" : ""}
              {bal(netSpread)}
            </span>
          </div>
          <p className="text-xs text-[var(--muted)] mt-0.5">{subtitle}</p>
        </div>

        {/* View Toggle */}
        <div className="flex items-center rounded-lg border border-[var(--border)] bg-[var(--surface-raised)] p-0.5 text-xs">
          <button
            type="button"
            onClick={() => setViewMode("cumulative")}
            className={cn(
              "px-2.5 py-1 rounded-md transition-all text-[11px]",
              viewMode === "cumulative"
                ? "bg-[var(--surface)] text-[var(--text)] font-bold shadow-xs"
                : "text-[var(--muted)] hover:text-[var(--text)] font-medium"
            )}
          >
            Kumulatif
          </button>
          <button
            type="button"
            onClick={() => setViewMode("daily")}
            className={cn(
              "px-2.5 py-1 rounded-md transition-all text-[11px]",
              viewMode === "daily"
                ? "bg-[var(--surface)] text-[var(--text)] font-bold shadow-xs"
                : "text-[var(--muted)] hover:text-[var(--text)] font-medium"
            )}
          >
            Harian
          </button>
        </div>
      </div>

      {/* Chart Canvas */}
      <div className="h-64 sm:h-72 w-full pt-2">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 10, right: 10, left: -15, bottom: 0 }}>
            <defs>
              <linearGradient id="incomeGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={chartColors.income} stopOpacity={0.3} />
                <stop offset="95%" stopColor={chartColors.income} stopOpacity={0.0} />
              </linearGradient>
              <linearGradient id="expenseGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={chartColors.expense} stopOpacity={0.25} />
                <stop offset="95%" stopColor={chartColors.expense} stopOpacity={0.0} />
              </linearGradient>
            </defs>
            <CartesianGrid {...chartGridProps} />
            <XAxis
              dataKey="label"
              {...chartAxisProps}
              interval="preserveStartEnd"
              minTickGap={24}
            />
            <YAxis
              {...chartAxisProps}
              tickFormatter={formatAxisCurrency}
              width={48}
            />
            <Tooltip
              content={(props: any) => {
                if (props.active && props.payload && props.payload.length) {
                  const p = props.payload[0].payload as TrendlinePoint;
                  return (
                    <div style={chartTooltipStyle} className="space-y-1.5 min-w-44">
                      <div className="text-xs font-semibold text-[var(--text)] border-b border-[var(--border)] pb-1">
                        {p.label} ({p.date})
                      </div>
                      {viewMode === "cumulative" ? (
                        <>
                          <div className="flex items-center justify-between gap-4 text-xs tabular font-medium">
                            <span className="text-emerald-500">Uang Masuk:</span>
                            <span className="font-bold text-emerald-500 tabular">+{bal(p.cumulative_inflow)}</span>
                          </div>
                          <div className="flex items-center justify-between gap-4 text-xs tabular font-medium">
                            <span className="text-rose-500">Uang Keluar:</span>
                            <span className="font-bold text-rose-500 tabular">-{bal(p.cumulative_outflow)}</span>
                          </div>
                          <div className="flex items-center justify-between gap-4 text-xs tabular font-medium pt-1 border-t border-[var(--border)]">
                            <span className="text-[var(--text-secondary)] font-medium">Selisih Bersih:</span>
                            <span
                              className={cn(
                                "font-bold tabular",
                                p.cumulative_net >= 0 ? "text-emerald-500" : "text-rose-500"
                              )}
                            >
                              {p.cumulative_net >= 0 ? "+" : ""}
                              {bal(p.cumulative_net)}
                            </span>
                          </div>
                        </>
                      ) : (
                        <>
                          <div className="flex items-center justify-between gap-4 text-xs tabular font-medium">
                            <span className="text-emerald-500">Uang Masuk:</span>
                            <span className="font-bold text-emerald-500 tabular">+{bal(p.income)}</span>
                          </div>
                          <div className="flex items-center justify-between gap-4 text-xs tabular font-medium">
                            <span className="text-rose-500">Uang Keluar:</span>
                            <span className="font-bold text-rose-500 tabular">-{bal(p.expense)}</span>
                          </div>
                        </>
                      )}
                    </div>
                  );
                }
                return null;
              }}
            />
            {viewMode === "cumulative" ? (
              <>
                <Area
                  type="monotone"
                  dataKey="cumulative_inflow"
                  name="Uang Masuk Kumulatif"
                  stroke={chartColors.income}
                  strokeWidth={2.2}
                  fillOpacity={1}
                  fill="url(#incomeGrad)"
                />
                <Area
                  type="monotone"
                  dataKey="cumulative_outflow"
                  name="Uang Keluar Kumulatif"
                  stroke={chartColors.expense}
                  strokeWidth={2.2}
                  fillOpacity={1}
                  fill="url(#expenseGrad)"
                />
              </>
            ) : (
              <>
                <Area
                  type="monotone"
                  dataKey="income"
                  name="Pemasukan Harian"
                  stroke={chartColors.income}
                  strokeWidth={2}
                  fillOpacity={1}
                  fill="url(#incomeGrad)"
                />
                <Area
                  type="monotone"
                  dataKey="expense"
                  name="Pengeluaran Harian"
                  stroke={chartColors.expense}
                  strokeWidth={2}
                  fillOpacity={1}
                  fill="url(#expenseGrad)"
                />
              </>
            )}
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Legend & Summary Info */}
      <div className="flex flex-wrap items-center justify-between text-xs text-[var(--muted)] pt-2 border-t border-[var(--border)]">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-emerald-500" />
            <span>Uang Masuk</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-rose-500" />
            <span>Uang Keluar</span>
          </div>
        </div>
        <div className="text-[11px] text-[var(--muted)] font-medium tabular">
          {data.length} titik data
        </div>
      </div>
    </div>
  );
}
