"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { chartAxisProps, chartColors, chartGridProps, chartTooltipStyle, formatAxisCurrency } from "@/components/charts/theme";
import { cn } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";

export interface CadencePoint {
  date: string;
  label: string;
  expense: number;
  benchmark_safe_daily?: number;
}

interface BurnCadenceChartProps {
  data: CadencePoint[];
  benchmark?: number;
  title?: string;
  subtitle?: string;
}

export function BurnCadenceChart({
  data,
  benchmark = 0,
  title = "Pengeluaran Harian",
  subtitle = "Pengeluaran harian dibandingkan dengan batas belanja",
}: BurnCadenceChartProps) {
  const { bal } = useAppCtx();

  if (!data || data.length === 0) {
    return (
      <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-6 text-center text-xs text-[var(--muted)]">
        Belum ada catatan pengeluaran.
      </div>
    );
  }

  const effectiveBenchmark = benchmark || data[0]?.benchmark_safe_daily || 0;
  const overBudgetDays = data.filter(
    (d) => effectiveBenchmark > 0 && d.expense > effectiveBenchmark
  ).length;


  return (
    <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-base font-bold tracking-tight text-[var(--text)]">{title}</h2>
            {overBudgetDays > 0 && (
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-amber-500/10 text-amber-600 dark:text-amber-400 border border-amber-500/20">
                {overBudgetDays} hari di atas batas belanja
              </span>
            )}
          </div>
          <p className="text-xs text-[var(--muted)] mt-0.5">{subtitle}</p>
        </div>

        {effectiveBenchmark > 0 && (
          <div className="flex items-center gap-1.5 text-xs text-[var(--muted)] tabular font-medium bg-[var(--surface-raised)] border border-[var(--border)] px-2.5 py-1 rounded-lg self-start sm:self-auto">
            <span className="h-0.5 w-3 bg-amber-500 border-b border-dashed border-amber-500" />
            <span>Batas Belanja: {bal(effectiveBenchmark)}/hari</span>
          </div>
        )}
      </div>

      <div className="h-64 sm:h-72 w-full pt-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 10, right: 10, left: -15, bottom: 0 }}>
            <CartesianGrid {...chartGridProps} />
            <XAxis
              dataKey="label"
              {...chartAxisProps}
              interval="preserveStartEnd"
              minTickGap={20}
            />
            <YAxis
              {...chartAxisProps}
              tickFormatter={formatAxisCurrency}
              width={48}
            />
            <Tooltip
              content={(props: any) => {
                if (props.active && props.payload && props.payload.length) {
                  const p = props.payload[0].payload as CadencePoint;
                  const isOver = effectiveBenchmark > 0 && p.expense > effectiveBenchmark;
                  return (
                    <div style={chartTooltipStyle} className="space-y-1.5 min-w-40">
                      <div className="text-xs font-semibold text-[var(--text)] border-b border-[var(--border)] pb-1">
                        {p.label} ({p.date})
                      </div>
                      <div className="flex items-center justify-between gap-4 text-xs tabular font-medium">
                        <span className="text-[var(--muted)]">Pengeluaran:</span>
                        <span className={cn("font-bold tabular", isOver ? "text-rose-500" : "text-[var(--text)]")}>
                          -{bal(p.expense)}
                        </span>
                      </div>
                      {effectiveBenchmark > 0 && (
                        <div className="flex items-center justify-between gap-4 text-xs tabular font-medium pt-1 border-t border-[var(--border)]">
                          <span className="text-[var(--muted)]">Batas Harian:</span>
                          <span className="font-medium text-[var(--text-secondary)] tabular">
                            {bal(effectiveBenchmark)}
                          </span>
                        </div>
                      )}
                      {isOver && (
                        <div className="text-[10px] text-rose-500 font-semibold pt-0.5">
                          ⚠️ Melebihi batas {bal(p.expense - effectiveBenchmark)}
                        </div>
                      )}
                    </div>
                  );
                }
                return null;
              }}
            />
            {effectiveBenchmark > 0 && (
              <ReferenceLine
                y={effectiveBenchmark}
                stroke={chartColors.warning}
                strokeDasharray="4 4"
                strokeWidth={1.5}
              />
            )}
            <Bar dataKey="expense" radius={[4, 4, 0, 0]}>
              {data.map((entry, index) => {
                const isOver = effectiveBenchmark > 0 && entry.expense > effectiveBenchmark;
                return (
                  <Cell
                    key={`cell-${index}`}
                    fill={isOver ? chartColors.expense : "var(--border-strong)"}
                    opacity={entry.expense > 0 ? 0.9 : 0.3}
                  />
                );
              })}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
