"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";

export interface HeatmapDay {
  day_index: number;
  day_name: string;
  day_short: string;
  total_spent: number;
  tx_count: number;
  average_spent: number;
}

interface SpendingHeatmapProps {
  data: HeatmapDay[];
  title?: string;
  subtitle?: string;
}

const DAY_NAMES_ID: Record<string, string> = {
  Sunday: "Minggu",
  Monday: "Senin",
  Tuesday: "Selasa",
  Wednesday: "Rabu",
  Thursday: "Kamis",
  Friday: "Jumat",
  Saturday: "Sabtu",
};

const DAY_SHORT_ID: Record<string, string> = {
  Sun: "Min",
  Mon: "Sen",
  Tue: "Sel",
  Wed: "Rab",
  Thu: "Kam",
  Fri: "Jum",
  Sat: "Sab",
  Sunday: "Min",
  Monday: "Sen",
  Tuesday: "Sel",
  Wednesday: "Rab",
  Thursday: "Kam",
  Friday: "Jum",
  Saturday: "Sab",
};

export function SpendingHeatmap({
  data,
  title = "Peta Pola Belanja Mingguan",
  subtitle = "Ketahui hari-hari dengan tingkat pengeluaran tertinggi",
}: SpendingHeatmapProps) {
  const { bal } = useAppCtx();

  const maxSpend = Math.max(...data.map((d) => d.total_spent), 1);
  const peakDay = data.reduce<HeatmapDay | null>((prev, cur) => {
    if (!prev || cur.total_spent > prev.total_spent) {
      return cur.total_spent > 0 ? cur : null;
    }
    return prev;
  }, null);

  const weekendSpend = (data[0]?.total_spent || 0) + (data[6]?.total_spent || 0);
  const weekdaySpend = data.slice(1, 6).reduce((acc, d) => acc + d.total_spent, 0);

  return (
    <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-base font-bold tracking-tight text-[var(--text)]">{title}</h2>
            {peakDay && (
              <span className="text-[10px] px-2 py-0.5 rounded-full font-bold bg-rose-500/10 text-rose-500 border border-rose-500/20">
                Puncak: {DAY_NAMES_ID[peakDay.day_name] || peakDay.day_name} ({bal(peakDay.total_spent)})
              </span>
            )}
          </div>
          <p className="text-xs text-[var(--muted)] mt-0.5">{subtitle}</p>
        </div>

        <div className="flex items-center gap-3 text-xs text-[var(--muted)] tabular font-medium self-start sm:self-auto">
          <span>Hari Kerja: {bal(weekdaySpend)}</span>
          <span>•</span>
          <span>Akhir Pekan: {bal(weekendSpend)}</span>
        </div>
      </div>

      {/* 7-Day Density Matrix */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-2.5 sm:gap-3 pt-2">
        {data.map((day) => {
          const ratio = day.total_spent / maxSpend;
          const isPeak = peakDay?.day_index === day.day_index && day.total_spent > 0;
          const shortLabel = DAY_SHORT_ID[day.day_short] || DAY_SHORT_ID[day.day_name] || day.day_short;

          return (
            <div
              key={day.day_index}
              className={cn(
                "p-3 rounded-2xl border transition-all flex flex-col justify-between",
                isPeak
                  ? "border-rose-500/40 bg-rose-500/10"
                  : ratio > 0.5
                  ? "border-rose-500/20 bg-rose-500/5"
                  : "border-[var(--border)] bg-[var(--surface-raised)]/50"
              )}
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-[var(--text)]">{shortLabel}</span>
                <span className="text-[10px] text-[var(--muted)] tabular font-medium">
                  {day.tx_count} transaksi
                </span>
              </div>

              <div className="mt-4">
                <div
                  className={cn(
                    "text-sm sm:text-base font-bold tabular tracking-tight",
                    day.total_spent > 0 ? "text-rose-500" : "text-[var(--muted)]"
                  )}
                >
                  {day.total_spent > 0 ? `-${bal(day.total_spent)}` : bal(0)}
                </div>
                <div className="text-[10px] text-[var(--muted)] mt-0.5 tabular font-medium">
                  {day.tx_count > 0 ? `Rata-rata ${bal(day.average_spent)}` : "Tanpa belanja"}
                </div>
              </div>

              {/* Density Bar */}
              <div className="w-full h-1 rounded-full bg-[var(--border)]/60 overflow-hidden mt-3">
                <div
                  style={{ width: `${Math.round(ratio * 100)}%` }}
                  className={cn(
                    "h-full rounded-full transition-all",
                    isPeak ? "bg-rose-500" : "bg-rose-400/70"
                  )}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
