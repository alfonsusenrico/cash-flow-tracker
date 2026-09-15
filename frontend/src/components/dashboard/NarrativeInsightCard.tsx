"use client";

import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";

interface NarrativeInsightCardProps {
  narrative?: {
    summary: string;
    trend_status: "MENURUN" | "MENINGKAT" | "STABIL" | string;
    top_driver?: string | null;
    top_driver_delta_pct?: number | null;
  };
  outflowDeltaPct?: number;
  timeframeLabel?: string;
}

export function NarrativeInsightCard({
  narrative,
  outflowDeltaPct = 0,
  timeframeLabel,
}: NarrativeInsightCardProps) {
  if (!narrative || !narrative.summary) return null;

  const trend = narrative.trend_status || "STABIL";
  const isDown = trend === "MENURUN";
  const isUp = trend === "MENINGKAT";

  return (
    <div className="card-squircle p-4 sm:p-5 space-y-3 border border-indigo-500/20 bg-gradient-to-br from-indigo-500/[0.04] via-[var(--surface)] to-[var(--surface-raised)] shadow-xs">
      {/* Header with Icon and Trend Badge */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <div
            className={cn(
              "h-7 w-7 rounded-lg flex items-center justify-center shrink-0",
              isDown
                ? "bg-emerald-500/10 text-emerald-500"
                : isUp
                ? "bg-rose-500/10 text-rose-500"
                : "bg-slate-500/10 text-slate-400"
            )}
          >
            <Icon
              name={isDown ? "trending-down" : isUp ? "trending-up" : "activity"}
              className="h-4 w-4"
            />
          </div>
          <span className="text-[11px] font-bold uppercase tracking-wider text-[var(--muted)]">
            Insight & Komparasi {timeframeLabel ? `• ${timeframeLabel}` : ""}
          </span>
        </div>

        <span
          className={cn(
            "text-[10px] font-bold px-2 py-0.5 rounded-full border shrink-0",
            isDown
              ? "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"
              : isUp
              ? "bg-rose-500/10 text-rose-500 border-rose-500/20"
              : "bg-slate-500/10 text-slate-400 border-slate-500/20"
          )}
        >
          {trend === "MENURUN"
            ? `HEMAT (${outflowDeltaPct}%)`
            : trend === "MENINGKAT"
            ? `NAIK (+${outflowDeltaPct}%)`
            : "STABIL"}
        </span>
      </div>

      {/* Conversational Indonesian Narrative Text */}
      <p className="text-xs sm:text-sm font-medium leading-relaxed text-[var(--text)]">
        {narrative.summary}
      </p>

      {/* Top Driver Highlight Pill */}
      {narrative.top_driver && (
        <div className="pt-2 border-t border-[var(--border)] flex items-center justify-between text-[11px]">
          <span className="text-[var(--muted)]">Driver Utama:</span>
          <span className="font-semibold text-[var(--text)] flex items-center gap-1.5">
            <span>{narrative.top_driver}</span>
            {narrative.top_driver_delta_pct !== undefined && narrative.top_driver_delta_pct !== null && (
              <span
                className={cn(
                  "px-1.5 py-0.2 rounded text-[10px]",
                  narrative.top_driver_delta_pct > 0
                    ? "bg-rose-500/10 text-rose-500"
                    : "bg-emerald-500/10 text-emerald-500"
                )}
              >
                {narrative.top_driver_delta_pct > 0 ? `+${narrative.top_driver_delta_pct}%` : `${narrative.top_driver_delta_pct}%`}
              </span>
            )}
          </span>
        </div>
      )}
    </div>
  );
}
