import React from "react";
import { cn } from "@/lib/utils";

export interface ProgressSegment {
  label: string;
  amount?: number;
  percentage: number;
  color: string;
  count?: number;
}

interface SegmentedProgressTrackProps {
  segments: ProgressSegment[];
  formatAmount?: (n: number) => string;
  height?: number;
  className?: string;
  showLegend?: boolean;
}

export function SegmentedProgressTrack({
  segments,
  formatAmount,
  height = 10,
  className,
  showLegend = true,
}: SegmentedProgressTrackProps) {
  // Normalize segments so sum does not exceed 100%
  const totalPct = segments.reduce((sum, s) => sum + s.percentage, 0);
  const factor = totalPct > 100 ? 100 / totalPct : 1;

  return (
    <div className={cn("space-y-2.5", className)}>
      {/* Track Bar */}
      <div
        className="w-full rounded-full bg-[var(--canvas-subtle)] overflow-hidden flex"
        style={{ height }}
      >
        {segments.map((seg, idx) => {
          const widthPct = Math.max(0, seg.percentage * factor);
          if (widthPct <= 0) return null;
          return (
            <div
              key={idx}
              style={{
                width: `${widthPct}%`,
                backgroundColor: seg.color,
              }}
              className="h-full transition-all duration-300 first:rounded-l-full last:rounded-r-full"
              title={`${seg.label}: ${seg.percentage.toFixed(1)}%`}
            />
          );
        })}
      </div>

      {/* Legend */}
      {showLegend && (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 pt-0.5 text-xs">
          {segments.map((seg, idx) => (
            <div key={idx} className="flex items-center gap-1.5 text-[var(--text-secondary)]">
              <span
                className="h-2 w-2 rounded-full shrink-0"
                style={{ backgroundColor: seg.color }}
              />
              <span className="text-[11px] font-medium text-[var(--text-muted)]">
                {seg.label}
              </span>
              <span className="text-[11px] font-bold tabular text-[var(--text-primary)]">
                {formatAmount && seg.amount !== undefined
                  ? formatAmount(seg.amount)
                  : `${seg.percentage.toFixed(0)}%`}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
