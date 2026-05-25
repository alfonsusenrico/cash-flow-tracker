import { cn } from "@/lib/utils";

interface ProgressBarProps {
  value: number; // 0-100
  max?: number;
  color?: "green" | "yellow" | "red" | "blue" | "orange";
  size?: "sm" | "md";
  className?: string;
  showLabel?: boolean;
}

const COLORS = {
  green: "bg-green-500",
  yellow: "bg-yellow-400",
  red: "bg-red-500",
  blue: "bg-blue-500",
  orange: "bg-orange-400",
};

export function ProgressBar({ value, max = 100, color = "green", size = "sm", className, showLabel }: ProgressBarProps) {
  const pct = Math.min(Math.max((value / max) * 100, 0), 100);
  const autoColor = pct >= 80 ? "red" : pct >= 60 ? "yellow" : "green";
  const barColor = COLORS[color === "green" && pct >= 80 ? autoColor : color];
  const h = size === "sm" ? "h-1.5" : "h-2";

  return (
    <div className={cn("flex items-center gap-2", className)}>
      <div className={cn("flex-1 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden", h)}>
        <div className={cn("h-full rounded-full transition-all", barColor)} style={{ width: `${pct}%` }} />
      </div>
      {showLabel && <span className="text-xs text-[var(--muted)] tabular w-8 text-right">{Math.round(pct)}%</span>}
    </div>
  );
}
