import { cn } from "@/lib/utils";

interface MetricTileProps {
  label: string;
  value: React.ReactNode;
  detail?: React.ReactNode;
  tone?: "neutral" | "positive" | "negative" | "warning" | "info";
  className?: string;
}

const toneClass = {
  neutral: "text-[var(--text)]",
  positive: "text-[var(--primary)]",
  negative: "text-[var(--danger)]",
  warning: "text-[var(--warning)]",
  info: "text-[var(--info)]",
};

export function MetricTile({ label, value, detail, tone = "neutral", className }: MetricTileProps) {
  return (
    <div className={cn("rounded-[var(--radius-md)] border border-[var(--border)] bg-[var(--surface)] p-3 shadow-[var(--shadow-sm)]", className)}>
      <p className="text-xs font-medium text-[var(--muted)]">{label}</p>
      <p className={cn("mt-1 text-lg font-bold tabular tracking-[-0.01em]", toneClass[tone])}>{value}</p>
      {detail && <p className="mt-0.5 text-xs text-[var(--muted)]">{detail}</p>}
    </div>
  );
}
