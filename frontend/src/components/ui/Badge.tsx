import { cn } from "@/lib/utils";

type BadgeVariant = "green" | "red" | "yellow" | "blue" | "gray" | "purple" | "orange";

const VARIANTS: Record<BadgeVariant, string> = {
  green: "bg-[var(--primary-light)] text-[var(--primary)]",
  red: "bg-[var(--danger-light)] text-[var(--danger)]",
  yellow: "bg-[var(--warning-light)] text-[var(--warning)]",
  blue: "bg-[var(--info-light)] text-[var(--info)]",
  gray: "bg-[var(--color-paper-2)] text-[var(--muted)]",
  purple: "bg-[var(--info-light)] text-[var(--info)]",
  orange: "bg-[var(--warning-light)] text-[var(--warning)]",
};

interface BadgeProps {
  children: React.ReactNode;
  variant?: BadgeVariant;
  className?: string;
  dot?: boolean;
}

export function Badge({ children, variant = "gray", className, dot }: BadgeProps) {
  return (
    <span className={cn("inline-flex items-center gap-1 px-2 py-0.5 rounded-[var(--radius-pill)] text-xs font-medium", VARIANTS[variant], className)}>
      {dot && <span className="w-1.5 h-1.5 rounded-full bg-current" />}
      {children}
    </span>
  );
}

export function StatusBadge({ status }: { status: string }) {
  const map: Record<string, { label: string; variant: BadgeVariant }> = {
    funded: { label: "Funded", variant: "green" },
    partial: { label: "Partial", variant: "yellow" },
    unfunded: { label: "Unfunded", variant: "red" },
    on_track: { label: "On Track", variant: "green" },
    at_risk: { label: "At Risk", variant: "yellow" },
    overdue: { label: "Overdue", variant: "red" },
    shortfall: { label: "Shortfall", variant: "orange" },
    active: { label: "Active", variant: "green" },
    draft: { label: "Draft", variant: "gray" },
    closed: { label: "Closed", variant: "gray" },
  };
  const cfg = map[status.toLowerCase()] ?? { label: status, variant: "gray" as BadgeVariant };
  return <Badge variant={cfg.variant}>{cfg.label}</Badge>;
}
