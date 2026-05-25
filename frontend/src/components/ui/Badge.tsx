import { cn } from "@/lib/utils";

type BadgeVariant = "green" | "red" | "yellow" | "blue" | "gray" | "purple" | "orange";

const VARIANTS: Record<BadgeVariant, string> = {
  green: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400",
  red: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
  yellow: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400",
  blue: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
  gray: "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400",
  purple: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400",
  orange: "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400",
};

interface BadgeProps {
  children: React.ReactNode;
  variant?: BadgeVariant;
  className?: string;
  dot?: boolean;
}

export function Badge({ children, variant = "gray", className, dot }: BadgeProps) {
  return (
    <span className={cn("inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium", VARIANTS[variant], className)}>
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
