import { cn } from "@/lib/utils";

interface CardProps {
  children: React.ReactNode;
  className?: string;
  green?: boolean;
  padding?: "sm" | "md" | "lg";
}

export function Card({ children, className, green, padding = "md" }: CardProps) {
  const padCls = { sm: "p-3", md: "p-4", lg: "p-5" }[padding];
  return (
    <div
      className={cn(
        "rounded-xl border",
        green ? "card-green border-transparent" : "bg-[var(--surface)] border-[var(--border)]",
        padCls,
        className
      )}
    >
      {children}
    </div>
  );
}

interface SectionTitleProps {
  children: React.ReactNode;
  action?: React.ReactNode;
  className?: string;
}

export function SectionTitle({ children, action, className }: SectionTitleProps) {
  return (
    <div className={cn("flex items-center justify-between mb-3", className)}>
      <h2 className="text-sm font-semibold text-[var(--text)]">{children}</h2>
      {action && <div>{action}</div>}
    </div>
  );
}
