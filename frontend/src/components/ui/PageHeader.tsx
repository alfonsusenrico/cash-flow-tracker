import { cn } from "@/lib/utils";

interface PageHeaderProps {
  eyebrow?: string;
  title: string;
  description?: string;
  action?: React.ReactNode;
  className?: string;
}

export function PageHeader({ eyebrow, title, description, action, className }: PageHeaderProps) {
  return (
    <div className={cn("mb-4 flex flex-col gap-3 border-b border-[var(--border)] pb-4 md:flex-row md:items-end md:justify-between", className)}>
      <div className="min-w-0">
        {eyebrow && <p className="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">{eyebrow}</p>}
        <h1 className="text-2xl font-bold tracking-[-0.02em] text-[var(--text)]">{title}</h1>
        {description && <p className="mt-1 max-w-2xl text-sm text-[var(--muted)]">{description}</p>}
      </div>
      {action && <div className="flex shrink-0 items-center gap-2">{action}</div>}
    </div>
  );
}
