import { cn } from "@/lib/utils";

export function FilterBar({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={cn("flex flex-wrap items-center gap-2 border-b border-[var(--border)] bg-[var(--surface)] px-4 py-3", className)}>
      {children}
    </div>
  );
}
