import { cn } from "@/lib/utils";

interface DetailDrawerProps {
  title: string;
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
  className?: string;
}

export function DetailDrawer({ title, open, onClose, children, className }: DetailDrawerProps) {
  if (!open) return null;
  return (
    <aside className={cn("w-80 shrink-0 border-l border-[var(--border)] bg-[var(--surface)]", className)}>
      <div className="flex items-center justify-between border-b border-[var(--border)] px-4 py-3">
        <h3 className="text-sm font-semibold">{title}</h3>
        <button onClick={onClose} className="rounded-[var(--radius-sm)] px-2 py-1 text-[var(--muted)] hover:bg-[var(--bg)] hover:text-[var(--text)]">Close</button>
      </div>
      {children}
    </aside>
  );
}
