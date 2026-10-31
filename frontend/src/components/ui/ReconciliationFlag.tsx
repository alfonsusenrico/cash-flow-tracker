import { Icon } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

interface ReconciliationFlagProps {
  /** Account or pocket the flag belongs to, used in the action's accessible name. */
  name: string;
  /** Opens "Sesuaikan Saldo" for this account; without it the flag is a plain label. */
  onReconcile?: () => void;
  /** Number of flagged pockets, for a parent card that summarises its pockets. */
  pocketCount?: number;
}

const flagClass =
  "inline-flex min-h-8 items-center gap-1 rounded-full border border-amber-500/40 bg-amber-500/10 px-2.5 text-xs font-semibold text-[var(--text)]";

/** Marks an account whose recorded balance went negative and needs reconciling with the bank. */
export function ReconciliationFlag({ name, onReconcile, pocketCount }: ReconciliationFlagProps) {
  const text = pocketCount ? `${pocketCount} kantong perlu rekonsiliasi` : "Perlu rekonsiliasi";
  const content = (
    <>
      <Icon name="alert-triangle" className="h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
      <span>{text}</span>
    </>
  );
  if (!onReconcile) {
    return <span className={flagClass}>{content}</span>;
  }
  return (
    <button
      type="button"
      onClick={onReconcile}
      aria-label={`${text}: sesuaikan saldo ${name}`}
      className={cn(
        flagClass,
        "hover:bg-amber-500/20 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]",
      )}
    >
      {content}
    </button>
  );
}
