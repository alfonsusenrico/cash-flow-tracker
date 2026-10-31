import { isHabitRecorded, type HabitBasis } from "@/lib/provenance";
import { cn } from "@/lib/utils";

export interface FeeLinkFields {
  movement_fee?: { transaction_id: string; amount: number } | null;
  fee_movement?: {
    movement_id: string;
    source_account_name: string | null;
    target_account_name: string | null;
    amount: number;
  } | null;
}

/** Review state in words; reviewed rows carry no mark to keep the ledger calm. */
export function ReviewStatusMark({ status, className }: { status?: string | null; className?: string }) {
  if (status !== "needs_review" && status !== "auto") return null;
  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center rounded-full border px-1.5 py-px text-[10px] font-semibold",
        status === "needs_review"
          ? "border-amber-500/30 bg-amber-500/10 text-amber-800 dark:text-amber-300"
          : "border-[var(--border)] bg-[var(--surface-raised)]/60 text-[var(--text-secondary)]",
        className,
      )}
    >
      {status === "needs_review" ? "Perlu ditinjau" : "Otomatis"}
    </span>
  );
}

/** A row recorded from the owner's own repeated answers, with the count it followed. */
export function HabitMark({ classifiedBy, basis, className }: {
  classifiedBy?: string | null;
  basis?: HabitBasis | null;
  className?: string;
}) {
  if (!isHabitRecorded(classifiedBy, basis)) return null;
  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center whitespace-nowrap rounded-full border border-[var(--border)] bg-[var(--surface-raised)]/60 px-1.5 py-px text-[10px] font-semibold text-[var(--text-secondary)]",
        className,
      )}
    >
      Dicatat dari kebiasaan · <span className="tabular-nums">{basis.agreeing} dari {basis.support}</span>
    </span>
  );
}

/** Links a transfer-fee row and its movement in both directions (movement pairing D5). */
export function feeLinkText(row: FeeLinkFields, bal: (amount: number) => string): string | null {
  if (row.fee_movement) {
    const route = [row.fee_movement.source_account_name, row.fee_movement.target_account_name].filter(Boolean).join(" → ");
    return `Biaya pindah saldo ${bal(row.fee_movement.amount)}${route ? ` · ${route}` : ""}`;
  }
  if (row.movement_fee) return `+ biaya transfer ${bal(row.movement_fee.amount)}`;
  return null;
}
