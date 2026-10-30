"use client";

interface Commitment {
  id: string;
  name: string;
  amount: number;
  due_date: string;
  overdue: boolean;
}

export interface UpcomingCommitments {
  window_days: number;
  total: number;
  items: Commitment[];
}

interface ObservedCashFlowCardProps {
  periodLabel?: string;
  inflow?: number;
  outflow?: number;
  net?: number;
  budget?: number;
  budgetRemaining?: number | null;
  commitments?: UpcomingCommitments;
  bal: (amount: number) => string;
  onOpenPlans?: () => void;
}

function shortDate(value: string) {
  return new Date(`${value}T00:00:00`).toLocaleDateString("id-ID", { day: "numeric", month: "short" });
}

/** Observed figures for the selected period; plans are listed separately and never treated as spent or available. */
export function ObservedCashFlowCard({
  periodLabel,
  inflow,
  outflow,
  net,
  budget,
  budgetRemaining,
  commitments,
  bal,
  onOpenPlans,
}: ObservedCashFlowCardProps) {
  const figures = [
    { label: "Masuk", amount: inflow },
    { label: "Keluar", amount: outflow },
    { label: "Neto", amount: net },
  ];
  const nextItems = commitments?.items.slice(0, 3) ?? [];

  return (
    <section aria-labelledby="observed-cash-flow-title" className="card-squircle space-y-4 border border-[var(--border)] p-5">
      <div>
        <h2 id="observed-cash-flow-title" className="text-sm font-bold text-[var(--text)]">Arus kas tercatat</h2>
        <p className="text-xs text-[var(--muted)]">{periodLabel || "Periode belum tersedia"}</p>
      </div>

      <dl className="grid grid-cols-3 gap-2 text-xs">
        {figures.map((figure) => (
          <div key={figure.label}>
            <dt className="text-[var(--muted)]">{figure.label}</dt>
            <dd className="font-bold tabular-nums text-[var(--text)]">
              {typeof figure.amount === "number" ? bal(figure.amount) : "Tidak tersedia"}
            </dd>
          </div>
        ))}
      </dl>

      <div className="border-t border-[var(--border)] pt-3 text-xs">
        {typeof budgetRemaining === "number" && typeof budget === "number" ? (
          <p className="text-[var(--text-secondary)]">
            Sisa anggaran periode ini:{" "}
            <span className="font-bold tabular-nums text-[var(--text)]">{bal(budgetRemaining)}</span>{" "}
            <span className="text-[var(--muted)]">dari {bal(budget)}</span>
          </p>
        ) : (
          <p className="text-[var(--muted)]">Anggaran bulanan belum diatur.</p>
        )}
      </div>

      <div className="space-y-2 border-t border-[var(--border)] pt-3 text-xs">
        <div className="flex items-baseline justify-between gap-2">
          <h3 className="font-semibold text-[var(--text)]">
            Rencana {commitments?.window_days ?? 30} hari ke depan
          </h3>
          {commitments && (
            <span className="font-bold tabular-nums text-[var(--text)]">{bal(commitments.total)}</span>
          )}
        </div>
        {!commitments ? (
          <p className="text-[var(--muted)]">Tidak tersedia.</p>
        ) : nextItems.length === 0 ? (
          <p className="text-[var(--muted)]">Tidak ada rencana pengeluaran.</p>
        ) : (
          <ul className="space-y-1">
            {nextItems.map((item) => (
              <li key={item.id} className="flex items-center justify-between gap-2">
                <span className="min-w-0 truncate">
                  {item.name}
                  <span className="ml-1 tabular-nums text-[var(--muted)]">
                    · {item.overdue ? `lewat ${shortDate(item.due_date)}` : shortDate(item.due_date)}
                  </span>
                </span>
                <span className="shrink-0 tabular-nums">{bal(item.amount)}</span>
              </li>
            ))}
          </ul>
        )}
        <p className="text-[var(--muted)]">Rencana adalah pengingat, bukan pembayaran atau dana yang pasti tersedia.</p>
        {onOpenPlans && (
          <button type="button" onClick={onOpenPlans} className="min-h-11 font-semibold underline underline-offset-2">
            Kelola rencana
          </button>
        )}
      </div>
    </section>
  );
}
