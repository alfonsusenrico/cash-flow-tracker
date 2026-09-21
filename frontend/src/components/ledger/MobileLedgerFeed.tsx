"use client";

import { useMemo } from "react";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";

export interface LedgerTxItem {
  id: string;
  account_id: string;
  account_name: string;
  category_id: string | null;
  category_name: string | null;
  category_icon: string | null;
  category_color: string | null;
  goal_id: string | null;
  goal_name: string | null;
  obligation_id: string | null;
  obligation_name: string | null;
  type: "expense" | "income";
  amount: number;
  notes: string | null;
  date: string;
  receipt_path: string | null;
  created_at: string;
  is_excluded_from_budget?: boolean;
  partner_id?: string;
  is_consolidated_transfer?: boolean;
  target_account_name?: string;
  target_account_id?: string;
  kakeibo_type?: string | null;
}

interface MobileLedgerFeedProps {
  transactions: LedgerTxItem[];
  onOpenEdit: (tx: LedgerTxItem) => void;
  bal: (amount: number) => string;
  isLoading?: boolean;
}

export function MobileLedgerFeed({
  transactions,
  onOpenEdit,
  bal,
  isLoading,
}: MobileLedgerFeedProps) {
  // Group transactions by YYYY-MM-DD
  const grouped = useMemo(() => {
    const map = new Map<string, { date: Date; items: LedgerTxItem[]; dayNet: number }>();

    transactions.forEach((tx) => {
      const d = new Date(tx.date);
      const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(
        d.getDate()
      ).padStart(2, "0")}`;

      const isMovement =
        tx.is_consolidated_transfer ||
        tx.category_name === "Internal Movement" ||
        !!tx.is_excluded_from_budget ||
        (tx.notes?.toLowerCase().includes("pindah saldo") ?? false);

      const entry = map.get(key) || { date: d, items: [], dayNet: 0 };
      entry.items.push(tx);

      if (!isMovement) {
        if (tx.type === "income") {
          entry.dayNet += tx.amount;
        } else {
          entry.dayNet -= tx.amount;
        }
      }

      map.set(key, entry);
    });

    return Array.from(map.entries()).sort(
      ([keyA], [keyB]) => new Date(keyB).getTime() - new Date(keyA).getTime()
    );
  }, [transactions]);

  if (isLoading) {
    return (
      <div className="space-y-4 py-6">
        <div className="h-6 w-32 rounded bg-[var(--border)]/40 animate-pulse" />
        <div className="space-y-2">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-16 rounded-2xl bg-[var(--border)]/30 animate-pulse" />
          ))}
        </div>
      </div>
    );
  }

  if (transactions.length === 0) {
    return (
      <div className="py-16 text-center space-y-2 card-squircle p-6 border border-[var(--border)] bg-[var(--surface)]">
        <div className="text-2xl">🔍</div>
        <p className="text-sm font-semibold text-[var(--text)]">Belum ada transaksi ditemukan</p>
        <p className="text-xs text-[var(--muted)]">
          Coba sesuaikan kata kunci pencarian atau filter Anda.
        </p>
      </div>
    );
  }

  const today = new Date();
  const todayKey = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(
    today.getDate()
  ).padStart(2, "0")}`;

  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayKey = `${yesterday.getFullYear()}-${String(yesterday.getMonth() + 1).padStart(2, "0")}-${String(
    yesterday.getDate()
  ).padStart(2, "0")}`;

  return (
    <div className="space-y-5">
      {grouped.map(([key, group]) => {
        const isToday = key === todayKey;
        const isYesterday = key === yesterdayKey;

        let dateLabel = group.date.toLocaleDateString("id-ID", {
          day: "numeric",
          month: "long",
          year: "numeric",
        });

        if (isToday) {
          dateLabel = `Hari Ini • ${group.date.toLocaleDateString("id-ID", { day: "numeric", month: "short" })}`;
        } else if (isYesterday) {
          dateLabel = `Kemarin • ${group.date.toLocaleDateString("id-ID", { day: "numeric", month: "short" })}`;
        }

        return (
          <div key={key} className="space-y-2">
            {/* Group Header */}
            <div className="flex items-center justify-between px-1">
              <span className="text-[11px] font-bold uppercase tracking-wider text-[var(--muted)]">
                {dateLabel}
              </span>
              <span
                className={cn(
                  "text-[11px] font-bold tabular tracking-tight",
                  group.dayNet > 0
                    ? "text-emerald-500"
                    : group.dayNet < 0
                    ? "text-rose-500"
                    : "text-[var(--muted)]"
                )}
              >
                {group.dayNet > 0 ? "+" : ""}
                {bal(group.dayNet)}
              </span>
            </div>

            {/* Group Card List */}
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] divide-y divide-[var(--border)] overflow-hidden shadow-2xs">
              {group.items.map((tx) => {
                const isMovement =
                  tx.is_consolidated_transfer ||
                  tx.category_name === "Internal Movement" ||
                  !!tx.is_excluded_from_budget ||
                  (tx.notes?.toLowerCase().includes("pindah saldo") ?? false);
                const isIncome = tx.type === "income" && !isMovement;

                return (
                  <div
                    key={tx.id}
                    onClick={() => onOpenEdit(tx)}
                    className="p-3 flex items-center justify-between hover:bg-[var(--surface-raised)]/60 active:bg-[var(--surface-raised)] transition-all cursor-pointer group"
                  >
                    {/* Left: Icon + Description */}
                    <div className="flex items-center gap-3 min-w-0 pr-2">
                      <div
                        className={cn(
                          "h-9 w-9 rounded-xl flex items-center justify-center text-xs font-bold shrink-0 shadow-2xs",
                          isMovement
                            ? "bg-sky-500/10 text-sky-500 border border-sky-500/20"
                            : isIncome
                            ? "bg-emerald-500/10 text-emerald-500 border border-emerald-500/20"
                            : "bg-rose-500/10 text-rose-500 border border-rose-500/20"
                        )}
                      >
                        <Icon
                          name={
                            isMovement
                              ? "repeat"
                              : isIncome
                              ? "arrow-down-left"
                              : "arrow-up-right"
                          }
                          className="h-4 w-4 stroke-[2.5]"
                        />
                      </div>

                      <div className="min-w-0">
                        <div className="text-xs font-semibold text-[var(--text)] group-hover:text-emerald-500 transition-colors truncate">
                          {tx.notes || (isMovement ? "Pindah Saldo" : tx.category_name || "Umum")}
                        </div>

                        <div className="text-[10px] text-[var(--muted)] flex items-center gap-1.5 mt-0.5 truncate">
                          <span className="truncate">{tx.account_name}</span>
                          {tx.target_account_name && (
                            <>
                              <span>&rarr;</span>
                              <span className="truncate">{tx.target_account_name}</span>
                            </>
                          )}
                          {tx.category_name && !isMovement && (
                            <>
                              <span>•</span>
                              <span className="truncate">{tx.category_name}</span>
                            </>
                          )}
                          {tx.kakeibo_type && (
                            <>
                              <span>•</span>
                              <span className="uppercase text-[9px] font-bold text-[var(--muted)]">
                                {tx.kakeibo_type}
                              </span>
                            </>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Right: Nominal */}
                    <div className="text-right shrink-0">
                      <div
                        className={cn(
                          "text-xs font-bold tabular tracking-tight",
                          isMovement
                            ? "text-sky-500"
                            : isIncome
                            ? "text-emerald-500"
                            : "text-rose-500"
                        )}
                      >
                        {isMovement
                          ? ""
                          : isIncome
                          ? "+"
                          : "-"}
                        {bal(tx.amount)}
                      </div>

                      {isMovement && (
                        <span className="inline-block mt-0.5 text-[9px] font-bold uppercase tracking-wider text-sky-500 bg-sky-500/10 px-1.5 py-0.2 rounded">
                          Transfer
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}
