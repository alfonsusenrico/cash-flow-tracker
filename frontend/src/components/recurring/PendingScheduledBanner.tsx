"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { fmtMoney } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { RecurringRule } from "@/types/recurring";

interface Props {
  onOpenRulesManager?: () => void;
}

export function PendingScheduledBanner({ onOpenRulesManager }: Props = {}) {
  const qc = useQueryClient();

  // Automatic execution is owned by the server scheduler; the client only lists manual confirmations.
  const { data } = useQuery<{ ok: boolean; pending_count: number; rules: RecurringRule[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    refetchInterval: 30000,
  });

  const pendingRules = data?.rules ?? [];

  // Execute selected or all pending rules
  const executeMutation = useMutation({
    mutationFn: async (ruleIds: string[]) => {
      return api.post("/recurring/execute", { rule_ids: ruleIds });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
    },
  });

  if (pendingRules.length === 0) return null;

  const totalPendingAmount = pendingRules.reduce((sum, r) => sum + r.amount, 0);

  return (
    <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 p-4 sm:p-5 text-[var(--text)] space-y-3">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div className="flex items-center gap-2.5">
          <div className="h-8 w-8 rounded-xl bg-amber-500/20 text-amber-600 dark:text-amber-400 flex items-center justify-center shrink-0">
            <Icon name="clock" className="h-4 w-4" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-bold text-xs sm:text-sm">
                {pendingRules.length} Transaksi Terjadwal Jatuh Tempo
              </span>
              <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-600 dark:text-amber-400">
                Perlu Konfirmasi
              </span>
            </div>
            <p className="text-xs text-[var(--muted)] mt-0.5">
              Total nominal: <span className="font-bold tabular text-[var(--text)]">{fmtMoney(totalPendingAmount)}</span>. Konfirmasi untuk mencatat ke rekening Anda.
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 self-start sm:self-auto">
          {onOpenRulesManager && (
            <button
              type="button"
              onClick={onOpenRulesManager}
              className="min-h-11 px-3 py-2 rounded-xl border border-amber-500/30 bg-white/40 dark:bg-black/20 text-xs font-semibold hover:bg-amber-500/20 transition-colors motion-reduce:transition-none text-amber-700 dark:text-amber-300"
            >
              Kelola Aturan
            </button>
          )}

          <button
            type="button"
            disabled={executeMutation.isPending}
            onClick={() => executeMutation.mutate(pendingRules.map((r) => r.id))}
            className="min-h-11 flex items-center justify-center gap-1.5 px-3.5 py-2 rounded-xl bg-amber-600 hover:bg-amber-700 text-white text-xs font-bold transition-[background-color,transform] active:scale-95 shadow-xs disabled:opacity-50 motion-reduce:transition-none"
          >
            <Icon name="check" className="h-3.5 w-3.5" />
            <span>{executeMutation.isPending ? "Mencatat…" : "Konfirmasi & Catat Semua"}</span>
          </button>
        </div>
      </div>

      {executeMutation.error && (
        <p role="alert" className="text-xs font-medium text-rose-700 dark:text-rose-300">
          {executeMutation.error.message || "Transaksi gagal dicatat. Periksa saldo dan coba lagi."}
        </p>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2 pt-1 border-t border-amber-500/15">
        {pendingRules.map((rule) => {
          const isTransfer = rule.type === "transfer";
          return (
            <div
              key={rule.id}
              className="p-2.5 rounded-xl bg-[var(--surface)]/80 border border-black/[0.06] dark:border-white/[0.06] flex items-center justify-between text-xs"
            >
              <div className="min-w-0 pr-2">
                <div className="font-semibold text-[var(--text)] truncate">{rule.name}</div>
                <div className="text-[11px] text-[var(--muted)] truncate font-medium">
                  {isTransfer ? `${rule.source_account_name} → ${rule.target_account_name}` : rule.source_account_name}
                </div>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <span className="font-bold tabular text-xs text-[var(--text)]">
                  {fmtMoney(rule.amount)}
                </span>
                <button
                  type="button"
                  disabled={executeMutation.isPending}
                  onClick={() => executeMutation.mutate([rule.id])}
                  aria-label={`Catat ${rule.name}`}
                  className="min-h-11 min-w-11 p-1 rounded-lg hover:bg-amber-500/20 text-amber-600 dark:text-amber-400 transition-colors motion-reduce:transition-none"
                >
                  <Icon name="check" className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
