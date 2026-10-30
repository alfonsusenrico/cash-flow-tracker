"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { fmtMoney } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { DueExpectation, OccurrenceCandidate } from "@/types/recurring";

interface Props {
  onOpenRulesManager?: () => void;
}

type OccurrenceAction =
  | { kind: "link"; rule: DueExpectation; transactionId: string }
  | { kind: "skip"; rule: DueExpectation }
  | { kind: "record"; rule: DueExpectation };

const MATCH_TEXT: Record<DueExpectation["match_state"], string> = {
  none: "Belum ada transaksi tercatat dengan nominal yang sama.",
  single: "Ada satu transaksi dengan nominal sama. Periksa sebelum mengaitkan.",
  ambiguous: "Beberapa transaksi bernominal sama. Pilih yang benar; tidak ada yang dikaitkan otomatis.",
};

function candidateLabel(candidate: OccurrenceCandidate) {
  const day = new Date(candidate.date).toLocaleDateString("id-ID", { day: "numeric", month: "short" });
  return `${day} · ${fmtMoney(candidate.amount)} · ${candidate.merchant || candidate.notes || candidate.account_name}`;
}

/** Due plans are reminders: the owner links an actual payment, skips the occurrence, or records a missed one. */
export function PendingScheduledBanner({ onOpenRulesManager }: Props = {}) {
  const qc = useQueryClient();
  const [errors, setErrors] = useState<Record<string, string>>({});

  const { data } = useQuery<{ ok: boolean; pending_count: number; rules: DueExpectation[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    refetchInterval: 30000,
  });
  const dueRules = data?.rules ?? [];

  const occurrence = useMutation({
    mutationFn: async (action: OccurrenceAction) => {
      const base = `/recurring/${action.rule.id}/occurrences`;
      const scheduled = { scheduled_for: action.rule.next_due_date };
      if (action.kind === "link") return api.post(`${base}/link`, { ...scheduled, transaction_id: action.transactionId });
      if (action.kind === "skip") return api.post(`${base}/skip`, scheduled);
      const reviewed = (action.rule.candidates ?? []).filter((item) => item.exact_amount).map((item) => item.id);
      return api.post(`${base}/record`, { ...scheduled, acknowledged_candidate_ids: reviewed });
    },
    onMutate: (action) => setErrors((current) => ({ ...current, [action.rule.id]: "" })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
    },
    onError: (error: Error, action) => {
      setErrors((current) => ({ ...current, [action.rule.id]: error.message || "Rencana belum diperbarui. Coba lagi." }));
      qc.invalidateQueries({ queryKey: queryKeys.recurring.pending });
    },
  });

  if (dueRules.length === 0) return null;

  return (
    <section
      aria-labelledby="due-plans-title"
      className="space-y-3 rounded-2xl border border-amber-500/20 bg-amber-500/10 p-4 text-[var(--text)] sm:p-5"
    >
      <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
        <div className="flex items-start gap-2.5">
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-amber-500/20 text-amber-700 dark:text-amber-300">
            <Icon name="clock" className="h-4 w-4" />
          </div>
          <div>
            <h2 id="due-plans-title" className="text-xs font-bold sm:text-sm">
              {dueRules.length} rencana jatuh tempo
            </h2>
            <p className="mt-0.5 text-xs text-[var(--text-secondary)]">
              Rencana tidak mengubah saldo. Kaitkan transaksi yang sudah tercatat, lewati jika tidak terjadi, atau catat
              manual hanya jika notifikasinya terlewat.
            </p>
          </div>
        </div>
        {onOpenRulesManager && (
          <button
            type="button"
            onClick={onOpenRulesManager}
            className="min-h-11 self-start rounded-xl border border-amber-500/30 px-3 text-xs font-semibold text-amber-800 hover:bg-amber-500/20 dark:text-amber-200"
          >
            Kelola rencana
          </button>
        )}
      </div>

      <ul className="space-y-2 border-t border-amber-500/15 pt-2">
        {dueRules.map((rule) => {
          const isTransfer = rule.type === "transfer" || rule.type === "investment_topup";
          const candidates = rule.candidates ?? [];
          const busy = occurrence.isPending && occurrence.variables?.rule.id === rule.id;
          const sameAmount = candidates.filter((item) => item.exact_amount);
          const otherAmounts = candidates.filter((item) => !item.exact_amount);
          const hasSameAmount = sameAmount.length > 0;
          const renderCandidate = (candidate: OccurrenceCandidate) => (
            <li key={candidate.id} className="flex min-w-0 items-center justify-between gap-2">
              <span className="min-w-0 truncate tabular-nums">{candidateLabel(candidate)}</span>
              <button
                type="button"
                disabled={busy}
                onClick={() => occurrence.mutate({ kind: "link", rule, transactionId: candidate.id })}
                aria-label={`Kaitkan ${candidateLabel(candidate)} ke ${rule.name}`}
                className="min-h-11 shrink-0 rounded-lg px-2 font-semibold underline underline-offset-2 disabled:opacity-50"
              >
                Kaitkan
              </button>
            </li>
          );
          return (
            <li key={rule.id} className="min-w-0 space-y-2 rounded-xl border border-black/[0.06] bg-[var(--surface)]/80 p-3 text-xs dark:border-white/[0.06]">
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <h3 className="truncate font-semibold">{rule.name}</h3>
                  <p className="truncate text-[11px] text-[var(--muted)]">
                    {isTransfer ? `${rule.source_account_name} → ${rule.target_account_name}` : rule.source_account_name}
                    {rule.obligation_name ? ` · ${rule.obligation_name}` : ""}
                  </p>
                  <p className="text-[11px] tabular-nums text-[var(--muted)]">Jatuh tempo {rule.next_due_date}</p>
                </div>
                <span className="shrink-0 font-bold tabular-nums">{fmtMoney(rule.amount)}</span>
              </div>

              <p className="text-[var(--text-secondary)]">{MATCH_TEXT[rule.match_state]}</p>
              {sameAmount.length > 0 && (
                <ul aria-label={`Transaksi bernominal sama untuk ${rule.name}`} className="min-w-0 space-y-1">
                  {sameAmount.map(renderCandidate)}
                </ul>
              )}
              {otherAmounts.length > 0 && (
                <details className="min-w-0">
                  <summary className="flex min-h-11 cursor-pointer items-center font-semibold">
                    Transaksi lain di sekitar jatuh tempo ({otherAmounts.length})
                  </summary>
                  <ul className="min-w-0 space-y-1">{otherAmounts.map(renderCandidate)}</ul>
                </details>
              )}

              <div className="flex flex-wrap gap-2 border-t border-[var(--border)] pt-2">
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => occurrence.mutate({ kind: "skip", rule })}
                  className="min-h-11 rounded-lg border border-[var(--border)] px-3 font-semibold disabled:opacity-50"
                >
                  Lewati kali ini
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => occurrence.mutate({ kind: "record", rule })}
                  className="min-h-11 rounded-lg border border-[var(--border)] px-3 font-semibold disabled:opacity-50"
                >
                  {hasSameAmount ? "Bukan salah satunya, catat manual" : "Catat manual (tidak tertangkap)"}
                </button>
              </div>
              {rule.type === "investment_topup" && <p className="text-[var(--muted)]">Catat top up hanya setelah debit berhasil.</p>}
              {errors[rule.id] && (
                <p role="alert" className="font-medium text-rose-700 dark:text-rose-300">
                  {errors[rule.id]}
                </p>
              )}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
