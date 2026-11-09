"use client";

import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { useFormat, type Format } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import type { DueExpectation, OccurrenceCandidate } from "@/types/recurring";

type OccurrenceAction =
  | { kind: "link"; transactionId: string }
  | { kind: "skip" }
  | { kind: "record" };

const MATCH_KEYS = {
  none: "matchNone",
  single: "matchSingle",
  ambiguous: "matchAmbiguous",
} as const satisfies Record<DueExpectation["match_state"], string>;

function candidateLabel(candidate: OccurrenceCandidate, format: Format) {
  return `${format.shortDateAt(candidate.date)} · ${format.money(candidate.amount)} · ${candidate.merchant || candidate.notes || candidate.account_name}`;
}

/**
 * One due occurrence of a plan: link a recorded payment, skip it, or record a missed one. Used by "Rencana jatuh
 * tempo" and by the plan's detail in "Rencana & Pengingat" (apply-markas-target D20); nothing is posted by itself.
 * With `withName` false the caller already shows the plan's name.
 */
export function DuePlanActions({ rule, withName = true }: { rule: DueExpectation; withName?: boolean }) {
  const t = useTranslations("shell.duePlans");
  const format = useFormat();
  const qc = useQueryClient();
  const [error, setError] = useState("");

  const occurrence = useMutation({
    mutationFn: async (action: OccurrenceAction) => {
      const base = `/recurring/${rule.id}/occurrences`;
      const scheduled = { scheduled_for: rule.next_due_date };
      if (action.kind === "link") return api.post(`${base}/link`, { ...scheduled, transaction_id: action.transactionId });
      if (action.kind === "skip") return api.post(`${base}/skip`, scheduled);
      const reviewed = (rule.candidates ?? []).filter((item) => item.exact_amount).map((item) => item.id);
      return api.post(`${base}/record`, { ...scheduled, acknowledged_candidate_ids: reviewed });
    },
    onMutate: () => setError(""),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
    },
    onError: (failure: Error) => {
      setError(failure.message || t("updateFailed"));
      qc.invalidateQueries({ queryKey: queryKeys.recurring.pending });
    },
  });

  const isTransfer = rule.type === "transfer" || rule.type === "investment_topup";
  const candidates = rule.candidates ?? [];
  const busy = occurrence.isPending;
  const sameAmount = candidates.filter((item) => item.exact_amount);
  const otherAmounts = candidates.filter((item) => !item.exact_amount);
  const renderCandidate = (candidate: OccurrenceCandidate) => (
    <li key={candidate.id} className="flex min-w-0 items-center justify-between gap-2">
      <span className="min-w-0 truncate tabular-nums">{candidateLabel(candidate, format)}</span>
      <button
        type="button"
        disabled={busy}
        onClick={() => occurrence.mutate({ kind: "link", transactionId: candidate.id })}
        aria-label={t("linkLabel", { candidate: candidateLabel(candidate, format), name: rule.name })}
        className="min-h-11 shrink-0 rounded-md px-2 font-semibold underline underline-offset-2 disabled:opacity-50"
      >
        {t("link")}
      </button>
    </li>
  );

  return (
    <>
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          {withName && <h3 className="truncate font-semibold">{rule.name}</h3>}
          <p className="truncate text-xs text-[var(--muted)]">
            {isTransfer ? `${rule.source_account_name} → ${rule.target_account_name}` : rule.source_account_name}
            {rule.obligation_name ? ` · ${rule.obligation_name}` : ""}
          </p>
          <p className="text-xs tabular-nums text-[var(--muted)]">{t("dueOn", { day: format.shortDate(rule.next_due_date) })}</p>
        </div>
        <span className="shrink-0 font-semibold tabular-nums">{format.money(rule.amount)}</span>
      </div>

      <p className="text-[var(--text-2)]">{t(MATCH_KEYS[rule.match_state])}</p>
      {sameAmount.length > 0 && (
        <ul aria-label={t("sameAmountList", { name: rule.name })} className="min-w-0 space-y-1">
          {sameAmount.map(renderCandidate)}
        </ul>
      )}
      {otherAmounts.length > 0 && (
        <details className="min-w-0">
          <summary className="flex min-h-11 cursor-pointer items-center font-semibold">
            {t("otherAmounts", { n: String(otherAmounts.length) })}
          </summary>
          <ul className="min-w-0 space-y-1">{otherAmounts.map(renderCandidate)}</ul>
        </details>
      )}

      <div className="flex flex-wrap gap-2 border-t border-[var(--line)] pt-2">
        <button
          type="button"
          disabled={busy}
          onClick={() => occurrence.mutate({ kind: "skip" })}
          className="min-h-11 rounded-md border border-[var(--line)] px-3 font-semibold disabled:opacity-50"
        >
          {t("skip")}
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => occurrence.mutate({ kind: "record" })}
          className="min-h-11 rounded-md border border-[var(--line)] px-3 font-semibold disabled:opacity-50"
        >
          {sameAmount.length > 0 ? t("recordNotListed") : t("recordMissed")}
        </button>
      </div>
      {rule.type === "investment_topup" && <p className="text-[var(--muted)]">{t("topupAfterDebit")}</p>}
      {error && (
        <p role="alert" className="font-medium text-out-fg">
          {error}
        </p>
      )}
    </>
  );
}
