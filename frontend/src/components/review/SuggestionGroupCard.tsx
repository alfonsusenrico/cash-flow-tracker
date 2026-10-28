"use client";

import { useId } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { kakeiboLabel } from "@/lib/kakeibo";
import { dateRangeLabel } from "@/lib/review";
import {
  shortDate,
  suggestionSourceLabel,
  type HistorySuggestion,
  type SuggestionGroup,
  type SuggestionValues,
} from "@/lib/historySuggestions";

export type SuggestionDecision = "accept" | "reject";

export interface SuggestionDecisionRequest {
  decision: SuggestionDecision;
  ids: string[];
  label: string;
}

interface SuggestionGroupCardProps {
  group: SuggestionGroup;
  pending: boolean;
  onDecide: (request: SuggestionDecisionRequest) => Promise<boolean>;
}

const PRIMARY = "min-h-11 rounded-xl bg-[#1E201E] px-4 text-xs font-bold text-white transition-transform active:scale-95 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50 dark:border dark:border-white/15";
const SECONDARY = "min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 text-xs font-semibold text-[var(--text)] transition-colors hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50";

function valuesLabel(values: SuggestionValues, offsetName?: string | null): string {
  return [
    values.notes || values.merchant || "Tanpa keterangan",
    values.category_name ?? "Tanpa kategori",
    values.kakeibo ? kakeiboLabel(values.kakeibo) : null,
    offsetName ? `mengurangi ${offsetName}` : null,
  ].filter(Boolean).join(" · ");
}

function rowName(item: HistorySuggestion): string {
  return `${item.before.notes || item.before.merchant || "transaksi"} ${shortDate(item.date)}`;
}

export function SuggestionGroupCard({ group, pending, onDecide }: SuggestionGroupCardProps) {
  const { bal } = useAppCtx();
  const headingId = `${useId()}-heading`;
  const ids = group.suggestions.map((item) => item.id);
  const range = dateRangeLabel(group.first_date, group.last_date);
  const proposedNotes = new Set(group.suggestions.map((item) => item.after.notes ?? ""));
  const sharedNotes = proposedNotes.size === 1 ? [...proposedNotes][0] : null;
  const afterLabel = [
    group.label,
    sharedNotes && sharedNotes !== group.label ? sharedNotes : null,
    group.category.name,
    group.kakeibo ? kakeiboLabel(group.kakeibo) : null,
    group.offset_category_names.length ? `mengurangi ${group.offset_category_names.join(", ")}` : null,
  ].filter(Boolean).join(" · ");

  return (
    <article aria-labelledby={headingId} className="space-y-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-xs">
      <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-1">
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-semibold uppercase tracking-wider text-[var(--muted)]">
            {group.type === "income" ? "Pemasukan" : "Pengeluaran"}
          </p>
          <h3 id={headingId} className="break-words text-sm font-bold text-[var(--text)]">{group.label}</h3>
          <p className="mt-0.5 text-xs text-[var(--muted)] tabular">
            {[`${group.count} transaksi`, range].filter(Boolean).join(" · ")}
          </p>
        </div>
        <div className="shrink-0 text-right">
          <span className="block text-sm font-bold tabular text-[var(--text)]">
            {group.type === "income" ? "+" : "-"}{bal(group.total)}
          </span>
          <span className="block text-[11px] text-[var(--muted)]">{group.type === "income" ? "masuk" : "keluar"}</span>
        </div>
      </div>

      <dl className="grid gap-1.5 rounded-xl border border-[var(--border)] p-3 text-xs sm:grid-cols-[auto_1fr] sm:gap-x-3">
        <dt className="font-semibold text-[var(--muted)]">Sebelum</dt>
        <dd className="min-w-0 space-y-0.5 text-[var(--text-secondary)]">
          {group.before.map((before) => (
            <p key={`${before.label}|${before.category_name}`} className="break-words">
              {before.label} · {before.category_name}
              {group.before.length > 1 && <span className="tabular"> ({before.count})</span>}
            </p>
          ))}
        </dd>
        <dt className="font-semibold text-[var(--muted)]">Sesudah</dt>
        <dd className="min-w-0 break-words font-semibold text-[var(--text)]">{afterLabel}</dd>
      </dl>

      <ul className="flex flex-wrap gap-1.5 text-[11px]" aria-label="Asal saran">
        {group.sources.map((source) => (
          <li key={source} className="rounded-full border border-[var(--border)] px-2 py-0.5 text-[var(--text-secondary)]">
            {suggestionSourceLabel(source)}
          </li>
        ))}
      </ul>

      <div className="flex flex-wrap gap-2">
        <button type="button" disabled={pending} onClick={() => onDecide({ decision: "accept", ids, label: group.label })} className={PRIMARY}>
          Terima semua ({group.count})
        </button>
        <button type="button" disabled={pending} onClick={() => onDecide({ decision: "reject", ids, label: group.label })} className={SECONDARY}>
          Tolak semua
        </button>
      </div>

      <details>
        <summary className="inline-flex min-h-11 cursor-pointer items-center rounded-lg text-xs font-semibold text-[var(--text-secondary)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]">
          Lihat {group.count} saran
        </summary>
        <ul className="mt-1 divide-y divide-[var(--border)]">
          {group.suggestions.map((item) => (
            <li key={item.id} className="space-y-1.5 py-2.5 text-xs">
              <div className="flex flex-wrap items-baseline justify-between gap-x-3">
                <p className="text-[11px] text-[var(--muted)] tabular">
                  {[shortDate(item.date), item.account_name, suggestionSourceLabel(item.source),
                    item.confidence !== null ? `keyakinan ${Math.round(item.confidence * 100)}%` : null].filter(Boolean).join(" · ")}
                </p>
                <span className="font-semibold tabular text-[var(--text)]">
                  {item.type === "income" ? "+" : "-"}{bal(item.amount)}
                </span>
              </div>
              <p className="break-words text-[var(--text-secondary)]">
                <span className="sr-only">Sebelum: </span>{valuesLabel(item.before)}
                <span aria-hidden="true"> → </span>
                <span className="sr-only">, sesudah: </span>
                <span className="font-semibold text-[var(--text)]">{valuesLabel(item.after, item.after.offset_category_name)}</span>
              </p>
              <p className="text-[11px] text-[var(--muted)]">{item.reason}</p>
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={pending}
                  aria-label={`Terima saran untuk ${rowName(item)}`}
                  onClick={() => onDecide({ decision: "accept", ids: [item.id], label: group.label })}
                  className={SECONDARY}
                >
                  Terima
                </button>
                <button
                  type="button"
                  disabled={pending}
                  aria-label={`Tolak saran untuk ${rowName(item)}`}
                  onClick={() => onDecide({ decision: "reject", ids: [item.id], label: group.label })}
                  className={SECONDARY}
                >
                  Tolak
                </button>
              </div>
            </li>
          ))}
        </ul>
      </details>
    </article>
  );
}
