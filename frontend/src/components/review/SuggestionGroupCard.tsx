"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { Choice } from "@/components/review/triage/Choice";
import { ItemHead } from "@/components/review/triage/ItemHead";
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
  headingId: string;
  pending: boolean;
  onDecide: (request: SuggestionDecisionRequest) => Promise<boolean>;
}

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

/** A "Saran dari riwayat" group as a Tinjau item: 1 Terima saran, 2 Tolak, and each row on its own. */
export function SuggestionGroupCard({ group, headingId, pending, onDecide }: SuggestionGroupCardProps) {
  const { bal } = useAppCtx();
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
  const first = group.suggestions[0];

  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={{ tone: "neutral", icon: "sparkles", text: "Saran dari riwayat" }}
        title={group.label}
        account={first?.account_name}
        date={group.last_date}
        amount={{ text: `${group.type === "income" ? "+" : "−"}${bal(group.total)}` }}
      />
      <p className="ws-notice">Opsional. Tidak ada yang berubah sebelum kamu menerima.</p>
      <dl className="ws-tri-note grid gap-1.5 sm:grid-cols-[auto_1fr] sm:gap-x-3">
        <dt className="text-[var(--muted)]">Sebelum</dt>
        <dd className="min-w-0 space-y-0.5 text-[var(--text-2)]">
          {group.before.map((before) => (
            <span key={`${before.label}|${before.category_name}`} className="block break-words">
              {before.label} · {before.category_name}
              {group.before.length > 1 && <span className="tabular"> ({before.count})</span>}
            </span>
          ))}
        </dd>
        <dt className="text-[var(--muted)]">Sesudah</dt>
        <dd className="min-w-0 break-words font-medium">{afterLabel}</dd>
      </dl>
      <p className="ws-caption tabular">
        {[`${group.count} transaksi`, range, ...group.sources.map(suggestionSourceLabel)].filter(Boolean).join(" · ")}
      </p>

      <div role="group" aria-label="Pilihan" className="ws-choices">
        <Choice
          n={1}
          primary
          disabled={pending}
          label={`Terima saran (${group.count})`}
          sub="Nama, kategori dan pilar diperbarui"
          onClick={() => void onDecide({ decision: "accept", ids, label: group.label })}
        />
        <Choice
          n={2}
          disabled={pending}
          label="Tolak"
          sub="Saran ini tidak diusulkan lagi"
          onClick={() => void onDecide({ decision: "reject", ids, label: group.label })}
        />
      </div>

      <ul className="ws-rows" aria-label={`Saran untuk ${group.label}`}>
        {group.suggestions.map((item) => (
          <li key={item.id}>
            <span className="ws-rows-t">
              <span className="sr-only">Sebelum: </span>{valuesLabel(item.before)}
              <span aria-hidden="true"> → </span>
              <span className="sr-only">, sesudah: </span>
              {valuesLabel(item.after, item.after.offset_category_name)}
              <span className="tabular">
                {[shortDate(item.date), item.account_name, item.reason,
                  item.confidence !== null ? `keyakinan ${Math.round(item.confidence * 100)}%` : null].filter(Boolean).join(" · ")}
              </span>
            </span>
            <span className="flex shrink-0 flex-wrap items-center justify-end gap-2">
              <span className="tabular font-medium">{item.type === "income" ? "+" : "−"}{bal(item.amount)}</span>
              <button
                type="button"
                disabled={pending}
                aria-label={`Terima saran untuk ${rowName(item)}`}
                onClick={() => void onDecide({ decision: "accept", ids: [item.id], label: group.label })}
                className="ws-btn ws-btn-sm"
              >
                Terima
              </button>
              <button
                type="button"
                disabled={pending}
                aria-label={`Tolak saran untuk ${rowName(item)}`}
                onClick={() => void onDecide({ decision: "reject", ids: [item.id], label: group.label })}
                className="ws-btn ws-btn-sm"
              >
                Tolak
              </button>
            </span>
          </li>
        ))}
      </ul>
    </>
  );
}
