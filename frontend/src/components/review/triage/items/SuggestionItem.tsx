"use client";

import { bucketName } from "@/components/beranda/words";
import { ListRow } from "@/components/workspace/lists";
import type { HistorySuggestion, SuggestionGroup, SuggestionValues } from "@/lib/historySuggestions";
import type { Translate } from "@/lib/i18n/types";
import { Answer, AnswerList } from "../Answers";
import { SignedAmount } from "../ItemBody";
import type { ItemContext, ItemParts, SaveResult } from "../types";

export type SuggestionDecision = "accept" | "reject";

export interface SuggestionDecisionRequest {
  decision: SuggestionDecision;
  ids: string[];
  label: string;
}

function valuesLabel(t: Translate, values: SuggestionValues, offsetName?: string | null): string {
  return [
    values.notes || values.merchant || t("terms.noDescription"),
    values.category_name ?? t("terms.noCategory"),
    values.kakeibo ? bucketName(t, values.kakeibo) : null,
    offsetName ? t("review.rows.reduces", { name: offsetName }) : null,
  ].filter(Boolean).join(" · ");
}

/** One suggestion: before → after as a plain row with its own "Terima" and "Tolak" (D9). */
function SuggestionRow({ item, label, ctx, onDecide }: {
  item: HistorySuggestion;
  label: string;
  ctx: ItemContext;
  onDecide: (request: SuggestionDecisionRequest) => Promise<SaveResult>;
}) {
  const { t, format } = ctx.words;
  const date = format.yearDateAt(item.date);
  const name = t("review.rows.rowName", { name: item.before.notes || item.before.merchant || t("review.rows.rowNameFallback"), date });
  return (
    <ListRow
      layout="stacked"
      title={
        <>
          <span className="sr-only">{t("review.rows.before")} </span>
          {valuesLabel(t, item.before)}
          <span aria-hidden="true"> → </span>
          <span className="sr-only">, {t("review.rows.after")} </span>
          {valuesLabel(t, item.after, item.after.offset_category_name)}
        </>
      }
      meta={[
        date,
        item.account_name,
        t(`review.source.${item.source}`),
        item.reason,
        item.confidence !== null ? t("review.rows.confidence", { percent: format.integer(item.confidence * 100) }) : null,
      ].filter(Boolean).join(" · ")}
      amount={<SignedAmount amount={item.amount} direction={item.type} />}
      actions={
        <>
          <button
            type="button"
            className="ws-btn ws-btn-quiet ws-btn-sm"
            disabled={ctx.pending}
            aria-label={t("review.rows.acceptFor", { row: name })}
            onClick={() => void onDecide({ decision: "accept", ids: [item.id], label })}
          >
            {t("review.rows.accept")}
          </button>
          <button
            type="button"
            className="ws-btn ws-btn-quiet ws-btn-sm"
            disabled={ctx.pending}
            aria-label={t("review.rows.rejectFor", { row: name })}
            onClick={() => void onDecide({ decision: "reject", ids: [item.id], label })}
          >
            {t("review.rows.reject")}
          </button>
        </>
      }
    />
  );
}

/** A "Saran dari riwayat" group as a Tinjau item: 1 accepts every suggestion (E), 2 rejects them, rows each (D9). */
export function suggestionParts(
  group: SuggestionGroup,
  ctx: ItemContext,
  onDecide: (request: SuggestionDecisionRequest) => Promise<SaveResult>,
): ItemParts {
  const { t, format } = ctx.words;
  const ids = group.suggestions.map((item) => item.id);
  const label = group.label;
  return {
    question: t("review.question.suggestion", { label, category: group.category.name, count: group.count }),
    meta: [
      t("review.meta.fromHistory"),
      <SignedAmount key="amount" amount={group.total} direction={group.type} />,
      t("review.meta.transactions", { count: group.count }),
      format.dateRangeAt(group.first_date, group.last_date),
    ],
    note: t("review.meta.optionalSuggestion"),
    answers: (
      <AnswerList labelledBy={ctx.headingId}>
        <Answer
          n={1}
          primary
          disabled={ctx.pending}
          label={t("review.answer.acceptSuggestion", { n: format.integer(group.count) })}
          detail={t("review.answer.acceptSuggestionDetail")}
          onClick={() => void onDecide({ decision: "accept", ids, label })}
        />
        <Answer
          n={2}
          disabled={ctx.pending}
          label={t("review.answer.reject")}
          detail={t("review.answer.rejectDetail")}
          onClick={() => void onDecide({ decision: "reject", ids, label })}
        />
      </AnswerList>
    ),
    rows: (
      <div className="tj-rows">
        <ul className="list" aria-label={t("review.rows.suggestionsFor", { label })}>
          {group.suggestions.map((item) => (
            <li key={item.id}>
              <SuggestionRow item={item} label={label} ctx={ctx} onDecide={onDecide} />
            </li>
          ))}
        </ul>
      </div>
    ),
  };
}
