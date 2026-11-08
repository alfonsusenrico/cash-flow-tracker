"use client";

import { useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { bucketName } from "@/components/beranda/words";
import { CategoryDialog, type CategoriesState, type CategoryDialogMode } from "@/components/review/CategoryDialog";
import { ListRow } from "@/components/workspace/lists";
import type { ReviewAction, ReviewGroup } from "@/lib/review";
import { transaksiHref } from "@/lib/triage";
import { Answer, AnswerList } from "../Answers";
import { SignedAmount } from "../ItemBody";
import { dateTime, groupLabel } from "../copy";
import type { ItemContext, ItemParts, SaveResult } from "../types";

export interface GroupActionRequest {
  action: ReviewAction;
  ids: string[];
  fields?: Record<string, string>;
  label: string;
}

const ROWS_SHOWN = 10;

/** 1 accepts every row (E), 2 and 3 open "Ubah kategori" and "Ganti nama", each one request (D9, D10). */
function GroupAnswers({
  group,
  label,
  automatic,
  ctx,
  categories,
  onAction,
}: {
  group: ReviewGroup;
  label: string;
  automatic: boolean;
  ctx: ItemContext;
  categories: CategoriesState;
  onAction: (request: GroupActionRequest) => Promise<SaveResult>;
}) {
  const t = useTranslations("review");
  const [mode, setMode] = useState<CategoryDialogMode | null>(null);
  const categoryOpener = useRef<HTMLButtonElement>(null);
  const nameOpener = useRef<HTMLButtonElement>(null);
  const ids = group.transactions.map((row) => row.id);
  const close = () => {
    const opener = mode === "name" ? nameOpener : categoryOpener;
    setMode(null);
    opener.current?.focus();
  };
  return (
    <>
      <AnswerList labelledBy={ctx.headingId}>
        <Answer
          n={1}
          primary
          disabled={ctx.pending}
          label={automatic ? t("answer.correct") : t("answer.acceptAll", { n: ctx.words.format.integer(group.count) })}
          detail={automatic ? t("answer.correctDetail") : t("answer.acceptAllDetail")}
          onClick={() => void onAction({ action: "accept", ids, label })}
        />
        <Answer
          ref={categoryOpener}
          n={2}
          label={t("answer.changeCategory")}
          detail={t("answer.changeCategoryDetail")}
          disabled={ctx.pending}
          onClick={() => setMode("category")}
        />
        <Answer
          ref={nameOpener}
          n={3}
          label={t("answer.rename")}
          detail={t("answer.renameDetail")}
          disabled={ctx.pending}
          onClick={() => setMode("name")}
        />
      </AnswerList>
      <CategoryDialog
        open={mode !== null}
        mode={mode ?? "category"}
        onClose={close}
        label={label}
        count={group.count}
        type={group.type}
        merchant={group.kind === "merchant" ? group.label : undefined}
        remembered={group.kind === "merchant" ? t("dialog.category.remembered", { label }) : t("dialog.category.rememberedSameText")}
        categories={categories}
        onSave={async (fields) => {
          const result = await onAction({ action: mode === "name" ? "rename" : "recategorise", ids, fields, label });
          if (result.saved) setMode(null);
          return result;
        }}
      />
    </>
  );
}

/** A group in "Perlu jawaban" or "Dicatat otomatis": one question for every row, the rows as links below (D9). */
export function groupParts(
  group: ReviewGroup,
  automatic: boolean,
  ctx: ItemContext,
  categories: CategoriesState,
  onAction: (request: GroupActionRequest) => Promise<SaveResult>,
): ItemParts {
  const { t, format } = ctx.words;
  const label = groupLabel(t, group);
  const category = group.category?.name;
  const question = automatic
    ? t("review.question.groupAuto", { label })
    : category
      ? t("review.question.groupCategory", { label, category })
      : t("review.question.groupUnknown", { label });
  return {
    question,
    meta: [
      t(`review.meta.groupKind.${group.kind}`),
      <SignedAmount key="amount" amount={group.total} direction={group.type} />,
      t("review.meta.transactions", { count: group.count }),
      format.dateRangeAt(group.first_date, group.last_date),
    ],
    note: automatic ? t("review.meta.optionalAuto") : undefined,
    reasons: group.reasons,
    answers: (
      <GroupAnswers key={`answers:${group.key}`} group={group} label={label} automatic={automatic} ctx={ctx} categories={categories} onAction={onAction} />
    ),
    rows: <GroupTransactions key={`rows:${group.key}`} group={group} automatic={automatic} ctx={ctx} />,
  };
}

/** "Transaksi · n": up to ten whole-row links to the transaction, then "Lihat semua n" (D9). */
function GroupTransactions({ group, automatic, ctx }: { group: ReviewGroup; automatic: boolean; ctx: ItemContext }) {
  const { t, format } = ctx.words;
  const [all, setAll] = useState(false);
  const rows = all ? group.transactions : group.transactions.slice(0, ROWS_SHOWN);
  return (
    <div className="tj-rows">
      <div className="sec-h">
        <h3 className="t-label">{t("review.rows.transactions", { n: format.integer(group.count) })}</h3>
      </div>
      <ul className="list" aria-label={t("review.rows.transactionsOf", { label: groupLabel(t, group) })}>
        {rows.map((row) => (
          <li key={row.id}>
            <ListRow
              layout="stacked"
              href={transaksiHref(row.id, automatic ? "otomatis" : "jawab")}
              title={row.notes || row.merchant || row.category_name || t("terms.noDescription")}
              meta={[
                dateTime(format, row.date),
                row.account_name,
                row.category_name ?? t("terms.noCategory"),
                row.kakeibo_type ? bucketName(t, row.kakeibo_type) : null,
                row.review_status === "needs_review" ? t("review.rows.needsReview") : t("review.rows.automatic"),
              ].filter(Boolean).join(" · ")}
              amount={<SignedAmount amount={row.amount} direction={row.type} />}
              chevron
            />
          </li>
        ))}
      </ul>
      {group.transactions.length > ROWS_SHOWN && !all && (
        <div className="list-foot">
          <button type="button" className="ws-btn ws-btn-quiet" onClick={() => setAll(true)}>
            {t("review.rows.showAll", { n: format.integer(group.transactions.length) })}
          </button>
        </div>
      )}
    </div>
  );
}
