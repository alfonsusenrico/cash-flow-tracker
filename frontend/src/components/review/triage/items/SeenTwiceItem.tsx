"use client";

import { ListRow } from "@/components/workspace/lists";
import type { SeenTwiceItem } from "@/lib/review";
import { transaksiHref } from "@/lib/triage";
import { Answer, AnswerList } from "../Answers";
import { SignedAmount } from "../ItemBody";
import { dateTime } from "../copy";
import type { ItemContext, ItemParts, SaveResult } from "../types";

function direction(type: SeenTwiceItem["type"]): "expense" | "income" | "transfer" {
  return type === "income" ? "income" : type === "expense" ? "expense" : "transfer";
}

/**
 * A second app's report of money already recorded once ("Terlihat dua kali"), or one an owner's account link
 * skipped ("Dilewati otomatis", only "Catat terpisah"): the recorded leg as a link and this report as a plain row
 * (recognise-one-arrival-seen-twice D3, reconcile-linked-account-reports D5, D6, apply-markas-tinjau D9).
 */
export function seenTwiceParts(
  item: SeenTwiceItem,
  ctx: ItemContext,
  onAcknowledge: (item: SeenTwiceItem) => Promise<SaveResult>,
  onRecordSeparately: (item: SeenTwiceItem) => Promise<SaveResult>,
): ItemParts {
  const { t, format } = ctx.words;
  const skipped = item.kind === "skipped";
  const { kept, link } = item;
  const route = kept?.movement
    ? t("review.rows.route", { source: kept.movement.source ?? "?", target: kept.movement.target ?? "?" })
    : kept?.account_name ?? null;
  const recordDetail = item.account_name
    ? t("review.answer.recordSeparatelyIn", { account: item.account_name })
    : t("review.answer.recordSeparatelyOwn");
  const accountLine = link
    ? t("review.rows.linked", { account: link.account_label ?? t("review.answer.account"), institution: link.institution_label })
    : !skipped && kept?.account_name
      ? t("review.rows.sameAccount", { account: kept.account_name, app: item.app })
      : null;
  const amount = item.amount === null ? t("review.meta.amountUnreadable") : <SignedAmount key="amount" amount={item.amount} direction={direction(item.type)} />;
  return {
    question: skipped ? t("review.question.skipped", { app: item.app }) : t("review.question.seenTwice", { app: item.app }),
    meta: [skipped ? t("review.meta.skipped") : t("review.meta.maybeTwice"), amount, dateTime(format, item.date), item.account_name],
    answers: (
      <AnswerList labelledBy={ctx.headingId}>
        {skipped ? (
          <Answer n={1} disabled={ctx.pending} label={t("review.answer.recordSeparately")} detail={recordDetail} onClick={() => void onRecordSeparately(item)} />
        ) : (
          <>
            <Answer
              n={1}
              primary
              disabled={ctx.pending}
              label={t("review.answer.sameOne")}
              detail={t("review.answer.sameOneDetail")}
              onClick={() => void onAcknowledge(item)}
            />
            <Answer n={2} disabled={ctx.pending} label={t("review.answer.recordSeparately")} detail={recordDetail} onClick={() => void onRecordSeparately(item)} />
          </>
        )}
      </AnswerList>
    ),
    rows: (
      <div className="tj-rows">
        <ul className="list">
          <li>
            {kept ? (
              <ListRow
                href={transaksiHref(kept.transaction_id, skipped ? "lewat" : "dua")}
                title={t("review.rows.recorded", { place: route ?? t("review.answer.account") })}
                meta={dateTime(format, kept.date) ?? t("review.rows.noTime")}
                amount={<SignedAmount amount={kept.amount} direction={direction(kept.type)} />}
                chevron
              />
            ) : (
              <ListRow title={t("review.rows.recordedGone")} />
            )}
          </li>
          <li>
            <ListRow
              title={t("review.rows.thisReport", { app: item.app })}
              meta={[dateTime(format, item.date), skipped ? t("review.meta.skipped") : t("review.rows.notRecordedAgain")].filter(Boolean).join(" · ")}
              amount={item.amount === null ? undefined : <SignedAmount amount={item.amount} direction={direction(item.type)} />}
            />
          </li>
        </ul>
        {accountLine && <p className="t-meta tj-line">{accountLine}</p>}
      </div>
    ),
  };
}
