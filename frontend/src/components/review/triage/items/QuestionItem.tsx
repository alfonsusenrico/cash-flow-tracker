"use client";

import { useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { AnswerDialog } from "@/components/review/AnswerDialog";
import { bucketName } from "@/components/beranda/words";
import type { Words } from "@/lib/i18n/types";
import type { PurposeQuestionEntry, PurposeSuggestion } from "@/lib/review";
import { Answer, AnswerList } from "../Answers";
import { SignedAmount } from "../ItemBody";
import { dateTime } from "../copy";
import type { ItemContext, ItemParts, SaveResult } from "../types";

export type PurposeAnswerBody = { suggestion_id: string } | { text: string };

function suggestionDetail({ t, format }: Words, suggestion: PurposeSuggestion, first: boolean): string {
  return [
    first ? t("review.answer.suggested") : null,
    suggestion.category_name,
    suggestion.kakeibo ? bucketName(t, suggestion.kakeibo) : null,
    suggestion.kind === "habit" && suggestion.basis
      ? t("review.answer.habitBasis", { agreeing: format.integer(suggestion.basis.agreeing), support: format.integer(suggestion.basis.support) })
      : null,
  ].filter(Boolean).join(" · ");
}

/** The server's suggestions, numbered with "Saran" first (E), then the quiet "Jawab sendiri" (D9, D11). */
function QuestionAnswers({
  entry,
  question,
  ctx,
  onAnswer,
}: {
  entry: PurposeQuestionEntry;
  question: string;
  ctx: ItemContext;
  onAnswer: (body: PurposeAnswerBody) => Promise<SaveResult>;
}) {
  const t = useTranslations();
  const [typing, setTyping] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);
  const close = () => {
    setTyping(false);
    opener.current?.focus();
  };
  const { suggestions } = entry.question;
  return (
    <>
      {suggestions.length > 0 && (
        <AnswerList labelledBy={ctx.headingId}>
          {suggestions.map((suggestion, index) => (
            <Answer
              key={suggestion.id}
              n={index + 1}
              primary={index === 0}
              disabled={ctx.pending}
              label={suggestion.label}
              detail={suggestionDetail(ctx.words, suggestion, index === 0)}
              onClick={() => void onAnswer({ suggestion_id: suggestion.id })}
            />
          ))}
        </AnswerList>
      )}
      <div className="tj-after">
        <button ref={opener} type="button" className="ws-btn ws-btn-quiet" aria-haspopup="dialog" onClick={() => setTyping(true)}>
          {t("review.answer.typeOwn")}
        </button>
      </div>
      <AnswerDialog
        open={typing}
        onClose={close}
        question={question}
        maxLength={entry.question.text_max_length}
        onSave={async (text) => {
          const result = await onAnswer({ text });
          if (result.saved) setTyping(false);
          return result;
        }}
      />
    </>
  );
}

/** "Belum dikenali": the server's question, or one without its amount while balances are hidden (D9, D17). */
export function questionParts(
  entry: PurposeQuestionEntry,
  ctx: ItemContext,
  onAnswer: (body: PurposeAnswerBody) => Promise<SaveResult>,
): ItemParts {
  const { t, format } = ctx.words;
  const { transaction } = entry;
  const income = transaction.type === "income";
  const question = ctx.hidden ? t("review.question.purposeHidden", { account: transaction.account_name }) : entry.question.prompt;
  return {
    question,
    meta: [
      income ? t("review.meta.moneyIn") : t("review.meta.noStore"),
      <SignedAmount key="amount" amount={transaction.amount} direction={income ? "income" : "expense"} />,
      dateTime(format, transaction.date),
      transaction.account_name,
    ],
    answers: <QuestionAnswers key={`answers:${entry.event_id}`} entry={entry} question={question} ctx={ctx} onAnswer={onAnswer} />,
    raw: transaction.notes,
  };
}
