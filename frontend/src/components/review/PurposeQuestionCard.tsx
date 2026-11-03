"use client";

import { useId, useState, type FormEvent } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Choice } from "@/components/review/triage/Choice";
import { ItemHead } from "@/components/review/triage/ItemHead";
import { kakeiboLabel } from "@/lib/kakeibo";
import { habitBasisText } from "@/lib/provenance";
import type { PurposeQuestionEntry, PurposeSuggestion } from "@/lib/review";

export type PurposeAnswerBody = { suggestion_id: string } | { text: string };

interface PurposeQuestionCardProps {
  entry: PurposeQuestionEntry;
  headingId: string;
  pending: boolean;
  /** Posts the answer with a fresh reply id; true when it was saved. */
  onAnswer: (entry: PurposeQuestionEntry, body: PurposeAnswerBody) => Promise<boolean>;
}

function suggestionSub(suggestion: PurposeSuggestion, first: boolean): string {
  return [
    first ? "Saran" : null,
    suggestion.category_name,
    suggestion.kakeibo ? kakeiboLabel(suggestion.kakeibo) : null,
    suggestion.kind === "habit" && suggestion.basis ? habitBasisText(suggestion.basis) : null,
  ].filter(Boolean).join(" · ");
}

/** "Belum dikenali": numbered suggestions with "Saran" first, then "Jawab sendiri" (D4). */
export function PurposeQuestionCard({ entry, headingId, pending, onAnswer }: PurposeQuestionCardProps) {
  const { bal, hideBalances } = useAppCtx();
  const inputId = useId();
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const { question, transaction } = entry;
  const maxLength = question.text_max_length ?? 200;

  const submitText = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = text.trim();
    if (!trimmed) {
      setError("Tulis jawaban singkat, misalnya \"makan siang di kantin\".");
      return;
    }
    setError("");
    if (await onAnswer(entry, { text: trimmed })) setText("");
  };

  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={{ tone: "warn", icon: "help-circle", text: "Belum dikenali" }}
        // The server's prompt names the amount; with balances hidden the title leaves it out.
        title={hideBalances ? `Apa ini? Dari ${transaction.account_name}` : question.prompt}
        account={transaction.account_name}
        date={transaction.date}
        source="notification"
        amount={{ text: `${transaction.type === "income" ? "+" : "−"}${bal(transaction.amount)}` }}
      />
      {transaction.notes && (
        <div className="ws-tri-note">
          <small>Tertulis di notifikasi</small>
          <p>{transaction.notes}</p>
        </div>
      )}
      {question.suggestions.length > 0 && (
        <div role="group" aria-label="Pilihan jawaban" className="ws-choices">
          {question.suggestions.map((suggestion, index) => (
            <Choice
              key={suggestion.id}
              n={index + 1}
              primary={index === 0}
              disabled={pending}
              label={suggestion.label}
              sub={suggestionSub(suggestion, index === 0)}
              onClick={() => void onAnswer(entry, { suggestion_id: suggestion.id })}
            />
          ))}
        </div>
      )}
      <form onSubmit={submitText} className="ws-form-row mt-4">
        <label htmlFor={inputId} className="ws-field">
          Jawab sendiri
          <input
            id={inputId}
            value={text}
            maxLength={maxLength}
            onChange={(event) => setText(event.target.value)}
            placeholder="misalnya: sewa lapangan futsal"
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? `${inputId}-error` : undefined}
          />
        </label>
        <button type="submit" disabled={pending} className="ws-btn ws-btn-ink">
          {pending ? "Menyimpan…" : "Simpan jawaban"}
        </button>
      </form>
      {error && <p id={`${inputId}-error`} role="alert" className="ws-alert">{error}</p>}
    </>
  );
}
