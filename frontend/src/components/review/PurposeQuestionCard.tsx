"use client";

import { useId, useState, type FormEvent } from "react";
import { useMutation } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useAppCtx } from "@/components/layout/AppLayout";
import { kakeiboLabel } from "@/lib/kakeibo";
import { newReplyId, reviewErrorMessage, type PurposeQuestionEntry } from "@/lib/review";

interface AnswerResponse {
  ok: boolean;
  answer: { status: "applied" | "needs_review"; notes: string | null };
}

type AnswerBody = { suggestion_id: string } | { text: string };

interface PurposeQuestionCardProps {
  entry: PurposeQuestionEntry;
  onAnswered: (message: string) => void;
}

export function PurposeQuestionCard({ entry, onAnswered }: PurposeQuestionCardProps) {
  const { bal } = useAppCtx();
  const headingId = useId();
  const inputId = useId();
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const { question, transaction } = entry;
  const maxLength = question.text_max_length ?? 200;

  const answer = useMutation({
    mutationFn: (body: AnswerBody) =>
      api.post<AnswerResponse>(`/ingest/notifications/${entry.event_id}/answer-purpose`, {
        question_id: question.question_id,
        reply_id: newReplyId(),
        ...body,
      }),
    onSuccess: (response) => {
      setError("");
      onAnswered(
        response.answer.status === "applied"
          ? `Jawaban tersimpan: ${response.answer.notes ?? "transaksi diperbarui"}.`
          : "Jawaban tersimpan sebagai catatan. Kategorinya masih perlu dipilih di daftar tinjauan.",
      );
    },
    onError: (failure) => setError(reviewErrorMessage(failure)),
  });

  const submitText = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = text.trim();
    if (!trimmed) {
      setError("Tulis jawaban singkat, misalnya \"makan siang di kantin\".");
      return;
    }
    answer.mutate({ text: trimmed });
  };

  const date = transaction.date
    ? new Date(transaction.date).toLocaleDateString("id-ID", { weekday: "short", day: "numeric", month: "short" })
    : null;

  return (
    <article aria-labelledby={headingId} className="space-y-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 id={headingId} className="text-sm font-bold text-[var(--text)]">{question.prompt}</h3>
          <p className="mt-0.5 text-xs text-[var(--muted)]">
            {[date, transaction.account_name, transaction.notes].filter(Boolean).join(" · ")}
          </p>
        </div>
        <span className="shrink-0 text-sm font-bold tabular text-[var(--text)]">
          {transaction.type === "income" ? "+" : "-"}{bal(transaction.amount)}
        </span>
      </div>

      {question.suggestions.length > 0 && (
        <div role="group" aria-label="Pilihan jawaban" className="flex flex-wrap gap-2">
          {question.suggestions.map((suggestion) => (
            <button
              key={suggestion.id}
              type="button"
              disabled={answer.isPending}
              onClick={() => answer.mutate({ suggestion_id: suggestion.id })}
              className="min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/50 px-3 py-1.5 text-left text-xs transition-colors hover:border-[var(--border-strong)] hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50"
            >
              <span className="block font-semibold text-[var(--text)]">{suggestion.label}</span>
              {(suggestion.category_name || suggestion.kakeibo) && (
                <span className="block text-[11px] text-[var(--muted)]">
                  {[suggestion.category_name, suggestion.kakeibo ? kakeiboLabel(suggestion.kakeibo) : null].filter(Boolean).join(" · ")}
                </span>
              )}
            </button>
          ))}
        </div>
      )}

      <form onSubmit={submitText} className="flex flex-col gap-2 sm:flex-row sm:items-end">
        <div className="min-w-0 flex-1 space-y-1">
          <label htmlFor={inputId} className="block text-[11px] font-medium text-[var(--muted)]">
            Jawab sendiri
          </label>
          <input
            id={inputId}
            value={text}
            maxLength={maxLength}
            onChange={(event) => setText(event.target.value)}
            placeholder="misalnya: sewa lapangan futsal"
            aria-invalid={error ? true : undefined}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus:border-[var(--border-strong)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
          />
        </div>
        <button
          type="submit"
          disabled={answer.isPending}
          className="min-h-11 shrink-0 rounded-xl bg-[#1E201E] px-4 text-xs font-bold text-white transition-transform active:scale-95 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50 dark:border dark:border-white/15"
        >
          {answer.isPending ? "Menyimpan…" : "Simpan jawaban"}
        </button>
      </form>
      {error && <p role="alert" className="text-[11px] font-medium text-rose-600 dark:text-rose-400">{error}</p>}
    </article>
  );
}
