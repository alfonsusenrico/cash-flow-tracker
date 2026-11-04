"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { useRefreshTransactionViews } from "@/components/transactions/useTransactionUndo";
import { Modal } from "@/components/ui/Modal";
import { useToast } from "@/components/workspace/overlays";
import { ApiError } from "@/lib/api";
import {
  CAPTURE_TEXT_LIMIT, amountRefused, answerCapture, newId, parseCapture, submitCapture, timeText, undoCapture, undoStillOpen,
  type Capture, type CapturePreview,
} from "@/lib/capture";

const PARSE_DELAY_MS = 300;

const KIND_TEXT: Record<string, string> = { expense: "Pengeluaran", income: "Masuk", movement: "Pindah saldo" };

/**
 * Text-first Catat (redesign-transactions-workspace D11). While the owner types, a parse-only preview
 * shows what the server reads; it writes nothing and asks no model. "Catat" records through the phone's
 * capture path, asks its follow-up questions inline and offers "Urungkan" for the capture's window.
 */
export default function CatatDialog({ open, onClose, onManual }: { open: boolean; onClose: () => void; onManual: (amount: number | null) => void }) {
  const { bal } = useAppCtx();
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();
  const fieldId = useId();
  const noteId = useId();
  const [text, setText] = useState("");
  const [preview, setPreview] = useState<CapturePreview | null>(null);
  const [previewFailed, setPreviewFailed] = useState(false);
  const [capture, setCapture] = useState<Capture | null>(null);
  const [answerText, setAnswerText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const clientId = useRef(newId());
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    setText("");
    setPreview(null);
    setCapture(null);
    setError("");
    clientId.current = newId();
  }, [open]);

  useEffect(() => {
    const value = text.trim();
    if (!value || capture) {
      setPreview(null);
      setPreviewFailed(false);
      return;
    }
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      parseCapture(value, controller.signal)
        .then((response) => {
          setPreview(response.preview);
          setPreviewFailed(false);
        })
        .catch((failure) => {
          if (controller.signal.aborted) return;
          // 429 or a failure keeps the last preview; the server still decides on "Catat".
          if (!(failure instanceof ApiError && failure.status === 429)) setPreview(null);
          setPreviewFailed(true);
        });
    }, PARSE_DELAY_MS);
    return () => {
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [text, capture]);

  const refused = amountRefused(preview);
  const refusal = preview?.problems.includes("several_amounts")
    ? "Ada lebih dari satu jumlah. Tulis satu jumlah saja."
    : "Jumlah belum ada. Tulis jumlahnya, misalnya 5rb.";

  const finish = (recorded: Capture) => {
    refresh();
    onClose();
    const { amount, description, time_label: when } = recorded.summary;
    const label = [description, amount !== null ? bal(amount) : null].filter(Boolean).join(" ");
    const undoOpen = undoStillOpen(recorded.undo_until);
    showToast(`Dicatat: ${label}${when ? `, ${when}` : ""}.`, undoOpen ? {
      onUndo: () => {
        undoCapture(recorded.id)
          .then(() => {
            refresh();
            showToast("Catatan dibatalkan.");
          })
          .catch((failure) => showToast(failure instanceof ApiError ? failure.message : "Gagal membatalkan."));
      },
    } : undefined);
  };

  const handle = (next: Capture) => {
    if (next.status === "recorded") finish(next);
    else {
      setCapture(next);
      setAnswerText("");
    }
  };

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    const value = text.trim();
    if (!value || refused || submitting) return;
    setSubmitting(true);
    setError("");
    try {
      const response = await submitCapture(value, clientId.current);
      handle(response.capture);
    } catch (failure) {
      // The same client id is resent on retry, so a capture that did reach the server is not recorded twice.
      setError(failure instanceof Error && failure.message ? failure.message : "Belum dapat dicatat. Coba lagi.");
    } finally {
      setSubmitting(false);
    }
  };

  const answer = async (body: Record<string, string>) => {
    if (!capture?.question || submitting) return;
    setSubmitting(true);
    setError("");
    try {
      const response = await answerCapture(capture, body);
      handle(response.capture);
    } catch (failure) {
      setError(failure instanceof Error && failure.message ? failure.message : "Jawaban belum tersimpan. Coba lagi.");
    } finally {
      setSubmitting(false);
    }
  };

  const facts: { key: string; text: string; missing: boolean }[] = [];
  if (preview && !refused) {
    if (preview.amount !== null) facts.push({ key: "amount", text: bal(preview.amount), missing: false });
    if (preview.kind && preview.kind !== "expense") facts.push({ key: "kind", text: KIND_TEXT[preview.kind], missing: false });
    facts.push({ key: "time", ...timeText(preview) });
    facts.push(preview.account_hint
      ? { key: "account", text: preview.account_hint.label, missing: false }
      : { key: "account", text: "Rekening: ditentukan saat dicatat", missing: true });
    facts.push(preview.category_hint
      ? { key: "category", text: preview.category_hint.label, missing: false }
      : { key: "category", text: "Kategori: dikenali saat dicatat", missing: true });
  }

  return (
    <Modal open={open} onClose={onClose} title="Catat transaksi">
      <div className="ws-catat space-y-3">
        {!capture && (
          <form onSubmit={submit} className="space-y-3">
            <label htmlFor={fieldId} className="block text-sm text-[var(--muted)]">Tulis transaksi</label>
            <input
              ref={inputRef}
              id={fieldId}
              type="text"
              data-autofocus
              autoComplete="off"
              maxLength={CAPTURE_TEXT_LIMIT}
              value={text}
              onChange={(event) => setText(event.target.value)}
              placeholder="Contoh: parkir 5rb jam 2"
              aria-describedby={noteId}
            />
            <div id={noteId} aria-live="polite">
              {!text.trim() && <p className="ws-caption">Satu kalimat dengan jumlahnya. Waktu dan rekening boleh ditulis.</p>}
              {refused && <p className="ws-catat-refuse">{refusal}</p>}
              {previewFailed && !refused && <p className="ws-caption">Pratinjau belum tersedia. Teks tetap dapat dicatat.</p>}
              {facts.length > 0 && (
                <ul className="ws-catat-preview" aria-label="Yang terbaca">
                  {facts.map((fact) => (
                    <li key={fact.key} className={`ws-catat-fact${fact.missing ? " is-missing" : ""}`}>{fact.text}</li>
                  ))}
                </ul>
              )}
            </div>
            {error && <p role="alert" className="ws-notice ws-tx-error">{error}</p>}
            <div className="ws-tx-actions">
              <button type="submit" className="ws-btn ws-btn-ink" disabled={!text.trim() || Boolean(refused) || submitting}>
                {submitting ? "Mencatat…" : "Catat"}
              </button>
              <button type="button" className="ws-btn" onClick={() => onManual(preview?.amount ?? null)}>Isi manual</button>
            </div>
          </form>
        )}
        {capture?.question && (
          <div className="ws-catat-q" role="group" aria-labelledby={`${fieldId}-q`}>
            <p id={`${fieldId}-q`}>{capture.question.prompt}</p>
            <div className="ws-tx-actions">
              {capture.question.options.map((option) => (
                <button key={option.id} type="button" className="ws-btn" disabled={submitting} onClick={() => void answer({ option_id: option.id })}>
                  {option.label}
                </button>
              ))}
            </div>
            {capture.question.allows_text && (
              <form className="mt-3 flex gap-2" onSubmit={(event) => { event.preventDefault(); if (answerText.trim()) void answer({ text: answerText.trim() }); }}>
                <label className="sr-only" htmlFor={`${fieldId}-a`}>Jawaban</label>
                <input id={`${fieldId}-a`} type="text" maxLength={50} value={answerText} onChange={(event) => setAnswerText(event.target.value)} />
                <button type="submit" className="ws-btn" disabled={!answerText.trim() || submitting}>Kirim</button>
              </form>
            )}
            {error && <p role="alert" className="ws-notice ws-tx-error mt-3">{error}</p>}
          </div>
        )}
      </div>
    </Modal>
  );
}
