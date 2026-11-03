"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { useRefreshTransactionViews } from "@/components/transactions/useTransactionUndo";
import { Modal } from "@/components/ui/Modal";
import { useToast } from "@/components/workspace/overlays";
import { ApiError, api } from "@/lib/api";

const PARSE_DELAY_MS = 300;
const TEXT_LIMIT = 200;
/** Every question type the web dialog can answer, so time questions are asked here too. */
const QUESTION_TYPES = ["amount", "time", "kind", "account", "category"];
const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const DAY = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });

interface Preview {
  amount: number | null;
  problems: string[];
  kind: "expense" | "income" | "movement" | null;
  description: string | null;
  occurred_at: string | null;
  time_phrase: string | null;
  time_options: string[];
  account_hint: { account_id: string; label: string } | null;
  category_hint: { category_id: string; label: string; pillar: string | null } | null;
}

interface CaptureQuestion {
  question_id: string;
  type: string;
  prompt: string;
  options: { id: string; label: string }[];
  allows_text: boolean;
}

interface Capture {
  id: string;
  status: "recorded" | "needs_detail" | "undone" | string;
  question: CaptureQuestion | null;
  summary: { amount: number | null; description: string | null; time_label: string | null };
  undo_until: string | null;
}

function deviceTime(): string {
  const now = new Date();
  const offset = -now.getTimezoneOffset();
  const sign = offset >= 0 ? "+" : "-";
  const pad = (value: number) => String(Math.floor(Math.abs(value))).padStart(2, "0");
  const local = new Date(now.getTime() + offset * 60_000).toISOString().slice(0, 19);
  return `${local}${sign}${pad(offset / 60)}:${pad(offset % 60)}`;
}

function newId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : "10000000-1000-4000-8000-100000000000".replace(/[018]/g, (c) => (Number(c) ^ (Math.random() * 16) >> (Number(c) / 4)).toString(16));
}

function timeText(preview: Preview): { text: string; missing: boolean } {
  if (preview.occurred_at) {
    const sameDay = DAY.format(new Date(preview.occurred_at)) === DAY.format(new Date());
    const clock = TIME.format(new Date(preview.occurred_at));
    if (!preview.time_phrase) return { text: `Waktu: sekarang, ${clock}`, missing: false };
    return { text: sameDay ? `Jam ${clock}` : `${preview.time_phrase}, ${clock}`, missing: false };
  }
  return { text: "Jam: ditanyakan setelah dicatat", missing: true };
}

/** Whether the capture's own undo window (10 s plus grace on the server) is still open. */
function undoStillOpen(until: string | null): boolean {
  return Boolean(until) && Date.parse(until as string) > Date.now();
}

const KIND_TEXT: Record<string, string> = { expense: "Keluar", income: "Masuk", movement: "Pindah saldo" };

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
  const [preview, setPreview] = useState<Preview | null>(null);
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
      api.post<{ preview: Preview }>("/capture/parse", { text: value, device_time: deviceTime() }, controller.signal)
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

  const refused = preview?.problems.includes("missing_amount") || preview?.problems.includes("several_amounts");
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
        api.post(`/capture/${recorded.id}/undo`)
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
      const response = await api.post<{ capture: Capture }>("/capture/text", {
        client_capture_id: clientId.current, text: value, device_time: deviceTime(), question_types: QUESTION_TYPES,
      });
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
      const response = await api.post<{ capture: Capture }>(`/capture/${capture.id}/answer`, {
        question_id: capture.question.question_id, reply_id: newId(), ...body,
      });
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
              maxLength={TEXT_LIMIT}
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
