"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useRefreshTransactionViews } from "@/components/transactions/useTransactionUndo";
import { KindAmount, bucketName, jakartaDay } from "@/components/transactions/rows";
import { useMoney } from "@/components/workspace/money";
import { useToast } from "@/components/workspace/overlays";
import { api, ApiError } from "@/lib/api";
import { isCreditAccountChoice } from "@/lib/accountOptions";
import {
  CAPTURE_TEXT_LIMIT, amountRefused, answerCapture, newId, parseCapture, submitCapture, undoCapture, undoStillOpen,
  type Capture, type CapturePreview,
} from "@/lib/capture";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { jakartaToday } from "./creditTerms";
import { CAPTURE_EXAMPLE_KEYS } from "./examples";
import type { FooterState } from "./types";

const PARSE_DELAY_MS = 300;
const KIND_TERM = { expense: "terms.spending", income: "terms.income", movement: "terms.transfer" } as const;

/**
 * "Tulis transaksi" (apply-markas-catat D3 to D5): one sentence, previewed by the server's parser while it is typed
 * (`POST /capture/parse`, 300 ms after typing stops, nothing written), recorded through `POST /capture/text` with the
 * same `client_capture_id` on a retry, follow-up questions answered inline, and "Urungkan" for the undo window.
 */
export function SentenceView({
  active,
  formId,
  onFooter,
  onReading,
  onRecorded,
}: {
  active: boolean;
  formId: string;
  onFooter: (state: FooterState) => void;
  /** The amount read so far, which "Isi manual" carries into the manual form. */
  onReading: (amount: number | null) => void;
  onRecorded: () => void;
}) {
  const t = useTranslations("capture");
  const money = useMoney();
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();
  const fieldId = useId();
  const refusalId = useId();
  const questionId = useId();
  const [text, setText] = useState("");
  const [preview, setPreview] = useState<CapturePreview | null>(null);
  const [previewFailed, setPreviewFailed] = useState(false);
  const [capture, setCapture] = useState<Capture | null>(null);
  const [answerText, setAnswerText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const clientId = useRef(newId());
  const inputRef = useRef<HTMLInputElement>(null);
  const questionRef = useRef<HTMLDivElement>(null);

  // Only a named account needs the list, to say that a credit purchase is paid later (D11).
  const accounts = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: Boolean(preview?.account_hint),
  }).data?.accounts;

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
  const canRecord = Boolean(text.trim()) && !refused && !submitting;
  const readAmount = preview && !refused ? preview.amount : null;

  useEffect(() => {
    onReading(readAmount);
  }, [onReading, readAmount]);

  useEffect(() => {
    if (!active) return;
    onFooter({ label: submitting ? t("sentence.recording") : t("sentence.record"), disabled: !canRecord, hidden: Boolean(capture) });
  }, [active, canRecord, capture, onFooter, submitting, t]);

  const finish = (recorded: Capture) => {
    refresh();
    onRecorded();
    const { amount, description, time_label: when } = recorded.summary;
    const label = [description, amount !== null ? money(amount) : null].filter(Boolean).join(" ");
    const undoOpen = undoStillOpen(recorded.undo_until);
    showToast(t("toast.recorded", { label, when: when || "none" }), undoOpen ? {
      onUndo: () => {
        undoCapture(recorded.id)
          .then(() => {
            refresh();
            showToast(t("toast.undone"));
          })
          .catch((failure) => showToast(failure instanceof ApiError ? failure.message : t("toast.undoFailed")));
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
      setError(failure instanceof Error && failure.message ? failure.message : t("sentence.failed"));
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
      setError(failure instanceof Error && failure.message ? failure.message : t("sentence.answerFailed"));
    } finally {
      setSubmitting(false);
    }
  };

  // A follow-up question replaces the field, so focus moves to its first answer, or else to its prompt (D3, D16).
  const questionKey = capture?.question?.question_id;
  useEffect(() => {
    if (!questionKey || !active) return;
    const group = questionRef.current;
    (group?.querySelector<HTMLElement>("button, input") ?? group)?.focus();
  }, [active, questionKey]);

  const chooseExample = (key: (typeof CAPTURE_EXAMPLE_KEYS)[number]) => {
    setText(t(`examples.${key}`));
    inputRef.current?.focus();
  };

  if (capture?.question) {
    return (
      <div ref={questionRef} hidden={!active} className="capture-question" role="group" aria-labelledby={questionId} tabIndex={-1}>
        <p id={questionId} className="t-body">{capture.question.prompt}</p>
        {capture.question.options.length > 0 && (
          <div className="quick-chips">
            {capture.question.options.map((option) => (
              <button key={option.id} type="button" className="ws-btn" disabled={submitting} onClick={() => void answer({ option_id: option.id })}>
                {option.label}
              </button>
            ))}
          </div>
        )}
        {capture.question.allows_text && (
          <form
            className="capture-answer"
            onSubmit={(event) => {
              event.preventDefault();
              if (answerText.trim()) void answer({ text: answerText.trim() });
            }}
          >
            <label className="sr-only" htmlFor={`${questionId}-answer`}>{t("sentence.answer")}</label>
            <input id={`${questionId}-answer`} type="text" maxLength={50} autoComplete="off" value={answerText}
              onChange={(event) => setAnswerText(event.target.value)} className="field-control" />
            <button type="submit" className="ws-btn" disabled={!answerText.trim() || submitting}>{t("sentence.send")}</button>
          </form>
        )}
        {error && <p role="alert" className="form-alert">{error}</p>}
      </div>
    );
  }

  return (
    <div hidden={!active} className="form-stack">
      <form id={formId} onSubmit={submit} noValidate>
        <div className="field">
          <label htmlFor={fieldId} className="sr-only">{t("sentence.label")}</label>
          <input
            ref={inputRef}
            id={fieldId}
            type="text"
            data-capture-field
            autoComplete="off"
            maxLength={CAPTURE_TEXT_LIMIT}
            value={text}
            onChange={(event) => setText(event.target.value)}
            placeholder={t("sentence.placeholder")}
            aria-invalid={refused ? true : undefined}
            aria-describedby={refused ? refusalId : undefined}
            className="field-control capture-sentence t-lead"
          />
          <div aria-live="polite">
            {refused && (
              <p id={refusalId} className="err">
                {preview?.problems.includes("several_amounts") ? t("sentence.severalAmounts") : t("sentence.noAmount")}
              </p>
            )}
          </div>
        </div>
      </form>
      <div>
        <p className="t-meta capture-examples-label" id={`${fieldId}-examples`}>{t("sentence.examples")}</p>
        <div className="quick-chips" role="group" aria-labelledby={`${fieldId}-examples`}>
          {CAPTURE_EXAMPLE_KEYS.map((key) => (
            <button key={key} type="button" className="ws-btn" onClick={() => chooseExample(key)}>
              {t(`examples.${key}`)}
            </button>
          ))}
        </div>
      </div>
      <div aria-live="polite">
        {previewFailed && !refused && <p className="t-meta">{t("sentence.previewFailed")}</p>}
        {preview && !refused && preview.amount !== null ? (
          <ReadingBox preview={preview} accounts={accounts ?? []} />
        ) : (
          !previewFailed && !refused && <p className="t-meta">{t("preview.empty")}</p>
        )}
      </div>
      {error && <p role="alert" className="form-alert">{error}</p>}
    </div>
  );
}

/** The "Terbaca" box (D5): the amount by meaning with its sign, then type, account, category, pillar and date. */
function ReadingBox({ preview, accounts }: { preview: CapturePreview; accounts: any[] }) {
  const t = useTranslations("capture.preview");
  const words = useTranslations();
  const money = useMoney();
  const format = useFormat();
  const kind = preview.kind ?? "expense";
  const hint = preview.account_hint;
  const onCredit = Boolean(hint && accounts.some((account) => String(account.id) === hint.account_id && isCreditAccountChoice(account)));
  const pillar = preview.category_hint?.pillar;

  let when: React.ReactNode = <span className="c-warning">{t("timeLater")}</span>;
  if (preview.occurred_at) {
    const day = jakartaDay(preview.occurred_at);
    const time = format.time(preview.occurred_at);
    if (day === jakartaToday()) when = t("today", { time });
    else if (day === jakartaToday(-1)) when = t("yesterday", { time });
    else when = t("otherDay", { date: format.shortDate(day), time });
  }

  return (
    <div className="box capture-read">
      <p className="t-label">{t("label")}</p>
      <p className="capture-read-fig">
        <KindAmount kind={kind} amount={preview.amount ?? 0} money={money} className="t-fig" />
      </p>
      <dl className="dl">
        <dt>{t("kind")}</dt>
        <dd>{words(KIND_TERM[kind])}</dd>
        <dt>{t("account")}</dt>
        <dd>
          {hint
            ? onCredit ? t("payLater", { account: hint.label }) : hint.label
            : <span className="c-warning">{t("accountLater")}</span>}
        </dd>
        <dt>{t("category")}</dt>
        <dd>{preview.category_hint ? preview.category_hint.label : <span className="c-warning">{t("categoryLater")}</span>}</dd>
        {pillar && (
          <>
            <dt>{t("pillar")}</dt>
            <dd>{bucketName(words, pillar)}</dd>
          </>
        )}
        <dt>{t("date")}</dt>
        <dd>{when}</dd>
      </dl>
    </div>
  );
}
