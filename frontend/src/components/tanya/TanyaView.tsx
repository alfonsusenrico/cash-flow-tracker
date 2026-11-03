"use client";

import { greetingName } from "@/lib/utils";
import { useCallback, useEffect, useId, useMemo, useRef, useState, type FormEvent, type KeyboardEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import type { CycleSummary } from "@/components/beranda/types";
import { useAppCtx } from "@/components/layout/AppLayout";
import { TransactionDrawer } from "@/components/transactions/TransactionDrawer";
import { useRefreshTransactionViews, useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { Icon } from "@/components/ui/Icon";
import { PRODUCT_NAME } from "@/components/workspace/navigation";
import { usePageShortcutGroup, type PageShortcutGroup } from "@/components/workspace/shortcuts";
import { api, ApiError } from "@/lib/api";
import { answerCapture, newId, undoCapture, type Capture } from "@/lib/capture";
import { queryKeys } from "@/lib/queryKeys";
import { TANYA_TEXT_LIMIT } from "@/lib/tanya/intents";
import { greeting, leadLine, recordedText } from "@/lib/tanya/replies";
import { buildCard, runTurn } from "@/lib/tanya/resolve";
import { appendMessage, bindThreadUser, clearThread, updateMessage, useThreadState } from "@/lib/tanya/thread";
import type { AssistantMessage, ChipKey, ThreadMessage, UserMessage } from "@/lib/tanya/types";
import { AnswerCard, CaptureCard, clock } from "./cards";
import styles from "./Tanya.module.css";
import { useTanyaDeps } from "./useTanyaDeps";

const PAGE_KEYS: PageShortcutGroup = {
  name: "Di Tanya",
  entries: [
    { keys: ["Enter"], text: "Kirim pesan" },
    { keys: ["Shift", "Enter"], text: "Baris baru di pesan" },
  ],
};

/** How far above the newest message counts as "scrolled up", in CSS pixels (D5). */
const SCROLLED_UP_PX = 120;
const COMPOSER_MAX_ROWS = 6;

const CHIP_QUERIES: Record<Exclude<ChipKey, "capture">, string> = {
  cycle: "Ringkasan siklus ini",
  waiting: "Menunggu jawaban",
  report: "Laporan terbaru",
  bills_week: "Tagihan minggu ini",
};

const SUGGESTION_DETAILS: Record<Exclude<ChipKey, "capture">, string> = {
  cycle: "Aman dipakai, masuk, dan keluar",
  waiting: "Transaksi yang perlu ditinjau",
  report: "Laporan bulanan Kakeibo",
  bills_week: "Tagihan dan setoran 7 hari ke depan",
};

function chipLabel(chip: ChipKey, attention: number | null): string {
  if (chip === "capture") return "Catat sebagai transaksi";
  if (chip === "waiting") return attention === null ? "Menunggu jawaban" : `Menunggu jawaban (${attention})`;
  return CHIP_QUERIES[chip];
}

/** Hides amounts inside server text (a question naming "Rp45.000") when balances are hidden. */
function maskAmounts(text: string, hidden: boolean): string {
  return hidden ? text.replace(/Rp\s?[\d.,]+/g, "Rp ••••••") : text;
}

function isUser(message: ThreadMessage): message is UserMessage {
  return message.role === "user";
}

/**
 * Tanya (add-tanya-conversation): a deterministic conversation over the owner's own figures. Questions are
 * answered with snapshot cards from existing endpoints, a typed transaction is recorded through the
 * capture path, and anything else gets the suggestion chips, never a guess. No model is asked for an
 * answer, and the thread lives in memory only.
 */
export function TanyaView() {
  const { user, bal, hideBalances, reviewAttention } = useAppCtx();
  const userId = user?.id ? String(user.id) : null;
  const deps = useTanyaDeps();
  const refreshViews = useRefreshTransactionViews();
  const { record } = useTransactionUndo();
  const thread = useThreadState();
  const messages = useMemo(() => (userId && thread.userId === userId ? thread.messages : []), [thread, userId]);
  const [text, setText] = useState("");
  const [pending, setPending] = useState(false);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [answerTexts, setAnswerTexts] = useState<Record<string, string>>({});
  const [status, setStatus] = useState("");
  const [editId, setEditId] = useState<string | null>(null);
  const [scrolledUp, setScrolledUp] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const scrolledUpRef = useRef(false);
  const fieldId = useId();
  const hintId = useId();
  usePageShortcutGroup(PAGE_KEYS);

  useEffect(() => {
    if (userId) bindThreadUser(userId);
  }, [userId]);

  const summaryQuery = useQuery<CycleSummary>({
    queryKey: queryKeys.dashboard.cycleSummary,
    queryFn: () => api.get("/dashboard/cycle-summary"),
    enabled: Boolean(userId),
  });

  // An open undo window closes on its own; re-render when the nearest one ends so "Urungkan" goes away.
  useEffect(() => {
    const deadlines = messages
      .flatMap((message) => (!isUser(message) && message.capture?.undo_until ? [Date.parse(message.capture.undo_until)] : []))
      .filter((deadline) => deadline > now);
    if (!deadlines.length) return;
    const timer = window.setTimeout(() => setNow(Date.now()), Math.min(...deadlines) - now + 50);
    return () => window.clearTimeout(timer);
  }, [messages, now]);

  useEffect(() => {
    const end = endRef.current;
    if (!end || typeof IntersectionObserver === "undefined") return;
    const observer = new IntersectionObserver(([entry]) => {
      scrolledUpRef.current = !entry.isIntersecting;
      if (entry.isIntersecting) setScrolledUp(false);
    }, { rootMargin: `0px 0px ${SCROLLED_UP_PX}px 0px` });
    observer.observe(end);
    return () => observer.disconnect();
  }, []);

  const lastId = messages.at(-1)?.id;
  useEffect(() => {
    if (!lastId) return;
    if (scrolledUpRef.current) {
      setScrolledUp(true);
      return;
    }
    const reduce = typeof window.matchMedia === "function" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    endRef.current?.scrollIntoView?.({ block: "end", behavior: reduce ? "auto" : "smooth" });
  }, [lastId]);

  const send = useCallback(async (value: string, options: { forceCapture?: boolean; resend?: UserMessage } = {}) => {
    const trimmed = value.trim().slice(0, TANYA_TEXT_LIMIT);
    if (!trimmed || pending) return;
    const message: UserMessage = options.resend ?? {
      id: newId(), role: "user", at: new Date().toISOString(), text: trimmed, clientCaptureId: newId(), failed: false,
    };
    if (options.resend) updateMessage(message.id, (current) => ({ ...current, failed: false }) as UserMessage);
    else appendMessage(message);
    setPending(true);
    setStatus("");
    try {
      const outcome = await runTurn(message.text, message.clientCaptureId, deps, { forceCapture: options.forceCapture });
      if (outcome.sendFailed) updateMessage(message.id, (current) => ({ ...current, failed: true }) as UserMessage);
      if (outcome.restoreText) setText((current) => current || message.text);
      if (outcome.reply) appendMessage({ id: newId(), role: "assistant", at: new Date().toISOString(), ...outcome.reply });
      if (outcome.route === "capture") refreshViews();
    } finally {
      setPending(false);
    }
  }, [deps, pending, refreshViews]);

  const submit = (event?: FormEvent) => {
    event?.preventDefault();
    const value = text;
    if (!value.trim() || pending) return;
    setText("");
    void send(value);
    composerRef.current?.focus();
  };

  const onComposerKey = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing) return;
    event.preventDefault();
    submit();
  };

  const onChip = (chip: ChipKey, message?: AssistantMessage) => {
    if (chip === "capture") {
      if (message?.captureText) void send(message.captureText, { forceCapture: true });
      return;
    }
    void send(CHIP_QUERIES[chip]);
  };

  const refreshCard = async (message: AssistantMessage) => {
    if (!message.card) return;
    setBusyId(message.id);
    const card = await buildCard(message.card.kind, message.card.range, deps, true);
    updateMessage(message.id, (current) => ({ ...current, card }) as AssistantMessage);
    setBusyId(null);
  };

  const setCapture = (id: string, capture: Capture, extra: Partial<AssistantMessage> = {}) =>
    updateMessage(id, (current) => ({ ...current, capture, ...extra }) as AssistantMessage);

  const undo = async (message: AssistantMessage) => {
    if (!message.capture) return;
    setBusyId(message.id);
    try {
      const { capture } = await undoCapture(message.capture.id);
      setCapture(message.id, capture, { notice: null });
      refreshViews();
      setStatus("Catatan dibatalkan.");
    } catch (failure) {
      const notice = failure instanceof ApiError ? failure.message : "Catatan belum dapat dibatalkan. Coba lagi.";
      updateMessage(message.id, (current) => ({ ...current, notice }) as AssistantMessage);
    } finally {
      setBusyId(null);
      composerRef.current?.focus();
    }
  };

  const answer = async (message: AssistantMessage, body: Record<string, string>) => {
    if (!message.capture?.question) return;
    setBusyId(message.id);
    try {
      const { capture } = await answerCapture(message.capture, body);
      const recorded = capture.status === "recorded";
      setCapture(message.id, capture, {
        notice: null,
        text: recorded ? recordedText(capture.transaction?.review_status === "needs_review") : message.text,
      });
      setAnswerTexts((current) => ({ ...current, [message.id]: "" }));
      if (recorded) refreshViews();
    } catch (failure) {
      const notice = failure instanceof ApiError ? failure.message : "Jawaban belum tersimpan. Coba lagi.";
      updateMessage(message.id, (current) => ({ ...current, notice }) as AssistantMessage);
    } finally {
      setBusyId(null);
    }
  };

  const summary = summaryQuery.data;
  const hour = new Date(now).getHours();
  const name = greetingName(user);
  const empty = messages.length === 0;
  const last = messages.at(-1);
  const trailingChips = !empty && !pending && !(last && !isUser(last) && last.chips.length > 0);
  const rows = Math.min(COMPOSER_MAX_ROWS, Math.max(2, text.split("\n").length));

  return (
    <div className={styles.page}>
      <header className={styles.intro}>
        <div className={styles.introText}>
          {/* The top bar holds the page h1 ("Tanya"); the greeting is the page's first section heading. */}
          <h2 className={styles.greeting}>{greeting(hour, name)}</h2>
          {summary && <p className={styles.lead}>{leadLine(summary, reviewAttention, bal)}</p>}
        </div>
        {!empty && (
          <ConfirmActionButton label="Hapus percakapan" confirmation="Hapus semua pesan di percakapan ini?" onConfirm={() => { clearThread(); composerRef.current?.focus(); }} className="ws-btn ws-btn-sm">
            Hapus percakapan
          </ConfirmActionButton>
        )}
      </header>

      {empty && (
        <ul className={styles.suggestions} aria-label="Saran pertanyaan">
          {(Object.keys(CHIP_QUERIES) as Exclude<ChipKey, "capture">[]).map((chip) => (
            <li key={chip}>
              <button type="button" className={styles.suggestion} onClick={() => onChip(chip)} disabled={pending}>
                <span className={styles.suggestionTitle}>{chipLabel(chip, reviewAttention)}</span>
                <span className={styles.suggestionDetail}>{SUGGESTION_DETAILS[chip]}</span>
              </button>
            </li>
          ))}
        </ul>
      )}

      <section role="log" aria-label="Percakapan" className={styles.thread} aria-busy={pending}>
        {!empty && <p className={styles.separator} aria-hidden="true"><span>Hari ini</span></p>}
        <ol className={styles.messages}>
          {messages.map((message, index) => {
            if (isUser(message)) {
              return (
                <li key={message.id} className={styles.user}>
                  <span className="sr-only">Pesan terkirim, {clock(message.at)}:</span>
                  <p className={styles.bubble}>{message.text}</p>
                  {message.failed && (
                    <p className={styles.failed}>
                      <span>Gagal terkirim.</span>
                      <button type="button" className="ws-btn ws-btn-sm" onClick={() => void send(message.text, { resend: message })} disabled={pending}>
                        Kirim ulang
                      </button>
                    </p>
                  )}
                </li>
              );
            }
            const capture = message.capture;
            const question = capture?.status === "needs_detail" ? capture.question : null;
            const later = question && index < messages.length - 1;
            const answerText = answerTexts[message.id] ?? "";
            return (
              <li key={message.id} className={styles.assistant}>
                <span className={styles.mark} aria-hidden="true"><Icon name="message-circle" /></span>
                <div className={styles.assistantBody}>
                  <span className="sr-only">{PRODUCT_NAME}, {clock(message.at)}:</span>
                  {message.text && <p className={styles.say}>{maskAmounts(message.text, hideBalances)}</p>}
                  {question && <p className={styles.say}>{maskAmounts(question.prompt, hideBalances)}</p>}
                  {message.card && (
                    <AnswerCard card={message.card} money={bal} onRefresh={() => void refreshCard(message)} refreshing={busyId === message.id} />
                  )}
                  {capture && capture.status !== "needs_detail" && (
                    <CaptureCard capture={capture} money={bal} onUndo={() => void undo(message)} onEdit={setEditId} undoing={busyId === message.id} />
                  )}
                  {question && (
                    <div className={styles.question} role="group" aria-label={maskAmounts(question.prompt, hideBalances)}>
                      {question.options.length > 0 && (
                        <div className={styles.chips}>
                          {question.options.map((option) => (
                            <button key={option.id} type="button" className="ws-btn ws-btn-sm" disabled={busyId === message.id} onClick={() => void answer(message, { option_id: option.id })}>
                              {option.label}
                            </button>
                          ))}
                        </div>
                      )}
                      {question.allows_text && (
                        <form className={styles.answerForm} onSubmit={(event) => { event.preventDefault(); if (answerText.trim()) void answer(message, { text: answerText.trim() }); }}>
                          <label className="sr-only" htmlFor={`${message.id}-answer`}>Jawaban</label>
                          <input id={`${message.id}-answer`} type="text" maxLength={50} value={answerText}
                            onChange={(event) => setAnswerTexts((current) => ({ ...current, [message.id]: event.target.value }))} />
                          <button type="submit" className="ws-btn ws-btn-sm" disabled={!answerText.trim() || busyId === message.id}>Jawab</button>
                        </form>
                      )}
                      {later && <p className={styles.cardNote}>Belum dicatat sampai pertanyaan ini dijawab.</p>}
                    </div>
                  )}
                  {message.notice && <p className={`ws-notice ${styles.notice}`}>{message.notice}</p>}
                  {message.chips.length > 0 && (
                    <div className={styles.chips}>
                      {message.chips.map((chip) => (
                        <button key={chip} type="button" className={styles.chip} disabled={pending} onClick={() => onChip(chip, message)}>
                          {chipLabel(chip, reviewAttention)}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </li>
            );
          })}
          {pending && (
            <li className={styles.assistant} aria-hidden="true">
              <span className={styles.mark}><Icon name="message-circle" /></span>
              <span className={styles.typing}><span /><span /><span /></span>
            </li>
          )}
        </ol>
        {trailingChips && (
          <div className={styles.chips} role="group" aria-label="Saran pertanyaan">
            {(Object.keys(CHIP_QUERIES) as Exclude<ChipKey, "capture">[]).map((chip) => (
              <button key={chip} type="button" className={styles.chip} onClick={() => onChip(chip)}>{chipLabel(chip, reviewAttention)}</button>
            ))}
          </div>
        )}
        <div ref={endRef} className={styles.end} />
      </section>

      <p className="sr-only" role="status">{status}</p>

      <div className={styles.composerWrap}>
        {scrolledUp && (
          <button type="button" className={`ws-btn ws-btn-sm ${styles.newMessages}`} onClick={() => { endRef.current?.scrollIntoView?.({ block: "end" }); setScrolledUp(false); }}>
            Pesan baru
          </button>
        )}
        <form className={styles.composer} onSubmit={submit}>
          <label htmlFor={fieldId} className="sr-only">Tulis transaksi atau pertanyaan</label>
          <textarea
            ref={composerRef}
            id={fieldId}
            rows={rows}
            maxLength={TANYA_TEXT_LIMIT}
            value={text}
            onChange={(event) => setText(event.target.value)}
            onKeyDown={onComposerKey}
            placeholder="Contoh: kopi 18rb tadi pagi"
            aria-describedby={hintId}
            autoComplete="off"
          />
          <button type="submit" className={styles.send} aria-label="Kirim" disabled={!text.trim() || pending}>
            <Icon name="arrow-right" />
          </button>
        </form>
        <p id={hintId} className={styles.hint}>Contoh: &ldquo;parkir 5rb jam 2&rdquo; atau &ldquo;tagihan minggu ini&rdquo;. Enter untuk kirim.</p>
      </div>

      <TransactionDrawer transactionId={editId} onClose={() => setEditId(null)} record={record} />
    </div>
  );
}
