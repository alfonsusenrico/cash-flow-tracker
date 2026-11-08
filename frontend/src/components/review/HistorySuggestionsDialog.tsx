"use client";

import { useEffect, useId, useLayoutEffect, useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import {
  defaultWindow,
  runNotice,
  windowError,
  type SuggestionEstimate,
  type SuggestionRun,
  type WindowProblem,
} from "@/lib/historySuggestions";
import { queryKeys } from "@/lib/queryKeys";
import { reviewErrorMessage } from "@/lib/review";

const RUN_POLL_MS = 2000;

function RunSummary({ run }: { run: SuggestionRun }) {
  const t = useTranslations("review.dialog.history");
  const format = useFormat();
  const notice = runNotice(run);
  if (run.status === "running") {
    return (
      <div className="field">
        <label htmlFor="suggestion-run-progress" className="t-strong">
          {t("progress", { processed: format.integer(run.processed_count), selected: format.integer(run.selected_count) })}
        </label>
        <progress id="suggestion-run-progress" value={run.processed_count} max={Math.max(run.selected_count, 1)} className="run-progress" />
      </div>
    );
  }
  const values = {
    from: format.yearDate(run.from_date),
    to: format.yearDate(run.to_date),
    processed: format.integer(run.processed_count),
    made: format.integer(run.memory_count + run.rule_count + run.ai_count),
    memory: format.integer(run.memory_count),
    rule: format.integer(run.rule_count),
    ai: format.integer(run.ai_count),
  };
  return (
    <div className="history-run">
      <p className="t-meta">
        {run.rejected_before_count > 0
          ? t("lastRejected", { ...values, rejected: format.integer(run.rejected_before_count) })
          : t("last", values)}
      </p>
      {notice && <p className={run.status === "failed" ? "t-meta c-expense" : "t-meta"}>{t(`notice.${notice.key}`, { code: notice.code })}</p>}
    </div>
  );
}

function EstimateLines({ estimate }: { estimate: SuggestionEstimate }) {
  const t = useTranslations("review.dialog.history");
  const format = useFormat();
  if (estimate.eligible_count === 0) return <p className="t-meta">{t("nothing")}</p>;
  return (
    <div className="history-estimate" role="group" aria-labelledby="suggestion-estimate-title">
      <p id="suggestion-estimate-title" className="t-strong">
        {t("estimateTitle", { count: estimate.selected_count })}
      </p>
      <p className="t-meta">{t("estimateMatches", { memory: format.integer(estimate.memory_count), rule: format.integer(estimate.rule_count) })}</p>
      <p className="t-meta">
        {estimate.ai_available
          ? t("estimateAi", { rows: format.integer(estimate.ai_rows), calls: format.integer(estimate.ai_calls), batch: format.integer(estimate.batch_size) })
          : !estimate.ai_unavailable_reason || estimate.ai_unavailable_reason === "ai_disabled"
            ? t("aiOff")
            : t("aiUnavailable", { reason: estimate.ai_unavailable_reason })}
      </p>
      {estimate.truncated && (
        <p className="t-meta">{t("estimateTruncated", { eligible: format.integer(estimate.eligible_count), max: format.integer(estimate.max_rows) })}</p>
      )}
    </div>
  );
}

/**
 * "Cari saran dari riwayat" (apply-markas-tinjau D12, history-suggestions): the period, the estimate before any
 * run, progress while it runs and the last run. A run goes on on the server when the dialog closes; reopening it
 * shows that run. Pending suggestions are decided as Tinjau items in "Saran".
 */
export function HistorySuggestionsDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const t = useTranslations("review.dialog.history");
  const common = useTranslations("common");
  const notices = useTranslations("review.notice");
  const qc = useQueryClient();
  const formId = useId();
  const initial = defaultWindow();
  const [from, setFrom] = useState(initial.from);
  const [to, setTo] = useState(initial.to);
  const [estimate, setEstimate] = useState<SuggestionEstimate | null>(null);
  const [problem, setProblem] = useState<WindowProblem | null>(null);
  const [pending, setPending] = useState(false);
  const [status, setStatus] = useState("");
  const [failure, setFailure] = useState("");
  const estimateButton = useRef<HTMLButtonElement>(null);
  // Recorded before the dialog moves focus inside, so closing it returns to "…" (or whatever opened it).
  const opener = useRef<HTMLElement | null>(null);
  useLayoutEffect(() => {
    if (open) opener.current = document.activeElement as HTMLElement | null;
  }, [open]);
  const close = () => {
    onClose();
    opener.current?.focus();
  };

  const runQuery = useQuery<{ run: SuggestionRun | null }>({
    queryKey: queryKeys.review.suggestionRun,
    queryFn: () => api.get("/review/suggestions/runs/latest"),
    enabled: open,
    refetchInterval: (query) => (open && query.state.data?.run?.status === "running" ? RUN_POLL_MS : false),
  });
  const run = runQuery.data?.run ?? null;
  const running = run?.status === "running";

  const lastStatus = useRef(run?.status);
  useEffect(() => {
    if (lastStatus.current === "running" && run && run.status !== "running") {
      void qc.invalidateQueries({ queryKey: queryKeys.review.suggestions });
      setStatus(t("done"));
    }
    lastStatus.current = run?.status;
  }, [run, qc, t]);

  useEffect(() => {
    if (open) return;
    setEstimate(null);
    setProblem(null);
    setStatus("");
    setFailure("");
  }, [open]);

  const fail = (error: unknown) => setFailure(reviewErrorMessage(error) ?? notices("requestFailed"));

  const requestEstimate = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setFailure("");
    setStatus("");
    const invalid = windowError(from, to);
    setProblem(invalid);
    if (invalid) return;
    setPending(true);
    try {
      const response = await api.post<{ estimate: SuggestionEstimate }>("/review/suggestions/runs", { from_date: from, to_date: to });
      setEstimate(response.estimate);
    } catch (error) {
      fail(error);
    } finally {
      setPending(false);
    }
  };

  const startRun = async () => {
    if (!estimate) return;
    setPending(true);
    setFailure("");
    try {
      await api.post("/review/suggestions/runs", { from_date: estimate.from_date, to_date: estimate.to_date, confirm: true });
      setEstimate(null);
      setStatus(t("started"));
    } catch (error) {
      fail(error);
    } finally {
      setPending(false);
      await qc.invalidateQueries({ queryKey: queryKeys.review.suggestionRun });
      await qc.invalidateQueries({ queryKey: queryKeys.review.suggestions });
    }
  };

  const cancel = () => {
    if (!estimate) {
      close();
      return;
    }
    setEstimate(null);
    estimateButton.current?.focus();
  };

  const changeWindow = (update: () => void) => {
    update();
    setEstimate(null);
  };

  const canStart = Boolean(estimate && estimate.eligible_count > 0) && !pending && !running;
  const problemId = `${formId}-problem`;
  return (
    <ShellDialog
      open={open}
      onClose={close}
      title={t("title")}
      width={520}
      closeLabel={common("close")}
      meta={t("meta")}
      footer={
        <>
          <button type="button" className="ws-btn ws-btn-quiet" onClick={cancel}>
            {common("cancel")}
          </button>
          <span className="grow" />
          <button ref={estimateButton} type="submit" form={formId} className="ws-btn" disabled={pending || running}>
            {t("estimate")}
          </button>
          <button type="button" className="ws-btn ws-btn-ink" disabled={!canStart} onClick={() => void startRun()}>
            {t("start")}
          </button>
        </>
      }
    >
      <form id={formId} noValidate onSubmit={requestEstimate} className="form-grid">
        <div className="field">
          <label htmlFor={`${formId}-from`}>{t("from")}</label>
          <input
            id={`${formId}-from`}
            type="date"
            className="border field-control"
            value={from}
            max={to || undefined}
            aria-invalid={problem ? true : undefined}
            aria-describedby={problem ? problemId : undefined}
            onChange={(event) => changeWindow(() => setFrom(event.target.value))}
          />
        </div>
        <div className="field">
          <label htmlFor={`${formId}-to`}>{t("to")}</label>
          <input
            id={`${formId}-to`}
            type="date"
            className="border field-control"
            value={to}
            min={from || undefined}
            max={initial.to}
            aria-invalid={problem ? true : undefined}
            aria-describedby={problem ? problemId : undefined}
            onChange={(event) => changeWindow(() => setTo(event.target.value))}
          />
        </div>
        {problem && (
          <p id={problemId} className="err full-row">
            {t(`window.${problem}`)}
          </p>
        )}
      </form>
      {estimate && <EstimateLines estimate={estimate} />}
      {run && <RunSummary run={run} />}
      <div aria-live="polite">{status && <p className="t-meta">{status}</p>}</div>
      {failure && (
        <p role="alert" className="t-meta c-expense">
          {failure}
        </p>
      )}
    </ShellDialog>
  );
}
