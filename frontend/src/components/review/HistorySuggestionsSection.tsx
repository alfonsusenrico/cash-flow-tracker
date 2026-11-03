"use client";

import { useEffect, useId, useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { reviewErrorMessage } from "@/lib/review";
import {
  aiUnavailableLabel,
  dateOnlyLabel,
  defaultWindow,
  runNotice,
  windowError,
  type SuggestionEstimate,
  type SuggestionRun,
} from "@/lib/historySuggestions";

const RUN_POLL_MS = 2000;
const PRIMARY = "ws-btn ws-btn-ink";
const SECONDARY = "ws-btn";

function RunSummary({ run }: { run: SuggestionRun }) {
  const notice = runNotice(run);
  const made = run.memory_count + run.rule_count + run.ai_count;
  if (run.status === "running") {
    return (
      <div className="space-y-1.5">
        <label htmlFor="suggestion-run-progress" className="block text-sm font-medium">
          Sedang membuat saran: {run.processed_count} dari {run.selected_count} transaksi diperiksa
        </label>
        <progress id="suggestion-run-progress" value={run.processed_count} max={Math.max(run.selected_count, 1)} className="h-2 w-full" />
      </div>
    );
  }
  return (
    <div className="space-y-1">
      <p className="ws-caption tabular">
        Terakhir: {dateOnlyLabel(run.from_date)} – {dateOnlyLabel(run.to_date)} · {run.processed_count} transaksi diperiksa ·{" "}
        {made} saran baru ({run.memory_count} dari ingatan, {run.rule_count} dari aturan, {run.ai_count} dari AI)
        {run.rejected_before_count > 0 ? ` · ${run.rejected_before_count} pernah ditolak` : ""}
      </p>
      {notice && (
        <p className={run.status === "failed" ? "ws-alert" : "ws-caption"}>
          {notice}
        </p>
      )}
    </div>
  );
}

function EstimateBox({ estimate, pending, onConfirm, onCancel }: {
  estimate: SuggestionEstimate;
  pending: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  if (estimate.eligible_count === 0) {
    return (
      <div className="ws-picker flex-row flex-wrap items-center justify-between">
        <p className="ws-caption">Tidak ada transaksi otomatis yang bisa diperbaiki di periode ini.</p>
        <button type="button" onClick={onCancel} className={SECONDARY}>Tutup</button>
      </div>
    );
  }
  return (
    <div role="group" aria-labelledby="suggestion-estimate-title" className="ws-picker">
      <p id="suggestion-estimate-title" className="font-medium tabular">
        Perkiraan: {estimate.selected_count} transaksi akan diperiksa
      </p>
      <ul className="ws-caption list-disc space-y-0.5 pl-4 tabular">
        <li>{estimate.memory_count} cocok dengan riwayat jawaban, {estimate.rule_count} dengan aturan</li>
        {estimate.ai_available ? (
          <li>{estimate.ai_rows} lewat AI dalam {estimate.ai_calls} panggilan (maks. {estimate.batch_size} per panggilan)</li>
        ) : (
          <li>{aiUnavailableLabel(estimate.ai_unavailable_reason)}</li>
        )}
        {estimate.truncated && (
          <li>
            Ada {estimate.eligible_count} transaksi; hanya {estimate.max_rows} terbaru yang diperiksa sekali jalan.
          </li>
        )}
      </ul>
      <div className="flex flex-wrap gap-2">
        <button type="button" disabled={pending} onClick={onConfirm} className={PRIMARY}>Mulai buat saran</button>
        <button type="button" disabled={pending} onClick={onCancel} className={SECONDARY}>Batal</button>
      </div>
    </div>
  );
}

/**
 * "Cari saran dari riwayat": period, the estimate before any run, progress while it runs and the last run
 * (redesign-review-triage D3). Pending suggestions are decided as Tinjau items; starting a run has no undo.
 */
export function HistorySuggestionsSection({ headingId }: { headingId?: string }) {
  const qc = useQueryClient();
  const baseId = useId();
  const initial = defaultWindow();
  const [from, setFrom] = useState(initial.from);
  const [to, setTo] = useState(initial.to);
  const [estimate, setEstimate] = useState<SuggestionEstimate | null>(null);
  const [pending, setPending] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const estimateButton = useRef<HTMLButtonElement>(null);

  const runQuery = useQuery<{ run: SuggestionRun | null }>({
    queryKey: queryKeys.review.suggestionRun,
    queryFn: () => api.get("/review/suggestions/runs/latest"),
    refetchInterval: (query) => (query.state.data?.run?.status === "running" ? RUN_POLL_MS : false),
  });
  const run = runQuery.data?.run ?? null;
  const running = run?.status === "running";

  const lastStatus = useRef(run?.status);
  useEffect(() => {
    if (lastStatus.current === "running" && run && run.status !== "running") {
      void qc.invalidateQueries({ queryKey: queryKeys.review.suggestions });
      setStatus("Pembuatan saran selesai. Sarannya ada di \"Saran dari riwayat\".");
    }
    lastStatus.current = run?.status;
  }, [run, qc]);

  const fail = (failure: unknown) => setError(reviewErrorMessage(failure));

  const requestEstimate = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");
    setStatus("");
    const invalid = windowError(from, to);
    if (invalid) {
      setError(invalid);
      return;
    }
    setPending(true);
    try {
      const response = await api.post<{ estimate: SuggestionEstimate }>("/review/suggestions/runs", { from_date: from, to_date: to });
      setEstimate(response.estimate);
    } catch (failure) {
      fail(failure);
    } finally {
      setPending(false);
    }
  };

  const confirmRun = async () => {
    if (!estimate) return;
    setPending(true);
    setError("");
    try {
      await api.post("/review/suggestions/runs", { from_date: estimate.from_date, to_date: estimate.to_date, confirm: true });
      setEstimate(null);
      setStatus("Pembuatan saran dimulai. Saran muncul di \"Saran dari riwayat\" setelah selesai.");
    } catch (failure) {
      fail(failure);
    } finally {
      setPending(false);
      await qc.invalidateQueries({ queryKey: queryKeys.review.suggestionRun });
      await qc.invalidateQueries({ queryKey: queryKeys.review.suggestions });
    }
  };

  const cancelEstimate = () => {
    setEstimate(null);
    estimateButton.current?.focus();
  };

  return (
    <div className="space-y-3">
      <p className="ws-caption">
        Nama, kategori dan pilar untuk transaksi lama yang dicatat otomatis. Tidak ada yang berubah sebelum diterima.
      </p>
      <form noValidate onSubmit={requestEstimate} className="ws-form-row" aria-labelledby={headingId}>
        <label htmlFor={`${baseId}-from`} className="ws-field">
          Dari tanggal
          <input id={`${baseId}-from`} type="date" value={from} max={to || undefined} onChange={(event) => { setFrom(event.target.value); setEstimate(null); }} />
        </label>
        <label htmlFor={`${baseId}-to`} className="ws-field">
          Sampai tanggal
          <input id={`${baseId}-to`} type="date" value={to} min={from || undefined} max={initial.to} onChange={(event) => { setTo(event.target.value); setEstimate(null); }} />
        </label>
        <button ref={estimateButton} type="submit" disabled={pending || running} className={SECONDARY}>
          Hitung perkiraan
        </button>
      </form>
      {estimate && <EstimateBox estimate={estimate} pending={pending || running} onConfirm={confirmRun} onCancel={cancelEstimate} />}
      {run && <RunSummary run={run} />}
      <div aria-live="polite">
        {status && <p className="ws-notice">{status}</p>}
      </div>
      {error && <p role="alert" className="ws-alert">{error}</p>}
    </div>
  );
}
