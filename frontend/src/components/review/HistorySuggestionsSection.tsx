"use client";

import { useEffect, useId, useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { reviewErrorMessage } from "@/lib/review";
import {
  aiUnavailableLabel,
  chunkSuggestionIds,
  dateOnlyLabel,
  defaultWindow,
  runNotice,
  windowError,
  type SuggestionDecisionResult,
  type SuggestionEstimate,
  type SuggestionList,
  type SuggestionRun,
} from "@/lib/historySuggestions";
import { SuggestionGroupCard, type SuggestionDecisionRequest } from "@/components/review/SuggestionGroupCard";

const RUN_POLL_MS = 2000;
const FIELD = "min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]";
const PRIMARY = "min-h-11 rounded-xl bg-[#1E201E] px-4 text-xs font-bold text-white transition-transform active:scale-95 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50 dark:border dark:border-white/15";
const SECONDARY = "min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 text-xs font-semibold text-[var(--text)] transition-colors hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50";

const DONE: Record<SuggestionDecisionRequest["decision"], string> = { accept: "diterima", reject: "ditolak" };

function RunSummary({ run }: { run: SuggestionRun }) {
  const notice = runNotice(run);
  const made = run.memory_count + run.rule_count + run.ai_count;
  if (run.status === "running") {
    return (
      <div className="space-y-1.5">
        <label htmlFor="suggestion-run-progress" className="block text-xs font-semibold text-[var(--text)]">
          Sedang membuat saran: {run.processed_count} dari {run.selected_count} transaksi diperiksa
        </label>
        <progress id="suggestion-run-progress" value={run.processed_count} max={Math.max(run.selected_count, 1)} className="h-2 w-full accent-[var(--primary)]" />
      </div>
    );
  }
  return (
    <div className="space-y-1 text-xs">
      <p className="text-[var(--text-secondary)] tabular">
        Terakhir: {dateOnlyLabel(run.from_date)} – {dateOnlyLabel(run.to_date)} · {run.processed_count} transaksi diperiksa ·{" "}
        {made} saran baru ({run.memory_count} dari ingatan, {run.rule_count} dari aturan, {run.ai_count} dari AI)
        {run.rejected_before_count > 0 ? ` · ${run.rejected_before_count} pernah Anda tolak` : ""}
      </p>
      {notice && (
        <p className={run.status === "failed" ? "font-medium text-rose-700 dark:text-rose-300" : "text-[var(--text-secondary)]"}>
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
      <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-[var(--border)] p-3 text-xs">
        <p className="text-[var(--text-secondary)]">Tidak ada transaksi otomatis yang bisa diperbaiki di periode ini.</p>
        <button type="button" onClick={onCancel} className={SECONDARY}>Tutup</button>
      </div>
    );
  }
  return (
    <div role="group" aria-labelledby="suggestion-estimate-title" className="space-y-2 rounded-xl border border-[var(--border)] p-3 text-xs">
      <p id="suggestion-estimate-title" className="font-semibold text-[var(--text)] tabular">
        Perkiraan: {estimate.selected_count} transaksi akan diperiksa
      </p>
      <ul className="list-disc space-y-0.5 pl-4 text-[var(--text-secondary)] tabular">
        <li>{estimate.memory_count} cocok dengan ingatan Anda, {estimate.rule_count} dengan aturan</li>
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

export function HistorySuggestionsSection() {
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
  const listQuery = useQuery<SuggestionList>({
    queryKey: queryKeys.review.suggestions,
    queryFn: () => api.get("/review/suggestions"),
  });
  const run = runQuery.data?.run ?? null;
  const running = run?.status === "running";

  const lastStatus = useRef(run?.status);
  useEffect(() => {
    if (lastStatus.current === "running" && run && run.status !== "running") {
      void qc.invalidateQueries({ queryKey: queryKeys.review.suggestions });
      setStatus("Pembuatan saran selesai.");
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
      setStatus("Pembuatan saran dimulai. Saran muncul di bawah setelah selesai.");
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

  const decide = async (request: SuggestionDecisionRequest): Promise<boolean> => {
    setPending(true);
    setError("");
    setStatus("");
    let done = 0;
    let skipped = 0;
    try {
      // Each request is one database transaction; a group larger than a batch is sent in parts.
      for (const ids of chunkSuggestionIds(request.ids)) {
        try {
          const result = await api.post<SuggestionDecisionResult>(`/review/suggestions/${request.decision}`, { suggestion_ids: ids });
          done += (request.decision === "accept" ? result.accepted : result.rejected) ?? 0;
          skipped += result.skipped.length;
        } catch (failure) {
          if (done === 0 && skipped === 0) throw failure;
          throw new Error(`${done} dari ${request.ids.length} saran ${DONE[request.decision]}; sisanya gagal: ${reviewErrorMessage(failure)}`);
        }
      }
      const note = skipped > 0 ? ` ${skipped} dilewati karena transaksinya sudah berubah atau sudah diputuskan.` : "";
      setStatus(`${request.label}: ${done} saran ${DONE[request.decision]}.${note}`);
      return true;
    } catch (failure) {
      fail(failure);
      return false;
    } finally {
      setPending(false);
      await Promise.all([
        qc.invalidateQueries({ queryKey: queryKeys.review.all }),
        qc.invalidateQueries({ queryKey: queryKeys.transactions.all }),
        qc.invalidateQueries({ queryKey: queryKeys.dashboard.all }),
      ]);
    }
  };

  const groups = listQuery.data?.groups ?? [];
  const pendingCount = listQuery.data?.pending ?? 0;

  return (
    <section aria-labelledby="review-history" className="space-y-3">
      <div>
        <h2 id="review-history" className="text-base font-bold tracking-tight text-[var(--text)]">
          Saran perbaikan riwayat{listQuery.data ? <span className="tabular text-[var(--muted)]"> ({pendingCount})</span> : null}
        </h2>
        <p className="text-xs text-[var(--muted)]">
          Nama, kategori dan pilar untuk transaksi lama yang dicatat otomatis. Tidak ada yang berubah sebelum Anda menerima.
        </p>
      </div>

      <div className="space-y-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-xs">
        <form noValidate onSubmit={requestEstimate} className="grid gap-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
          <div className="space-y-1">
            <label htmlFor={`${baseId}-from`} className="block text-[11px] font-medium text-[var(--muted)]">Dari tanggal</label>
            <input id={`${baseId}-from`} type="date" value={from} max={to || undefined} onChange={(event) => { setFrom(event.target.value); setEstimate(null); }} className={FIELD} />
          </div>
          <div className="space-y-1">
            <label htmlFor={`${baseId}-to`} className="block text-[11px] font-medium text-[var(--muted)]">Sampai tanggal</label>
            <input id={`${baseId}-to`} type="date" value={to} min={from || undefined} max={initial.to} onChange={(event) => { setTo(event.target.value); setEstimate(null); }} className={FIELD} />
          </div>
          <button ref={estimateButton} type="submit" disabled={pending || running} className={SECONDARY}>
            Hitung perkiraan
          </button>
        </form>
        {estimate && <EstimateBox estimate={estimate} pending={pending || running} onConfirm={confirmRun} onCancel={cancelEstimate} />}
        {run && <RunSummary run={run} />}
        <div aria-live="polite" className="text-xs">
          {status && <p className="font-medium text-[var(--text)]">{status}</p>}
        </div>
        {error && (
          <p role="alert" className="rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-xs font-medium text-rose-700 dark:text-rose-300">
            {error}
          </p>
        )}
      </div>

      {listQuery.isLoading ? (
        <p className="py-6 text-center text-xs text-[var(--muted)]">Memuat saran…</p>
      ) : listQuery.isError ? (
        <div className="space-y-2 py-6 text-center">
          <p className="text-sm font-semibold text-[var(--text)]">Saran gagal dimuat.</p>
          <button type="button" onClick={() => listQuery.refetch()} className={SECONDARY}>Coba lagi</button>
        </div>
      ) : groups.length === 0 ? (
        <p className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-4 py-6 text-center text-xs text-[var(--muted)]">
          {run ? "Tidak ada saran yang menunggu keputusan." : "Belum ada saran. Pilih periode lalu hitung perkiraan."}
        </p>
      ) : (
        <div className="space-y-3">
          {listQuery.data?.truncated && (
            <p className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-900 dark:text-amber-200">
              Hanya 1.000 saran terbaru yang ditampilkan. Putuskan yang ada untuk melihat sisanya.
            </p>
          )}
          {groups.map((group) => (
            <SuggestionGroupCard key={group.key} group={group} pending={pending} onDecide={decide} />
          ))}
        </div>
      )}
    </section>
  );
}
