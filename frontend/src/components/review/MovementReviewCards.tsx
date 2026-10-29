"use client";

import Link from "next/link";
import { useId, useState, type FormEvent } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { AccountSelectOptions, type AccountOptionItem } from "@/components/ui/AccountSelectOptions";
import { filterLiquidAccountChoices } from "@/lib/accountOptions";
import { reviewReasonLabel, type AmbiguousItem, type CompletionCandidate, type ReviewItem } from "@/lib/review";

const PRIMARY = "min-h-11 rounded-xl bg-[#1E201E] px-4 text-xs font-bold text-white transition-transform active:scale-95 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50 dark:border dark:border-white/15";
const SECONDARY = "min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 text-xs font-semibold text-[var(--text)] transition-colors hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50";

function when(date: string | null): string | null {
  return date
    ? new Date(date).toLocaleString("id-ID", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" })
    : null;
}

function EditLink({ item }: { item: ReviewItem }) {
  return (
    <Link
      href={`/ledger?edit=${item.id}&return=review`}
      className="inline-flex min-h-11 items-center rounded-lg px-2 text-xs font-semibold text-[var(--text)] underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
    >
      Ubah di buku kas
    </Link>
  );
}

interface AwaitingMovementCardProps {
  item: ReviewItem;
  accounts: AccountOptionItem[];
  pending: boolean;
  /** Rows already on the chosen account that may be the missing leg; set after the server refuses to duplicate. */
  candidates?: CompletionCandidate[];
  onComplete: (item: ReviewItem, accountId: string, createNew?: boolean) => Promise<boolean>;
  onLinkExisting: (item: ReviewItem, candidateId: string) => Promise<boolean>;
}

/**
 * A one-sided own transfer: the owner names the other account. When that account already holds a matching
 * row, the owner links it or explicitly creates a new leg.
 */
export function AwaitingMovementCard({ item, accounts, pending, candidates, onComplete, onLinkExisting }: AwaitingMovementCardProps) {
  const { bal } = useAppCtx();
  const baseId = useId();
  const [accountId, setAccountId] = useState("");
  const [candidateId, setCandidateId] = useState("");
  const [error, setError] = useState("");
  const outgoing = item.type === "expense";

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!accountId) {
      setError(outgoing ? "Pilih rekening tujuan." : "Pilih rekening asal.");
      return;
    }
    setError("");
    await onComplete(item, accountId);
  };

  return (
    <article aria-labelledby={`${baseId}-heading`} className="space-y-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 id={`${baseId}-heading`} className="text-sm font-bold text-[var(--text)]">
            {outgoing ? `Keluar dari ${item.account_name}, ke rekening mana?` : `Masuk ke ${item.account_name}, dari rekening mana?`}
          </h3>
          <p className="mt-0.5 text-xs text-[var(--muted)]">
            {[when(item.date), item.notes, ...item.reasons.map(reviewReasonLabel)].filter(Boolean).join(" · ")}
          </p>
        </div>
        <span className="shrink-0 text-sm font-bold tabular text-[var(--text)]">{bal(item.amount)}</span>
      </div>
      <form onSubmit={submit} className="flex flex-col gap-2 sm:flex-row sm:items-end">
        <div className="min-w-0 flex-1 space-y-1">
          <label htmlFor={`${baseId}-account`} className="block text-[11px] font-medium text-[var(--muted)]">
            {outgoing ? "Rekening tujuan" : "Rekening asal"}
          </label>
          <select
            id={`${baseId}-account`}
            value={accountId}
            onChange={(event) => setAccountId(event.target.value)}
            aria-invalid={error ? true : undefined}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
          >
            <option value="">Pilih rekening atau kantong</option>
            {/* Pockets are listed under their parent; a parent with pockets is not a valid counterpart. */}
            <AccountSelectOptions accounts={filterLiquidAccountChoices(accounts)} excludeAccountId={item.account_id} />
          </select>
        </div>
        <button type="submit" disabled={pending} className={PRIMARY}>Lengkapi pindah saldo</button>
        <EditLink item={item} />
      </form>
      {candidates && candidates.length > 0 && (
        <fieldset className="space-y-2 rounded-xl border border-[var(--border)] p-3">
          <legend className="px-1 text-xs font-bold text-[var(--text)]">Sudah ada di rekening ini</legend>
          <p className="text-[11px] text-[var(--muted)]">
            Transaksi ini mungkin sisi lain pindah saldo. Hubungkan agar tidak tercatat dua kali.
          </p>
          {candidates.map((candidate) => (
            <label key={candidate.id} className="flex min-h-11 cursor-pointer items-center gap-3 rounded-lg border border-[var(--border)] px-3 py-2">
              <input
                type="radio"
                name={`${baseId}-candidate`}
                value={candidate.id}
                checked={candidateId === candidate.id}
                onChange={() => setCandidateId(candidate.id)}
              />
              <span className="min-w-0 flex-1 text-xs text-[var(--text)]">
                {[when(candidate.date), candidate.category_name, candidate.notes].filter(Boolean).join(" · ")}
              </span>
              <span className="shrink-0 text-xs font-bold tabular text-[var(--text)]">{bal(candidate.amount)}</span>
            </label>
          ))}
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              disabled={pending || !candidateId}
              onClick={() => onLinkExisting(item, candidateId)}
              className={PRIMARY}
            >
              Hubungkan
            </button>
            <button type="button" disabled={pending} onClick={() => onComplete(item, accountId, true)} className={SECONDARY}>
              Tetap buat baru
            </button>
          </div>
        </fieldset>
      )}
      {error && <p role="alert" className="text-[11px] font-medium text-rose-600 dark:text-rose-400">{error}</p>}
    </article>
  );
}

interface AmbiguousMovementCardProps {
  item: AmbiguousItem;
  pending: boolean;
  onLink: (item: AmbiguousItem, counterpartId: string) => Promise<boolean>;
  onKeepSeparate: (item: AmbiguousItem) => Promise<boolean>;
}

/** A leg with several possible counterparts: the owner links one or keeps it apart. */
export function AmbiguousMovementCard({ item, pending, onLink, onKeepSeparate }: AmbiguousMovementCardProps) {
  const { bal } = useAppCtx();
  const baseId = useId();
  const [choice, setChoice] = useState("");
  const [error, setError] = useState("");
  const outgoing = item.type === "expense";

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!choice) {
      setError("Pilih pasangan transaksinya.");
      return;
    }
    setError("");
    await onLink(item, choice);
  };

  return (
    <article aria-labelledby={`${baseId}-heading`} className="space-y-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 id={`${baseId}-heading`} className="text-sm font-bold text-[var(--text)]">
            {outgoing ? `Keluar dari ${item.account_name}` : `Masuk ke ${item.account_name}`}
          </h3>
          <p className="mt-0.5 text-xs text-[var(--muted)]">
            {[when(item.date), item.notes, ...item.reasons.map(reviewReasonLabel)].filter(Boolean).join(" · ")}
          </p>
        </div>
        <span className="shrink-0 text-sm font-bold tabular text-[var(--text)]">{bal(item.amount)}</span>
      </div>
      <form onSubmit={submit} className="space-y-3">
        {item.candidates.length > 0 ? (
          <fieldset className="space-y-1.5">
            <legend className="mb-1 text-[11px] font-medium text-[var(--muted)]">
              {outgoing ? "Uang ini masuk ke" : "Uang ini berasal dari"}
            </legend>
            {item.candidates.map((candidate) => (
              <label
                key={candidate.id}
                className="flex min-h-11 cursor-pointer items-center gap-3 rounded-xl border border-[var(--border)] px-3 py-2 text-xs has-[:checked]:border-[var(--border-strong)] has-[:checked]:bg-[var(--surface-raised)]"
              >
                <input
                  type="radio"
                  name={`${baseId}-candidate`}
                  value={candidate.id}
                  checked={choice === candidate.id}
                  onChange={() => setChoice(candidate.id)}
                  className="h-4 w-4 accent-[#1E201E] dark:accent-white"
                />
                <span className="min-w-0 flex-1">
                  <span className="block font-semibold text-[var(--text)]">{candidate.account_name ?? "Rekening"}</span>
                  <span className="block text-[11px] text-[var(--muted)]">{when(candidate.date)}</span>
                </span>
                <span className="font-semibold tabular text-[var(--text)]">{bal(candidate.amount)}</span>
              </label>
            ))}
          </fieldset>
        ) : (
          <p className="text-xs text-[var(--muted)]">Tidak ada pasangan dengan nominal yang sama.</p>
        )}
        <div className="flex flex-wrap items-center gap-2">
          {item.candidates.length > 0 && (
            <button type="submit" disabled={pending} className={PRIMARY}>Gabungkan sebagai pindah saldo</button>
          )}
          <button type="button" disabled={pending} onClick={() => onKeepSeparate(item)} className={SECONDARY}>
            Tetap terpisah
          </button>
          <EditLink item={item} />
        </div>
      </form>
      {error && <p role="alert" className="text-[11px] font-medium text-rose-600 dark:text-rose-400">{error}</p>}
    </article>
  );
}
