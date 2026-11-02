"use client";

import Link from "next/link";
import { useId, useState, type FormEvent } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { AccountSelectOptions, type AccountOptionItem } from "@/components/ui/AccountSelectOptions";
import { filterLiquidAccountChoices } from "@/lib/accountOptions";
import {
  reviewReasonLabel,
  type AmbiguousItem,
  type AwaitingItem,
  type CompletionCandidate,
  type ReviewItem,
  type TopupProductChoice,
} from "@/lib/review";

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

export interface TopupOptions {
  /** Amount-tracked mutual-fund products a debit can become a top-up of. */
  products: TopupProductChoice[];
  /** Unit-tracked mutual funds, which must switch to amount tracking first. */
  unitFunds: TopupProductChoice[];
  onConvert: (item: ReviewItem, productId: string, suggestedId?: string) => Promise<boolean>;
  onReject: (item: ReviewItem, suggestedId?: string) => Promise<boolean>;
}

interface AwaitingMovementCardProps {
  item: AwaitingItem;
  accounts: AccountOptionItem[];
  pending: boolean;
  /** Rows already on the chosen account that may be the missing leg; set after the server refuses to duplicate. */
  candidates?: CompletionCandidate[];
  onComplete: (item: ReviewItem, accountId: string, createNew?: boolean) => Promise<boolean>;
  onLinkExisting: (item: ReviewItem, candidateId: string) => Promise<boolean>;
  /** Present for an outgoing leg from a liquid account: it may have bought a mutual fund. */
  topup?: TopupOptions;
}

const PRODUCT_FIELD = "min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]";

/**
 * "Ini top up investasi": the debit becomes the outgoing leg of a top-up of the chosen product. Unit-tracked
 * funds cannot take an amount, so they are named with the way to make them eligible.
 */
function TopupProductForm({ item, topup, pending, suggestedId, onCancel }: {
  item: AwaitingItem;
  topup: TopupOptions;
  pending: boolean;
  suggestedId?: string;
  onCancel: () => void;
}) {
  const baseId = useId();
  const products = suggestedId ? topup.products.filter((product) => product.id !== suggestedId) : topup.products;
  const [productId, setProductId] = useState(products.length === 1 ? products[0].id : "");
  const [error, setError] = useState("");

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!productId) {
      setError("Pilih produk reksadana tujuan.");
      document.getElementById(`${baseId}-product`)?.focus();
      return;
    }
    setError("");
    await topup.onConvert(item, productId, suggestedId);
  };

  return (
    <form onSubmit={submit} aria-label="Jadikan top up investasi" className="space-y-2 rounded-xl border border-[var(--border)] p-3">
      {products.length > 0 ? (
        <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
          <div className="min-w-0 flex-1 space-y-1">
            <label htmlFor={`${baseId}-product`} className="block text-[11px] font-medium text-[var(--muted)]">Produk tujuan</label>
            <select
              id={`${baseId}-product`}
              value={productId}
              onChange={(event) => setProductId(event.target.value)}
              aria-invalid={error ? true : undefined}
              aria-describedby={error ? `${baseId}-error` : undefined}
              className={PRODUCT_FIELD}
            >
              <option value="">Pilih produk…</option>
              {products.map((product) => (
                <option key={product.id} value={product.id}>{product.label}</option>
              ))}
            </select>
          </div>
          <button type="submit" disabled={pending} className={PRIMARY}>Jadikan top up</button>
          <button type="button" disabled={pending} onClick={onCancel} className={SECONDARY}>Batal</button>
        </div>
      ) : (
        <p className="text-xs text-[var(--text)]">Belum ada produk reksadana yang dicatat berdasarkan nominal.</p>
      )}
      {topup.unitFunds.length > 0 && (
        <p className="text-[11px] text-[var(--muted)]">
          {topup.unitFunds.map((fund) => fund.label).join(", ")} masih dicatat per unit. Ubah dulu ke pelacakan nominal di{" "}
          <Link href="/accounts" className="font-semibold text-[var(--text)] underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]">
            Rekening
          </Link>{" "}
          (menu produk, &quot;Ubah ke pelacakan nominal&quot;).
        </p>
      )}
      <p className="text-[11px] text-[var(--muted)]">Saldo {item.account_name} tidak berubah lagi karena debit ini sudah tercatat.</p>
      {error && <p id={`${baseId}-error`} role="alert" className="text-[11px] font-medium text-rose-600 dark:text-rose-400">{error}</p>}
    </form>
  );
}

/** A learned top-up for this debit: one-tap accept, another product, or "not a top-up". */
function TopupSuggestionPanel({ item, topup, pending }: { item: AwaitingItem; topup: TopupOptions; pending: boolean }) {
  const baseId = useId();
  const suggestion = item.topup_suggestion;
  const [picking, setPicking] = useState(false);
  const [choice, setChoice] = useState("");
  const [error, setError] = useState("");
  if (!suggestion) return null;
  const target = suggestion.target_account_id;

  const chooseOne = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!choice) {
      setError("Pilih produknya.");
      return;
    }
    setError("");
    await topup.onConvert(item, choice);
  };

  return (
    <section aria-labelledby={`${baseId}-prompt`} className="space-y-2 rounded-xl border border-violet-500/25 bg-investment-soft p-3">
      <p className="text-[11px] font-semibold uppercase tracking-wider text-investment">Top up investasi</p>
      <p id={`${baseId}-prompt`} className="text-sm font-semibold text-[var(--text)]">{suggestion.prompt}</p>
      <p className="text-[11px] text-[var(--text-secondary)]">
        {target && suggestion.target_label ? `${suggestion.target_label}. ` : null}{suggestion.explanation}
      </p>
      {target ? (
        <div className="flex flex-wrap gap-2">
          <button type="button" disabled={pending} onClick={() => topup.onConvert(item, target, target)} className={PRIMARY}>
            Ya, top up
          </button>
          <button
            type="button"
            disabled={pending}
            aria-expanded={picking}
            onClick={() => setPicking((open) => !open)}
            className={SECONDARY}
          >
            Pilih produk lain
          </button>
          <button type="button" disabled={pending} onClick={() => topup.onReject(item, target)} className={SECONDARY}>
            Bukan top up
          </button>
        </div>
      ) : (
        <form onSubmit={chooseOne} className="space-y-2">
          <fieldset className="space-y-1.5">
            <legend className="mb-1 text-[11px] font-medium text-[var(--muted)]">Top up ke</legend>
            {suggestion.choices.map((product) => (
              <label key={product.id} className="flex min-h-11 cursor-pointer items-center gap-3 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-xs text-[var(--text)]">
                <input
                  type="radio"
                  name={`${baseId}-choice`}
                  value={product.id}
                  checked={choice === product.id}
                  onChange={() => setChoice(product.id)}
                  className="h-4 w-4 accent-[#1E201E] dark:accent-white"
                />
                <span className="font-semibold">{product.label}</span>
              </label>
            ))}
          </fieldset>
          <div className="flex flex-wrap gap-2">
            <button type="submit" disabled={pending} className={PRIMARY}>Jadikan top up</button>
            <button type="button" disabled={pending} onClick={() => topup.onReject(item)} className={SECONDARY}>Bukan top up</button>
          </div>
          {error && <p role="alert" className="text-[11px] font-medium text-rose-600 dark:text-rose-400">{error}</p>}
        </form>
      )}
      {picking && target && (
        <TopupProductForm item={item} topup={topup} pending={pending} suggestedId={target} onCancel={() => setPicking(false)} />
      )}
    </section>
  );
}

/**
 * A one-sided own transfer: the owner names the other account. When that account already holds a matching
 * row, the owner links it or explicitly creates a new leg.
 */
export function AwaitingMovementCard({ item, accounts, pending, candidates, onComplete, onLinkExisting, topup }: AwaitingMovementCardProps) {
  const { bal } = useAppCtx();
  const baseId = useId();
  const [accountId, setAccountId] = useState("");
  const [topupOpen, setTopupOpen] = useState(false);
  const offerTopup = Boolean(topup) && item.type === "expense";
  const suggested = offerTopup && item.topup_suggestion !== null;
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
      {suggested && topup && <TopupSuggestionPanel item={item} topup={topup} pending={pending} />}
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
      {offerTopup && !suggested && topup && (
        <div className="space-y-2">
          <button
            type="button"
            disabled={pending}
            aria-expanded={topupOpen}
            onClick={() => setTopupOpen((open) => !open)}
            className={SECONDARY}
          >
            Ini top up investasi
          </button>
          {topupOpen && <TopupProductForm item={item} topup={topup} pending={pending} onCancel={() => setTopupOpen(false)} />}
        </div>
      )}
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
