/**
 * The tab's undo stack for Tinjau answers (redesign-review-triage D15). Tokens are capabilities, so they
 * live only in memory: never in storage or the URL. "Urungkan" and Z undo the newest entry until it expires.
 */

export const UNDO_STACK_LIMIT = 20;

/** Fields every covered review action adds to its response; both null when nothing changed. */
export interface UndoFields {
  undo_token?: string | null;
  expires_at?: string | null;
}

export interface UndoBatch {
  token: string;
  /** Rows the batch changed, for "n dari m transaksi dibatalkan". */
  rows: number;
  /** The server's expiry of this token, in epoch milliseconds. */
  expiresAt: number;
}

export interface UndoEntry {
  id: number;
  /** Batches in the order they were saved; undo walks them newest first. */
  batches: UndoBatch[];
  expiresAt: number;
  label: string;
  /** A transaction of the answered item, to select it again after the undo. */
  transactionId: string | null;
  /** Folder name the item returns to, for "kembali ke {folder}"; empty outside Tinjau. */
  folderLabel: string;
}

export interface UndoResponse {
  ok: boolean;
  kind: string;
  already_undone: boolean;
  transaction_ids: string[];
  removed_transaction_ids: string[];
  restored: { id: string }[];
}

export type UndoFailure = "expired" | "conflict" | "not_found" | "network";

export interface UndoOutcome {
  /** Rows restored (or already restored) out of `total`. */
  restored: number;
  total: number;
  failure: UndoFailure | null;
  /** The transactions a conflict names, for the link to Transaksi. */
  conflictIds: string[];
}

export class UndoStack {
  private entries: UndoEntry[] = [];
  private counter = 0;

  constructor(private readonly now: () => number = () => Date.now()) {}

  /** Adds an entry when at least one batch carried a token; it expires with its earliest batch. */
  push(entry: Omit<UndoEntry, "id" | "expiresAt">): UndoEntry | null {
    const batches = entry.batches.filter((batch) => batch.token && Number.isFinite(batch.expiresAt));
    if (!batches.length) return null;
    const expiresAt = Math.min(...batches.map((batch) => batch.expiresAt));
    this.counter += 1;
    const stored: UndoEntry = { ...entry, batches, expiresAt, id: this.counter };
    this.entries = [stored, ...this.entries].slice(0, UNDO_STACK_LIMIT);
    return stored;
  }

  /** The newest entry that has not expired; expired entries are dropped. */
  newest(): UndoEntry | null {
    const now = this.now();
    this.entries = this.entries.filter((entry) => entry.expiresAt > now);
    return this.entries[0] ?? null;
  }

  drop(id: number): void {
    this.entries = this.entries.filter((entry) => entry.id !== id);
  }
}

/** A batch's token and row count from an action response; null without a token (older API, or no-op). */
export function batchOf(response: UndoFields | null | undefined, rows: number): UndoBatch | null {
  if (!response?.undo_token || !response.expires_at) return null;
  const expiresAt = Date.parse(response.expires_at);
  return Number.isFinite(expiresAt) ? { token: response.undo_token, rows, expiresAt } : null;
}

function failureOf(error: unknown): { failure: UndoFailure; conflictIds: string[] } {
  if (error && typeof error === "object" && "status" in error) {
    const status = (error as { status: unknown }).status;
    const detail = (error as { detail?: unknown }).detail;
    const ids = detail && typeof detail === "object" && Array.isArray((detail as { transaction_ids?: unknown }).transaction_ids)
      ? ((detail as { transaction_ids: string[] }).transaction_ids)
      : [];
    if (status === 410) return { failure: "expired", conflictIds: [] };
    if (status === 409) return { failure: "conflict", conflictIds: ids };
    if (status === 404) return { failure: "not_found", conflictIds: [] };
  }
  return { failure: "network", conflictIds: [] };
}

/**
 * Undoes an entry's batches newest first. A refusal stops the walk; the batches before it stay saved.
 * A network failure is retryable (the endpoint answers a repeat with `already_undone`).
 */
export async function undoEntry(
  entry: UndoEntry,
  post: (token: string) => Promise<UndoResponse>,
): Promise<UndoOutcome> {
  const total = entry.batches.reduce((sum, batch) => sum + batch.rows, 0);
  let restored = 0;
  for (const batch of [...entry.batches].reverse()) {
    try {
      await post(batch.token);
      restored += batch.rows;
    } catch (error) {
      return { restored, total, ...failureOf(error) };
    }
  }
  return { restored, total, failure: null, conflictIds: [] };
}

const FAILURE_TEXT: Record<Exclude<UndoFailure, "network">, string> = {
  expired: "Sudah lewat 10 menit, jadi tidak dapat dibatalkan di sini.",
  conflict: "Tidak dapat dibatalkan: transaksi sudah berubah sejak itu.",
  not_found: "Tidak dapat dibatalkan lagi di sini.",
};

/** The text for an undo outcome (D14, D15). */
export function undoMessage(outcome: UndoOutcome, entry: UndoEntry): string {
  if (outcome.failure === null) {
    return entry.folderLabel ? `Dibatalkan. ${entry.label} kembali ke ${entry.folderLabel}.` : `Dibatalkan: ${entry.label}.`;
  }
  if (outcome.failure === "network") return "Gagal membatalkan. Coba lagi.";
  const reason = FAILURE_TEXT[outcome.failure];
  if (outcome.restored > 0) return `${outcome.restored} dari ${outcome.total} transaksi dibatalkan. Sisanya: ${reason}`;
  return reason;
}

export const NOTHING_TO_UNDO = "Tidak ada yang dapat dibatalkan.";
