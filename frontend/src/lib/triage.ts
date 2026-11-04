import type { SuggestionGroup, SuggestionList } from "@/lib/historySuggestions";
import type { AmbiguousItem, AwaitingItem, PurposeQuestionEntry, ReviewGroup, ReviewQueue, SeenTwiceItem } from "@/lib/review";

/**
 * Tinjau's folders and items, derived from the existing queue and suggestion responses
 * (redesign-review-triage D1, D2, D6, D7). Counts are rows, so the three needed folders add up to the
 * badge's `attention`; positions count items.
 */

export type NeededFolder = "apa" | "jawab" | "pindah";
export type OptionalFolder = "saran" | "otomatis" | "dua" | "lewat";
export type FolderKey = "semua" | NeededFolder | OptionalFolder;

export const NEEDED_FOLDERS: readonly NeededFolder[] = ["apa", "jawab", "pindah"];
export const OPTIONAL_FOLDERS: readonly OptionalFolder[] = ["otomatis", "saran", "dua", "lewat"];
export const FOLDER_PARAMS: readonly FolderKey[] = ["apa", "jawab", "pindah", "saran", "otomatis", "dua", "lewat"];

export function isOptionalFolder(folder: FolderKey): folder is OptionalFolder {
  return (OPTIONAL_FOLDERS as readonly string[]).includes(folder);
}

export const FOLDER_LABELS: Record<FolderKey, string> = {
  semua: "Semua",
  apa: "Belum dikenali",
  jawab: "Perlu jawaban",
  pindah: "Pindah saldo belum lengkap",
  saran: "Saran dari riwayat",
  otomatis: "Dicatat otomatis",
  dua: "Terlihat dua kali",
  lewat: "Dilewati otomatis",
};

/**
 * The Transaksi drawer for a row of Tinjau; closing it returns to the same item (redesign-transactions-workspace
 * D14). Only the five folder parameters travel, never "semua".
 */
export function transaksiHref(transactionId: string, folder?: FolderKey | null): string {
  const back = folder && FOLDER_PARAMS.includes(folder) ? `&folder=${folder}` : "";
  return `/ledger?edit=${transactionId}&return=review${back}`;
}

export type TriageItem =
  | { kind: "question"; id: string; folder: "apa"; transactionIds: string[]; entry: PurposeQuestionEntry }
  | { kind: "group"; id: string; folder: "jawab" | "otomatis"; transactionIds: string[]; group: ReviewGroup }
  | { kind: "awaiting"; id: string; folder: "pindah"; transactionIds: string[]; item: AwaitingItem }
  | { kind: "ambiguous"; id: string; folder: "pindah"; transactionIds: string[]; item: AmbiguousItem }
  | { kind: "suggestion"; id: string; folder: "saran"; transactionIds: string[]; group: SuggestionGroup }
  | { kind: "seen_twice"; id: string; folder: "dua" | "lewat"; transactionIds: string[]; item: SeenTwiceItem };

export interface Triage {
  folders: Record<NeededFolder | OptionalFolder, TriageItem[]>;
  /** Rows per folder: the needed three add up to `attention` when nothing is truncated. */
  counts: Record<NeededFolder | OptionalFolder, number>;
  /** Automatic rows that sit in mixed "Perlu jawaban" groups (D1). */
  autoInMixedGroups: number;
  attention: number;
  unreviewed: number;
  truncated: boolean;
}

export function buildTriage(queue: ReviewQueue, suggestions?: SuggestionList | null): Triage {
  const apa: TriageItem[] = queue.questions.map((entry) => ({
    kind: "question", id: `q:${entry.event_id}`, folder: "apa", transactionIds: [entry.transaction.id], entry,
  }));
  const jawab: TriageItem[] = [];
  const otomatis: TriageItem[] = [];
  for (const group of queue.groups) {
    const ids = group.transactions.map((row) => row.id);
    if (group.needs_review_count > 0) jawab.push({ kind: "group", id: `g:${group.key}`, folder: "jawab", transactionIds: ids, group });
    else otomatis.push({ kind: "group", id: `g:${group.key}`, folder: "otomatis", transactionIds: ids, group });
  }
  const pindah: TriageItem[] = [
    ...queue.awaiting.map((item): TriageItem => ({ kind: "awaiting", id: `a:${item.id}`, folder: "pindah", transactionIds: [item.id], item })),
    ...queue.ambiguous.map((item): TriageItem => ({ kind: "ambiguous", id: `m:${item.id}`, folder: "pindah", transactionIds: [item.id], item })),
  ];
  const saran: TriageItem[] = (suggestions?.groups ?? []).map((group) => ({
    kind: "suggestion", id: `s:${group.key}`, folder: "saran",
    transactionIds: group.suggestions.map((item) => item.transaction_id), group,
  }));
  const dua: TriageItem[] = (queue.seen_twice ?? []).map((item) => ({
    kind: "seen_twice", id: `d:${item.event_id}`, folder: "dua",
    transactionIds: item.kept ? [item.kept.transaction_id] : [], item,
  }));
  const lewat: TriageItem[] = (queue.skipped ?? []).map((item) => ({
    kind: "seen_twice", id: `l:${item.event_id}`, folder: "lewat",
    transactionIds: item.kept ? [item.kept.transaction_id] : [], item,
  }));
  const neededGroups = jawab.map((item) => (item as Extract<TriageItem, { kind: "group" }>).group);
  return {
    folders: { apa, jawab, pindah, saran, otomatis, dua, lewat },
    counts: {
      apa: apa.length,
      jawab: neededGroups.reduce((sum, group) => sum + group.needs_review_count, 0),
      pindah: pindah.length,
      saran: suggestions?.pending ?? saran.reduce((sum, item) => sum + item.transactionIds.length, 0),
      otomatis: otomatis.reduce((sum, item) => sum + (item as Extract<TriageItem, { kind: "group" }>).group.auto_count, 0),
      dua: dua.length,
      lewat: lewat.length,
    },
    autoInMixedGroups: neededGroups.reduce((sum, group) => sum + group.auto_count, 0),
    attention: queue.counts.attention,
    unreviewed: queue.counts.unreviewed,
    truncated: queue.truncated,
  };
}

/** Items of a folder; "Semua" is the three needed folders in order. */
export function folderItems(triage: Triage, folder: FolderKey): TriageItem[] {
  if (folder === "semua") return NEEDED_FOLDERS.flatMap((key) => triage.folders[key]);
  return triage.folders[folder];
}

export function neededCount(triage: Triage): number {
  return NEEDED_FOLDERS.reduce((sum, key) => sum + triage.folders[key].length, 0);
}

/**
 * The item to show after `answeredId` left the list: the following one in the list as it was before the
 * action, else the one before it (D6). Null when none of them is still there.
 */
export function nextAfter(before: readonly string[], answeredId: string, remaining: ReadonlySet<string>): string | null {
  const index = before.indexOf(answeredId);
  if (index < 0) return before.find((id) => remaining.has(id)) ?? null;
  for (let next = index + 1; next < before.length; next += 1) if (remaining.has(before[next])) return before[next];
  for (let previous = index - 1; previous >= 0; previous -= 1) if (remaining.has(before[previous])) return before[previous];
  return null;
}

const LEGACY_HASHES: Record<string, FolderKey> = {
  "review-questions": "apa",
  "review-groups": "jawab",
  "review-awaiting": "pindah",
  "review-ambiguous": "pindah",
};

export function folderFromParams(folder: string | null, hash: string | null): FolderKey | null {
  if (folder && (FOLDER_PARAMS as readonly string[]).includes(folder)) return folder as FolderKey;
  const key = (hash ?? "").replace(/^#/, "");
  return LEGACY_HASHES[key] ?? null;
}

export interface DeepLink {
  folder: FolderKey;
  itemId: string | null;
  /** The link named a transaction that no longer waits in that place. */
  resolved: boolean;
}

/**
 * `/review?item=<transaction id>&folder=…` (or a legacy hash) to a folder and item (D7). A transaction can
 * sit in several places; the named folder decides, else the first needed folder that has it.
 */
export function resolveDeepLink(triage: Triage, transactionId: string | null, folder: FolderKey | null): DeepLink {
  const holds = (item: TriageItem) => transactionId !== null && item.transactionIds.includes(transactionId);
  if (folder && folder !== "semua") {
    const found = triage.folders[folder].find(holds);
    if (found) return { folder, itemId: found.id, resolved: false };
    return { folder: triage.folders[folder].length ? folder : "semua", itemId: null, resolved: transactionId !== null };
  }
  for (const key of [...NEEDED_FOLDERS, "saran", "otomatis", "dua", "lewat"] as const) {
    const found = triage.folders[key].find(holds);
    if (found) return { folder: (NEEDED_FOLDERS as readonly string[]).includes(key) ? "semua" : key, itemId: found.id, resolved: false };
  }
  return { folder: "semua", itemId: null, resolved: transactionId !== null };
}

/** The folder an item belongs to when it is shown on its own (for messages and "kembali ke"). */
export function itemFolderLabel(item: TriageItem): string {
  return FOLDER_LABELS[item.folder];
}

/** Short title of an item for the list, the toast and announcements. */
export function itemTitle(item: TriageItem): string {
  switch (item.kind) {
    case "question":
      return item.entry.transaction.merchant || item.entry.transaction.notes || "Pembayaran tanpa nama toko";
    case "group":
    case "suggestion":
      return item.group.label;
    case "awaiting":
      return item.item.type === "expense" ? `Keluar dari ${item.item.account_name}` : `Masuk ke ${item.item.account_name}`;
    case "ambiguous":
      return item.item.type === "expense" ? `Keluar dari ${item.item.account_name}` : `Masuk ke ${item.item.account_name}`;
    case "seen_twice":
      return item.item.kind === "skipped" ? `${item.item.app}: dilewati` : `${item.item.app}: laporan kedua`;
  }
}

/** The transaction a "Transaksi" link opens for an item. */
export function itemTransactionId(item: TriageItem): string | null {
  return item.transactionIds[0] ?? null;
}

/** The URL of the current place: `?item=<transaction id>&folder=<key>` (D7, phase plan D14). */
export function triageSearch(folder: FolderKey, item: TriageItem | null): string {
  const params = new URLSearchParams();
  const transaction = item ? itemTransactionId(item) : null;
  if (transaction) params.set("item", transaction);
  if (folder !== "semua") params.set("folder", folder);
  const query = params.toString();
  return query ? `?${query}` : "";
}
