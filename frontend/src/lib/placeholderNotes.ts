/**
 * Notes the server writes when the owner gave none (`notes.movement_default`, `notes.movement` and `notes.topup` of
 * `backend/app/messages/{id,en}.json`, and the older "Internal Movement"). They say nothing about the move, so a
 * transfer or top-up carrying one is titled by its route or product instead (apply-markas-transaksi D11). A test
 * reads the backend catalogues so this list cannot drift from them.
 */
export const PLACEHOLDER_NOTES: readonly string[] = [
  "Internal Movement",
  "Pindah saldo",
  "Transfer",
  "Top up Investasi",
  "Investment top-up",
];

export function isPlaceholderNote(note: string | null | undefined): boolean {
  const trimmed = note?.trim();
  return !trimmed || PLACEHOLDER_NOTES.includes(trimmed);
}

/**
 * Notes a notification writes on an expense or income when it knows nothing better (`notes.transaction`,
 * `notes.generic_*` and `notes.sender`). They are not the owner's words, so a row carrying one is titled by its
 * merchant when it has one (open-beranda-figures D7). A test reads the backend catalogues.
 */
export const GENERIC_ROW_NOTES: readonly string[] = [
  "Transaksi",
  "Transaction",
  "Pengeluaran",
  "Spending",
  "Pemasukan",
  "Income",
  "Transfer masuk",
  "Transfer in",
  "Transfer keluar",
  "Transfer out",
];

/** The note an expense or income is titled by, or null when it has none or only a generic one. */
export function ownRowNote(note: string | null | undefined): string | null {
  const trimmed = note?.trim();
  return trimmed && !GENERIC_ROW_NOTES.includes(trimmed) ? trimmed : null;
}
