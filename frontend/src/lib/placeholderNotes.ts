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
