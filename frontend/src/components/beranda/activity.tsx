import { isWaiting, jakartaDay } from "@/components/transactions/rows";
import type { ActivityRow } from "./types";

/** At most this many rows of today and yesterday on Beranda (beranda-cycle-summary). */
export const ACTIVITY_LIMIT = 8;

/**
 * Beranda's "Aktivitas terbaru" (apply-markas-beranda D7, owner decision 5): rows that wait for an answer are
 * left out, because "Perlu tindakan" and Tinjau list them; then the newest eight are kept.
 */
export function recentActivity(rows: ActivityRow[]): ActivityRow[] {
  return rows.filter((row) => !isWaiting(row)).slice(0, ACTIVITY_LIMIT);
}

export interface ActivityDay {
  day: string;
  rows: ActivityRow[];
}

/** Rows grouped by their Asia/Jakarta day, in the order given (newest first). */
export function groupByDay(rows: ActivityRow[]): ActivityDay[] {
  const days: ActivityDay[] = [];
  for (const row of rows) {
    const day = jakartaDay(row.date);
    const last = days[days.length - 1];
    if (last?.day === day) last.rows.push(row);
    else days.push({ day, rows: [row] });
  }
  return days;
}
