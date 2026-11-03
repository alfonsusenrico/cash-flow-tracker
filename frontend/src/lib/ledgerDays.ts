/**
 * Day groups of Transaksi (redesign-transactions-workspace D3, D5). The server says which local day a
 * row belongs to (`local_date`) and what each day adds up to (`daily_totals`); the client only groups.
 * It never sums loaded rows into day totals, because a day can continue on a page not loaded yet.
 */

export interface DailyTotal {
  date: string;
  /** Matching rows of the day, also those on pages not loaded yet. */
  count: number;
  inflow: number;
  outflow: number;
  movements: number;
  waiting_count: number;
  waiting_amount: number;
}

export interface DayRow {
  id: string;
  date: string;
  local_date?: string;
}

export interface DayGroup<T extends DayRow> {
  date: string;
  rows: T[];
  /** Null when the server sent no totals (an older API): the heading then shows the date only. */
  totals: DailyTotal | null;
  /** Rows of this day the server counts but the loaded pages do not hold yet. */
  remaining: number;
}

const JAKARTA_DAY = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });

/** The row's local day: the server's, or the Jakarta day of its instant from an older API. */
export function localDay(row: DayRow): string {
  return row.local_date ?? JAKARTA_DAY.format(new Date(row.date));
}

/** Rows of several pages without repeats, in the order they arrived. */
export function uniqueRows<T extends { id: string }>(pages: T[][]): T[] {
  const seen = new Set<string>();
  const rows: T[] = [];
  for (const page of pages) {
    for (const row of page) {
      if (seen.has(row.id)) continue;
      seen.add(row.id);
      rows.push(row);
    }
  }
  return rows;
}

/**
 * Groups shown rows by local day, newest first as the server ordered them. `loaded` are the raw rows
 * behind the shown ones (paired legs count twice there), so `remaining` compares like with like.
 */
export function groupByDay<T extends DayRow>(shown: T[], loaded: DayRow[], totals: DailyTotal[] | undefined): DayGroup<T>[] {
  const byDate = new Map((totals ?? []).map((total) => [total.date, total]));
  const loadedPerDay = new Map<string, number>();
  for (const row of loaded) {
    const day = localDay(row);
    loadedPerDay.set(day, (loadedPerDay.get(day) ?? 0) + 1);
  }
  const groups: DayGroup<T>[] = [];
  for (const row of shown) {
    const day = localDay(row);
    let group = groups[groups.length - 1];
    if (!group || group.date !== day) {
      const total = byDate.get(day) ?? null;
      group = { date: day, rows: [], totals: total, remaining: total ? Math.max(0, total.count - (loadedPerDay.get(day) ?? 0)) : 0 };
      groups.push(group);
    }
    group.rows.push(row);
  }
  return groups;
}

const DAY_NAME = new Intl.DateTimeFormat("id-ID", { weekday: "long", day: "numeric", month: "long", timeZone: "UTC" });

/** "Senin, 2 November" for a local day; "hari ini" and "kemarin" are added by the caller. */
export function dayName(day: string): string {
  return DAY_NAME.format(new Date(`${day}T00:00:00Z`));
}

export function todayLocal(now: Date = new Date()): string {
  return JAKARTA_DAY.format(now);
}

export function previousDay(day: string): string {
  const date = new Date(`${day}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() - 1);
  return date.toISOString().slice(0, 10);
}
