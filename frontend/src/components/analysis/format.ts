/** Dates and labels for Analisis. Dates are local ISO days (YYYY-MM-DD) from the server, read as UTC midnight. */

const LONG = new Intl.DateTimeFormat("id-ID", { weekday: "long", day: "numeric", month: "long", timeZone: "UTC" });
const DAY_MONTH = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "long", timeZone: "UTC" });
const SHORT = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "UTC" });
const WEEKDAY_SHORT = new Intl.DateTimeFormat("id-ID", { weekday: "short", timeZone: "UTC" });

export function isoDay(value: string): Date {
  return new Date(`${value.slice(0, 10)}T00:00:00Z`);
}

export function addDays(value: string, days: number): string {
  const date = isoDay(value);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

export function dayDiff(from: string, to: string): number {
  return Math.round((isoDay(to).getTime() - isoDay(from).getTime()) / 86_400_000);
}

/** "Senin, 2 November" */
export function longDay(value: string): string {
  return LONG.format(isoDay(value));
}

/** "2 November" */
export function dayMonth(value: string): string {
  return DAY_MONTH.format(isoDay(value));
}

/** "2 Nov" */
export function shortDay(value: string): string {
  return SHORT.format(isoDay(value));
}

export function weekdayShort(value: string): string {
  return WEEKDAY_SHORT.format(isoDay(value));
}

/** "1–2 Nov", or "28 Okt – 2 Nov" across months. */
export function rangeLabel(start: string, end: string): string {
  const from = isoDay(start);
  const to = isoDay(end);
  if (start === end) return shortDay(start);
  if (from.getUTCMonth() === to.getUTCMonth() && from.getUTCFullYear() === to.getUTCFullYear()) {
    return `${from.getUTCDate()}–${shortDay(end)}`;
  }
  return `${shortDay(start)} – ${shortDay(end)}`;
}

/** Monday-first weekday index (0 = Monday … 6 = Sunday), as the calendar grid starts on Monday (OD46). */
export function mondayIndex(value: string): number {
  return (isoDay(value).getUTCDay() + 6) % 7;
}

/** Today's local date in the application timezone. */
export function todayIso(now: Date = new Date()): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta", year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}

const EXPECTED_REASONS: Record<string, string> = {
  too_few_receipts: "baru sedikit gaji yang tercatat",
  irregular_receipts: "tanggal gaji belum teratur",
  no_salary_source: "sumber gaji belum dipilih",
  no_cycle: "periode ini mengikuti bulan kalender",
};

export function expectedReason(reason: string | null): string {
  return (reason && EXPECTED_REASONS[reason]) || "datanya belum cukup";
}

/** A short amount for a narrow day button: "109rb", "1,2 jt". Hidden balances stay hidden. */
export function compactMoney(amount: number, money: (value: number) => string): string {
  if (!/\d/.test(money(1))) return money(amount);
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(1).replace(".", ",").replace(/,0$/, "")} jt`;
  if (amount >= 1_000) return `${Math.round(amount / 1_000)}rb`;
  return String(amount);
}
