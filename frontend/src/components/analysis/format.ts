import { jakartaDay } from "@/components/transactions/rows";

/** Day arithmetic for Analisis. Dates are local ISO days (YYYY-MM-DD) from the server, read as UTC midnight; display goes through `lib/format.ts`. */

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

/** Monday-first weekday index (0 = Monday … 6 = Sunday), as the calendar grid starts on Monday. */
export function mondayIndex(value: string): number {
  return (isoDay(value).getUTCDay() + 6) % 7;
}

/** Today's local date in the application timezone. */
export function todayIso(now: Date = new Date()): string {
  return jakartaDay(now);
}
