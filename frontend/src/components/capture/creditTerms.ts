/**
 * The first due date and instalment amounts Catat shows before a credit purchase is saved (apply-markas-catat D10).
 * The rules are the server's "Credit Due Dates" (`app/services/credit_terms.py`): the bill closes on the first bill
 * day after the purchase (1 when the account has none), the first payment day on or after the close is the first due
 * date, days are clamped to short months, and no payment day means no due date. The server stays the authority: these
 * values are shown and never sent unless the owner edits the date.
 *
 * Dates are local calendar dates in Asia/Jakarta as "YYYY-MM-DD".
 */

const DEFAULT_STATEMENT_DAY = 1;
export const MIN_INSTALLMENTS = 1;
export const MAX_INSTALLMENTS = 36;

interface Day {
  year: number;
  /** 1 to 12. */
  month: number;
  day: number;
}

function parseDay(iso: string): Day {
  const [year, month, day] = iso.split("-").map(Number);
  return { year, month, day };
}

function formatDay({ year, month, day }: Day): string {
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

/** `day` of the month, clamped to the month's last day. */
function dayInMonth(year: number, month: number, day: number): Day {
  return { year, month, day: Math.min(day, daysInMonth(year, month)) };
}

function shiftMonth(year: number, month: number, months: number): { year: number; month: number } {
  const index = year * 12 + month - 1 + months;
  return { year: Math.floor(index / 12), month: (index % 12) + 1 };
}

function compare(a: Day, b: Day): number {
  return formatDay(a).localeCompare(formatDay(b));
}

/** The first bill day after the purchase; a purchase on the bill day goes into the next bill. */
export function statementClose(purchaseDate: string, statementDay: number | null | undefined): string {
  const purchase = parseDay(purchaseDate);
  const day = statementDay || DEFAULT_STATEMENT_DAY;
  const candidate = dayInMonth(purchase.year, purchase.month, day);
  if (compare(candidate, purchase) > 0) return formatDay(candidate);
  const next = shiftMonth(purchase.year, purchase.month, 1);
  return formatDay(dayInMonth(next.year, next.month, day));
}

/** The first payment day on or after the bill's close, or null without a payment day. */
export function derivedFirstDueDate(
  purchaseDate: string,
  statementDay: number | null | undefined,
  paymentDay: number | null | undefined,
): string | null {
  if (!paymentDay) return null;
  const close = parseDay(statementClose(purchaseDate, statementDay));
  const candidate = dayInMonth(close.year, close.month, paymentDay);
  if (compare(candidate, close) >= 0) return formatDay(candidate);
  const next = shiftMonth(close.year, close.month, 1);
  return formatDay(dayInMonth(next.year, next.month, paymentDay));
}

/** Whole-rupiah instalments, the remainder on the first, so they always add up to the amount. */
export function instalmentAmounts(amount: number, count: number): number[] {
  const base = Math.floor(amount / count);
  return [base + amount - base * count, ...Array.from({ length: count - 1 }, () => base)];
}

/** The calendar date of an instant in Asia/Jakarta, as the server reads a purchase's date. */
export function jakartaDate(instant: string | Date): string {
  return new Date(instant).toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
}

/** Today's calendar date in Asia/Jakarta, or a day before or after it. */
export function jakartaToday(offsetDays = 0): string {
  return jakartaDate(new Date(Date.now() + offsetDays * 86_400_000));
}
