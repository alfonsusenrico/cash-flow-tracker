import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function fmtIDR(n: number): string {
  return `Rp ${n.toLocaleString("id-ID")}`;
}

export function fmtUSD(n: number): string {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD" });
}

export function fmtMoney(n: number, currency: "IDR" | "USD" = "IDR"): string {
  return currency === "USD" ? fmtUSD(n) : fmtIDR(n);
}

export function currentMonthYM(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
}

export function clampNumber(value: number, min = 0, max = 100): number {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, value));
}

export function parseClampedNumber(value: string, min = 0, max = 100): number {
  return clampNumber(parseFloat(value) || 0, min, max);
}

/**
 * Format a Date or ISO timestamp as a value suitable for an `<input type="datetime-local">`.
 *
 * The browser's `datetime-local` input always expects the value to be in the user's local
 * timezone. `new Date().toISOString()` returns UTC, so naively slicing it produces a string
 * that visually shows UTC time as if it were local — leaving the user to mistakenly "fix" the
 * displayed time and accidentally store it as a future UTC instant on the server.
 *
 * This helper shifts by the local timezone offset before stringifying, so the returned value
 * round-trips correctly through `<input type="datetime-local">`.
 */
export function toDatetimeLocal(value?: string | Date | null): string {
  const source = value ? new Date(value) : new Date();
  if (Number.isNaN(source.getTime())) return "";
  const local = new Date(source.getTime() - source.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 16);
}

/**
 * Convert a `<input type="datetime-local">` value (interpreted by the browser as local time)
 * into an ISO 8601 UTC timestamp suitable for the API. JavaScript already parses
 * "YYYY-MM-DDTHH:mm" as local time, so we just rely on `toISOString()` for the conversion.
 *
 * Returns an empty string for empty / unparseable input so callers can decide whether to
 * fall back to the server default.
 */
export function fromDatetimeLocal(value: string): string {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString();
}
