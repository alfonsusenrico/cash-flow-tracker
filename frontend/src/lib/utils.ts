import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

let _activeCurrency: "IDR" | "USD" = "IDR";
let _activeUsdIdrRate: number = 16500;

export function setCurrencyConfig(currency?: string | null, rate?: number | null) {
  if (currency === "USD" || currency === "IDR") {
    _activeCurrency = currency;
  }
  if (typeof rate === "number" && rate > 0) {
    _activeUsdIdrRate = rate;
  }
}

export function getCurrencyConfig(): { currency: "IDR" | "USD"; rate: number } {
  return { currency: _activeCurrency, rate: _activeUsdIdrRate };
}

export function convertAmount(
  amountInIDR: number,
  targetCurrency: "IDR" | "USD" = _activeCurrency,
  rate: number = _activeUsdIdrRate
): number {
  if (targetCurrency === "USD") {
    return amountInIDR / (rate > 0 ? rate : 16500);
  }
  return amountInIDR;
}

export function fmtIDR(n: number): string {
  const isNeg = n < 0;
  const abs = Math.abs(Math.round(n));
  return `${isNeg ? "-" : ""}Rp ${abs.toLocaleString("id-ID")}`;
}

export function fmtUSD(n: number, isAlreadyUSD = false, rate: number = _activeUsdIdrRate): string {
  const val = isAlreadyUSD ? n : n / (rate > 0 ? rate : 16500);
  return val.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

export function fmtMoney(
  n: number,
  currency?: "IDR" | "USD",
  rate?: number,
  options?: { isAlreadyConverted?: boolean; maxFractionDigits?: number }
): string {
  const cur = currency || _activeCurrency;
  const fx = rate || _activeUsdIdrRate;

  if (cur === "USD") {
    const usdVal = options?.isAlreadyConverted ? n : n / (fx > 0 ? fx : 16500);
    return usdVal.toLocaleString("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: options?.maxFractionDigits !== undefined ? 0 : 2,
      maximumFractionDigits: options?.maxFractionDigits ?? 2,
    });
  }
  return fmtIDR(n);
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

/**
 * Formats a raw number or string input with Indonesian thousand dots (e.g. 20000000 -> "20.000.000").
 * Strips non-digit characters and leading zeroes (unless the number is 0).
 * Returns an empty string if no digits are present.
 */
export function formatNumberWithDots(val: string | number | null | undefined): string {
  if (val === null || val === undefined) return "";
  const str = String(val).trim();
  const digits = str.replace(/[^0-9]/g, "");
  if (!digits) return "";
  const clean = digits.replace(/^0+(?=\d)/, "");
  return clean.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

/**
 * Parses a string formatted with dots into a plain integer (e.g. "20.000.000" -> 20000000).
 */
export function parseNumberFromDots(val: string | number | null | undefined): number {
  if (val === null || val === undefined) return 0;
  const digits = String(val).replace(/[^0-9]/g, "");
  return digits ? parseInt(digits, 10) || 0 : 0;
}

/**
 * Formats a decimal price/currency input string with Indonesian thousand dots for the integer part
 * and allows decimal comma or trailing dot (e.g. "2578000" -> "2.578.000", "1922,45" -> "1.922,45", "1922." -> "1.922,").
 */
export function formatDecimalInput(val: string | number | null | undefined): string {
  if (val === null || val === undefined) return "";

  if (typeof val === "number") {
    if (isNaN(val)) return "";
    const strNum = String(val);
    if (strNum.includes(".")) {
      const [intPart, decPart] = strNum.split(".");
      const formattedInt = formatNumberWithDots(intPart) || "0";
      return `${formattedInt},${decPart}`;
    }
    return formatNumberWithDots(val);
  }

  let str = String(val).trim();
  if (!str) return "";

  // If user explicitly typed a dot at the end, convert it to comma
  if (str.endsWith(".") && !str.includes(",")) {
    str = str.slice(0, -1) + ",";
  }

  if (str.includes(",")) {
    const parts = str.split(",");
    const intDigits = parts[0].replace(/[^0-9]/g, "");
    const decDigits = parts.slice(1).join("").replace(/[^0-9]/g, "");
    const cleanInt = intDigits ? intDigits.replace(/^0+(?=\d)/, "") : "0";
    const formattedInt = cleanInt ? cleanInt.replace(/\B(?=(\d{3})+(?!\d))/g, ".") : "0";
    return parts.length > 1 ? `${formattedInt},${decDigits}` : formattedInt;
  }

  // Integer Rupiah without comma: strip all non-digits and apply thousand dots
  const digits = str.replace(/[^0-9]/g, "");
  if (!digits) return "";
  const clean = digits.replace(/^0+(?=\d)/, "");
  return clean.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

/**
 * Parses a decimal price/currency string (e.g. "2.578.000" -> 2578000, "1.922,45" -> 1922.45)
 * into a float number.
 */
export function parseDecimal(val: string | number | null | undefined): number {
  if (val === null || val === undefined) return 0;
  if (typeof val === "number") return isNaN(val) ? 0 : val;
  const str = String(val).trim();
  if (!str) return 0;

  if (str.includes(",")) {
    const parts = str.split(",");
    const intClean = parts[0].replace(/[^0-9]/g, "") || "0";
    const decClean = parts.slice(1).join("").replace(/[^0-9]/g, "");
    const combined = decClean ? `${intClean}.${decClean}` : intClean;
    const num = parseFloat(combined);
    return isNaN(num) ? 0 : num;
  }

  const digits = str.replace(/[^0-9]/g, "");
  return digits ? parseInt(digits, 10) || 0 : 0;
}

/**
 * Parses quantity/units string (e.g. "0.543", "0,543", "2871,1295", "10") into float.
 * Both dot and comma are treated as decimal separators.
 */
export function parseUnits(val: string | number | null | undefined): number {
  if (val === null || val === undefined) return 0;
  if (typeof val === "number") return isNaN(val) ? 0 : val;
  const str = String(val).trim();
  if (!str) return 0;
  const clean = str.replace(",", ".");
  const num = parseFloat(clean);
  return isNaN(num) ? 0 : num;
}
