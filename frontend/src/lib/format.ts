"use client";

import { createContext, createElement, useContext, useMemo, type ReactNode } from "react";
import { useLocale } from "next-intl";
import { DEFAULT_REGION, parseLanguage, type Language, type Region } from "@/i18n/language";
import { fmtUSD, getCurrencyConfig } from "@/lib/utils";

/**
 * One formatter by language and region (add-language-system D6). Words follow the language; digit grouping, date
 * order and the clock follow the region, so an English account in region Indonesia still reads "Rp 1.234.567"
 * and "Thursday, 5 November". Messages receive these strings as arguments and never format numbers themselves.
 */
export interface FormatConfig {
  language: Language;
  region: Region;
  /** The display currency; amounts are always stored in IDR. */
  currency?: "IDR" | "USD";
  /** USD/IDR for the USD display conversion. */
  rate?: number;
}

const ZONE = "Asia/Jakarta";

/** Digits follow the region alone: Indonesia groups with a dot and writes a decimal comma, the United States the reverse. */
function numberLocale(region: Region): string {
  return region === "US" ? "en-US" : "id-ID";
}

/** A calendar date (`YYYY-MM-DD`, or the date part of an ISO string) as midnight UTC, so the day never shifts. */
function calendarDay(iso: string): Date {
  return new Date(`${iso.slice(0, 10)}T00:00:00Z`);
}

function instant(value: Date | string): Date {
  return typeof value === "string" ? new Date(value) : value;
}

export function createFormat({ language, region, currency = "IDR", rate }: FormatConfig) {
  const dateLocale = `${language}-${region}`;
  const digits = numberLocale(region);
  // CLDR writes September "Sept" in English outside the United States; the server's labels and every other short
  // month have three letters, so English short months read "Sep" (apply-markas-cleanup D11).
  const withMonths = (options: Intl.DateTimeFormatOptions, timeZone: string): { format: (date: Date) => string } => {
    const formatter = new Intl.DateTimeFormat(dateLocale, { ...options, timeZone });
    if (language !== "en" || options.month !== "short") return formatter;
    return {
      format: (date) => formatter.formatToParts(date).map((part) => (part.type === "month" && part.value === "Sept" ? "Sep" : part.value)).join(""),
    };
  };
  const dayFormat = (options: Intl.DateTimeFormatOptions) => withMonths(options, "UTC");
  const zoneFormat = (options: Intl.DateTimeFormatOptions) => withMonths(options, ZONE);
  const shortDay = dayFormat({ day: "numeric", month: "short" });
  const longDay = dayFormat({ day: "numeric", month: "long" });
  const weekdayDay = dayFormat({ weekday: "long", day: "numeric", month: "long" });
  const monthName = dayFormat({ month: "long" });
  const shortMonth = dayFormat({ month: "short" });
  const weekdayShort = dayFormat({ weekday: "short" });
  const monthYear = dayFormat({ month: "long", year: "numeric" });
  const shortMonthYear = dayFormat({ month: "short", year: "numeric" });
  const weekdayInstant = zoneFormat({ weekday: "long", day: "numeric", month: "long" });
  const shortInstant = zoneFormat({ day: "numeric", month: "short" });
  const yearDay = dayFormat({ day: "numeric", month: "short", year: "numeric" });
  const yearInstant = zoneFormat({ day: "numeric", month: "short", year: "numeric" });
  const zoneDay = zoneFormat({ year: "numeric", month: "2-digit", day: "2-digit" });
  // Indonesia writes "18.23" with two-digit hours; the United States "6:23 PM".
  const clock = zoneFormat(region === "US" ? { hour: "numeric", minute: "2-digit" } : { hour: "2-digit", minute: "2-digit" });
  const clockSeconds = zoneFormat(
    region === "US" ? { hour: "numeric", minute: "2-digit", second: "2-digit" } : { hour: "2-digit", minute: "2-digit", second: "2-digit" },
  );

  /** A whole number with the region's grouping, such as "1.234.567". */
  const integer = (value: number) => Math.round(value).toLocaleString(digits);

  return {
    language,
    region,
    /**
     * Money as today's exact strings: IDR is "Rp", a normal space and the region's grouping with no decimals
     * (never `Intl` currency style, which writes a no-break space and "IDR"); USD display is `en-US` currency.
     */
    money(amountInIdr: number): string {
      if (currency === "USD") return fmtUSD(amountInIdr, false, rate ?? getCurrencyConfig().rate);
      const sign = amountInIdr < 0 ? "-" : "";
      return `${sign}Rp ${integer(Math.abs(amountInIdr))}`;
    },
    integer,
    /** A decimal with at most `maximumFractionDigits`, such as "5,4" (ID) or "5.4" (US). */
    decimal(value: number, maximumFractionDigits = 1): string {
      return value.toLocaleString(digits, { maximumFractionDigits });
    },
    /** A percentage with exactly `fractionDigits` decimals and no sign, such as "8,0%" (ID) or "8.0%" (US). */
    percent(value: number, fractionDigits = 1): string {
      return `${value.toLocaleString(digits, { minimumFractionDigits: fractionDigits, maximumFractionDigits: fractionDigits })}%`;
    },
    /** An amount input's digits grouped by the region: "20.000.000" or "20,000,000". */
    groupDigits(value: string | number | null | undefined): string {
      return groupDigits(value, region);
    },
    /** "5 Nov" (ID) or "Nov 5" (US) for a calendar date. */
    shortDate: (iso: string) => shortDay.format(calendarDay(iso)),
    /** "5 November" for a calendar date. */
    longDate: (iso: string) => longDay.format(calendarDay(iso)),
    /** "Kamis, 5 November" or "Thursday, 5 November" for a calendar date. */
    weekdayDate: (iso: string) => weekdayDay.format(calendarDay(iso)),
    /** The month's name of a calendar date. */
    monthName: (iso: string) => monthName.format(calendarDay(iso)),
    /** "Sen" (ID) or "Mon" (EN) for a calendar date. */
    weekdayShort: (iso: string) => weekdayShort.format(calendarDay(iso)),
    /** "Agu" (ID) or "Aug" (EN) for a calendar date. */
    shortMonth: (iso: string) => shortMonth.format(calendarDay(iso)),
    /** "November 2026" for a calendar date. */
    monthYear: (iso: string) => monthYear.format(calendarDay(iso)),
    /** "Nov 2026" for a calendar date. */
    shortMonthYear: (iso: string) => shortMonthYear.format(calendarDay(iso)),
    /** Today's or another instant's weekday and date in the app's time zone. */
    weekdayAt: (value: Date | string) => weekdayInstant.format(instant(value)),
    /** "5 Nov" for an instant in the app's time zone. */
    shortDateAt: (value: Date | string) => shortInstant.format(instant(value)),
    /** "5 Nov 2026" (ID) or "Nov 5, 2026" (US) for a calendar date. */
    yearDate: (iso: string) => yearDay.format(calendarDay(iso)),
    /** "5 Nov 2026" for an instant in the app's time zone. */
    yearDateAt: (value: Date | string) => yearInstant.format(instant(value)),
    /**
     * Two instants as one range in the app's time zone: "3 Okt 2026" on one day, else "3 Okt – 17 Okt 2026"
     * (the start without its year, as Tinjau has always written it); null without both ends.
     */
    dateRangeAt(first: string | null, last: string | null): string | null {
      if (!first || !last) return null;
      if (zoneDay.format(instant(first)) === zoneDay.format(instant(last))) return yearInstant.format(instant(last));
      return `${shortInstant.format(instant(first))} – ${yearInstant.format(instant(last))}`;
    },
    /** "18.23" (ID) or "6:23 PM" (US) for an instant in the app's time zone. */
    time: (value: Date | string) => clock.format(instant(value)),
    /** "18.23.05" (ID) or "6:23:05 PM" (US), for a test session's notification times. */
    timeSeconds: (value: Date | string) => clockSeconds.format(instant(value)),
    /**
     * A short amount for a calendar cell or a chart tick (apply-markas-analisis D22): "79rb", "1,3jt" in Indonesian,
     * "79K", "1.3M" in English, the decimal mark by region. `narrow` drops the decimal ("1jt") for a 45 px cell;
     * `hidden` returns the mask "••" while balances are hidden. The USD display converts first and writes "$1.3K".
     */
    compactMoney(amountInIdr: number, { narrow = false, hidden = false }: { narrow?: boolean; hidden?: boolean } = {}): string {
      if (hidden) return "••";
      const usd = currency === "USD";
      const value = usd ? amountInIdr / ((rate ?? getCurrencyConfig().rate) || 16500) : amountInIdr;
      const sign = value < 0 ? "-" : "";
      const units = usd || language === "en" ? ["K", "M", "B"] : ["rb", "jt", "M"];
      const prefix = usd ? "$" : "";
      // The USD display keeps en-US digits, as `money` does.
      const marks = usd ? "en-US" : digits;
      let scaled = Math.abs(value);
      let step = -1;
      // Step up while the rounded figure would read 1000 of the unit, so 999.600 reads "1jt", never "1000rb".
      while (step < units.length - 1 && Math.round(scaled) >= 1000) {
        scaled /= 1000;
        step += 1;
      }
      if (step < 0) return `${sign}${prefix}${Math.round(scaled).toLocaleString(marks)}`;
      // One decimal below 10 of a unit ("1,3jt", "1,5rb"); whole units above it ("81rb", "12jt").
      const decimals = narrow || scaled >= 10 ? 0 : 1;
      const text = scaled.toLocaleString(marks, { maximumFractionDigits: decimals });
      return `${sign}${prefix}${text}${units[step]}`;
    },
  };
}

export type Format = ReturnType<typeof createFormat>;

/** Digits only, grouped with the region's thousands separator; leading zeros dropped; empty without digits. */
export function groupDigits(value: string | number | null | undefined, region: Region): string {
  if (value === null || value === undefined) return "";
  const digitsOnly = String(value).trim().replace(/[^0-9]/g, "");
  if (!digitsOnly) return "";
  const clean = digitsOnly.replace(/^0+(?=\d)/, "");
  return clean.replace(/\B(?=(\d{3})+(?!\d))/g, region === "US" ? "," : ".");
}

interface RegionSettings {
  region: Region;
  currency: "IDR" | "USD";
  rate: number;
}

const RegionContext = createContext<RegionSettings | null>(null);

/** The signed-in account's region and display currency for `useFormat`; the sign-in page has none and uses ID. */
export function RegionProvider({ value, children }: { value: RegionSettings; children: ReactNode }) {
  return createElement(RegionContext.Provider, { value }, children);
}

/** The formatter for the interface language and the account's region and currency. */
export function useFormat(): Format {
  const language = parseLanguage(useLocale()) ?? "id";
  const settings = useContext(RegionContext);
  const region = settings?.region ?? DEFAULT_REGION;
  const currency = settings?.currency ?? getCurrencyConfig().currency;
  const rate = settings?.rate ?? getCurrencyConfig().rate;
  return useMemo(() => createFormat({ language, region, currency, rate }), [language, region, currency, rate]);
}
