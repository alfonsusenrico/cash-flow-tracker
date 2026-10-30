import { isKakeiboPillar, type KakeiboPillar } from "@/lib/kakeibo";

export type LedgerPeriod = "all" | "cycle" | "month";
export type LedgerTypeFilter = "all" | "expense" | "income";
export type LedgerReviewFilter = "all" | "needs_review" | "auto" | "reviewed";

/** Category filter value that selects rows without a category ("Tanpa kategori"). */
export const UNCATEGORIZED = "uncategorized";

export interface LedgerFilters {
  period: LedgerPeriod;
  offset: number;
  category: string;
  kakeibo: KakeiboPillar | "all";
  type: LedgerTypeFilter;
  account: string;
  review: LedgerReviewFilter;
  /** ISO instants of an exact window from a report link; ignored with a period. */
  from: string | null;
  to: string | null;
  /** Search text from a report link, such as a merchant name. */
  q: string;
}

export const DEFAULT_LEDGER_FILTERS: LedgerFilters = {
  period: "all",
  offset: 0,
  category: "all",
  kakeibo: "all",
  type: "all",
  account: "all",
  review: "all",
  from: null,
  to: null,
  q: "",
};

// Matches the backend's period_offset bounds.
const MIN_OFFSET = -36;
const MAX_OFFSET = 12;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ISO_INSTANT_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:\d{2})$/;
const MAX_SEARCH_LENGTH = 100;

function instantOrNull(value: string | null): string | null {
  return value && ISO_INSTANT_PATTERN.test(value) && !Number.isNaN(Date.parse(value)) ? value : null;
}

function oneOf<T extends string>(value: string | null, allowed: readonly T[], fallback: T): T {
  return value && (allowed as readonly string[]).includes(value) ? (value as T) : fallback;
}

/** Filters from the ledger URL; anything invalid falls back to "all" so a stale link still opens. */
export function parseLedgerFilters(params: URLSearchParams | null): LedgerFilters {
  if (!params) return DEFAULT_LEDGER_FILTERS;
  const period = oneOf(params.get("period"), ["cycle", "month"] as const, "all");
  const rawOffset = Number.parseInt(params.get("offset") ?? "0", 10);
  const offset = period === "all" || Number.isNaN(rawOffset) ? 0 : Math.min(MAX_OFFSET, Math.max(MIN_OFFSET, rawOffset));
  const category = params.get("category");
  const account = params.get("account");
  const kakeibo = params.get("kakeibo");
  const ranged = period === "all";
  return {
    period,
    offset,
    category: category === UNCATEGORIZED || (category && UUID_PATTERN.test(category)) ? category : "all",
    kakeibo: isKakeiboPillar(kakeibo) ? kakeibo : "all",
    type: oneOf(params.get("type"), ["expense", "income"] as const, "all"),
    account: account && UUID_PATTERN.test(account) ? account : "all",
    review: oneOf(params.get("review"), ["needs_review", "auto", "reviewed"] as const, "all"),
    from: ranged ? instantOrNull(params.get("from")) : null,
    to: ranged ? instantOrNull(params.get("to")) : null,
    q: (params.get("q") ?? "").trim().slice(0, MAX_SEARCH_LENGTH),
  };
}

/** The URL query for filters, leaving out defaults so links stay short. */
export function ledgerFilterQuery(filters: Partial<LedgerFilters>): string {
  const merged = { ...DEFAULT_LEDGER_FILTERS, ...filters };
  const params = new URLSearchParams();
  if (merged.period !== "all") {
    params.set("period", merged.period);
    if (merged.offset !== 0) params.set("offset", String(merged.offset));
  }
  if (merged.category !== "all") params.set("category", merged.category);
  if (merged.kakeibo !== "all") params.set("kakeibo", merged.kakeibo);
  if (merged.type !== "all") params.set("type", merged.type);
  if (merged.account !== "all") params.set("account", merged.account);
  if (merged.review !== "all") params.set("review", merged.review);
  if (merged.period === "all" && merged.from) params.set("from", merged.from);
  if (merged.period === "all" && merged.to) params.set("to", merged.to);
  if (merged.q) params.set("q", merged.q);
  return params.toString();
}

export function ledgerHref(filters: Partial<LedgerFilters>): string {
  const query = ledgerFilterQuery(filters);
  return query ? `/ledger?${query}` : "/ledger";
}

/** Query parameters of `GET /transactions` for the filters (without paging and search). */
export function ledgerApiParams(filters: LedgerFilters): URLSearchParams {
  const params = new URLSearchParams();
  if (filters.period !== "all") {
    params.set("period", filters.period);
    params.set("period_offset", String(filters.offset));
  }
  if (filters.category === UNCATEGORIZED) params.set("uncategorized", "true");
  else if (filters.category !== "all") params.set("category_id", filters.category);
  if (filters.kakeibo !== "all") params.set("kakeibo_type", filters.kakeibo);
  if (filters.type !== "all") params.set("type", filters.type);
  if (filters.account !== "all") params.set("account_id", filters.account);
  if (filters.review !== "all") params.set("review_status", filters.review);
  if (filters.period === "all" && filters.from) params.set("from_date", filters.from);
  if (filters.period === "all" && filters.to) params.set("to_date", filters.to);
  return params;
}

export function hasDateRange(filters: LedgerFilters): boolean {
  return filters.period === "all" && Boolean(filters.from || filters.to);
}

export function hasRowFilters(filters: LedgerFilters): boolean {
  return filters.category !== "all" || filters.kakeibo !== "all" || filters.type !== "all"
    || filters.account !== "all" || filters.review !== "all" || hasDateRange(filters);
}

/** A report period of the dashboard (`timeframe` + `cycle_offset`) as ledger filters; null for rolling windows. */
export function reportPeriodFilters(timeframe: string, offset: number): Pick<LedgerFilters, "period" | "offset"> | null {
  if (timeframe === "month" || timeframe === "cycle") return { period: timeframe, offset };
  return null;
}
