"use client";

import { Icon, type IconName } from "@/components/ui/Icon";
import { INDONESIAN_ROW_LABELS, type RowLabels } from "@/components/transactions/rowLabels";
import { createFormat, type Format } from "@/lib/format";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { isMovementCategory } from "@/lib/systemCategories";
import type { ActivityRow } from "./types";

/** At most this many rows of today and yesterday on Beranda (beranda-cycle-summary). */
export const ACTIVITY_LIMIT = 8;

export type RowKind = "topup" | "movement" | "income" | "expense";

export function rowKind(row: ActivityRow): RowKind {
  if (row.investment_topup_id) return "topup";
  if (row.is_consolidated_transfer || row.movement_id || isMovementCategory(row.category_system_key) || row.is_excluded_from_budget) {
    return "movement";
  }
  return row.type === "income" ? "income" : "expense";
}

export function isWaiting(row: ActivityRow): boolean {
  return row.review_status === "needs_review";
}

/** "Bank · Pocket" for a pocket, the account's own name otherwise; null when the side is unknown. */
export function accountLabel(name: string | null | undefined, parent: string | null | undefined): string | null {
  if (!name) return null;
  return parent ? `${parent} · ${name}` : name;
}

export interface MovementRoute {
  source: string | null;
  target: string | null;
}

/**
 * Where a movement's money came from and where it went (design D2). A merged row is always its outbound
 * leg; a linked leg follows its role; a leg nobody paired takes its side from its type, the other side unknown.
 */
export function movementRoute(row: ActivityRow): MovementRoute {
  const own = accountLabel(row.account_name, row.account_parent_name);
  if (row.is_consolidated_transfer || row.is_inferred_transfer) {
    return { source: own, target: accountLabel(row.target_account_name, row.target_account_parent_name) };
  }
  const partner = accountLabel(row.transfer_target_account_name, row.transfer_target_account_parent_name);
  return ownLegIsTarget(row) ? { source: partner, target: own } : { source: own, target: partner };
}

/** Whether the row's own account received the money: an inbound leg, or an unpaired income. */
function ownLegIsTarget(row: ActivityRow): boolean {
  if (row.is_consolidated_transfer || row.is_inferred_transfer) return false;
  return row.movement_role ? row.movement_role === "inbound" : row.type === "income";
}

/** The Indonesian label of a movement whose other leg is not known yet (OD114), for pages not translated yet. */
export const UNPAIRED_LABEL = INDONESIAN_ROW_LABELS.unpaired;

/** Formats of the pages not translated yet: Indonesian, region Indonesia. */
const INDONESIAN_FORMAT = createFormat({ language: "id", region: "ID" });

export function routeIncomplete(row: ActivityRow): boolean {
  if (rowKind(row) !== "movement") return false;
  const route = movementRoute(row);
  return !route.source || !route.target;
}

/**
 * A movement seen from the account a list is filtered to (design D3): "Masuk dari …" when the money came in,
 * "Keluar ke …" when it left. Merged rows only exist in unfiltered lists, so the row's own leg is the viewed one.
 */
export function movementDirection(row: ActivityRow, labels: RowLabels = INDONESIAN_ROW_LABELS): string {
  const route = movementRoute(row);
  const ownIsTarget = ownLegIsTarget(row);
  const other = ownIsTarget ? route.source : route.target;
  if (!other) return labels.unpaired;
  return ownIsTarget ? labels.inFrom(other) : labels.outTo(other);
}

export function rowTitle(row: ActivityRow, labels: RowLabels = INDONESIAN_ROW_LABELS): string {
  const kind = rowKind(row);
  if (kind === "topup") return labels.investmentTopUp;
  if (kind === "movement") {
    const route = movementRoute(row);
    return `${route.source ?? "?"} → ${route.target ?? "?"}`;
  }
  return row.merchant || row.notes || row.category_name || labels.undescribed;
}

const JAKARTA_DAY = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });

export function jakartaDay(value: Date | string): string {
  return JAKARTA_DAY.format(typeof value === "string" ? new Date(value) : value);
}

/** Status cell: waiting rows say so instead of a pillar; movements are not counted. */
export function rowStatus(row: ActivityRow, labels: RowLabels = INDONESIAN_ROW_LABELS): { text: string; bucket: string; color?: string } {
  const kind = rowKind(row);
  if (isWaiting(row)) return { text: labels.waiting, bucket: "waiting" };
  if (kind === "movement") return { text: labels.notCounted, bucket: "none" };
  if (kind === "income") return { text: labels.income, bucket: "none" };
  if (kind === "topup") return { text: labels.bucket("saving"), bucket: "saving", color: kakeiboBucketInfo("saving")?.color };
  const bucket = row.kakeibo_type || "unclassified";
  return { text: labels.bucket(bucket), bucket, color: kakeiboBucketInfo(bucket)?.color };
}

/** Category cell: the owner's category name; "—" where the pillar text already says what the row is. */
export function rowCategory(row: ActivityRow, labels: RowLabels = INDONESIAN_ROW_LABELS): string {
  const kind = rowKind(row);
  if (kind === "movement" || kind === "topup") return "—";
  if (row.category_name) return row.category_name;
  return isWaiting(row) ? "—" : labels.noCategory;
}

const AVATAR: Record<RowKind | "waiting", { icon: IconName | null; tone: string }> = {
  topup: { icon: "trending-up", tone: "ws-tone-inv" },
  movement: { icon: "repeat", tone: "ws-tone-tr" },
  income: { icon: "arrow-down-left", tone: "ws-tone-in" },
  expense: { icon: "arrow-up-right", tone: "ws-tone-out" },
  waiting: { icon: null, tone: "ws-tone-warn" },
};

export function RowAvatar({ row }: { row: ActivityRow }) {
  const look = isWaiting(row) && rowKind(row) === "expense" ? AVATAR.waiting : AVATAR[rowKind(row)];
  return (
    <span className={`ws-ava ${look.tone}`} aria-hidden="true">
      {look.icon ? <Icon name={look.icon} /> : "?"}
    </span>
  );
}

export function RowAmount({ row, bal }: { row: ActivityRow; bal: (amount: number) => string }) {
  const kind = rowKind(row);
  if (kind === "topup") return <span className="ws-amt-inv">{bal(row.amount)}</span>;
  if (kind === "movement") {
    return (
      <span className="ws-amt-tr inline-flex items-center gap-1">
        <Icon name="repeat" className="h-3.5 w-3.5" />
        {bal(row.amount)}
      </span>
    );
  }
  if (kind === "income") return <span className="ws-amt-in">+{bal(row.amount)}</span>;
  return <span className="ws-amt-out">−{bal(row.amount)}</span>;
}

/** Day and time on the right of a row, e.g. "3 Nov · 21.04". */
export function rowWhen(row: ActivityRow, format: Format = INDONESIAN_FORMAT): string {
  return `${format.shortDateAt(row.date)} · ${format.time(row.date)}`;
}

/** The row's time in Asia/Jakarta, such as "18.23"; the day is in the day heading. */
export function rowTime(row: ActivityRow, format: Format = INDONESIAN_FORMAT): string {
  return format.time(row.date);
}

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

/** The desktop meta beside the title: the account with its bank, or what a movement says about itself. */
export function activityMeta(row: ActivityRow, labels: RowLabels = INDONESIAN_ROW_LABELS): { text: string; warning?: boolean } {
  const kind = rowKind(row);
  if (kind === "movement") return routeIncomplete(row) ? { text: labels.unpaired, warning: true } : { text: row.notes ?? "" };
  return { text: accountLabel(row.account_name, row.account_parent_name) ?? "" };
}

/** The phone's second line after the time: the pillar, the income's category, or the movement. */
export function activityPhoneStatus(row: ActivityRow, labels: RowLabels = INDONESIAN_ROW_LABELS): { text: string; warning?: boolean } {
  const kind = rowKind(row);
  if (kind === "income") return { text: row.category_name ?? rowStatus(row, labels).text };
  if (kind === "movement") return routeIncomplete(row) ? { text: labels.unpaired, warning: true } : { text: labels.transfer };
  return { text: rowStatus(row, labels).text };
}
