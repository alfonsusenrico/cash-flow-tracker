"use client";

import Link from "next/link";
import { Icon, type IconName } from "@/components/ui/Icon";
import { Card, CardHeader } from "@/components/workspace/primitives";
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

/** The label of a movement whose other leg is not known yet (OD114). */
export const UNPAIRED_LABEL = "Pasangan belum ditemukan";

export function routeIncomplete(row: ActivityRow): boolean {
  if (rowKind(row) !== "movement") return false;
  const route = movementRoute(row);
  return !route.source || !route.target;
}

/**
 * A movement seen from the account a list is filtered to (design D3): "Masuk dari …" when the money came in,
 * "Keluar ke …" when it left. Merged rows only exist in unfiltered lists, so the row's own leg is the viewed one.
 */
export function movementDirection(row: ActivityRow): string {
  const route = movementRoute(row);
  const ownIsTarget = ownLegIsTarget(row);
  const other = ownIsTarget ? route.source : route.target;
  if (!other) return UNPAIRED_LABEL;
  return ownIsTarget ? `Masuk dari ${other}` : `Keluar ke ${other}`;
}

export function rowTitle(row: ActivityRow): string {
  const kind = rowKind(row);
  if (kind === "topup") return "Top up Investasi";
  if (kind === "movement") {
    const route = movementRoute(row);
    return `${route.source ?? "?"} → ${route.target ?? "?"}`;
  }
  return row.merchant || row.notes || row.category_name || "Tanpa keterangan";
}

const JAKARTA_TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const JAKARTA_DAY = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });

export function jakartaDay(value: Date | string): string {
  return JAKARTA_DAY.format(typeof value === "string" ? new Date(value) : value);
}

/** Status cell: waiting rows say so instead of a pillar; movements are not counted. */
export function rowStatus(row: ActivityRow): { text: string; bucket: string; color?: string } {
  const kind = rowKind(row);
  if (isWaiting(row)) return { text: "Menunggu jawaban", bucket: "waiting" };
  if (kind === "movement") return { text: "Tidak dihitung", bucket: "none" };
  if (kind === "income") return { text: "Uang masuk", bucket: "none" };
  if (kind === "topup") {
    const info = kakeiboBucketInfo("saving");
    return { text: info?.label ?? "Tabungan", bucket: "saving", color: info?.color };
  }
  const bucket = row.kakeibo_type || "unclassified";
  const info = kakeiboBucketInfo(bucket);
  return { text: info?.label ?? bucket, bucket, color: info?.color };
}

/** Category cell: the owner's category name; "—" where the pillar text already says what the row is. */
export function rowCategory(row: ActivityRow): string {
  const kind = rowKind(row);
  if (kind === "movement" || kind === "topup") return "—";
  if (row.category_name) return row.category_name;
  return isWaiting(row) ? "—" : "Tanpa kategori";
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

const SHORT_DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });

/** Where the money went or came from: account and category, or what the movement or top-up was. */
function rowSubtitle(row: ActivityRow): string {
  const kind = rowKind(row);
  if (isWaiting(row)) return `${row.account_name} · menunggu jawaban`;
  if (kind === "movement") {
    return ["Pindah saldo", routeIncomplete(row) ? UNPAIRED_LABEL : row.notes].filter(Boolean).join(" · ");
  }
  if (kind === "topup") return row.notes || row.account_name;
  // A pocket named like its category ("Transportasi · Transportasi") is said once.
  return [...new Set([row.account_name, row.category_name].filter(Boolean))].join(" · ");
}

/** Day and time on the right of a row, e.g. "3 Nov · 21.04". */
export function rowWhen(row: ActivityRow): string {
  return `${SHORT_DAY.format(new Date(row.date))} · ${JAKARTA_TIME.format(new Date(row.date))}`;
}

/** "Aktivitas terbaru": today's and yesterday's rows, newest first, with the detail on select. */
export function ActivityCard({
  rows,
  bal,
  onSelect,
  error,
}: {
  rows: ActivityRow[] | null;
  bal: (amount: number) => string;
  onSelect: (row: ActivityRow) => void;
  error: boolean;
}) {
  return (
    <Card labelledBy="beranda-activity">
      <CardHeader
        id="beranda-activity"
        title="Aktivitas terbaru"
        count={rows?.length}
        countLabel={rows ? `${rows.length} transaksi` : undefined}
        extra={<span className="text-sm font-normal text-[var(--muted)]">hari ini dan kemarin</span>}
        action={
          <Link href="/ledger" className="ws-linkbtn ws-accent-link">
            Semua transaksi
          </Link>
        }
      />
      {error && <p className="ws-caption mt-3">Aktivitas belum bisa dimuat.</p>}
      {rows && rows.length === 0 && <p className="ws-caption mt-3">Belum ada transaksi hari ini dan kemarin.</p>}
      {rows && rows.length > 0 && (
        // The whole row opens the transaction (owner preference, refine-workspace-layout).
        <ul className="ws-act-list">
          {rows.map((row) => (
            <li key={row.id}>
              <button type="button" className="ws-act-row" onClick={() => onSelect(row)}>
                <RowAvatar row={row} />
                <span className="ws-who-t">
                  <b>{rowTitle(row)}</b>
                  <span>{rowSubtitle(row)}</span>
                </span>
                <span className="ws-act-r">
                  <RowAmount row={row} bal={bal} />
                  <span>{rowWhen(row)}</span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}
