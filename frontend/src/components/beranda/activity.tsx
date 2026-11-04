"use client";

import Link from "next/link";
import { Icon, type IconName } from "@/components/ui/Icon";
import { Card, CardHeader } from "@/components/workspace/primitives";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { classifierLabel, sourceLabel } from "@/lib/provenance";
import type { ActivityRow } from "./types";

/** At most this many rows of today and yesterday on Beranda (beranda-cycle-summary). */
export const ACTIVITY_LIMIT = 8;

export type RowKind = "topup" | "movement" | "income" | "expense";

export function rowKind(row: ActivityRow): RowKind {
  if (row.investment_topup_id) return "topup";
  if (row.is_consolidated_transfer || row.movement_id || row.category_name === "Internal Movement" || row.is_excluded_from_budget) {
    return "movement";
  }
  return row.type === "income" ? "income" : "expense";
}

export function isWaiting(row: ActivityRow): boolean {
  return row.review_status === "needs_review";
}

export function rowTitle(row: ActivityRow): string {
  const kind = rowKind(row);
  if (kind === "topup") return "Top up Investasi";
  if (kind === "movement") {
    const target = row.target_account_name ?? row.transfer_target_account_name;
    return target ? `${row.account_name} → ${target}` : row.account_name;
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

export function provenanceText(row: ActivityRow): string | null {
  const parts = [classifierLabel(row.classified_by), sourceLabel(row.source)].filter(Boolean);
  return parts.length ? parts.join(" · ") : null;
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
  return <span>−{bal(row.amount)}</span>;
}

const SHORT_DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });

/** Where the money went or came from: account and category, or what the movement or top-up was. */
function rowSubtitle(row: ActivityRow): string {
  const kind = rowKind(row);
  if (isWaiting(row)) return `${row.account_name} · menunggu jawaban`;
  if (kind === "movement") return row.notes ? `Pindah saldo · ${row.notes}` : "Pindah saldo";
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
        extra={<span className="text-[13.5px] font-normal text-[var(--muted)]">hari ini dan kemarin</span>}
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
