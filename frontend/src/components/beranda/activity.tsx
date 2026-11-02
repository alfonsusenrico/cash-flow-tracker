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

export function rowTime(row: ActivityRow, today: string): string {
  const time = JAKARTA_TIME.format(new Date(row.date));
  return jakartaDay(row.date) === today ? time : `kemarin ${time}`;
}

/** Status cell: waiting rows say so instead of a pillar; movements are not counted. */
export function rowStatus(row: ActivityRow): { text: string; bucket: string; color?: string } {
  const kind = rowKind(row);
  if (isWaiting(row)) return { text: "Nunggu jawaban", bucket: "waiting" };
  if (kind === "movement") return { text: "Nggak dihitung", bucket: "none" };
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

function StatusCell({ row }: { row: ActivityRow }) {
  const status = rowStatus(row);
  return (
    <span className="ws-kk" data-bucket={status.bucket} style={status.color ? ({ "--kc": status.color } as React.CSSProperties) : undefined}>
      {status.text}
    </span>
  );
}

function rowSubtitle(row: ActivityRow, today: string): string {
  const kind = rowKind(row);
  const where = kind === "movement" ? "Pindah saldo" : kind === "topup" ? row.notes || row.account_name : row.account_name;
  return `${where} · ${rowTime(row, today)}`;
}

/** Desktop "Aktivitas terbaru": today's and yesterday's rows, newest first, with the detail on select. */
export function ActivityCard({
  rows,
  today,
  bal,
  onSelect,
  error,
}: {
  rows: ActivityRow[] | null;
  today: string;
  bal: (amount: number) => string;
  onSelect: (row: ActivityRow) => void;
  error: boolean;
}) {
  return (
    <Card labelledBy="beranda-activity">
      <CardHeader
        id="beranda-activity"
        icon="receipt"
        title="Aktivitas terbaru"
        count={rows?.length}
        countLabel={rows ? `${rows.length} transaksi` : undefined}
        extra={<span className="text-[13.5px] font-normal text-[var(--muted)]">hari ini dan kemarin</span>}
        action={
          <Link href="/ledger" className="ws-btn">
            Semua transaksi
          </Link>
        }
      />
      {error && <p className="ws-caption mt-3">Aktivitas belum bisa dimuat.</p>}
      {rows && rows.length === 0 && <p className="ws-caption mt-3">Belum ada transaksi hari ini dan kemarin.</p>}
      {rows && rows.length > 0 && (
        <table className="ws-tbl">
          <caption className="sr-only">Transaksi hari ini dan kemarin</caption>
          <thead>
            <tr>
              <th scope="col">Transaksi</th>
              <th scope="col" className="ws-hide-narrow">
                Kategori
              </th>
              <th scope="col" className="ws-hide-narrow">
                Dicatat
              </th>
              <th scope="col" className="r">
                Jumlah
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                <td>
                  <div className="ws-who">
                    <RowAvatar row={row} />
                    <div className="ws-who-t">
                      <button type="button" className="ws-rowbtn" onClick={() => onSelect(row)}>
                        {rowTitle(row)}
                      </button>
                      <span>{rowSubtitle(row, today)}</span>
                    </div>
                  </div>
                </td>
                <td className="ws-hide-narrow">
                  <StatusCell row={row} />
                </td>
                <td className="ws-hide-narrow">
                  <span className="ws-prov">{provenanceText(row) ?? "—"}</span>
                </td>
                <td className="r">
                  <RowAmount row={row} bal={bal} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

/** Phone "Hari ini": today's rows as a tappable list. */
export function TodayList({
  rows,
  today,
  bal,
  onSelect,
}: {
  rows: ActivityRow[];
  today: string;
  bal: (amount: number) => string;
  onSelect: (row: ActivityRow) => void;
}) {
  if (!rows.length) return <p className="ws-caption px-1">Belum ada transaksi hari ini.</p>;
  return (
    <ul className="ws-ph-list">
      {rows.map((row) => (
        <li key={row.id}>
          <button type="button" className="ws-ph-row" onClick={() => onSelect(row)}>
            <RowAvatar row={row} />
            <span className="ws-who-t">
              <b>{rowTitle(row)}</b>
              <span>
                {rowTime(row, today)} · {rowStatus(row).text}
              </span>
            </span>
            <RowAmount row={row} bal={bal} />
          </button>
        </li>
      ))}
    </ul>
  );
}
