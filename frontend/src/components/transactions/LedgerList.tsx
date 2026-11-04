"use client";

import { useRef, type PointerEvent as ReactPointerEvent } from "react";
import { RowAmount, RowAvatar, provenanceText, rowKind, rowStatus, rowTitle } from "@/components/beranda/activity";
import { Pill, type Tone } from "@/components/workspace/primitives";
import { dayName, previousDay, todayLocal, type DayGroup } from "@/lib/ledgerDays";
import type { LedgerRow } from "./types";

const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const HOLD_MS = 600;

const KIND_PILL: { [key: string]: { tone: Tone; text: string } } = {
  expense: { tone: "out", text: "Pengeluaran" },
  income: { tone: "in", text: "Masuk" },
  movement: { tone: "tr", text: "Pindah saldo" },
  topup: { tone: "inv", text: "Top up Investasi" },
};

export interface ListSelection {
  active: boolean;
  selectedIds: string[];
  /** A row that cannot join a merge (already a movement, or the pair is full). */
  disabled: (row: LedgerRow) => boolean;
  toggle: (row: LedgerRow) => void;
  /** Long-press on a phone row starts the selection with that row. */
  start: (row: LedgerRow) => void;
}

interface ListProps {
  groups: DayGroup<LedgerRow>[];
  bal: (amount: number) => string;
  openRow: (row: LedgerRow) => void;
  selection: ListSelection;
  loadMore: (() => void) | null;
  /** Which row's drawer is open, to mark it in the list. */
  openId: string | null;
}

function dayLabel(day: string): string {
  const today = todayLocal();
  if (day === today) return `${dayName(day)} · hari ini`;
  if (day === previousDay(today)) return `${dayName(day)} · kemarin`;
  return dayName(day);
}

/** The heading's figures: the whole day under the filters, from the server; nothing when it sent none. */
function DayTotals({ group, bal }: { group: DayGroup<LedgerRow>; bal: (amount: number) => string }) {
  const totals = group.totals;
  if (!totals) return null;
  const parts: React.ReactNode[] = [];
  if (totals.spending) parts.push(<span key="out">Pengeluaran <b>{bal(totals.spending)}</b></span>);
  if (totals.inflow) parts.push(<span key="in">Masuk <b className="ws-amt-in">{bal(totals.inflow)}</b></span>);
  if (totals.movements) parts.push(<span key="mv">Pindah saldo <b>{bal(totals.movements)}</b></span>);
  if (totals.waiting_count) parts.push(<span key="wait" className="ws-tx-waiting">{totals.waiting_count} menunggu jawaban</span>);
  if (!parts.length) return null;
  return (
    <span className="ws-tx-daytot">
      {parts.map((part, index) => (
        <span key={index}>
          {index > 0 && <span aria-hidden="true"> · </span>}
          {part}
        </span>
      ))}
    </span>
  );
}

function subLine(row: LedgerRow): string {
  const kind = rowKind(row);
  const where = kind === "movement" || kind === "topup" ? (row.notes || row.account_name) : row.account_name;
  const checked = row.review_status === "auto" ? " · belum dicek" : "";
  return `${where}${checked}`;
}

function SelectBox({ row, selection }: { row: LedgerRow; selection: ListSelection }) {
  return (
    <input
      type="checkbox"
      className="ws-tx-check"
      checked={selection.selectedIds.includes(row.id)}
      disabled={selection.disabled(row)}
      onChange={() => selection.toggle(row)}
      aria-label={`Pilih ${rowTitle(row)} ${TIME.format(new Date(row.date))}`}
    />
  );
}

function useHold(selection: ListSelection) {
  const timer = useRef<{ id: number; x: number; y: number; pointer: number } | null>(null);
  const fired = useRef(false);
  const clear = () => {
    if (timer.current) window.clearTimeout(timer.current.id);
    timer.current = null;
  };
  return {
    fired,
    handlers: (row: LedgerRow) => ({
      onPointerDown: (event: ReactPointerEvent) => {
        if (selection.active || row.movement_id || event.button !== 0 || event.pointerType === "mouse") return;
        fired.current = false;
        clear();
        timer.current = {
          id: window.setTimeout(() => {
            fired.current = true;
            timer.current = null;
            selection.start(row);
          }, HOLD_MS),
          x: event.clientX,
          y: event.clientY,
          pointer: event.pointerId,
        };
      },
      onPointerMove: (event: ReactPointerEvent) => {
        const held = timer.current;
        if (held && held.pointer === event.pointerId && Math.hypot(event.clientX - held.x, event.clientY - held.y) > 10) clear();
      },
      onPointerUp: clear,
      onPointerCancel: clear,
      onContextMenu: (event: React.MouseEvent) => {
        if (fired.current) event.preventDefault();
      },
    }),
  };
}

function MoreOfDay({ group, loadMore, colSpan }: { group: DayGroup<LedgerRow>; loadMore: (() => void) | null; colSpan?: number }) {
  if (!group.remaining) return null;
  const body = (
    <>
      {group.remaining} lagi di hari ini
      {loadMore && (
        <>
          {" · "}
          <button type="button" className="ws-linkbtn" onClick={loadMore}>Muat lagi</button>
        </>
      )}
    </>
  );
  return colSpan ? (
    <tr className="ws-tx-more">
      <td colSpan={colSpan}>{body}</td>
    </tr>
  ) : (
    <li className="ws-tx-more">{body}</li>
  );
}

/** Desktop and wide tablet: one table, one `tbody` per day whose first row is the day heading (D1). */
export function LedgerTable({ groups, bal, openRow, selection, loadMore, openId }: ListProps) {
  return (
    <table className="ws-tbl ws-tx-tbl">
      <caption className="sr-only">Transaksi, dikelompokkan per hari, terbaru di atas</caption>
      <thead>
        <tr>
          <th scope="col">Transaksi</th>
          <th scope="col" className="ws-tx-col-time">Jam</th>
          <th scope="col" className="ws-tx-col-kind">Jenis</th>
          <th scope="col" className="ws-tx-col-cat">Kategori</th>
          <th scope="col" className="ws-tx-col-prov">Dicatat</th>
          <th scope="col" className="r">Jumlah</th>
        </tr>
      </thead>
      {groups.map((group) => (
        <tbody key={group.date}>
          <tr className="ws-tx-day">
            <th scope="rowgroup" colSpan={6}>
              <span className="ws-tx-dname">{dayLabel(group.date)}</span>
              <DayTotals group={group} bal={bal} />
            </th>
          </tr>
          {group.rows.map((row) => {
            const kind = rowKind(row);
            const status = rowStatus(row);
            return (
              <tr
                key={row.id}
                className="ws-tx-row"
                data-open={openId === row.id || undefined}
                data-selected={selection.selectedIds.includes(row.id) || undefined}
                onClick={(event) => {
                  if ((event.target as HTMLElement).closest("button, input, a")) return;
                  if (selection.active) {
                    if (!selection.disabled(row)) selection.toggle(row);
                  } else openRow(row);
                }}
              >
                <td>
                  <div className="ws-who">
                    {selection.active ? <SelectBox row={row} selection={selection} /> : <RowAvatar row={row} />}
                    <div className="ws-who-t">
                      <button type="button" className="ws-rowbtn" data-tx-row={row.id} onClick={() => (selection.active ? !selection.disabled(row) && selection.toggle(row) : openRow(row))}>
                        {rowTitle(row)}
                      </button>
                      <span>{subLine(row)}</span>
                    </div>
                  </div>
                </td>
                <td className="ws-tx-col-time tabular-nums">{TIME.format(new Date(row.date))}</td>
                <td className="ws-tx-col-kind">
                  <Pill tone={KIND_PILL[kind].tone}>{KIND_PILL[kind].text}</Pill>
                </td>
                <td className="ws-tx-col-cat">
                  <span className="ws-kk" data-bucket={status.bucket} style={status.color ? ({ "--kc": status.color } as React.CSSProperties) : undefined}>
                    {status.text}
                  </span>
                </td>
                <td className="ws-tx-col-prov">
                  <span className="ws-prov">{provenanceText(row) ?? "—"}</span>
                </td>
                <td className="r tabular-nums">
                  <RowAmount row={row} bal={bal} />
                </td>
              </tr>
            );
          })}
          <MoreOfDay group={group} loadMore={loadMore} colSpan={6} />
        </tbody>
      ))}
    </table>
  );
}

/** Phone: a purpose-built feed with a sticky heading per day and rows of at least 56 px (D1). */
export function LedgerFeed({ groups, bal, openRow, selection, loadMore, openId }: ListProps) {
  const hold = useHold(selection);
  return (
    <div className="ws-tx-feed">
      {groups.map((group) => (
        <section key={group.date} aria-labelledby={`tx-day-${group.date}`}>
          <h2 id={`tx-day-${group.date}`} className="ws-tx-feed-h">
            <span className="ws-tx-dname">{dayLabel(group.date)}</span>
            <DayTotals group={group} bal={bal} />
          </h2>
          <ul className="ws-ph-list">
            {group.rows.map((row) => (
              <li key={row.id} className="ws-tx-feed-row" data-open={openId === row.id || undefined} data-selected={selection.selectedIds.includes(row.id) || undefined}>
                {selection.active && <SelectBox row={row} selection={selection} />}
                <button
                  type="button"
                  className="ws-ph-row"
                  data-tx-row={row.id}
                  {...hold.handlers(row)}
                  onClick={() => {
                    if (hold.fired.current) {
                      hold.fired.current = false;
                      return;
                    }
                    if (selection.active) {
                      if (!selection.disabled(row)) selection.toggle(row);
                    } else openRow(row);
                  }}
                >
                  <RowAvatar row={row} />
                  <span className="ws-who-t">
                    <b>{rowTitle(row)}</b>
                    <span>
                      {TIME.format(new Date(row.date))} · {rowStatus(row).text}
                    </span>
                  </span>
                  <RowAmount row={row} bal={bal} />
                </button>
              </li>
            ))}
            <MoreOfDay group={group} loadMore={loadMore} />
          </ul>
        </section>
      ))}
    </div>
  );
}
