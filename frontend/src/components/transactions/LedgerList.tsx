"use client";

import { useRef, type MouseEvent as ReactMouseEvent, type PointerEvent as ReactPointerEvent } from "react";
import { useTranslations } from "next-intl";
import { DayHeading, ListRow } from "@/components/workspace/lists";
import type { Format } from "@/lib/format";
import { previousDay, type DayGroup } from "@/lib/ledgerDays";
import { RowAmount, RowStatus, maskedTag, rowCategory, rowMeta, rowPhoneLine, rowTitle, type RowLabels, type RowLine } from "./rows";
import type { LedgerRow } from "./types";

const HOLD_MS = 600;

export interface ListSelection {
  active: boolean;
  selectedIds: string[];
  /** A row that cannot join a merge (already a movement, or the pair is full). */
  disabled: (row: LedgerRow) => boolean;
  toggle: (row: LedgerRow) => void;
  /** Holding a phone row starts the selection with that row (deviation 12). */
  start: (row: LedgerRow) => void;
}

interface LedgerListProps {
  groups: DayGroup<LedgerRow>[];
  openRow: (row: LedgerRow) => void;
  selection: ListSelection;
  /** Loads the next page; null when every row is loaded or a page is on its way. */
  loadMore: (() => void) | null;
  /** The row whose dialog is open. */
  openId: string | null;
  /** The list is filtered to one account, so a movement says whether its money came in or left. */
  accountView: boolean;
  today: string;
  labels: RowLabels;
  format: Format;
  money: (amount: number) => string;
}

function Line({ text, warning }: RowLine) {
  return warning ? <span className="c-warning">{text}</span> : <>{text}</>;
}

/** The phone's long-press, which starts a merge with the held row (today's `useHold`, kept). */
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
      onContextMenu: (event: ReactMouseEvent) => {
        if (fired.current) event.preventDefault();
      },
    }),
  };
}

/** The heading's figures: the whole day under the filters from the server's `daily_totals`, muted (D10). */
function DayTotals({ group, money }: { group: DayGroup<LedgerRow>; money: (amount: number) => string }) {
  const t = useTranslations("transactions.day");
  const totals = group.totals;
  if (!totals) return null;
  const amount = (value: number) => maskedTag(money(value));
  const parts = [
    totals.inflow ? t.rich("income", { amount: amount(totals.inflow) }) : null,
    totals.spending ? t.rich("spending", { amount: amount(totals.spending) }) : null,
    totals.movements ? t.rich("transfer", { amount: amount(totals.movements) }) : null,
    totals.waiting_count ? <span className="c-warning">{t("waiting", { count: totals.waiting_count })}</span> : null,
  ].filter(Boolean);
  if (!parts.length) return null;
  return (
    <>
      {parts.map((part, index) => (
        <span key={index}>
          {index > 0 && " · "}
          {part}
        </span>
      ))}
    </>
  );
}

/**
 * Transaksi's list (D10 to D12): day groups under sticky headings, one-line rows from 768 px and two lines on the
 * phone, each row one button that opens the dialog, or one label with a checkbox while merging.
 */
export function LedgerList({ groups, openRow, selection, loadMore, openId, accountView, today, labels, format, money }: LedgerListProps) {
  const t = useTranslations("transactions");
  const hold = useHold(selection);
  const yesterday = previousDay(today);
  const dayLabel = (day: string) => {
    const name = format.weekdayDate(day);
    if (day === today) return t("day.today", { day: name });
    if (day === yesterday) return t("day.yesterday", { day: name });
    return name;
  };
  return (
    <>
      {groups.map((group) => (
        <div key={group.date} className="day-group">
          <DayHeading label={dayLabel(group.date)} totals={group.totals ? <DayTotals group={group} money={money} /> : undefined} />
          <ul className="list">
            {group.rows.map((row) => {
              const time = format.time(row.date);
              const title = rowTitle(row, labels);
              const meta = rowMeta(row, labels, format, accountView);
              const phone = rowPhoneLine(row, labels, format, accountView);
              const checked = selection.selectedIds.includes(row.id);
              return (
                <li key={row.id}>
                  <ListRow
                    rowKey={row.id}
                    time={time}
                    title={title}
                    meta={meta.text ? <Line {...meta} /> : undefined}
                    phoneMeta={
                      <>
                        {time} · <Line {...phone} />
                      </>
                    }
                    category={rowCategory(row, labels)}
                    status={<RowStatus row={row} labels={labels} />}
                    amount={<RowAmount row={row} money={money} />}
                    selected={openId === row.id}
                    selectable={selection.active ? {
                      checked,
                      disabled: !checked && selection.disabled(row),
                      onToggle: () => selection.toggle(row),
                      label: t("merge.select", { title, time }),
                    } : undefined}
                    pressHandlers={selection.active ? undefined : hold.handlers(row)}
                    onClick={() => {
                      if (hold.fired.current) {
                        hold.fired.current = false;
                        return;
                      }
                      openRow(row);
                    }}
                  />
                </li>
              );
            })}
          </ul>
          {group.remaining > 0 && loadMore && (
            <div className="list-foot">
              <button type="button" className="ws-btn ws-btn-quiet" onClick={loadMore}>
                {t("more.inDay", { remaining: format.integer(group.remaining) })}
              </button>
            </div>
          )}
        </div>
      ))}
    </>
  );
}
