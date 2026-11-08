"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { RowAmount, rowPhoneLine, rowTitle, useRowLabels } from "@/components/transactions/rows";
import { TransactionDialog } from "@/components/transactions/TransactionDialog";
import type { LedgerRow } from "@/components/transactions/types";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { HeroFigure } from "@/components/workspace/figures";
import { useMediaQuery } from "@/components/workspace/layers";
import { ListRow, Panel, PanelBlock, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { ErrorState } from "@/components/workspace/primitives";
import { api } from "@/lib/api";
import { type Format, useFormat } from "@/lib/format";
import { ledgerHref } from "@/lib/ledgerFilters";
import { cn } from "@/lib/utils";
import { addDays, dayDiff, mondayIndex } from "../format";
import type { CalendarDay, CalendarPlan, CalendarResponse, PeriodBasis } from "../types";

type Money = (amount: number) => string;
type Translate = ReturnType<typeof useTranslations<"analysis">>;

/** Below 360 px a cell is about 45 px wide, so its amount drops the decimal ("1jt"). */
const NARROW_QUERY = "(max-width: 359px)";
/** A Monday, so the seven column heads are named in the interface language. */
const MONDAY = "2026-11-02";

const PLAN_STATUS = { due: "planDue", overdue: "planOverdue", met: "planMet", skipped: "planSkipped" } as const satisfies Record<CalendarPlan["status"], string>;
const REASONS = {
  too_few_receipts: "calendar.reasons.too_few_receipts",
  irregular_receipts: "calendar.reasons.irregular_receipts",
  no_salary_source: "calendar.reasons.no_salary_source",
  no_cycle: "calendar.reasons.no_cycle",
} as const;

export function calendarQueryKey(basis: PeriodBasis, offset: number) {
  return ["dashboard", "calendar", basis, offset] as const;
}

const MARK_COLOUR = { income: "var(--in-solid)", transfer: "var(--tr-solid)", topUp: "var(--inv-solid)", bill: "var(--warn-solid)" } as const;

interface Marker {
  key: "income" | "transfer" | "topUp" | "bill" | "waiting";
  colour: string;
}

/** The day's markers: income, transfers, top-ups and plans not yet met are filled; waiting rows are outlined. */
export function dayMarkers(day: CalendarDay): Marker[] {
  const marks: Marker[] = [];
  if (day.income_count > 0) marks.push({ key: "income", colour: MARK_COLOUR.income });
  if (day.movement_count > 0) marks.push({ key: "transfer", colour: MARK_COLOUR.transfer });
  if (day.topup_count > 0) marks.push({ key: "topUp", colour: MARK_COLOUR.topUp });
  if (day.plans.some((plan) => plan.status === "due" || plan.status === "overdue")) marks.push({ key: "bill", colour: MARK_COLOUR.bill });
  // Outlined, not filled: the non-colour cue between a waiting row and a bill (apply-markas-analisis D11).
  if (day.waiting_count > 0) marks.push({ key: "waiting", colour: "var(--warn-fg)" });
  return marks;
}

/** The accessible name of a cell: date, spending up to today, waiting rows and markers (cycle-calendar "Kalender Grid"). */
export function dayName(day: CalendarDay, today: string, money: Money, format: Format, t: Translate): string {
  const parts = [`${format.weekdayDate(day.date)}${day.date === today ? `, ${t("day.today")}` : ""}`];
  if (day.date <= today) parts.push(t("calendar.cellSpending", { amount: money(day.spend) }));
  if (day.waiting_count) parts.push(t("calendar.cellWaiting", { count: day.waiting_count }));
  const marks = dayMarkers(day)
    .filter((mark) => mark.key !== "waiting")
    .map((mark) => (mark.key === "bill" ? t("calendar.cellBill", { names: day.plans.map((plan) => plan.label).join(", ") }) : t(`calendar.${mark.key}`)));
  if (marks.length) parts.push(marks.join(", "));
  return `${parts.join(". ")}.`;
}

function weeksOf(days: CalendarDay[]): (CalendarDay | string)[][] {
  const first = days[0].date;
  const last = days[days.length - 1].date;
  const cells: (CalendarDay | string)[] = [];
  for (let index = mondayIndex(first); index > 0; index--) cells.push(addDays(first, -index));
  cells.push(...days);
  let next = addDays(last, 1);
  while (cells.length % 7) {
    cells.push(next);
    next = addDays(next, 1);
  }
  const weeks = [];
  for (let index = 0; index < cells.length; index += 7) weeks.push(cells.slice(index, index + 7));
  return weeks;
}

function defaultDay(data: CalendarResponse): string {
  return data.window.is_current ? data.window.today : data.window.end;
}

/**
 * Kalender (apply-markas-analisis D11 to D13): the period's spending as the hero, a Monday-first grid with compact
 * amounts, 6 px markers and a 4 px heat strip from the server's level, and the day panel with its transactions and
 * the bills until payday. The phone keeps the whole grid.
 */
export function KalenderTab({
  basis,
  offset,
  day,
  onSelectDay,
  onWindowLabel,
}: {
  basis: PeriodBasis;
  offset: number;
  day: string | null;
  onSelectDay: (day: string) => void;
  onWindowLabel: (label: string) => void;
}) {
  const t = useTranslations("analysis");
  const money = useMoney();
  const format = useFormat();
  const query = useQuery<CalendarResponse>({
    queryKey: calendarQueryKey(basis, offset),
    queryFn: () => api.get(`/dashboard/calendar?timeframe=${basis}&cycle_offset=${offset}`),
  });
  const label = query.data?.window.label;
  useEffect(() => {
    if (label) onWindowLabel(label);
  }, [label, onWindowLabel]);

  if (query.isPending) {
    return (
      <div className="cols an-cols">
        <div className="stack" role="status" aria-busy="true">
          <div>
            <span className="skel is-label" style={{ width: 200 }} />
            <span className="skel is-hero" style={{ width: 240 }} />
          </div>
          <div className="cal is-skeleton" aria-hidden="true">
            {Array.from({ length: 35 }, (_, index) => <span key={index} className="cal-cell is-out" />)}
          </div>
        </div>
      </div>
    );
  }
  if (query.isError) {
    return <ErrorState message={t("states.calendarError")} onRetry={() => query.refetch()} />;
  }
  const data = query.data;
  if (!data.days.length) return <ErrorState message={t("states.calendarError")} onRetry={() => query.refetch()} />;
  const inWindow = day && day >= data.window.start && day <= data.window.end ? day : defaultDay(data);
  const selected = data.days.find((entry) => entry.date === inWindow) ?? data.days[data.days.length - 1];
  const { window, expected_salary: expected } = data;
  let line = window.is_current && window.day_index ? t("calendar.dayOf", { day: format.integer(window.day_index), total: format.integer(window.days_total) }) : t("calendar.daysTotal", { count: window.days_total });
  if (window.is_current && window.basis === "cycle") {
    const reasonKey = expected.reason && expected.reason in REASONS ? REASONS[expected.reason as keyof typeof REASONS] : "calendar.reasons.unknown";
    const reason = t(reasonKey);
    line += expected.available && expected.date
      ? ` · ${t("calendar.untilPayday", { count: dayDiff(window.today, expected.date), date: format.shortDate(expected.date) })}`
      : ` · ${t("calendar.paydayUnknown", { reason })}`;
  }

  return (
    <div className="cols an-cols">
      <div className="stack">
        <HeroFigure id="an-cal-hero" label={t("calendar.heroLabel")} figure={<MaskedMoney text={money(data.totals.spend)} />} line={line} />
        <CalendarGrid data={data} selected={selected.date} onSelect={onSelectDay} money={money} />
      </div>
      <Panel label={t("day.panel")}>
        <DayPanel day={selected} data={data} money={money} />
        {window.is_current && <Upcoming data={data} money={money} onSelect={onSelectDay} />}
      </Panel>
    </div>
  );
}

function CalendarGrid({ data, selected, onSelect, money }: { data: CalendarResponse; selected: string; onSelect: (day: string) => void; money: Money }) {
  const t = useTranslations("analysis");
  const format = useFormat();
  const { hideBalances } = useAppCtx();
  const narrow = useMediaQuery(NARROW_QUERY);
  const gridRef = useRef<HTMLDivElement>(null);
  const moved = useRef(false);
  const weeks = useMemo(() => weeksOf(data.days), [data.days]);
  const first = data.days[0].date;
  const last = data.days[data.days.length - 1].date;
  const today = data.window.today;
  const [focused, setFocused] = useState(selected);

  useEffect(() => setFocused(selected), [selected]);
  useEffect(() => {
    if (!moved.current) return;
    moved.current = false;
    gridRef.current?.querySelector<HTMLElement>(`[data-day="${focused}"]`)?.focus();
  }, [focused]);

  // Arrow keys, Home, End, PageUp and PageDown move the selected day; they act only with focus in the grid, so
  // they work whatever the single-key shortcut setting is (apply-markas-analisis D20).
  const onKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const steps: Record<string, number> = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7, PageUp: -7, PageDown: 7 };
    let next: string | null = null;
    if (event.key in steps) next = addDays(focused, steps[event.key]);
    else if (event.key === "Home") next = addDays(focused, -mondayIndex(focused));
    else if (event.key === "End") next = addDays(focused, 6 - mondayIndex(focused));
    else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelect(focused);
      return;
    }
    if (next === null) return;
    event.preventDefault();
    if (next < first) next = first;
    if (next > last) next = last;
    moved.current = true;
    setFocused(next);
    onSelect(next);
  };

  const thresholds = data.heat_thresholds.length === 4 ? data.heat_thresholds : null;
  const periodName = data.window.label;
  return (
    <section aria-label={t("calendar.gridLabel", { period: periodName })}>
      <div ref={gridRef} className="cal" role="grid" aria-label={t("calendar.gridLabel", { period: periodName })} onKeyDown={onKey}>
        <div role="row" className="cal-row">
          {Array.from({ length: 7 }, (_, index) => (
            <div key={index} role="columnheader" className="dow">
              {format.weekdayShort(addDays(MONDAY, index))}
            </div>
          ))}
        </div>
        {weeks.map((week, row) => (
          <div role="row" key={row} className="cal-row">
            {week.map((entry) => {
              if (typeof entry === "string") {
                return (
                  <div key={entry} className="cal-cell is-out" aria-hidden="true">
                    <span className="d">{Number(entry.slice(8)) === 1 ? format.shortDate(entry) : Number(entry.slice(8))}</span>
                  </div>
                );
              }
              const dayNumber = Number(entry.date.slice(8));
              const marks = dayMarkers(entry);
              const past = entry.date <= today;
              return (
                <div
                  key={entry.date}
                  role="gridcell"
                  data-day={entry.date}
                  tabIndex={entry.date === focused ? 0 : -1}
                  aria-selected={entry.date === selected}
                  aria-current={entry.date === today ? "date" : undefined}
                  aria-label={dayName(entry, today, money, format, t)}
                  className={cn("cal-cell", entry.date === today && "is-today")}
                  onClick={() => {
                    setFocused(entry.date);
                    onSelect(entry.date);
                  }}
                  onFocus={() => setFocused(entry.date)}
                >
                  <span className="d" aria-hidden="true">{dayNumber === 1 || entry.date === first ? format.shortDate(entry.date) : dayNumber}</span>
                  <span className="t-amt cal-amt" aria-hidden="true">
                    {past && entry.spend > 0 ? format.compactMoney(entry.spend, { narrow, hidden: hideBalances }) : ""}
                  </span>
                  <span className="marks" aria-hidden="true">
                    {marks.map((mark) => (
                      <span key={mark.key} className={cn("mark", mark.key === "waiting" && "is-outline")} style={mark.key === "waiting" ? undefined : { background: mark.colour }} />
                    ))}
                  </span>
                  {past && entry.heat > 0 && <span className="heat" style={{ opacity: Math.min(5, entry.heat) * 0.2 }} aria-hidden="true" />}
                </div>
              );
            })}
          </div>
        ))}
      </div>
      <div className="chart-foot cal-legend">
        {(["income", "transfer", "topUp", "bill", "waiting"] as const).map((key) => (
          <span key={key} className="t-meta leg">
            <span className={cn("mark", key === "waiting" && "is-outline")} style={key === "waiting" ? undefined : { background: MARK_COLOUR[key] }} aria-hidden="true" />
            {t(`calendar.${key}`)}
          </span>
        ))}
        {thresholds && (
          <span className="t-meta leg heat-legend">
            <span>{t("calendar.heatLegend")}</span>
            <span>{t("calendar.heatFrom", { amount: money(thresholds[0]) })}</span>
            <span className="heat-steps" aria-hidden="true">
              {[1, 2, 3, 4, 5].map((level) => <span key={level} style={{ opacity: level * 0.2 }} />)}
            </span>
            <span>{t("calendar.heatAbove", { amount: money(thresholds[3]) })}</span>
          </span>
        )}
      </div>
    </section>
  );
}

function localBounds(day: string): { from: string; to: string } {
  return { from: `${day}T00:00:00+07:00`, to: `${day}T23:59:59.999+07:00` };
}

function DayPanel({ day, data, money }: { day: CalendarDay; data: CalendarResponse; money: Money }) {
  const t = useTranslations("analysis");
  const format = useFormat();
  const labels = useRowLabels();
  const { record } = useTransactionUndo();
  const [openId, setOpenId] = useState<string | null>(null);
  const today = data.window.today;
  const future = day.date > today;
  const bounds = localBounds(day.date);
  const rows = useQuery<{ transactions: LedgerRow[]; total: number }>({
    queryKey: ["transactions", "day", day.date],
    queryFn: () => api.get(`/transactions?from_date=${encodeURIComponent(bounds.from)}&to_date=${encodeURIComponent(bounds.to)}&logical_movements=true&limit=50`),
    enabled: !future,
  });
  const relative = day.date === today ? ` · ${t("day.today")}` : day.date === addDays(today, -1) ? ` · ${t("day.yesterday")}` : "";

  let list;
  if (future) list = <p className="t-body muted day-empty">{t("day.future")}</p>;
  else if (rows.isPending) list = <div role="status" aria-busy="true"><span className="skel is-line" /><span className="skel is-line" /></div>;
  else if (rows.isError) list = <ErrorState message={t("day.loadFailed")} onRetry={() => rows.refetch()} />;
  else if (rows.data.transactions.length === 0) list = <p className="t-body muted day-empty">{t("day.none")}</p>;
  else {
    list = (
      <ul className="list day-rows">
        {rows.data.transactions.map((row) => (
          <li key={row.id}>
            <ListRow
              rowKey={row.id}
              layout="stacked"
              onClick={() => setOpenId(row.id)}
              title={rowTitle(row, labels)}
              meta={`${format.time(row.date)} · ${rowPhoneLine(row, labels, format).text}`}
              amount={<RowAmount row={row} money={money} />}
            />
          </li>
        ))}
      </ul>
    );
  }

  return (
    <PanelBlock>
      <SectionHeader label={`${format.weekdayDate(day.date)}${relative}`} />
      <p className="t-fig"><MaskedMoney text={money(future ? 0 : day.spend)} /></p>
      <p className="t-meta">{day.date === today ? t("day.spentToday") : t("day.spentThatDay")}</p>
      {day.waiting.length > 0 && (
        <div className="day-group">
          <h3 className="t-label">{t("day.waiting", { count: day.waiting.length })}</h3>
          <ul className="list">
            {day.waiting.map((row) => (
              <li key={row.transaction_id}>
                <ListRow href={`/review?item=${row.transaction_id}`} layout="stacked" title={row.label} meta={t("day.answerInReview")} amount={<MaskedMoney text={money(row.amount)} className="t-amt" />} />
              </li>
            ))}
          </ul>
        </div>
      )}
      {day.plans.length > 0 && (
        <div className="day-group">
          {day.plans.map((plan) => (
            <p key={`${plan.rule_id}-${plan.status}`} className="t-body day-plan">
              {t.rich("day.planned", { name: plan.label, amount: () => <MaskedMoney text={money(plan.amount)} className="t-amt" /> })}
              <span className="t-meta"> · {t(`day.${PLAN_STATUS[plan.status]}`)}</span>
            </p>
          ))}
        </div>
      )}
      {list}
      {!future && (
        <div className="panel-actions">
          <Link className="ws-btn" href={ledgerHref({ from: bounds.from, to: bounds.to })}>{t("day.openLedger")}</Link>
        </div>
      )}
      <TransactionDialog
        transactionId={openId}
        onClose={() => {
          const id = openId;
          setOpenId(null);
          if (id) document.querySelector<HTMLElement>(`[data-row="${CSS.escape(id)}"]`)?.focus();
        }}
        record={record}
      />
    </PanelBlock>
  );
}

function Upcoming({ data, money, onSelect }: { data: CalendarResponse; money: Money; onSelect: (day: string) => void }) {
  const t = useTranslations("analysis.upcoming");
  const format = useFormat();
  const toSalary = data.upcoming_basis === "expected_salary";
  return (
    <PanelBlock>
      <SectionHeader label={toSalary ? t("beforePayday") : t("toPeriodEnd")} />
      <p className="t-meta">{t("unpaid")}</p>
      {data.upcoming.length === 0 ? (
        <p className="t-body muted day-empty">{t("none")}</p>
      ) : (
        <>
          <ul className="list">
            {data.upcoming.map((plan) => (
              <li key={`${plan.rule_id}-${plan.date}`}>
                <ListRow
                  onClick={() => onSelect(plan.date)}
                  layout="stacked"
                  title={plan.label}
                  meta={format.shortDate(plan.date)}
                  amount={<MaskedMoney text={money(plan.amount)} className="t-amt" />}
                />
              </li>
            ))}
          </ul>
          <div className="up-total">
            <span className="t-body">{t("total")}</span>
            <MaskedMoney text={money(data.upcoming_total)} className="t-amt" />
          </div>
        </>
      )}
    </PanelBlock>
  );
}
