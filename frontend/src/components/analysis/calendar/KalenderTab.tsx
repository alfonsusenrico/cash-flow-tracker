"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { rowTitle } from "@/components/beranda/activity";
import { TransactionDrawer } from "@/components/transactions/TransactionDrawer";
import type { LedgerRow } from "@/components/transactions/types";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { Icon } from "@/components/ui/Icon";
import { Card, CardHeader, ErrorState, Pill, Skeleton, type Tone } from "@/components/workspace/primitives";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { api } from "@/lib/api";
import { ledgerHref } from "@/lib/ledgerFilters";
import { cn } from "@/lib/utils";
import { addDays, compactMoney, dayDiff, dayMonth, expectedReason, longDay, mondayIndex, shortDay, weekdayShort } from "../format";
import styles from "../analysis.module.css";
import type { CalendarDay, CalendarPlan, CalendarResponse, PeriodBasis } from "../types";

type Money = (amount: number) => string;

const HEAT = [null, styles.h1, styles.h2, styles.h3, styles.h4, styles.h5];
const WEEKDAYS = ["Sen", "Sel", "Rab", "Kam", "Jum", "Sab", "Min"];

const PLAN_STATUS: Record<CalendarPlan["status"], { tone: Tone; text: string }> = {
  due: { tone: "neutral", text: "Belum dibayar" },
  overdue: { tone: "warn", text: "Terlambat" },
  met: { tone: "ok", text: "Sudah dibayar" },
  skipped: { tone: "neutral", text: "Dilewati" },
};

export function calendarQueryKey(basis: PeriodBasis, offset: number) {
  return ["dashboard", "calendar", basis, offset] as const;
}

/** The accessible name of a day cell: date, spending, waiting rows and plans (cycle-calendar "Kalender Grid"). */
export function dayName(day: CalendarDay, money: Money, today: string): string {
  const parts = [`${longDay(day.date)}${day.date === today ? ", hari ini" : ""}`];
  const facts: string[] = [];
  if (day.date <= today) facts.push(`belanja ${money(day.spend)}`);
  if (day.waiting_count) facts.push(`${day.waiting_count} menunggu jawaban`);
  if (day.income_count) facts.push(`masuk ${money(day.income_total)}`);
  if (day.movement_count) facts.push(`pindah saldo ${money(day.movement_total)}`);
  if (day.topup_count) facts.push(`top up ${money(day.topup_total)}`);
  for (const plan of day.plans) facts.push(`tagihan ${plan.label}`);
  return facts.length ? `${parts[0]}: ${facts.join(", ")}` : parts[0];
}

/** "Sampai Rp 50.000" … "Di atas Rp 400.000" for the five heat levels (OD47). */
export function heatLevels(thresholds: number[], money: Money): { level: number; text: string }[] {
  if (thresholds.length !== 4) return [];
  return [
    { level: 1, text: `sampai ${money(thresholds[0])}` },
    { level: 2, text: `${money(thresholds[0])} – ${money(thresholds[1])}` },
    { level: 3, text: `${money(thresholds[1])} – ${money(thresholds[2])}` },
    { level: 4, text: `${money(thresholds[2])} – ${money(thresholds[3])}` },
    { level: 5, text: `di atas ${money(thresholds[3])}` },
  ];
}

function defaultDay(data: CalendarResponse): string {
  const { window } = data;
  return window.is_current ? window.today : window.end;
}

export function KalenderTab({
  basis,
  offset,
  money,
  day,
  onSelectDay,
  onLabel,
}: {
  basis: PeriodBasis;
  offset: number;
  money: Money;
  day: string | null;
  onSelectDay: (day: string) => void;
  onLabel: (label: string) => void;
}) {
  const rootRef = useRef<HTMLDivElement>(null);
  const phone = useWidthAtMost(rootRef, 760);
  const query = useQuery<CalendarResponse>({
    queryKey: calendarQueryKey(basis, offset),
    queryFn: () => api.get(`/dashboard/calendar?timeframe=${basis}&cycle_offset=${offset}`),
  });
  useEffect(() => {
    if (query.data) onLabel(query.data.window.label);
  }, [query.data, onLabel]);

  let body;
  if (query.isPending) {
    body = <div className={styles.stack} aria-busy="true"><Skeleton style={{ height: 76 }} /><Skeleton style={{ height: 420 }} /></div>;
  } else if (query.isError) {
    body = <ErrorState message="Kalender periode ini belum bisa dimuat." onRetry={() => query.refetch()} />;
  } else {
    const data = query.data;
    const inWindow = day && day >= data.window.start && day <= data.window.end ? day : defaultDay(data);
    const selected = data.days.find((entry) => entry.date === inWindow) ?? data.days[data.days.length - 1];
    body = (
      <>
        <ProgressBand data={data} money={money} />
        {phone ? (
          <div className={styles.stack}>
            <WeekStrip data={data} selected={selected.date} onSelect={onSelectDay} money={money} />
            <DaySheet day={selected} data={data} money={money} />
            <UpcomingCard data={data} money={money} />
          </div>
        ) : (
          <div className={styles.grid}>
            <CalendarGrid data={data} selected={selected.date} onSelect={onSelectDay} money={money} />
            <div className={styles.stack}>
              <DaySheet day={selected} data={data} money={money} />
              <UpcomingCard data={data} money={money} />
            </div>
          </div>
        )}
      </>
    );
  }
  return <div ref={rootRef} className={styles.stack}>{body}</div>;
}

function ProgressBand({ data, money }: { data: CalendarResponse; money: Money }) {
  const { window, expected_salary: expected } = data;
  let headline = `${window.label} · ${window.days_total} hari`;
  if (window.is_current && window.day_index) headline = `Hari ke-${window.day_index} dari ${window.days_total}`;
  let salary: string | null = null;
  if (window.is_current && basis(window)) {
    salary = expected.available && expected.date
      ? `${dayDiff(window.today, expected.date)} hari lagi sampai gaji ${dayMonth(expected.date)}`
      : `Tanggal gaji berikutnya belum bisa diperkirakan: ${expectedReason(expected.reason)}.`;
  }
  return (
    <Card className={styles.band} label="Kemajuan periode">
      <div className={styles.bandTop}>
        <b>{headline}</b>
        {salary && <span className={styles.caption}>{salary}</span>}
        <span className={styles.grow} />
        <span className={styles.bandFigure}><span className={styles.caption}>Belanja periode ini</span><b>{money(data.totals.spend)}</b></span>
      </div>
      <div className={styles.ticks} aria-hidden="true">
        {data.days.map((entry) => (
          <i key={entry.date} className={cn(entry.date < window.today && styles.past, entry.date === window.today && styles.now)} />
        ))}
      </div>
    </Card>
  );
}

function basis(window: CalendarResponse["window"]): boolean {
  return window.basis === "cycle";
}

function weeksOf(days: CalendarDay[]): (CalendarDay | null)[][] {
  const cells: (CalendarDay | null)[] = [...Array(mondayIndex(days[0].date)).fill(null), ...days];
  while (cells.length % 7) cells.push(null);
  const weeks = [];
  for (let index = 0; index < cells.length; index += 7) weeks.push(cells.slice(index, index + 7));
  return weeks;
}

function CalendarGrid({ data, selected, onSelect, money }: { data: CalendarResponse; selected: string; onSelect: (day: string) => void; money: Money }) {
  const [focused, setFocused] = useState(selected);
  const gridRef = useRef<HTMLDivElement>(null);
  const moved = useRef(false);
  const weeks = useMemo(() => weeksOf(data.days), [data.days]);
  const first = data.days[0].date;
  const last = data.days[data.days.length - 1].date;
  const today = data.window.today;

  useEffect(() => setFocused(selected), [selected]);
  useEffect(() => {
    if (!moved.current) return;
    moved.current = false;
    gridRef.current?.querySelector<HTMLButtonElement>(`[data-day="${focused}"]`)?.focus();
  }, [focused]);

  const onKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const steps: Record<string, number> = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7, PageUp: -7, PageDown: 7 };
    let next: string | null = null;
    if (event.key in steps) next = addDays(focused, steps[event.key]);
    else if (event.key === "Home") next = addDays(focused, -mondayIndex(focused));
    else if (event.key === "End") next = addDays(focused, 6 - mondayIndex(focused));
    if (next === null) return;
    event.preventDefault();
    if (next < first) next = first;
    if (next > last) next = last;
    moved.current = true;
    setFocused(next);
  };

  const levels = heatLevels(data.heat_thresholds, money);
  return (
    <Card labelledBy="calendar-heading">
      <CardHeader id="calendar-heading" icon="calendar" title="Kalender" caption="Belanja per hari, dengan masuk, pindah saldo, top up dan tagihan." />
      <div className={styles.calHead} aria-hidden="true">{WEEKDAYS.map((name) => <span key={name}>{name}</span>)}</div>
      <div ref={gridRef} role="grid" aria-labelledby="calendar-heading" className={styles.calGrid} onKeyDown={onKey}>
        {weeks.map((week, row) => (
          <div role="row" key={row} className={styles.calRow}>
            {week.map((entry, column) => (
              entry ? (
                <div role="gridcell" key={entry.date} className={styles.cell} aria-selected={entry.date === selected}>
                  <button
                    type="button"
                    data-day={entry.date}
                    tabIndex={entry.date === focused ? 0 : -1}
                    aria-label={dayName(entry, money, today)}
                    className={cn(styles.day, entry.date <= today && HEAT[entry.heat], entry.date === today && styles.isToday)}
                    aria-current={entry.date === today ? "date" : undefined}
                    onClick={() => onSelect(entry.date)}
                    onFocus={() => setFocused(entry.date)}
                  >
                    <span className={styles.dayNumber} aria-hidden="true">
                      {Number(entry.date.slice(8))}{Number(entry.date.slice(8)) === 1 || entry.date === first ? ` ${shortDay(entry.date).split(" ")[1]}` : ""}
                      {entry.waiting_count > 0 && <span className={styles.qtag}><Icon name="help-circle" />{entry.waiting_count}</span>}
                    </span>
                    {entry.date <= today && entry.spend > 0 && <span className={styles.spend} aria-hidden="true">{money(entry.spend)}</span>}
                    <span className={styles.marks} aria-hidden="true">
                      {entry.income_count > 0 && <span className={cn(styles.mark, styles.markIn)}><Icon name="arrow-down-left" />Masuk</span>}
                      {entry.movement_count > 0 && <span className={cn(styles.mark, styles.markTr)}><Icon name="arrows-right-left" />Pindah saldo</span>}
                      {entry.topup_count > 0 && <span className={cn(styles.mark, styles.markInv)}><Icon name="trending-up" />Top up</span>}
                      {entry.plans.slice(0, 1).map((plan) => <span key={plan.rule_id} className={cn(styles.mark, styles.markBill)}><Icon name="repeat" />{plan.label}</span>)}
                    </span>
                  </button>
                </div>
              ) : (
                <div role="gridcell" key={`blank-${row}-${column}`} className={styles.blank} aria-hidden="true" />
              )
            ))}
          </div>
        ))}
      </div>
      <div className={styles.legend}>
        {levels.length > 0 ? (
          <ul className={styles.levels} aria-label="Tingkat belanja per hari">
            {levels.map((level) => <li key={level.level}><i className={HEAT[level.level] ?? undefined} aria-hidden="true" />{level.text}</li>)}
          </ul>
        ) : <span className={styles.caption}>Belum ada belanja di periode ini.</span>}
        <span className={cn(styles.mark, styles.markIn)}><Icon name="arrow-down-left" />Masuk</span>
        <span className={cn(styles.mark, styles.markTr)}><Icon name="arrows-right-left" />Pindah saldo</span>
        <span className={cn(styles.mark, styles.markInv)}><Icon name="trending-up" />Top up</span>
        <span className={cn(styles.mark, styles.markBill)}><Icon name="repeat" />Tagihan</span>
        <span className={styles.qtag}><Icon name="help-circle" />Menunggu jawaban</span>
      </div>
      <p className={styles.caption} style={{ marginTop: 8 }}>Jumlah belanja tertulis di tiap hari. Tombol panah memindahkan hari, Enter membuka rinciannya.</p>
    </Card>
  );
}

function WeekStrip({ data, selected, onSelect, money }: { data: CalendarResponse; selected: string; onSelect: (day: string) => void; money: Money }) {
  const monday = addDays(selected, -mondayIndex(selected));
  const week = Array.from({ length: 7 }, (_, index) => addDays(monday, index));
  const byDate = new Map(data.days.map((entry) => [entry.date, entry]));
  const first = data.days[0].date;
  const last = data.days[data.days.length - 1].date;
  const today = data.window.today;
  const previous = addDays(monday, -7);
  const next = addDays(monday, 7);
  const clampTo = (value: string) => (value < first ? first : value > last ? last : value);
  return (
    <div className={styles.weekStrip} role="group" aria-label={`Minggu ${shortDay(week[0])} – ${shortDay(week[6])}`}>
      <button type="button" className="ws-icon-btn" aria-label="Minggu sebelumnya" disabled={addDays(monday, -1) < first} onClick={() => onSelect(clampTo(previous))}>
        <Icon name="chevron-left" />
      </button>
      {week.map((date) => {
        const entry = byDate.get(date);
        return (
          <button
            key={date}
            type="button"
            className={cn(styles.weekDay, entry && date <= today && HEAT[entry.heat])}
            aria-pressed={date === selected}
            disabled={!entry}
            aria-label={entry ? dayName(entry, money, today) : `${longDay(date)}, di luar periode`}
            onClick={() => onSelect(date)}
          >
            <span aria-hidden="true">{weekdayShort(date)}</span>
            <b aria-hidden="true">{Number(date.slice(8))}</b>
            {entry && date <= today && <small aria-hidden="true">{entry.spend ? compactMoney(entry.spend, money) : "–"}</small>}
          </button>
        );
      })}
      <button type="button" className="ws-icon-btn" aria-label="Minggu berikutnya" disabled={next > last} onClick={() => onSelect(clampTo(next))}>
        <Icon name="chevron-right" />
      </button>
    </div>
  );
}

function localBounds(day: string): { from: string; to: string } {
  return { from: `${day}T00:00:00+07:00`, to: `${day}T23:59:59.999+07:00` };
}

function DaySheet({ day, data, money }: { day: CalendarDay; data: CalendarResponse; money: Money }) {
  const { record } = useTransactionUndo();
  const [openId, setOpenId] = useState<string | null>(null);
  const future = day.date > data.window.today;
  const bounds = localBounds(day.date);
  const rows = useQuery<{ transactions: LedgerRow[]; total: number }>({
    queryKey: ["transactions", "day", day.date],
    queryFn: () => api.get(`/transactions?from_date=${encodeURIComponent(bounds.from)}&to_date=${encodeURIComponent(bounds.to)}&logical_movements=true&limit=50`),
    enabled: !future,
  });
  return (
    <Card labelledBy="day-sheet-heading">
      <CardHeader
        id="day-sheet-heading"
        title={longDay(day.date)}
        caption={`${day.date === data.window.today ? "Hari ini · " : ""}hari ke-${dayDiff(data.window.start, day.date) + 1} periode · belanja ${money(day.spend)}`}
      />
      {day.waiting.length > 0 && (
        <>
          <h3 className={styles.sheetHeading}><Icon name="help-circle" />Menunggu jawaban · {day.waiting.length}</h3>
          <div className={styles.sheetRows}>
            {day.waiting.map((row) => (
              <Link key={row.transaction_id} className={styles.sheetRow} href={`/review?item=${row.transaction_id}`}>
                <span><b>{row.label}</b><span>{row.section === "awaiting" || row.section === "ambiguous" ? "Pindah saldo belum lengkap" : "Perlu jawaban"} · jawab di Tinjau</span></span>
                <span className={styles.amount}>{money(row.amount)}</span>
              </Link>
            ))}
          </div>
        </>
      )}
      {day.plans.length > 0 && (
        <>
          <h3 className={styles.sheetHeading}><Icon name="repeat" />Tagihan</h3>
          <div className={styles.sheetRows}>
            {day.plans.map((plan) => (
              <div key={`${plan.rule_id}-${plan.status}`} className={styles.sheetRow} style={{ cursor: "default" }}>
                <span><b>{plan.label}</b><span>{money(plan.amount)}</span></span>
                <Pill tone={PLAN_STATUS[plan.status].tone}>{PLAN_STATUS[plan.status].text}</Pill>
              </div>
            ))}
          </div>
        </>
      )}
      <h3 className={styles.sheetHeading}><Icon name="receipt" />Transaksi</h3>
      {future ? (
        <p className={styles.caption}>Hari ini belum terjadi.</p>
      ) : rows.isPending ? (
        <Skeleton style={{ height: 88 }} />
      ) : rows.isError ? (
        <ErrorState message="Transaksi hari ini belum bisa dimuat." onRetry={() => rows.refetch()} />
      ) : rows.data.transactions.length === 0 ? (
        <p className={styles.caption}>Tidak ada transaksi.</p>
      ) : (
        <div className={styles.sheetRows}>
          {rows.data.transactions.map((row) => (
            <button key={row.id} type="button" className={styles.sheetRow} onClick={() => setOpenId(row.id)}>
              <span><b>{rowTitle(row)}</b><span>{row.account_name}{row.category_name ? ` · ${row.category_name}` : ""}</span></span>
              <span className={styles.amount}>{row.type === "income" ? "+" : "−"}{money(row.amount)}</span>
            </button>
          ))}
        </div>
      )}
      {!future && (
        <p style={{ marginTop: 10 }}>
          <Link className="ws-linkbtn" href={ledgerHref({ from: bounds.from, to: bounds.to })}>Lihat di Transaksi</Link>
        </p>
      )}
      <TransactionDrawer transactionId={openId} onClose={() => setOpenId(null)} record={record} />
    </Card>
  );
}

function UpcomingCard({ data, money }: { data: CalendarResponse; money: Money }) {
  if (!data.window.is_current) return null;
  const toSalary = data.upcoming_basis === "expected_salary";
  return (
    <Card labelledBy="upcoming-heading">
      <CardHeader
        id="upcoming-heading"
        icon="repeat"
        title={toSalary ? "Sebelum gaji berikutnya" : "Sampai akhir periode"}
        caption={toSalary && data.expected_salary.date ? `Tagihan sampai ${dayMonth(data.expected_salary.date)}.` : "Tagihan yang belum dibayar sampai akhir periode ini."}
      />
      {data.upcoming.length === 0 ? (
        <p className={styles.caption}>Tidak ada tagihan yang belum dibayar.</p>
      ) : (
        <>
          <ol className={styles.timeline}>
            {data.upcoming.map((plan) => (
              <li key={`${plan.rule_id}-${plan.date}`}>
                <span><b>{shortDay(plan.date)}</b><span>{dayDiff(data.window.today, plan.date) === 0 ? "hari ini" : `${dayDiff(data.window.today, plan.date)} hari lagi`}</span></span>
                <span><Link className="ws-linkbtn" href="/goals">{plan.label}</Link></span>
                <span>{money(plan.amount)}</span>
              </li>
            ))}
          </ol>
          <div className={styles.total}><span>Total</span><span>{money(data.upcoming_total)}</span></div>
        </>
      )}
    </Card>
  );
}
