"use client";

import { useEffect, useId, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { ChartTable, ChartTableButton } from "@/components/charts/ChartTable";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { RadioSegments } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import type { SpendingBucket } from "@/lib/monthlyReport";
import { todayIso } from "./format";
import { PILLAR_TERM } from "./KakeiboRows";
import type { DailyCategoryAmount, SpendingTotals } from "./types";

type Money = (amount: number) => string;

/** One day's Pengeluaran (apply-markas-analisis D31) and, from a current API, its split (improve-daily-spending-detail D2). */
export interface DailyPoint {
  date: string;
  amount: number;
  byPillar?: Record<SpendingBucket, number>;
  byCategory?: DailyCategoryAmount[];
}

export type DailyTableMode = "pillar" | "category";

const HEIGHT = 200;
const BOTTOM = 28;
const TOP = 8;
/** A 12 px Inter date label is about 6.5 px per character; two labels closer than this would touch. */
const LABEL_GAP = 4;
/** Per kategori shows this many of the period's largest categories; the rest share one "Lainnya" column. */
const TOP_CATEGORIES = 5;
const PILLAR_COLUMNS: SpendingBucket[] = ["need", "want", "culture", "unexpected", "unclassified"];
export const TABLE_MODE_KEY = "analisis-daily-table";

/** The scale's top: whole Rp 500.000 steps, or Rp 100.000 steps below that, so a quiet period is not flattened. */
export function niceMax(max: number): number {
  const step = max < 500_000 ? 100_000 : 500_000;
  return Math.max(step, Math.ceil(max / step) * step);
}

/** The prototype's bar geometry: a 4 px inset each side, bars `max(4, floor(step) − 4)` wide, centred in their step. */
export function barGeometry(width: number, count: number) {
  const step = (width - 8) / Math.max(1, count);
  const barWidth = Math.max(4, Math.floor(step) - 4);
  return { step, barWidth, x: (index: number) => 4 + index * step + (step - barWidth) / 2 };
}

/**
 * Date labels every `ceil(n/8)` bars (`ceil(n/4)` below 400 px) plus today's; a regular label that would touch
 * today's is skipped, so no two labels overlap (apply-markas-analisis D7).
 */
export function dateLabelIndexes(texts: string[], width: number, todayIndex: number): number[] {
  const count = texts.length;
  const every = Math.ceil(count / (width < 400 ? 4 : 8));
  const { step } = barGeometry(width, count);
  const centre = (index: number) => 4 + index * step + step / 2;
  const half = (index: number) => (texts[index].length * 6.5) / 2;
  const chosen: number[] = [];
  const touches = (a: number, b: number) => Math.abs(centre(a) - centre(b)) < half(a) + half(b) + LABEL_GAP;
  for (let index = 0; index < count; index += every) {
    if (todayIndex >= 0 && index !== todayIndex && touches(index, todayIndex)) continue;
    if (chosen.length && touches(index, chosen[chosen.length - 1])) continue;
    chosen.push(index);
  }
  if (todayIndex >= 0 && !chosen.includes(todayIndex)) chosen.push(todayIndex);
  return chosen.sort((a, b) => a - b);
}

/** A category's identity across days: its id, or its name when it has none (spending without a category). */
function categoryKey(entry: DailyCategoryAmount): string {
  return entry.category_id ?? `name:${entry.name}`;
}

/**
 * Per kategori's columns (improve-daily-spending-detail D3): the period's five largest categories by summed
 * Pengeluaran, largest first, and whether the remaining ones need a "Lainnya" column.
 */
function topCategories(days: DailyPoint[]): { columns: { key: string; name: string }[]; hasOther: boolean } {
  const totals = new Map<string, { name: string; total: number }>();
  for (const day of days) {
    for (const entry of day.byCategory ?? []) {
      const key = categoryKey(entry);
      const current = totals.get(key);
      totals.set(key, { name: current?.name ?? entry.name, total: (current?.total ?? 0) + entry.amount });
    }
  }
  const ranked = [...totals.entries()]
    .sort(([, a], [, b]) => b.total - a.total || a.name.localeCompare(b.name))
    .map(([key, { name }]) => ({ key, name }));
  return { columns: ranked.slice(0, TOP_CATEGORIES), hasOther: ranked.length > TOP_CATEGORIES };
}

function useWidth<T extends HTMLElement>(fallback: number) {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(fallback);
  useEffect(() => {
    const element = ref.current;
    if (!element || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[entries.length - 1];
      if (entry) setWidth(Math.max(200, Math.round(entry.contentRect.width)));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return { ref, width };
}

function readTableMode(): DailyTableMode {
  try {
    return window.localStorage.getItem(TABLE_MODE_KEY) === "category" ? "category" : "pillar";
  } catch {
    // Storage can be blocked (private windows, cleared site data); the table then opens per pillar.
    return "pillar";
  }
}

/** The table's column choice, remembered per browser as a convenience. */
function useTableMode(): [DailyTableMode, (next: DailyTableMode) => void] {
  const [mode, setMode] = useState<DailyTableMode>("pillar");
  useEffect(() => {
    // Read after mounting so the server render and the first client render agree.
    setMode(readTableMode());
  }, []);
  const choose = (next: DailyTableMode) => {
    setMode(next);
    try {
      window.localStorage.setItem(TABLE_MODE_KEY, next);
    } catch {
      // Without storage the choice lasts for this visit only.
    }
  };
  return [mode, choose];
}

/**
 * "Pengeluaran harian" (apply-markas-analisis D7, improve-daily-spending-detail D1 and D3): one SVG bar per day up to
 * today, drawn at the column's real width so its text stays 12 px, the highest day in the expense mark colour, today
 * hatched, a legend naming both, and the same values as a table split per pillar or per category. A day's date and
 * amount show above its bar on hover or tap; each bar is a button that opens the day in Kalender.
 */
export function DailySpendingChart({
  days,
  money,
  onOpenDay,
  spendingTotals,
  today = todayIso(),
}: {
  days: DailyPoint[];
  money: Money;
  onOpenDay: (date: string) => void;
  spendingTotals?: SpendingTotals;
  today?: string;
}) {
  const t = useTranslations("analysis.daily");
  const format = useFormat();
  const { hideBalances } = useAppCtx();
  const [asTable, setAsTable] = useState(false);
  const shown = days.filter((day) => day.date <= today);
  const peak = shown.reduce<DailyPoint | null>((best, day) => (day.amount > 0 && (!best || day.amount > best.amount) ? day : best), null);
  const todayEntry = shown.find((day) => day.date === today) ?? null;

  return (
    <section aria-labelledby="an-daily">
      <SectionHeader
        id="an-daily"
        label={t("title")}
        aside={shown.length > 0 ? <ChartTableButton asTable={asTable} onToggle={() => setAsTable((value) => !value)} showTable={t("showTable")} showChart={t("showChart")} /> : undefined}
      />
      {shown.length === 0 ? (
        <p className="caption">{t("noDays")}</p>
      ) : (
        <>
          <ChartTable
            asTable={asTable}
            chart={<Bars days={shown} peak={peak} today={today} money={money} hidden={hideBalances} onOpenDay={onOpenDay} />}
            table={<DailyTable days={shown} peak={peak} today={today} money={money} />}
          />
          <div className="chart-foot">
            {peak && (
              <span className="t-meta leg">
                <span className="leg-sw" style={{ background: "var(--out-solid)" }} aria-hidden="true" />
                {t.rich("peak", { date: format.shortDate(peak.date), amount: () => <MaskedMoney text={money(peak.amount)} className="t-amt" /> })}
              </span>
            )}
            {todayEntry && (
              <span className="t-meta leg">
                <span className="leg-sw is-hatch" aria-hidden="true" />
                {t.rich("today", { amount: () => <MaskedMoney text={money(todayEntry.amount)} className="t-amt" /> })}
              </span>
            )}
            <span className="t-meta">{t("scope")}</span>
            {spendingTotals && spendingTotals.reimbursed > 0 && (
              <span className="t-meta">
                {t.rich("reimbursedLine", {
                  gross: () => <MaskedMoney text={money(spendingTotals.gross)} className="t-amt" />,
                  reimbursed: () => <MaskedMoney text={`−${money(spendingTotals.reimbursed)}`} className="t-amt" />,
                })}
              </span>
            )}
          </div>
        </>
      )}
    </section>
  );
}

interface Column {
  key: string;
  label: string;
  value: (day: DailyPoint) => number;
}

/**
 * The chart's table (improve-daily-spending-detail D3): per pillar or per category with the day's total, the date
 * column fixed while the rest scrolls inside its box. An API without the split gives the date and total only.
 */
function DailyTable({ days, peak, today, money }: { days: DailyPoint[]; peak: DailyPoint | null; today: string; money: Money }) {
  const t = useTranslations("analysis.daily");
  const terms = useTranslations("terms");
  const format = useFormat();
  const { hideBalances } = useAppCtx();
  const [mode, setMode] = useTableMode();
  const split = days.every((day) => day.byPillar && day.byCategory);

  let columns: Column[] = [];
  if (split && mode === "pillar") {
    columns = PILLAR_COLUMNS
      .filter((pillar) => pillar !== "unclassified" || days.some((day) => (day.byPillar?.unclassified ?? 0) > 0))
      .map((pillar) => ({ key: pillar, label: terms(PILLAR_TERM[pillar]), value: (day) => day.byPillar?.[pillar] ?? 0 }));
  } else if (split) {
    const { columns: top, hasOther } = topCategories(days);
    const sumOf = (day: DailyPoint, keep: (key: string) => boolean) =>
      (day.byCategory ?? []).reduce((sum, entry) => (keep(categoryKey(entry)) ? sum + entry.amount : sum), 0);
    const topKeys = new Set(top.map((column) => column.key));
    columns = top.map((column) => ({ key: column.key, label: column.name, value: (day) => sumOf(day, (key) => key === column.key) }));
    if (hasOther) columns.push({ key: "other", label: t("otherCategories"), value: (day) => sumOf(day, (key) => !topKeys.has(key)) });
  }
  const caption = !split ? t("tableCaption") : mode === "pillar" ? t("tableCaptionPillar") : t("tableCaptionCategory");

  return (
    <>
      {split && (
        <RadioSegments
          label={t("columns")}
          className="daily-tbl-mode"
          options={[{ value: "pillar", label: t("byPillar") }, { value: "category", label: t("byCategory") }]}
          value={mode}
          onChange={setMode}
        />
      )}
      {/* A scrolling region is reached by keyboard and named by the table it holds (WCAG 2.1.1). */}
      <div className="tbl-scroll" tabIndex={0} role="region" aria-label={caption}>
        <table className={split ? "tbl tbl-daily" : "tbl"}>
          <caption className="sr-only">{caption}</caption>
          <thead>
            <tr>
              <th scope="col">{t("date")}</th>
              {columns.map((column) => (
                <th key={column.key} scope="col" className="num">
                  {column.label}
                </th>
              ))}
              <th scope="col" className="num">{split ? t("total") : t("amount")}</th>
            </tr>
          </thead>
          <tbody>
            {days.map((day) => (
              <tr key={day.date}>
                <td>
                  {/* With the breakdown columns the date is short and never breaks; only its suffix may wrap below it. */}
                  {split ? <span className="tbl-date">{`${format.weekdayShort(day.date)}, ${format.shortDate(day.date)}`}</span> : format.weekdayDate(day.date)}
                  {peak?.date === day.date ? t("peakSuffix") : ""}
                  {day.date === today ? t("todaySuffix") : ""}
                </td>
                {columns.map((column) => {
                  const amount = column.value(day);
                  return (
                    <td key={column.key} className="num">
                      {amount === 0 && !hideBalances ? (
                        <span className="tbl-zero">
                          <span aria-hidden="true">–</span>
                          <span className="sr-only">{money(0)}</span>
                        </span>
                      ) : (
                        <MaskedMoney text={money(amount)} className="tbl-amt" />
                      )}
                    </td>
                  );
                })}
                <td className="num"><MaskedMoney text={money(day.amount)} className="t-amt" /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function Bars({
  days,
  peak,
  today,
  money,
  hidden,
  onOpenDay,
}: {
  days: DailyPoint[];
  peak: DailyPoint | null;
  today: string;
  money: Money;
  hidden: boolean;
  onOpenDay: (date: string) => void;
}) {
  const t = useTranslations("analysis.daily");
  const common = useTranslations("common");
  const format = useFormat();
  const hatchId = useId().replace(/:/g, "");
  const { ref, width } = useWidth<HTMLDivElement>(720);
  // `touch` marks a tip opened by a tap: only then does it offer "Lihat di Kalender", since the tap did not open the day.
  const [active, setActive] = useState<{ date: string; touch: boolean } | null>(null);
  const lastPointer = useRef<string | null>(null);
  const max = Math.max(0, ...days.map((day) => day.amount));
  const top = niceMax(max);
  const y = (value: number) => HEIGHT - BOTTOM - ((HEIGHT - BOTTOM - TOP) * value) / top;
  const { barWidth, x, step } = barGeometry(width, days.length);
  const todayIndex = days.findIndex((day) => day.date === today);
  const texts = days.map((day) => format.shortDate(day.date));
  const labelled = dateLabelIndexes(texts, width, todayIndex);

  useEffect(() => {
    if (!active) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setActive(null);
    };
    const onOutside = (event: PointerEvent) => {
      if (!ref.current?.contains(event.target as Node)) setActive(null);
    };
    window.addEventListener("keydown", onKey);
    document.addEventListener("pointerdown", onOutside);
    return () => {
      window.removeEventListener("keydown", onKey);
      document.removeEventListener("pointerdown", onOutside);
    };
  }, [active, ref]);

  const activeIndex = active ? days.findIndex((day) => day.date === active.date) : -1;
  const activeDay = activeIndex >= 0 ? days[activeIndex] : null;
  const label = peak
    ? t("chartLabel", { date: format.shortDate(peak.date), amount: money(peak.amount) })
    : t("chartLabelEmpty");
  const dayName = (date: string) => (date === today ? t("tipToday", { date: format.shortDate(date) }) : format.shortDate(date));
  const anchor = (centre: number, text: string): "start" | "middle" | "end" => {
    const half = (text.length * 6.5) / 2;
    if (centre - half < 0) return "start";
    if (centre + half > width) return "end";
    return "middle";
  };
  const open = (event: ReactMouseEvent<HTMLButtonElement>, date: string) => {
    // Browsers without a pointer type on `click` fall back to the type of the press that started it.
    const native = (event.nativeEvent as Partial<PointerEvent>).pointerType;
    const pointer = native === "mouse" || native === "touch" || native === "pen" ? native : lastPointer.current;
    lastPointer.current = null;
    // A tap only shows the tip (it has no hover); Enter, Space (detail 0) and a mouse click open the day at once.
    if (event.detail !== 0 && (pointer === "touch" || pointer === "pen")) return;
    onOpenDay(date);
  };

  return (
    <div
      ref={ref}
      className="chart"
      onPointerLeave={(event) => {
        if (event.pointerType === "mouse") setActive(null);
      }}
    >
      <svg width={width} height={HEIGHT} viewBox={`0 0 ${width} ${HEIGHT}`} role="img" aria-label={label}>
        <defs>
          <pattern id={hatchId} width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <rect width="2" height="4" fill="var(--window)" opacity="0.6" />
          </pattern>
        </defs>
        {[0, top / 2, top].map((tick) => (
          <g key={tick}>
            <line x1={0} x2={width} y1={y(tick)} y2={y(tick)} stroke="var(--line)" strokeWidth={1} />
            {tick > 0 && !hidden && (
              <text className="ax" x={0} y={y(tick) - 4} textAnchor="start">{format.compactMoney(tick)}</text>
            )}
          </g>
        ))}
        {days.map((day, index) => {
          const height = Math.max(day.amount > 0 ? 2 : 0, HEIGHT - BOTTOM - y(day.amount));
          const isPeak = peak?.date === day.date;
          const left = x(index);
          const barTop = HEIGHT - BOTTOM - height;
          return (
            <g key={day.date}>
              <rect x={left} y={barTop} width={barWidth} height={height} fill={isPeak ? "var(--out-solid)" : "var(--k-none)"} data-bar={isPeak ? "peak" : day.date === today ? "today" : "day"} />
              {day.date === today && <rect x={left} y={barTop} width={barWidth} height={height} fill={`url(#${hatchId})`} />}
            </g>
          );
        })}
        {labelled.map((index) => {
          const centre = 4 + index * step + step / 2;
          const anchorAt = anchor(centre, texts[index]);
          return (
            <text key={days[index].date} className="ax" x={anchorAt === "start" ? 0 : anchorAt === "end" ? width : centre} y={HEIGHT - 8} textAnchor={anchorAt}>
              {texts[index]}
            </text>
          );
        })}
      </svg>
      {/* The whole step above the axis is the button, so a 4 px bar and a Rp 0 day are still easy to point at. */}
      {days.map((day, index) => (
        <button
          key={day.date}
          type="button"
          className="chart-hit"
          data-day={day.date}
          style={{ left: 4 + index * step, width: step, top: TOP, height: HEIGHT - BOTTOM - TOP }}
          aria-label={t("barLabel", { date: dayName(day.date), amount: hidden ? common("hidden") : money(day.amount) })}
          onPointerEnter={(event) => {
            if (event.pointerType === "mouse") setActive({ date: day.date, touch: false });
          }}
          onPointerDown={(event) => {
            lastPointer.current = event.pointerType;
            if (event.pointerType !== "mouse") setActive((current) => (current?.date === day.date ? null : { date: day.date, touch: true }));
          }}
          onClick={(event) => open(event, day.date)}
        />
      ))}
      {activeDay && active && (
        <span
          className="chart-tip"
          data-testid="daily-spend-tip"
          style={{
            left: Math.min(Math.max(x(activeIndex) + barWidth / 2, 60), width - 60),
            bottom: Math.min(HEIGHT - y(activeDay.amount) + 6, active.touch ? HEIGHT - 64 : HEIGHT - 40),
          }}
        >
          <span>{dayName(activeDay.date)}</span>
          <MaskedMoney text={money(activeDay.amount)} className="t-amt" />
          {active.touch && (
            <button type="button" className="chart-tip-link" onClick={() => onOpenDay(activeDay.date)}>
              {t("openDay")}
            </button>
          )}
        </span>
      )}
    </div>
  );
}
