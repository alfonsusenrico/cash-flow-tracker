"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { ChartTable, ChartTableButton } from "@/components/charts/ChartTable";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { todayIso } from "./format";

type Money = (amount: number) => string;

/** One day's Pengeluaran (apply-markas-analisis D31). */
export interface DailyPoint {
  date: string;
  amount: number;
}

const HEIGHT = 200;
const BOTTOM = 28;
const TOP = 8;
/** A 12 px Inter date label is about 6.5 px per character; two labels closer than this would touch. */
const LABEL_GAP = 4;

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

/**
 * "Pengeluaran harian" (apply-markas-analisis D7): one SVG bar per day up to today, drawn at the column's real width
 * so its text stays 12 px, the highest day in the expense mark colour, today hatched, a legend naming both, and the
 * same values as a table. A day's date and amount show above its bar on hover or tap.
 */
export function DailySpendingChart({ days, money, today = todayIso() }: { days: DailyPoint[]; money: Money; today?: string }) {
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
            chart={<Bars days={shown} peak={peak} today={today} money={money} hidden={hideBalances} />}
            table={
              <div className="tbl-scroll">
                <table className="tbl">
                  <caption className="sr-only">{t("tableCaption")}</caption>
                  <thead>
                    <tr>
                      <th scope="col">{t("date")}</th>
                      <th scope="col" className="num">{t("amount")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {shown.map((day) => (
                      <tr key={day.date}>
                        <td>
                          {format.weekdayDate(day.date)}
                          {peak?.date === day.date ? t("peakSuffix") : ""}
                          {day.date === today ? t("todaySuffix") : ""}
                        </td>
                        <td className="num"><MaskedMoney text={money(day.amount)} className="t-amt" /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            }
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
          </div>
        </>
      )}
    </section>
  );
}

function Bars({ days, peak, today, money, hidden }: { days: DailyPoint[]; peak: DailyPoint | null; today: string; money: Money; hidden: boolean }) {
  const t = useTranslations("analysis.daily");
  const format = useFormat();
  const hatchId = useId().replace(/:/g, "");
  const { ref, width } = useWidth<HTMLDivElement>(720);
  const [active, setActive] = useState<string | null>(null);
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

  const activeIndex = active ? days.findIndex((day) => day.date === active) : -1;
  const activeDay = activeIndex >= 0 ? days[activeIndex] : null;
  const label = peak
    ? t("chartLabel", { date: format.shortDate(peak.date), amount: money(peak.amount) })
    : t("chartLabelEmpty");
  const anchor = (centre: number, text: string): "start" | "middle" | "end" => {
    const half = (text.length * 6.5) / 2;
    if (centre - half < 0) return "start";
    if (centre + half > width) return "end";
    return "middle";
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
            <g
              key={day.date}
              data-day={day.date}
              onPointerEnter={(event) => {
                if (event.pointerType === "mouse") setActive(day.date);
              }}
              onPointerDown={(event) => {
                if (event.pointerType !== "mouse") setActive((current) => (current === day.date ? null : day.date));
              }}
            >
              {/* The whole step is the hover target, so a 4 px bar is still easy to point at. */}
              <rect x={4 + index * step} y={TOP} width={step} height={HEIGHT - BOTTOM - TOP} fill="transparent" />
              <rect x={left} y={barTop} width={barWidth} height={height} fill={isPeak ? "var(--out-solid)" : "var(--k-none)"} data-bar={isPeak ? "peak" : day.date === today ? "today" : "day"}>
                <title>{`${format.shortDate(day.date)}: ${money(day.amount)}`}</title>
              </rect>
              {day.date === today && <rect x={left} y={barTop} width={barWidth} height={height} fill={`url(#${hatchId})`} pointerEvents="none" />}
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
      {activeDay && (
        <span
          className="chart-tip"
          data-testid="daily-spend-tip"
          style={{
            left: Math.min(Math.max(x(activeIndex) + barWidth / 2, 60), width - 60),
            bottom: Math.min(HEIGHT - y(activeDay.amount) + 6, HEIGHT - 40),
          }}
        >
          <span>{activeDay.date === today ? t("tipToday", { date: format.shortDate(activeDay.date) }) : format.shortDate(activeDay.date)}</span>
          <MaskedMoney text={money(activeDay.amount)} className="t-amt" />
        </span>
      )}
    </div>
  );
}
