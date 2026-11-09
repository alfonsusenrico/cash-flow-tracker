"use client";

import Link from "next/link";
import { useTranslations } from "next-intl";
import { RowAmount, RowStatus, rowMeta, rowPhoneLine, rowTitle, useRowLabels, type RowLine } from "@/components/transactions/rows";
import { DayHeading, ListRow, SectionHeader } from "@/components/workspace/lists";
import { groupByDay } from "./activity";
import type { Words } from "@/lib/i18n/types";
import type { ActivityRow } from "./types";
import { useWords } from "./words";

function dayLabel(day: string, today: string, yesterday: string, { t, format }: Words): string {
  const name = format.weekdayDate(day);
  if (day === today) return t("home.dayToday", { day: name });
  if (day === yesterday) return t("home.dayYesterday", { day: name });
  return name;
}

function Line({ text, warning }: RowLine) {
  return warning ? <span className="c-warning">{text}</span> : <>{text}</>;
}

/** "Aktivitas terbaru" (D7): today's and yesterday's answered rows under day headings, and "Semua transaksi". */
export function ActivitySection({
  rows,
  today,
  yesterday,
  bal,
  onSelect,
  error,
}: {
  rows: ActivityRow[] | null;
  today: string;
  yesterday: string;
  bal: (amount: number) => string;
  onSelect: (row: ActivityRow) => void;
  error: boolean;
}) {
  const words = useWords();
  const t = useTranslations("home");
  const labels = useRowLabels();
  return (
    <section aria-labelledby="beranda-activity">
      <SectionHeader
        id="beranda-activity"
        label={t("recentActivity")}
        aside={rows ? <span className="aside">{t("activityAside", { n: String(rows.length) })}</span> : undefined}
      />
      {error && <p className="t-meta">{t("activityLoadError")}</p>}
      {rows && rows.length === 0 && <p className="t-body muted">{t("noActivity")}</p>}
      {rows &&
        groupByDay(rows).map((group) => (
          <div key={group.day} className="day-group">
            <DayHeading label={dayLabel(group.day, today, yesterday, words)} />
            <ul className="list">
              {group.rows.map((row) => {
                const meta = rowMeta(row, labels, words.format);
                const phone = rowPhoneLine(row, labels, words.format);
                const time = words.format.time(row.date);
                return (
                  <li key={row.id}>
                    <ListRow
                      onClick={() => onSelect(row)}
                      time={time}
                      title={rowTitle(row, labels)}
                      meta={meta.text ? <Line {...meta} /> : undefined}
                      phoneMeta={
                        <>
                          {time} · <Line {...phone} />
                        </>
                      }
                      status={<RowStatus row={row} labels={labels} />}
                      amount={<RowAmount row={row} money={bal} />}
                    />
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
      <div className="list-foot">
        <Link href="/ledger" className="ws-btn">
          {t("allTransactions")}
        </Link>
      </div>
    </section>
  );
}
