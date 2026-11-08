"use client";

import Link from "next/link";
import { useTranslations } from "next-intl";
import type { RowLabels } from "@/components/transactions/rowLabels";
import { DayHeading, ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { activityMeta, activityPhoneStatus, groupByDay, rowKind, rowStatus, rowTime, rowTitle } from "./activity";
import type { Words } from "@/lib/i18n/types";
import type { ActivityRow } from "./types";
import { rowLabelsFor, useWords } from "./words";

function dayLabel(day: string, today: string, yesterday: string, { t, format }: Words): string {
  const name = format.weekdayDate(day);
  if (day === today) return t("home.dayToday", { day: name });
  if (day === yesterday) return t("home.dayYesterday", { day: name });
  return name;
}

/** The amount with its sign and type colour (D7): expenses in the ink, income green, movements sky, top-ups lavender. */
function ActivityAmount({ row, bal }: { row: ActivityRow; bal: (amount: number) => string }) {
  const kind = rowKind(row);
  if (kind === "topup") return <MaskedMoney text={bal(row.amount)} className="t-amt c-invest" />;
  if (kind === "movement") return <MaskedMoney text={bal(row.amount)} className="t-amt c-transfer" />;
  if (kind === "income") return <MaskedMoney text={`+${bal(row.amount)}`} className="t-amt c-income" />;
  return <MaskedMoney text={`−${bal(row.amount)}`} className="t-amt" />;
}

function Status({ row, labels }: { row: ActivityRow; labels: RowLabels }) {
  const status = rowStatus(row, labels);
  const kind = rowKind(row);
  if (kind === "income" || kind === "movement") return <span>{status.text}</span>;
  return (
    <>
      <span className={status.bucket === "unclassified" ? "dot is-unclassified" : "dot"} style={status.color ? { background: status.color } : undefined} aria-hidden="true" />
      <span>{status.text}</span>
    </>
  );
}

function Warning({ text, warning }: { text: string; warning?: boolean }) {
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
  const labels = rowLabelsFor(words.t);
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
                const meta = activityMeta(row, labels);
                const phone = activityPhoneStatus(row, labels);
                const time = rowTime(row, words.format);
                return (
                  <li key={row.id}>
                    <ListRow
                      onClick={() => onSelect(row)}
                      time={time}
                      title={rowTitle(row, labels)}
                      meta={meta.text ? <Warning {...meta} /> : undefined}
                      phoneMeta={
                        <>
                          {time} · <Warning {...phone} />
                        </>
                      }
                      status={<Status row={row} labels={labels} />}
                      amount={<ActivityAmount row={row} bal={bal} />}
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
