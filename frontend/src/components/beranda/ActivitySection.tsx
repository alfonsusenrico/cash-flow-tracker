"use client";

import Link from "next/link";
import { DayHeading, ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { activityMeta, activityPhoneStatus, groupByDay, rowKind, rowStatus, rowTime, rowTitle } from "./activity";
import type { ActivityRow } from "./types";

const WEEKDAY_DATE = new Intl.DateTimeFormat("id-ID", { weekday: "long", day: "numeric", month: "long", timeZone: "UTC" });

function dayLabel(day: string, today: string, yesterday: string): string {
  const name = WEEKDAY_DATE.format(new Date(`${day}T00:00:00Z`));
  if (day === today) return `${name} · hari ini`;
  if (day === yesterday) return `${name} · kemarin`;
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

function Status({ row }: { row: ActivityRow }) {
  const status = rowStatus(row);
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
  return (
    <section aria-labelledby="beranda-activity">
      <SectionHeader
        id="beranda-activity"
        label="Aktivitas terbaru"
        aside={rows ? <span className="aside">hari ini dan kemarin · {rows.length}</span> : undefined}
      />
      {error && <p className="t-meta">Aktivitas belum bisa dimuat.</p>}
      {rows && rows.length === 0 && <p className="t-body muted">Belum ada transaksi hari ini dan kemarin.</p>}
      {rows &&
        groupByDay(rows).map((group) => (
          <div key={group.day} className="day-group">
            <DayHeading label={dayLabel(group.day, today, yesterday)} />
            <ul className="list">
              {group.rows.map((row) => {
                const meta = activityMeta(row);
                const phone = activityPhoneStatus(row);
                return (
                  <li key={row.id}>
                    <ListRow
                      onClick={() => onSelect(row)}
                      time={rowTime(row)}
                      title={rowTitle(row)}
                      meta={meta.text ? <Warning {...meta} /> : undefined}
                      phoneMeta={
                        <>
                          {rowTime(row)} · <Warning {...phone} />
                        </>
                      }
                      status={<Status row={row} />}
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
          Semua transaksi
        </Link>
      </div>
    </section>
  );
}
