"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { shortDate, unavailableReason } from "@/components/beranda/explanations";
import type { CycleSummary, GoalLine, PlanLine } from "@/components/beranda/types";
import { ErrorState, Pill } from "@/components/workspace/primitives";
import { LEDGER_NAV_HREF } from "@/components/workspace/navigation";
import type { Capture } from "@/lib/capture";
import { undoStillOpen } from "@/lib/capture";
import { KAKEIBO_BUCKETS, UNCLASSIFIED, kakeiboLabel } from "@/lib/kakeibo";
import type { BillsRange } from "@/lib/tanya/intents";
import type { CardSnapshot, WaitingCardData } from "@/lib/tanya/types";
import type { ReportReadiness } from "@/lib/reportReady";
import styles from "./Tanya.module.css";

type Money = (amount: number) => string;

const CLOCK = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit" });

export function clock(iso: string): string {
  return CLOCK.format(new Date(iso));
}

const WEEK_DAYS = 7;

function dayNumber(iso: string): number {
  return Date.parse(`${iso.slice(0, 10)}T00:00:00Z`) / 86_400_000;
}

/** The scheduled lines a bills card shows: all of the window, or plans due in the next 7 days (OD67). */
export function billLines(summary: CycleSummary, range: BillsRange | undefined): (PlanLine | GoalLine)[] {
  if (range !== "week") return summary.scheduled.lines;
  const today = dayNumber(summary.period.today);
  return summary.scheduled.lines.filter((line) => line.kind === "plan" && dayNumber(line.due_date) - today < WEEK_DAYS);
}

/** The one-sentence summary a screen reader hears for a card (D5). */
export function cardSummary(card: CardSnapshot, money: Money): string {
  if (card.status === "error" || !card.data) return "Data ini tidak dapat dimuat.";
  const data = card.data;
  if (data.kind === "waiting") return data.waiting.attention ? `${data.waiting.attention} perlu ditinjau` : "Tidak ada yang perlu ditinjau";
  if (data.kind === "report") return data.report.ready && data.report.period ? `Laporan ${data.report.period.label} siap` : "Laporan berikutnya belum siap";
  const summary = data.summary;
  if (data.kind === "kakeibo") return `Keluar ${money(summary.keluar.total)} per pilar Kakeibo`;
  if (data.kind === "bills") {
    const lines = billLines(summary, card.range);
    return lines.length ? `${lines.length} tagihan atau setoran, total ${money(lines.reduce((sum, line) => sum + line.amount, 0))}` : "Tidak ada tagihan atau setoran";
  }
  const safe = summary.safe_to_spend;
  const first = safe.status === "available" && safe.per_day !== null ? `Aman dipakai hari ini ${money(safe.per_day)}` : `Sisa ${money(summary.remaining.total)}`;
  return `${first}, Keluar ${money(summary.keluar.total)}, ${summary.waiting.count} menunggu jawaban`;
}

function Rows({ children, label }: { children: ReactNode; label?: string }) {
  return <dl className={styles.rows} aria-label={label}>{children}</dl>;
}

function Row({ label, value, note, marker }: { label: ReactNode; value: ReactNode; note?: ReactNode; marker?: ReactNode }) {
  return (
    <div className={styles.row}>
      <dt>{marker}{label}{note && <span className={styles.rowNote}>{note}</span>}</dt>
      <dd className="tabular-nums">{value}</dd>
    </div>
  );
}

function CycleBody({ summary, money }: { summary: CycleSummary; money: Money }) {
  const safe = summary.safe_to_spend;
  const available = safe.status === "available" && safe.per_day !== null && safe.days_left !== null;
  const { waiting } = summary;
  return (
    <>
      <p className={styles.cardEyebrow}>{summary.period.label}</p>
      <Rows>
        {available ? (
          <Row label="Aman dipakai hari ini" value={money(safe.per_day as number)} note={`Untuk ${safe.days_left} hari sampai gaji berikutnya`} />
        ) : (
          <Row label="Sisa periode ini" value={money(summary.remaining.total)} note={`Angka per hari belum tersedia: ${unavailableReason(summary).short}`} />
        )}
        <Row label="Keluar" value={money(summary.keluar.total)} />
        <Row label="Masuk" value={money(summary.masuk.total)} />
        <Row label="Kekayaan bersih" value={money(summary.net_worth.total)} />
        <Row
          label="Menunggu jawaban"
          value={waiting.count ? `${waiting.count} · ${money(waiting.total)}` : "0"}
          note={waiting.count ? `${money(waiting.in_keluar)} di antaranya sudah termasuk dalam Keluar` : "Semua sudah ditinjau"}
        />
      </Rows>
      <div className={styles.cardLinks}>
        <Link href="/">Buka Beranda</Link>
        <Link href={LEDGER_NAV_HREF}>Lihat transaksi</Link>
      </div>
    </>
  );
}

function KakeiboBody({ summary, money }: { summary: CycleSummary; money: Money }) {
  const lines = new Map(summary.keluar.lines.map((line) => [line.key, line]));
  const comparison = summary.comparison;
  const belanja = comparison.belanja;
  return (
    <>
      <p className={styles.cardEyebrow}>{summary.period.label}</p>
      <Rows label="Keluar per pilar">
        {KAKEIBO_BUCKETS.map((bucket) => {
          const line = lines.get(bucket.value);
          if (!line) return null;
          const unclassified = bucket.value === UNCLASSIFIED;
          return (
            <Row
              key={bucket.value}
              label={bucket.label}
              value={money(line.amount)}
              note={line.note ?? undefined}
              marker={<span className={unclassified ? `${styles.swatch} ${styles.swatchDashed}` : styles.swatch} style={{ background: unclassified ? undefined : bucket.color, borderColor: bucket.color }} aria-hidden="true" />}
            />
          );
        })}
        <Row label={<strong>Total keluar</strong>} value={<strong>{money(summary.keluar.total)}</strong>} />
      </Rows>
      {summary.keluar.offsets_total > 0 && <p className={styles.cardNote}>Sudah dikurangi ganti uang {money(summary.keluar.offsets_total)}.</p>}
      {comparison.status === "available" && belanja.previous !== null && (
        <p className={styles.cardNote}>
          Belanja {money(belanja.current)} dalam {comparison.compared_days} hari pertama, dibanding {money(belanja.previous)} pada {comparison.previous_label ?? "periode lalu"} di hari yang sama.
        </p>
      )}
      <div className={styles.cardLinks}>
        <Link href="/insights">Buka Analisis</Link>
      </div>
    </>
  );
}

function BillsBody({ summary, range, money }: { summary: CycleSummary; range: BillsRange | undefined; money: Money }) {
  const lines = billLines(summary, range);
  const heading = range === "week"
    ? "7 hari ke depan"
    : summary.scheduled.window_basis === "expected_salary" ? "Sampai gaji berikutnya" : "30 hari ke depan";
  return (
    <>
      <p className={styles.cardEyebrow}>{heading}</p>
      {lines.length === 0 ? (
        <p className={styles.cardNote}>Tidak ada tagihan atau setoran dalam periode ini.</p>
      ) : (
        <Rows label={`Tagihan dan setoran, ${heading}`}>
          {lines.map((line) => (
            <Row
              key={line.kind === "plan" ? `plan:${line.rule_id}:${line.due_date}` : `goal:${line.goal_id}`}
              label={line.label}
              note={line.kind === "plan" ? `${line.overdue ? "Lewat jatuh tempo, " : "Jatuh tempo "}${shortDate(line.due_date)}` : "Setoran target periode ini"}
              value={money(line.amount)}
            />
          ))}
          <Row label={<strong>Total</strong>} value={<strong>{money(lines.reduce((sum, line) => sum + line.amount, 0))}</strong>} />
        </Rows>
      )}
      {range === "week" && summary.scheduled.lines.some((line) => line.kind === "goal") && (
        <p className={styles.cardNote}>Setoran target dihitung per periode, jadi tidak termasuk dalam 7 hari ini.</p>
      )}
      <div className={styles.cardLinks}>
        <Link href="/goals">Buka Target &amp; Tagihan</Link>
      </div>
    </>
  );
}

function WaitingBody({ waiting, money }: { waiting: WaitingCardData; money: Money }) {
  return (
    <>
      {waiting.attention === 0 ? (
        <p className={styles.cardNote}>Tidak ada yang perlu ditinjau.</p>
      ) : (
        <>
          <p className={styles.cardEyebrow}>{waiting.attention} perlu ditinjau</p>
          <ul className={styles.items}>
            {waiting.items.map((item) => (
              <li key={item.id}>
                <Link href={item.href} className={styles.itemLink}>
                  <span className={styles.itemTitle}>{item.title}</span>
                  <span className={styles.rowNote}>{item.folderLabel}</span>
                </Link>
                <span className="tabular-nums">{money(item.amount)}</span>
              </li>
            ))}
          </ul>
        </>
      )}
      <div className={styles.cardLinks}>
        <Link href="/review">Buka Tinjau</Link>
      </div>
    </>
  );
}

function ReportBody({ report }: { report: ReportReadiness }) {
  if (!report.ready || !report.period || !report.url) {
    return (
      <>
        <p className={styles.cardNote}>Laporan berikutnya belum siap.</p>
        <div className={styles.cardLinks}><Link href="/laporan">Buka Laporan</Link></div>
      </>
    );
  }
  return (
    <>
      <p className={styles.cardEyebrow}>Laporan {report.period.label}</p>
      <p className={styles.cardNote}>Periode selesai {shortDate(report.period.end_date)}.</p>
      <div className={styles.cardLinks}><Link href={report.url}>Buka laporan</Link></div>
    </>
  );
}

const CARD_TITLES: Record<CardSnapshot["kind"], string> = {
  cycle: "Ringkasan periode",
  kakeibo: "Keluar per pilar Kakeibo",
  bills: "Tagihan dan setoran",
  waiting: "Menunggu jawaban",
  report: "Laporan terbaru",
};

/** A snapshot card: its figures, "Per hh.mm" and "Perbarui", or an error with retry (D3, D9). */
export function AnswerCard({
  card, money, onRefresh, refreshing,
}: { card: CardSnapshot; money: Money; onRefresh: () => void; refreshing: boolean }) {
  const data = card.data;
  return (
    <div className={styles.card} role="group" aria-label={`${CARD_TITLES[card.kind]}: ${cardSummary(card, money)}`}>
      <p className={styles.cardTitle}>{CARD_TITLES[card.kind]}</p>
      {card.status === "error" || !data ? (
        <ErrorState message="Data ini tidak dapat dimuat." onRetry={onRefresh} />
      ) : (
        <>
          {data.kind === "cycle" && <CycleBody summary={data.summary} money={money} />}
          {data.kind === "kakeibo" && <KakeiboBody summary={data.summary} money={money} />}
          {data.kind === "bills" && <BillsBody summary={data.summary} range={card.range} money={money} />}
          {data.kind === "waiting" && <WaitingBody waiting={data.waiting} money={money} />}
          {data.kind === "report" && <ReportBody report={data.report} />}
          <div className={styles.cardFoot}>
            <span>Per {clock(card.asOf)}</span>
            <button type="button" className="ws-btn ws-btn-sm" onClick={onRefresh} disabled={refreshing}>
              {refreshing ? "Memperbarui…" : "Perbarui"}
            </button>
          </div>
        </>
      )}
    </div>
  );
}

function captureStatus(capture: Capture): { tone: "ok" | "warn" | "neutral"; text: string } {
  if (capture.status === "undone") return { tone: "neutral", text: "Dibatalkan" };
  if (capture.status !== "recorded") return { tone: "warn", text: "Menunggu jawaban" };
  if (capture.transaction?.review_status === "needs_review") return { tone: "warn", text: "Masuk Tinjau" };
  return { tone: "ok", text: "Dicatat" };
}

/** A recorded (or still questioned) capture: what was read, "Urungkan" while its window is open, "Ubah" (D6). */
export function CaptureCard({
  capture, money, onUndo, onEdit, undoing,
}: { capture: Capture; money: Money; onUndo: () => void; onEdit: (transactionId: string) => void; undoing: boolean }) {
  const status = captureStatus(capture);
  const row = capture.transaction;
  const amount = row?.amount ?? capture.summary.amount;
  const description = capture.summary.description ?? row?.notes ?? row?.merchant ?? "Transaksi";
  const category = row?.category_name ? `${row.category_name}${row.kakeibo ? ` · ${kakeiboLabel(row.kakeibo)}` : ""}` : null;
  const label = [description, amount !== null && amount !== undefined ? money(amount) : null, status.text].filter(Boolean).join(", ");
  return (
    <div className={styles.card} role="group" aria-label={`Catatan: ${label}`}>
      <div className={styles.captureHead}>
        <p className={styles.cardTitle}>{description}</p>
        <Pill tone={status.tone}>{status.text}</Pill>
      </div>
      <Rows>
        {amount !== null && amount !== undefined && <Row label="Jumlah" value={money(amount)} />}
        {capture.summary.time_label && <Row label="Waktu" value={capture.summary.time_label} />}
        {row?.account_name && <Row label="Rekening" value={row.account_name} />}
        {category && <Row label="Kategori" value={category} />}
      </Rows>
      {capture.status === "recorded" && row && (
        <div className={styles.cardFoot}>
          {undoStillOpen(capture.undo_until) && (
            <button type="button" className="ws-btn ws-btn-sm" onClick={onUndo} disabled={undoing}>Urungkan</button>
          )}
          <button type="button" className="ws-btn ws-btn-sm" onClick={() => onEdit(row.id)}>Ubah</button>
        </div>
      )}
    </div>
  );
}
