"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { type MonthlyReport, reportLedgerHref, shortDate } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { LINK_CLASS } from "./ReportParts";
import styles from "./report.module.css";

export function dataQualityIssues(report: MonthlyReport): number {
  const quality = report.data_quality;
  return quality.pending_review + quality.unclassified.count + quality.possible_duplicates + quality.days_without_records.count;
}

/** "Kualitas data" as one line with its actions, green when nothing is open (D8). */
export function DataQualityLine({ report, onClassify }: { report: MonthlyReport; onClassify: () => void }) {
  const { openQuickAdd } = useAppCtx();
  const { period, data_quality: quality } = report;
  const days = quality.days_without_records;
  const total = dataQualityIssues(report);
  const linkClass = cn("inline-flex min-h-6 items-center text-xs font-semibold", LINK_CLASS);
  if (total === 0) {
    return (
      <p className={styles.notice} data-tone="ok" role="status">
        <b>Data lengkap.</b> Semua transaksi periode ini sudah ditinjau dan diklasifikasi.
      </p>
    );
  }
  const parts = [
    quality.pending_review > 0 && `${quality.pending_review} menunggu di Tinjau`,
    quality.unclassified.count > 0 && `${quality.unclassified.count} belum diklasifikasi`,
    quality.possible_duplicates > 0 && `${quality.possible_duplicates} kemungkinan duplikat`,
    days.count > 0 && `${days.count} hari tanpa catatan`,
  ].filter(Boolean);
  return (
    <div className={styles.notice}>
      <span><b>Kualitas data:</b> {parts.join(" · ")}</span>
      <span className={styles.noticeLinks}>
        {quality.pending_review > 0 && <Link href="/review" className={linkClass}>Buka Tinjau</Link>}
        {quality.unclassified.count > 0 && <button type="button" onClick={onClassify} className={linkClass}>Lengkapi klasifikasi</button>}
        {quality.possible_duplicates > 0 && <Link href={reportLedgerHref(period, { dup: true })} className={linkClass}>Periksa duplikat</Link>}
        {days.count > 0 && (
          <button type="button" onClick={() => openQuickAdd("expense")} className={linkClass} title={days.dates.map(shortDate).join(", ")}>
            Catat transaksi
          </button>
        )}
      </span>
    </div>
  );
}
