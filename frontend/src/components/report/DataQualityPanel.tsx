"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { type MonthlyReport, reportLedgerHref, shortDate } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { LINK_CLASS, ReportPanel } from "./ReportParts";

export function dataQualityIssues(report: MonthlyReport): number {
  const quality = report.data_quality;
  return quality.pending_review + quality.unclassified.count + quality.possible_duplicates + quality.days_without_records.count;
}

function Item({ count, children }: { count: number; children: React.ReactNode }) {
  return (
    <li className="flex items-start gap-3 py-2">
      <span className="min-w-8 shrink-0 text-right text-base font-black tabular-nums text-[var(--text)]">{count}</span>
      <div className="min-w-0 flex-1 text-sm text-[var(--text)]">{children}</div>
    </li>
  );
}

export function DataQualityPanel({ report, onClassify }: { report: MonthlyReport; onClassify: () => void }) {
  const { bal, openQuickAdd } = useAppCtx();
  const { period, data_quality: quality } = report;
  const days = quality.days_without_records;
  const actionClass = cn("inline-flex min-h-11 items-center text-xs font-semibold", LINK_CLASS);

  return (
    <ReportPanel id="report-quality" title="Kualitas data">
      {dataQualityIssues(report) === 0 ? (
        <p className="text-sm text-[var(--text-secondary)]">Semua transaksi periode ini sudah ditinjau dan diklasifikasi.</p>
      ) : (
        <>
          <p className="text-xs text-[var(--text-secondary)]">Perbaiki ini dulu supaya angka di laporan makin tepat.</p>
          <ul className="divide-y divide-[var(--border)]">
            {quality.pending_review > 0 && (
              <Item count={quality.pending_review}>
                <p>transaksi menunggu di Tinjau</p>
                <Link href="/review" className={actionClass}>Buka Tinjau</Link>
              </Item>
            )}
            {quality.unclassified.count > 0 && (
              <Item count={quality.unclassified.count}>
                <p>
                  pengeluaran belum diklasifikasi,{" "}
                  <Link
                    href={reportLedgerHref(period, { type: "expense", kakeibo: "unclassified" })}
                    className={cn("tabular-nums", LINK_CLASS)}
                  >
                    {bal(quality.unclassified.amount)}
                  </Link>
                </p>
                <button type="button" onClick={onClassify} className={actionClass}>Lengkapi klasifikasi</button>
              </Item>
            )}
            {quality.possible_duplicates > 0 && (
              <Item count={quality.possible_duplicates}>
                <p>kemungkinan dobel: rekening, nominal dan jenis sama dalam 3 menit</p>
                <Link href={reportLedgerHref(period, { dup: true })} className={actionClass}>Periksa di Transaksi</Link>
              </Item>
            )}
            {days.count > 0 && (
              <Item count={days.count}>
                <p>hari tanpa catatan</p>
                <details className="text-xs text-[var(--text-secondary)]">
                  <summary className="inline-flex min-h-11 cursor-pointer items-center font-semibold text-[var(--text)]">Lihat tanggalnya</summary>
                  <p className="tabular-nums">{days.dates.map(shortDate).join(", ")}</p>
                </details>
                <button type="button" onClick={() => openQuickAdd("expense")} className={actionClass}>Catat transaksi</button>
              </Item>
            )}
          </ul>
        </>
      )}
    </ReportPanel>
  );
}
