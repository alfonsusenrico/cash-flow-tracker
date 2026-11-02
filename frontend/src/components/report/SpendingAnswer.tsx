"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import {
  type MonthlyReport,
  categoryLedgerHref,
  formatPercent,
  percentOf,
  reportLedgerHref,
  shortDate,
} from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { AmountRow, AnswerAmount, LINK_CLASS, QuestionSection, SubHeading } from "./ReportParts";

const CADENCE_LABEL = { weekly: "mingguan", monthly: "bulanan" } as const;

function PillarList({ report, onClassify }: { report: MonthlyReport; onClassify: () => void }) {
  const { bal } = useAppCtx();
  const { period, spending } = report;
  return (
    <div className="space-y-2">
      <SubHeading>Per pilar Kakeibo</SubHeading>
      <ul className="space-y-2">
        {spending.pillars.map((item) => {
          const info = kakeiboBucketInfo(item.pillar);
          const share = percentOf(item.amount, spending.total);
          const unclassified = item.pillar === "unclassified";
          return (
            <li key={item.pillar} className="space-y-1">
              <div className="flex min-w-0 items-center justify-between gap-3">
                <span className="flex min-w-0 items-center gap-2 text-sm text-[var(--text)]">
                  <span aria-hidden="true" className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ backgroundColor: info?.color }} />
                  <span className="truncate">{info?.label ?? item.pillar}</span>
                  <span className="shrink-0 text-xs tabular-nums text-[var(--muted)]">{formatPercent(share)}</span>
                </span>
                {item.amount > 0 ? (
                  <Link
                    href={reportLedgerHref(period, { type: "expense", kakeibo: item.pillar })}
                    aria-label={`${bal(item.amount)}, lihat transaksi ${info?.label ?? item.pillar}`}
                    className={cn("inline-flex min-h-11 shrink-0 items-center text-sm font-bold tabular-nums text-[var(--text)]", LINK_CLASS)}
                  >
                    {bal(item.amount)}
                  </Link>
                ) : (
                  <span className="inline-flex min-h-11 shrink-0 items-center text-sm tabular-nums text-[var(--muted)]">{bal(0)}</span>
                )}
              </div>
              <div aria-hidden="true" className="h-1.5 overflow-hidden rounded-full bg-[var(--border)]">
                <div className="h-full rounded-full" style={{ width: `${Math.min(100, share)}%`, backgroundColor: info?.color }} />
              </div>
              {unclassified && item.amount > 0 && (
                <button
                  type="button"
                  onClick={onClassify}
                  className={cn("inline-flex min-h-11 items-center text-xs font-semibold text-[var(--text)]", LINK_CLASS)}
                >
                  Lengkapi klasifikasi
                </button>
              )}
            </li>
          );
        })}
      </ul>
      <p className="text-xs text-[var(--text-secondary)]">
        Persentase dari total uang keluar. Pengeluaran tanpa pilar tidak dihitung sebagai Kebutuhan.
      </p>
    </div>
  );
}

function TopLists({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, spending } = report;
  return (
    <div className="grid gap-4 sm:grid-cols-2">
      <div className="space-y-1">
        <SubHeading>Kategori terbesar</SubHeading>
        {spending.top_categories.length === 0 ? (
          <p className="text-sm text-[var(--muted)]">Belum ada pengeluaran.</p>
        ) : (
          <ul className="divide-y divide-[var(--border)]">
            {spending.top_categories.map((item) => (
              <AmountRow
                key={item.category_id ?? "uncategorized"}
                label={item.name}
                amount={bal(item.amount)}
                href={categoryLedgerHref(period, item)}
                linkLabel={`lihat transaksi ${item.name}`}
              />
            ))}
          </ul>
        )}
      </div>
      <div className="space-y-1">
        <SubHeading>Tempat belanja terbesar</SubHeading>
        {spending.top_merchants.length === 0 ? (
          <p className="text-sm text-[var(--muted)]">Belum ada nama tempat belanja tercatat.</p>
        ) : (
          <ul className="divide-y divide-[var(--border)]">
            {spending.top_merchants.map((item) => (
              <AmountRow
                key={item.search_text}
                label={item.name}
                amount={bal(item.amount)}
                href={reportLedgerHref(period, { type: "expense", q: item.search_text })}
                linkLabel={`lihat transaksi ${item.name}`}
              />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function UnexpectedAndRecurring({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, spending } = report;
  const { unexpected, recurring } = spending;
  return (
    <div className="grid gap-4 sm:grid-cols-2">
      <div className="space-y-1">
        <SubHeading>Tak terduga</SubHeading>
        {unexpected.count === 0 ? (
          <p className="text-sm text-[var(--muted)]">Tidak ada pengeluaran tak terduga.</p>
        ) : (
          <>
            <ul className="divide-y divide-[var(--border)]">
              {unexpected.items.map((item) => (
                <AmountRow
                  key={item.id}
                  label={item.merchant || item.notes || item.category_name || "Tanpa keterangan"}
                  detail={shortDate(item.date.slice(0, 10))}
                  amount={bal(item.amount)}
                  href={`/ledger?edit=${item.id}`}
                  linkLabel="buka transaksi"
                />
              ))}
            </ul>
            {unexpected.count > unexpected.items.length && (
              <Link
                href={reportLedgerHref(period, { type: "expense", kakeibo: "unexpected" })}
                className={cn("inline-flex min-h-11 items-center text-xs font-semibold", LINK_CLASS)}
              >
                Lihat semua {unexpected.count} transaksi tak terduga
              </Link>
            )}
          </>
        )}
      </div>
      <div className="space-y-1">
        <SubHeading>Langganan dan tagihan berulang</SubHeading>
        {recurring.charges.length === 0 ? (
          <p className="text-sm text-[var(--muted)]">Belum terlihat tagihan yang berulang pada periode ini.</p>
        ) : (
          <ul className="divide-y divide-[var(--border)]">
            {recurring.charges.map((charge) => (
              <AmountRow
                key={charge.search_text}
                label={charge.merchant}
                detail={`${CADENCE_LABEL[charge.cadence]}${charge.existing_plan ? " · sudah ada rencana" : ""}${charge.amount_changed ? " · nominal berubah" : ""}`}
                amount={bal(charge.last_amount)}
                href={reportLedgerHref(period, { type: "expense", q: charge.search_text })}
                linkLabel={`lihat transaksi ${charge.merchant}`}
              />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function EssentialBrief({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const essential = report.spending.essential;
  return (
    <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/40 p-3 text-sm">
      <SubHeading>Biaya hidup pokok · saat ini</SubHeading>
      {essential.amount === null ? (
        <p className="mt-1 text-[var(--text-secondary)]">Belum bisa dihitung. Butuh satu bulan lengkap atau anggaran Kebutuhan.</p>
      ) : (
        <p className="mt-1 text-[var(--text)]">
          <span className="font-bold tabular-nums">{bal(essential.amount)}</span> per bulan
          {essential.coverage_months !== null && (
            <>
              {" "}· dana darurat cukup untuk <span className="font-bold tabular-nums">{essential.coverage_months.toLocaleString("id-ID")}</span> bulan
            </>
          )}
          {essential.limited_history && <span className="text-[var(--text-secondary)]"> (riwayat masih sedikit)</span>}
        </p>
      )}
      <Link href="/insights" className={cn("mt-1 inline-flex min-h-11 items-center text-xs font-semibold", LINK_CLASS)}>
        Cara hitungnya di Analisis
      </Link>
    </div>
  );
}

export function SpendingAnswer({ report, onClassify }: { report: MonthlyReport; onClassify: () => void }) {
  const { bal } = useAppCtx();
  const { period, flow } = report;
  return (
    <QuestionSection
      id="report-spending"
      number={3}
      question="Berapa yang dibelanjakan?"
      answer={
        <>
          <AnswerAmount href={reportLedgerHref(period, { type: "expense" })} amount={bal(flow.outflow)} label="lihat semua uang keluar" />
          {flow.reimbursement_offset > 0 && (
            <p className="text-xs text-[var(--text-secondary)]">
              Sudah dikurangi ganti uang <span className="tabular-nums">{bal(flow.reimbursement_offset)}</span> dari{" "}
              <span className="tabular-nums">{bal(flow.gross_outflow)}</span>.
            </p>
          )}
        </>
      }
    >
      <PillarList report={report} onClassify={onClassify} />
      <TopLists report={report} />
      <UnexpectedAndRecurring report={report} />
      <EssentialBrief report={report} />
    </QuestionSection>
  );
}
