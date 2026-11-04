"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { type MonthlyReport, reportLedgerHref, shortDate } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { AmountRow, LINK_CLASS, ReportPanel, SubHeading } from "./ReportParts";

const CADENCE_LABEL = { weekly: "mingguan", monthly: "bulanan" } as const;

/** "Tempat belanja terbesar": gross spending per merchant, linked to its rows. */
export function TopMerchantsCard({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, spending } = report;
  if (spending.top_merchants.length === 0) return null;
  return (
    <ReportPanel id="report-merchants" title="Tempat belanja terbesar">
      <ul className="divide-y divide-[var(--line)]">
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
    </ReportPanel>
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
            <ul className="divide-y divide-[var(--line)]">
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
          <ul className="divide-y divide-[var(--line)]">
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
    <div className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)]/40 p-3 text-sm">
      <SubHeading>Biaya hidup pokok · saat ini</SubHeading>
      {essential.amount === null ? (
        <p className="mt-1 text-[var(--text-2)]">Belum bisa dihitung. Butuh satu bulan lengkap atau anggaran Kebutuhan.</p>
      ) : (
        <p className="mt-1 text-[var(--text)]">
          <span className="font-bold tabular-nums">{bal(essential.amount)}</span> per bulan
          {essential.coverage_months !== null && (
            <>
              {" "}· dana darurat cukup untuk <span className="font-bold tabular-nums">{essential.coverage_months.toLocaleString("id-ID")}</span> bulan
            </>
          )}
          {essential.limited_history && <span className="text-[var(--text-2)]"> (riwayat masih sedikit)</span>}
        </p>
      )}
      <Link href="/insights" className={cn("mt-1 inline-flex min-h-11 items-center text-xs font-semibold", LINK_CLASS)}>
        Cara hitungnya di Analisis
      </Link>
    </div>
  );
}

/** Unexpected items, recurring charges and the essential cost, folded so the page leads with the figures. */
export function SpendingDetailsCard({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { flow } = report;
  return (
    <ReportPanel id="report-spending-details" title="Rincian pengeluaran">
      {flow.reimbursement_offset > 0 && (
        <p className="text-xs text-[var(--text-2)]">
          Sudah dikurangi ganti uang <span className="tabular-nums">{bal(flow.reimbursement_offset)}</span>.
        </p>
      )}
      <details>
        <summary className="inline-flex min-h-11 cursor-pointer items-center text-sm font-semibold text-[var(--text)]">
          Tak terduga, tagihan berulang dan biaya hidup pokok
        </summary>
        <div className="space-y-4 pt-2">
          <UnexpectedAndRecurring report={report} />
          <EssentialBrief report={report} />
        </div>
      </details>
    </ReportPanel>
  );
}
