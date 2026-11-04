"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { type MonthlyReport, categoryLedgerHref, reportLedgerHref } from "@/lib/monthlyReport";
import { AmountRow, ReportPanel } from "./ReportParts";

/** "Sumber uang masuk": income per category; the total is the Masuk KPI. */
export function IncomeSourcesCard({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, income } = report;
  return (
    <ReportPanel id="report-income" title="Sumber uang masuk">
      {income.sources.length === 0 ? (
        <p className="text-sm text-[var(--muted)]">Belum ada uang masuk tercatat pada periode ini.</p>
      ) : (
        <ul className="divide-y divide-[var(--line)]">
          {income.sources.map((source) => (
            <AmountRow
              key={source.category_id ?? "uncategorized"}
              label={source.name}
              detail={`${source.count} transaksi`}
              amount={bal(source.amount)}
              href={categoryLedgerHref(period, { category_id: source.category_id, is_uncategorized: !source.category_id }, "income")}
              linkLabel={`lihat uang masuk ${source.name}`}
            />
          ))}
        </ul>
      )}
      <p className="text-xs text-[var(--text-2)]">Ganti uang dan pindah saldo antar rekening tidak dihitung sebagai uang masuk.</p>
    </ReportPanel>
  );
}

/** "Rincian tabungan": the two parts of Ditabung & investasi; the total is the KPI. */
export function SavingsCard({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, savings } = report;
  return (
    <ReportPanel id="report-savings" title="Rincian tabungan">
      <ul className="divide-y divide-[var(--line)]">
        <AmountRow
          label="Pengeluaran berpilar Tabungan"
          amount={bal(savings.saving_expenses)}
          href={reportLedgerHref(period, { type: "expense", kakeibo: "saving" })}
          linkLabel="lihat pengeluaran berpilar Tabungan"
        />
        <AmountRow
          label="Pindah bersih ke rekening tabungan atau investasi"
          detail="Dihitung dari pindah saldo ke dan dari rekening target atau investasi"
          amount={bal(savings.net_saving_transfers)}
        />
      </ul>
    </ReportPanel>
  );
}
