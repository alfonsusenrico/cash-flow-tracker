"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { type MonthlyReport, categoryLedgerHref, reportLedgerHref } from "@/lib/monthlyReport";
import { AmountRow, AnswerAmount, QuestionSection } from "./ReportParts";

export function IncomeAnswer({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, income } = report;
  return (
    <QuestionSection
      id="report-income"
      number={1}
      question="Berapa uang masuk?"
      answer={<AnswerAmount href={reportLedgerHref(period, { type: "income" })} amount={bal(income.total)} label="lihat semua uang masuk" />}
    >
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
      <p className="text-xs text-[var(--text-2)]">Ganti uang dari teman dan pindah saldo antar rekening tidak dihitung sebagai uang masuk.</p>
    </QuestionSection>
  );
}

export function SavingsAnswer({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { period, savings } = report;
  return (
    <QuestionSection
      id="report-savings"
      number={2}
      question="Berapa yang ditabung?"
      answer={<AnswerAmount href={null} amount={bal(savings.fresh_saving)} label="tabungan baru" />}
    >
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
      <div className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)]/40 p-3">
        <p className="text-sm text-[var(--text)]">
          Selisih uang masuk dan keluar:{" "}
          <span className="font-bold tabular-nums">{bal(savings.surplus)}</span>
        </p>
        <p className="mt-1 text-xs text-[var(--text-2)]">
          Selisih ini surplus arus kas, bukan bukti uang sudah disisihkan. Tabungan baru hanya dari pilar Tabungan dan
          pindah saldo ke rekening tabungan atau investasi.
        </p>
      </div>
    </QuestionSection>
  );
}
