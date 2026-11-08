"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Button } from "@/components/ui/Button";
import { type MonthlyReport, categoryLedgerHref } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { LINK_CLASS, ReportPanel } from "./ReportParts";

export function BudgetPanel({ report, onEdit }: { report: MonthlyReport; onEdit: () => void }) {
  const { bal } = useAppCtx();
  const { budget, period } = report;
  if (!budget) {
    return (
      <ReportPanel id="report-budget" title="Anggaran">
        <p className="text-sm text-[var(--text-2)]">Belum ada anggaran. Anggaran tidak wajib; laporan tetap lengkap tanpanya.</p>
        <Button type="button" className="min-h-11" onClick={onEdit}>Atur anggaran</Button>
      </ReportPanel>
    );
  }
  return (
    <ReportPanel id="report-budget" title="Anggaran dan realisasi">
      {budget.overall && (
        <p className="text-sm text-[var(--text)]">
          Total: keluar <span className="font-semibold tabular-nums">{bal(budget.overall.spent)}</span> dari anggaran{" "}
          <span className="tabular-nums">{bal(budget.overall.budget)}</span>,{" "}
          {budget.overall.remaining >= 0 ? `sisa ${bal(budget.overall.remaining)}` : `lebih ${bal(-budget.overall.remaining)}`}.
        </p>
      )}
      {budget.categories.length > 0 && (
        <ul className="divide-y divide-[var(--line)]">
          {budget.categories.map((row) => (
            <li key={row.category_id} className="space-y-1 py-2">
              <div className="flex min-w-0 items-center justify-between gap-3 text-sm">
                <span className="min-w-0 truncate text-[var(--text)]">{row.name}</span>
                <Link
                  href={categoryLedgerHref(period, { category_id: row.category_id })}
                  aria-label={`${bal(row.spent)} dari ${bal(row.budget)}, lihat transaksi ${row.name}`}
                  className={cn("inline-flex min-h-11 shrink-0 items-center tabular-nums", LINK_CLASS)}
                >
                  <span className="font-semibold">{bal(row.spent)}</span>
                  <span className="text-[var(--muted)]">&nbsp;/ {bal(row.budget)}</span>
                </Link>
              </div>
              <p className="text-xs text-[var(--text-2)]">
                {row.remaining >= 0 ? `Sisa ${bal(row.remaining)}` : `Lebih ${bal(-row.remaining)}`}
              </p>
            </li>
          ))}
        </ul>
      )}
      {budget.period_basis === "cycle" && (
        <p className="text-xs text-[var(--text-2)]">Anggaran bulanan dibandingkan apa adanya dengan satu siklus gaji.</p>
      )}
      <Button type="button" className="min-h-11" onClick={onEdit}>Ubah anggaran</Button>
    </ReportPanel>
  );
}
