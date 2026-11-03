"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { type MonthlyReport, reportLedgerHref, shortDate } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { LINK_CLASS, ReportPanel, SubHeading } from "./ReportParts";

/** "Rp 200.000 lebih tinggi" / "lebih rendah" / "sama"; amounts only, never a percentage or a verdict. */
function difference(bal: (amount: number) => string, current: number, other: number): string {
  if (current === other) return "sama";
  return `${bal(Math.abs(current - other))} ${current > other ? "lebih tinggi" : "lebih rendah"}`;
}

export function ComparisonPanel({ report }: { report: MonthlyReport }) {
  const { bal } = useAppCtx();
  const { previous, previous_partial: partial, previous_period: previousPeriod, median } = report.comparison;
  const previousLabel = previous.previous_label ?? previousPeriod?.label ?? "periode sebelumnya";

  return (
    <ReportPanel id="report-comparison" title="Dibanding sebelumnya">
      <div className="space-y-1 text-sm">
        <SubHeading>{previousLabel}</SubHeading>
        {!previous.previous_has_data || !previous.previous ? (
          <p className="text-[var(--text-2)]">Belum ada data {previousLabel} untuk dibandingkan.</p>
        ) : (
          <>
            <p className="text-[var(--text)]">
              Pada {previous.compared_days} hari pertama, uang keluar{" "}
              <Link
                href={reportLedgerHref(previous.current, { type: "expense" })}
                className={cn("font-bold tabular-nums", LINK_CLASS)}
              >
                {bal(previous.current.outflow)}
              </Link>
              , {difference(bal, previous.current.outflow, previous.previous.outflow)} dari{" "}
              <Link
                href={reportLedgerHref(previous.previous, { type: "expense" })}
                className={cn("font-bold tabular-nums", LINK_CLASS)}
              >
                {bal(previous.previous.outflow)}
              </Link>
              .
            </p>
            <p className="text-[var(--text-2)]">
              Uang masuk {bal(previous.current.inflow)}, {difference(bal, previous.current.inflow, previous.previous.inflow)} dari{" "}
              {bal(previous.previous.inflow)}.
            </p>
            {partial && previousPeriod?.first_recorded_date && (
              <p className="text-xs text-[var(--text-2)]">
                {previousPeriod.label} baru tercatat sejak {shortDate(previousPeriod.first_recorded_date)}, jadi angkanya belum lengkap.
              </p>
            )}
          </>
        )}
      </div>

      <div className="space-y-1 border-t border-[var(--line)] pt-3 text-sm">
        <SubHeading>Median 3 periode</SubHeading>
        {!median.available ? (
          <p className="text-[var(--text-2)]">
            Muncul setelah ada {median.required} periode lengkap sebelum ini. Sekarang baru {median.period_count}.
          </p>
        ) : (
          <>
            <p className="text-[var(--text)]">
              Pada {median.compared_days} hari pertama, uang keluar {bal(median.current.outflow)},{" "}
              {difference(bal, median.current.outflow, median.median.outflow)} dari median {bal(median.median.outflow)}.
            </p>
            <p className="text-xs text-[var(--text-2)]">
              Median dari {median.periods.map((item) => item.label).join(", ")}, masing-masing {median.compared_days} hari pertama.
            </p>
          </>
        )}
      </div>
    </ReportPanel>
  );
}
