"use client";

import type { ReactNode } from "react";

/**
 * The chart-and-table toggle (apply-markas-analisis D7, D24): one quiet small button with `aria-pressed` that swaps
 * a chart for the same values as a table. The button stays mounted and keeps focus; only its words change.
 */
export function ChartTableButton({
  asTable,
  onToggle,
  showTable,
  showChart,
}: {
  asTable: boolean;
  onToggle: () => void;
  showTable: string;
  showChart: string;
}) {
  return (
    <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm ct-toggle" aria-pressed={asTable} onClick={onToggle}>
      {asTable ? showChart : showTable}
    </button>
  );
}

/** The chart or its table, whichever the toggle chose. */
export function ChartTable({ asTable, chart, table }: { asTable: boolean; chart: ReactNode; table: ReactNode }) {
  return <>{asTable ? table : chart}</>;
}
