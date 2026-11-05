import { getCurrencyConfig } from "@/lib/utils";

export const chartColors = {
  income: "var(--in-solid)",
  incomeLight: "var(--in-bg)",
  expense: "var(--out-solid)",
  expenseLight: "var(--out-bg)",
  warning: "var(--warn-solid)",
  grid: "var(--line)",
  text: "var(--muted)",
};

export const chartGridProps = {
  strokeDasharray: "3 3",
  stroke: "var(--line)",
  vertical: false,
};

export const chartAxisProps = {
  stroke: "transparent",
  tick: { fill: "var(--muted)", fontSize: 12, fontFamily: "inherit" },
  tickLine: false,
};

export const chartTooltipStyle = {
  backgroundColor: "var(--dialog)",
  border: "1px solid var(--line)",
  borderRadius: "var(--r-panel)",
  boxShadow: "var(--shadow-float)",
  color: "var(--text)",
  fontSize: "12px",
  padding: "8px 12px",
};

export function formatAxisCurrency(val: number): string {
  const { currency, rate } = getCurrencyConfig();
  if (currency === "USD") {
    const usd = val / (rate > 0 ? rate : 16500);
    const abs = Math.abs(usd);
    const sign = usd < 0 ? "-" : "";
    if (abs >= 1_000_000) {
      return `${sign}$${(abs / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
    }
    if (abs >= 1_000) {
      return `${sign}$${(abs / 1_000).toFixed(1).replace(/\.0$/, "")}k`;
    }
    return `${sign}$${Math.round(abs)}`;
  }

  // IDR
  const abs = Math.abs(val);
  const sign = val < 0 ? "-" : "";
  if (abs >= 1_000_000_000) {
    return `${sign}${(abs / 1_000_000_000).toFixed(1).replace(/\.0$/, "")}B`;
  }
  if (abs >= 1_000_000) {
    return `${sign}${(abs / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  }
  if (abs >= 1_000) {
    return `${sign}${(abs / 1_000).toFixed(0)}k`;
  }
  return `${sign}${Math.round(abs)}`;
}
