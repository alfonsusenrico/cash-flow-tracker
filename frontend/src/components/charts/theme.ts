import { getCurrencyConfig } from "@/lib/utils";

export const chartColors = {
  income: "var(--color-income)",
  incomeLight: "var(--color-income-soft)",
  expense: "var(--color-expense)",
  expenseLight: "var(--color-expense-soft)",
  transfer: "var(--color-transfer)",
  transferLight: "var(--color-transfer-soft)",
  warning: "var(--color-warning)",
  accent: "var(--color-transfer)",
  cyan: "var(--color-kakeibo-need)",
  grid: "var(--border)",
  text: "var(--muted)",
};

export const chartGridProps = {
  strokeDasharray: "3 3",
  stroke: "var(--border)",
  vertical: false,
};

export const chartAxisProps = {
  stroke: "transparent",
  tick: { fill: "var(--muted)", fontSize: 11, fontFamily: "inherit" },
  tickLine: false,
};

export const chartTooltipStyle = {
  backgroundColor: "var(--surface)",
  border: "1px solid var(--border)",
  borderRadius: "12px",
  boxShadow: "0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1)",
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
