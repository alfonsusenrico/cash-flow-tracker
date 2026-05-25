"use client";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";

interface Metric {
  value: number | null;
  pct?: number | null;
  months?: number | null;
  status: "ok" | "warn" | "critical";
  label: string;
}

interface GoalMetric {
  goal: string;
  required: number | null;
  available: number;
  feasible: boolean;
  status: string;
  progress_pct: number;
  eta_months: number | null;
}

interface DashboardResponse {
  month: string;
  range: { from: string; to: string };
  health_score: number;
  net_worth: number;
  liquid_assets: number;
  invested_assets: number;
  total_in: number;
  total_out: number;
  metrics: {
    safe_to_spend: Metric;
    emergency_fund: Metric;
    savings_rate: Metric;
    investment_rate: Metric;
    cash_runway: Metric;
    monthly_drift: Metric | null;
  };
  goals: GoalMetric[];
  warnings: { key: string; label: string; severity: "warn" | "critical" }[];
}

const STATUS_COLOR: Record<string, string> = {
  ok: "text-green-600",
  warn: "text-yellow-500",
  critical: "text-red-500",
};

const STATUS_BG: Record<string, string> = {
  ok: "bg-green-50 border-green-200 dark:bg-green-900/20 dark:border-green-800",
  warn: "bg-yellow-50 border-yellow-200 dark:bg-yellow-900/20 dark:border-yellow-800",
  critical: "bg-red-50 border-red-200 dark:bg-red-900/20 dark:border-red-800",
};

function MetricCard({ metric, extra }: { metric: Metric; extra?: string }) {
  return (
    <div className={`border rounded-xl p-4 space-y-1 ${STATUS_BG[metric.status]}`}>
      <div className="text-xs text-[var(--muted)]">{metric.label}</div>
      <div className={`text-xl font-bold ${STATUS_COLOR[metric.status]}`}>
        {metric.value != null ? fmtMoney(metric.value) : "—"}
      </div>
      {metric.pct != null && <div className="text-xs text-[var(--muted)]">{metric.pct}%</div>}
      {metric.months != null && <div className="text-xs text-[var(--muted)]">{metric.months} months</div>}
      {extra && <div className="text-xs text-[var(--muted)]">{extra}</div>}
    </div>
  );
}

export default function FinancialDashboardPage() {
  const { hideBalances } = useAppCtx();
  const { data, isLoading, error } = useQuery<DashboardResponse>({
    queryKey: ["dashboard"],
    queryFn: () => api.get("/dashboard"),
  });

  const bal = (n: number) => hideBalances ? "••••" : fmtMoney(n);

  if (isLoading) return <div className="p-8 text-[var(--muted)]">Loading…</div>;
  if (error) return <div className="p-8 text-[var(--danger)]">{String(error)}</div>;
  if (!data) return null;

  const scoreColor = data.health_score >= 80 ? "text-green-600" : data.health_score >= 50 ? "text-yellow-500" : "text-red-500";

  return (
    <div className="p-4 space-y-5 max-w-4xl mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold">Financial Health</h1>
          <p className="text-xs text-[var(--muted)]">{data.range.from} – {data.range.to}</p>
        </div>
        <div className="text-right">
          <div className="text-xs text-[var(--muted)]">Health Score</div>
          <div className={`text-4xl font-bold ${scoreColor}`}>{data.health_score}</div>
          <div className="text-xs text-[var(--muted)]">/ 100</div>
        </div>
      </div>

      {/* Warnings */}
      {data.warnings.length > 0 && (
        <div className="space-y-2">
          {data.warnings.map((w) => (
            <div key={w.key} className={`border rounded-xl px-4 py-2.5 text-sm flex items-center gap-2 ${w.severity === "critical" ? "bg-red-50 border-red-200 dark:bg-red-900/20 dark:border-red-800 text-red-600" : "bg-yellow-50 border-yellow-200 dark:bg-yellow-900/20 dark:border-yellow-800 text-yellow-600"}`}>
              <span>{w.severity === "critical" ? "🚨" : "⚠️"}</span>
              <span>{w.label} needs attention</span>
            </div>
          ))}
        </div>
      )}

      {/* Net worth summary */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4">
          <div className="text-xs text-[var(--muted)]">Net Worth</div>
          <div className="text-2xl font-bold">{bal(data.net_worth)}</div>
        </div>
        <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4">
          <div className="text-xs text-[var(--muted)]">Liquid</div>
          <div className="text-xl font-semibold">{bal(data.liquid_assets)}</div>
        </div>
        <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4">
          <div className="text-xs text-[var(--muted)]">Invested</div>
          <div className="text-xl font-semibold">{bal(data.invested_assets)}</div>
        </div>
      </div>

      {/* Metric cards */}
      <section>
        <h2 className="text-xs font-semibold text-[var(--muted)] uppercase tracking-wide mb-3">Key Metrics</h2>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
          <MetricCard metric={data.metrics.safe_to_spend} />
          <MetricCard metric={data.metrics.emergency_fund} extra={data.metrics.emergency_fund.months != null ? `${data.metrics.emergency_fund.months} months coverage` : undefined} />
          <MetricCard metric={data.metrics.savings_rate} extra={data.metrics.savings_rate.pct != null ? `${data.metrics.savings_rate.pct}% of income` : undefined} />
          <MetricCard metric={data.metrics.investment_rate} extra={data.metrics.investment_rate.pct != null ? `${data.metrics.investment_rate.pct}% of income` : undefined} />
          <MetricCard metric={data.metrics.cash_runway} extra={data.metrics.cash_runway.months != null ? `${data.metrics.cash_runway.months} months` : undefined} />
          {data.metrics.monthly_drift && (
            <MetricCard metric={data.metrics.monthly_drift} extra={data.metrics.monthly_drift.pct != null ? `${data.metrics.monthly_drift.pct > 0 ? "+" : ""}${data.metrics.monthly_drift.pct}% vs plan` : undefined} />
          )}
        </div>
      </section>

      {/* This month */}
      <section>
        <h2 className="text-xs font-semibold text-[var(--muted)] uppercase tracking-wide mb-3">This Month</h2>
        <div className="grid grid-cols-3 gap-3">
          <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-3">
            <div className="text-xs text-[var(--muted)]">Income</div>
            <div className="text-lg font-semibold text-green-600">{bal(data.total_in)}</div>
          </div>
          <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-3">
            <div className="text-xs text-[var(--muted)]">Expenses</div>
            <div className="text-lg font-semibold text-red-500">{bal(data.total_out)}</div>
          </div>
          <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-3">
            <div className="text-xs text-[var(--muted)]">Net</div>
            <div className={`text-lg font-semibold ${data.total_in - data.total_out >= 0 ? "text-green-600" : "text-red-500"}`}>
              {bal(data.total_in - data.total_out)}
            </div>
          </div>
        </div>
      </section>

      {/* Goals */}
      {data.goals.length > 0 && (
        <section>
          <h2 className="text-xs font-semibold text-[var(--muted)] uppercase tracking-wide mb-3">Goal Progress</h2>
          <div className="space-y-2">
            {data.goals.map((g, i) => (
              <div key={i} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3">
                <div className="flex justify-between items-center mb-1.5">
                  <span className="text-sm font-medium">{g.goal}</span>
                  <span className={`text-xs font-medium ${STATUS_COLOR[g.status]}`}>
                    {g.progress_pct}%{g.eta_months != null && g.eta_months > 0 ? ` · ${g.eta_months}mo` : g.eta_months === 0 ? " · Done!" : ""}
                  </span>
                </div>
                <div className="h-1.5 bg-[var(--bg)] rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full ${g.status === "ok" ? "bg-green-500" : g.status === "warn" ? "bg-yellow-400" : "bg-red-400"}`}
                    style={{ width: `${Math.min(g.progress_pct, 100)}%` }}
                  />
                </div>
                {g.required != null && (
                  <div className="text-xs text-[var(--muted)] mt-1">
                    Needs {fmtMoney(g.required)}/mo · {g.feasible ? "✓ on track" : `⚠ short by ${fmtMoney(g.required - g.available)}/mo`}
                  </div>
                )}
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
