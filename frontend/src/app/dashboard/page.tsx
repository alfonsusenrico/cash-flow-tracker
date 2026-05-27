"use client";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { DonutChart } from "@/components/ui/DonutChart";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { Sparkline } from "@/components/ui/Sparkline";
import type { SummaryResponse } from "@/types/domain";

interface DashboardData {
  month: string;
  health_score: number;
  net_worth: number;
  liquid_assets: number;
  invested_assets: number;
  total_in: number;
  total_out: number;
  metrics: {
    safe_to_spend: { value: number; pct: number | null; status: string; label: string };
    emergency_fund: { value: number; months: number | null; status: string; label: string };
    savings_rate: { value: number; pct: number | null; status: string; label: string };
    investment_rate: { value: number; pct: number | null; status: string; label: string };
    cash_runway: { value: number; months: number | null; days: number | null; status: string; label: string };
    monthly_drift: { value: number; pct: number | null; status: string; label: string } | null;
  };
  obligations: {
    receivable_outstanding: number;
    payable_outstanding: number;
    receivable_overdue: number;
    payable_overdue: number;
    payable_due_this_cycle: number;
    due_soon: number;
    open_count: number;
    net_expected: number;
  };
  goals: { goal: string; required: number | null; available: number; feasible: boolean; status: string; progress_pct: number; eta_months: number | null }[];
  warnings: { key: string; label: string; severity: string }[];
}

const METRIC_ICONS: Record<string, string> = {
  safe_to_spend: "💳",
  emergency_fund: "🛡️",
  savings_rate: "💰",
  investment_rate: "📈",
  cash_runway: "⏱️",
  monthly_drift: "📉",
};

const STATUS_VALUE_COLOR: Record<string, string> = {
  ok: "text-primary",
  warn: "text-warning",
  critical: "text-danger",
};

export default function DashboardPage() {
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••••" : fmtMoney(n);

  const { data: dash, isLoading: dashLoading } = useQuery<DashboardData>({
    queryKey: ["dashboard"],
    queryFn: () => api.get("/dashboard"),
  });

  const { data: summary } = useQuery<SummaryResponse>({
    queryKey: ["summary"],
    queryFn: () => api.get("/summary"),
  });

  const { data: nwData } = useQuery<{ net_worth: number; liquid_assets: number; invested_assets: number; history: { as_of_date: string; net_worth: number }[] }>({
    queryKey: ["net-worth"],
    queryFn: () => api.get("/assets/net-worth"),
  });

  if (dashLoading) return <div className="p-6 text-[var(--muted)]">Loading…</div>;

  const score = dash?.health_score ?? 0;
  const scoreColor = score >= 70 ? "#16a34a" : score >= 40 ? "#f59e0b" : "#dc2626";
  const scoreLabel = score >= 70 ? "Good" : score >= 40 ? "Fair" : "Poor";
  const nwHistory = (nwData?.history ?? []).map((h) => h.net_worth).reverse();

  const metricEntries = dash ? Object.entries(dash.metrics).filter(([, v]) => v !== null) as [string, any][] : [];

  return (
    <div className="p-5 space-y-4">
      {/* Warning banner */}
      {dash?.warnings?.filter((w) => w.severity === "critical").map((w) => (
        <div key={w.key} className="flex items-center justify-between px-4 py-3 rounded-xl border border-yellow-200 bg-yellow-50 dark:bg-yellow-900/20 dark:border-yellow-800">
          <div className="flex items-center gap-3">
            <span className="text-warning text-lg">⚠️</span>
            <div>
              <p className="text-sm font-semibold text-yellow-800 dark:text-yellow-300">Attention needed</p>
              <p className="text-xs text-yellow-700 dark:text-yellow-400">{w.label}</p>
            </div>
          </div>
          <Link href="/analysis" prefetch={false} className="text-xs font-semibold text-warning border border-warning/30 px-3 py-1.5 rounded-lg hover:bg-warning/10 transition-colors">Review now</Link>
        </div>
      ))}

      {/* Row 1: Health Score + Safe to Spend + Net Worth */}
      <div className="grid grid-cols-3 gap-4">
        {/* Health Score */}
        <Card>
          <SectionTitle>Health Score <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
          <div className="flex flex-col items-center py-2">
            <DonutChart value={score} size={120} color={scoreColor} label={String(score)} sublabel={scoreLabel} />
            <p className="text-xs text-[var(--muted)] text-center mt-3 leading-relaxed">
              {score >= 70 ? "You're on track! Keep building your emergency fund and reducing expenses." : "Review your spending and savings to improve your score."}
            </p>
            <Link href="/analysis" prefetch={false} className="mt-3 text-xs text-primary border border-primary/30 px-4 py-1.5 rounded-lg hover:bg-primary/5 transition-colors">View full analysis</Link>
          </div>
        </Card>

        {/* Safe to Spend */}
        <Card green>
          <div className="flex items-center justify-between mb-2">
            <span className="text-white/80 text-sm font-medium">Safe to Spend <span className="text-white/50 text-xs">ⓘ</span></span>
            <Link href="/ledger" prefetch={false} className="text-xs text-white/80 border border-white/30 px-3 py-1 rounded-lg hover:bg-white/10 transition-colors">See breakdown →</Link>
          </div>
          <div className="py-3">
            <p className="text-3xl font-bold text-white tabular">{bal(dash?.metrics.safe_to_spend.value ?? 0)}</p>
            <p className="text-white/70 text-sm mt-1">for the rest of this pay cycle</p>
          </div>
          <div className="mt-3 flex items-center gap-2 bg-white/10 rounded-lg px-3 py-2">
            <span className="text-white text-sm">✓</span>
            <div>
              <p className="text-white text-sm font-medium">You're within plan</p>
              <p className="text-white/70 text-xs">
                {dash?.metrics.monthly_drift?.value != null && dash.metrics.monthly_drift.value < 0
                  ? `Great job! You're ${bal(Math.abs(dash.metrics.monthly_drift.value))} under your plan.`
                  : "Keep tracking your spending."}
              </p>
            </div>
          </div>
        </Card>

        {/* Net Worth */}
        <Card>
          <div className="flex items-center justify-between mb-1">
            <SectionTitle>Net Worth <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
            <button type="button" disabled title="Dashboard period selection is coming soon" className="text-xs text-[var(--muted)] border border-[var(--border)] px-2 py-1 rounded-lg opacity-60 cursor-not-allowed">All time</button>
          </div>
          <p className="text-2xl font-bold tabular">{bal(nwData?.net_worth ?? 0)}</p>
          <p className="text-xs text-[var(--muted)] mt-0.5">Total Assets minus Liabilities</p>
          <div className="flex items-center justify-between mt-2">
            <div className="flex items-center gap-1 text-xs text-primary">
              <span>↑</span>
              <span className="font-medium">Growing</span>
            </div>
            {nwHistory.length > 1 && <Sparkline data={nwHistory} width={100} height={32} color="#16a34a" />}
          </div>
          <div className="mt-3 pt-3 border-t border-[var(--border)]">
            <p className="text-xs text-[var(--muted)] mb-2">This Month ({summary?.range?.from} – {summary?.range?.to}) <span className="float-right"><Link href="/analysis" prefetch={false} className="text-primary hover:underline">View details</Link></span></p>
            <div className="grid grid-cols-3 gap-2 text-xs">
              <div><p className="text-[var(--muted)]">Income</p><p className="font-semibold text-primary tabular">{bal(dash?.total_in ?? 0)}</p></div>
              <div><p className="text-[var(--muted)]">Expenses</p><p className="font-semibold text-danger tabular">{bal(dash?.total_out ?? 0)}</p></div>
              <div><p className="text-[var(--muted)]">Net</p><p className={`font-semibold tabular ${(dash?.total_in ?? 0) - (dash?.total_out ?? 0) >= 0 ? "text-primary" : "text-danger"}`}>{bal((dash?.total_in ?? 0) - (dash?.total_out ?? 0))}</p></div>
            </div>
          </div>
        </Card>
      </div>

      {/* Row 1b: Payables and receivables */}
      {dash?.obligations && dash.obligations.open_count > 0 && (
        <div className="grid grid-cols-4 gap-3">
          <Card padding="sm">
            <p className="text-xs text-[var(--muted)]">Receivable</p>
            <p className="text-lg font-bold tabular text-primary">{bal(dash.obligations.receivable_outstanding)}</p>
            <p className="text-xs text-[var(--muted)]">Expected money in, not counted as cash yet</p>
          </Card>
          <Card padding="sm">
            <p className="text-xs text-[var(--muted)]">Payable</p>
            <p className="text-lg font-bold tabular text-danger">{bal(dash.obligations.payable_outstanding)}</p>
            <p className="text-xs text-[var(--muted)]">{bal(dash.obligations.payable_due_this_cycle)} due this pay cycle</p>
          </Card>
          <Card padding="sm">
            <p className="text-xs text-[var(--muted)]">Overdue</p>
            <p className="text-lg font-bold tabular text-warning">{bal(dash.obligations.receivable_overdue + dash.obligations.payable_overdue)}</p>
            <p className="text-xs text-[var(--muted)]">Receivable and payable past due</p>
          </Card>
          <Card padding="sm">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-[var(--muted)]">Net Expected</p>
                <p className={`text-lg font-bold tabular ${dash.obligations.net_expected >= 0 ? "text-primary" : "text-danger"}`}>{bal(dash.obligations.net_expected)}</p>
              </div>
              <Link href="/obligations" prefetch={false} className="text-xs text-primary hover:underline">Manage</Link>
            </div>
            <p className="text-xs text-[var(--muted)]">{dash.obligations.open_count} open item{dash.obligations.open_count === 1 ? "" : "s"}</p>
          </Card>
        </div>
      )}

      {/* Row 2: Metric cards */}
      <div className="grid grid-cols-6 gap-3">
        {metricEntries.map(([key, m]) => (
          <Card key={key} padding="sm">
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 rounded-lg bg-[var(--bg)] flex items-center justify-center text-base">{METRIC_ICONS[key] ?? "📊"}</div>
              <p className="text-xs text-[var(--muted)] leading-tight">{m.label}</p>
            </div>
            <p className={`text-base font-bold tabular ${STATUS_VALUE_COLOR[m.status] ?? "text-[var(--text)]"}`}>
              {key === "savings_rate" || key === "investment_rate" ? `${m.pct ?? 0}%`
                : key === "emergency_fund" ? `${m.months ?? 0} months`
                : key === "cash_runway" ? `${m.days ?? 0} days`
                : key === "monthly_drift" ? (m.value >= 0 ? `+${bal(m.value)}` : bal(m.value))
                : bal(m.value ?? 0)}
            </p>
            <p className="text-xs text-[var(--muted)] mt-0.5">
              {key === "savings_rate" || key === "investment_rate" ? "of income"
                : key === "emergency_fund" || key === "cash_runway" ? "of expenses"
                : key === "monthly_drift" ? "vs plan"
                : `${m.pct ?? 0}% of income`}
            </p>
          </Card>
        ))}
      </div>

      {/* Row 3: Accounts Overview + Goals Progress */}
      <div className="grid grid-cols-3 gap-4">
        {/* Accounts Overview */}
        <div className="col-span-2">
          <Card padding="sm">
            <SectionTitle>Accounts Overview <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                  <th className="text-left pb-2 font-medium">Account</th>
                  <th className="text-right pb-2 font-medium">Starting Balance</th>
                  <th className="text-right pb-2 font-medium">Current Balance</th>
                  <th className="text-right pb-2 font-medium">In</th>
                  <th className="text-right pb-2 font-medium">Out</th>
                  <th className="text-right pb-2 font-medium">Quota Left</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {summary?.accounts?.map((acc) => (
                  <tr key={acc.account_id} className="hover:bg-[var(--bg)] transition-colors">
                    <td className="py-2">
                      <div className="flex items-center gap-2">
                        <div className="w-6 h-6 rounded-md bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center text-xs">🏦</div>
                        <div>
                          <p className="font-medium text-[var(--text)]">{acc.account_name}</p>
                        </div>
                      </div>
                    </td>
                    <td className="py-2 text-right tabular">{bal(acc.starting_balance)}</td>
                    <td className="py-2 text-right tabular font-medium">{bal(acc.current_balance)}</td>
                    <td className="py-2 text-right tabular text-primary">{bal(acc.total_in)}</td>
                    <td className="py-2 text-right tabular text-danger">{bal(acc.total_out)}</td>
                    <td className="py-2 text-right">
                      {acc.budget_pct != null ? (
                        <div className="flex items-center gap-1.5 justify-end">
                          <ProgressBar value={Math.max(0, 100 - acc.budget_pct)} intent="quota" size="sm" className="w-16" />
                          <span className="tabular text-[var(--muted)]">{Math.max(0, 100 - acc.budget_pct)}%</span>
                        </div>
                      ) : <span className="text-[var(--muted)]">—</span>}
                    </td>
                  </tr>
                ))}
                {summary?.accounts && (
                  <tr className="font-semibold border-t-2 border-[var(--border)]">
                    <td className="py-2">Total</td>
                    <td className="py-2 text-right tabular">{bal(summary.accounts.reduce((s, a) => s + a.starting_balance, 0))}</td>
                    <td className="py-2 text-right tabular">{bal(summary.accounts.reduce((s, a) => s + a.current_balance, 0))}</td>
                    <td className="py-2 text-right tabular text-primary">{bal(summary.accounts.reduce((s, a) => s + a.total_in, 0))}</td>
                    <td className="py-2 text-right tabular text-danger">{bal(summary.accounts.reduce((s, a) => s + a.total_out, 0))}</td>
                    <td className="py-2 text-right text-[var(--muted)]">—</td>
                  </tr>
                )}
              </tbody>
            </table>
          </Card>
        </div>

        {/* Goals + Quick Actions */}
        <div className="space-y-3">
          <Card padding="sm">
            <SectionTitle>
              Goals Progress <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span>
              <Link href="/goals" prefetch={false} className="text-xs text-primary hover:underline ml-auto">View all</Link>
            </SectionTitle>
            <div className="space-y-3">
              {dash?.goals?.slice(0, 3).map((g, i) => (
                <div key={i}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-medium truncate">{g.goal}</span>
                    <span className="text-xs text-[var(--muted)] tabular ml-2">{g.progress_pct}%</span>
                  </div>
                  <ProgressBar value={g.progress_pct} color={g.feasible ? "green" : "yellow"} />
                </div>
              ))}
              {(!dash?.goals || dash.goals.length === 0) && (
                <p className="text-xs text-[var(--muted)] text-center py-2">No active goals</p>
              )}
            </div>
          </Card>

          {/* Quick Actions */}
          <Card padding="sm">
            <SectionTitle>Quick Actions</SectionTitle>
            <div className="grid grid-cols-3 gap-2">
              {[
                { label: "Add Transaction", icon: "+", href: "/ledger?action=add", primary: true },
                { label: "Transfer", icon: "⇄", href: "/ledger?action=transfer" },
                { label: "Record Net Worth", icon: "📌", href: "/net-worth?action=record" },
              ].map((a) => (
                <a key={a.label} href={a.href} className={`flex flex-col items-center gap-1.5 p-3 rounded-xl text-center transition-colors ${a.primary ? "bg-primary text-white hover:bg-primary-hover" : "bg-[var(--bg)] hover:bg-[var(--border)] text-[var(--text)]"}`}>
                  <span className="text-lg">{a.icon}</span>
                  <span className="text-xs font-medium leading-tight">{a.label}</span>
                </a>
              ))}
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}
