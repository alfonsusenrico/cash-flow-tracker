"use client";
import { useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Modal } from "@/components/ui/Modal";
import { DonutChart } from "@/components/ui/DonutChart";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { Sparkline } from "@/components/ui/Sparkline";
import { PageHeader } from "@/components/ui/PageHeader";
import { MetricTile } from "@/components/ui/MetricTile";
import { Icon, type IconName } from "@/components/ui/Icon";
import type { SummaryResponse } from "@/types/domain";

interface SafeBreakdownAccount {
  account_id: string;
  account_name: string;
  profile_type: string;
  is_no_limit: boolean;
  balance: number;
}

interface SafeBreakdownAllocation {
  item_id: string;
  label: string;
  group: string;
  bucket_kind: string | null;
  bucket_name: string | null;
  target_account_id: string | null;
  target_account_name: string | null;
  planned_amount: number;
  funded_amount: number;
  remaining_amount: number;
  include_in_emergency_base: boolean;
  status: string;
}

interface SafeBreakdownPayable {
  obligation_id: string;
  title: string;
  due_date: string | null;
  outstanding_amount: number;
  counterparty_name: string | null;
}

interface SafeToSpendBreakdown {
  spendable_balance: number;
  planned_spending: number;
  actual_spending: number;
  remaining_spend_budget: number;
  committed_allocations: number;
  payables_due_this_cycle: number;
  payables_due_this_cycle_count: number;
  capped_available: number;
  final_safe_to_spend: number;
  spendable_accounts: SafeBreakdownAccount[];
  spending_allocations: SafeBreakdownAllocation[];
  payables_due: SafeBreakdownPayable[];
}

interface DashboardData {
  month: string;
  health_score: number;
  net_worth: number;
  liquid_assets: number;
  invested_assets: number;
  total_in: number;
  total_out: number;
  metrics: {
    safe_to_spend: { value: number; pct: number | null; status: string; label: string; breakdown?: SafeToSpendBreakdown };
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
    payable_due_this_cycle_count: number;
    due_soon: number;
    open_count: number;
    net_expected: number;
  };
  goals: { goal: string; required: number | null; available: number; feasible: boolean; status: string; progress_pct: number; eta_months: number | null }[];
  warnings: { key: string; label: string; severity: string }[];
}

const METRIC_HELP: Record<string, string> = {
  health_score: "Average of metric statuses: ok = 100, warn = 50, critical = 0.",
  safe_to_spend: "Spendable balance capped by remaining spending plan, minus payables due this cycle.",
  net_worth: "Current liquid account balances plus invested assets.",
  cash_runway: "Liquid assets divided by the monthly emergency spending base, converted to days.",
  monthly_drift: "Actual spending minus planned spending. Negative means under plan.",
};

const QUICK_ACTIONS: { label: string; icon: IconName; href: string; primary?: boolean }[] = [
  { label: "Add Transaction", icon: "plus", href: "/ledger?action=add", primary: true },
  { label: "Move Accounts", icon: "move", href: "/ledger?action=movement" },
  { label: "Record Net Worth", icon: "netWorth", href: "/net-worth?action=record" },
];

export default function DashboardPage() {
  const { hideBalances } = useAppCtx();
  const [safeBreakdownOpen, setSafeBreakdownOpen] = useState(false);
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

  if (dashLoading) return <div className="workbench-page text-[var(--muted)]">Loading…</div>;

  const score = dash?.health_score ?? 0;
  const scoreColor = score >= 70 ? "var(--primary)" : score >= 40 ? "var(--warning)" : "var(--danger)";
  const scoreLabel = score >= 70 ? "Good" : score >= 40 ? "Fair" : "Poor";
  const nwHistory = (nwData?.history ?? []).map((h) => h.net_worth).reverse();
  const safeBreakdown = dash?.metrics.safe_to_spend.breakdown;

  const metricEntries = dash ? Object.entries(dash.metrics).filter(([, v]) => v !== null) as [string, any][] : [];
  const metricValue = (key: string, m: any) => (
    key === "savings_rate" || key === "investment_rate" ? `${m.pct ?? 0}%`
      : key === "emergency_fund" ? `${m.months ?? 0} months`
      : key === "cash_runway" ? `${m.days ?? 0} days`
      : key === "monthly_drift" ? (m.value >= 0 ? `+${bal(m.value)}` : bal(m.value))
      : bal(m.value ?? 0)
  );

  return (
    <div className="workbench-page space-y-4">
      <PageHeader
        title="Summary"
        description="A compact view of spendable cash, plan drift, runway, and account movement for the active pay cycle."
      />
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
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)_minmax(0,1fr)]">
        {/* Health Score */}
        <Card>
          <SectionTitle>Health Score <span className="text-[var(--muted)] text-xs font-normal" title={METRIC_HELP.health_score}>ⓘ</span></SectionTitle>
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
            <span className="text-white/80 text-sm font-medium">Safe to Spend <span className="text-white/50 text-xs" title={METRIC_HELP.safe_to_spend}>ⓘ</span></span>
            <button type="button" onClick={() => setSafeBreakdownOpen((open) => !open)} className="text-xs text-white/80 border border-white/30 px-3 py-1 rounded-lg hover:bg-white/10 transition-colors">
              {safeBreakdownOpen ? "Hide breakdown" : "See breakdown →"}
            </button>
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
            <SectionTitle>Net Worth <span className="text-[var(--muted)] text-xs font-normal" title={METRIC_HELP.net_worth}>ⓘ</span></SectionTitle>
            <button type="button" disabled title="Dashboard period selection is coming soon" className="text-xs text-[var(--muted)] border border-[var(--border)] px-2 py-1 rounded-lg opacity-60 cursor-not-allowed">All time</button>
          </div>
          <p className="text-2xl font-bold tabular">{bal(nwData?.net_worth ?? 0)}</p>
          <p className="text-xs text-[var(--muted)] mt-0.5">Liquid balances plus invested assets</p>
          <div className="flex items-center justify-between mt-2">
            <div className="flex items-center gap-1 text-xs text-primary">
              <span>↑</span>
              <span className="font-medium">Growing</span>
            </div>
            {nwHistory.length > 1 && <Sparkline data={nwHistory} width={100} height={32} color="var(--primary)" />}
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

      <Modal open={safeBreakdownOpen && !!safeBreakdown} onClose={() => setSafeBreakdownOpen(false)} title="Safe to Spend Breakdown" wide>
        {safeBreakdown && (
          <>
          <p className="text-xs text-[var(--muted)] mb-4">
            min(spendable balance, remaining plan) - payables due = {bal(safeBreakdown.final_safe_to_spend)}
          </p>

          <div className="grid grid-cols-1 gap-3 mb-4 md:grid-cols-2 xl:grid-cols-4">
            {[
              { label: "Spendable Balance", value: safeBreakdown.spendable_balance, tone: "text-primary" },
              { label: "Remaining Plan", value: safeBreakdown.remaining_spend_budget, tone: "text-info" },
              { label: "Payables Due", value: safeBreakdown.payables_due_this_cycle, tone: "text-danger" },
              { label: "Safe to Spend", value: safeBreakdown.final_safe_to_spend, tone: "text-primary" },
            ].map((item) => (
              <div key={item.label} className="rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2">
                <p className="text-xs text-[var(--muted)]">{item.label}</p>
                <p className={`text-sm font-bold tabular ${item.tone}`}>{bal(item.value)}</p>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            <div>
              <p className="text-xs font-semibold mb-2">Spendable Accounts</p>
              <div className="space-y-2 max-h-40 overflow-y-auto pr-1">
                {safeBreakdown.spendable_accounts.map((account) => (
                  <div key={account.account_id} className="flex items-center justify-between gap-3 text-xs">
                    <div className="min-w-0">
                      <p className="font-medium truncate">{account.account_name}</p>
                      <p className="text-[var(--muted)]">{account.profile_type.replace("_", " ")}{account.is_no_limit ? " · no limit" : ""}</p>
                    </div>
                    <span className="font-semibold tabular text-right">{bal(account.balance)}</span>
                  </div>
                ))}
                {safeBreakdown.spendable_accounts.length === 0 && <p className="text-xs text-[var(--muted)]">No spendable accounts</p>}
              </div>
            </div>

            <div>
              <p className="text-xs font-semibold mb-2">Spending Allocations</p>
              <div className="space-y-2 max-h-40 overflow-y-auto pr-1">
                {safeBreakdown.spending_allocations.map((item) => (
                  <div key={item.item_id} className="flex items-center justify-between gap-3 text-xs">
                    <div className="min-w-0">
                      <p className="font-medium truncate">{item.label}</p>
                      <p className="text-[var(--muted)] truncate">{item.target_account_name ?? item.bucket_name ?? item.group.replace("_", " ")}</p>
                    </div>
                    <div className="text-right">
                      <p className="font-semibold tabular">{bal(item.planned_amount)}</p>
                      <p className="text-[var(--muted)] tabular">{bal(item.remaining_amount)} left</p>
                    </div>
                  </div>
                ))}
                {safeBreakdown.spending_allocations.length === 0 && <p className="text-xs text-[var(--muted)]">No spending allocations</p>}
              </div>
            </div>

            <div>
              <p className="text-xs font-semibold mb-2">Payables Due This Cycle</p>
              <div className="space-y-2 max-h-40 overflow-y-auto pr-1">
                {safeBreakdown.payables_due.map((payable) => (
                  <div key={payable.obligation_id} className="flex items-center justify-between gap-3 text-xs">
                    <div className="min-w-0">
                      <p className="font-medium truncate">{payable.title}</p>
                      <p className="text-[var(--muted)] truncate">{payable.counterparty_name ?? "No counterparty"}{payable.due_date ? ` · due ${payable.due_date}` : ""}</p>
                    </div>
                    <span className="font-semibold tabular text-danger text-right">{bal(payable.outstanding_amount)}</span>
                  </div>
                ))}
                {safeBreakdown.payables_due.length === 0 && <p className="text-xs text-[var(--muted)]">No due payables</p>}
                {safeBreakdown.payables_due_this_cycle_count > safeBreakdown.payables_due.length && (
                  <p className="text-xs text-[var(--muted)]">
                    +{safeBreakdown.payables_due_this_cycle_count - safeBreakdown.payables_due.length} more payable item
                  </p>
                )}
              </div>
            </div>
          </div>
          </>
        )}
      </Modal>

      {/* Row 1b: Payables and receivables */}
      {dash?.obligations && dash.obligations.open_count > 0 && (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
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
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-6">
        {metricEntries.map(([key, m]) => (
          <MetricTile
            key={key}
            label={m.label}
            tone={m.status === "critical" ? "negative" : m.status === "warn" ? "warning" : "positive"}
            value={metricValue(key, m)}
            detail={
              <>
                {key === "savings_rate" || key === "investment_rate" ? "of income"
                : key === "emergency_fund" || key === "cash_runway" ? "of expenses"
                : key === "monthly_drift" ? "vs plan"
                : `${m.pct ?? 0}% of income`}
                {METRIC_HELP[key] && <span title={METRIC_HELP[key]}> · info</span>}
              </>
            }
          />
        ))}
      </div>

      {/* Row 3: Accounts Overview + Goals Progress */}
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(320px,0.85fr)]">
        {/* Accounts Overview */}
        <div className="min-w-0">
          <Card padding="sm">
            <SectionTitle>Accounts Overview <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
            <div className="overflow-x-auto">
            <table className="w-full min-w-[760px] text-xs">
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
                        <span className={`h-2 w-2 rounded-full ${acc.current_balance > 0 ? "bg-[var(--primary)]" : "bg-[var(--color-rule-strong)]"}`} />
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
            </div>
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
                  <ProgressBar value={g.progress_pct} color={g.feasible ? "green" : "yellow"} intent="completion" />
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
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3 xl:grid-cols-1">
              {QUICK_ACTIONS.map((a) => (
                <a key={a.label} href={a.href} className={`flex items-center gap-2 rounded-[var(--radius-md)] px-3 py-2 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus)] ${a.primary ? "bg-primary text-white hover:bg-primary-hover" : "bg-[var(--bg)] hover:bg-[var(--border)] text-[var(--text)]"}`}>
                  <Icon name={a.icon} className="h-4 w-4 shrink-0" />
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
