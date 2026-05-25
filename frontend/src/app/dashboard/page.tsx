"use client";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import type { SummaryResponse } from "@/types/domain";

export default function DashboardPage() {
  const { hideBalances } = useAppCtx();
  const { data, isLoading, error } = useQuery<SummaryResponse>({
    queryKey: ["summary"],
    queryFn: () => api.get("/summary"),
  });

  const bal = (n: number) => hideBalances ? "••••" : fmtMoney(n);

  if (isLoading) return <div className="p-8 text-[var(--muted)]">Loading…</div>;
  if (error) return <div className="p-8 text-[var(--danger)]">{String(error)}</div>;
  if (!data) return null;

  return (
    <div className="p-4 space-y-4 max-w-4xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Summary</h1>
          <p className="text-xs text-[var(--muted)]">{data.range.from} – {data.range.to} · payday {data.payday.default_day}</p>
        </div>
        <div className="text-right">
          <div className="text-xs text-[var(--muted)]">Total Asset</div>
          <div className="text-2xl font-bold">{bal(data.total_asset)}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {data.accounts.map((acc) => (
          <div key={acc.account_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4 space-y-3">
            <div className="font-semibold truncate">{acc.account_name}</div>

            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
              <span className="text-[var(--muted)] text-xs">Balance</span>
              <span className="text-right font-medium">{bal(acc.current_balance)}</span>
              <span className="text-[var(--muted)] text-xs">In</span>
              <span className="text-right text-green-600">{bal(acc.total_in)}</span>
              <span className="text-[var(--muted)] text-xs">Out</span>
              <span className="text-right text-red-500">{bal(acc.total_out)}</span>
            </div>

            {acc.budget != null && (
              <div>
                <div className="flex justify-between text-xs text-[var(--muted)] mb-1">
                  <span>Budget</span>
                  <span>{acc.budget_pct ?? 0}% of {bal(acc.budget)}</span>
                </div>
                <div className="h-1.5 bg-[var(--bg)] rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${
                      acc.budget_status === "critical" ? "bg-red-500" :
                      acc.budget_status === "warn" ? "bg-yellow-400" : "bg-green-500"
                    }`}
                    style={{ width: `${Math.min(acc.budget_pct ?? 0, 100)}%` }}
                  />
                </div>
                {acc.budget_remaining != null && (
                  <p className="text-xs text-[var(--muted)] mt-1">
                    {acc.budget_remaining >= 0 ? `${bal(acc.budget_remaining)} remaining` : `${bal(Math.abs(acc.budget_remaining))} over budget`}
                  </p>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
