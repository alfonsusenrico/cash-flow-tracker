"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtIDR, currentMonthYM } from "@/lib/utils";
import type { SummaryResponse } from "@/types/domain";

export default function DashboardPage() {
  const month = currentMonthYM();
  const { data, isLoading, error } = useQuery<SummaryResponse>({
    queryKey: ["summary", month],
    queryFn: () => api.get<SummaryResponse>(`/summary?month=${month}`),
  });

  if (isLoading) return <div className="p-8 text-gray-500">Loading…</div>;
  if (error) return <div className="p-8 text-red-500">{String(error)}</div>;
  if (!data) return null;

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Summary</h1>
        <span className="text-sm text-gray-500">
          {data.range.from} – {data.range.to}
        </span>
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-xl p-4 shadow">
        <div className="text-sm text-gray-500">Total Asset</div>
        <div className="text-3xl font-bold mt-1">{fmtIDR(data.total_asset)}</div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {data.accounts.map((acc) => (
          <div
            key={acc.account_id}
            className="bg-white dark:bg-gray-800 rounded-xl p-4 shadow space-y-2"
          >
            <div className="font-semibold truncate">{acc.account_name}</div>
            <div className="flex justify-between text-sm">
              <span className="text-gray-500">Balance</span>
              <span className="font-medium">{fmtIDR(acc.current_balance)}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-green-600">In</span>
              <span>{fmtIDR(acc.total_in)}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-red-500">Out</span>
              <span>{fmtIDR(acc.total_out)}</span>
            </div>
            {acc.budget != null && (
              <div className="mt-2">
                <div className="flex justify-between text-xs text-gray-500 mb-1">
                  <span>Budget</span>
                  <span>
                    {acc.budget_pct ?? 0}% of {fmtIDR(acc.budget)}
                  </span>
                </div>
                <div className="h-1.5 bg-gray-200 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full ${
                      acc.budget_status === "critical"
                        ? "bg-red-500"
                        : acc.budget_status === "warn"
                          ? "bg-yellow-400"
                          : "bg-green-500"
                    }`}
                    style={{ width: `${Math.min(acc.budget_pct ?? 0, 100)}%` }}
                  />
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
