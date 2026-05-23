"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney, currentMonthYM } from "@/lib/utils";
import AppLayout from "@/components/layout/AppLayout";

interface AnalysisResponse {
  range: { from: string; to: string };
  month: string;
  total_asset: number;
  totals: { total_in: number; total_out: number; net: number };
  daily: { date: string; total_in: number; total_out: number; net: number }[];
  weekly: { from: string; to: string; total_in: number; total_out: number; net: number }[];
  categories: {
    account_id: string;
    account_name: string;
    total_in: number;
    total_out: number;
    net: number;
    topup_base: number;
    usage_pct: number | null;
  }[];
}

export default function AnalysisPage() {
  const [month, setMonth] = useState(currentMonthYM());

  const { data, isLoading } = useQuery<AnalysisResponse>({
    queryKey: ["analysis", month],
    queryFn: () => api.get(`/analysis?month=${month}`),
  });

  const maxOut = Math.max(...(data?.categories.map((c) => c.total_out) ?? [1]), 1);

  return (
    <AppLayout>
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold">Analysis</h1>
          <input
            type="month"
            value={month}
            onChange={(e) => setMonth(e.target.value)}
            className="border rounded px-2 py-1.5 text-sm dark:bg-gray-800 dark:border-gray-600"
          />
        </div>

        {isLoading && <p className="text-gray-500">Loading…</p>}

        {data && (
          <>
            {/* Monthly totals */}
            <div className="grid grid-cols-3 gap-4">
              {[
                { label: "Total In", value: data.totals.total_in, color: "text-green-600" },
                { label: "Total Out", value: data.totals.total_out, color: "text-red-500" },
                { label: "Net", value: data.totals.net, color: data.totals.net >= 0 ? "text-green-600" : "text-red-500" },
              ].map((item) => (
                <div key={item.label} className="bg-white dark:bg-gray-800 rounded-xl p-4 shadow">
                  <div className="text-xs text-gray-500 mb-1">{item.label}</div>
                  <div className={`text-xl font-bold ${item.color}`}>{fmtMoney(item.value)}</div>
                </div>
              ))}
            </div>

            {/* Daily grid */}
            <section>
              <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">Daily</h2>
              <div className="grid grid-cols-7 gap-1">
                {data.daily.map((day) => {
                  const net = day.net;
                  const isPos = net >= 0;
                  return (
                    <div
                      key={day.date}
                      className="bg-white dark:bg-gray-800 rounded-lg p-2 shadow-sm text-center"
                    >
                      <div className="text-xs text-gray-400">
                        {new Date(day.date).toLocaleDateString("id-ID", { day: "numeric", month: "short" })}
                      </div>
                      <div className={`text-xs font-semibold mt-0.5 ${isPos ? "text-green-600" : "text-red-500"}`}>
                        {net !== 0 ? fmtMoney(Math.abs(net)) : "–"}
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>

            {/* Categories */}
            <section>
              <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">
                Categories (by spend)
              </h2>
              <div className="space-y-2">
                {data.categories.map((cat) => (
                  <div key={cat.account_id} className="bg-white dark:bg-gray-800 rounded-xl px-4 py-3 shadow-sm">
                    <div className="flex justify-between items-center mb-1">
                      <span className="font-medium text-sm">{cat.account_name}</span>
                      <span className="text-sm text-red-500 font-medium">{fmtMoney(cat.total_out)}</span>
                    </div>
                    <div className="h-1.5 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-red-400 rounded-full"
                        style={{ width: `${Math.min((cat.total_out / maxOut) * 100, 100)}%` }}
                      />
                    </div>
                    {cat.usage_pct != null && (
                      <div className="text-xs text-gray-400 mt-1">{cat.usage_pct}% of budget</div>
                    )}
                  </div>
                ))}
              </div>
            </section>
          </>
        )}
      </div>
    </AppLayout>
  );
}
