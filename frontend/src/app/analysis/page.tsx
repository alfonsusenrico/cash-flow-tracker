"use client";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney, currentMonthYM } from "@/lib/utils";

interface AnalysisResponse {
  range: { from: string; to: string };
  month: string;
  total_asset: number;
  totals: { total_in: number; total_out: number; net: number };
  daily: { date: string; total_in: number; total_out: number; net: number }[];
  categories: { account_id: string; account_name: string; total_in: number; total_out: number; net: number; topup_base: number; usage_pct: number | null }[];
}

export default function AnalysisPage() {
  const [month, setMonth] = useState(currentMonthYM());
  const { data, isLoading } = useQuery<AnalysisResponse>({
    queryKey: ["analysis", month],
    queryFn: () => api.get(`/analysis?month=${month}`),
  });
  const maxOut = Math.max(...(data?.categories.map((c) => c.total_out) ?? [1]), 1);

  return (
    <div className="p-4 space-y-5 max-w-4xl mx-auto">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Analysis</h1>
        <input type="month" value={month} onChange={(e) => setMonth(e.target.value)}
          className="border border-[var(--border)] rounded px-2 py-1.5 text-sm bg-[var(--surface)] text-[var(--text)]" />
      </div>
      {isLoading && <p className="text-[var(--muted)]">Loading…</p>}
      {data && (
        <>
          <div className="grid grid-cols-3 gap-3">
            {[
              { label: "In", value: data.totals.total_in, cls: "text-green-600" },
              { label: "Out", value: data.totals.total_out, cls: "text-red-500" },
              { label: "Net", value: data.totals.net, cls: data.totals.net >= 0 ? "text-green-600" : "text-red-500" },
            ].map((item) => (
              <div key={item.label} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-3">
                <div className="text-xs text-[var(--muted)]">{item.label}</div>
                <div className={`text-lg font-bold ${item.cls}`}>{fmtMoney(item.value)}</div>
              </div>
            ))}
          </div>

          <section>
            <h2 className="text-xs font-semibold text-[var(--muted)] uppercase tracking-wide mb-2">Daily</h2>
            <div className="grid grid-cols-7 gap-1">
              {data.daily.map((day) => (
                <div key={day.date} className="bg-[var(--surface)] border border-[var(--border)] rounded p-1.5 text-center">
                  <div className="text-[10px] text-[var(--muted)]">
                    {new Date(day.date).toLocaleDateString("id-ID", { day: "numeric", month: "short" })}
                  </div>
                  <div className={`text-[11px] font-semibold ${day.net >= 0 ? "text-green-600" : "text-red-500"}`}>
                    {day.net !== 0 ? fmtMoney(Math.abs(day.net)) : "–"}
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section>
            <h2 className="text-xs font-semibold text-[var(--muted)] uppercase tracking-wide mb-2">Categories</h2>
            <div className="space-y-2">
              {data.categories.map((cat) => (
                <div key={cat.account_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3">
                  <div className="flex justify-between items-center mb-1">
                    <span className="text-sm font-medium">{cat.account_name}</span>
                    <span className="text-sm text-red-500 font-medium">{fmtMoney(cat.total_out)}</span>
                  </div>
                  <div className="h-1.5 bg-[var(--bg)] rounded-full overflow-hidden">
                    <div className="h-full bg-red-400 rounded-full" style={{ width: `${Math.min((cat.total_out / maxOut) * 100, 100)}%` }} />
                  </div>
                  {cat.usage_pct != null && <div className="text-xs text-[var(--muted)] mt-1">{cat.usage_pct}% of budget</div>}
                </div>
              ))}
            </div>
          </section>
        </>
      )}
    </div>
  );
}
