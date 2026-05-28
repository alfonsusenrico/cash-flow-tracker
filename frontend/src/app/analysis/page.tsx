"use client";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney, currentMonthYM } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Sparkline } from "@/components/ui/Sparkline";

interface AnalysisData {
  range: { from: string; to: string };
  totals: { total_in: number; total_out: number; net: number };
  daily: { date: string; total_in: number; total_out: number; net: number }[];
  categories: { account_id: string; account_name: string; total_in: number; total_out: number; net: number; topup_base: number; usage_pct: number | null }[];
}

const CAT_COLORS = ["#dc2626","#f59e0b","#f97316","#16a34a","#3b82f6","#8b5cf6","#ec4899","#6b7280"];

export default function AnalysisPage() {
  const { hideBalances } = useAppCtx();
  const month = currentMonthYM();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);

  const { data, isLoading } = useQuery<AnalysisData>({
    queryKey: ["analysis", month],
    queryFn: () => api.get(`/analysis?month=${month}`),
  });

  const maxOut = Math.max(...(data?.categories.map((c) => c.total_out) ?? [1]), 1);
  const totalExpenses = data?.totals.total_out ?? 0;

  // Build calendar grid
  const dailyByDate: Record<string, { net: number; total_in: number; total_out: number }> = {};
  data?.daily.forEach((d) => { dailyByDate[d.date] = d; });

  const fromDate = data?.range.from ? new Date(data.range.from) : null;
  const toDate = data?.range.to ? new Date(data.range.to) : null;

  // Generate calendar weeks
  const calendarDays: (string | null)[] = [];
  if (fromDate && toDate) {
    const start = new Date(fromDate);
    const dayOfWeek = start.getDay() === 0 ? 6 : start.getDay() - 1; // Mon=0
    for (let i = 0; i < dayOfWeek; i++) calendarDays.push(null);
    const cur = new Date(start);
    while (cur <= toDate) {
      calendarDays.push(cur.toISOString().slice(0, 10));
      cur.setDate(cur.getDate() + 1);
    }
  }

  const netHistory = data?.daily.map((d) => d.net) ?? [];

  return (
    <div className="workbench-page space-y-4">
      {/* Tabs + period toggle */}
      <div className="flex items-center justify-between">
        <div className="flex gap-1 border-b border-[var(--border)]">
          {(["monthly", "weekly", "daily"] as const).map((t) => (
            <button key={t} type="button" disabled={t !== "monthly"} title={t !== "monthly" ? "Weekly and daily review modes are coming soon" : undefined}
              className={`px-4 py-2 text-sm font-medium capitalize transition-colors border-b-2 -mb-px ${t === "monthly" ? "border-primary text-primary" : "border-transparent text-[var(--muted)] opacity-50 cursor-not-allowed"}`}>
              {t === "monthly" ? "Monthly Review" : t === "weekly" ? "Weekly" : "Daily"}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <button type="button" disabled className="px-3 py-1.5 rounded-lg border border-primary text-primary text-xs font-medium bg-primary/5">This Cycle</button>
          <button type="button" disabled title="Cycle comparison is coming soon" className="px-3 py-1.5 rounded-lg border border-[var(--border)] text-[var(--muted)] text-xs font-medium opacity-50 cursor-not-allowed">vs Last Cycle</button>
          <button type="button" disabled title="Analysis filters are coming soon" className="px-3 py-1.5 rounded-lg border border-[var(--border)] text-[var(--muted)] text-xs font-medium opacity-50 cursor-not-allowed">⊟ Filters</button>
        </div>
      </div>

      {isLoading && <div className="text-[var(--muted)] text-sm">Loading…</div>}

      {data && (
        <>
          {/* Top 3 stat cards */}
          <div className="grid grid-cols-3 gap-4">
            {[
              { label: "Total Income", value: data.totals.total_in, color: "text-primary", sparkColor: "#16a34a" },
              { label: "Total Expenses", value: data.totals.total_out, color: "text-danger", sparkColor: "#dc2626" },
              { label: "Net (Income - Expenses)", value: data.totals.net, color: data.totals.net >= 0 ? "text-info" : "text-danger", sparkColor: "#3b82f6" },
            ].map((item) => (
              <Card key={item.label}>
                <p className="text-xs text-[var(--muted)] mb-1">{item.label} <span className="text-[var(--muted)] text-xs">ⓘ</span></p>
                <div className="flex items-end justify-between">
                  <p className={`text-2xl font-bold tabular ${item.color}`}>{bal(item.value)}</p>
                  <Sparkline data={netHistory.slice(-15)} width={80} height={32} color={item.sparkColor} />
                </div>
                <p className="text-xs text-[var(--muted)] mt-1">vs last cycle</p>
              </Card>
            ))}
          </div>

          <div className="grid grid-cols-3 gap-4">
            {/* Daily calendar grid */}
            <div className="col-span-2">
              <Card>
                <div className="flex items-center justify-between mb-3">
                  <SectionTitle>Daily Net (Income - Expenses) <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
                  <select className="text-xs border border-[var(--border)] rounded px-2 py-1 bg-[var(--surface)]">
                    <option>Net (Rp)</option>
                  </select>
                </div>
                {/* Day headers */}
                <div className="grid grid-cols-7 gap-0.5 mb-1">
                  {["Mon","Tue","Wed","Thu","Fri","Sat","Sun"].map((d) => (
                    <div key={d} className="text-center text-xs text-[var(--muted)] font-medium py-1">{d}</div>
                  ))}
                </div>
                {/* Calendar cells */}
                <div className="grid grid-cols-7 gap-0.5">
                  {calendarDays.map((dateStr, i) => {
                    if (!dateStr) return <div key={i} />;
                    const d = dailyByDate[dateStr];
                    const net = d?.net ?? 0;
                    const isToday = dateStr === new Date().toISOString().slice(0, 10);
                    return (
                      <div key={dateStr} className={`p-1.5 rounded text-center text-xs cursor-pointer hover:bg-[var(--bg)] transition-colors ${isToday ? "border border-primary" : ""}`}>
                        <p className="text-[var(--muted)] text-[10px]">{new Date(dateStr + "T00:00:00").getDate()}</p>
                        {net !== 0 && (
                          <p className={`font-medium text-[10px] tabular ${net > 0 ? "text-primary" : "text-danger"}`}>
                            {net > 0 ? "+" : ""}{(net / 1000).toFixed(0)}k
                          </p>
                        )}
                        {net === 0 && <p className="text-[var(--muted)] text-[10px]">—</p>}
                      </div>
                    );
                  })}
                </div>
                {/* Color scale */}
                <div className="flex items-center gap-2 mt-3">
                  <div className="flex-1 h-1.5 rounded-full" style={{ background: "linear-gradient(to right, #dc2626, #f5f5f5, #16a34a)" }} />
                  <div className="flex justify-between w-full text-[10px] text-[var(--muted)] -mt-1">
                    <span>-1.000.000</span><span>0</span><span>+1.000.000</span>
                  </div>
                </div>

                {/* Cash Flow Trend */}
                <div className="mt-4 pt-4 border-t border-[var(--border)]">
                  <div className="flex items-center justify-between mb-2">
                    <p className="text-sm font-semibold">Cash Flow Trend <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></p>
                    <select className="text-xs border border-[var(--border)] rounded px-2 py-1 bg-[var(--surface)]"><option>Cumulative</option></select>
                  </div>
                  <div className="flex items-center gap-4 text-xs text-[var(--muted)] mb-2">
                    <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-primary inline-block" /> Income</span>
                    <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-danger inline-block" /> Expenses</span>
                    <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-info inline-block" /> Net</span>
                  </div>
                  <div className="h-32 relative">
                    <svg width="100%" height="100%" viewBox="0 0 400 128" preserveAspectRatio="none">
                      {data.daily.length > 1 && (() => {
                        const pts = data.daily.map((d, i) => ({ x: (i / (data.daily.length - 1)) * 400, in: d.total_in, out: d.total_out, net: d.net }));
                        const maxVal = Math.max(...pts.map((p) => Math.max(p.in, p.out)), 1);
                        const toY = (v: number) => 128 - (v / maxVal) * 120 - 4;
                        const line = (key: "in" | "out" | "net", color: string) => {
                          const d = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x},${toY(key === "in" ? p.in : key === "out" ? p.out : p.net + maxVal / 2)}`).join(" ");
                          return <path key={key} d={d} fill="none" stroke={color} strokeWidth={1.5} strokeLinecap="round" />;
                        };
                        return [line("in", "#16a34a"), line("out", "#dc2626"), line("net", "#3b82f6")];
                      })()}
                    </svg>
                  </div>
                </div>
              </Card>
            </div>

            {/* Spending by Category + Insights */}
            <div className="space-y-4">
              <Card>
                <SectionTitle>
                  Spending by Category <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span>
                  <Link href="/ledger" prefetch={false} className="text-xs text-primary hover:underline ml-auto">View all</Link>
                </SectionTitle>
                <div className="space-y-2.5">
                  {data.categories.slice(0, 8).map((cat, i) => (
                    <div key={cat.account_id}>
                      <div className="flex items-center justify-between mb-1">
                        <div className="flex items-center gap-2">
                          <div className="w-5 h-5 rounded flex items-center justify-center text-xs" style={{ background: CAT_COLORS[i % CAT_COLORS.length] + "20" }}>
                            <span style={{ color: CAT_COLORS[i % CAT_COLORS.length] }}>●</span>
                          </div>
                          <span className="text-xs text-[var(--text)]">{cat.account_name}</span>
                        </div>
                        <div className="flex items-center gap-2 text-xs">
                          <span className="tabular text-[var(--text)]">{bal(cat.total_out)}</span>
                          <span className="text-[var(--muted)] w-8 text-right">{totalExpenses > 0 ? Math.round((cat.total_out / totalExpenses) * 100) : 0}%</span>
                        </div>
                      </div>
                      <div className="h-1 bg-[var(--bg)] rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${(cat.total_out / maxOut) * 100}%`, background: CAT_COLORS[i % CAT_COLORS.length] }} />
                      </div>
                    </div>
                  ))}
                  <div className="pt-2 border-t border-[var(--border)] flex justify-between text-xs font-semibold">
                    <span>Total Expenses</span>
                    <span className="text-danger tabular">{bal(totalExpenses)}</span>
                  </div>
                </div>
              </Card>

              <Card>
                <SectionTitle>
                  Insights <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span>
                  <button type="button" disabled title="Expanded insights are coming soon" className="text-xs text-[var(--muted)] ml-auto cursor-not-allowed">View all insights</button>
                </SectionTitle>
                <div className="space-y-3">
                  {[
                    { icon: "📈", color: "text-primary", title: `Great! Your savings rate is ${data.totals.total_in > 0 ? Math.round(((data.totals.total_in - data.totals.total_out) / data.totals.total_in) * 100) : 0}%`, desc: "You're on track to reach your monthly saving target." },
                  ].map((ins, i) => (
                    <div key={i} className="flex gap-2.5">
                      <span className="text-lg mt-0.5">{ins.icon}</span>
                      <div>
                        <p className={`text-xs font-semibold ${ins.color}`}>{ins.title}</p>
                        <p className="text-xs text-[var(--muted)] mt-0.5">{ins.desc}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </Card>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
