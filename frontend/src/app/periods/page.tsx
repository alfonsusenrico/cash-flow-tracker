"use client";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { MonthlyPeriod } from "@/types/domain";

export default function PeriodsPage() {
  const { data, isLoading } = useQuery<{ periods: MonthlyPeriod[] }>({
    queryKey: ["periods"],
    queryFn: () => api.get("/periods"),
  });
  const statusCls: Record<string, string> = {
    open: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400",
    closed: "bg-[var(--bg)] text-[var(--muted)]",
    reviewed: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400",
  };

  return (
    <div className="p-4 space-y-3 max-w-2xl mx-auto">
      <h1 className="text-xl font-bold">Monthly Periods</h1>
      {isLoading && <p className="text-[var(--muted)]">Loading…</p>}
      {data?.periods.map((p) => (
        <div key={p.period_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3 flex items-center justify-between">
          <div>
            <div className="font-semibold">{p.month}</div>
            <div className="text-xs text-[var(--muted)]">{p.from_date} – {p.to_date} · payday {p.payday_day}</div>
          </div>
          <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${statusCls[p.status] ?? ""}`}>{p.status}</span>
        </div>
      ))}
    </div>
  );
}
