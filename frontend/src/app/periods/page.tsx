"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { MonthlyPeriod } from "@/types/domain";
import AppLayout from "@/components/layout/AppLayout";

export default function PeriodsPage() {
  const { data, isLoading } = useQuery<{ periods: MonthlyPeriod[] }>({
    queryKey: ["periods"],
    queryFn: () => api.get("/periods"),
  });

  const statusColor: Record<string, string> = {
    open: "bg-green-100 text-green-800",
    closed: "bg-gray-100 text-gray-600",
    reviewed: "bg-blue-100 text-blue-800",
  };

  return (
    <AppLayout>
      <div className="p-6 space-y-4">
        <h1 className="text-2xl font-bold">Monthly Periods</h1>
        {isLoading && <p className="text-gray-500">Loading…</p>}
        <div className="space-y-2">
          {data?.periods.map((p) => (
            <div
              key={p.period_id}
              className="bg-white dark:bg-gray-800 rounded-xl px-4 py-3 shadow-sm flex items-center justify-between"
            >
              <div>
                <div className="font-semibold">{p.month}</div>
                <div className="text-xs text-gray-500">
                  {p.from_date} – {p.to_date} · payday {p.payday_day}
                </div>
              </div>
              <span
                className={`text-xs font-medium px-2 py-0.5 rounded-full ${statusColor[p.status] ?? ""}`}
              >
                {p.status}
              </span>
            </div>
          ))}
        </div>
      </div>
    </AppLayout>
  );
}
