"use client";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { Button } from "@/components/ui/Button";

interface NetWorthResponse {
  as_of: string;
  liquid_assets: number;
  invested_assets: number;
  total_cost_basis: number;
  unrealized_gain: number;
  net_worth: number;
  history: { as_of_date: string; liquid_assets: number; invested_assets: number; net_worth: number }[];
}

export default function NetWorthPage() {
  const qc = useQueryClient();
  const { data, isLoading } = useQuery<NetWorthResponse>({
    queryKey: ["net-worth"],
    queryFn: () => api.get("/assets/net-worth"),
  });

  const snapshotMut = useMutation({
    mutationFn: () => api.post("/assets/net-worth/snapshot", {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["net-worth"] }),
  });

  if (isLoading) return <div className="p-8 text-[var(--muted)]">Loading…</div>;
  if (!data) return null;

  const history = [...(data.history ?? [])].reverse();
  const maxNW = Math.max(...history.map((h) => h.net_worth), 1);

  return (
    <div className="p-4 max-w-3xl mx-auto space-y-5">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Net Worth</h1>
          <p className="text-xs text-[var(--muted)]">As of {data.as_of}</p>
        </div>
        <Button size="sm" variant="secondary" onClick={() => snapshotMut.mutate()} disabled={snapshotMut.isPending}>
          {snapshotMut.isPending ? "Saving…" : "Record Today"}
        </Button>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
        {[
          { label: "Net Worth", value: data.net_worth, bold: true },
          { label: "Liquid (Cash)", value: data.liquid_assets },
          { label: "Invested", value: data.invested_assets },
          { label: "Cost Basis", value: data.total_cost_basis },
          { label: "Unrealized Gain", value: data.unrealized_gain, color: data.unrealized_gain >= 0 ? "text-green-600" : "text-red-500" },
        ].map((item) => (
          <div key={item.label} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-3">
            <div className="text-xs text-[var(--muted)]">{item.label}</div>
            <div className={`text-lg font-${item.bold ? "bold" : "semibold"} ${item.color ?? ""}`}>{fmtMoney(item.value)}</div>
          </div>
        ))}
      </div>

      {/* History chart (simple bar chart) */}
      {history.length > 0 && (
        <section>
          <h2 className="text-sm font-semibold text-[var(--muted)] uppercase tracking-wide mb-3">History</h2>
          <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4">
            <div className="flex items-end gap-1 h-32">
              {history.map((h) => (
                <div key={h.as_of_date} className="flex-1 flex flex-col items-center gap-1 group relative">
                  <div
                    className="w-full bg-blue-500 rounded-t transition-all"
                    style={{ height: `${Math.max(4, (h.net_worth / maxNW) * 100)}%` }}
                  />
                  <div className="absolute bottom-full mb-1 hidden group-hover:block bg-[var(--text)] text-[var(--bg)] text-xs rounded px-1.5 py-0.5 whitespace-nowrap z-10">
                    {h.as_of_date}: {fmtMoney(h.net_worth)}
                  </div>
                </div>
              ))}
            </div>
            <div className="flex justify-between text-xs text-[var(--muted)] mt-1">
              <span>{history[0]?.as_of_date}</span>
              <span>{history[history.length - 1]?.as_of_date}</span>
            </div>
          </div>
        </section>
      )}

      {history.length === 0 && (
        <p className="text-[var(--muted)] text-sm text-center py-6">No history yet. Click "Record Today" to start tracking.</p>
      )}
    </div>
  );
}
