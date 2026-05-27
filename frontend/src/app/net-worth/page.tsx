"use client";
import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Button } from "@/components/ui/Button";

interface NWResponse {
  as_of: string;
  liquid_assets: number;
  invested_assets: number;
  total_cost_basis: number;
  unrealized_gain: number;
  net_worth: number;
  history: { as_of_date: string; liquid_assets: number; invested_assets: number; net_worth: number }[];
}

const PERIODS = ["7D", "30D", "90D", "1Y", "All"];

export default function NetWorthPage() {
  const qc = useQueryClient();
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [period, setPeriod] = useState("90D");
  const [showAllSnapshots, setShowAllSnapshots] = useState(false);
  const actionHandledRef = useRef(false);

  const { data, isLoading } = useQuery<NWResponse>({ queryKey: ["net-worth"], queryFn: () => api.get("/assets/net-worth") });
  const snapshotMut = useMutation({ mutationFn: () => api.post("/assets/net-worth/snapshot", {}), onSuccess: () => qc.invalidateQueries({ queryKey: ["net-worth"] }) });

  useEffect(() => {
    if (actionHandledRef.current) return;
    actionHandledRef.current = true;
    const params = new URLSearchParams(window.location.search);
    if (params.get("action") === "record") {
      snapshotMut.mutate();
      params.delete("action");
      const nextUrl = params.toString() ? `${window.location.pathname}?${params.toString()}` : window.location.pathname;
      window.history.replaceState(null, "", nextUrl);
    }
  }, [snapshotMut]);

  if (isLoading) return <div className="p-6 text-[var(--muted)]">Loading…</div>;
  if (!data) return null;

  const allHistory = [...(data.history ?? [])].reverse();
  const periodLimit: Record<string, number | null> = { "7D": 7, "30D": 30, "90D": 90, "1Y": 365, All: null };
  const history = periodLimit[period] ? allHistory.slice(-periodLimit[period]!) : allHistory;
  const maxNW = Math.max(...history.map((h) => h.net_worth), 1);
  const minNW = Math.min(...history.map((h) => h.net_worth), 0);
  const range = maxNW - minNW || 1;
  const total = data.liquid_assets + data.invested_assets;
  const liquidPct = total > 0 ? (data.liquid_assets / total) * 100 : 0;
  const investedPct = total > 0 ? (data.invested_assets / total) * 100 : 0;
  const liabPct = total > 0 ? 0.9 : 0; // placeholder

  // Previous snapshot for change calculation
  const prevNW = history.length > 1 ? history[history.length - 2].net_worth : data.net_worth;
  const change = data.net_worth - prevNW;
  const changePct = prevNW > 0 ? ((change / prevNW) * 100).toFixed(2) : "0.00";

  return (
    <div className="p-5 space-y-4">
      <div className="flex justify-end">
        <Button variant="primary" onClick={() => snapshotMut.mutate()} disabled={snapshotMut.isPending}>
          📌 {snapshotMut.isPending ? "Recording…" : "Record Today"}
        </Button>
      </div>

      {/* Top row */}
      <div className="grid grid-cols-3 gap-4">
        {/* Current Net Worth */}
        <Card green padding="md">
          <div className="flex items-center justify-between mb-2">
            <p className="text-white/80 text-xs">Current Net Worth <span className="text-white/50">ⓘ</span></p>
            <button type="button" disabled title="Account-level net worth filtering is coming soon" className="text-xs text-white/50 border border-white/20 px-2 py-0.5 rounded cursor-not-allowed">All Accounts</button>
          </div>
          <p className="text-3xl font-bold text-white tabular">{bal(data.net_worth)}</p>
          <p className="text-white/70 text-xs mt-1">Total Assets minus Total Liabilities</p>
          <div className="flex items-center gap-1 mt-2">
            <span className="text-white text-sm">{change >= 0 ? "▲" : "▼"}</span>
            <span className="text-white font-semibold text-sm">{bal(Math.abs(change))} ({changePct}%) vs last month</span>
          </div>
        </Card>

        {/* Stat cards */}
        <div className="grid grid-cols-2 gap-3">
          {[
            { label: "Liquid Assets", value: data.liquid_assets, sub: `${liquidPct.toFixed(1)}% of total assets`, icon: "💵", color: "text-primary" },
            { label: "Invested Assets", value: data.invested_assets, sub: `${investedPct.toFixed(1)}% of total assets`, icon: "📈", color: "text-info" },
            { label: "Liabilities", value: null, sub: "Not tracked yet", icon: "🏦", color: "text-[var(--muted)]" },
            { label: "Change vs Last Month", value: Math.abs(change), sub: `${changePct}% improvement`, icon: "📊", color: change >= 0 ? "text-primary" : "text-danger" },
          ].map((s) => (
            <Card key={s.label} padding="sm">
              <div className="flex items-center justify-between mb-1">
                <p className="text-xs text-[var(--muted)]">{s.label}</p>
                <div className="w-7 h-7 rounded-lg bg-[var(--bg)] flex items-center justify-center text-sm">{s.icon}</div>
              </div>
              <p className={`text-base font-bold tabular ${s.color}`}>
                {s.value != null ? bal(s.value) : <span className="text-xs font-normal text-[var(--muted)]">Coming soon</span>}
              </p>
              <p className="text-xs text-[var(--muted)]">{s.sub}</p>
            </Card>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* History chart */}
        <div className="col-span-2">
          <Card padding="md">
            <div className="flex items-center justify-between mb-3">
              <SectionTitle>Net Worth History <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
              <div className="flex gap-1">
                {PERIODS.map((p) => (
                  <button key={p} type="button" onClick={() => setPeriod(p)} className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${p === period ? "bg-primary text-white" : "border border-[var(--border)] text-[var(--muted)] hover:bg-[var(--bg)]"}`}>{p}</button>
                ))}
              </div>
            </div>
            {history.length > 1 ? (
              <div className="relative h-48">
                <svg width="100%" height="100%" viewBox="0 0 600 192" preserveAspectRatio="none">
                  {/* Area fill */}
                  <defs>
                    <linearGradient id="nwGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#16a34a" stopOpacity="0.2" />
                      <stop offset="100%" stopColor="#16a34a" stopOpacity="0" />
                    </linearGradient>
                  </defs>
                  {(() => {
                    const pts = history.map((h, i) => ({ x: (i / (history.length - 1)) * 600, y: 192 - ((h.net_worth - minNW) / range) * 180 - 6 }));
                    const line = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x},${p.y}`).join(" ");
                    const area = `${line} L600,192 L0,192 Z`;
                    return (
                      <>
                        <path d={area} fill="url(#nwGrad)" />
                        <path d={line} fill="none" stroke="#16a34a" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
                        {pts.map((p, i) => <circle key={i} cx={p.x} cy={p.y} r={3} fill="#16a34a" />)}
                      </>
                    );
                  })()}
                </svg>
                <div className="flex justify-between text-xs text-[var(--muted)] mt-1">
                  <span>{history[0]?.as_of_date}</span>
                  <span>{history[history.length - 1]?.as_of_date}</span>
                </div>
                <p className="text-xs text-[var(--muted)] mt-1">Showing last {history.length} days</p>
              </div>
            ) : (
              <div className="h-48 flex items-center justify-center text-[var(--muted)] text-sm">
                No history yet. Click "Record Today" to start tracking.
              </div>
            )}
          </Card>
        </div>

        {/* Composition donut */}
        <Card padding="md" className="h-full">
          <div className="flex items-center justify-between mb-3">
            <SectionTitle>Net Worth Composition <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
            <span className="text-xs text-[var(--muted)]">Account balances</span>
          </div>
          <div className="flex flex-col items-center">
            <div className="relative w-28 h-28">
              <svg width="112" height="112" viewBox="0 0 112 112" className="donut-ring">
                {(() => {
                  const r = 44; const circ = 2 * Math.PI * r;
                  const segs = [
                    { pct: liquidPct / 100, color: "#16a34a" },
                    { pct: investedPct / 100, color: "#3b82f6" },
                    { pct: liabPct / 100, color: "#dc2626" },
                  ];
                  let offset = 0;
                  return segs.map((s, i) => {
                    const dash = s.pct * circ;
                    const el = <circle key={i} cx="56" cy="56" r={r} fill="none" stroke={s.color} strokeWidth="14" strokeDasharray={`${dash} ${circ - dash}`} strokeDashoffset={-offset} />;
                    offset += dash;
                    return el;
                  });
                })()}
              </svg>
              <div className="absolute inset-0 flex flex-col items-center justify-center">
                <p className="text-xs text-[var(--muted)]">Rp</p>
                <p className="text-sm font-bold tabular">{hideBalances ? "••••" : (data.net_worth / 1_000_000).toFixed(1) + "M"}</p>
                <p className="text-xs text-[var(--muted)]">Total</p>
              </div>
            </div>
            <div className="mt-3 space-y-1.5 w-full text-xs">
              {[
                { label: "Liquid Assets", value: data.liquid_assets, pct: liquidPct, color: "#16a34a" },
                { label: "Invested Assets", value: data.invested_assets, pct: investedPct, color: "#3b82f6" },
                { label: "Liabilities", value: null, pct: 0, color: "#dc2626" },
              ].map((s) => (
                <div key={s.label} className="flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <span className="w-2 h-2 rounded-full" style={{ background: s.color }} />
                    <span>{s.label}</span>
                  </div>
                  <div className="text-right">
                    {s.value != null
                      ? <><span className="tabular font-medium">{bal(s.value)}</span><span className="text-[var(--muted)] ml-1">({s.pct.toFixed(1)}%)</span></>
                      : <span className="text-[var(--muted)] text-xs">Not tracked</span>
                    }
                  </div>
                </div>
              ))}
            </div>
            <p className="text-xs text-[var(--muted)] mt-2">Last updated: Today, {new Date().toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })}</p>
          </div>
        </Card>
      </div>

      {/* Snapshots table */}
      <Card padding="sm">
        <SectionTitle>Recent Net Worth Snapshots <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[var(--muted)] border-b border-[var(--border)]">
              <th className="text-left pb-2 font-medium">Date</th>
              <th className="text-right pb-2 font-medium">Net Worth</th>
              <th className="text-right pb-2 font-medium">Change vs Previous</th>
              <th className="text-right pb-2 font-medium">Change %</th>
              <th className="text-right pb-2 font-medium">Assets</th>
              <th className="text-right pb-2 font-medium">Liabilities</th>
              <th className="text-left pb-2 font-medium">Notes</th>
              <th className="pb-2"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {(showAllSnapshots ? allHistory : allHistory.slice(-5)).reverse().map((h) => {
              const idx = allHistory.findIndex((candidate) => candidate.as_of_date === h.as_of_date);
              const prev = idx > 0 ? allHistory[idx - 1] : undefined;
              const chg = prev ? h.net_worth - prev.net_worth : 0;
              const chgPct = prev && prev.net_worth > 0 ? ((chg / prev.net_worth) * 100).toFixed(2) : "—";
              const changeClass = chgPct === "—" ? "text-[var(--muted)]" : chg >= 0 ? "text-primary" : "text-danger";
              const isToday = h.as_of_date === new Date().toISOString().slice(0, 10);
              return (
                <tr key={h.as_of_date} className="hover:bg-[var(--bg)] transition-colors">
                  <td className="py-2.5 font-medium">{h.as_of_date}{isToday && <span className="ml-1 text-primary text-[10px]">(Today)</span>}</td>
                  <td className="py-2.5 text-right tabular font-medium">{bal(h.net_worth)}</td>
                  <td className={`py-2.5 text-right tabular ${changeClass}`}>{prev ? `${chg >= 0 ? "▲" : "▼"} ${bal(Math.abs(chg))}` : "—"}</td>
                  <td className={`py-2.5 text-right tabular ${changeClass}`}>{chgPct !== "—" ? `${chgPct}%` : "—"}</td>
                  <td className="py-2.5 text-right tabular">{bal(h.liquid_assets + h.invested_assets)}</td>
                  <td className="py-2.5 text-right tabular text-danger">—</td>
                  <td className="py-2.5 text-[var(--muted)]">—</td>
                  <td />
                </tr>
              );
            })}
            {history.length === 0 && <tr><td colSpan={8} className="text-center py-6 text-[var(--muted)]">No snapshots yet.</td></tr>}
          </tbody>
        </table>
        {allHistory.length > 5 && (
          <button type="button" onClick={() => setShowAllSnapshots((v) => !v)} className="w-full text-xs text-primary hover:underline mt-3 py-1">
            {showAllSnapshots ? "Show latest snapshots" : "View all snapshots"}
          </button>
        )}
      </Card>
    </div>
  );
}
