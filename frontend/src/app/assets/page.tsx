"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Sparkline } from "@/components/ui/Sparkline";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface Asset { asset_id: string; name: string; class: string; currency: string; ticker: string | null; total_quantity: number; current_value: number; total_cost_basis: number; unrealized_gain: number; latest_price: number | null; price_date: string | null; }

const CLASS_ICONS: Record<string, string> = { stock: "📈", etf: "📊", mutual_fund: "🏦", bond: "📜", crypto: "₿", metal: "🥇", property: "🏠", other: "📦" };
const CLASS_COLORS: Record<string, string> = { stock: "#16a34a", etf: "#3b82f6", mutual_fund: "#8b5cf6", bond: "#f59e0b", crypto: "#f97316", metal: "#eab308", property: "#06b6d4", other: "#6b7280" };
const CLASSES = ["stock", "etf", "mutual_fund", "bond", "crypto", "metal", "property", "other"];

export default function AssetsPage() {
  const qc = useQueryClient();
  const { accounts, hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [assetModal, setAssetModal] = useState<"create" | "edit" | null>(null);
  const [editingAsset, setEditingAsset] = useState<Asset | null>(null);
  const [assetForm, setAssetForm] = useState({ name: "", class: "stock", currency: "IDR", ticker: "", notes: "" });
  const [holdingModal, setHoldingModal] = useState<string | null>(null);
  const [holdingForm, setHoldingForm] = useState({ quantity: 0, cost_basis: 0, acquired_at: new Date().toISOString().slice(0, 10), account_id: "", notes: "" });
  const [snapshotModal, setSnapshotModal] = useState<string | null>(null);
  const [snapshotForm, setSnapshotForm] = useState({ unit_price: 0, as_of_date: new Date().toISOString().slice(0, 10) });
  const [groupBy, setGroupBy] = useState("Asset Type");
  const [search, setSearch] = useState("");
  const [err, setErr] = useState("");

  const { data } = useQuery<{ assets: Asset[] }>({ queryKey: ["assets"], queryFn: () => api.get("/assets") });
  const { data: nwData } = useQuery<any>({ queryKey: ["net-worth"], queryFn: () => api.get("/assets/net-worth") });
  const inv = () => qc.invalidateQueries({ queryKey: ["assets"] });

  const saveAssetMut = useMutation({ mutationFn: () => editingAsset ? api.put(`/assets/${editingAsset.asset_id}`, assetForm) : api.post("/assets", assetForm), onSuccess: () => { inv(); setAssetModal(null); setEditingAsset(null); }, onError: (e: Error) => setErr(e.message) });
  const deleteAssetMut = useMutation({ mutationFn: (id: string) => api.del(`/assets/${id}`), onSuccess: () => { inv(); setAssetModal(null); setEditingAsset(null); } });
  const addHoldingMut = useMutation({ mutationFn: () => api.post(`/assets/${holdingModal}/holdings`, { ...holdingForm, account_id: holdingForm.account_id || null }), onSuccess: () => { inv(); setHoldingModal(null); }, onError: (e: Error) => setErr(e.message) });
  const addSnapshotMut = useMutation({ mutationFn: () => api.post(`/assets/${snapshotModal}/snapshots`, snapshotForm), onSuccess: () => { inv(); setSnapshotModal(null); }, onError: (e: Error) => setErr(e.message) });

  const allAssets = data?.assets ?? [];
  const assets = allAssets.filter((a) => !search || `${a.name} ${a.ticker ?? ""} ${a.class}`.toLowerCase().includes(search.toLowerCase()));
  const totalValue = allAssets.reduce((s, a) => s + a.current_value, 0);
  const totalGain = allAssets.reduce((s, a) => s + a.unrealized_gain, 0);
  const totalCost = allAssets.reduce((s, a) => s + a.total_cost_basis, 0);
  const filteredValue = assets.reduce((s, a) => s + a.current_value, 0);
  const filteredGain = assets.reduce((s, a) => s + a.unrealized_gain, 0);
  const filteredCost = assets.reduce((s, a) => s + a.total_cost_basis, 0);
  const filteredGainPct = filteredCost > 0 ? ((filteredGain / filteredCost) * 100).toFixed(2) : "0.00";
  const gainPct = totalCost > 0 ? ((totalGain / totalCost) * 100).toFixed(2) : "0.00";
  const nwHistory = (nwData?.history ?? []).map((h: any) => h.net_worth).reverse();

  // Asset allocation by class
  const byClass: Record<string, number> = {};
  allAssets.forEach((a) => { byClass[a.class] = (byClass[a.class] ?? 0) + a.current_value; });
  const allocEntries = Object.entries(byClass).sort((a, b) => b[1] - a[1]);

  return (
    <div className="workbench-page space-y-4">
      {/* Header buttons */}
      <div className="flex items-center gap-2 justify-end -mt-1">
        <Button variant="primary" disabled title="Use the + button on a specific asset row to add a holding">+ Holding</Button>
        <Button variant="secondary" disabled title="Use the price button on a specific asset row to update a price">↻ Update Prices</Button>
        <Button variant="secondary" disabled title="More portfolio actions are coming soon">⋯</Button>
      </div>

      {/* Top 3 cards */}
      <div className="grid grid-cols-3 gap-4">
        {/* Portfolio value */}
        <Card green padding="md">
          <div className="flex items-center justify-between mb-2">
            <p className="text-white/80 text-xs">Total Portfolio Value</p>
            <button type="button" disabled title="Asset account filtering is coming soon" className="text-xs text-white/50 border border-white/20 px-2 py-0.5 rounded cursor-not-allowed">All Accounts</button>
          </div>
          <p className="text-3xl font-bold text-white tabular">{bal(totalValue)}</p>
          <p className="text-white/70 text-xs mt-1">Total unrealized gain</p>
          <p className="text-white font-semibold text-sm mt-0.5">{bal(totalGain)} ({gainPct}%)</p>
          {nwHistory.length > 1 && <div className="mt-3"><Sparkline data={nwHistory} width={200} height={40} color="rgba(255,255,255,0.8)" /></div>}
        </Card>

        {/* Unrealized G/L */}
        <Card padding="md">
          <p className="text-xs text-[var(--muted)] mb-2">Unrealized Gain / Loss</p>
          <p className={`text-2xl font-bold tabular ${totalGain >= 0 ? "text-primary" : "text-danger"}`}>{bal(totalGain)} {gainPct}%</p>
          <div className="grid grid-cols-2 gap-3 mt-3 text-xs">
            <div><p className="text-[var(--muted)]">Day Change</p><p className="font-semibold text-primary">—</p></div>
            <div><p className="text-[var(--muted)]">Cost Basis</p><p className="font-semibold tabular">{bal(totalCost)}</p></div>
          </div>
        </Card>

        {/* Asset Allocation donut */}
        <Card padding="md">
          <SectionTitle>Asset Allocation</SectionTitle>
          <div className="flex items-center gap-3">
            <div className="relative w-20 h-20">
              <svg width="80" height="80" viewBox="0 0 80 80" className="donut-ring">
                {(() => {
                  const circ = 2 * Math.PI * 30;
                  return allocEntries.map(([cls, val], i) => {
                    const pct = totalValue > 0 ? val / totalValue : 0;
                    const dash = pct * circ;
                    const offset = allocEntries
                      .slice(0, i)
                      .reduce((sum, [, previous]) => sum + (totalValue > 0 ? previous / totalValue : 0) * circ, 0);
                    return <circle key={cls} cx="40" cy="40" r="30" fill="none" stroke={CLASS_COLORS[cls] ?? "#6b7280"} strokeWidth="12" strokeDasharray={`${dash} ${circ - dash}`} strokeDashoffset={-offset} />;
                  });
                })()}
              </svg>
            </div>
            <div className="flex-1 space-y-1">
              {allocEntries.slice(0, 5).map(([cls, val]) => (
                <div key={cls} className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-1.5">
                    <span className="w-2 h-2 rounded-full" style={{ background: CLASS_COLORS[cls] ?? "#6b7280" }} />
                    <span className="capitalize">{cls.replace("_", " ")}</span>
                  </div>
                  <span className="tabular">{totalValue > 0 ? ((val / totalValue) * 100).toFixed(1) : 0}%</span>
                </div>
              ))}
              <button type="button" disabled title="Allocation drilldown is coming soon" className="text-xs text-[var(--muted)] cursor-not-allowed">View full allocation</button>
            </div>
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* Holdings table */}
        <div className="col-span-2">
          <Card padding="sm">
            <div className="flex items-center justify-between mb-3">
              <SectionTitle>Holdings <span className="text-[var(--muted)] text-xs font-normal">ⓘ</span></SectionTitle>
              <div className="flex items-center gap-2">
                <span className="text-xs text-[var(--muted)]">Group by:</span>
                <select value={groupBy} onChange={(e) => setGroupBy(e.target.value)} className="text-xs border border-[var(--border)] rounded px-2 py-1 bg-[var(--surface)]">
                  <option>Asset Type</option>
                </select>
                <input placeholder="Search holdings" value={search} onChange={(e) => setSearch(e.target.value)} className="border border-[var(--border)] rounded px-2 py-1 text-xs bg-[var(--surface)] w-32" />
                <button type="button" disabled title="Advanced holding filters are coming soon" className="text-[var(--muted)] text-sm opacity-50 cursor-not-allowed">⊟</button>
              </div>
            </div>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                  <th className="text-left pb-2 font-medium">Asset</th>
                  <th className="text-right pb-2 font-medium">Qty / Lots</th>
                  <th className="text-right pb-2 font-medium">Avg Buy (IDR)</th>
                  <th className="text-right pb-2 font-medium">Current Price (IDR)</th>
                  <th className="text-right pb-2 font-medium">Current Value</th>
                  <th className="text-right pb-2 font-medium">Unrealized G/L (IDR)</th>
                  <th className="text-right pb-2 font-medium">G/L (%)</th>
                  <th className="pb-2"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {assets.length === 0 && <tr><td colSpan={8} className="text-center py-8 text-[var(--muted)]">No assets yet. Add your first holding.</td></tr>}
                {assets.map((a) => {
                  const avgBuy = a.total_quantity > 0 ? a.total_cost_basis / a.total_quantity : 0;
                  const glPct = a.total_cost_basis > 0 ? ((a.unrealized_gain / a.total_cost_basis) * 100).toFixed(2) : "0.00";
                  return (
                    <tr key={a.asset_id} className="hover:bg-[var(--bg)] transition-colors">
                      <td className="py-2.5">
                        <div className="flex items-center gap-2">
                          <div className="w-7 h-7 rounded-lg flex items-center justify-center text-sm" style={{ background: (CLASS_COLORS[a.class] ?? "#6b7280") + "20" }}>{CLASS_ICONS[a.class] ?? "📦"}</div>
                          <div>
                            <p className="font-medium">{a.name}</p>
                            <p className="text-[var(--muted)] capitalize">{a.class.replace("_", " ")}</p>
                          </div>
                        </div>
                      </td>
                      <td className="py-2.5 text-right tabular">{a.total_quantity > 0 ? a.total_quantity.toLocaleString() : "—"}</td>
                      <td className="py-2.5 text-right tabular">{avgBuy > 0 ? fmtMoney(avgBuy) : "—"}</td>
                      <td className="py-2.5 text-right tabular">
                        <p>{a.latest_price ? fmtMoney(a.latest_price) : "—"}</p>
                        <p className="text-[var(--muted)] text-[10px]">{a.price_date ?? ""}</p>
                      </td>
                      <td className="py-2.5 text-right tabular font-medium">{bal(a.current_value)}</td>
                      <td className={`py-2.5 text-right tabular font-medium ${a.unrealized_gain >= 0 ? "text-primary" : "text-danger"}`}>
                        {a.unrealized_gain >= 0 ? "+" : ""}{bal(a.unrealized_gain)}
                      </td>
                      <td className={`py-2.5 text-right tabular ${parseFloat(glPct) >= 0 ? "text-primary" : "text-danger"}`}>
                        {parseFloat(glPct) >= 0 ? "+" : ""}{glPct}%
                      </td>
                      <td className="py-2.5">
                        <div className="flex gap-1">
                          <button onClick={() => { setSnapshotForm({ unit_price: a.latest_price ?? 0, as_of_date: new Date().toISOString().slice(0, 10) }); setErr(""); setSnapshotModal(a.asset_id); }} className="text-[var(--muted)] hover:text-[var(--text)] text-xs">💲</button>
                          <button onClick={() => { setHoldingForm({ quantity: 0, cost_basis: 0, acquired_at: new Date().toISOString().slice(0, 10), account_id: "", notes: "" }); setErr(""); setHoldingModal(a.asset_id); }} className="text-[var(--muted)] hover:text-[var(--text)] text-xs">+</button>
                          <button onClick={() => { setEditingAsset(a); setAssetForm({ name: a.name, class: a.class, currency: a.currency, ticker: a.ticker ?? "", notes: "" }); setErr(""); setAssetModal("edit"); }} className="text-[var(--muted)] hover:text-[var(--text)] text-xs">✏️</button>
                          <button onClick={() => confirm(`Delete "${a.name}"?`) && deleteAssetMut.mutate(a.asset_id)} className="text-danger hover:opacity-80 text-xs">🗑️</button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
                {assets.length > 0 && (
                  <tr className="font-semibold border-t-2 border-[var(--border)]">
                    <td className="py-2.5" colSpan={4}>Total</td>
                    <td className="py-2.5 text-right tabular">{bal(filteredValue)}</td>
                    <td className={`py-2.5 text-right tabular ${filteredGain >= 0 ? "text-primary" : "text-danger"}`}>{filteredGain >= 0 ? "+" : ""}{bal(filteredGain)}</td>
                    <td className={`py-2.5 text-right tabular ${parseFloat(filteredGainPct) >= 0 ? "text-primary" : "text-danger"}`}>{filteredGainPct}%</td>
                    <td />
                  </tr>
                )}
              </tbody>
            </table>
            <div className="mt-3 p-2.5 rounded-lg bg-[var(--bg)] text-xs text-[var(--muted)] flex items-center gap-2">
              <span>ⓘ</span>
              <span>Prices are entered manually. Last updated: {new Date().toLocaleDateString()}</span>
              <button type="button" disabled title="Use the price button on a specific asset row to update a price" className="ml-auto text-[var(--muted)] cursor-not-allowed">↻ Update Prices</button>
            </div>
            <p className="text-xs text-[var(--muted)] text-center mt-2">This is not a trading platform. Prices are for reference only.</p>
          </Card>
        </div>

        {/* Watchlist + Notes */}
        <div className="space-y-4">
          <Card padding="md">
            <div className="flex items-center justify-between mb-3">
              <SectionTitle>Watchlist</SectionTitle>
              <button type="button" disabled title="Watchlist is coming soon" className="text-xs text-[var(--muted)] cursor-not-allowed">+ Add</button>
            </div>
            <div className="text-center py-4 space-y-1">
              <p className="text-2xl">👁️</p>
              <p className="text-xs font-medium text-[var(--text)]">Watchlist coming soon</p>
              <p className="text-xs text-[var(--muted)]">Track asset prices without holding them.</p>
            </div>
          </Card>

          <Card padding="md">
            <div className="flex items-center justify-between mb-3">
              <SectionTitle>Notes</SectionTitle>
              <button type="button" disabled title="Investment notes are coming soon" className="text-xs text-[var(--muted)] cursor-not-allowed">+ New Note</button>
            </div>
            <div className="text-center py-4 space-y-1">
              <p className="text-2xl">📝</p>
              <p className="text-xs font-medium text-[var(--text)]">Investment notes coming soon</p>
              <p className="text-xs text-[var(--muted)]">Add research notes for each asset.</p>
            </div>
          </Card>

          <Button variant="primary" className="w-full" onClick={() => { setEditingAsset(null); setAssetForm({ name: "", class: "stock", currency: "IDR", ticker: "", notes: "" }); setErr(""); setAssetModal("create"); }}>
            + Add New Asset
          </Button>
        </div>
      </div>

      {/* Modals */}
      <Modal open={assetModal !== null} onClose={() => setAssetModal(null)} title={editingAsset ? "Edit Asset" : "New Asset"}>
        <form onSubmit={(e) => { e.preventDefault(); saveAssetMut.mutate(); }} className="space-y-3">
          <Input label="Name" value={assetForm.name} onChange={(e) => setAssetForm({ ...assetForm, name: e.target.value })} required placeholder="e.g. BBCA, Bitcoin, Gold" />
          <Select label="Class" value={assetForm.class} onChange={(e) => setAssetForm({ ...assetForm, class: e.target.value })}>
            {CLASSES.map((c) => <option key={c} value={c}>{CLASS_ICONS[c]} {c.replace("_", " ")}</option>)}
          </Select>
          <Input label="Ticker (optional)" value={assetForm.ticker} onChange={(e) => setAssetForm({ ...assetForm, ticker: e.target.value })} placeholder="e.g. BBCA.JK" />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            {editingAsset && <Button type="button" variant="danger" onClick={() => confirm(`Delete "${editingAsset.name}"?`) && deleteAssetMut.mutate(editingAsset.asset_id)} disabled={deleteAssetMut.isPending}>Delete</Button>}
            <Button type="submit" variant="primary" className="flex-1" disabled={saveAssetMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setAssetModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={!!holdingModal} onClose={() => setHoldingModal(null)} title="Add Holding">
        <form onSubmit={(e) => { e.preventDefault(); addHoldingMut.mutate(); }} className="space-y-3">
          <Input label="Quantity" type="number" min={0} step="any" value={String(holdingForm.quantity)} onChange={(e) => setHoldingForm({ ...holdingForm, quantity: parseFloat(e.target.value) || 0 })} required />
          <MoneyInput label="Cost Basis (total purchase cost)" value={holdingForm.cost_basis} onChange={(v) => setHoldingForm({ ...holdingForm, cost_basis: v })} />
          <Input label="Acquired Date" type="date" value={holdingForm.acquired_at} onChange={(e) => setHoldingForm({ ...holdingForm, acquired_at: e.target.value })} required />
          <Select label="Account/Broker (optional)" value={holdingForm.account_id} onChange={(e) => setHoldingForm({ ...holdingForm, account_id: e.target.value })}>
            <option value="">— none —</option>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={addHoldingMut.isPending}>Add</Button>
            <Button type="button" variant="secondary" onClick={() => setHoldingModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={!!snapshotModal} onClose={() => setSnapshotModal(null)} title="Update Price">
        <form onSubmit={(e) => { e.preventDefault(); addSnapshotMut.mutate(); }} className="space-y-3">
          <MoneyInput label="Unit Price" value={snapshotForm.unit_price} onChange={(v) => setSnapshotForm({ ...snapshotForm, unit_price: v })} required />
          <Input label="As of Date" type="date" value={snapshotForm.as_of_date} onChange={(e) => setSnapshotForm({ ...snapshotForm, as_of_date: e.target.value })} required />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={addSnapshotMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setSnapshotModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
