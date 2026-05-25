"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";
import { useAppCtx } from "@/components/layout/AppLayout";

interface Asset {
  asset_id: string;
  name: string;
  class: string;
  currency: string;
  ticker: string | null;
  total_quantity: number;
  current_value: number;
  total_cost_basis: number;
  unrealized_gain: number;
  latest_price: number | null;
  price_date: string | null;
}

const CLASSES = ["stock", "etf", "mutual_fund", "bond", "crypto", "metal", "property", "other"];
const CLASS_LABELS: Record<string, string> = { stock: "📈 Stock", etf: "📊 ETF", mutual_fund: "🏦 Mutual Fund", bond: "📜 Bond", crypto: "₿ Crypto", metal: "🥇 Metal", property: "🏠 Property", other: "📦 Other" };

const EMPTY_ASSET = { name: "", class: "other", currency: "IDR", ticker: "", notes: "" };
const EMPTY_HOLDING = { quantity: 0, cost_basis: 0, acquired_at: new Date().toISOString().slice(0, 10), account_id: "", notes: "" };
const EMPTY_SNAPSHOT = { unit_price: 0, as_of_date: new Date().toISOString().slice(0, 10) };

export default function AssetsPage() {
  const qc = useQueryClient();
  const { accounts } = useAppCtx();
  const [assetModal, setAssetModal] = useState<"create" | "edit" | null>(null);
  const [editingAsset, setEditingAsset] = useState<Asset | null>(null);
  const [assetForm, setAssetForm] = useState(EMPTY_ASSET);
  const [holdingModal, setHoldingModal] = useState<string | null>(null); // asset_id
  const [holdingForm, setHoldingForm] = useState(EMPTY_HOLDING);
  const [snapshotModal, setSnapshotModal] = useState<string | null>(null); // asset_id
  const [snapshotForm, setSnapshotForm] = useState(EMPTY_SNAPSHOT);
  const [err, setErr] = useState("");

  const { data } = useQuery<{ assets: Asset[] }>({ queryKey: ["assets"], queryFn: () => api.get("/assets") });
  const inv = () => qc.invalidateQueries({ queryKey: ["assets"] });

  const saveAssetMut = useMutation({
    mutationFn: () => editingAsset ? api.put(`/assets/${editingAsset.asset_id}`, assetForm) : api.post("/assets", assetForm),
    onSuccess: () => { inv(); setAssetModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteAssetMut = useMutation({
    mutationFn: (id: string) => api.del(`/assets/${id}`),
    onSuccess: inv,
  });

  const addHoldingMut = useMutation({
    mutationFn: () => api.post(`/assets/${holdingModal}/holdings`, { ...holdingForm, account_id: holdingForm.account_id || null }),
    onSuccess: () => { inv(); setHoldingModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const addSnapshotMut = useMutation({
    mutationFn: () => api.post(`/assets/${snapshotModal}/snapshots`, snapshotForm),
    onSuccess: () => { inv(); setSnapshotModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const assets = data?.assets ?? [];
  const totalValue = assets.reduce((s, a) => s + a.current_value, 0);
  const totalGain = assets.reduce((s, a) => s + a.unrealized_gain, 0);

  return (
    <div className="p-4 max-w-3xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Assets</h1>
          <p className="text-xs text-[var(--muted)]">Track investments and non-cash assets.</p>
        </div>
        <Button variant="primary" size="sm" onClick={() => { setEditingAsset(null); setAssetForm(EMPTY_ASSET); setErr(""); setAssetModal("create"); }}>+ New Asset</Button>
      </div>

      {assets.length > 0 && (
        <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4 grid grid-cols-2 gap-4 text-sm">
          <div><div className="text-xs text-[var(--muted)]">Total Value</div><div className="text-xl font-bold">{fmtMoney(totalValue)}</div></div>
          <div><div className="text-xs text-[var(--muted)]">Unrealized Gain</div><div className={`text-xl font-bold ${totalGain >= 0 ? "text-green-600" : "text-red-500"}`}>{fmtMoney(totalGain)}</div></div>
        </div>
      )}

      <div className="space-y-2">
        {assets.length === 0 && <p className="text-[var(--muted)] text-sm text-center py-8">No assets yet.</p>}
        {assets.map((a) => (
          <div key={a.asset_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4 space-y-2">
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="font-medium">{a.name} {a.ticker && <span className="text-xs text-[var(--muted)]">({a.ticker})</span>}</div>
                <div className="text-xs text-[var(--muted)]">{CLASS_LABELS[a.class] ?? a.class} · {a.total_quantity} units</div>
              </div>
              <div className="text-right">
                <div className="font-semibold">{fmtMoney(a.current_value)}</div>
                <div className={`text-xs ${a.unrealized_gain >= 0 ? "text-green-600" : "text-red-500"}`}>
                  {a.unrealized_gain >= 0 ? "+" : ""}{fmtMoney(a.unrealized_gain)}
                </div>
              </div>
            </div>
            {a.latest_price != null && (
              <div className="text-xs text-[var(--muted)]">Latest price: {fmtMoney(a.latest_price)} on {a.price_date}</div>
            )}
            <div className="flex gap-1 flex-wrap">
              <Button size="sm" variant="secondary" onClick={() => { setSnapshotForm({ unit_price: a.latest_price ?? 0, as_of_date: new Date().toISOString().slice(0, 10) }); setErr(""); setSnapshotModal(a.asset_id); }}>Update Price</Button>
              <Button size="sm" variant="secondary" onClick={() => { setHoldingForm(EMPTY_HOLDING); setErr(""); setHoldingModal(a.asset_id); }}>+ Holding</Button>
              <Button size="sm" variant="ghost" onClick={() => { setEditingAsset(a); setAssetForm({ name: a.name, class: a.class, currency: a.currency, ticker: a.ticker ?? "", notes: "" }); setErr(""); setAssetModal("edit"); }}>Edit</Button>
              <Button size="sm" variant="danger" onClick={() => confirm(`Archive "${a.name}"?`) && deleteAssetMut.mutate(a.asset_id)}>Archive</Button>
            </div>
          </div>
        ))}
      </div>

      {/* Asset modal */}
      <Modal open={assetModal !== null} onClose={() => setAssetModal(null)} title={editingAsset ? "Edit Asset" : "New Asset"}>
        <form onSubmit={(e) => { e.preventDefault(); saveAssetMut.mutate(); }} className="space-y-4">
          <Input label="Name" value={assetForm.name} onChange={(e) => setAssetForm({ ...assetForm, name: e.target.value })} required placeholder="e.g. BBCA, Bitcoin, Gold" />
          <Select label="Class" value={assetForm.class} onChange={(e) => setAssetForm({ ...assetForm, class: e.target.value })}>
            {CLASSES.map((c) => <option key={c} value={c}>{CLASS_LABELS[c]}</option>)}
          </Select>
          <Input label="Ticker (optional)" value={assetForm.ticker} onChange={(e) => setAssetForm({ ...assetForm, ticker: e.target.value })} placeholder="e.g. BBCA.JK" />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveAssetMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setAssetModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      {/* Add holding modal */}
      <Modal open={!!holdingModal} onClose={() => setHoldingModal(null)} title="Add Holding">
        <form onSubmit={(e) => { e.preventDefault(); addHoldingMut.mutate(); }} className="space-y-4">
          <Input label="Quantity" type="number" min={0} step="any" value={String(holdingForm.quantity)} onChange={(e) => setHoldingForm({ ...holdingForm, quantity: parseFloat(e.target.value) || 0 })} required />
          <MoneyInput label="Cost Basis (total purchase cost)" value={holdingForm.cost_basis} onChange={(v) => setHoldingForm({ ...holdingForm, cost_basis: v })} />
          <Input label="Acquired Date" type="date" value={holdingForm.acquired_at} onChange={(e) => setHoldingForm({ ...holdingForm, acquired_at: e.target.value })} required />
          <Select label="Account/Broker (optional)" value={holdingForm.account_id} onChange={(e) => setHoldingForm({ ...holdingForm, account_id: e.target.value })}>
            <option value="">— none —</option>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={addHoldingMut.isPending}>Add</Button>
            <Button type="button" variant="secondary" onClick={() => setHoldingModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      {/* Price snapshot modal */}
      <Modal open={!!snapshotModal} onClose={() => setSnapshotModal(null)} title="Update Price">
        <form onSubmit={(e) => { e.preventDefault(); addSnapshotMut.mutate(); }} className="space-y-4">
          <MoneyInput label="Unit Price" value={snapshotForm.unit_price} onChange={(v) => setSnapshotForm({ ...snapshotForm, unit_price: v })} required />
          <Input label="As of Date" type="date" value={snapshotForm.as_of_date} onChange={(e) => setSnapshotForm({ ...snapshotForm, as_of_date: e.target.value })} required />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={addSnapshotMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setSnapshotModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
