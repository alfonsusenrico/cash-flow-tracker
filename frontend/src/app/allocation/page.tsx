"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney, currentMonthYM } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface AllocationItem {
  item_id: string;
  bucket_id: string | null;
  label: string;
  mode: "fixed" | "percent";
  value: number;
  priority: number;
  planned_amount: number;
  funded_amount: number;
  status: string;
}

interface Plan {
  plan_id: string;
  month: string;
  expected_income: number;
  status: string;
  notes: string | null;
  items?: AllocationItem[];
}

interface Bucket { bucket_id: string; name: string; }

export default function AllocationPage() {
  const qc = useQueryClient();
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null);
  const [planModal, setPlanModal] = useState(false);
  const [itemModal, setItemModal] = useState(false);
  const [editingItem, setEditingItem] = useState<AllocationItem | null>(null);
  const [fundModal, setFundModal] = useState<AllocationItem | null>(null);
  const [fundAmount, setFundAmount] = useState(0);
  const [planForm, setPlanForm] = useState({ month: currentMonthYM(), expected_income: 0, notes: "" });
  const [itemForm, setItemForm] = useState({ label: "", mode: "percent" as "fixed" | "percent", value: 0, bucket_id: "", priority: 50 });
  const [err, setErr] = useState("");

  const { data: plansData } = useQuery<{ plans: Plan[] }>({ queryKey: ["allocation-plans"], queryFn: () => api.get("/allocation-plans") });
  const { data: planDetail } = useQuery<Plan>({
    queryKey: ["allocation-plan", selectedPlan],
    queryFn: () => api.get(`/allocation-plans/${selectedPlan}`),
    enabled: !!selectedPlan,
  });
  const { data: bucketsData } = useQuery<{ buckets: Bucket[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });

  const inv = () => { qc.invalidateQueries({ queryKey: ["allocation-plans"] }); qc.invalidateQueries({ queryKey: ["allocation-plan", selectedPlan] }); };

  const createPlanMut = useMutation({
    mutationFn: () => api.post("/allocation-plans", { ...planForm, expected_income: planForm.expected_income }),
    onSuccess: (r: any) => { inv(); setPlanModal(false); setSelectedPlan(r.plan_id); },
    onError: (e: Error) => setErr(e.message),
  });

  const activateMut = useMutation({
    mutationFn: (id: string) => api.post(`/allocation-plans/${id}/activate`, {}),
    onSuccess: inv,
  });

  const saveItemMut = useMutation({
    mutationFn: () =>
      editingItem
        ? api.put(`/allocation-plans/${selectedPlan}/items/${editingItem.item_id}`, { ...itemForm, bucket_id: itemForm.bucket_id || null })
        : api.post(`/allocation-plans/${selectedPlan}/items`, { ...itemForm, bucket_id: itemForm.bucket_id || null }),
    onSuccess: () => { inv(); setItemModal(false); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteItemMut = useMutation({
    mutationFn: (itemId: string) => api.del(`/allocation-plans/${selectedPlan}/items/${itemId}`),
    onSuccess: inv,
  });

  const fundMut = useMutation({
    mutationFn: () => api.post(`/allocation-plans/${selectedPlan}/items/${fundModal!.item_id}/fund`, { amount: fundAmount }),
    onSuccess: () => { inv(); setFundModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const plan = planDetail;
  const plans = plansData?.plans ?? [];
  const buckets = bucketsData?.buckets ?? [];

  const totalPlanned = plan?.items?.reduce((s, i) => s + i.planned_amount, 0) ?? 0;
  const totalFunded = plan?.items?.reduce((s, i) => s + i.funded_amount, 0) ?? 0;

  return (
    <div className="p-4 max-w-3xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Allocation Plans</h1>
          <p className="text-xs text-[var(--muted)]">Distribute income across buckets each month.</p>
        </div>
        <Button variant="primary" size="sm" onClick={() => { setPlanForm({ month: currentMonthYM(), expected_income: 0, notes: "" }); setErr(""); setPlanModal(true); }}>+ New Plan</Button>
      </div>

      {/* Plan selector */}
      {plans.length > 0 && (
        <Select label="Select Plan" value={selectedPlan ?? ""} onChange={(e) => setSelectedPlan(e.target.value || null)}>
          <option value="">— choose a plan —</option>
          {plans.map((p) => <option key={p.plan_id} value={p.plan_id}>{p.month} · {p.status}</option>)}
        </Select>
      )}

      {/* Plan detail */}
      {plan && (
        <div className="space-y-3">
          <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4 space-y-2">
            <div className="flex items-center justify-between">
              <div>
                <div className="font-semibold">{plan.month}</div>
                <div className="text-xs text-[var(--muted)]">Expected income: {fmtMoney(plan.expected_income)} · Status: {plan.status}</div>
              </div>
              <div className="flex gap-2">
                {plan.status === "draft" && (
                  <Button size="sm" variant="primary" onClick={() => activateMut.mutate(plan.plan_id)}>Activate</Button>
                )}
                <Button size="sm" variant="secondary" onClick={() => { setEditingItem(null); setItemForm({ label: "", mode: "percent", value: 0, bucket_id: "", priority: 50 }); setErr(""); setItemModal(true); }}>+ Add Item</Button>
              </div>
            </div>
            <div className="flex gap-4 text-sm">
              <span>Planned: <strong>{fmtMoney(totalPlanned)}</strong></span>
              <span>Funded: <strong className="text-green-600">{fmtMoney(totalFunded)}</strong></span>
              <span>Remaining: <strong>{fmtMoney(plan.expected_income - totalFunded)}</strong></span>
            </div>
          </div>

          {/* Items */}
          <div className="space-y-2">
            {(plan.items ?? []).map((item) => (
              <div key={item.item_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex-1">
                    <div className="font-medium text-sm">{item.label}</div>
                    <div className="text-xs text-[var(--muted)]">
                      {item.mode === "percent" ? `${item.value}%` : fmtMoney(item.value)} → {fmtMoney(item.planned_amount)} planned
                    </div>
                    <div className="mt-1.5 h-1.5 bg-[var(--bg)] rounded-full overflow-hidden">
                      <div className="h-full bg-green-500 rounded-full" style={{ width: `${item.planned_amount > 0 ? Math.min((item.funded_amount / item.planned_amount) * 100, 100) : 0}%` }} />
                    </div>
                    <div className="text-xs text-[var(--muted)] mt-0.5">{fmtMoney(item.funded_amount)} funded · {item.status}</div>
                  </div>
                  <div className="flex gap-1 shrink-0">
                    <Button size="sm" variant="primary" onClick={() => { setFundAmount(item.planned_amount - item.funded_amount); setFundModal(item); }}>Fund</Button>
                    <Button size="sm" variant="ghost" onClick={() => { setEditingItem(item); setItemForm({ label: item.label, mode: item.mode, value: item.value, bucket_id: item.bucket_id ?? "", priority: item.priority }); setErr(""); setItemModal(true); }}>Edit</Button>
                    <Button size="sm" variant="danger" onClick={() => confirm("Delete item?") && deleteItemMut.mutate(item.item_id)}>✕</Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Create plan modal */}
      <Modal open={planModal} onClose={() => setPlanModal(false)} title="New Allocation Plan">
        <form onSubmit={(e) => { e.preventDefault(); createPlanMut.mutate(); }} className="space-y-4">
          <Input label="Month (YYYY-MM)" value={planForm.month} onChange={(e) => setPlanForm({ ...planForm, month: e.target.value })} required />
          <MoneyInput label="Expected Income" value={planForm.expected_income} onChange={(v) => setPlanForm({ ...planForm, expected_income: v })} />
          <Input label="Notes (optional)" value={planForm.notes} onChange={(e) => setPlanForm({ ...planForm, notes: e.target.value })} />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={createPlanMut.isPending}>Create</Button>
            <Button type="button" variant="secondary" onClick={() => setPlanModal(false)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      {/* Add/edit item modal */}
      <Modal open={itemModal} onClose={() => setItemModal(false)} title={editingItem ? "Edit Item" : "Add Item"}>
        <form onSubmit={(e) => { e.preventDefault(); saveItemMut.mutate(); }} className="space-y-4">
          <Input label="Label" value={itemForm.label} onChange={(e) => setItemForm({ ...itemForm, label: e.target.value })} required placeholder="e.g. Emergency Fund" />
          <Select label="Mode" value={itemForm.mode} onChange={(e) => setItemForm({ ...itemForm, mode: e.target.value as "fixed" | "percent" })}>
            <option value="percent">Percent of income (%)</option>
            <option value="fixed">Fixed amount (Rp)</option>
          </Select>
          {itemForm.mode === "percent"
            ? <Input label="Percentage" type="number" min={0} max={100} step={0.1} value={String(itemForm.value)} onChange={(e) => setItemForm({ ...itemForm, value: parseFloat(e.target.value) || 0 })} />
            : <MoneyInput label="Amount" value={itemForm.value} onChange={(v) => setItemForm({ ...itemForm, value: v })} />
          }
          <Select label="Bucket (optional)" value={itemForm.bucket_id} onChange={(e) => setItemForm({ ...itemForm, bucket_id: e.target.value })}>
            <option value="">— none —</option>
            {buckets.map((b) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          <Input label="Priority" type="number" min={1} value={String(itemForm.priority)} onChange={(e) => setItemForm({ ...itemForm, priority: parseInt(e.target.value) || 50 })} />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveItemMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setItemModal(false)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      {/* Fund modal */}
      <Modal open={!!fundModal} onClose={() => setFundModal(null)} title={`Fund: ${fundModal?.label}`}>
        <div className="space-y-4">
          <p className="text-sm text-[var(--muted)]">Planned: {fmtMoney(fundModal?.planned_amount ?? 0)} · Already funded: {fmtMoney(fundModal?.funded_amount ?? 0)}</p>
          <MoneyInput label="Amount to fund" value={fundAmount} onChange={setFundAmount} />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button variant="primary" className="flex-1" disabled={fundMut.isPending} onClick={() => fundMut.mutate()}>Confirm</Button>
            <Button variant="secondary" onClick={() => setFundModal(null)}>Cancel</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
