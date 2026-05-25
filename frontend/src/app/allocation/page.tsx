"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney, currentMonthYM } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { DonutChart } from "@/components/ui/DonutChart";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

export default function AllocationPage() {
  const qc = useQueryClient();
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null);
  const [planModal, setPlanModal] = useState(false);
  const [itemModal, setItemModal] = useState(false);
  const [editingItem, setEditingItem] = useState<any>(null);
  const [fundModal, setFundModal] = useState<any>(null);
  const [fundAmount, setFundAmount] = useState(0);
  const [planForm, setPlanForm] = useState({ month: currentMonthYM(), expected_income: 0, notes: "" });
  const [itemForm, setItemForm] = useState({ label: "", mode: "percent", value: 0, bucket_id: "", priority: 50 });
  const [err, setErr] = useState("");

  const { data: plansData } = useQuery<{ plans: any[] }>({ queryKey: ["allocation-plans"], queryFn: () => api.get("/allocation-plans") });
  const { data: planDetail } = useQuery<any>({ queryKey: ["allocation-plan", selectedPlan], queryFn: () => api.get(`/allocation-plans/${selectedPlan}`), enabled: !!selectedPlan });
  const { data: bucketsData } = useQuery<{ buckets: any[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });

  const inv = () => { qc.invalidateQueries({ queryKey: ["allocation-plans"] }); qc.invalidateQueries({ queryKey: ["allocation-plan", selectedPlan] }); };

  const createPlanMut = useMutation({ mutationFn: () => api.post("/allocation-plans", planForm), onSuccess: (r: any) => { inv(); setPlanModal(false); setSelectedPlan(r.plan_id); }, onError: (e: Error) => setErr(e.message) });
  const activateMut = useMutation({ mutationFn: (id: string) => api.post(`/allocation-plans/${id}/activate`, {}), onSuccess: inv });
  const saveItemMut = useMutation({
    mutationFn: () => editingItem
      ? api.put(`/allocation-plans/${selectedPlan}/items/${editingItem.item_id}`, { ...itemForm, bucket_id: itemForm.bucket_id || null })
      : api.post(`/allocation-plans/${selectedPlan}/items`, { ...itemForm, bucket_id: itemForm.bucket_id || null }),
    onSuccess: () => { inv(); setItemModal(false); }, onError: (e: Error) => setErr(e.message),
  });
  const deleteItemMut = useMutation({ mutationFn: (itemId: string) => api.del(`/allocation-plans/${selectedPlan}/items/${itemId}`), onSuccess: inv });
  const fundMut = useMutation({ mutationFn: () => api.post(`/allocation-plans/${selectedPlan}/items/${fundModal.item_id}/fund`, { amount: fundAmount }), onSuccess: () => { inv(); setFundModal(null); }, onError: (e: Error) => setErr(e.message) });

  const plan = planDetail;
  const plans = plansData?.plans ?? [];
  const buckets = bucketsData?.buckets ?? [];
  const totalPlanned = plan?.items?.reduce((s: number, i: any) => s + i.planned_amount, 0) ?? 0;
  const totalFunded = plan?.items?.reduce((s: number, i: any) => s + i.funded_amount, 0) ?? 0;
  const fundedPct = totalPlanned > 0 ? Math.round((totalFunded / totalPlanned) * 100) : 0;

  return (
    <div className="p-5 space-y-4">
      {/* Warning banner placeholder */}
      <div className="flex items-center justify-between px-4 py-3 rounded-xl border border-yellow-200 bg-yellow-50 dark:bg-yellow-900/20">
        <div className="flex items-center gap-3">
          <span className="text-warning">⚠️</span>
          <div><p className="text-sm font-semibold text-yellow-800 dark:text-yellow-300">Attention needed</p><p className="text-xs text-yellow-700 dark:text-yellow-400">Your emergency fund coverage is below the recommended 3 months.</p></div>
        </div>
        <button className="text-xs font-semibold text-warning border border-warning/30 px-3 py-1.5 rounded-lg">Review now</button>
      </div>

      {/* Header */}
      <Card padding="md">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-6">
            <div>
              <p className="text-xs text-[var(--muted)]">Pay Cycle</p>
              <p className="font-bold">{plan?.month ?? currentMonthYM()}</p>
              <p className="text-xs text-[var(--muted)]">3 days until payday</p>
            </div>
            <div>
              <p className="text-xs text-[var(--muted)]">Expected Income</p>
              <p className="text-2xl font-bold tabular">{bal(plan?.expected_income ?? 0)}</p>
            </div>
            {plan && <Button size="sm" variant="secondary">Edit</Button>}
          </div>
          <div className="flex gap-2">
            {plan?.status === "draft" && <Button variant="primary" onClick={() => activateMut.mutate(plan.plan_id)}>Activate Plan</Button>}
            <Button variant="secondary" onClick={() => { setPlanForm({ month: currentMonthYM(), expected_income: 0, notes: "" }); setErr(""); setPlanModal(true); }}>+ New Plan</Button>
          </div>
        </div>
        {plans.length > 0 && (
          <div className="mt-3 pt-3 border-t border-[var(--border)]">
            <Select value={selectedPlan ?? ""} onChange={(e) => setSelectedPlan(e.target.value || null)} className="text-xs w-48">
              <option value="">— select plan —</option>
              {plans.map((p) => <option key={p.plan_id} value={p.plan_id}>{p.month} · {p.status}</option>)}
            </Select>
          </div>
        )}
      </Card>

      {plan && (
        <div className="grid grid-cols-3 gap-4">
          {/* Allocation Overview */}
          <div className="col-span-2 space-y-4">
            <Card green padding="md">
              <SectionTitle className="text-white/80">Allocation Overview <span className="text-white/50 text-xs font-normal">ⓘ</span></SectionTitle>
              <div className="grid grid-cols-3 gap-4 mb-3">
                {[
                  { label: "Planned", value: totalPlanned, sub: "100% of income" },
                  { label: "Funded", value: totalFunded, sub: `${fundedPct}% of plan` },
                  { label: "Remaining", value: totalPlanned - totalFunded, sub: `${100 - fundedPct}% left to fund` },
                ].map((s) => (
                  <div key={s.label}>
                    <p className="text-white/70 text-xs">{s.label}</p>
                    <p className="text-xl font-bold text-white tabular">{bal(s.value)}</p>
                    <p className="text-white/60 text-xs">{s.sub}</p>
                  </div>
                ))}
              </div>
              <div className="h-2 bg-white/20 rounded-full overflow-hidden">
                <div className="h-full bg-white rounded-full transition-all" style={{ width: `${fundedPct}%` }} />
              </div>
            </Card>

            {/* Items table */}
            <Card padding="sm">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <SectionTitle>Allocation Items</SectionTitle>
                  <Badge variant="gray">{plan.items?.length ?? 0} items</Badge>
                </div>
                <div className="flex gap-2">
                  <Button size="sm" variant="primary" onClick={() => { setEditingItem(null); setItemForm({ label: "", mode: "percent", value: 0, bucket_id: "", priority: 50 }); setErr(""); setItemModal(true); }}>+ Add Item</Button>
                </div>
              </div>
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                    <th className="text-left pb-2 font-medium w-6">#</th>
                    <th className="text-left pb-2 font-medium">Item</th>
                    <th className="text-left pb-2 font-medium">Linked Bucket</th>
                    <th className="text-left pb-2 font-medium">Mode</th>
                    <th className="text-right pb-2 font-medium">Planned</th>
                    <th className="text-right pb-2 font-medium">Funded</th>
                    <th className="text-left pb-2 font-medium w-24">Progress</th>
                    <th className="text-center pb-2 font-medium">Status</th>
                    <th className="pb-2"></th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[var(--border)]">
                  {(plan.items ?? []).map((item: any, idx: number) => (
                    <tr key={item.item_id} className="hover:bg-[var(--bg)] transition-colors">
                      <td className="py-2.5 text-[var(--muted)]">{idx + 1}</td>
                      <td className="py-2.5">
                        <p className="font-medium">{item.label}</p>
                        <p className="text-[var(--muted)]">{item.mode === "percent" ? `${item.value}% of income` : fmtMoney(item.value)}</p>
                      </td>
                      <td className="py-2.5 text-[var(--muted)]">{buckets.find((b) => b.bucket_id === item.bucket_id)?.name ?? "—"}</td>
                      <td className="py-2.5"><Badge variant="blue">{item.mode === "percent" ? `${item.value}%` : "Fixed"}</Badge></td>
                      <td className="py-2.5 text-right tabular">{bal(item.planned_amount)}</td>
                      <td className="py-2.5 text-right tabular">{bal(item.funded_amount)}</td>
                      <td className="py-2.5">
                        <ProgressBar value={item.planned_amount > 0 ? (item.funded_amount / item.planned_amount) * 100 : 0} showLabel />
                      </td>
                      <td className="py-2.5 text-center"><StatusBadge status={item.status} /></td>
                      <td className="py-2.5">
                        <div className="flex gap-1">
                          <button onClick={() => { setFundAmount(item.planned_amount - item.funded_amount); setFundModal(item); }} className="text-xs text-primary hover:underline">Fund</button>
                          <button onClick={() => { setEditingItem(item); setItemForm({ label: item.label, mode: item.mode, value: item.value, bucket_id: item.bucket_id ?? "", priority: item.priority }); setErr(""); setItemModal(true); }} className="text-[var(--muted)] hover:text-[var(--text)]">⋯</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  <tr className="font-semibold border-t-2 border-[var(--border)]">
                    <td className="py-2.5" colSpan={4}>Total</td>
                    <td className="py-2.5 text-right tabular">{bal(totalPlanned)}</td>
                    <td className="py-2.5 text-right tabular">{bal(totalFunded)}</td>
                    <td className="py-2.5"><ProgressBar value={fundedPct} showLabel /></td>
                    <td /><td />
                  </tr>
                </tbody>
              </table>
            </Card>
          </div>

          {/* Right: Plan Health + Summary + Timeline */}
          <div className="space-y-4">
            <Card padding="md">
              <SectionTitle>Plan Health</SectionTitle>
              <div className="flex flex-col items-center py-2">
                <DonutChart value={fundedPct} size={100} label={`${fundedPct}%`} sublabel="Funded" color={fundedPct >= 80 ? "#16a34a" : "#f59e0b"} />
                <div className="mt-3 space-y-1.5 w-full text-xs">
                  <div className="flex items-center gap-2"><span className="text-primary">✓</span><span>On track. You're on track to fund this plan.</span></div>
                  <div className="flex items-center gap-2"><span className="text-primary">✓</span><span>{(plan.items ?? []).filter((i: any) => i.status === "funded").length} items fully funded</span></div>
                  <div className="flex items-center gap-2"><span className="text-warning">○</span><span>{(plan.items ?? []).filter((i: any) => i.status !== "funded").length} items need funding</span></div>
                </div>
              </div>
            </Card>

            <Card padding="md">
              <SectionTitle>Plan Summary</SectionTitle>
              <div className="space-y-2 text-xs">
                {[["Expected Income", bal(plan.expected_income)], ["Total Planned", bal(totalPlanned)], ["Total Funded", bal(totalFunded)], ["Remaining", bal(totalPlanned - totalFunded)]].map(([k, v]) => (
                  <div key={k} className="flex justify-between">
                    <span className="text-[var(--muted)]">{k}</span>
                    <span className={`font-semibold tabular ${k === "Remaining" ? "text-danger" : ""}`}>{v}</span>
                  </div>
                ))}
              </div>
            </Card>

            <Card padding="md">
              <SectionTitle>Funding Timeline</SectionTitle>
              <div className="space-y-3 text-xs">
                {[
                  { label: "Plan created", done: true, sub: "Allocation plan saved" },
                  { label: "Plan reviewed", done: true, sub: "All items reviewed and confirmed" },
                  { label: "Payday", done: false, sub: `Income expected: ${bal(plan.expected_income)}`, active: true },
                  { label: "Fund items", done: false, sub: "Distribute income to all items" },
                  { label: "Review & adjust", done: false, sub: "Review progress and adjust plan" },
                ].map((step, i) => (
                  <div key={i} className="flex gap-3">
                    <div className={`w-5 h-5 rounded-full flex items-center justify-center text-xs shrink-0 mt-0.5 ${step.done ? "bg-primary text-white" : step.active ? "border-2 border-primary bg-white" : "border-2 border-[var(--border)] bg-white"}`}>
                      {step.done ? "✓" : ""}
                    </div>
                    <div>
                      <p className="font-medium">{step.label}</p>
                      <p className="text-[var(--muted)]">{step.sub}</p>
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          </div>
        </div>
      )}

      {/* Modals */}
      <Modal open={planModal} onClose={() => setPlanModal(false)} title="New Allocation Plan">
        <form onSubmit={(e) => { e.preventDefault(); createPlanMut.mutate(); }} className="space-y-3">
          <Input label="Month (YYYY-MM)" value={planForm.month} onChange={(e) => setPlanForm({ ...planForm, month: e.target.value })} required />
          <MoneyInput label="Expected Income" value={planForm.expected_income} onChange={(v) => setPlanForm({ ...planForm, expected_income: v })} />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={createPlanMut.isPending}>Create</Button>
            <Button type="button" variant="secondary" onClick={() => setPlanModal(false)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={itemModal} onClose={() => setItemModal(false)} title={editingItem ? "Edit Item" : "Add Item"}>
        <form onSubmit={(e) => { e.preventDefault(); saveItemMut.mutate(); }} className="space-y-3">
          <Input label="Label" value={itemForm.label} onChange={(e) => setItemForm({ ...itemForm, label: e.target.value })} required />
          <Select label="Mode" value={itemForm.mode} onChange={(e) => setItemForm({ ...itemForm, mode: e.target.value })}>
            <option value="percent">Percent of income (%)</option>
            <option value="fixed">Fixed amount (Rp)</option>
          </Select>
          {itemForm.mode === "percent"
            ? <Input label="Percentage" type="number" min={0} max={100} step={0.1} value={String(itemForm.value)} onChange={(e) => setItemForm({ ...itemForm, value: parseFloat(e.target.value) || 0 })} />
            : <MoneyInput label="Amount" value={itemForm.value} onChange={(v) => setItemForm({ ...itemForm, value: v })} />}
          <Select label="Bucket (optional)" value={itemForm.bucket_id} onChange={(e) => setItemForm({ ...itemForm, bucket_id: e.target.value })}>
            <option value="">— none —</option>
            {buckets.map((b: any) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveItemMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setItemModal(false)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={!!fundModal} onClose={() => setFundModal(null)} title={`Fund: ${fundModal?.label}`}>
        <div className="space-y-3">
          <p className="text-sm text-[var(--muted)]">Planned: {bal(fundModal?.planned_amount ?? 0)} · Funded: {bal(fundModal?.funded_amount ?? 0)}</p>
          <MoneyInput label="Amount to fund" value={fundAmount} onChange={setFundAmount} />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button variant="primary" className="flex-1" disabled={fundMut.isPending} onClick={() => fundMut.mutate()}>Confirm</Button>
            <Button variant="secondary" onClick={() => setFundModal(null)}>Cancel</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
