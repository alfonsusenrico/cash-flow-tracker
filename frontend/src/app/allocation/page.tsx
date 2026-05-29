"use client";
import { useEffect, useMemo, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { clampNumber, currentMonthYM, fmtMoney, parseClampedNumber } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Badge, StatusBadge } from "@/components/ui/Badge";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { DonutChart } from "@/components/ui/DonutChart";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

const IMPORTANCE_COLORS: Record<string, "red" | "blue" | "gray"> = { mandatory: "red", standard: "blue", flexible: "gray" };
const STATE_ICONS: Record<string, string> = {
  draft: "✏️", ready_for_payday: "⏳", needs_funding: "💸",
  in_progress: "🔄", mandatory_funded: "✅", complete: "🎉", closed: "🔒",
};

export default function AllocationPage() {
  const qc = useQueryClient();
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null);
  const [planModal, setPlanModal] = useState(false);
  const [editingPlan, setEditingPlan] = useState(false);
  const [itemModal, setItemModal] = useState(false);
  const [editingItem, setEditingItem] = useState<any>(null);
  const [historyDrawer, setHistoryDrawer] = useState(false);
  const [closingReportModal, setClosingReportModal] = useState(false);
  const [strategyModal, setStrategyModal] = useState(false);
  const [strategyPreview, setStrategyPreview] = useState<any>(null);
  const [planForm, setPlanForm] = useState({ month: currentMonthYM(), expected_income: 0, notes: "", funding_source_account_id: "", auto_fund_enabled: true });
  const [itemForm, setItemForm] = useState({ label: "", mode: "percent", value: 0, bucket_id: "", target_account_id: "", include_in_emergency_base: true, priority: 50, importance: "standard", category_id: "", notes: "" });
  const [err, setErr] = useState("");

  const { data: plansData } = useQuery<{ plans: any[] }>({ queryKey: ["allocation-plans"], queryFn: () => api.get("/allocation-plans") });
  const { data: planDetail } = useQuery<any>({ queryKey: ["allocation-plan", selectedPlan], queryFn: () => api.get(`/allocation-plans/${selectedPlan}`), enabled: !!selectedPlan });
  const { data: fundingStatus } = useQuery<any>({ queryKey: ["allocation-funding-status", selectedPlan], queryFn: () => api.get(`/allocation-plans/${selectedPlan}/funding-status`), enabled: !!selectedPlan });
  const { data: bucketsData } = useQuery<{ buckets: any[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });
  const { data: accountsData } = useQuery<{ accounts: any[] }>({ queryKey: ["accounts"], queryFn: () => api.get("/accounts") });
  const { data: categoriesData } = useQuery<{ categories: any[] }>({ queryKey: ["categories"], queryFn: () => api.get("/categories") });
  const { data: historyData } = useQuery<{ history: any[] }>({ queryKey: ["allocation-history", selectedPlan], queryFn: () => api.get(`/allocation-plans/${selectedPlan}/history?limit=50`), enabled: !!selectedPlan && historyDrawer });
  const { data: closingReport } = useQuery<any>({ queryKey: ["allocation-closing-report", selectedPlan], queryFn: () => api.get(`/allocation-plans/${selectedPlan}/closing-report`), enabled: !!selectedPlan && closingReportModal });

  const inv = (planId = selectedPlan) => {
    qc.invalidateQueries({ queryKey: ["allocation-plans"] });
    qc.invalidateQueries({ queryKey: ["allocation-funding-status", planId] });
    if (planId) qc.invalidateQueries({ queryKey: ["allocation-plan", planId] });
  };

  const savePlanMut = useMutation({
    mutationFn: () => editingPlan && selectedPlan ? api.put(`/allocation-plans/${selectedPlan}`, planForm) : api.post("/allocation-plans", planForm),
    onSuccess: (r: any) => {
      const planId = r?.plan_id ?? selectedPlan;
      inv(planId);
      qc.invalidateQueries({ queryKey: ["budgets"] });
      qc.invalidateQueries({ queryKey: ["accounts-summary"] });
      qc.invalidateQueries({ queryKey: ["dashboard"] });
      setPlanModal(false);
      setEditingPlan(false);
      if (r?.plan_id) setSelectedPlan(r.plan_id);
    },
    onError: (e: Error) => setErr(e.message),
  });
  const activateMut = useMutation({ mutationFn: (id: string) => api.post(`/allocation-plans/${id}/activate`, {}), onSuccess: (_r, id) => inv(id) });
  const deletePlanMut = useMutation({
    mutationFn: (id: string) => api.del(`/allocation-plans/${id}`),
    onSuccess: (_r, id) => {
      inv(id);
      qc.invalidateQueries({ queryKey: ["budgets"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["accounts-summary"] });
      qc.invalidateQueries({ queryKey: ["dashboard"] });
      setSelectedPlan(null);
    },
    onError: (e: Error) => setErr(e.message),
  });
  const saveItemMut = useMutation({
    mutationFn: () => {
      const payload = {
        ...itemForm,
        value: itemForm.mode === "percent" ? clampNumber(itemForm.value) : itemForm.value,
        bucket_id: itemForm.bucket_id || null,
        target_account_id: itemForm.target_account_id || null,
        category_id: itemForm.category_id || null,
        notes: itemForm.notes || null,
      };
      return editingItem
        ? api.put(`/allocation-plans/${selectedPlan}/items/${editingItem.item_id}`, payload)
        : api.post(`/allocation-plans/${selectedPlan}/items`, payload);
    },
    onSuccess: () => { inv(); setItemModal(false); }, onError: (e: Error) => setErr(e.message),
  });
  const deleteItemMut = useMutation({ mutationFn: (itemId: string) => api.del(`/allocation-plans/${selectedPlan}/items/${itemId}`), onSuccess: () => { inv(); setItemModal(false); setEditingItem(null); } });
  const toggleEmergencyBaseMut = useMutation({
    mutationFn: ({ itemId, include }: { itemId: string; include: boolean }) =>
      api.put(`/allocation-plans/${selectedPlan}/items/${itemId}/emergency-base`, { include_in_emergency_base: include }),
    onSuccess: () => inv(),
    onError: (e: Error) => setErr(e.message),
  });
  const allocateMut = useMutation({
    mutationFn: () => api.post(`/allocation-plans/${selectedPlan}/allocate-funds`, { source_account_id: plan?.funding_source_account_id || fundingStatus?.source_account_id || null }),
    onSuccess: () => {
      inv();
      qc.invalidateQueries({ queryKey: ["accounts-summary"] });
      qc.invalidateQueries({ queryKey: ["budgets"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
    },
    onError: (e: Error) => setErr(e.message),
  });
  const previewStrategyMut = useMutation({
    mutationFn: () => api.post<any>("/strategy-rules/from-allocation/preview", { plan_id: selectedPlan }),
    onSuccess: (r) => { setStrategyPreview(r); setStrategyModal(true); },
    onError: (e: Error) => setErr(e.message),
  });
  const applyStrategyMut = useMutation({
    mutationFn: () => api.post<any>("/strategy-rules/from-allocation/apply", { plan_id: selectedPlan }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["strategy-rules"] });
      setStrategyModal(false);
      setStrategyPreview(null);
    },
    onError: (e: Error) => setErr(e.message),
  });

  const plan = planDetail;
  const plans = useMemo(() => plansData?.plans ?? [], [plansData?.plans]);
  const buckets = bucketsData?.buckets ?? [];
  const accounts = accountsData?.accounts ?? [];
  const categories = categoriesData?.categories ?? [];
  const expenseCategories = categories.filter((c: any) => c.kind === "expense" && !c.is_archived);
  const payrollAccounts = accounts.filter((a: any) => a.is_payroll_source);
  const planState: string = plan?.plan_state ?? "";
  const planStateDesc: string = plan?.plan_state_description ?? "";
  const nextAction: string | null = plan?.next_recommended_action ?? null;
  const emergencyDefaultForBucket = (bucketId: string) => {
    const kind = buckets.find((b: any) => b.bucket_id === bucketId)?.kind;
    if (kind === "investment" || kind === "emergency" || kind === "goal" || kind === "sinking") return false;
    return true;
  };

  useEffect(() => {
    const planId = new URLSearchParams(window.location.search).get("plan");
    if (!planId || selectedPlan || !plans.some((p) => p.plan_id === planId)) return;
    setSelectedPlan(planId);
    window.history.replaceState(null, "", window.location.pathname);
  }, [plans, selectedPlan]);

  useEffect(() => {
    if (selectedPlan || plans.length === 0) return;
    setSelectedPlan(plans[0].plan_id);
  }, [plans, selectedPlan]);

  const expectedIncome = plan?.expected_income ?? 0;
  const totalPlanned = plan?.items?.reduce((s: number, i: any) => s + i.planned_amount, 0) ?? 0;
  const totalFunded = plan?.items?.reduce((s: number, i: any) => s + i.funded_amount, 0) ?? 0;
  const canDeletePlan = !!plan && plan.status !== "closed";
  const fundedPct = totalPlanned > 0 ? Math.round((totalFunded / totalPlanned) * 100) : 0;
  const plannedPct = expectedIncome > 0 ? Math.round((totalPlanned / expectedIncome) * 100) : 0;
  const unallocatedIncome = expectedIncome - totalPlanned;
  const unallocatedPct = expectedIncome > 0 ? Math.round((unallocatedIncome / expectedIncome) * 100) : 0;
  const stillToFund = Math.max(totalPlanned - totalFunded, 0);
  const plannedBarPct = Math.max(0, Math.min(plannedPct, 100));
  const itemPercentValue = itemForm.mode === "percent" ? clampNumber(itemForm.value) : itemForm.value;
  const itemPercentAmount = Math.round(expectedIncome * (itemPercentValue / 100));
  const itemFixedPercent = expectedIncome > 0 ? clampNumber(Math.round((itemForm.value / expectedIncome) * 100)) : 0;
  const emergencyHealth = plan?.health?.emergency_fund;
  const allocationBlocked = totalPlanned > expectedIncome;

  // Target absolute amount (in IDR) for the "Use all" button — absorbs the
  // remaining unallocated income on top of whatever this item is already
  // contributing in the saved plan. For a new item the saved planned amount
  // is 0, so the target equals unallocatedIncome.
  const editingItemPlanned = Number(editingItem?.planned_amount ?? 0);
  const useAllTargetPlanned = Math.max(0, unallocatedIncome + editingItemPlanned);
  const canUseAll = unallocatedIncome > 0 && expectedIncome > 0;
  const applyUseAll = () => {
    if (!canUseAll) return;
    if (itemForm.mode === "percent") {
      const targetPercent = clampNumber((useAllTargetPlanned / expectedIncome) * 100);
      setItemForm({ ...itemForm, value: targetPercent });
    } else {
      setItemForm({ ...itemForm, value: useAllTargetPlanned });
    }
  };

  return (
    <div className="workbench-page space-y-4">
      {emergencyHealth && emergencyHealth.status !== "ok" && (
        <div className="flex items-center justify-between px-4 py-3 rounded-xl border border-yellow-200 bg-yellow-50 dark:bg-yellow-900/20">
          <div className="flex items-center gap-3">
            <span className="text-warning">!</span>
            <div>
              <p className="text-sm font-semibold text-yellow-800 dark:text-yellow-300">Emergency fund coverage needs attention</p>
              <p className="text-xs text-yellow-700 dark:text-yellow-400">
                Current emergency buckets cover {emergencyHealth.coverage_months ?? 0} of {emergencyHealth.target_months} months.
                Monthly need is {bal(emergencyHealth.monthly_need)} and the remaining gap is {bal(emergencyHealth.gap)}.
              </p>
            </div>
          </div>
          <a href="#allocation-items" className="text-xs font-semibold text-warning border border-warning/30 px-3 py-1.5 rounded-lg">Review items</a>
        </div>
      )}

      {/* Header */}
      <Card padding="md">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-6">
            <div>
              <p className="text-xs text-[var(--muted)]">Pay Cycle Ending Month</p>
              <p className="font-bold">{plan?.month ?? currentMonthYM()}</p>
              <p className="text-xs text-[var(--muted)]">Funded from the previous payday</p>
            </div>
            <div>
              <p className="text-xs text-[var(--muted)]">Expected Income</p>
              <p className="text-2xl font-bold tabular">{bal(plan?.expected_income ?? 0)}</p>
            </div>
            {plan && (
              <Button
                size="sm"
                variant="secondary"
                onClick={() => { setEditingPlan(true); setPlanForm({ month: plan.month, expected_income: plan.expected_income, notes: plan.notes ?? "", funding_source_account_id: plan.funding_source_account_id ?? "", auto_fund_enabled: plan.auto_fund_enabled ?? true }); setErr(""); setPlanModal(true); }}
                disabled={plan.status === "closed"}
                title={plan.status === "closed" ? "Closed plans can no longer be edited" : undefined}
              >
                Edit
              </Button>
            )}
            {plan && (
              <Button
                size="sm"
                variant="secondary"
                onClick={() => { setErr(""); setStrategyPreview(null); previewStrategyMut.mutate(); }}
                disabled={previewStrategyMut.isPending || !selectedPlan || (plan.items?.length ?? 0) === 0}
              >
                {previewStrategyMut.isPending ? "Reviewing..." : "Review Strategy"}
              </Button>
            )}
          </div>
          <div className="flex gap-2">
            {plan?.status === "draft" && <Button variant="primary" onClick={() => activateMut.mutate(plan.plan_id)} disabled={allocationBlocked} title={allocationBlocked ? "Planned allocation exceeds expected income" : undefined}>Activate Plan</Button>}
            {plan?.status === "active" && (
              <Button
                variant="primary"
                onClick={() => { setErr(""); allocateMut.mutate(); }}
                disabled={allocateMut.isPending || !fundingStatus?.can_allocate}
                title={(fundingStatus?.reasons ?? []).join(" ") || undefined}
              >
                {allocateMut.isPending ? "Allocating..." : "Allocate Funds"}
              </Button>
            )}
            {canDeletePlan && (
              <Button
                variant="danger"
                onClick={() => {
                  const isActive = plan.status === "active";
                  const hasFunded = totalFunded > 0;
                  const lines = [`Delete allocation plan ${plan.month}?`];
                  if (isActive) {
                    lines.push("This plan is active. Items, funding runs, and any auto-generated budgets will be removed.");
                  } else {
                    lines.push("This removes the plan and its items.");
                  }
                  if (hasFunded) {
                    lines.push("Already-funded transfers (totaling " + fmtMoney(totalFunded) + ") will stay in your ledger and will not be reversed.");
                  }
                  if (confirm(lines.join("\n\n"))) deletePlanMut.mutate(plan.plan_id);
                }}
                disabled={deletePlanMut.isPending}
              >
                Delete Plan
              </Button>
            )}
            {plan && <Button variant="secondary" onClick={() => setHistoryDrawer(true)}>History</Button>}
            {plan && planState === "complete" && (
              <Button variant="secondary" onClick={() => setClosingReportModal(true)}>Close Month</Button>
            )}
            <Button variant="secondary" onClick={() => { setEditingPlan(false); setPlanForm({ month: currentMonthYM(), expected_income: 0, notes: "", funding_source_account_id: payrollAccounts.length === 1 ? payrollAccounts[0].account_id : "", auto_fund_enabled: true }); setErr(""); setPlanModal(true); }}>+ New Plan</Button>
          </div>
        </div>
        {plan?.status === "active" && fundingStatus && !fundingStatus.can_allocate && (fundingStatus.reasons?.length ?? 0) > 0 && (
          <div className="mt-3 rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-xs text-[var(--muted)]">
            <p className="font-semibold text-[var(--text)]">Allocate Funds is waiting</p>
            <p>{fundingStatus.reasons.join(" ")}</p>
          </div>
        )}
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
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Allocation Overview */}
          <div className="col-span-2 space-y-4">
            <Card green padding="md">
              <SectionTitle className="text-white/80">Allocation Overview <span className="text-white/50 text-xs font-normal">ⓘ</span></SectionTitle>
              <div className="grid grid-cols-3 gap-4 mb-3">
                {[
                  { label: "Planned", value: totalPlanned, sub: `${plannedPct}% of income` },
                  { label: "Funded", value: totalFunded, sub: `${fundedPct}% of plan` },
                  {
                    label: "Unallocated",
                    value: unallocatedIncome,
                    sub: unallocatedIncome >= 0
                      ? `${unallocatedPct}% income not assigned`
                      : `${Math.abs(unallocatedPct)}% over income`,
                  },
                ].map((s) => (
                  <div key={s.label}>
                    <p className="text-white/70 text-xs">{s.label}</p>
                    <p className="text-xl font-bold text-white tabular">{bal(s.value)}</p>
                    <p className="text-white/60 text-xs">{s.sub}</p>
                  </div>
                ))}
              </div>
              <div className="h-2 bg-white/20 rounded-full overflow-hidden">
                <div className="h-full bg-white rounded-full transition-all" style={{ width: `${plannedBarPct}%` }} />
              </div>
            </Card>

            {/* Items table */}
            <Card padding="sm" id="allocation-items">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <SectionTitle>Allocation Items</SectionTitle>
                  <Badge variant="gray">{plan.items?.length ?? 0} items</Badge>
                </div>
                <div className="flex gap-2">
                  <Button size="sm" variant="primary" onClick={() => { setEditingItem(null); setItemForm({ label: "", mode: "percent", value: 0, bucket_id: "", target_account_id: "", include_in_emergency_base: true, priority: 50, importance: "standard", category_id: "", notes: "" }); setErr(""); setItemModal(true); }}>+ Add Item</Button>
                </div>
              </div>
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                    <th className="text-left pb-2 font-medium w-6">#</th>
                    <th className="text-left pb-2 font-medium">Item</th>
                    <th className="text-left pb-2 font-medium">Bucket</th>
                    <th className="text-left pb-2 font-medium">Type</th>
                    <th className="text-left pb-2 font-medium">Target Account</th>
                    <th className="text-left pb-2 font-medium">Mode</th>
                    <th className="text-left pb-2 font-medium">Importance</th>
                    <th className="text-right pb-2 font-medium">Planned</th>
                    <th className="text-right pb-2 font-medium">Funded</th>
                    <th className="text-right pb-2 font-medium">Actual</th>
                    <th className="text-right pb-2 font-medium">Drift</th>
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
                        {item.item_notes && <p className="text-[var(--muted)] text-xs italic">{item.item_notes}</p>}
                      </td>
                      <td className="py-2.5 text-[var(--muted)]">{item.bucket_name ?? "—"}</td>
                      <td className="py-2.5"><Badge variant="gray">{(item.group ?? "").replace(/_/g, " ")}</Badge></td>
                      <td className="py-2.5 text-[var(--muted)]">{item.target_account_name ?? "—"}</td>
                      <td className="py-2.5"><Badge variant="blue">{item.mode === "percent" ? `${item.value}%` : "Fixed"}</Badge></td>
                      <td className="py-2.5"><Badge variant={IMPORTANCE_COLORS[item.importance ?? "standard"] ?? "gray"}>{item.importance ?? "standard"}</Badge></td>
                      <td className="py-2.5 text-right tabular">{bal(item.planned_amount)}</td>
                      <td className="py-2.5 text-right tabular">{bal(item.funded_amount)}</td>
                      <td className="py-2.5 text-right tabular">{bal(item.actual_amount ?? 0)}</td>
                      <td className={`py-2.5 text-right tabular ${(item.drift_amount ?? 0) > 0 ? "text-danger" : (item.drift_amount ?? 0) < 0 ? "text-primary" : ""}`}>{(item.drift_amount ?? 0) > 0 ? "+" : ""}{bal(item.drift_amount ?? 0)}</td>
                      <td className="py-2.5">
                        <ProgressBar value={item.planned_amount > 0 ? (item.funded_amount / item.planned_amount) * 100 : 0} showLabel />
                      </td>
                      <td className="py-2.5 text-center"><StatusBadge status={item.status} /></td>
                      <td className="py-2.5">
                        <div className="flex gap-1">
                          <button onClick={() => { setEditingItem(item); setItemForm({ label: item.label, mode: item.mode, value: item.mode === "percent" ? clampNumber(item.value) : item.value, bucket_id: item.bucket_id ?? "", target_account_id: item.target_account_id ?? "", include_in_emergency_base: item.include_in_emergency_base ?? true, priority: item.priority, importance: item.importance ?? "standard", category_id: item.category_id ?? "", notes: item.item_notes ?? "" }); setErr(""); setItemModal(true); }} className="text-xs text-[var(--muted)] hover:text-[var(--text)]">Edit</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  <tr className="font-semibold border-t-2 border-[var(--border)]">
                    <td className="py-2.5" colSpan={7}>Total</td>
                    <td className="py-2.5 text-right tabular">{bal(totalPlanned)}</td>
                    <td className="py-2.5 text-right tabular">{bal(totalFunded)}</td>
                    <td className="py-2.5 text-right tabular">{bal(plan?.items?.reduce((s: number, i: any) => s + (i.actual_amount ?? 0), 0) ?? 0)}</td>
                    <td className="py-2.5 text-right tabular">{bal(plan?.items?.reduce((s: number, i: any) => s + (i.drift_amount ?? 0), 0) ?? 0)}</td>
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
                {/* Mandatory funded donut */}
                {(() => {
                  const mandatoryItems = (plan.items ?? []).filter((i: any) => i.importance === "mandatory");
                  const mandatoryPlanned = mandatoryItems.reduce((s: number, i: any) => s + i.planned_amount, 0);
                  const mandatoryFunded = mandatoryItems.reduce((s: number, i: any) => s + i.funded_amount, 0);
                  const mandatoryPct = mandatoryPlanned > 0 ? Math.round((mandatoryFunded / mandatoryPlanned) * 100) : 0;
                  if (mandatoryItems.length === 0) return null;
                  return (
                    <div className="mt-2 flex flex-col items-center">
                      <DonutChart value={mandatoryPct} size={64} label={`${mandatoryPct}%`} sublabel="Mandatory" color={mandatoryPct >= 100 ? "#16a34a" : "#ef4444"} />
                    </div>
                  );
                })()}
                <div className="mt-3 space-y-1.5 w-full text-xs">
                  <div className="flex items-center gap-2">
                    <span>{STATE_ICONS[planState] ?? "○"}</span>
                    <span className="font-semibold">{planStateDesc || planState}</span>
                  </div>
                  <div className="flex items-center gap-2"><span className="text-primary">✓</span><span>{(plan.items ?? []).filter((i: any) => i.status === "funded").length} items fully funded</span></div>
                  <div className="flex items-center gap-2"><span className="text-warning">○</span><span>{(plan.items ?? []).filter((i: any) => i.status !== "funded").length} items need funding</span></div>
                  {emergencyHealth && (
                    <div className="pt-2 mt-2 border-t border-[var(--border)] space-y-1">
                      <div className="flex justify-between gap-2">
                        <span className="text-[var(--muted)]">Emergency coverage</span>
                        <span className="font-semibold tabular">{emergencyHealth.coverage_months ?? 0} months</span>
                      </div>
                      <div className="flex justify-between gap-2">
                        <span className="text-[var(--muted)]">6-month target</span>
                        <span className="font-semibold tabular">{bal(emergencyHealth.target_amount)}</span>
                      </div>
                      <div className="flex justify-between gap-2">
                        <span className="text-[var(--muted)]">Current emergency buckets</span>
                        <span className="font-semibold tabular">{bal(emergencyHealth.current_amount)}</span>
                      </div>
                      <div className="flex justify-between gap-2">
                        <span className="text-[var(--muted)]">Monthly baseline</span>
                        <span className="font-semibold tabular">{bal(emergencyHealth.monthly_need)}</span>
                      </div>
                      <div className="pt-2 mt-2 border-t border-[var(--border)]">
                        <p className="mb-1 font-semibold text-[var(--text)]">Baseline items</p>
                        <div className="space-y-1.5 max-h-44 overflow-auto pr-1">
                          {(emergencyHealth.baseline_items ?? []).map((item: any) => (
                            <label key={item.item_id} className="flex items-start gap-2 rounded-md px-1 py-1 hover:bg-[var(--bg)]">
                              <input
                                type="checkbox"
                                className="mt-0.5"
                                checked={!!item.include_in_emergency_base}
                                disabled={toggleEmergencyBaseMut.isPending}
                                onChange={(e) => toggleEmergencyBaseMut.mutate({ itemId: item.item_id, include: e.target.checked })}
                              />
                              <span className="min-w-0 flex-1">
                                <span className="block truncate font-medium text-[var(--text)]">{item.label}</span>
                                <span className="block text-[var(--muted)]">{item.group?.replace(/_/g, " ") ?? "allocation item"}</span>
                              </span>
                              <span className="tabular font-semibold">{bal(item.planned_amount)}</span>
                            </label>
                          ))}
                          {(emergencyHealth.baseline_items?.length ?? 0) === 0 && (
                            <p className="text-[var(--muted)]">Add allocation items to define the spending baseline.</p>
                          )}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </Card>

            {nextAction && (
              <Card padding="md">
                <SectionTitle>Next Recommended Action</SectionTitle>
                <p className="text-xs text-[var(--text)] mt-1">{nextAction}</p>
              </Card>
            )}

            <Card padding="md">
              <SectionTitle>Plan Summary</SectionTitle>
              <div className="space-y-2 text-xs">
                {[
                  ["Expected Income", bal(expectedIncome), ""],
                  ["Total Planned", bal(totalPlanned), ""],
                  ["Unallocated Income", bal(unallocatedIncome), unallocatedIncome < 0 ? "text-danger" : "text-primary"],
                  ["Total Funded", bal(totalFunded), ""],
                  ["Still to Fund", bal(stillToFund), stillToFund > 0 ? "text-warning" : "text-primary"],
                ].map(([k, v, tone]) => (
                  <div key={k} className="flex justify-between">
                    <span className="text-[var(--muted)]">{k}</span>
                    <span className={`font-semibold tabular ${tone}`}>{v}</span>
                  </div>
                ))}
              </div>
            </Card>

            <Card padding="md">
              <SectionTitle>Funding Timeline</SectionTitle>
              <div className="space-y-3 text-xs">
                {(() => {
                  const items = plan.items ?? [];
                  const mandatoryItems = items.filter((i: any) => i.importance === "mandatory");
                  const strategicItems = items.filter((i: any) => i.importance === "standard");
                  const flexibleItems = items.filter((i: any) => i.importance === "flexible");
                  const allFunded = (arr: any[]) => arr.length > 0 && arr.every((i: any) => i.status === "funded" || i.status === "overflowed");
                  const payrollReceived = fundingStatus?.payroll_received ?? false;
                  const steps = [
                    { label: "Plan created", done: !!plan.created_at, sub: plan.created_at ? new Date(plan.created_at).toLocaleDateString() : "" },
                    { label: "Plan reviewed", done: plan.status !== "draft", sub: plan.activated_at ? `Activated ${new Date(plan.activated_at).toLocaleDateString()}` : "Activate to mark reviewed" },
                    { label: "Payroll received", done: payrollReceived, sub: payrollReceived ? "Income confirmed" : `Expected: ${bal(plan.expected_income)}` },
                    { label: "Mandatory items funded", done: allFunded(mandatoryItems), sub: mandatoryItems.length === 0 ? "No mandatory items" : `${mandatoryItems.filter((i: any) => i.status === "funded").length}/${mandatoryItems.length} funded` },
                    { label: "Standard items funded", done: allFunded(strategicItems), sub: `${strategicItems.filter((i: any) => i.status === "funded").length}/${strategicItems.length} funded` },
                    { label: "Flexible items funded", done: allFunded(flexibleItems), sub: `${flexibleItems.filter((i: any) => i.status === "funded").length}/${flexibleItems.length} funded` },
                    { label: "Month reviewed", done: plan.status === "closed", sub: "Review spending vs plan" },
                    { label: "Period closed", done: plan.status === "closed", sub: plan.status === "closed" ? "Closed" : "Close period when done" },
                  ];
                  return steps.map((step, i) => (
                    <div key={i} className="flex gap-3">
                      <div className={`w-5 h-5 rounded-full flex items-center justify-center text-xs shrink-0 mt-0.5 ${step.done ? "bg-primary text-white" : "border-2 border-[var(--border)] bg-white"}`}>
                        {step.done ? "✓" : ""}
                      </div>
                      <div>
                        <p className="font-medium">{step.label}</p>
                        <p className="text-[var(--muted)]">{step.sub}</p>
                      </div>
                    </div>
                  ));
                })()}
              </div>
            </Card>
          </div>
        </div>
      )}

      {/* Modals */}
      <Modal open={planModal} onClose={() => setPlanModal(false)} title={editingPlan ? "Edit Allocation Plan" : "New Allocation Plan"}>
        <form onSubmit={(e) => { e.preventDefault(); savePlanMut.mutate(); }} className="space-y-3">
          <Input
            label="Pay Cycle Ending Month"
            type="month"
            value={planForm.month}
            onChange={(e) => setPlanForm({ ...planForm, month: e.target.value })}
            required
            disabled={editingPlan}
          />
          <MoneyInput label="Expected Income" value={planForm.expected_income} onChange={(v) => setPlanForm({ ...planForm, expected_income: v })} />
          <Select label="Payroll Source Account" value={planForm.funding_source_account_id} onChange={(e) => setPlanForm({ ...planForm, funding_source_account_id: e.target.value })}>
            <option value="">— choose when funding —</option>
            {accounts.map((a: any) => <option key={a.account_id} value={a.account_id}>{a.account_name}{a.is_payroll_source ? " · payroll" : ""}</option>)}
          </Select>
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={planForm.auto_fund_enabled} onChange={(e) => setPlanForm({ ...planForm, auto_fund_enabled: e.target.checked })} />
            Automatically allocate funds on payroll date
          </label>
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={savePlanMut.isPending}>{editingPlan ? "Save" : "Create"}</Button>
            <Button type="button" variant="secondary" onClick={() => { setPlanModal(false); setEditingPlan(false); }}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={itemModal} onClose={() => setItemModal(false)} title={editingItem ? "Edit Item" : "Add Item"}>
        <form onSubmit={(e) => { e.preventDefault(); saveItemMut.mutate(); }} className="space-y-3">
          <Input label="Label" value={itemForm.label} onChange={(e) => setItemForm({ ...itemForm, label: e.target.value })} required />
          <Select label="Mode" value={itemForm.mode} onChange={(e) => {
            const mode = e.target.value;
            setItemForm({ ...itemForm, mode, value: mode === "percent" ? clampNumber(itemForm.value) : itemForm.value });
          }}>
            <option value="percent">Percent of income (%)</option>
            <option value="fixed">Fixed amount (Rp)</option>
          </Select>
          {itemForm.mode === "percent"
            ? <Input label="Percentage" type="number" min={0} max={100} step={0.1} value={String(itemForm.value)} onChange={(e) => setItemForm({ ...itemForm, value: parseClampedNumber(e.target.value) })} />
            : <MoneyInput label="Amount" value={itemForm.value} onChange={(v) => setItemForm({ ...itemForm, value: v })} />}
          <div className="rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-xs">
            <p className="text-[var(--muted)]">Conversion based on expected income</p>
            <p className="font-semibold tabular text-[var(--text)]">
              {expectedIncome > 0
                ? itemForm.mode === "percent"
                  ? `${itemPercentValue || 0}% = ${bal(itemPercentAmount)}`
                  : `${bal(itemForm.value)} = ${itemFixedPercent}%`
                : "Set expected income on the plan to calculate this."}
            </p>
          </div>
          {/* Importance */}
          <div>
            <p className="text-xs font-medium text-[var(--muted)] mb-1">Importance</p>
            <div className="flex rounded-lg overflow-hidden border border-[var(--border)]">
              {(["mandatory", "standard", "flexible"] as const).map((imp) => (
                <button key={imp} type="button" onClick={() => setItemForm({ ...itemForm, importance: imp })}
                  className={`flex-1 py-1.5 text-xs font-medium transition-colors capitalize ${itemForm.importance === imp ? (imp === "mandatory" ? "bg-danger text-white" : imp === "standard" ? "bg-primary text-white" : "bg-[var(--muted)] text-white") : "bg-[var(--surface)] text-[var(--muted)]"}`}>
                  {imp}
                </button>
              ))}
            </div>
          </div>
          <Select label="Bucket (optional)" value={itemForm.bucket_id} onChange={(e) => {
            const bucketId = e.target.value;
            const linked = buckets.find((b: any) => b.bucket_id === bucketId)?.linked_account_ids;
            setItemForm({
              ...itemForm,
              bucket_id: bucketId,
              target_account_id: linked?.length === 1 ? linked[0] : itemForm.target_account_id,
              include_in_emergency_base: emergencyDefaultForBucket(bucketId),
            });
          }}>
            <option value="">— none —</option>
            {buckets.map((b: any) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          <Select label="Target Account" value={itemForm.target_account_id} onChange={(e) => setItemForm({ ...itemForm, target_account_id: e.target.value })}>
            <option value="">— infer from bucket if one account is linked —</option>
            {accounts.map((a: any) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          <Select label="Spending Category (for actuals)" value={itemForm.category_id} onChange={(e) => setItemForm({ ...itemForm, category_id: e.target.value })}>
            <option value="">— none (use target account) —</option>
            {expenseCategories.map((c: any) => <option key={c.category_id} value={c.category_id}>{c.name}</option>)}
          </Select>
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={itemForm.include_in_emergency_base} onChange={(e) => setItemForm({ ...itemForm, include_in_emergency_base: e.target.checked })} />
            Include this spending in emergency fund coverage
          </label>
          <Input label="Notes (optional)" value={itemForm.notes} onChange={(e) => setItemForm({ ...itemForm, notes: e.target.value })} placeholder="e.g. Fixed until 2027-03" />
          <div className="rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-xs">
            <p className="text-[var(--muted)]">Remaining unallocated balance</p>
            <div className="flex items-center justify-between gap-2">
              <p className={`font-semibold tabular ${unallocatedIncome < 0 ? "text-danger" : "text-[var(--text)]"}`}>{bal(unallocatedIncome)}</p>
              <button
                type="button"
                onClick={applyUseAll}
                disabled={!canUseAll}
                title={canUseAll ? `Add ${bal(unallocatedIncome)} to this item` : (unallocatedIncome <= 0 ? "No remaining income to add" : "Set expected income on the plan first")}
                className="text-xs font-semibold text-primary hover:underline disabled:text-[var(--muted)] disabled:no-underline disabled:cursor-not-allowed"
              >
                Use all
              </button>
            </div>
          </div>
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            {editingItem && <Button type="button" variant="danger" onClick={() => confirm(`Delete "${editingItem.label}"?`) && deleteItemMut.mutate(editingItem.item_id)} disabled={deleteItemMut.isPending}>Delete</Button>}
            <Button type="submit" variant="primary" className="flex-1" disabled={saveItemMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setItemModal(false)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={strategyModal} onClose={() => setStrategyModal(false)} title="Review Strategy From Allocation">
        <div className="space-y-3">
          <p className="text-sm text-[var(--muted)]">
            This converts the selected monthly allocation into reusable percentage-based strategy rules.
            It will update matching rules by bucket or name and create rules for new items.
          </p>
          {strategyPreview && (
            <>
              <div className="grid grid-cols-3 gap-2">
                {[
                  ["Income", bal(strategyPreview.expected_income)],
                  ["Month", strategyPreview.month],
                  ["Suggestions", String(strategyPreview.suggestions?.length ?? 0)],
                ].map(([label, value]) => (
                  <div key={label} className="rounded-lg bg-[var(--bg)] p-2 text-xs">
                    <p className="text-[var(--muted)]">{label}</p>
                    <p className="font-semibold tabular">{value}</p>
                  </div>
                ))}
              </div>
              <div className="space-y-2 max-h-80 overflow-auto pr-1">
                {(strategyPreview.suggestions ?? []).map((s: any) => (
                  <div key={`${s.name}-${s.target_bucket_id ?? ""}`} className="rounded-lg border border-[var(--border)] p-3 text-xs">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="font-semibold">{s.name}</p>
                        <p className="text-[var(--muted)]">{s.target_bucket_name ?? "No linked bucket"} · {s.group.replace(/_/g, " ")}</p>
                      </div>
                      <Badge variant={s.action === "create" ? "green" : s.action === "update" ? "blue" : "gray"}>{s.action}</Badge>
                    </div>
                    <div className="mt-2 flex justify-between gap-3">
                      <span className="text-[var(--muted)]">Allocation amount</span>
                      <span className="font-semibold tabular">{bal(s.source_amount)} = {s.value}%</span>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button
              variant="primary"
              className="flex-1"
              disabled={applyStrategyMut.isPending || !strategyPreview || (strategyPreview.suggestions?.length ?? 0) === 0}
              onClick={() => { setErr(""); applyStrategyMut.mutate(); }}
            >
              {applyStrategyMut.isPending ? "Applying..." : "Apply to Strategy"}
            </Button>
            <Button variant="secondary" onClick={() => setStrategyModal(false)}>Cancel</Button>
          </div>
        </div>
      </Modal>

      {/* History drawer */}
      <Modal open={historyDrawer} onClose={() => setHistoryDrawer(false)} title="Plan History">
        <div className="space-y-2 max-h-[70vh] overflow-auto pr-1">
          {(historyData?.history ?? []).length === 0 && <p className="text-xs text-[var(--muted)]">No history yet.</p>}
          {(historyData?.history ?? []).map((entry: any) => (
            <div key={entry.audit_id} className="rounded-lg border border-[var(--border)] p-3 text-xs">
              <div className="flex items-center justify-between gap-2 mb-1">
                <span className="font-semibold capitalize">{entry.kind === "item" ? `Item · ${entry.action}` : `Plan · ${entry.action}`}</span>
                <span className="text-[var(--muted)]">{new Date(entry.created_at).toLocaleString()}</span>
              </div>
              {entry.kind === "item" && entry.before_state && (
                <p className="text-[var(--muted)]">
                  {entry.before_state.label ?? entry.item_id}
                  {entry.before_state.planned_amount !== entry.after_state?.planned_amount
                    ? ` · planned ${fmtMoney(entry.before_state.planned_amount)} → ${fmtMoney(entry.after_state?.planned_amount ?? 0)}`
                    : ""}
                </p>
              )}
              {entry.kind === "plan" && entry.before_state && entry.after_state && (
                <p className="text-[var(--muted)]">
                  {entry.before_state.expected_income !== entry.after_state.expected_income
                    ? `income ${fmtMoney(entry.before_state.expected_income)} → ${fmtMoney(entry.after_state.expected_income)}`
                    : entry.action}
                </p>
              )}
              {entry.reason && <p className="text-[var(--muted)] italic">{entry.reason}</p>}
            </div>
          ))}
        </div>
        <div className="pt-2">
          <Button variant="secondary" onClick={() => setHistoryDrawer(false)}>Close</Button>
        </div>
      </Modal>

      {/* Closing Report modal */}
      <Modal open={closingReportModal} onClose={() => setClosingReportModal(false)} title={`Closing Report · ${plan?.month ?? ""}`}>
        {closingReport ? (
          <div className="space-y-4 max-h-[75vh] overflow-auto pr-1">
            <div className="grid grid-cols-2 gap-3 text-xs">
              {[
                ["Expected Income", fmtMoney(closingReport.income?.expected ?? 0)],
                ["Actual Income", fmtMoney(closingReport.income?.actual ?? 0)],
                ["Income Variance", fmtMoney(closingReport.income?.variance ?? 0)],
                ["Total Planned", fmtMoney(closingReport.spending?.planned ?? 0)],
                ["Total Funded", fmtMoney(closingReport.spending?.funded ?? 0)],
                ["Total Actual Spent", fmtMoney(closingReport.spending?.actual ?? 0)],
                ["Total Drift", fmtMoney(closingReport.spending?.drift ?? 0)],
                ["Emergency Coverage", `${closingReport.emergency?.coverage_months_end ?? "—"} months`],
              ].map(([k, v]) => (
                <div key={k} className="rounded-lg bg-[var(--bg)] p-2">
                  <p className="text-[var(--muted)]">{k}</p>
                  <p className="font-semibold tabular">{v}</p>
                </div>
              ))}
            </div>
            <div>
              <p className="text-xs font-semibold mb-2">Items</p>
              <div className="space-y-1">
                {(closingReport.items ?? []).map((item: any) => (
                  <div key={item.item_id} className="flex items-center justify-between text-xs border-b border-[var(--border)] py-1.5">
                    <div className="flex items-center gap-2">
                      <Badge variant={IMPORTANCE_COLORS[item.importance ?? "standard"] ?? "gray"}>{item.importance}</Badge>
                      <span>{item.label}</span>
                    </div>
                    <div className="flex gap-3 tabular text-right">
                      <span className="text-[var(--muted)]">{fmtMoney(item.planned)}</span>
                      <span>{fmtMoney(item.actual)}</span>
                      <span className={item.drift > 0 ? "text-danger" : item.drift < 0 ? "text-primary" : ""}>{item.drift > 0 ? "+" : ""}{fmtMoney(item.drift)}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            {(closingReport.overspent_items ?? []).length > 0 && (
              <div className="rounded-lg border border-yellow-200 bg-yellow-50 dark:bg-yellow-900/20 p-3 text-xs">
                <p className="font-semibold text-yellow-800 dark:text-yellow-300 mb-1">Overspent items</p>
                {closingReport.overspent_items.map((i: any) => (
                  <p key={i.label} className="text-yellow-700 dark:text-yellow-400">{i.label}: +{fmtMoney(i.drift)}</p>
                ))}
              </div>
            )}
            <p className="text-xs text-[var(--muted)]">Plan edits this cycle: {closingReport.audit_summary?.plan_edits ?? 0} · Item edits: {closingReport.audit_summary?.item_edits ?? 0}</p>
          </div>
        ) : (
          <p className="text-xs text-[var(--muted)]">Loading report…</p>
        )}
        <div className="flex gap-2 pt-2">
          <Button variant="secondary" onClick={() => setClosingReportModal(false)}>Close</Button>
        </div>
      </Modal>
    </div>
  );
}
