"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface Projection {
  inflation_adjusted_target: number;
  current_amount: number;
  progress_pct: number;
  months_remaining: number | null;
  required_monthly: number | null;
  eta_months: number | null;
  feasible: boolean;
  shortfall_per_month: number;
}

interface Goal {
  goal_id: string;
  name: string;
  target_amount: number;
  target_date: string | null;
  current_amount: number;
  inflation_rate: number;
  expected_return: number;
  linked_bucket_name: string | null;
  priority: number;
  status: string;
  notes: string | null;
  projection: Projection;
}

interface Bucket { bucket_id: string; name: string; }

const EMPTY = { name: "", target_amount: 0, target_date: "", inflation_rate: 5, expected_return: 6, linked_bucket_id: "", priority: 50, notes: "", status: "active" };

export default function GoalsPage() {
  const qc = useQueryClient();
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Goal | null>(null);
  const [form, setForm] = useState(EMPTY);
  const [contributeGoal, setContributeGoal] = useState<Goal | null>(null);
  const [contributeAmount, setContributeAmount] = useState(0);
  const [err, setErr] = useState("");

  const { data } = useQuery<{ goals: Goal[] }>({ queryKey: ["goals"], queryFn: () => api.get("/goals") });
  const { data: bucketsData } = useQuery<{ buckets: Bucket[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });

  const inv = () => qc.invalidateQueries({ queryKey: ["goals"] });

  const saveMut = useMutation({
    mutationFn: () => {
      const payload = {
        ...form,
        target_amount: form.target_amount,
        target_date: form.target_date || null,
        inflation_rate: form.inflation_rate / 100,
        expected_return: form.expected_return / 100,
        linked_bucket_id: form.linked_bucket_id || null,
      };
      return editing ? api.put(`/goals/${editing.goal_id}`, payload) : api.post("/goals", payload);
    },
    onSuccess: () => { inv(); setModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => api.del(`/goals/${id}`),
    onSuccess: inv,
  });

  const contributeMut = useMutation({
    mutationFn: () => api.post(`/goals/${contributeGoal!.goal_id}/contribute`, { amount: contributeAmount }),
    onSuccess: () => { inv(); setContributeGoal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  function openCreate() { setEditing(null); setForm(EMPTY); setErr(""); setModal("create"); }
  function openEdit(g: Goal) {
    setEditing(g);
    setForm({
      name: g.name, target_amount: g.target_amount,
      target_date: g.target_date?.slice(0, 10) ?? "",
      inflation_rate: Math.round(g.inflation_rate * 100),
      expected_return: Math.round(g.expected_return * 100),
      linked_bucket_id: "", priority: g.priority, notes: g.notes ?? "", status: g.status,
    });
    setErr(""); setModal("edit");
  }

  const goals = data?.goals ?? [];
  const buckets = bucketsData?.buckets ?? [];

  return (
    <div className="p-4 max-w-3xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Financial Goals</h1>
          <p className="text-xs text-[var(--muted)]">Track progress toward your financial targets.</p>
        </div>
        <Button variant="primary" size="sm" onClick={openCreate}>+ New Goal</Button>
      </div>

      <div className="space-y-3">
        {goals.length === 0 && <p className="text-[var(--muted)] text-sm text-center py-8">No goals yet.</p>}
        {goals.map((g) => {
          const p = g.projection;
          return (
            <div key={g.goal_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4 space-y-3">
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="font-semibold">{g.name}</div>
                  <div className="text-xs text-[var(--muted)] mt-0.5">
                    Target: {fmtMoney(p.inflation_adjusted_target)}
                    {g.target_date && ` · by ${g.target_date.slice(0, 10)}`}
                    {g.linked_bucket_name && ` · ${g.linked_bucket_name}`}
                  </div>
                </div>
                <div className="flex gap-1 shrink-0">
                  <Button size="sm" variant="primary" onClick={() => { setContributeAmount(p.required_monthly ?? 0); setContributeGoal(g); }}>+ Contribute</Button>
                  <Button size="sm" variant="ghost" onClick={() => openEdit(g)}>Edit</Button>
                  <Button size="sm" variant="danger" onClick={() => confirm(`Cancel goal "${g.name}"?`) && deleteMut.mutate(g.goal_id)}>✕</Button>
                </div>
              </div>

              {/* Progress bar */}
              <div>
                <div className="flex justify-between text-xs text-[var(--muted)] mb-1">
                  <span>{fmtMoney(g.current_amount)} saved</span>
                  <span>{p.progress_pct}%</span>
                </div>
                <div className="h-2 bg-[var(--bg)] rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${p.progress_pct >= 100 ? "bg-green-500" : p.feasible ? "bg-blue-500" : "bg-yellow-400"}`}
                    style={{ width: `${Math.min(p.progress_pct, 100)}%` }}
                  />
                </div>
              </div>

              {/* Projection details */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs">
                {p.required_monthly != null && (
                  <div className="bg-[var(--bg)] rounded-lg p-2">
                    <div className="text-[var(--muted)]">Required/month</div>
                    <div className="font-semibold">{fmtMoney(p.required_monthly)}</div>
                  </div>
                )}
                {p.eta_months != null && (
                  <div className="bg-[var(--bg)] rounded-lg p-2">
                    <div className="text-[var(--muted)]">ETA</div>
                    <div className="font-semibold">{p.eta_months === 0 ? "Done!" : `${p.eta_months} months`}</div>
                  </div>
                )}
                {p.months_remaining != null && (
                  <div className="bg-[var(--bg)] rounded-lg p-2">
                    <div className="text-[var(--muted)]">Time left</div>
                    <div className="font-semibold">{p.months_remaining} months</div>
                  </div>
                )}
                {!p.feasible && p.shortfall_per_month > 0 && (
                  <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-2">
                    <div className="text-red-500">Shortfall/month</div>
                    <div className="font-semibold text-red-500">{fmtMoney(p.shortfall_per_month)}</div>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Create/edit modal */}
      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Goal" : "New Goal"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-4">
          <Input label="Goal Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required placeholder="e.g. Emergency Fund, House Down Payment" />
          <MoneyInput label="Target Amount" value={form.target_amount} onChange={(v) => setForm({ ...form, target_amount: v })} required />
          <Input label="Target Date (optional)" type="date" value={form.target_date} onChange={(e) => setForm({ ...form, target_date: e.target.value })} />
          <div className="grid grid-cols-2 gap-3">
            <Input label="Inflation Rate (%/year)" type="number" min={0} max={50} step={0.1} value={String(form.inflation_rate)} onChange={(e) => setForm({ ...form, inflation_rate: parseFloat(e.target.value) || 0 })} />
            <Input label="Expected Return (%/year)" type="number" min={0} max={50} step={0.1} value={String(form.expected_return)} onChange={(e) => setForm({ ...form, expected_return: parseFloat(e.target.value) || 0 })} />
          </div>
          <Select label="Linked Bucket (optional)" value={form.linked_bucket_id} onChange={(e) => setForm({ ...form, linked_bucket_id: e.target.value })}>
            <option value="">— none —</option>
            {buckets.map((b) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          {editing && (
            <Select label="Status" value={form.status} onChange={(e) => setForm({ ...form, status: e.target.value })}>
              <option value="active">Active</option>
              <option value="paused">Paused</option>
              <option value="completed">Completed</option>
            </Select>
          )}
          <Input label="Notes (optional)" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      {/* Contribute modal */}
      <Modal open={!!contributeGoal} onClose={() => setContributeGoal(null)} title={`Contribute to: ${contributeGoal?.name}`}>
        <div className="space-y-4">
          <p className="text-sm text-[var(--muted)]">
            Current: {fmtMoney(contributeGoal?.current_amount ?? 0)} · Target: {fmtMoney(contributeGoal?.projection.inflation_adjusted_target ?? 0)}
          </p>
          <MoneyInput label="Amount" value={contributeAmount} onChange={setContributeAmount} />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button variant="primary" className="flex-1" disabled={contributeMut.isPending} onClick={() => contributeMut.mutate()}>Confirm</Button>
            <Button variant="secondary" onClick={() => setContributeGoal(null)}>Cancel</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
