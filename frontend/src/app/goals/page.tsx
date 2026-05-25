"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { DonutChart } from "@/components/ui/DonutChart";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface Goal {
  goal_id: string; name: string; target_amount: number; target_date: string | null;
  current_amount: number; inflation_rate: number; expected_return: number;
  linked_bucket_name: string | null; priority: number; status: string; notes: string | null;
  projection: { inflation_adjusted_target: number; progress_pct: number; months_remaining: number | null; required_monthly: number | null; eta_months: number | null; feasible: boolean; shortfall_per_month: number };
}

const GOAL_ICONS = ["🏠", "✈️", "🛡️", "📱", "🎓", "🚗", "💍", "🏖️", "💻", "🎯"];

export default function GoalsPage() {
  const qc = useQueryClient();
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Goal | null>(null);
  const [form, setForm] = useState({ name: "", target_amount: 0, target_date: "", inflation_rate: 5, expected_return: 6, linked_bucket_id: "", priority: 50, notes: "", status: "active" });
  const [contributeGoal, setContributeGoal] = useState<Goal | null>(null);
  const [contributeAmount, setContributeAmount] = useState(0);
  const [err, setErr] = useState("");

  const { data } = useQuery<{ goals: Goal[] }>({ queryKey: ["goals"], queryFn: () => api.get("/goals") });
  const { data: bucketsData } = useQuery<{ buckets: any[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });
  const inv = () => qc.invalidateQueries({ queryKey: ["goals"] });

  const saveMut = useMutation({
    mutationFn: () => {
      const p = { ...form, target_date: form.target_date || null, inflation_rate: form.inflation_rate / 100, expected_return: form.expected_return / 100, linked_bucket_id: form.linked_bucket_id || null };
      return editing ? api.put(`/goals/${editing.goal_id}`, p) : api.post("/goals", p);
    },
    onSuccess: () => { inv(); setModal(null); }, onError: (e: Error) => setErr(e.message),
  });
  const deleteMut = useMutation({ mutationFn: (id: string) => api.del(`/goals/${id}`), onSuccess: inv });
  const contributeMut = useMutation({ mutationFn: () => api.post(`/goals/${contributeGoal!.goal_id}/contribute`, { amount: contributeAmount }), onSuccess: () => { inv(); setContributeGoal(null); }, onError: (e: Error) => setErr(e.message) });

  function openCreate() { setEditing(null); setForm({ name: "", target_amount: 0, target_date: "", inflation_rate: 5, expected_return: 6, linked_bucket_id: "", priority: 50, notes: "", status: "active" }); setErr(""); setModal("create"); }
  function openEdit(g: Goal) { setEditing(g); setForm({ name: g.name, target_amount: g.target_amount, target_date: g.target_date?.slice(0, 10) ?? "", inflation_rate: Math.round(g.inflation_rate * 100), expected_return: Math.round(g.expected_return * 100), linked_bucket_id: "", priority: g.priority, notes: g.notes ?? "", status: g.status }); setErr(""); setModal("edit"); }

  const goals = data?.goals ?? [];
  const buckets = bucketsData?.buckets ?? [];
  const onTrack = goals.filter((g) => g.projection.feasible).length;
  const needsAttention = goals.filter((g) => !g.projection.feasible).length;
  const totalNeeded = goals.reduce((s, g) => s + (g.projection.required_monthly ?? 0), 0);
  const overallPct = goals.length > 0 ? Math.round((onTrack / goals.length) * 100) : 0;

  return (
    <div className="p-5 space-y-4">
      <div className="grid grid-cols-3 gap-4">
        {/* Goals at a glance */}
        <div className="col-span-2">
          <Card padding="sm">
            <SectionTitle>Your Goals at a Glance</SectionTitle>
            <div className="grid grid-cols-4 gap-4">
              {[
                { icon: "🏠", value: goals.length, label: "Total Goals" },
                { icon: "✓", value: `${onTrack}`, sub: `${overallPct}%`, label: "On Track", color: "text-primary" },
                { icon: "⏱️", value: `${needsAttention}`, sub: `${100 - overallPct}%`, label: "Needs Attention", color: "text-warning" },
                { icon: "💰", value: bal(totalNeeded), label: "Total Needed / month", color: "text-info" },
              ].map((s) => (
                <div key={s.label} className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-xl bg-[var(--bg)] flex items-center justify-center text-xl">{s.icon}</div>
                  <div>
                    <p className={`text-xl font-bold ${s.color ?? ""}`}>{s.value}</p>
                    {s.sub && <p className="text-xs text-[var(--muted)]">{s.sub}</p>}
                    <p className="text-xs text-[var(--muted)]">{s.label}</p>
                  </div>
                </div>
              ))}
            </div>
          </Card>
        </div>

        {/* Overall Goal Health */}
        <Card padding="md">
          <SectionTitle>Overall Goal Health</SectionTitle>
          <div className="flex flex-col items-center">
            <DonutChart value={overallPct} size={100} label={`${overallPct}%`} sublabel="On Track" color={overallPct >= 70 ? "#16a34a" : "#f59e0b"} />
            <div className="mt-3 space-y-1.5 w-full text-xs">
              <div className="flex justify-between"><span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-primary inline-block" /> On Track</span><span>{onTrack} goals · {overallPct}%</span></div>
              <div className="flex justify-between"><span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-warning inline-block" /> Needs Attention</span><span>{needsAttention} goal · {100 - overallPct}%</span></div>
              <div className="flex justify-between"><span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-gray-300 inline-block" /> Not Started</span><span>0 goals · 0%</span></div>
            </div>
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* Goals list */}
        <div className="col-span-2 space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold">Your Goals</h2>
            <div className="flex items-center gap-2 text-xs text-[var(--muted)]">
              Sort by <select className="border border-[var(--border)] rounded px-2 py-1 bg-[var(--surface)] text-xs"><option>Priority</option></select>
            </div>
          </div>

          {goals.map((g, i) => {
            const p = g.projection;
            const icon = GOAL_ICONS[i % GOAL_ICONS.length];
            const isShortfall = !p.feasible;
            return (
              <Card key={g.goal_id} padding="md">
                <div className="flex items-start gap-4">
                  <div className="w-14 h-14 rounded-xl bg-[var(--bg)] flex items-center justify-center text-2xl shrink-0">{icon}</div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2">
                        {i === 0 && <Badge variant="green">Top Priority</Badge>}
                        <h3 className="font-bold text-sm">{g.name}</h3>
                      </div>
                      <div className="flex items-center gap-2">
                        <Badge variant={isShortfall ? "orange" : "green"} dot>{isShortfall ? "Shortfall" : "On Track"}</Badge>
                        <button className="text-[var(--muted)]">⋯</button>
                      </div>
                    </div>
                    {g.linked_bucket_name && <p className="text-xs text-[var(--muted)] mb-2">Bucket: <span className="text-primary">{g.linked_bucket_name}</span></p>}
                    <ProgressBar value={p.progress_pct} color={isShortfall ? "orange" : "green"} size="md" className="mb-1" />
                    <div className="flex justify-between text-xs text-[var(--muted)] mb-2">
                      <span>{p.progress_pct}%</span>
                      <span>{bal(p.inflation_adjusted_target - g.current_amount)} to go</span>
                    </div>
                  </div>
                  <div className="shrink-0 text-right space-y-1">
                    <div>
                      <p className="text-xs text-[var(--muted)]">Monthly Contribution</p>
                      <p className="font-bold text-sm tabular">{p.required_monthly ? bal(p.required_monthly) : "—"}</p>
                    </div>
                    {isShortfall && (
                      <div>
                        <p className="text-xs text-[var(--muted)]">Needed / month</p>
                        <p className="font-bold text-sm tabular text-danger">{bal(p.required_monthly ?? 0)}</p>
                      </div>
                    )}
                    <div>
                      <p className="text-xs text-[var(--muted)]">ETA</p>
                      <p className="font-bold text-sm">{p.eta_months === 0 ? "Done!" : p.eta_months ? `${p.eta_months} months` : "—"}</p>
                    </div>
                    <Button size="sm" variant="secondary" onClick={() => { setContributeAmount(p.required_monthly ?? 0); setContributeGoal(g); }}>+ Contribute</Button>
                  </div>
                </div>
              </Card>
            );
          })}

          <button onClick={openCreate} className="w-full py-3 border-2 border-dashed border-[var(--border)] rounded-xl text-sm text-[var(--muted)] hover:border-primary hover:text-primary transition-colors">
            + Add New Goal
          </button>
        </div>

        {/* Upcoming Deadlines */}
        <div className="space-y-4">
          <Card padding="md">
            <SectionTitle>Upcoming Deadlines</SectionTitle>
            <div className="space-y-3">
              {goals.filter((g) => g.target_date).slice(0, 4).map((g, i) => (
                <div key={g.goal_id} className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-[var(--bg)] flex items-center justify-center text-sm">{GOAL_ICONS[i % GOAL_ICONS.length]}</div>
                  <div className="flex-1 min-w-0">
                    <p className="text-xs font-medium truncate">{g.name}</p>
                    <p className="text-xs text-[var(--muted)]">ETA in {g.projection.eta_months ?? "?"} months</p>
                  </div>
                  {g.target_date && (
                    <Badge variant="blue">{new Date(g.target_date).toLocaleDateString("en-US", { month: "short", year: "numeric" })}</Badge>
                  )}
                </div>
              ))}
              {goals.filter((g) => g.target_date).length === 0 && <p className="text-xs text-[var(--muted)] text-center py-2">No deadlines set</p>}
            </div>
          </Card>

          <Card padding="md">
            <div className="flex flex-col items-center text-center py-2">
              <span className="text-3xl mb-2">🏆</span>
              <p className="font-bold text-sm text-primary">You're building a better future!</p>
              <p className="text-xs text-[var(--muted)] mt-1">Stay consistent and keep your goals on track.</p>
            </div>
          </Card>
        </div>
      </div>

      {/* Modals */}
      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Goal" : "New Goal"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-3">
          <Input label="Goal Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required placeholder="e.g. Emergency Fund, House Down Payment" />
          <MoneyInput label="Target Amount" value={form.target_amount} onChange={(v) => setForm({ ...form, target_amount: v })} required />
          <Input label="Target Date (optional)" type="date" value={form.target_date} onChange={(e) => setForm({ ...form, target_date: e.target.value })} />
          <div className="grid grid-cols-2 gap-3">
            <Input label="Inflation Rate (%/year)" type="number" min={0} max={50} step={0.1} value={String(form.inflation_rate)} onChange={(e) => setForm({ ...form, inflation_rate: parseFloat(e.target.value) || 0 })} />
            <Input label="Expected Return (%/year)" type="number" min={0} max={50} step={0.1} value={String(form.expected_return)} onChange={(e) => setForm({ ...form, expected_return: parseFloat(e.target.value) || 0 })} />
          </div>
          <Select label="Linked Bucket (optional)" value={form.linked_bucket_id} onChange={(e) => setForm({ ...form, linked_bucket_id: e.target.value })}>
            <option value="">— none —</option>
            {buckets.map((b: any) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          {editing && <Select label="Status" value={form.status} onChange={(e) => setForm({ ...form, status: e.target.value })}>
            <option value="active">Active</option><option value="paused">Paused</option><option value="completed">Completed</option>
          </Select>}
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>

      <Modal open={!!contributeGoal} onClose={() => setContributeGoal(null)} title={`Contribute to: ${contributeGoal?.name}`}>
        <div className="space-y-3">
          <p className="text-sm text-[var(--muted)]">Current: {bal(contributeGoal?.current_amount ?? 0)} · Target: {bal(contributeGoal?.projection.inflation_adjusted_target ?? 0)}</p>
          <MoneyInput label="Amount" value={contributeAmount} onChange={setContributeAmount} />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button variant="primary" className="flex-1" disabled={contributeMut.isPending} onClick={() => contributeMut.mutate()}>Confirm</Button>
            <Button variant="secondary" onClick={() => setContributeGoal(null)}>Cancel</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
