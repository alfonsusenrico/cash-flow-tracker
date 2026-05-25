"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface Bucket {
  bucket_id: string; name: string; kind: string;
  target_amount: number | null; linked_account_id: string | null;
  priority: number; is_archived: boolean; notes: string | null;
}

const KIND_ICONS: Record<string, string> = { spending: "💳", sinking: "🪣", emergency: "🛡️", goal: "🎯", investment: "📈" };
const KIND_COLORS: Record<string, string> = { spending: "#3b82f6", sinking: "#8b5cf6", emergency: "#f59e0b", goal: "#16a34a", investment: "#06b6d4" };
const PRIORITY_LABELS: Record<number, { label: string; color: "red" | "yellow" | "gray" }> = {
  10: { label: "High", color: "red" }, 50: { label: "Medium", color: "yellow" }, 90: { label: "Low", color: "gray" },
};

function getPriorityBadge(priority: number) {
  if (priority <= 20) return { label: "High", color: "red" as const };
  if (priority <= 60) return { label: "Medium", color: "yellow" as const };
  return { label: "Low", color: "gray" as const };
}

const EMPTY = { name: "", kind: "spending", target_amount: 0, linked_account_id: "", priority: 50, notes: "" };

export default function BucketsPage() {
  const qc = useQueryClient();
  const { accounts, hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Bucket | null>(null);
  const [form, setForm] = useState(EMPTY);
  const [err, setErr] = useState("");
  const [search, setSearch] = useState("");

  const { data } = useQuery<{ buckets: Bucket[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });
  const inv = () => qc.invalidateQueries({ queryKey: ["buckets"] });

  const saveMut = useMutation({
    mutationFn: () => {
      const payload = { ...form, target_amount: form.target_amount || null, linked_account_id: form.linked_account_id || null };
      return editing ? api.put(`/buckets/${editing.bucket_id}`, payload) : api.post("/buckets", payload);
    },
    onSuccess: () => { inv(); setModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({ mutationFn: (id: string) => api.del(`/buckets/${id}`), onSuccess: inv });

  const buckets = (data?.buckets ?? []).filter((b) => !b.is_archived && (!search || b.name.toLowerCase().includes(search.toLowerCase())));
  const totalEarmarked = buckets.reduce((s, b) => s + (b.target_amount ?? 0), 0);
  const onTrack = buckets.filter((b) => b.target_amount).length;

  function openCreate() { setEditing(null); setForm(EMPTY); setErr(""); setModal("create"); }
  function openEdit(b: Bucket) {
    setEditing(b);
    setForm({ name: b.name, kind: b.kind, target_amount: b.target_amount ?? 0, linked_account_id: b.linked_account_id ?? "", priority: b.priority, notes: b.notes ?? "" });
    setErr(""); setModal("edit");
  }

  return (
    <div className="p-5 space-y-4">
      {/* Summary stats */}
      <Card padding="sm">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs text-[var(--muted)]">Total Earmarked</p>
            <p className="text-2xl font-bold text-primary tabular">{bal(totalEarmarked)}</p>
            <p className="text-xs text-[var(--muted)]">Across {buckets.length} active buckets</p>
          </div>
          <div className="flex gap-6 text-sm">
            <div className="text-center"><p className="text-xs text-[var(--muted)]">Total Target</p><p className="font-bold tabular">{bal(totalEarmarked)}</p></div>
            <div className="text-center"><p className="text-xs text-[var(--muted)]">On Track</p><p className="font-bold text-primary">● {onTrack} buckets</p></div>
            <div className="text-center"><p className="text-xs text-[var(--muted)]">At Risk</p><p className="font-bold text-warning">● 0 buckets</p></div>
            <div className="text-center"><p className="text-xs text-[var(--muted)]">Overdue</p><p className="font-bold text-danger">● 0 buckets</p></div>
          </div>
          <Button variant="primary" onClick={openCreate}>+ New Bucket</Button>
        </div>
      </Card>

      {/* Filters */}
      <div className="flex items-center gap-2">
        <div className="relative flex-1 max-w-xs">
          <input placeholder="Search buckets..." value={search} onChange={(e) => setSearch(e.target.value)}
            className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-xs bg-[var(--surface)] pl-8" />
          <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[var(--muted)] text-xs">🔍</span>
        </div>
        <select className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]"><option>Status: Active</option></select>
        <select className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]"><option>Priority: All</option></select>
        <select className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]"><option>Sort by: Priority</option></select>
        <div className="ml-auto flex gap-1">
          <button className="p-1.5 rounded border border-[var(--border)] hover:bg-[var(--bg)] text-sm">⊞</button>
          <button className="p-1.5 rounded border border-[var(--border)] hover:bg-[var(--bg)] text-sm">☰</button>
        </div>
      </div>

      {/* Bucket cards grid */}
      <div className="grid grid-cols-3 gap-4">
        {buckets.length === 0 && <p className="col-span-3 text-center text-[var(--muted)] py-8">No buckets yet. Create one to get started.</p>}
        {buckets.map((b) => {
          const pBadge = getPriorityBadge(b.priority);
          const icon = KIND_ICONS[b.kind] ?? "🪣";
          const color = KIND_COLORS[b.kind] ?? "#6b7280";
          const linkedAcc = accounts.find((a) => a.account_id === b.linked_account_id);
          return (
            <Card key={b.bucket_id} padding="md">
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center gap-3">
                  <div className="w-12 h-12 rounded-xl flex items-center justify-center text-2xl" style={{ background: color + "20" }}>{icon}</div>
                  <div>
                    <p className="font-bold text-sm">{b.name}</p>
                    <p className="text-xs text-[var(--muted)]">{b.notes ?? `${b.kind} bucket`}</p>
                  </div>
                </div>
                <button onClick={() => openEdit(b)} className="text-[var(--muted)] hover:text-[var(--text)]">⋯</button>
              </div>
              <div className="flex items-center gap-2 mb-3">
                <Badge variant={pBadge.color}>{pBadge.label}</Badge>
                <Badge variant="green">On Track</Badge>
              </div>
              <div className="grid grid-cols-2 gap-3 text-xs mb-3">
                <div><p className="text-[var(--muted)]">Current Amount</p><p className="font-bold tabular text-sm">—</p></div>
                <div className="text-right"><p className="text-[var(--muted)]">Target Amount</p><p className="font-bold tabular text-sm">{b.target_amount ? bal(b.target_amount) : "—"}</p></div>
              </div>
              {b.target_amount && <ProgressBar value={0} size="md" className="mb-3" />}
              <div className="grid grid-cols-2 gap-3 text-xs border-t border-[var(--border)] pt-3">
                <div>
                  <p className="text-[var(--muted)]">Linked Account</p>
                  <p className="font-medium">{linkedAcc ? `${linkedAcc.account_name.slice(0, 8)}… ••••` : "—"}</p>
                </div>
                <div className="text-right">
                  <p className="text-[var(--muted)]">Target Date</p>
                  <p className="font-medium">—</p>
                </div>
              </div>
            </Card>
          );
        })}
      </div>

      {/* Motivational banner */}
      {buckets.length > 0 && (
        <Card padding="md">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span className="text-2xl">🎉</span>
              <div>
                <p className="font-bold text-primary">You're building a better future!</p>
                <p className="text-xs text-[var(--muted)]">Consistency today creates freedom tomorrow. Keep going!</p>
              </div>
            </div>
            <Button size="sm" variant="secondary">View Goals →</Button>
          </div>
        </Card>
      )}

      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Bucket" : "New Bucket"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-3">
          <Input label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <Select label="Kind" value={form.kind} onChange={(e) => setForm({ ...form, kind: e.target.value })}>
            {Object.entries(KIND_ICONS).map(([k, v]) => <option key={k} value={k}>{v} {k}</option>)}
          </Select>
          <MoneyInput label="Target Amount (optional)" value={form.target_amount} onChange={(v) => setForm({ ...form, target_amount: v })} />
          <Select label="Linked Account (optional)" value={form.linked_account_id} onChange={(e) => setForm({ ...form, linked_account_id: e.target.value })}>
            <option value="">— none —</option>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          <Input label="Priority (lower = higher)" type="number" min={1} max={999} value={String(form.priority)} onChange={(e) => setForm({ ...form, priority: parseInt(e.target.value) || 50 })} />
          <Input label="Notes (optional)" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
