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

interface Bucket {
  bucket_id: string;
  name: string;
  kind: "spending" | "sinking" | "emergency" | "goal" | "investment";
  target_amount: number | null;
  linked_account_id: string | null;
  priority: number;
  is_archived: boolean;
  notes: string | null;
}

const KIND_LABELS: Record<string, string> = {
  spending: "💳 Spending",
  sinking: "🪣 Sinking Fund",
  emergency: "🛡️ Emergency",
  goal: "🎯 Goal",
  investment: "📈 Investment",
};

const EMPTY = { name: "", kind: "spending" as Bucket["kind"], target_amount: 0, linked_account_id: "", priority: 50, notes: "" };

export default function BucketsPage() {
  const qc = useQueryClient();
  const { accounts } = useAppCtx();
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Bucket | null>(null);
  const [form, setForm] = useState(EMPTY);
  const [err, setErr] = useState("");

  const { data } = useQuery<{ buckets: Bucket[] }>({
    queryKey: ["buckets"],
    queryFn: () => api.get("/buckets"),
  });

  const invalidate = () => qc.invalidateQueries({ queryKey: ["buckets"] });

  const saveMut = useMutation({
    mutationFn: (f: typeof EMPTY) =>
      editing
        ? api.put(`/buckets/${editing.bucket_id}`, { ...f, target_amount: f.target_amount || null, linked_account_id: f.linked_account_id || null })
        : api.post("/buckets", { ...f, target_amount: f.target_amount || null, linked_account_id: f.linked_account_id || null }),
    onSuccess: () => { invalidate(); setModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => api.del(`/buckets/${id}`),
    onSuccess: invalidate,
  });

  function openCreate() { setEditing(null); setForm(EMPTY); setErr(""); setModal("create"); }
  function openEdit(b: Bucket) {
    setEditing(b);
    setForm({ name: b.name, kind: b.kind, target_amount: b.target_amount ?? 0, linked_account_id: b.linked_account_id ?? "", priority: b.priority, notes: b.notes ?? "" });
    setErr(""); setModal("edit");
  }

  const buckets = data?.buckets ?? [];

  return (
    <div className="p-4 max-w-2xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Buckets</h1>
          <p className="text-xs text-[var(--muted)]">Named pockets of money earmarked for a purpose.</p>
        </div>
        <Button variant="primary" size="sm" onClick={openCreate}>+ New Bucket</Button>
      </div>

      <div className="space-y-2">
        {buckets.length === 0 && <p className="text-[var(--muted)] text-sm text-center py-8">No buckets yet.</p>}
        {buckets.map((b) => (
          <div key={b.bucket_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3 flex items-center justify-between gap-3">
            <div>
              <div className="font-medium">{b.name}</div>
              <div className="text-xs text-[var(--muted)] mt-0.5 flex gap-2">
                <span>{KIND_LABELS[b.kind] ?? b.kind}</span>
                {b.target_amount != null && <span>target: {fmtMoney(b.target_amount)}</span>}
                <span>priority: {b.priority}</span>
              </div>
            </div>
            <div className="flex gap-2 shrink-0">
              <Button size="sm" variant="ghost" onClick={() => openEdit(b)}>Edit</Button>
              <Button size="sm" variant="danger" onClick={() => confirm(`Archive "${b.name}"?`) && deleteMut.mutate(b.bucket_id)}>Archive</Button>
            </div>
          </div>
        ))}
      </div>

      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Bucket" : "New Bucket"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(form); }} className="space-y-4">
          <Input label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required placeholder="e.g. Emergency Fund" />
          <Select label="Kind" value={form.kind} onChange={(e) => setForm({ ...form, kind: e.target.value as Bucket["kind"] })}>
            {Object.entries(KIND_LABELS).map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </Select>
          <MoneyInput label="Target Amount (optional)" value={form.target_amount} onChange={(v) => setForm({ ...form, target_amount: v })} />
          <Select label="Linked Account (optional)" value={form.linked_account_id} onChange={(e) => setForm({ ...form, linked_account_id: e.target.value })}>
            <option value="">— none —</option>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          <Input label="Priority (lower = higher priority)" type="number" min={1} max={999} value={String(form.priority)} onChange={(e) => setForm({ ...form, priority: parseInt(e.target.value) || 50 })} />
          <Input label="Notes (optional)" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>{saveMut.isPending ? "Saving…" : "Save"}</Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
