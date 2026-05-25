"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface Rule {
  rule_id: string;
  name: string;
  trigger: string;
  mode: string;
  target_bucket_id: string | null;
  target_bucket_name: string | null;
  value: number;
  cap: number | null;
  floor: number | null;
  priority: number;
  is_active: boolean;
  notes: string | null;
}

interface Bucket { bucket_id: string; name: string; }

interface Preview {
  income: number;
  total_allocated: number;
  remaining: number;
  allocations: { rule_id: string; rule_name: string; target_bucket_name: string | null; mode: string; amount: number }[];
}

const EMPTY = { name: "", trigger: "manual", mode: "percent", target_bucket_id: "", value: 0, cap: 0, floor: 0, priority: 50, is_active: true, notes: "" };

export default function StrategyPage() {
  const qc = useQueryClient();
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Rule | null>(null);
  const [form, setForm] = useState(EMPTY);
  const [previewIncome, setPreviewIncome] = useState(0);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [err, setErr] = useState("");

  const { data: rulesData } = useQuery<{ rules: Rule[] }>({ queryKey: ["strategy-rules"], queryFn: () => api.get("/strategy-rules") });
  const { data: bucketsData } = useQuery<{ buckets: Bucket[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });

  const inv = () => qc.invalidateQueries({ queryKey: ["strategy-rules"] });

  const saveMut = useMutation({
    mutationFn: () => {
      const payload = { ...form, target_bucket_id: form.target_bucket_id || null, cap: form.cap || null, floor: form.floor || null };
      return editing ? api.put(`/strategy-rules/${editing.rule_id}`, payload) : api.post("/strategy-rules", payload);
    },
    onSuccess: () => { inv(); setModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => api.del(`/strategy-rules/${id}`),
    onSuccess: inv,
  });

  const previewMut = useMutation({
    mutationFn: () => api.post<Preview>("/strategy-rules/preview", { income: previewIncome }),
    onSuccess: (r) => setPreview(r),
    onError: (e: Error) => setErr(e.message),
  });

  function openCreate() { setEditing(null); setForm(EMPTY); setErr(""); setModal("create"); }
  function openEdit(r: Rule) {
    setEditing(r);
    setForm({ name: r.name, trigger: r.trigger, mode: r.mode, target_bucket_id: r.target_bucket_id ?? "", value: r.value, cap: r.cap ?? 0, floor: r.floor ?? 0, priority: r.priority, is_active: r.is_active, notes: r.notes ?? "" });
    setErr(""); setModal("edit");
  }

  const rules = rulesData?.rules ?? [];
  const buckets = bucketsData?.buckets ?? [];

  return (
    <div className="p-4 max-w-2xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Strategy Rules</h1>
          <p className="text-xs text-[var(--muted)]">Define how income is distributed when it arrives.</p>
        </div>
        <Button variant="primary" size="sm" onClick={openCreate}>+ New Rule</Button>
      </div>

      {/* Preview */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-xl p-4 space-y-3">
        <h2 className="text-sm font-semibold">Preview Allocation</h2>
        <div className="flex gap-2 items-end">
          <MoneyInput label="Income amount" value={previewIncome} onChange={setPreviewIncome} />
          <Button variant="secondary" size="sm" onClick={() => previewMut.mutate()} disabled={previewMut.isPending || previewIncome <= 0}>Preview</Button>
        </div>
        {preview && (
          <div className="space-y-1.5 text-sm">
            {preview.allocations.map((a) => (
              <div key={a.rule_id} className="flex justify-between">
                <span className="text-[var(--muted)]">{a.rule_name} → {a.target_bucket_name ?? "—"}</span>
                <span className="font-medium">{fmtMoney(a.amount)}</span>
              </div>
            ))}
            <div className="border-t border-[var(--border)] pt-1 flex justify-between font-semibold">
              <span>Remaining unallocated</span>
              <span className={preview.remaining > 0 ? "text-yellow-500" : "text-green-600"}>{fmtMoney(preview.remaining)}</span>
            </div>
          </div>
        )}
      </div>

      {/* Rules list */}
      <div className="space-y-2">
        {rules.length === 0 && <p className="text-[var(--muted)] text-sm text-center py-6">No rules yet.</p>}
        {rules.map((r) => (
          <div key={r.rule_id} className={`bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3 flex items-center justify-between gap-3 ${!r.is_active ? "opacity-50" : ""}`}>
            <div>
              <div className="font-medium text-sm">{r.name}</div>
              <div className="text-xs text-[var(--muted)] mt-0.5">
                {r.mode === "percent" ? `${r.value}%` : r.mode === "fixed" ? fmtMoney(r.value) : r.mode} → {r.target_bucket_name ?? "—"} · priority {r.priority} · {r.trigger}
              </div>
            </div>
            <div className="flex gap-2 shrink-0">
              <Button size="sm" variant="ghost" onClick={() => openEdit(r)}>Edit</Button>
              <Button size="sm" variant="danger" onClick={() => confirm(`Delete "${r.name}"?`) && deleteMut.mutate(r.rule_id)}>Delete</Button>
            </div>
          </div>
        ))}
      </div>

      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Rule" : "New Rule"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-4">
          <Input label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <Select label="Trigger" value={form.trigger} onChange={(e) => setForm({ ...form, trigger: e.target.value })}>
            <option value="manual">Manual</option>
            <option value="income_arrival">On Income Arrival</option>
          </Select>
          <Select label="Mode" value={form.mode} onChange={(e) => setForm({ ...form, mode: e.target.value })}>
            <option value="percent">Percent (%)</option>
            <option value="fixed">Fixed Amount</option>
            <option value="target_balance">Target Balance</option>
            <option value="overflow">Overflow (remainder)</option>
          </Select>
          {form.mode !== "overflow" && (
            form.mode === "percent"
              ? <Input label="Percentage" type="number" min={0} max={100} step={0.1} value={String(form.value)} onChange={(e) => setForm({ ...form, value: parseFloat(e.target.value) || 0 })} />
              : <MoneyInput label="Amount" value={form.value} onChange={(v) => setForm({ ...form, value: v })} />
          )}
          <Select label="Target Bucket" value={form.target_bucket_id} onChange={(e) => setForm({ ...form, target_bucket_id: e.target.value })}>
            <option value="">— none —</option>
            {buckets.map((b) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          <MoneyInput label="Cap (max per run, 0 = no cap)" value={form.cap} onChange={(v) => setForm({ ...form, cap: v })} />
          <Input label="Priority (lower = runs first)" type="number" min={1} value={String(form.priority)} onChange={(e) => setForm({ ...form, priority: parseInt(e.target.value) || 50 })} />
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={form.is_active} onChange={(e) => setForm({ ...form, is_active: e.target.checked })} className="rounded" />
            Active
          </label>
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
