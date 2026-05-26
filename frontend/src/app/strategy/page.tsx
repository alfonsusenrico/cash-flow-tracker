"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { clampNumber, currentMonthYM, fmtMoney, parseClampedNumber } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";

interface Rule { rule_id: string; name: string; trigger: string; mode: string; target_bucket_id: string | null; target_bucket_name: string | null; target_bucket_kind: string | null; value: number; cap: number | null; floor: number | null; priority: number; is_active: boolean; notes: string | null; }
interface PreviewAllocation {
  rule_id: string;
  rule_name: string;
  target_bucket_id: string | null;
  target_bucket_name: string | null;
  target_bucket_kind: string | null;
  group: string;
  mode: string;
  amount: number;
  base_amount: number;
  remaining_after: number;
  skipped: boolean;
  reason: string;
  bucket_current_amount: number | null;
  bucket_target_amount: number | null;
}
interface PreviewSummary { group: string; label: string; amount: number; percent: number; }
interface Preview { income: number; total_allocated: number; remaining: number; allocations: PreviewAllocation[]; summary: PreviewSummary[]; }
interface ApplyResponse { plan_id: string; month: string; expected_income: number; items: PreviewAllocation[]; remaining: number; total_allocated: number; }

const MODE_COLORS: Record<string, "blue" | "green" | "purple" | "gray"> = { percent: "blue", fixed: "green", target_balance: "purple", overflow: "gray" };
const MODE_LABELS: Record<string, string> = { percent: "Percentage", fixed: "Fixed Amount", target_balance: "Target Balance", overflow: "Remainder" };

const EMPTY = { name: "", trigger: "manual", mode: "percent", target_bucket_id: "", value: 0, cap: 0, floor: 0, priority: 50, is_active: true, notes: "" };

export default function StrategyPage() {
  const qc = useQueryClient();
  const router = useRouter();
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Rule | null>(null);
  const [form, setForm] = useState(EMPTY);
  const [previewIncome, setPreviewIncome] = useState(10_000_000);
  const [planMonth, setPlanMonth] = useState(currentMonthYM());
  const [preview, setPreview] = useState<Preview | null>(null);
  const [err, setErr] = useState("");

  const { data: rulesData } = useQuery<{ rules: Rule[] }>({ queryKey: ["strategy-rules"], queryFn: () => api.get("/strategy-rules") });
  const { data: bucketsData } = useQuery<{ buckets: any[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });

  const inv = () => qc.invalidateQueries({ queryKey: ["strategy-rules"] });
  const saveMut = useMutation({
    mutationFn: () => {
      const p = {
        ...form,
        value: form.mode === "percent" ? clampNumber(form.value) : form.value,
        target_bucket_id: form.target_bucket_id || null,
        cap: form.cap || null,
        floor: form.floor || null,
      };
      return editing ? api.put(`/strategy-rules/${editing.rule_id}`, p) : api.post("/strategy-rules", p);
    },
    onSuccess: () => { inv(); setModal(null); }, onError: (e: Error) => setErr(e.message),
  });
  const deleteMut = useMutation({ mutationFn: (id: string) => api.del(`/strategy-rules/${id}`), onSuccess: inv });
  const previewMut = useMutation({ mutationFn: () => api.post<Preview>("/strategy-rules/preview", { income: previewIncome }), onSuccess: (r) => setPreview(r), onError: (e: Error) => setErr(e.message) });
  const applyMut = useMutation({
    mutationFn: () => api.post<ApplyResponse>("/strategy-rules/apply", { month: planMonth, expected_income: previewIncome }),
    onSuccess: (r) => router.push(`/allocation?plan=${r.plan_id}`),
    onError: (e: Error) => setErr(e.message),
  });

  function openCreate() { setEditing(null); setForm(EMPTY); setErr(""); setModal("create"); }
  function openEdit(r: Rule) {
    setEditing(r);
    setForm({
      name: r.name,
      trigger: r.trigger,
      mode: r.mode,
      target_bucket_id: r.target_bucket_id ?? "",
      value: r.mode === "percent" ? clampNumber(r.value) : r.value,
      cap: r.cap ?? 0,
      floor: r.floor ?? 0,
      priority: r.priority,
      is_active: r.is_active,
      notes: r.notes ?? "",
    });
    setErr("");
    setModal("edit");
  }

  const rules = rulesData?.rules ?? [];
  const buckets = bucketsData?.buckets ?? [];
  const maxAlloc = Math.max(...(preview?.allocations.map((a) => a.amount) ?? [1]), 1);
  const rulePercentValue = form.mode === "percent" ? clampNumber(form.value) : form.value;
  const rulePercentAmount = Math.round(previewIncome * (rulePercentValue / 100));
  const ruleFixedPercent = previewIncome > 0 ? clampNumber(Math.round((form.value / previewIncome) * 100)) : 0;

  return (
    <div className="p-5">
      <p className="text-xs text-[var(--muted)] mb-4">Set reusable rules, preview them against income, then create an allocation plan from the result.</p>
      <div className="grid grid-cols-2 gap-5">
        {/* Left: Rules table */}
        <Card padding="sm">
          <div className="flex items-center justify-between mb-3">
            <div>
              <SectionTitle>Strategy Rules</SectionTitle>
              <p className="text-xs text-[var(--muted)] -mt-2">Rules run by type first: fixed, target balance, percentage, then overflow. Priority orders rules inside each type.</p>
            </div>
            <div className="flex gap-2">
              <Button size="sm" variant="primary" onClick={openCreate}>+ Add Rule</Button>
              <Button size="sm" variant="secondary" disabled title="Use each rule's priority field to change order. Drag reorder is coming soon.">⇄ Reorder</Button>
            </div>
          </div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                <th className="text-left pb-2 font-medium w-6"></th>
                <th className="text-left pb-2 font-medium w-8">Priority</th>
                <th className="text-left pb-2 font-medium">Rule & Bucket</th>
                <th className="text-left pb-2 font-medium">Type</th>
                <th className="text-right pb-2 font-medium">Target</th>
                <th className="text-right pb-2 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {rules.length === 0 && <tr><td colSpan={6} className="text-center py-6 text-[var(--muted)]">No rules yet. Add your first rule.</td></tr>}
              {rules.map((r, idx) => (
                <tr key={r.rule_id} className={`hover:bg-[var(--bg)] transition-colors ${!r.is_active ? "opacity-50" : ""}`}>
                  <td className="py-2.5 text-[var(--muted)] cursor-grab">⋮⋮</td>
                  <td className="py-2.5">
                    <span className="w-6 h-6 rounded-full bg-[var(--bg)] flex items-center justify-center font-bold text-[var(--text)]">{idx + 1}</span>
                  </td>
                  <td className="py-2.5">
                    <div className="flex items-center gap-2">
                      <div className="w-7 h-7 rounded-lg bg-[var(--bg)] flex items-center justify-center text-sm">🪣</div>
                      <div>
                        <p className="font-medium">{r.name}</p>
                        <p className="text-[var(--muted)]">{r.target_bucket_name ?? "—"}{r.target_bucket_kind ? ` · ${r.target_bucket_kind}` : ""}</p>
                      </div>
                    </div>
                  </td>
                  <td className="py-2.5"><Badge variant={MODE_COLORS[r.mode] ?? "gray"}>{MODE_LABELS[r.mode] ?? r.mode}</Badge></td>
                  <td className="py-2.5 text-right tabular">
                    {r.mode === "percent" ? `${r.value}%` : r.mode === "fixed" ? bal(r.value) : r.mode === "overflow" ? "Remainder" : bal(r.value)}
                    <p className="text-[var(--muted)]">{r.mode === "percent" ? "of income" : r.mode === "fixed" ? "per cycle" : ""}</p>
                  </td>
                  <td className="py-2.5 text-right">
                    <div className="flex gap-1 justify-end">
                      <button onClick={() => openEdit(r)} className="p-1 rounded hover:bg-[var(--bg)] text-[var(--muted)]">✏️</button>
                      <button onClick={() => confirm(`Delete "${r.name}"?`) && deleteMut.mutate(r.rule_id)} className="p-1 rounded hover:bg-[var(--bg)] text-danger">🗑️</button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="mt-3 p-3 rounded-lg bg-[var(--bg)] text-xs text-[var(--muted)] flex items-start gap-2">
            <span>ⓘ</span>
            <div>
              <p className="font-medium text-[var(--text)]">Rules are applied in order from top to bottom.</p>
              <p>Fixed rules reserve exact amounts. Target balance rules fill bucket shortfalls. Percentage rules use total income. Overflow gets what remains.</p>
            </div>
          </div>
        </Card>

        {/* Right: Preview */}
        <Card padding="md">
          <div className="flex items-center justify-between mb-4">
            <SectionTitle>Preview Distribution</SectionTitle>
            <button onClick={() => setPreview(null)} className="text-xs text-[var(--muted)] border border-[var(--border)] px-2 py-1 rounded-lg hover:bg-[var(--bg)]">Reset</button>
          </div>
          <p className="text-xs text-[var(--muted)] mb-2">See how income will be distributed, then turn it into this cycle's allocation plan.</p>
          <Input
            label="Pay Cycle Ending Month"
            type="month"
            value={planMonth}
            onChange={(e) => setPlanMonth(e.target.value)}
            className="mb-3"
          />
          <p className="text-xs font-medium mb-1">Enter Income Amount</p>
          <div className="flex items-center gap-2 mb-4">
            <div className="flex-1 border border-[var(--border)] rounded-lg px-3 py-2 flex items-center gap-2">
              <span className="text-sm text-[var(--muted)]">Rp</span>
              <input type="text" value={previewIncome.toLocaleString("id-ID")} onChange={(e) => setPreviewIncome(parseInt(e.target.value.replace(/\./g, "")) || 0)} className="flex-1 bg-transparent text-sm font-bold outline-none tabular" />
            </div>
            <button onClick={() => setPreviewIncome((v) => Math.max(0, v - 1_000_000))} className="w-8 h-8 rounded-lg border border-[var(--border)] hover:bg-[var(--bg)] font-bold">−</button>
            <button onClick={() => setPreviewIncome((v) => v + 1_000_000)} className="w-8 h-8 rounded-lg border border-[var(--border)] hover:bg-[var(--bg)] font-bold">+</button>
          </div>
          {err && <p className="text-xs text-danger mb-2">{err}</p>}
          <Button variant="primary" className="w-full mb-4" onClick={() => { setErr(""); previewMut.mutate(); }} disabled={previewMut.isPending}>
            {previewMut.isPending ? "Calculating…" : "Preview"}
          </Button>

          {preview && (
            <>
              <div className="grid grid-cols-3 gap-2 mb-4">
                {[
                  { label: "Total Income", value: preview.income },
                  { label: "Total Allocated", value: preview.total_allocated, color: "text-primary" },
                  { label: "Remaining (Free Flow)", value: preview.remaining, color: preview.remaining > 0 ? "text-primary" : "text-[var(--text)]" },
                ].map((s) => (
                  <div key={s.label} className="bg-[var(--bg)] rounded-lg p-2.5 text-xs">
                    <p className="text-[var(--muted)]">{s.label}</p>
                    <p className={`font-bold tabular text-sm ${s.color ?? ""}`}>{bal(s.value)}</p>
                    <p className="text-[var(--muted)]">{preview.income > 0 ? Math.round((s.value / preview.income) * 100) : 0}%</p>
                  </div>
                ))}
              </div>

              {(preview.summary ?? []).length > 0 && (
                <div className="mb-4">
                  <SectionTitle>Strategy Overview</SectionTitle>
                  <div className="grid grid-cols-2 gap-2">
                    {preview.summary.map((s) => (
                      <div key={s.group} className="rounded-lg border border-[var(--border)] p-2.5 text-xs">
                        <div className="flex items-center justify-between gap-2">
                          <p className="font-medium">{s.label}</p>
                          <Badge variant={s.group === "investment" ? "purple" : s.group === "emergency_buffer" ? "green" : s.group.includes("spending") ? "blue" : "gray"}>{s.percent}%</Badge>
                        </div>
                        <p className="mt-1 font-bold tabular">{bal(s.amount)}</p>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <SectionTitle>Distribution Breakdown</SectionTitle>
              <table className="w-full text-xs mb-4">
                <thead>
                  <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                    <th className="text-left pb-2 font-medium w-6">#</th>
                    <th className="text-left pb-2 font-medium">Bucket</th>
                    <th className="text-left pb-2 font-medium">Type</th>
                  <th className="text-right pb-2 font-medium">Amount (Rp)</th>
                  <th className="text-right pb-2 font-medium">% of Income</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                  {preview.allocations.map((a, i) => (
                    <tr key={a.rule_id} className={a.skipped ? "opacity-60" : ""}>
                      <td className="py-2 text-[var(--muted)]">{i + 1}</td>
                      <td className="py-2">
                        <p className="font-medium">{a.target_bucket_name ?? a.rule_name}</p>
                        <p className="text-[var(--muted)]">{a.group.replace(/_/g, " ")} · {a.reason}</p>
                      </td>
                      <td className="py-2"><Badge variant={a.mode === "percent" ? "blue" : a.mode === "overflow" ? "gray" : "green"}>{a.mode === "percent" ? `${a.amount > 0 ? Math.round((a.amount / preview.income) * 100) : 0}% of income` : a.mode === "overflow" ? "Remainder" : "Fixed"}</Badge></td>
                      <td className="py-2 text-right tabular font-medium">{bal(a.amount)}</td>
                      <td className="py-2 text-right">
                        <div className="flex items-center gap-1.5 justify-end">
                          <ProgressBar value={preview.income > 0 ? (a.amount / preview.income) * 100 : 0} className="w-16" />
                          <span className="tabular w-8 text-right">{preview.income > 0 ? Math.round((a.amount / preview.income) * 100) : 0}%</span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {preview.remaining === 0 && (
                <div className="flex items-center justify-between p-3 rounded-xl bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800">
                  <div className="flex items-center gap-2">
                    <span className="text-primary">✓</span>
                    <div>
                      <p className="text-xs font-semibold text-primary">All set!</p>
                      <p className="text-xs text-[var(--muted)]">100% of your income has been allocated.</p>
                    </div>
                  </div>
                  <div className="text-right text-xs">
                    <p className="text-[var(--muted)]">Total</p>
                    <p className="font-bold tabular">{bal(preview.income)} 100%</p>
                  </div>
                </div>
              )}

              <Button
                variant="primary"
                className="w-full mt-3"
                disabled={applyMut.isPending || preview.total_allocated <= 0}
                onClick={() => { setErr(""); applyMut.mutate(); }}
              >
                {applyMut.isPending ? "Creating Allocation Plan…" : "Create Allocation Plan"}
              </Button>
            </>
          )}
        </Card>
      </div>

      <Modal open={modal !== null} onClose={() => setModal(null)} title={editing ? "Edit Rule" : "New Rule"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-3">
          <Input label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <Select label="Trigger" value={form.trigger} onChange={(e) => setForm({ ...form, trigger: e.target.value })}>
            <option value="manual">Manual</option>
            <option value="income_arrival">On Income Arrival</option>
          </Select>
          <Select label="Mode" value={form.mode} onChange={(e) => {
            const mode = e.target.value;
            setForm({ ...form, mode, value: mode === "percent" ? clampNumber(form.value) : form.value });
          }}>
            <option value="percent">Percentage</option>
            <option value="fixed">Fixed Amount</option>
            <option value="target_balance">Target Balance</option>
            <option value="overflow">Remainder (Overflow)</option>
          </Select>
          {form.mode !== "overflow" && (
            form.mode === "percent"
              ? <Input label="Percentage (%)" type="number" min={0} max={100} step={0.1} value={String(form.value)} onChange={(e) => setForm({ ...form, value: parseClampedNumber(e.target.value) })} />
              : <MoneyInput label="Amount" value={form.value} onChange={(v) => setForm({ ...form, value: v })} />
          )}
          {(form.mode === "percent" || form.mode === "fixed") && (
            <div className="rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-xs">
              <p className="text-[var(--muted)]">Conversion based on preview income</p>
              <p className="font-semibold tabular text-[var(--text)]">
                {previewIncome > 0
                  ? form.mode === "percent"
                    ? `${rulePercentValue || 0}% = ${bal(rulePercentAmount)}`
                    : `${bal(form.value)} = ${ruleFixedPercent}%`
                  : "Set preview income to calculate this."}
              </p>
            </div>
          )}
          <Select label="Target Bucket" value={form.target_bucket_id} onChange={(e) => setForm({ ...form, target_bucket_id: e.target.value })}>
            <option value="">— none —</option>
            {buckets.map((b: any) => <option key={b.bucket_id} value={b.bucket_id}>{b.name}</option>)}
          </Select>
          <Input label="Priority (lower = runs first)" type="number" min={1} value={String(form.priority)} onChange={(e) => setForm({ ...form, priority: parseInt(e.target.value) || 50 })} />
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={form.is_active} onChange={(e) => setForm({ ...form, is_active: e.target.checked })} />
            Active
          </label>
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
