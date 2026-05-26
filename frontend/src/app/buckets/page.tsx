"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
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
  linked_account_ids?: string[];
  linked_accounts?: { account_id: string; account_name: string; balance: number }[];
  linked_goals?: string[];
  current_amount?: number;
  priority: number; is_archived: boolean; notes: string | null;
}

const KIND_ICONS: Record<string, string> = { spending: "💳", sinking: "🪣", emergency: "🛡️", goal: "🎯", investment: "📈" };
const KIND_COLORS: Record<string, string> = { spending: "#3b82f6", sinking: "#8b5cf6", emergency: "#f59e0b", goal: "#16a34a", investment: "#06b6d4" };
function getPriorityBadge(priority: number) {
  if (priority <= 20) return { label: "High priority", filter: "high", color: "red" as const };
  if (priority <= 60) return { label: "Medium priority", filter: "medium", color: "yellow" as const };
  return { label: "Low priority", filter: "low", color: "gray" as const };
}

const EMPTY = { name: "", kind: "spending", target_amount: 0, linked_account_ids: [] as string[], priority: 50, notes: "" };

export default function BucketsPage() {
  const qc = useQueryClient();
  const { accounts, hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Bucket | null>(null);
  const [form, setForm] = useState(EMPTY);
  const [err, setErr] = useState("");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("active");
  const [priorityFilter, setPriorityFilter] = useState("all");
  const [sortBy, setSortBy] = useState("priority");
  const actionHandledRef = useRef(false);

  const { data } = useQuery<{ buckets: Bucket[] }>({ queryKey: ["buckets"], queryFn: () => api.get("/buckets") });
  const inv = () => qc.invalidateQueries({ queryKey: ["buckets"] });

  const saveMut = useMutation({
    mutationFn: () => {
      const payload = {
        ...form,
        target_amount: form.target_amount || null,
        linked_account_ids: form.linked_account_ids,
        linked_account_id: form.linked_account_ids[0] ?? null,
      };
      return editing ? api.put(`/buckets/${editing.bucket_id}`, payload) : api.post("/buckets", payload);
    },
    onSuccess: () => { inv(); setModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({ mutationFn: (id: string) => api.del(`/buckets/${id}`), onSuccess: () => { inv(); setModal(null); } });

  useEffect(() => {
    if (actionHandledRef.current) return;
    actionHandledRef.current = true;
    const params = new URLSearchParams(window.location.search);
    if (params.get("action") === "new") {
      openCreate();
      params.delete("action");
      const nextUrl = params.toString() ? `${window.location.pathname}?${params.toString()}` : window.location.pathname;
      window.history.replaceState(null, "", nextUrl);
    }
  }, []);

  const buckets = (data?.buckets ?? [])
    .filter((b) => {
      const pBadge = getPriorityBadge(b.priority);
      if (statusFilter === "active" && b.is_archived) return false;
      if (statusFilter === "archived" && !b.is_archived) return false;
      if (priorityFilter !== "all" && pBadge.filter !== priorityFilter) return false;
      if (search && !b.name.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    })
    .sort((a, b) => {
      if (sortBy === "name") return a.name.localeCompare(b.name);
      if (sortBy === "target") return (b.target_amount ?? 0) - (a.target_amount ?? 0);
      return a.priority - b.priority;
    });
  const totalEarmarked = buckets.reduce((s, b) => s + (b.current_amount ?? 0), 0);
  const totalTarget = buckets.reduce((s, b) => s + (b.target_amount ?? 0), 0);
  const onTrack = buckets.filter((b) => (b.target_amount ?? 0) > 0 && (b.current_amount ?? 0) >= (b.target_amount ?? 0)).length;
  const belowTarget = buckets.filter((b) => (b.target_amount ?? 0) > 0 && (b.current_amount ?? 0) < (b.target_amount ?? 0)).length;

  function openCreate() { setEditing(null); setForm(EMPTY); setErr(""); setModal("create"); }
  function openEdit(b: Bucket) {
    setEditing(b);
    setForm({
      name: b.name,
      kind: b.kind,
      target_amount: b.target_amount ?? 0,
      linked_account_ids: b.linked_account_ids?.length ? b.linked_account_ids : b.linked_account_id ? [b.linked_account_id] : [],
      priority: b.priority,
      notes: b.notes ?? "",
    });
    setErr(""); setModal("edit");
  }

  function toggleAccount(accountId: string) {
    setForm((prev) => ({
      ...prev,
      linked_account_ids: prev.linked_account_ids.includes(accountId)
        ? prev.linked_account_ids.filter((id) => id !== accountId)
        : [...prev.linked_account_ids, accountId],
    }));
  }

  return (
    <div className="p-5 space-y-4">
      {/* Summary stats */}
      <Card padding="sm">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs text-[var(--muted)]">Total Earmarked</p>
            <p className="text-2xl font-bold text-primary tabular">{bal(totalEarmarked)}</p>
            <p className="text-xs text-[var(--muted)]">Actual linked account balance across {buckets.length} active buckets</p>
          </div>
          <div className="flex gap-6 text-sm">
            <div className="text-center"><p className="text-xs text-[var(--muted)]">Total Target</p><p className="font-bold tabular">{bal(totalTarget)}</p></div>
            <div className="text-center"><p className="text-xs text-[var(--muted)]">On Track</p><p className="font-bold text-primary">● {onTrack} buckets</p></div>
            <div className="text-center"><p className="text-xs text-[var(--muted)]">Below Target</p><p className="font-bold text-warning">● {belowTarget} buckets</p></div>
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
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]">
          <option value="active">Status: Active</option>
          <option value="archived">Status: Archived</option>
          <option value="all">Status: All</option>
        </select>
        <select value={priorityFilter} onChange={(e) => setPriorityFilter(e.target.value)} className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]">
          <option value="all">Priority: All</option>
          <option value="high">Priority: High</option>
          <option value="medium">Priority: Medium</option>
          <option value="low">Priority: Low</option>
        </select>
        <select value={sortBy} onChange={(e) => setSortBy(e.target.value)} className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]">
          <option value="priority">Sort by: Priority</option>
          <option value="name">Sort by: Name</option>
          <option value="target">Sort by: Target</option>
        </select>
        <div className="ml-auto flex gap-1">
          <button type="button" disabled title="Grid/list switching is coming soon" className="p-1.5 rounded border border-[var(--border)] text-sm opacity-50 cursor-not-allowed">⊞</button>
          <button type="button" disabled title="Grid/list switching is coming soon" className="p-1.5 rounded border border-[var(--border)] text-sm opacity-50 cursor-not-allowed">☰</button>
        </div>
      </div>

      {/* Bucket cards grid */}
      <div className="grid grid-cols-3 gap-4">
        {buckets.length === 0 && <p className="col-span-3 text-center text-[var(--muted)] py-8">No buckets yet. Create one to get started.</p>}
        {buckets.map((b) => {
          const pBadge = getPriorityBadge(b.priority);
          const icon = KIND_ICONS[b.kind] ?? "🪣";
          const color = KIND_COLORS[b.kind] ?? "#6b7280";
          const currentAmount = b.current_amount ?? 0;
          const targetAmount = b.target_amount ?? 0;
          const progress = targetAmount > 0 ? Math.min(100, Math.round((currentAmount / targetAmount) * 100)) : 0;
          const targetMet = targetAmount > 0 && currentAmount >= targetAmount;
          const hasTargetGap = targetAmount > 0 && currentAmount < targetAmount;
          const linkedAccounts = b.linked_accounts?.length
            ? b.linked_accounts
            : accounts.filter((a) => (b.linked_account_ids ?? [b.linked_account_id]).includes(a.account_id))
                .map((a) => ({ account_id: a.account_id, account_name: a.account_name, balance: a.balance ?? 0 }));
          const linkedLabel = linkedAccounts.length === 0
            ? "—"
            : linkedAccounts.length === 1
              ? linkedAccounts[0].account_name
              : `${linkedAccounts.length} accounts`;
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
                <button onClick={() => openEdit(b)} className="text-xs text-[var(--muted)] hover:text-[var(--text)]">Edit</button>
              </div>
              <div className="flex items-center gap-2 mb-3">
                <Badge variant={pBadge.color}>{pBadge.label}</Badge>
                {targetMet && <Badge variant="green">Target met</Badge>}
                {hasTargetGap && <Badge variant="yellow">Below target</Badge>}
              </div>
              <div className="grid grid-cols-2 gap-3 text-xs mb-3">
                <div><p className="text-[var(--muted)]">Current Amount</p><p className="font-bold tabular text-sm">{bal(currentAmount)}</p></div>
                <div className="text-right"><p className="text-[var(--muted)]">Target Amount</p><p className="font-bold tabular text-sm">{b.target_amount ? bal(b.target_amount) : "—"}</p></div>
              </div>
              {b.target_amount && <ProgressBar value={progress} intent="completion" size="md" className="mb-3" />}
              <div className="grid grid-cols-2 gap-3 text-xs border-t border-[var(--border)] pt-3">
                <div>
                  <p className="text-[var(--muted)]">Linked Accounts</p>
                  <p className="font-medium">{linkedLabel}</p>
                </div>
                <div className="text-right">
                  <p className="text-[var(--muted)]">Progress</p>
                  <p className="font-medium">{targetAmount > 0 ? `${progress}%` : "—"}</p>
                </div>
              </div>
              {!!b.linked_goals?.length && (
                <div className="mt-3 border-t border-[var(--border)] pt-3 text-xs">
                  <p className="text-[var(--muted)]">Linked Goals</p>
                  <p className="font-medium">{b.linked_goals.join(", ")}</p>
                </div>
              )}
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
            <Link href="/goals" prefetch={false} className="inline-flex items-center justify-center font-medium rounded transition-colors px-3 py-1.5 text-xs bg-[var(--surface)] border border-[var(--border)] hover:bg-[var(--bg)] text-[var(--text)]">View Goals →</Link>
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
          <div className="flex flex-col gap-1">
            <p className="text-xs font-medium text-[var(--muted)]">Linked Accounts (optional)</p>
            <div className="max-h-44 overflow-auto rounded border border-[var(--border)] bg-[var(--surface)] p-2 space-y-1">
              {accounts.length === 0 && <p className="text-xs text-[var(--muted)] px-1 py-2">Create an account first to link bucket balance.</p>}
              {accounts.map((a) => (
                <label key={a.account_id} className="flex items-center justify-between gap-3 rounded px-2 py-1.5 text-sm hover:bg-[var(--bg)]">
                  <span className="flex items-center gap-2 min-w-0">
                    <input
                      type="checkbox"
                      checked={form.linked_account_ids.includes(a.account_id)}
                      onChange={() => toggleAccount(a.account_id)}
                    />
                    <span className="truncate">{a.account_name}</span>
                  </span>
                  <span className="text-xs tabular text-[var(--muted)]">{bal(a.balance ?? 0)}</span>
                </label>
              ))}
            </div>
            <p className="text-xs text-[var(--muted)]">
              Current bucket amount is the sum of the selected account balances.
            </p>
          </div>
          <Input label="Priority (lower = higher)" type="number" min={1} max={999} value={String(form.priority)} onChange={(e) => setForm({ ...form, priority: parseInt(e.target.value) || 50 })} />
          <Input label="Notes (optional)" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            {editing && (
              <Button type="button" variant="danger" onClick={() => confirm(`Delete "${editing.name}"?`) && deleteMut.mutate(editing.bucket_id)} disabled={deleteMut.isPending}>Delete</Button>
            )}
            <Button type="submit" variant="primary" className="flex-1" disabled={saveMut.isPending}>Save</Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
