"use client";

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card } from "@/components/ui/Card";
import { Input, Select } from "@/components/ui/Input";
import { Modal } from "@/components/ui/Modal";
import { MoneyInput } from "@/components/ui/MoneyInput";
import type { Account, Obligation, ObligationSettlement, ObligationSummary } from "@/types/domain";

type Tab = "open" | "receivable" | "payable" | "all";

const EMPTY_FORM = {
  kind: "receivable" as "receivable" | "payable",
  title: "",
  counterparty_name: "",
  counterparty_type: "person",
  principal_amount: 0,
  issue_date: new Date().toISOString().slice(0, 10),
  due_date: "",
  default_account_id: "",
  notes: "",
  recurrence_frequency: "none",
  auto_post_enabled: false,
  auto_post_day: "",
};

function statusBadge(status: Obligation["status"]) {
  if (status === "settled") return <Badge variant="green">Settled</Badge>;
  if (status === "partial") return <Badge variant="blue">Partial</Badge>;
  if (status === "written_off") return <Badge variant="gray">Written off</Badge>;
  if (status === "cancelled") return <Badge variant="gray">Cancelled</Badge>;
  return <Badge variant="yellow">Open</Badge>;
}

export default function ObligationsPage() {
  const qc = useQueryClient();
  const [tab, setTab] = useState<Tab>("open");
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<Obligation | null>(null);
  const [form, setForm] = useState(EMPTY_FORM);
  const [detailId, setDetailId] = useState<string | null>(null);
  const [settling, setSettling] = useState<Obligation | null>(null);
  const [settlement, setSettlement] = useState({ amount: 0, account_id: "", settled_at: new Date().toISOString().slice(0, 10), notes: "" });
  const [err, setErr] = useState("");

  const { data: summary } = useQuery<ObligationSummary>({ queryKey: ["obligations-summary"], queryFn: () => api.get("/obligations/summary") });
  const { data: accountsData } = useQuery<{ accounts: Account[] }>({ queryKey: ["accounts"], queryFn: () => api.get("/accounts") });
  const { data, isLoading } = useQuery<{ obligations: Obligation[] }>({
    queryKey: ["obligations", tab],
    queryFn: () => {
      if (tab === "receivable") return api.get("/obligations?kind=receivable");
      if (tab === "payable") return api.get("/obligations?kind=payable");
      if (tab === "all") return api.get("/obligations?status=all");
      return api.get("/obligations?status=open,partial");
    },
  });
  const { data: detailData } = useQuery<{ obligation: Obligation; settlements: ObligationSettlement[] }>({
    queryKey: ["obligation", detailId],
    queryFn: () => api.get(`/obligations/${detailId}`),
    enabled: !!detailId,
  });

  const accounts = accountsData?.accounts ?? [];
  const obligations = useMemo(() => data?.obligations ?? [], [data?.obligations]);
  const activeSummary = summary ?? {
    receivable_outstanding: 0,
    payable_outstanding: 0,
    receivable_overdue: 0,
    payable_overdue: 0,
    due_soon: 0,
    open_count: 0,
    net_expected: 0,
  };

  const overdueIds = useMemo(() => {
    const today = new Date().toISOString().slice(0, 10);
    return new Set(obligations.filter((o) => o.due_date && o.due_date < today && ["open", "partial"].includes(o.status)).map((o) => o.obligation_id));
  }, [obligations]);

  function invalidate() {
    qc.invalidateQueries({ queryKey: ["obligations"] });
    qc.invalidateQueries({ queryKey: ["obligations-summary"] });
    qc.invalidateQueries({ queryKey: ["dashboard"] });
    qc.invalidateQueries({ queryKey: ["summary"] });
    qc.invalidateQueries({ queryKey: ["accounts"] });
    if (detailId) qc.invalidateQueries({ queryKey: ["obligation", detailId] });
  }

  function openCreate(kind: "receivable" | "payable" = "receivable") {
    setEditing(null);
    setForm({ ...EMPTY_FORM, kind });
    setErr("");
    setFormOpen(true);
  }

  function openEdit(obligation: Obligation) {
    setEditing(obligation);
    setForm({
      kind: obligation.kind,
      title: obligation.title,
      counterparty_name: obligation.counterparty_name ?? "",
      counterparty_type: obligation.counterparty_type ?? "person",
      principal_amount: obligation.principal_amount,
      issue_date: obligation.issue_date,
      due_date: obligation.due_date ?? "",
      default_account_id: obligation.default_account_id ?? "",
      notes: obligation.notes ?? "",
      recurrence_frequency: obligation.recurrence_frequency,
      auto_post_enabled: obligation.auto_post_enabled,
      auto_post_day: obligation.auto_post_day ? String(obligation.auto_post_day) : "",
    });
    setErr("");
    setFormOpen(true);
  }

  function openSettle(obligation: Obligation) {
    setSettling(obligation);
    setSettlement({
      amount: obligation.outstanding_amount,
      account_id: obligation.default_account_id ?? "",
      settled_at: new Date().toISOString().slice(0, 10),
      notes: "",
    });
    setErr("");
  }

  const saveMut = useMutation({
    mutationFn: () => {
      const payload = {
        ...form,
        default_account_id: form.default_account_id || null,
        due_date: form.due_date || null,
        auto_post_day: form.auto_post_day || null,
      };
      return editing ? api.put(`/obligations/${editing.obligation_id}`, payload) : api.post("/obligations", payload);
    },
    onSuccess: () => { invalidate(); setFormOpen(false); setEditing(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const settleMut = useMutation({
    mutationFn: () => api.post(`/obligations/${settling?.obligation_id}/settlements`, settlement),
    onSuccess: () => { invalidate(); setSettling(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const reverseMut = useMutation({
    mutationFn: ({ obligationId, settlementId }: { obligationId: string; settlementId: string }) => api.del(`/obligations/${obligationId}/settlements/${settlementId}`),
    onSuccess: () => invalidate(),
    onError: (e: Error) => setErr(e.message),
  });

  const cancelMut = useMutation({
    mutationFn: (id: string) => api.post(`/obligations/${id}/cancel`, {}),
    onSuccess: () => { invalidate(); setDetailId(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const writeOffMut = useMutation({
    mutationFn: (id: string) => api.post(`/obligations/${id}/write-off`, {}),
    onSuccess: () => { invalidate(); setDetailId(null); },
    onError: (e: Error) => setErr(e.message),
  });

  return (
    <div className="p-5 space-y-4">
      <div className="grid grid-cols-5 gap-3">
        <Card padding="sm">
          <p className="text-xs text-[var(--muted)]">Receivable</p>
          <p className="text-lg font-bold tabular text-primary">{fmtMoney(activeSummary.receivable_outstanding)}</p>
          <p className="text-xs text-[var(--muted)]">Expected money in</p>
        </Card>
        <Card padding="sm">
          <p className="text-xs text-[var(--muted)]">Payable</p>
          <p className="text-lg font-bold tabular text-danger">{fmtMoney(activeSummary.payable_outstanding)}</p>
          <p className="text-xs text-[var(--muted)]">Committed money out</p>
        </Card>
        <Card padding="sm">
          <p className="text-xs text-[var(--muted)]">Overdue</p>
          <p className="text-lg font-bold tabular text-warning">{fmtMoney(activeSummary.receivable_overdue + activeSummary.payable_overdue)}</p>
          <p className="text-xs text-[var(--muted)]">Past due items</p>
        </Card>
        <Card padding="sm">
          <p className="text-xs text-[var(--muted)]">Due Soon</p>
          <p className="text-lg font-bold tabular">{fmtMoney(activeSummary.due_soon)}</p>
          <p className="text-xs text-[var(--muted)]">Next 30 days</p>
        </Card>
        <Card padding="sm">
          <p className="text-xs text-[var(--muted)]">Net Expected</p>
          <p className={`text-lg font-bold tabular ${activeSummary.net_expected >= 0 ? "text-primary" : "text-danger"}`}>{fmtMoney(activeSummary.net_expected)}</p>
          <p className="text-xs text-[var(--muted)]">{activeSummary.open_count} open item{activeSummary.open_count === 1 ? "" : "s"}</p>
        </Card>
      </div>

      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-1 border border-[var(--border)] rounded-lg p-1 bg-[var(--surface)]">
          {(["open", "receivable", "payable", "all"] as Tab[]).map((item) => (
            <button
              key={item}
              type="button"
              onClick={() => setTab(item)}
              className={`px-3 py-1.5 rounded text-xs font-medium capitalize ${tab === item ? "bg-[var(--primary)] text-white" : "text-[var(--muted)] hover:text-[var(--text)]"}`}
            >
              {item}
            </button>
          ))}
        </div>
        <div className="flex gap-2">
          <Button variant="secondary" onClick={() => openCreate("receivable")}>+ Receivable</Button>
          <Button variant="primary" onClick={() => openCreate("payable")}>+ Payable</Button>
        </div>
      </div>

      <Card padding="sm">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[var(--muted)] border-b border-[var(--border)]">
              <th className="text-left pb-2 font-medium">Item</th>
              <th className="text-left pb-2 font-medium">Party</th>
              <th className="text-left pb-2 font-medium">Due</th>
              <th className="text-right pb-2 font-medium">Original</th>
              <th className="text-right pb-2 font-medium">Outstanding</th>
              <th className="text-right pb-2 font-medium">Status</th>
              <th className="text-right pb-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {isLoading && (
              <tr><td colSpan={7} className="py-8 text-center text-[var(--muted)]">Loading...</td></tr>
            )}
            {!isLoading && obligations.map((o) => (
              <tr key={o.obligation_id} className="hover:bg-[var(--bg)]">
                <td className="py-2">
                  <div className="flex items-center gap-2">
                    <span className={`w-2 h-2 rounded-full ${o.kind === "receivable" ? "bg-[var(--primary)]" : "bg-[var(--danger)]"}`} />
                    <div>
                      <p className="font-medium text-[var(--text)]">{o.title}</p>
                      <p className="text-[var(--muted)] capitalize">{o.kind}{o.default_account_name ? ` -> ${o.default_account_name}` : ""}</p>
                    </div>
                  </div>
                </td>
                <td className="py-2 text-[var(--muted)]">{o.counterparty_name || "-"}</td>
                <td className={`py-2 ${overdueIds.has(o.obligation_id) ? "text-danger font-semibold" : "text-[var(--muted)]"}`}>{o.due_date || "-"}</td>
                <td className="py-2 text-right tabular">{fmtMoney(o.principal_amount)}</td>
                <td className="py-2 text-right tabular font-semibold">{fmtMoney(o.outstanding_amount)}</td>
                <td className="py-2 text-right">{statusBadge(o.status)}</td>
                <td className="py-2 text-right">
                  <div className="flex items-center justify-end gap-1">
                    {["open", "partial"].includes(o.status) && (
                      <Button size="sm" variant={o.kind === "receivable" ? "primary" : "secondary"} onClick={() => openSettle(o)}>
                        {o.kind === "receivable" ? "Receive" : "Pay"}
                      </Button>
                    )}
                    <Button size="sm" variant="ghost" onClick={() => setDetailId(o.obligation_id)}>Details</Button>
                  </div>
                </td>
              </tr>
            ))}
            {!isLoading && obligations.length === 0 && (
              <tr><td colSpan={7} className="py-8 text-center text-[var(--muted)]">No payables or receivables yet.</td></tr>
            )}
          </tbody>
        </table>
      </Card>

      <Modal open={formOpen} onClose={() => setFormOpen(false)} title={editing ? "Edit Item" : "Add Payable or Receivable"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <Select label="Type" value={form.kind} onChange={(e) => setForm({ ...form, kind: e.target.value as "receivable" | "payable" })}>
              <option value="receivable">Receivable - money owed to me</option>
              <option value="payable">Payable - money I owe</option>
            </Select>
            <Select label="Party Type" value={form.counterparty_type} onChange={(e) => setForm({ ...form, counterparty_type: e.target.value })}>
              <option value="person">Person</option>
              <option value="client">Client</option>
              <option value="vendor">Vendor</option>
              <option value="institution">Institution</option>
              <option value="other">Other</option>
            </Select>
          </div>
          <Input label="Title" value={form.title} onChange={(e) => setForm({ ...form, title: e.target.value })} required placeholder="e.g. May freelance invoice" />
          <Input label="Party Name" value={form.counterparty_name} onChange={(e) => setForm({ ...form, counterparty_name: e.target.value })} placeholder="e.g. Client A, Friend, Vendor" />
          <MoneyInput label="Amount" value={form.principal_amount} onChange={(v) => setForm({ ...form, principal_amount: v })} required />
          <div className="grid grid-cols-2 gap-3">
            <Input label="Issue Date" type="date" value={form.issue_date} onChange={(e) => setForm({ ...form, issue_date: e.target.value })} />
            <Input label="Due Date" type="date" value={form.due_date} onChange={(e) => setForm({ ...form, due_date: e.target.value })} />
          </div>
          <Select label="Default Settlement Account" value={form.default_account_id} onChange={(e) => setForm({ ...form, default_account_id: e.target.value })}>
            <option value="">Choose when settling</option>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          <Input label="Notes" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} placeholder="Optional context" />
          <div className="grid grid-cols-2 gap-3">
            <Select label="Recurrence" value={form.recurrence_frequency} onChange={(e) => setForm({ ...form, recurrence_frequency: e.target.value })}>
              <option value="none">None</option>
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
              <option value="quarterly">Quarterly</option>
              <option value="yearly">Yearly</option>
            </Select>
            <Input label="Auto-post Day" type="number" min={1} max={31} value={form.auto_post_day} onChange={(e) => setForm({ ...form, auto_post_day: e.target.value })} placeholder="Future automation" />
          </div>
          <label className="flex items-center gap-2 text-xs text-[var(--muted)]">
            <input type="checkbox" checked={form.auto_post_enabled} onChange={(e) => setForm({ ...form, auto_post_enabled: e.target.checked })} />
            Enable auto-post when automation is available
          </label>
          {err && <p className="text-xs text-danger">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="button" variant="secondary" onClick={() => setFormOpen(false)}>Cancel</Button>
            <Button type="submit" variant="primary" disabled={saveMut.isPending} className="flex-1">{saveMut.isPending ? "Saving..." : "Save"}</Button>
          </div>
        </form>
      </Modal>

      <Modal open={!!settling} onClose={() => setSettling(null)} title={settling?.kind === "receivable" ? "Receive Payment" : "Pay Obligation"}>
        {settling && (
          <form onSubmit={(e) => { e.preventDefault(); settleMut.mutate(); }} className="space-y-3">
            <div className="rounded-lg border border-[var(--border)] p-3 text-xs">
              <p className="font-semibold">{settling.title}</p>
              <p className="text-[var(--muted)]">Outstanding: {fmtMoney(settling.outstanding_amount)}</p>
            </div>
            <MoneyInput label="Amount" value={settlement.amount} onChange={(v) => setSettlement({ ...settlement, amount: v })} required />
            <Select label="Account" value={settlement.account_id} onChange={(e) => setSettlement({ ...settlement, account_id: e.target.value })} required>
              <option value="">Select account</option>
              {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name} - {fmtMoney(a.balance ?? 0)}</option>)}
            </Select>
            <Input label="Date" type="date" value={settlement.settled_at} onChange={(e) => setSettlement({ ...settlement, settled_at: e.target.value })} />
            <Input label="Notes" value={settlement.notes} onChange={(e) => setSettlement({ ...settlement, notes: e.target.value })} placeholder="Optional" />
            <p className="text-xs text-[var(--muted)]">Remaining after this: {fmtMoney(Math.max(0, settling.outstanding_amount - settlement.amount))}</p>
            {err && <p className="text-xs text-danger">{err}</p>}
            <div className="flex gap-2">
              <Button type="button" variant="secondary" onClick={() => setSettling(null)}>Cancel</Button>
              <Button type="submit" variant="primary" disabled={settleMut.isPending} className="flex-1">{settleMut.isPending ? "Processing..." : "Confirm"}</Button>
            </div>
          </form>
        )}
      </Modal>

      <Modal open={!!detailId} onClose={() => setDetailId(null)} title="Obligation Details">
        {detailData?.obligation && (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-3 text-xs">
              <div><p className="text-[var(--muted)]">Title</p><p className="font-semibold">{detailData.obligation.title}</p></div>
              <div><p className="text-[var(--muted)]">Status</p>{statusBadge(detailData.obligation.status)}</div>
              <div><p className="text-[var(--muted)]">Party</p><p>{detailData.obligation.counterparty_name || "-"}</p></div>
              <div><p className="text-[var(--muted)]">Due Date</p><p>{detailData.obligation.due_date || "-"}</p></div>
              <div><p className="text-[var(--muted)]">Original</p><p className="tabular">{fmtMoney(detailData.obligation.principal_amount)}</p></div>
              <div><p className="text-[var(--muted)]">Outstanding</p><p className="tabular font-semibold">{fmtMoney(detailData.obligation.outstanding_amount)}</p></div>
            </div>
            <div>
              <p className="text-xs font-semibold mb-2">Settlement History</p>
              <div className="space-y-2">
                {detailData.settlements.map((s) => (
                  <div key={s.settlement_id} className="flex items-center justify-between rounded-lg border border-[var(--border)] p-2 text-xs">
                    <div>
                      <p className={s.reversed_at ? "line-through text-[var(--muted)]" : "font-medium"}>{fmtMoney(s.amount)} via {s.account_name}</p>
                      <p className="text-[var(--muted)]">{new Date(s.settled_at).toLocaleString("id-ID")}{s.reversed_at ? " - reversed" : ""}</p>
                    </div>
                    {!s.reversed_at && (
                      <Button size="sm" variant="ghost" onClick={() => confirm("Reverse this settlement and delete its linked transaction?") && reverseMut.mutate({ obligationId: detailData.obligation.obligation_id, settlementId: s.settlement_id })}>
                        Reverse
                      </Button>
                    )}
                  </div>
                ))}
                {detailData.settlements.length === 0 && <p className="text-xs text-[var(--muted)]">No settlements yet.</p>}
              </div>
            </div>
            {err && <p className="text-xs text-danger">{err}</p>}
            <div className="flex flex-wrap gap-2">
              <Button variant="secondary" onClick={() => openEdit(detailData.obligation)}>Edit</Button>
              {["open", "partial"].includes(detailData.obligation.status) && (
                <Button variant="primary" onClick={() => openSettle(detailData.obligation)}>{detailData.obligation.kind === "receivable" ? "Receive" : "Pay"}</Button>
              )}
              {["open", "partial"].includes(detailData.obligation.status) && (
                <>
                  <Button variant="secondary" onClick={() => confirm("Cancel this item? It will no longer be counted as outstanding.") && cancelMut.mutate(detailData.obligation.obligation_id)}>Cancel Item</Button>
                  <Button variant="danger" onClick={() => confirm("Write off this item? It will close without creating a transaction.") && writeOffMut.mutate(detailData.obligation.obligation_id)}>Write Off</Button>
                </>
              )}
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}
