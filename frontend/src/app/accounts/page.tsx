"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";
import { fmtMoney } from "@/lib/utils";
import type { Account, SummaryAccount } from "@/types/domain";

interface AccountForm {
  account_name: string;
  initial_balance: number;
  profile_type: Account["profile_type"];
  is_payroll_source: boolean;
  is_no_limit: boolean;
  is_buffer: boolean;
  fixed_limit_amount: number;
  budget: number;
}

const EMPTY: AccountForm = {
  account_name: "",
  initial_balance: 0,
  profile_type: "dynamic_spending",
  is_payroll_source: false,
  is_no_limit: false,
  is_buffer: false,
  fixed_limit_amount: 0,
  budget: 0,
};

export default function AccountsPage() {
  const qc = useQueryClient();
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [editing, setEditing] = useState<Account | null>(null);
  const [form, setForm] = useState<AccountForm>(EMPTY);
  const [err, setErr] = useState("");

  const { data: accountsData } = useQuery<{ accounts: Account[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  // Get current month budgets from summary
  const { data: summaryData } = useQuery<{ accounts: SummaryAccount[] }>({
    queryKey: ["summary-accounts"],
    queryFn: () => api.get("/summary"),
    select: (d: any) => d,
  });

  const budgetByAccount: Record<string, number> = {};
  (summaryData as any)?.accounts?.forEach((a: SummaryAccount) => {
    if (a.budget != null) budgetByAccount[a.account_id] = a.budget;
  });

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["accounts"] });
    qc.invalidateQueries({ queryKey: ["summary"] });
    qc.invalidateQueries({ queryKey: ["summary-accounts"] });
  };

  const createMut = useMutation({
    mutationFn: (f: AccountForm) =>
      api.post("/accounts", { account_name: f.account_name, initial_balance: f.initial_balance }),
    onSuccess: async (res: any) => {
      // set profile
      await api.put(`/accounts/${res.account_id}/profile`, {
        profile_type: form.profile_type,
        is_payroll_source: form.is_payroll_source,
        is_no_limit: form.is_no_limit,
        is_buffer: form.is_buffer,
        fixed_limit_amount: form.fixed_limit_amount || null,
      });
      // set budget if provided
      if (form.budget > 0) {
        const month = new Date().toISOString().slice(0, 7);
        await api.post("/budgets", { account_id: res.account_id, month, amount: form.budget });
      }
      invalidate();
      setModal(null);
    },
    onError: (e: Error) => setErr(e.message),
  });

  const editMut = useMutation({
    mutationFn: async (f: AccountForm) => {
      await api.put(`/accounts/${editing!.account_id}`, { account_name: f.account_name });
      await api.put(`/accounts/${editing!.account_id}/profile`, {
        profile_type: f.profile_type,
        is_payroll_source: f.is_payroll_source,
        is_no_limit: f.is_no_limit,
        is_buffer: f.is_buffer,
        fixed_limit_amount: f.fixed_limit_amount || null,
      });
      if (f.budget > 0) {
        const month = new Date().toISOString().slice(0, 7);
        await api.post("/budgets", { account_id: editing!.account_id, month, amount: f.budget });
      }
    },
    onSuccess: () => { invalidate(); setModal(null); },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => api.del(`/accounts/${id}`),
    onSuccess: () => invalidate(),
  });

  function openCreate() {
    setForm(EMPTY);
    setErr("");
    setModal("create");
  }

  function openEdit(acc: Account) {
    setEditing(acc);
    setForm({
      account_name: acc.account_name,
      initial_balance: 0,
      profile_type: acc.profile_type,
      is_payroll_source: acc.is_payroll_source,
      is_no_limit: acc.is_no_limit,
      is_buffer: acc.is_buffer,
      fixed_limit_amount: acc.fixed_limit_amount ?? 0,
      budget: budgetByAccount[acc.account_id] ?? 0,
    });
    setErr("");
    setModal("edit");
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (modal === "create") createMut.mutate(form);
    else editMut.mutate(form);
  }

  const accounts = accountsData?.accounts ?? [];
  const busy = createMut.isPending || editMut.isPending;

  return (
    <div className="p-4 max-w-2xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Accounts</h1>
        <Button variant="primary" size="sm" onClick={openCreate}>+ New Account</Button>
      </div>

      <div className="space-y-2">
        {accounts.length === 0 && (
          <p className="text-[var(--muted)] text-sm text-center py-8">No accounts yet. Create one to get started.</p>
        )}
        {accounts.map((acc) => (
          <div key={acc.account_id} className="bg-[var(--surface)] border border-[var(--border)] rounded-xl px-4 py-3 flex items-center justify-between gap-3">
            <div>
              <div className="font-medium">{acc.account_name}</div>
              <div className="text-xs text-[var(--muted)] mt-0.5 flex gap-2">
                <span>{acc.profile_type.replace("_", " ")}</span>
                {acc.is_payroll_source && <span className="text-green-600">payroll</span>}
                {acc.is_buffer && <span className="text-blue-500">buffer</span>}
                {budgetByAccount[acc.account_id] != null && (
                  <span>limit: {fmtMoney(budgetByAccount[acc.account_id])}</span>
                )}
              </div>
            </div>
            <div className="flex gap-2 shrink-0">
              <Button size="sm" variant="ghost" onClick={() => openEdit(acc)}>Edit</Button>
              <Button
                size="sm"
                variant="danger"
                onClick={() => confirm(`Delete "${acc.account_name}"?`) && deleteMut.mutate(acc.account_id)}
              >
                Delete
              </Button>
            </div>
          </div>
        ))}
      </div>

      <Modal open={modal !== null} onClose={() => setModal(null)} title={modal === "create" ? "New Account" : "Edit Account"}>
        <form onSubmit={handleSubmit} className="space-y-4">
          <Input
            label="Account Name"
            value={form.account_name}
            onChange={(e) => setForm({ ...form, account_name: e.target.value })}
            required
            placeholder="e.g. Cash, BCA, GoPay"
          />
          {modal === "create" && (
            <MoneyInput
              label="Initial Balance"
              value={form.initial_balance}
              onChange={(v) => setForm({ ...form, initial_balance: v })}
            />
          )}
          <MoneyInput
            label="Monthly Budget Limit"
            value={form.budget}
            onChange={(v) => setForm({ ...form, budget: v })}
          />
          <Select
            label="Profile Type"
            value={form.profile_type}
            onChange={(e) => setForm({ ...form, profile_type: e.target.value as Account["profile_type"] })}
          >
            <option value="dynamic_spending">Dynamic Spending</option>
            <option value="fixed_spending">Fixed Spending</option>
            <option value="tabungan">Savings (Tabungan)</option>
          </Select>
          {form.profile_type === "fixed_spending" && (
            <MoneyInput
              label="Fixed Limit Amount"
              value={form.fixed_limit_amount}
              onChange={(v) => setForm({ ...form, fixed_limit_amount: v })}
            />
          )}
          <div className="flex flex-col gap-2">
            {[
              { key: "is_payroll_source", label: "Payroll source (income arrives here)" },
              { key: "is_buffer", label: "Buffer account" },
              { key: "is_no_limit", label: "No spending limit" },
            ].map(({ key, label }) => (
              <label key={key} className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={form[key as keyof AccountForm] as boolean}
                  onChange={(e) => setForm({ ...form, [key]: e.target.checked })}
                  className="rounded"
                />
                {label}
              </label>
            ))}
          </div>
          {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
          <div className="flex gap-2 pt-1">
            <Button type="submit" variant="primary" className="flex-1" disabled={busy}>
              {busy ? "Saving…" : "Save"}
            </Button>
            <Button type="button" variant="secondary" onClick={() => setModal(null)}>Cancel</Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
