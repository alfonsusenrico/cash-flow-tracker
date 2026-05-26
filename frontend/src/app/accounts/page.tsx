"use client";
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Card, SectionTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { ProgressBar } from "@/components/ui/ProgressBar";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";
import type { Account } from "@/types/domain";

export default function AccountsPage() {
  const qc = useQueryClient();
  const { hideBalances } = useAppCtx();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const [selected, setSelected] = useState<Account | null>(null);
  const [modal, setModal] = useState<"create" | "edit" | null>(null);
  const [form, setForm] = useState({ account_name: "", profile_type: "dynamic_spending", is_payroll_source: false, is_buffer: false, is_no_limit: false, fixed_limit_amount: 0, budget: 0, initial_balance: 0 });
  const [err, setErr] = useState("");
  const [search, setSearch] = useState("");
  const [profileFilter, setProfileFilter] = useState("all");

  const { data } = useQuery<{ accounts: Account[] }>({ queryKey: ["accounts"], queryFn: () => api.get("/accounts") });
  const { data: summaryData } = useQuery<any>({ queryKey: ["summary"], queryFn: () => api.get("/summary") });

  const isBudgetedSpendingProfile = (profileType: string, isNoLimit = false) =>
    !isNoLimit && (profileType === "dynamic_spending" || profileType === "fixed_spending");

  const budgetByAcc: Record<string, { budget_id?: string; pct: number; quotaPct: number; status: string; amount: number }> = {};
  summaryData?.accounts?.forEach((a: any) => {
    if (a.budget != null) {
      budgetByAcc[a.account_id] = {
        budget_id: a.budget_id,
        pct: a.budget_pct ?? 0,
        quotaPct: Math.max(0, 100 - (a.budget_pct ?? 0)),
        status: a.budget_status ?? "ok",
        amount: a.budget,
      };
    }
  });
  const summaryByAccount = new Map<string, any>((summaryData?.accounts ?? []).map((a: any) => [a.account_id, a]));
  const currentBalance = (account: Account) => account.balance ?? summaryByAccount.get(account.account_id)?.current_balance ?? 0;

  const inv = () => { qc.invalidateQueries({ queryKey: ["accounts"] }); qc.invalidateQueries({ queryKey: ["summary"] }); };

  const saveMut = useMutation({
    mutationFn: async () => {
      if (modal === "create") {
        const res: any = await api.post("/accounts", { account_name: form.account_name, initial_balance: form.initial_balance });
        await api.put(`/accounts/${res.account_id}/profile`, { profile_type: form.profile_type, is_payroll_source: form.is_payroll_source, is_buffer: form.is_buffer, is_no_limit: form.is_no_limit, fixed_limit_amount: form.fixed_limit_amount || null });
        if (isBudgetedSpendingProfile(form.profile_type, form.is_no_limit) && form.budget > 0) {
          await api.post("/budgets", { account_id: res.account_id, month: new Date().toISOString().slice(0, 7), amount: form.budget });
        }
        return null;
      } else if (selected) {
        const profileRes: any = await api.put(`/accounts/${selected.account_id}/profile`, { account_name: form.account_name, profile_type: form.profile_type, is_payroll_source: form.is_payroll_source, is_buffer: form.is_buffer, is_no_limit: form.is_no_limit, fixed_limit_amount: form.fixed_limit_amount || null });
        const existingBudgetId = budgetByAcc[selected.account_id]?.budget_id;
        if (isBudgetedSpendingProfile(form.profile_type, form.is_no_limit) && form.budget > 0) {
          await api.post("/budgets", { account_id: selected.account_id, month: new Date().toISOString().slice(0, 7), amount: form.budget });
        } else if (existingBudgetId) {
          await api.del(`/budgets/${existingBudgetId}`);
        }
        return profileRes.account ?? null;
      }
      throw new Error("Choose an account before saving changes.");
    },
    onSuccess: (account) => {
      if (account) {
        qc.setQueryData<{ accounts: Account[] }>(["accounts"], (old) => old ? {
          accounts: old.accounts.map((a) => a.account_id === account.account_id ? { ...a, ...account } : a),
        } : old);
        setSelected((prev) => prev?.account_id === account.account_id ? { ...prev, ...account } : account);
      }
      inv();
      setModal(null);
    },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => api.del(`/accounts/${id}`),
    onSuccess: () => { inv(); setSelected(null); },
  });

  const allAccounts = data?.accounts ?? [];
  const accounts = allAccounts.filter((a) => {
    if (search && !a.account_name.toLowerCase().includes(search.toLowerCase())) return false;
    if (profileFilter !== "all" && a.profile_type !== profileFilter) return false;
    return true;
  });
  const totalBalance = allAccounts.reduce((s, a) => s + currentBalance(a), 0);
  const filteredBalance = accounts.reduce((s, a) => s + currentBalance(a), 0);

  function openCreate() {
    setForm({ account_name: "", profile_type: "dynamic_spending", is_payroll_source: false, is_buffer: false, is_no_limit: false, fixed_limit_amount: 0, budget: 0, initial_balance: 0 });
    setErr(""); setModal("create");
  }
  function openEdit(acc: Account) {
    setSelected(acc);
    setForm({ account_name: acc.account_name, profile_type: acc.profile_type, is_payroll_source: acc.is_payroll_source, is_buffer: acc.is_buffer, is_no_limit: acc.is_no_limit, fixed_limit_amount: acc.fixed_limit_amount ?? 0, budget: budgetByAcc[acc.account_id]?.amount ?? 0, initial_balance: 0 });
    setErr(""); setModal("edit");
  }

  return (
    <div className="flex h-[calc(100vh-56px)]">
      <div className="flex-1 p-5 overflow-auto">
        {/* Stats */}
        <div className="grid grid-cols-4 gap-3 mb-4">
          {[
            { label: "Total Accounts", value: allAccounts.length, sub: "Across institutions", icon: "🏦" },
            { label: "Total Balance", value: null, money: totalBalance, sub: "All accounts", icon: "💰" },
            { label: "Payroll Sources", value: allAccounts.filter((a) => a.is_payroll_source).length, sub: "Salary accounts", icon: "📋" },
            { label: "With Buffer", value: allAccounts.filter((a) => a.is_buffer).length, sub: "Flexible cushion", icon: "🛡️" },
          ].map((s) => (
            <Card key={s.label} padding="sm">
              <div className="flex items-center gap-2 mb-1">
                <span className="text-lg">{s.icon}</span>
                <p className="text-xs text-[var(--muted)]">{s.label}</p>
              </div>
              <p className="text-xl font-bold">{s.money != null ? (hideBalances ? "Rp ••••" : fmtMoney(s.money)) : s.value}</p>
              <p className="text-xs text-[var(--muted)]">{s.sub}</p>
            </Card>
          ))}
        </div>

        {/* Table */}
        <Card padding="sm">
          <div className="flex items-center justify-between mb-3">
            <div className="flex gap-2">
              <input placeholder="Search accounts" value={search} onChange={(e) => setSearch(e.target.value)} className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-xs bg-[var(--surface)] w-48" />
              <select value={profileFilter} onChange={(e) => setProfileFilter(e.target.value)} className="border border-[var(--border)] rounded-lg px-2 py-1.5 text-xs bg-[var(--surface)]">
                <option value="all">All Accounts</option>
                <option value="dynamic_spending">Dynamic Spending</option>
                <option value="fixed_spending">Fixed Spending</option>
                <option value="tabungan">Savings</option>
              </select>
            </div>
            <Button size="sm" variant="primary" onClick={openCreate}>+ Add Account</Button>
          </div>
          <table className="w-full table-fixed text-xs">
            <colgroup>
              <col className="w-[23%]" />
              <col className="w-[14%]" />
              <col className="w-[14%]" />
              <col className="w-[13%]" />
              <col className="w-[15%]" />
              <col className="w-[9%]" />
              <col className="w-[6%]" />
              <col className="w-[6%]" />
            </colgroup>
            <thead>
              <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                <th className="text-left pb-2 font-medium">Account</th>
                <th className="text-left pb-2 font-medium">Profile</th>
                <th className="text-right pb-2 font-medium">Balance</th>
                <th className="text-right pb-2 pr-4 font-medium">Limit</th>
                <th className="text-left pb-2 pl-4 font-medium">Quota</th>
                <th className="text-center pb-2 font-medium">Payroll</th>
                <th className="text-center pb-2 font-medium">Buffer</th>
                <th className="text-right pb-2 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {accounts.map((acc) => {
                const budget = budgetByAcc[acc.account_id];
                const currentBal = currentBalance(acc);
                const showsSpendingBudget = isBudgetedSpendingProfile(acc.profile_type, acc.is_no_limit);
                const spendingLimit = showsSpendingBudget
                  ? (acc.fixed_limit_amount ? acc.fixed_limit_amount : budget?.amount)
                  : null;
                return (
                  <tr key={acc.account_id} onClick={() => setSelected(acc)} className={`hover:bg-[var(--bg)] cursor-pointer transition-colors ${selected?.account_id === acc.account_id ? "bg-primary/5" : ""}`}>
                    <td className="py-2.5">
                      <div className="flex items-center gap-2">
                        <div className="w-7 h-7 rounded-lg bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center text-sm">🏦</div>
                        <div>
                          <p className="font-medium text-[var(--text)]">{acc.account_name}</p>
                          <p className="text-[var(--muted)] capitalize">{acc.profile_type.replace("_", " ")}</p>
                        </div>
                      </div>
                    </td>
                    <td className="py-2.5"><Badge variant="gray">{acc.profile_type.replace("_", " ")}</Badge></td>
                    <td className="py-2.5 text-right tabular font-medium">{bal(currentBal)}</td>
                    <td className="py-2.5 pr-4 text-right tabular">{spendingLimit ? bal(spendingLimit) : <span className="text-[var(--muted)]">—</span>}</td>
                    <td className="py-2.5 pl-4">
                      {showsSpendingBudget && budget ? <ProgressBar value={budget.quotaPct} intent="quota" showLabel /> : <span className="text-[var(--muted)]">—</span>}
                    </td>
                    <td className="py-2.5 text-center">{acc.is_payroll_source ? <span className="text-primary">✓</span> : <span className="text-[var(--muted)]">—</span>}</td>
                    <td className="py-2.5 text-center">{acc.is_buffer ? <span className="text-primary">✓</span> : <span className="text-[var(--muted)]">—</span>}</td>
                    <td className="py-2.5 text-right">
                      <div className="flex gap-1 justify-end">
                        <button onClick={(e) => { e.stopPropagation(); openEdit(acc); }} className="p-1 rounded hover:bg-[var(--bg)] text-[var(--muted)]">✏️</button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              <tr className="font-semibold border-t-2 border-[var(--border)]">
                <td className="py-2.5">Total</td>
                <td /><td className="py-2.5 text-right tabular">{bal(filteredBalance)}</td>
                <td /><td /><td /><td /><td />
              </tr>
            </tbody>
          </table>
          <p className="text-xs text-[var(--muted)] mt-3">Showing 1 to {accounts.length} of {accounts.length} accounts</p>
        </Card>
      </div>

      {/* Detail panel */}
      {selected && (
        <div className="w-72 border-l border-[var(--border)] bg-[var(--surface)] flex flex-col overflow-y-auto">
          <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--border)]">
            <h3 className="font-semibold text-sm">{selected.account_name}</h3>
            <button onClick={() => setSelected(null)} className="text-[var(--muted)]">✕</button>
          </div>
          <div className="p-4 space-y-3 text-xs flex-1">
            <SectionTitle>Account Overview</SectionTitle>
            {[
              ["Profile Type", selected.profile_type.replace("_", " ")],
              ["Current Balance", bal(currentBalance(selected))],
              ["Monthly Spending Limit", isBudgetedSpendingProfile(selected.profile_type, selected.is_no_limit) && budgetByAcc[selected.account_id] ? bal(budgetByAcc[selected.account_id].amount) : "—"],
              ["Payroll Source", selected.is_payroll_source ? "Yes" : "No"],
              ["Buffer Account", selected.is_buffer ? "Yes" : "No"],
              ["Institution", selected.institution ?? "—"],
              ["Account Number", selected.account_number ?? "—"],
            ].map(([k, v]) => (
              <div key={k} className="flex justify-between">
                <span className="text-[var(--muted)]">{k}</span>
                <span className="font-medium capitalize">{v}</span>
              </div>
            ))}
          </div>
          <div className="p-4 border-t border-[var(--border)] space-y-2">
            <Button size="sm" variant="secondary" className="w-full" onClick={() => openEdit(selected)}>✏️ Edit Account</Button>
            <Button size="sm" variant="danger" className="w-full" onClick={() => confirm(`Delete "${selected.account_name}"?`) && deleteMut.mutate(selected.account_id)}>🗑️ Delete Account</Button>
          </div>
        </div>
      )}

      {/* Modal */}
      <Modal open={modal !== null} onClose={() => setModal(null)} title={modal === "create" ? "New Account" : "Edit Account"}>
        <form onSubmit={(e) => { e.preventDefault(); saveMut.mutate(); }} className="space-y-3">
          <Input label="Account Name" value={form.account_name} onChange={(e) => setForm({ ...form, account_name: e.target.value })} required />
          {modal === "create" && <MoneyInput label="Initial Balance" value={form.initial_balance} onChange={(v) => setForm({ ...form, initial_balance: v })} />}
          <Select label="Profile Type" value={form.profile_type} onChange={(e) => setForm({ ...form, profile_type: e.target.value })}>
            <option value="dynamic_spending">Dynamic Spending</option>
            <option value="fixed_spending">Fixed Spending</option>
            <option value="tabungan">Savings</option>
          </Select>
          {isBudgetedSpendingProfile(form.profile_type, form.is_no_limit) ? (
            <MoneyInput label="Monthly Spending Limit" value={form.budget} onChange={(v) => setForm({ ...form, budget: v })} />
          ) : (
            <p className="text-xs text-[var(--muted)]">
              Savings accounts do not use monthly spending limits. Use Buckets or Goals to track savings targets.
            </p>
          )}
          <div className="space-y-2">
            {[["is_payroll_source", "Payroll source"], ["is_buffer", "Buffer account"], ["is_no_limit", "No spending limit"]].map(([k, l]) => (
              <label key={k} className="flex items-center gap-2 text-sm cursor-pointer">
                <input type="checkbox" checked={form[k as keyof typeof form] as boolean} onChange={(e) => setForm({ ...form, [k]: e.target.checked })} />
                {l}
              </label>
            ))}
          </div>
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
