"use client";

import { useState, useMemo, useCallback } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { Icon } from "@/components/ui/Icon";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";

interface GoalItem {
  id: string;
  name: string;
  target_amount: number;
  current_amount: number;
  remaining_amount: number;
  percentage_completed: number;
  target_date: string | null;
  monthly_target_pace: number | null;
  color: string;
  icon: string;
  is_emergency?: boolean;
  is_archived: boolean;
  account_ids?: string[];
  linked_accounts?: Array<{
    id: string;
    name: string;
    type: string;
    balance: number;
  }>;
  created_at: string;
}

interface ObligationItem {
  id: string;
  name: string;
  total_amount: number;
  remaining_amount: number;
  paid_amount: number;
  payoff_percentage: number;
  due_date: string | null;
  minimum_payment: number | null;
  estimated_payoff_months: number | null;
  notes: string | null;
  is_archived: boolean;
  created_at: string;
}

export default function GoalsAndDebtsPage() {
  const qc = useQueryClient();
  const { bal, currency } = useAppCtx();

  // Modals state
  const [goalModalOpen, setGoalModalOpen] = useState(false);
  const [editingGoal, setEditingGoal] = useState<GoalItem | null>(null);
  const [depositGoal, setDepositGoal] = useState<GoalItem | null>(null);

  const [obligationModalOpen, setObligationModalOpen] = useState(false);
  const [editingObligation, setEditingObligation] = useState<ObligationItem | null>(null);
  const [payObligation, setPayObligation] = useState<ObligationItem | null>(null);

  // Goal Form
  const [goalName, setGoalName] = useState("");
  const [goalTarget, setGoalTarget] = useState("");
  const [goalCurrent, setGoalCurrent] = useState("");
  const [goalDate, setGoalDate] = useState("");
  const [goalColor, setGoalColor] = useState("#10b981");
  const [goalIcon, setGoalIcon] = useState("target");
  const [goalIsEmergency, setGoalIsEmergency] = useState(false);
  const [goalAccountIds, setGoalAccountIds] = useState<string[]>([]);
  const [goalError, setGoalError] = useState("");

  // Obligation Form
  const [obName, setObName] = useState("");
  const [obTotal, setObTotal] = useState("");
  const [obRemaining, setObRemaining] = useState("");
  const [obDueDate, setObDueDate] = useState("");
  const [obMinPayment, setObMinPayment] = useState("");
  const [obNotes, setObNotes] = useState("");
  const [obError, setObError] = useState("");

  // Action Form (Deposit / Payoff)
  const [actionAccount, setActionAccount] = useState("");
  const [depositTargetAccount, setDepositTargetAccount] = useState("");
  const [actionAmount, setActionAmount] = useState("");
  const [actionError, setActionError] = useState("");

  // Queries
  const { data: goalsData, isLoading: goalsLoading } = useQuery<{ ok: boolean; goals: GoalItem[]; summary: any }>({
    queryKey: ["goals"],
    queryFn: () => api.get("/goals"),
  });

  const { data: obligationsData, isLoading: obligationsLoading } = useQuery<{
    ok: boolean;
    obligations: ObligationItem[];
    summary: any;
  }>({
    queryKey: ["obligations"],
    queryFn: () => api.get("/obligations"),
  });

  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const topAccounts = useMemo(() => accounts.filter((a: any) => !a.parent_id), [accounts]);

  // Deduplicated map of all accounts & child pockets by ID
  const accountsMap = useMemo(() => {
    const map = new Map<string, any>();
    accounts.forEach((a: any) => {
      map.set(String(a.id), a);
      if (a.children && Array.isArray(a.children)) {
        a.children.forEach((c: any) => map.set(String(c.id), c));
      }
    });
    return map;
  }, [accounts]);

  // Calculates linked balance with hierarchy deduplication:
  // If an account's parent is also selected, omit the child because parent.balance already contains it.
  const calculateGoalLinkedBalance = useCallback(
    (selectedIds: string[]) => {
      const selectedSet = new Set(selectedIds.map(String));
      let total = 0;
      for (const id of selectedSet) {
        const acc = accountsMap.get(id);
        if (!acc) continue;
        if (acc.parent_id && selectedSet.has(String(acc.parent_id))) {
          continue;
        }
        total += Number(acc.balance || 0);
      }
      return total;
    },
    [accountsMap]
  );
  const goals = goalsData?.goals ?? [];
  const activeGoals = goals.filter((g) => !g.is_archived);

  const obligations = obligationsData?.obligations ?? [];
  const activeObligations = obligations.filter((o) => !o.is_archived);
  const obligationsSummary = obligationsData?.summary;

  // Open Create Goal
  const handleOpenNewGoal = () => {
    setEditingGoal(null);
    setGoalName("");
    setGoalTarget("");
    setGoalCurrent("0");
    setGoalDate("");
    setGoalColor("#10b981");
    setGoalIcon("target");
    setGoalIsEmergency(false);
    setGoalAccountIds([]);
    setGoalError("");
    setGoalModalOpen(true);
  };

  // Open Edit Goal
  const handleOpenEditGoal = (g: GoalItem) => {
    setEditingGoal(g);
    setGoalName(g.name);
    setGoalTarget(formatNumberWithDots(g.target_amount));
    setGoalCurrent(formatNumberWithDots(g.current_amount));
    setGoalDate(g.target_date || "");
    setGoalColor(g.color);
    setGoalIcon(g.icon);
    setGoalIsEmergency(Boolean(g.is_emergency));
    setGoalAccountIds(g.account_ids || g.linked_accounts?.map((a) => a.id) || []);
    setGoalError("");
    setGoalModalOpen(true);
  };

  // Open Create Obligation
  const handleOpenNewObligation = () => {
    setEditingObligation(null);
    setObName("");
    setObTotal("");
    setObRemaining("");
    setObDueDate("");
    setObMinPayment("");
    setObNotes("");
    setObError("");
    setObligationModalOpen(true);
  };

  // Open Edit Obligation
  const handleOpenEditObligation = (o: ObligationItem) => {
    setEditingObligation(o);
    setObName(o.name);
    setObTotal(formatNumberWithDots(o.total_amount));
    setObRemaining(formatNumberWithDots(o.remaining_amount));
    setObDueDate(o.due_date || "");
    setObMinPayment(o.minimum_payment ? formatNumberWithDots(o.minimum_payment) : "");
    setObNotes(o.notes || "");
    setObError("");
    setObligationModalOpen(true);
  };

  // Mutations
  const saveGoalMutation = useMutation({
    mutationFn: async () => {
      const name = goalName.trim();
      if (!name) throw new Error("Nama target wajib diisi");
      const target = parseNumberFromDots(goalTarget);
      if (target <= 0) throw new Error("Target nominal harus lebih dari 0");
      const current = parseNumberFromDots(goalCurrent);

      const payload = {
        name,
        target_amount: target,
        current_amount: goalAccountIds.length > 0 ? 0 : current,
        target_date: goalDate || (editingGoal ? "" : null),
        color: goalColor,
        icon: goalIcon,
        is_emergency: goalIsEmergency,
        account_ids: goalAccountIds,
      };

      if (editingGoal) {
        return api.patch(`/goals/${editingGoal.id}`, payload);
      } else {
        return api.post("/goals", payload);
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setGoalModalOpen(false);
    },
    onError: (err: any) => {
      setGoalError(err?.message || "Operasi gagal");
    },
  });

  const saveObligationMutation = useMutation({
    mutationFn: async () => {
      const name = obName.trim();
      if (!name) throw new Error("Nama tagihan/utang wajib diisi");
      const total = parseNumberFromDots(obTotal);
      if (total <= 0) throw new Error("Total nominal harus lebih dari 0");
      const remaining = obRemaining ? parseNumberFromDots(obRemaining) : total;
      const minPay = obMinPayment ? parseNumberFromDots(obMinPayment) : null;

      if (editingObligation) {
        return api.patch(`/obligations/${editingObligation.id}`, {
          name,
          total_amount: total,
          remaining_amount: remaining,
          due_date: obDueDate || "",
          minimum_payment: minPay,
          notes: obNotes.trim() || null,
        });
      } else {
        return api.post("/obligations", {
          name,
          total_amount: total,
          remaining_amount: remaining,
          due_date: obDueDate || null,
          minimum_payment: minPay,
          notes: obNotes.trim() || null,
        });
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["obligations"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setObligationModalOpen(false);
    },
    onError: (err: any) => {
      setObError(err?.message || "Operasi gagal");
    },
  });

  const archiveGoalMutation = useMutation({
    mutationFn: (id: string) => api.del(`/goals/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
    },
  });

  const archiveObligationMutation = useMutation({
    mutationFn: (id: string) => api.del(`/obligations/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["obligations"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
    },
  });

  // Deposit into Goal Mutation
  const depositGoalMutation = useMutation({
    mutationFn: async () => {
      if (!depositGoal) return;
      const amt = parseNumberFromDots(actionAmount);
      if (amt <= 0) throw new Error("Masukkan nominal yang valid");
      const accId = actionAccount || accounts[0]?.id;
      if (!accId) throw new Error("Pilih rekening sumber");

      const hasLinked = depositGoal.linked_accounts && depositGoal.linked_accounts.length > 0;
      const targetAccId = hasLinked
        ? (depositTargetAccount || depositGoal.linked_accounts![0].id)
        : null;

      if (hasLinked && accId === targetAccId) {
        throw new Error("Rekening sumber dan rekening tujuan tidak boleh sama");
      }

      return api.post("/transactions", {
        account_id: accId,
        transfer_target_account_id: targetAccId,
        type: "transfer",
        amount: amt,
        goal_id: depositGoal.id,
        notes: `Setor tabungan: ${depositGoal.name}`,
        date: new Date().toISOString(),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      setDepositGoal(null);
    },
    onError: (err: any) => setActionError(err?.message || "Setor tabungan gagal"),
  });

  // Payoff Obligation Mutation
  const payObligationMutation = useMutation({
    mutationFn: async () => {
      if (!payObligation) return;
      const amt = parseNumberFromDots(actionAmount);
      if (amt <= 0) throw new Error("Masukkan nominal yang valid");
      const accId = actionAccount || accounts[0]?.id;
      if (!accId) throw new Error("Pilih rekening sumber");

      return api.post("/transactions", {
        account_id: accId,
        type: "expense",
        amount: amt,
        obligation_id: payObligation.id,
        notes: `Bayar tagihan: ${payObligation.name}`,
        date: new Date().toISOString(),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["obligations"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setPayObligation(null);
    },
    onError: (err: any) => setActionError(err?.message || "Pembayaran gagal"),
  });

  if (goalsLoading || obligationsLoading) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-16 rounded-2xl bg-[var(--border)]/30" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="h-44 rounded-3xl bg-[var(--border)]/30" />
          <div className="h-44 rounded-3xl bg-[var(--border)]/30" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* 1. Header & Actions */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-[var(--border)]">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
              Target Tabungan & Rencana Pelunasan
            </span>
          </div>
          <h1 className="text-2xl font-extrabold tracking-tight text-[var(--text)] mt-1">
            Target & Tagihan
          </h1>
        </div>

        <div className="flex items-center gap-2.5 self-start sm:self-auto">
          <button
            type="button"
            onClick={handleOpenNewGoal}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-btn bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-semibold shadow-2xs transition-all active:scale-95"
          >
            <span className="font-bold text-sm leading-none">+</span>
            <span>Target Baru</span>
          </button>

          <button
            type="button"
            onClick={handleOpenNewObligation}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-btn bg-rose-600 hover:bg-rose-700 text-white text-xs font-semibold shadow-2xs transition-all active:scale-95"
          >
            <span className="font-bold text-sm leading-none">+</span>
            <span>Tagihan Baru</span>
          </button>
        </div>
      </div>

      {/* 3. Savings Goals Cards Grid */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-bold tracking-tight text-[var(--text)]">Target Tabungan</h2>
            <p className="text-xs text-[var(--muted)]">
              Kumpulkan dana darurat, liburan, dan tabungan impian Anda
            </p>
          </div>
          <button
            type="button"
            onClick={handleOpenNewGoal}
            className="text-xs font-semibold text-emerald-500 hover:underline"
          >
            + Tambah Target
          </button>
        </div>

        {activeGoals.length === 0 ? (
          <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-8 text-center text-xs text-[var(--muted)] space-y-2">
            <p>Belum ada target tabungan aktif.</p>
            <button
              type="button"
              onClick={handleOpenNewGoal}
              className="px-4 py-2 rounded-xl bg-emerald-600 text-white font-semibold hover:bg-emerald-700"
            >
              Buat Target Pertama Anda
            </button>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {activeGoals.map((g) => (
              <div
                key={g.id}
                className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs flex flex-col justify-between group hover:border-[var(--border-strong)] transition-all"
              >
                <div>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2.5">
                      <span
                        className="h-3 w-3 rounded-full shrink-0"
                        style={{ backgroundColor: g.color || "#10b981" }}
                      />
                      <h3 className="font-bold text-sm text-[var(--text)] truncate max-w-[170px]">
                        {g.name}
                      </h3>
                      {g.is_emergency && (
                        <span className="px-1.5 py-0.5 rounded text-[9px] font-bold uppercase bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 border border-emerald-500/30 shrink-0">
                          🛡️ Dana Darurat
                        </span>
                      )}
                    </div>

                    <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        type="button"
                        onClick={() => handleOpenEditGoal(g)}
                        className="p-1 rounded hover:bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)]"
                      >
                        <Icon name="edit" className="h-3.5 w-3.5" />
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          if (confirm(`Arsipkan target "${g.name}"?`)) {
                            archiveGoalMutation.mutate(g.id);
                          }
                        }}
                        className="p-1 rounded hover:bg-rose-500/10 text-[var(--muted)] hover:text-rose-500"
                      >
                        <Icon name="trash" className="h-3.5 w-3.5" />
                      </button>
                    </div>
                  </div>

                  {/* Amounts */}
                  <div className="mt-4 flex items-baseline justify-between">
                    <span className="text-xl sm:text-2xl font-bold tabular tracking-tight text-emerald-500 select-all">
                      {bal(g.current_amount)}
                    </span>
                    <span className="text-xs text-[var(--muted)] tabular">
                      dari {bal(g.target_amount)}
                    </span>
                  </div>

                  {/* Progress Bar */}
                  <div className="w-full h-2 rounded-full bg-[var(--border)]/60 overflow-hidden mt-2">
                    <div
                      style={{
                        width: `${Math.min(g.percentage_completed, 100)}%`,
                        backgroundColor: g.color || "#10b981",
                      }}
                      className="h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
                    />
                  </div>

                  {/* Pace & Deadline metadata */}
                  <div className="mt-3 flex items-center justify-between text-[11px] text-[var(--muted)]">
                    <span>{g.percentage_completed}% terkumpul</span>
                    {g.target_date && <span>Target: {g.target_date}</span>}
                  </div>

                  {/* Linked Accounts Chips */}
                  {g.linked_accounts && g.linked_accounts.length > 0 && (
                    <div className="mt-3 pt-2.5 border-t border-[var(--border)]/50">
                      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--muted)] mb-1.5 flex items-center gap-1">
                        <span>🔗 Rekening Terhubung ({g.linked_accounts.length})</span>
                      </div>
                      <div className="flex flex-wrap gap-1.5">
                        {g.linked_accounts.map((la) => (
                          <div
                            key={la.id}
                            className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md bg-[var(--surface-raised)] border border-[var(--border)] text-[10px]"
                          >
                            <span className="text-[var(--text)] font-medium">{la.name}:</span>
                            <span className="font-semibold text-emerald-500 tabular">{bal(la.balance)}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>

                {/* Bottom Card Action */}
                <div className="pt-3 border-t border-[var(--border)] flex items-center justify-between text-xs">
                  {g.monthly_target_pace ? (
                    <span className="text-[11px] text-[var(--text-secondary)] font-medium">
                      Nabung: {bal(g.monthly_target_pace)}/bln
                    </span>
                  ) : (
                    <span className="text-[11px] text-[var(--muted)]">Tanpa tenggat waktu</span>
                  )}

                  <button
                    type="button"
                    onClick={() => {
                      setDepositGoal(g);
                      setActionAccount(accounts[0]?.id || "");
                      setDepositTargetAccount(g.linked_accounts?.[0]?.id || "");
                      setActionAmount("");
                      setActionError("");
                    }}
                    className="flex items-center gap-1 font-semibold text-emerald-500 hover:text-emerald-400"
                  >
                    <span>+ Tambah Tabungan</span>
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* 4. Debts & Obligations Payoff Workbench */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-bold tracking-tight text-[var(--text)]">
              Tagihan & Utang
            </h2>
            <p className="text-xs text-[var(--muted)]">
              Pantau progres pelunasan utang, cicilan, dan tagihan rutin
            </p>
          </div>
          <button
            type="button"
            onClick={handleOpenNewObligation}
            className="text-xs font-semibold text-rose-500 hover:underline"
          >
            + Tambah Tagihan
          </button>
        </div>

        {activeObligations.length === 0 ? (
          <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-8 text-center text-xs text-[var(--muted)] space-y-2">
            <p>Tidak ada tagihan atau utang aktif. Anda bebas utang!</p>
          </div>
        ) : (
          <div className="space-y-4">
            {/* Debt Payoff Summary Banner */}
            <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-3 shadow-xs">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="h-7 w-7 rounded-xl bg-rose-500/10 text-rose-500 flex items-center justify-center text-xs font-bold">
                    ⚡
                  </div>
                  <span className="text-xs font-bold uppercase tracking-wider text-[var(--text)]">
                    Progres Pelunasan Utang
                  </span>
                </div>
                <span className="text-xs font-bold tabular text-rose-500">
                  {obligationsSummary?.overall_payoff_percentage ?? 0}% Terbayar
                </span>
              </div>

              <div className="flex items-baseline justify-between pt-1">
                <div>
                  <div className="text-2xl sm:text-3xl font-bold tabular tracking-tight text-rose-500 select-all">
                    {bal(obligationsSummary?.total_remaining ?? 0)}
                  </div>
                  <div className="text-[11px] text-[var(--muted)] mt-0.5">
                    Sisa dari total tagihan {bal(obligationsSummary?.total_debt ?? 0)}
                  </div>
                </div>
                <div className="text-right text-xs text-[var(--muted)]">
                  <div>{bal(obligationsSummary?.total_paid ?? 0)} terbayar</div>
                  <div className="text-[10px] text-[var(--text-secondary)]">
                    Cicilan Min: {bal(obligationsSummary?.total_minimum_monthly ?? 0)}/bln
                  </div>
                </div>
              </div>

              <div className="w-full h-2 rounded-full bg-[var(--border)]/60 overflow-hidden">
                <div
                  style={{
                    width: `${Math.min(obligationsSummary?.overall_payoff_percentage ?? 0, 100)}%`,
                  }}
                  className="h-full rounded-full bg-rose-500 transition-all"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {activeObligations.map((o) => (
              <div
                key={o.id}
                className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs flex flex-col justify-between group hover:border-[var(--border-strong)] transition-all"
              >
                <div>
                  <div className="flex items-center justify-between">
                    <h3 className="font-bold text-sm text-[var(--text)] truncate max-w-[180px]">
                      {o.name}
                    </h3>

                    <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        type="button"
                        onClick={() => handleOpenEditObligation(o)}
                        className="p-1 rounded hover:bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)]"
                      >
                        <Icon name="edit" className="h-3.5 w-3.5" />
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          if (confirm(`Arsipkan tagihan "${o.name}"?`)) {
                            archiveObligationMutation.mutate(o.id);
                          }
                        }}
                        className="p-1 rounded hover:bg-rose-500/10 text-[var(--muted)] hover:text-rose-500"
                      >
                        <Icon name="trash" className="h-3.5 w-3.5" />
                      </button>
                    </div>
                  </div>

                  {/* Amounts */}
                  <div className="mt-4 flex items-baseline justify-between">
                    <span className="text-xl sm:text-2xl font-bold tabular tracking-tight text-rose-500 select-all">
                      {bal(o.remaining_amount)}
                    </span>
                    <span className="text-xs text-[var(--muted)] tabular">
                      dari {bal(o.total_amount)}
                    </span>
                  </div>

                  {/* Progress Bar */}
                  <div className="w-full h-2 rounded-full bg-[var(--border)]/60 overflow-hidden mt-2">
                    <div
                      style={{ width: `${Math.min(o.payoff_percentage, 100)}%` }}
                      className="h-full rounded-full bg-rose-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
                    />
                  </div>

                  {/* Payoff metadata */}
                  <div className="mt-3 flex items-center justify-between text-[11px] text-[var(--muted)]">
                    <span>{o.payoff_percentage}% terbayar</span>
                    {o.estimated_payoff_months && (
                      <span>~{o.estimated_payoff_months} bln lagi</span>
                    )}
                  </div>
                </div>

                {/* Bottom Card Action */}
                <div className="pt-3 border-t border-[var(--border)] flex items-center justify-between text-xs">
                  {o.minimum_payment ? (
                    <span className="text-[11px] text-[var(--text-secondary)] font-medium">
                      Cicilan: {bal(o.minimum_payment)}/bln
                    </span>
                  ) : (
                    <span className="text-[11px] text-[var(--muted)]">Tanpa cicilan minimum</span>
                  )}

                  <button
                    type="button"
                    onClick={() => {
                      setPayObligation(o);
                      setActionAmount(o.minimum_payment ? formatNumberWithDots(o.minimum_payment) : "");
                      setActionError("");
                    }}
                    className="flex items-center gap-1 font-semibold text-rose-500 hover:text-rose-400"
                  >
                    <span>Bayar Tagihan &rarr;</span>
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>

      {/* Goal Create/Edit Modal */}
      <Modal
        open={goalModalOpen}
        onClose={() => setGoalModalOpen(false)}
        title={editingGoal ? "Ubah Target Tabungan" : "Buat Target Tabungan Baru"}
      >
        <div className="space-y-4 pt-2 text-xs">
          {goalError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
              {goalError}
            </div>
          )}

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Nama Target</label>
            <input
              type="text"
              value={goalName}
              onChange={(e) => setGoalName(e.target.value)}
              placeholder="Contoh: Dana Darurat, Laptop Baru"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="font-medium text-[var(--muted)]">Target Nominal</label>
              {currency === "USD" && parseNumberFromDots(goalTarget) > 0 && (
                <span className="text-[11px] font-bold text-emerald-500 tabular">
                  ≈ {bal(parseNumberFromDots(goalTarget))}
                </span>
              )}
            </div>
            <input
              type="text"
              inputMode="numeric"
              value={goalTarget}
              onChange={(e) => setGoalTarget(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 20.000.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] font-bold tabular"
            />
          </div>

          {/* Linked Accounts Checklist */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="font-medium text-[var(--muted)] block">Hubungkan Rekening / Kantong (Opsional)</label>
              <span className="text-[11px] text-[var(--muted)] font-medium">{goalAccountIds.length} dipilih</span>
            </div>
            <p className="text-[11px] text-[var(--muted)] mb-2 leading-relaxed">
              Pilih satu atau beberapa rekening (misal RDPU Bibit, Deposito, Emas). Saldo target akan otomatis mengikuti akumulasi saldo rekening yang dipilih secara real-time.
            </p>
            <div className="space-y-1.5 max-h-52 overflow-y-auto rounded-xl border border-[var(--border)] p-2 bg-[var(--surface-raised)]/50">
              {topAccounts.length === 0 ? (
                <p className="text-[11px] text-[var(--muted)] p-2">Belum ada rekening aktif.</p>
              ) : (
                topAccounts.map((acc: any) => {
                  const childAccounts = (acc.children || []).map((c: any) => ({
                    ...c,
                    id: String(c.id),
                  }));
                  const hasChildren = childAccounts.length > 0;
                  const accId = String(acc.id);
                  const childIds = childAccounts.map((c: any) => c.id);

                  // An account is checked if explicitly in goalAccountIds OR if all its children are checked
                  const isChecked =
                    goalAccountIds.includes(accId) ||
                    (hasChildren && childIds.length > 0 && childIds.every((cid: string) => goalAccountIds.includes(cid)));

                  const handleParentToggle = (checked: boolean) => {
                    if (checked) {
                      setGoalAccountIds((prev) => {
                        const set = new Set(prev);
                        set.add(accId);
                        childIds.forEach((cid: string) => set.add(cid));
                        return Array.from(set);
                      });
                    } else {
                      setGoalAccountIds((prev) => {
                        const set = new Set(prev);
                        set.delete(accId);
                        childIds.forEach((cid: string) => set.delete(cid));
                        return Array.from(set);
                      });
                    }
                  };

                  const handleChildToggle = (childId: string, checked: boolean) => {
                    setGoalAccountIds((prev) => {
                      const set = new Set(prev);
                      if (checked) {
                        set.add(childId);
                        if (childIds.every((cid: string) => set.has(cid))) {
                          set.add(accId);
                        }
                      } else {
                        // If parent was in set, expand parent into other child pockets first
                        if (set.has(accId)) {
                          set.delete(accId);
                          childIds.forEach((cid: string) => {
                            if (cid !== childId) set.add(cid);
                          });
                        }
                        set.delete(childId);
                      }
                      return Array.from(set);
                    });
                  };

                  return (
                    <div key={acc.id} className="space-y-1">
                      <label
                        className={cn(
                          "flex items-center justify-between p-2 rounded-lg cursor-pointer transition-colors text-xs border",
                          isChecked
                            ? "bg-emerald-500/10 border-emerald-500/30 text-[var(--text)]"
                            : "hover:bg-[var(--surface-raised)] border-transparent text-[var(--text-secondary)]"
                        )}
                      >
                        <div className="flex items-center gap-2.5">
                          <input
                            type="checkbox"
                            checked={isChecked}
                            onChange={(e) => handleParentToggle(e.target.checked)}
                            className="rounded border-[var(--border)] text-emerald-600 focus:ring-emerald-500 h-4 w-4"
                          />
                          <div>
                            <div className="font-semibold text-[var(--text)] flex items-center gap-1.5">
                              <span>{acc.name}</span>
                              {hasChildren && (
                                <span className="text-[9px] px-1.5 py-0.5 rounded bg-blue-500/20 text-blue-400 font-medium">
                                  Akun Induk
                                </span>
                              )}
                            </div>
                            <div className="text-[10px] text-[var(--muted)] capitalize">{acc.type}</div>
                          </div>
                        </div>
                        <span className="font-bold text-xs tabular text-[var(--text)]">
                          {bal(acc.balance)}
                        </span>
                      </label>

                      {/* Child pockets indented */}
                      {hasChildren &&
                        childAccounts.map((child: any) => {
                          const isChildChecked =
                            goalAccountIds.includes(child.id) || goalAccountIds.includes(accId);
                          return (
                            <label
                              key={child.id}
                              className={cn(
                                "flex items-center justify-between p-2 pl-4 ml-4 rounded-lg cursor-pointer transition-colors text-xs border border-l-2",
                                isChildChecked
                                  ? "bg-emerald-500/10 border-emerald-500/30 text-[var(--text)]"
                                  : "hover:bg-[var(--surface-raised)] border-transparent border-l-[var(--border)] text-[var(--text-secondary)]"
                              )}
                            >
                              <div className="flex items-center gap-2.5">
                                <input
                                  type="checkbox"
                                  checked={isChildChecked}
                                  onChange={(e) => handleChildToggle(child.id, e.target.checked)}
                                  className="rounded border-[var(--border)] text-emerald-600 focus:ring-emerald-500 h-4 w-4"
                                />
                                <div>
                                  <div className="font-medium text-[var(--text)] flex items-center gap-1">
                                    <span className="text-[var(--muted)]">↳</span>
                                    <span>{child.name}</span>
                                  </div>
                                  <div className="text-[10px] text-[var(--muted)]">Kantong</div>
                                </div>
                              </div>
                              <span className="font-bold text-xs tabular text-emerald-500">
                                {bal(child.balance)}
                              </span>
                            </label>
                          );
                        })}
                    </div>
                  );
                })
              )}
            </div>
          </div>

          {/* If accounts linked: show live total banner. If unlinked: show manual starting balance input */}
          {goalAccountIds.length > 0 ? (
            <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-3 flex items-center justify-between">
              <div>
                <div className="text-[10px] uppercase font-bold text-emerald-600 dark:text-emerald-400">
                  Total Saldo Terhubung
                </div>
                <div className="text-base font-bold tabular text-emerald-600 dark:text-emerald-400">
                  {bal(calculateGoalLinkedBalance(goalAccountIds))}
                </div>
              </div>
              <div className="text-right text-[11px] text-[var(--muted)] font-medium">
                {parseNumberFromDots(goalTarget) > 0 && (
                  <span>
                    {Math.min(
                      100,
                      Math.round(
                        (calculateGoalLinkedBalance(goalAccountIds) /
                          parseNumberFromDots(goalTarget)) *
                          100
                      )
                    )}
                    % dari target
                  </span>
                )}
              </div>
            </div>
          ) : (
            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="font-medium text-[var(--muted)]">Nominal Awal Terkumpul</label>
                {currency === "USD" && parseNumberFromDots(goalCurrent) > 0 && (
                  <span className="text-[11px] font-bold text-emerald-500 tabular">
                    ≈ {bal(parseNumberFromDots(goalCurrent))}
                  </span>
                )}
              </div>
              <input
                type="text"
                inputMode="numeric"
                value={goalCurrent}
                onChange={(e) => setGoalCurrent(formatNumberWithDots(e.target.value))}
                placeholder="0"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] font-bold tabular"
              />
            </div>
          )}

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Target Tanggal Selesai (Opsional)</label>
            <input
              type="date"
              value={goalDate}
              onChange={(e) => setGoalDate(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Warna Label</label>
            <div className="flex items-center gap-2">
              {["#10b981", "#3b82f6", "#f59e0b", "#8b5cf6", "#ec4899"].map((c) => (
                <button
                  key={c}
                  type="button"
                  onClick={() => setGoalColor(c)}
                  className={cn(
                    "h-6 w-6 rounded-full border-2 transition-transform",
                    goalColor === c ? "scale-110 border-white" : "border-transparent"
                  )}
                  style={{ backgroundColor: c }}
                />
              ))}
            </div>
          </div>

          {/* Dana Darurat Toggle */}
          <div className="p-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/60 space-y-1">
            <label className="flex items-center gap-2.5 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={goalIsEmergency}
                onChange={(e) => setGoalIsEmergency(e.target.checked)}
                className="rounded border-[var(--border)] text-emerald-600 focus:ring-emerald-500 h-4 w-4"
              />
              <span className="font-semibold text-xs text-[var(--text)] flex items-center gap-1.5">
                <span>🛡️ Tandai sebagai Dana Darurat (Emergency Fund)</span>
              </span>
            </label>
            <p className="text-[11px] text-[var(--muted)] pl-6.5 leading-relaxed">
              Saldo rekening/kantong yang terhubung ke target ini akan dihitung secara langsung sebagai cadangan <strong>Ketahanan Dana</strong> pada ringkasan keuangan.
            </p>
          </div>

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setGoalModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="button"
              disabled={saveGoalMutation.isPending}
              onClick={() => saveGoalMutation.mutate()}
              className="flex-1 rounded-xl bg-emerald-600 text-white py-2 text-xs font-semibold hover:bg-emerald-700 disabled:opacity-50"
            >
              {saveGoalMutation.isPending ? "Menyimpan..." : editingGoal ? "Simpan Perubahan" : "Buat Target"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Obligation Create/Edit Modal */}
      <Modal
        open={obligationModalOpen}
        onClose={() => setObligationModalOpen(false)}
        title={editingObligation ? "Ubah Tagihan" : "Tambah Tagihan / Utang Baru"}
      >
        <div className="space-y-4 pt-2 text-xs">
          {obError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
              {obError}
            </div>
          )}

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Nama Tagihan / Utang</label>
            <input
              type="text"
              value={obName}
              onChange={(e) => setObName(e.target.value)}
              placeholder="Contoh: Cicilan Motor, Kartu Kredit"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Total Tagihan / Pinjaman (IDR)</label>
            <input
              type="text"
              inputMode="numeric"
              value={obTotal}
              onChange={(e) => setObTotal(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 15.000.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] font-bold tabular"
            />
          </div>

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Sisa Tagihan Saat Ini (IDR)</label>
            <input
              type="text"
              inputMode="numeric"
              value={obRemaining}
              onChange={(e) => setObRemaining(formatNumberWithDots(e.target.value))}
              placeholder="Kosongkan jika sama dengan total"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular"
            />
          </div>

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Cicilan Minimal per Bulan (Opsional)</label>
            <input
              type="text"
              inputMode="numeric"
              value={obMinPayment}
              onChange={(e) => setObMinPayment(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 1.000.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular"
            />
          </div>

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">Jatuh Tempo (Opsional)</label>
            <input
              type="date"
              value={obDueDate}
              onChange={(e) => setObDueDate(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setObligationModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="button"
              disabled={saveObligationMutation.isPending}
              onClick={() => saveObligationMutation.mutate()}
              className="flex-1 rounded-xl bg-rose-600 text-white py-2 text-xs font-semibold hover:bg-rose-700 disabled:opacity-50"
            >
              {saveObligationMutation.isPending ? "Menyimpan..." : editingObligation ? "Simpan Perubahan" : "Tambah Tagihan"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Goal Deposit Modal */}
      {depositGoal && (
        <Modal
          open={Boolean(depositGoal)}
          onClose={() => setDepositGoal(null)}
          title={`Setor Tabungan: ${depositGoal.name}`}
        >
          <div className="space-y-4 pt-2 text-xs">
            {actionError && (
              <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
                {actionError}
              </div>
            )}

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">Dari Rekening / Dompet</label>
              <select
                value={actionAccount}
                onChange={(e) => setActionAccount(e.target.value)}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              >
                <AccountSelectOptions
                  accounts={accounts}
                  formatBalance={bal}
                  allowParentSelection={true}
                  excludeAccountId={depositTargetAccount}
                />
              </select>
            </div>

            {depositGoal.linked_accounts && depositGoal.linked_accounts.length > 0 && (
              <div>
                <label className="font-medium text-[var(--muted)] block mb-1">Ke Rekening Simpanan (Tujuan)</label>
                <select
                  value={depositTargetAccount}
                  onChange={(e) => setDepositTargetAccount(e.target.value)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <AccountSelectOptions
                    accounts={depositGoal.linked_accounts}
                    formatBalance={bal}
                    allowParentSelection={true}
                    excludeAccountId={actionAccount}
                  />
                </select>
              </div>
            )}

            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="font-medium text-[var(--muted)]">Nominal Setoran</label>
                {currency === "USD" && parseNumberFromDots(actionAmount) > 0 && (
                  <span className="text-[11px] font-bold text-emerald-500 tabular">
                    ≈ {bal(parseNumberFromDots(actionAmount))}
                  </span>
                )}
              </div>
              <input
                type="text"
                inputMode="numeric"
                value={actionAmount}
                onChange={(e) => setActionAmount(formatNumberWithDots(e.target.value))}
                placeholder="Contoh: 500.000"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setDepositGoal(null)}
                className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={depositGoalMutation.isPending}
                onClick={() => depositGoalMutation.mutate()}
                className="flex-1 rounded-xl bg-emerald-600 text-white py-2 text-xs font-semibold hover:bg-emerald-700 disabled:opacity-50"
              >
                {depositGoalMutation.isPending ? "Memproses..." : "Setor Sekarang"}
              </button>
            </div>
          </div>
        </Modal>
      )}

      {/* Pay Obligation Modal */}
      {payObligation && (
        <Modal
          open={Boolean(payObligation)}
          onClose={() => setPayObligation(null)}
          title={`Bayar Tagihan: ${payObligation.name}`}
        >
          <div className="space-y-4 pt-2 text-xs">
            {actionError && (
              <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
                {actionError}
              </div>
            )}

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">Bayar dari Rekening / Dompet</label>
              <select
                value={actionAccount}
                onChange={(e) => setActionAccount(e.target.value)}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              >
                <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} />
              </select>
            </div>

            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="font-medium text-[var(--muted)]">Nominal Pembayaran</label>
                {currency === "USD" && parseNumberFromDots(actionAmount) > 0 && (
                  <span className="text-[11px] font-bold text-emerald-500 tabular">
                    ≈ {bal(parseNumberFromDots(actionAmount))}
                  </span>
                )}
              </div>
              <input
                type="text"
                inputMode="numeric"
                value={actionAmount}
                onChange={(e) => setActionAmount(formatNumberWithDots(e.target.value))}
                placeholder="Contoh: 1.000.000"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setPayObligation(null)}
                className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={payObligationMutation.isPending}
                onClick={() => payObligationMutation.mutate()}
                className="flex-1 rounded-xl bg-rose-600 text-white py-2 text-xs font-semibold hover:bg-rose-700 disabled:opacity-50"
              >
                {payObligationMutation.isPending ? "Memproses..." : "Catat Pembayaran"}
              </button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  );
}
