"use client";

import { useState, useMemo, useCallback, useRef } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { FormSection } from "@/components/ui/FormField";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
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

const GOAL_COLORS = [
  { value: "#10b981", label: "Hijau" },
  { value: "#3b82f6", label: "Biru" },
  { value: "#f59e0b", label: "Amber" },
  { value: "#8b5cf6", label: "Ungu" },
  { value: "#ec4899", label: "Merah muda" },
];

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
  const [goalBackingConfirmed, setGoalBackingConfirmed] = useState(false);
  const [goalError, setGoalError] = useState("");

  // Obligation Form
  const [obName, setObName] = useState("");
  const [obTotal, setObTotal] = useState("");
  const [obRemaining, setObRemaining] = useState("");
  const [obDueDate, setObDueDate] = useState("");
  const [obMinPayment, setObMinPayment] = useState("");
  const [obNotes, setObNotes] = useState("");
  const [obError, setObError] = useState("");
  const obNameRef = useRef<HTMLInputElement>(null);
  const obTotalRef = useRef<HTMLInputElement>(null);
  const obRemainingRef = useRef<HTMLInputElement>(null);
  const obDueDateRef = useRef<HTMLInputElement>(null);

  // Action Form (Deposit / Payoff)
  const [actionAccount, setActionAccount] = useState("");
  const [actionAmount, setActionAmount] = useState("");
  const [actionError, setActionError] = useState("");

  // Queries
  const { data: goalsData, isLoading: goalsLoading } = useQuery<{ ok: boolean; goals: GoalItem[]; summary: any }>({
    queryKey: queryKeys.goals,
    queryFn: () => api.get("/goals"),
  });

  const { data: obligationsData, isLoading: obligationsLoading } = useQuery<{
    ok: boolean;
    obligations: ObligationItem[];
    summary: any;
  }>({
    queryKey: queryKeys.obligations,
    queryFn: () => api.get("/obligations"),
  });

  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const topAccounts = useMemo(() => accounts.filter((a: any) => !a.parent_id), [accounts]);
  const obligationTotalValue = parseNumberFromDots(obTotal);
  const obligationRemainingValue = obRemaining.trim()
    ? parseNumberFromDots(obRemaining)
    : obligationTotalValue;
  const remainingExceedsTotal =
    Boolean(obRemaining.trim()) &&
    obligationTotalValue > 0 &&
    obligationRemainingValue > obligationTotalValue;

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
  const startingGoalAccountIds = editingGoal
    ? editingGoal.account_ids ?? editingGoal.linked_accounts?.map((account) => account.id) ?? []
    : [];
  const goalBackingChanged =
    [...goalAccountIds].map(String).sort().join(",") !==
    [...startingGoalAccountIds].map(String).sort().join(",");

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
    setGoalBackingConfirmed(false);
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
    setGoalBackingConfirmed(false);
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
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setGoalModalOpen(false);
    },
    onError: (err: any) => {
      setGoalError(err?.message || "Operasi gagal");
    },
  });

  const handleSaveGoal = () => {
    if (goalBackingChanged && !goalBackingConfirmed) {
      setGoalBackingConfirmed(true);
      return;
    }
    saveGoalMutation.mutate();
  };

  const updateGoalAccountIds = (update: (current: string[]) => string[]) => {
    setGoalBackingConfirmed(false);
    setGoalAccountIds(update);
  };

  const saveObligationMutation = useMutation({
    mutationFn: async () => {
      const name = obName.trim();
      if (!name) throw new Error("Nama tagihan/utang wajib diisi");
      const total = parseNumberFromDots(obTotal);
      if (total <= 0) throw new Error("Total nominal harus lebih dari 0");
      const remaining = obRemaining ? parseNumberFromDots(obRemaining) : total;
      const minPay = obMinPayment ? parseNumberFromDots(obMinPayment) : null;
      if (remaining < 0) throw new Error("Sisa tagihan tidak boleh negatif");
      if (remaining > total) throw new Error("Sisa tagihan tidak boleh melebihi total tagihan");

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
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setObligationModalOpen(false);
    },
    onError: (err: any) => {
      setObError(err?.message || "Operasi gagal");
      const message = String(err?.message || "").toLowerCase();
      if (message.includes("sisa") || message.includes("remaining")) obRemainingRef.current?.focus();
      else if (message.includes("total")) obTotalRef.current?.focus();
      else if (message.includes("tempo") || message.includes("date")) obDueDateRef.current?.focus();
      else obNameRef.current?.focus();
    },
  });

  const archiveGoalMutation = useMutation({
    mutationFn: (id: string) => api.del(`/goals/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    },
  });

  const archiveObligationMutation = useMutation({
    mutationFn: (id: string) => api.del(`/obligations/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    },
  });

  // Standalone goals are adjusted as progress records, not financial transactions.
  const depositGoalMutation = useMutation({
    mutationFn: async () => {
      if (!depositGoal) return;
      const amt = parseNumberFromDots(actionAmount);
      if (amt <= 0) throw new Error("Masukkan nominal yang valid");
      if (depositGoal.linked_accounts?.length) {
        throw new Error("Progres target ini mengikuti saldo rekening terhubung");
      }
      return api.post(`/goals/${depositGoal.id}/adjust`, { amount: amt });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setDepositGoal(null);
    },
    onError: (err: any) => setActionError(err?.message || "Gagal mencatat progres target"),
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
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
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
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-btn bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-semibold shadow-2xs transition-[background-color,transform] active:scale-95"
          >
            <span className="font-bold text-sm leading-none">+</span>
            <span>Target Baru</span>
          </button>

          <button
            type="button"
            onClick={handleOpenNewObligation}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-btn bg-rose-600 hover:bg-rose-700 text-white text-xs font-semibold shadow-2xs transition-[background-color,transform] active:scale-95"
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
                className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs flex flex-col justify-between group hover:border-[var(--border-strong)] transition-colors"
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

                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        onClick={() => handleOpenEditGoal(g)}
                        aria-label={`Ubah target ${g.name}`}
                        className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl text-[var(--muted)] hover:bg-[var(--surface-raised)] hover:text-[var(--text)]"
                      >
                        <Icon name="edit" className="h-3.5 w-3.5" />
                      </button>
                      <ConfirmActionButton
                        label={`Arsipkan target ${g.name}`}
                        confirmation={`Arsipkan target "${g.name}"?`}
                        onConfirm={() => archiveGoalMutation.mutate(g.id)}
                        className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl text-[var(--muted)] hover:bg-rose-500/10 hover:text-rose-600"
                      >
                        <Icon name="trash" className="h-3.5 w-3.5" />
                      </ConfirmActionButton>
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
                      className="h-full rounded-full transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
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

                  {g.linked_accounts?.length ? (
                    <span className="text-right text-[11px] text-[var(--muted)]">
                      Otomatis dari saldo rekening
                    </span>
                  ) : (
                    <button
                      type="button"
                      onClick={() => {
                        setDepositGoal(g);
                        setActionAmount("");
                        setActionError("");
                      }}
                      className="flex min-h-11 items-center gap-1 font-semibold text-emerald-600 hover:text-emerald-500"
                    >
                      <span>+ Catat Progres</span>
                    </button>
                  )}
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
              <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
                <div className="flex min-w-0 items-center gap-2">
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

              <div className="flex flex-col gap-2 pt-1 sm:flex-row sm:items-end sm:justify-between">
                <div className="min-w-0">
                  <div className="text-2xl sm:text-3xl font-bold tabular tracking-tight text-rose-500 select-all">
                    {bal(obligationsSummary?.total_remaining ?? 0)}
                  </div>
                  <div className="text-[11px] text-[var(--muted)] mt-0.5">
                    Sisa dari total tagihan {bal(obligationsSummary?.total_debt ?? 0)}
                  </div>
                </div>
                <div className="min-w-0 text-left text-xs text-[var(--muted)] sm:text-right">
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
                  className="h-full rounded-full bg-rose-500 transition-[width] motion-reduce:transition-none"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {activeObligations.map((o) => (
              <div
                key={o.id}
                className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6 space-y-4 shadow-xs flex flex-col justify-between group hover:border-[var(--border-strong)] transition-colors"
              >
                <div>
                  <div className="flex items-center justify-between">
                    <h3 className="font-bold text-sm text-[var(--text)] truncate max-w-[180px]">
                      {o.name}
                    </h3>

                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        onClick={() => handleOpenEditObligation(o)}
                        aria-label={`Ubah tagihan ${o.name}`}
                        className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl text-[var(--muted)] hover:bg-[var(--surface-raised)] hover:text-[var(--text)]"
                      >
                        <Icon name="edit" className="h-3.5 w-3.5" />
                      </button>
                      <ConfirmActionButton
                        label={`Arsipkan tagihan ${o.name}`}
                        confirmation={`Arsipkan tagihan "${o.name}"?`}
                        onConfirm={() => archiveObligationMutation.mutate(o.id)}
                        className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl text-[var(--muted)] hover:bg-rose-500/10 hover:text-rose-600"
                      >
                        <Icon name="trash" className="h-3.5 w-3.5" />
                      </ConfirmActionButton>
                    </div>
                  </div>

                  {/* Amounts */}
                  <div className="mt-4 flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
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
                      className="h-full rounded-full bg-rose-500 transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
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
        <form
          className="space-y-4 pt-2 text-xs"
          onSubmit={(event) => {
            event.preventDefault();
            if (!saveGoalMutation.isPending) handleSaveGoal();
          }}
        >
          {goalError && (
            <div role="alert" aria-live="assertive" className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
              {goalError}
            </div>
          )}

          <FormSection title="Target" className="border-t-0 pt-0">
          <div>
            <label htmlFor="goal-name" className="font-medium text-[var(--muted)] block mb-1">Nama Target</label>
            <input
              id="goal-name"
              name="name"
              type="text"
              value={goalName}
              onChange={(e) => setGoalName(e.target.value)}
              placeholder="Contoh: Dana Darurat, Laptop Baru"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <div className="flex items-center justify-between mb-1">
              <label htmlFor="goal-target-amount" className="font-medium text-[var(--muted)]">Target Nominal</label>
              {currency === "USD" && parseNumberFromDots(goalTarget) > 0 && (
                <span className="text-[11px] font-bold text-emerald-500 tabular">
                  ≈ {bal(parseNumberFromDots(goalTarget))}
                </span>
              )}
            </div>
            <input
              id="goal-target-amount"
              name="target_amount"
              type="text"
              inputMode="numeric"
              value={goalTarget}
              onChange={(e) => setGoalTarget(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 20.000.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] font-bold tabular"
            />
          </div>

          </FormSection>

          <FormSection title="Sumber progres" description="Tanpa rekening, progres diatur manual. Dengan rekening, progres mengikuti saldonya tanpa menghitung rekening induk dan kantong dua kali.">
          <div>
            <div className="flex items-center justify-between mb-1">
              <span className="font-medium text-[var(--muted)] block">Hubungkan rekening / kantong (opsional)</span>
              <span className="text-[11px] text-[var(--muted)] font-medium">{goalAccountIds.length} dipilih</span>
            </div>
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
                      updateGoalAccountIds((prev) => {
                        const set = new Set(prev);
                        set.add(accId);
                        childIds.forEach((cid: string) => set.add(cid));
                        return Array.from(set);
                      });
                    } else {
                      updateGoalAccountIds((prev) => {
                        const set = new Set(prev);
                        set.delete(accId);
                        childIds.forEach((cid: string) => set.delete(cid));
                        return Array.from(set);
                      });
                    }
                  };

                  const handleChildToggle = (childId: string, checked: boolean) => {
                    updateGoalAccountIds((prev) => {
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
                <label htmlFor="goal-current-amount" className="font-medium text-[var(--muted)]">Progres awal (tanpa transaksi)</label>
                {currency === "USD" && parseNumberFromDots(goalCurrent) > 0 && (
                  <span className="text-[11px] font-bold text-emerald-500 tabular">
                    ≈ {bal(parseNumberFromDots(goalCurrent))}
                  </span>
                )}
              </div>
              <input
                id="goal-current-amount"
                name="current_amount"
                type="text"
                inputMode="numeric"
                value={goalCurrent}
                onChange={(e) => setGoalCurrent(formatNumberWithDots(e.target.value))}
                placeholder="0"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] font-bold tabular"
              />
              <p className="mt-1 text-xs text-[var(--muted)]">Nilai ini hanya mencatat progres target; saldo rekening tidak berubah.</p>
            </div>
          )}
          </FormSection>

          <FormSection title="Pengaturan tambahan">
          <div>
            <label htmlFor="goal-target-date" className="font-medium text-[var(--muted)] block mb-1">Target Tanggal Selesai (Opsional)</label>
            <input
              id="goal-target-date"
              name="target_date"
              type="date"
              value={goalDate}
              onChange={(e) => setGoalDate(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <span className="font-medium text-[var(--muted)] block mb-1">Warna Label</span>
            <div role="group" aria-label="Pilih warna target" className="flex flex-wrap items-center gap-2">
              {GOAL_COLORS.map((color) => (
                <button
                  key={color.value}
                  type="button"
                  onClick={() => setGoalColor(color.value)}
                  aria-label={`Warna ${color.label}`}
                  aria-pressed={goalColor === color.value}
                  className={cn(
                    "min-h-11 min-w-11 rounded-full border-4 border-[var(--surface)] transition-transform focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--text)]",
                    goalColor === color.value ? "scale-105 ring-2 ring-[var(--text)]" : "opacity-80 hover:opacity-100"
                  )}
                  style={{ backgroundColor: color.value }}
                />
              ))}
            </div>
          </div>

          {/* Dana Darurat Toggle */}
          <div className="p-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/60 space-y-1">
            <label className="flex items-center gap-2.5 cursor-pointer select-none">
              <input
                id="goal-emergency"
                name="is_emergency"
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
              {goalAccountIds.length > 0
                ? "Saldo rekening yang terhubung akan dihitung sebagai cadangan Ketahanan Dana."
                : "Progres manual target ini akan dihitung sebagai cadangan Ketahanan Dana."}
            </p>
          </div>
          </FormSection>

          {goalBackingChanged && goalBackingConfirmed && (
            <p role="alert" aria-live="polite" className="rounded-xl border border-amber-500/25 bg-amber-500/10 p-3 text-[11px] text-[var(--text)]">
              {goalAccountIds.length > 0
                ? "Setelah disimpan, progres akan mengikuti saldo rekening terhubung; nominal progres manual tidak menjadi sumber perhitungan."
                : "Setelah disimpan, progres kembali menjadi nilai manual dan tidak lagi mengikuti saldo rekening."}
              {" "}Pilih “{editingGoal ? "Konfirmasi & simpan" : "Konfirmasi & buat target"}” untuk melanjutkan.
            </p>
          )}

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setGoalModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="submit"
              disabled={saveGoalMutation.isPending}
              className="min-h-11 flex-1 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
            >
              {saveGoalMutation.isPending
                ? "Menyimpan…"
                : goalBackingChanged && goalBackingConfirmed
                ? editingGoal ? "Konfirmasi & simpan" : "Konfirmasi & buat target"
                : editingGoal ? "Simpan Perubahan" : "Buat Target"}
            </button>
          </div>
        </form>
      </Modal>

      {/* Obligation Create/Edit Modal */}
      <Modal
        open={obligationModalOpen}
        onClose={() => setObligationModalOpen(false)}
        title={editingObligation ? "Ubah Tagihan" : "Tambah Tagihan / Utang Baru"}
      >
        <form
          className="space-y-4 pt-2 text-xs"
          onSubmit={(event) => {
            event.preventDefault();
            saveObligationMutation.mutate();
          }}
        >
          {obError && (
            <div role="alert" aria-live="assertive" className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
              {obError}
            </div>
          )}

          <p className="text-xs leading-relaxed text-[var(--muted)]">
            Tagihan otomatis diarsipkan saat sisa mencapai nol dan aktif kembali jika saldo dikoreksi.
          </p>

          <FormSection title="Jumlah tagihan" description="Sisa saat ini menentukan progres pelunasan. Kosong berarti belum ada pembayaran." className="border-t-0 pt-0">
          <div>
            <label htmlFor="obligation-name" className="font-medium text-[var(--muted)] block mb-1">Nama Tagihan / Utang</label>
            <input
              ref={obNameRef}
              id="obligation-name"
              name="name"
              type="text"
              value={obName}
              onChange={(e) => setObName(e.target.value)}
              placeholder="Contoh: Cicilan Motor, Kartu Kredit"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <label htmlFor="obligation-total" className="font-medium text-[var(--muted)] block mb-1">Total Tagihan / Pinjaman (IDR)</label>
            <input
              ref={obTotalRef}
              id="obligation-total"
              name="total_amount"
              type="text"
              inputMode="numeric"
              value={obTotal}
              onChange={(e) => setObTotal(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 15.000.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] font-bold tabular"
            />
          </div>

          <div>
            <label htmlFor="obligation-remaining" className="font-medium text-[var(--muted)] block mb-1">Sisa Tagihan Saat Ini (IDR)</label>
          <input
              ref={obRemainingRef}
              id="obligation-remaining"
              name="remaining_amount"
              type="text"
              inputMode="numeric"
              value={obRemaining}
              onChange={(e) => setObRemaining(formatNumberWithDots(e.target.value))}
              aria-invalid={remainingExceedsTotal || undefined}
              aria-describedby={remainingExceedsTotal ? "obligation-remaining-error" : undefined}
              placeholder="Kosongkan jika sama dengan total"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular"
            />
            {remainingExceedsTotal && (
              <p id="obligation-remaining-error" role="alert" className="mt-1 text-[11px] font-medium text-rose-600">
                Sisa tagihan tidak boleh melebihi total tagihan.
              </p>
            )}
          </div>
          {obligationTotalValue > 0 && !remainingExceedsTotal && (
            <div role="status" className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3 text-xs text-[var(--text)]">
              Sudah dibayar: {bal(Math.max(0, obligationTotalValue - obligationRemainingValue))} · Sisa: {bal(obligationRemainingValue)}
            </div>
          )}
          </FormSection>

          <FormSection title="Rencana pembayaran">
          <div>
            <label htmlFor="obligation-minimum-payment" className="font-medium text-[var(--muted)] block mb-1">Cicilan Minimal per Bulan (Opsional)</label>
            <input
              id="obligation-minimum-payment"
              name="minimum_payment"
              type="text"
              inputMode="numeric"
              value={obMinPayment}
              onChange={(e) => setObMinPayment(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 1.000.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular"
            />
          </div>

          <div>
            <label htmlFor="obligation-due-date" className="font-medium text-[var(--muted)] block mb-1">Jatuh Tempo (Opsional)</label>
            <input
              ref={obDueDateRef}
              id="obligation-due-date"
              name="due_date"
              type="date"
              value={obDueDate}
              onChange={(e) => setObDueDate(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div>
            <label htmlFor="obligation-notes" className="font-medium text-[var(--muted)] block mb-1">Catatan (Opsional)</label>
            <textarea
              id="obligation-notes"
              name="notes"
              rows={2}
              value={obNotes}
              onChange={(event) => setObNotes(event.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>
          </FormSection>

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setObligationModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="submit"
              disabled={saveObligationMutation.isPending}
              className="min-h-11 flex-1 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
            >
              {saveObligationMutation.isPending ? "Menyimpan…" : editingObligation ? "Simpan Perubahan" : "Tambah Tagihan"}
            </button>
          </div>
        </form>
      </Modal>

      {/* Standalone goal progress adjustment */}
      {depositGoal && (
        <Modal
          open={Boolean(depositGoal)}
          onClose={() => setDepositGoal(null)}
          title={`Catat Progres: ${depositGoal.name}`}
        >
          <form
            className="space-y-4 pt-2 text-xs"
            onSubmit={(event) => {
              event.preventDefault();
              if (!depositGoalMutation.isPending) depositGoalMutation.mutate();
            }}
          >
            {actionError && (
              <div role="alert" aria-live="assertive" className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
                {actionError}
              </div>
            )}

            <p className="rounded-xl border border-sky-500/25 bg-sky-500/10 p-3 leading-relaxed text-sky-700 dark:text-sky-300">
              Ini hanya memperbarui angka progres target. Tidak ada transaksi dan tidak ada saldo rekening yang berubah.
            </p>

            <div>
              <div className="flex items-center justify-between mb-1">
                <label htmlFor="goal-progress-adjustment" className="font-medium text-[var(--muted)]">Tambahan Progres</label>
                {currency === "USD" && parseNumberFromDots(actionAmount) > 0 && (
                  <span className="text-[11px] font-bold text-emerald-500 tabular">
                    ≈ {bal(parseNumberFromDots(actionAmount))}
                  </span>
                )}
              </div>
              <input
                id="goal-progress-adjustment"
                name="amount"
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
                type="submit"
                disabled={depositGoalMutation.isPending}
                className="flex-1 rounded-xl bg-emerald-600 py-2 text-xs font-semibold text-white hover:bg-emerald-700 dark:text-[#062A1E] disabled:opacity-50"
              >
                {depositGoalMutation.isPending ? "Menyimpan…" : "Simpan Progres"}
              </button>
            </div>
          </form>
        </Modal>
      )}

      {/* Pay Obligation Modal */}
      {payObligation && (
        <Modal
          open={Boolean(payObligation)}
          onClose={() => setPayObligation(null)}
          title={`Bayar Tagihan: ${payObligation.name}`}
        >
          <form
            className="space-y-4 pt-2 text-xs"
            onSubmit={(event) => {
              event.preventDefault();
              if (!payObligationMutation.isPending) payObligationMutation.mutate();
            }}
          >
            {actionError && (
              <div role="alert" aria-live="assertive" className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-rose-500">
                {actionError}
              </div>
            )}

            <div>
              <label htmlFor="obligation-payment-account" className="font-medium text-[var(--muted)] block mb-1">Bayar dari Rekening / Dompet</label>
              <select
                id="obligation-payment-account"
                name="account_id"
                value={actionAccount}
                onChange={(e) => setActionAccount(e.target.value)}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              >
                <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} />
              </select>
            </div>

            <div>
              <div className="flex items-center justify-between mb-1">
                <label htmlFor="obligation-payment-amount" className="font-medium text-[var(--muted)]">Nominal Pembayaran</label>
                {currency === "USD" && parseNumberFromDots(actionAmount) > 0 && (
                  <span className="text-[11px] font-bold text-emerald-500 tabular">
                    ≈ {bal(parseNumberFromDots(actionAmount))}
                  </span>
                )}
              </div>
              <input
                id="obligation-payment-amount"
                name="amount"
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
                type="submit"
                disabled={payObligationMutation.isPending}
                className="flex-1 rounded-xl bg-rose-600 text-white py-2 text-xs font-semibold hover:bg-rose-700 disabled:opacity-50"
              >
                {payObligationMutation.isPending ? "Memproses…" : "Catat Pembayaran"}
              </button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  );
}
