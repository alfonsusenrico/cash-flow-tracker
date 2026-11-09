"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { AccountItem, AccountsResponse, NetWorthResponse } from "@/components/accounts/types";
import type { CycleSummary, PlanLine } from "@/components/beranda/types";
import { jakartaDay } from "@/components/transactions/rows";
import { api } from "@/lib/api";
import { creditBillsPath, type CreditBillsResponse } from "@/lib/creditBills";
import { queryKeys } from "@/lib/queryKeys";
import type { RecurringRule } from "@/types/recurring";
import type { DetectedCharge } from "./bills";
import type { GoalItem, ObligationItem } from "./types";

/**
 * Every read of Target & Tagihan (apply-markas-target D9, D14, D23). Each section reads its own request, so one
 * failure never hides another. Debts are read with `include_archived=true` under their own key, so "Sudah lunas"
 * can list paid-off debts while Catat's debt picker and the plan form keep reading the active list.
 */
export function useGoalData() {
  const [today] = useState(() => jakartaDay(new Date()));
  const goalsQuery = useQuery<{ ok: boolean; goals: GoalItem[] }>({ queryKey: queryKeys.goals, queryFn: () => api.get("/goals") });
  const debtsQuery = useQuery<{ ok: boolean; obligations: ObligationItem[] }>({
    queryKey: queryKeys.obligationsWithArchived,
    queryFn: () => api.get("/obligations?include_archived=true"),
  });
  const accountsQuery = useQuery<AccountsResponse>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });
  const netWorthQuery = useQuery<NetWorthResponse>({ queryKey: queryKeys.dashboard.netWorth, queryFn: () => api.get("/dashboard/net-worth") });
  const summaryQuery = useQuery<CycleSummary>({ queryKey: queryKeys.dashboard.cycleSummary, queryFn: () => api.get("/dashboard/cycle-summary") });
  const chargesQuery = useQuery<{ charges: DetectedCharge[] }>({ queryKey: queryKeys.dashboard.recurringCharges, queryFn: () => api.get("/dashboard/recurring-charges") });
  // The shell reads the same list for the bell, so this adds no request; it gives each plan row its schedule.
  const rulesQuery = useQuery<{ rules: RecurringRule[] }>({ queryKey: queryKeys.recurring.all, queryFn: () => api.get("/recurring") });
  const windowEnd = summaryQuery.data?.scheduled.window_end;
  const billsPath = windowEnd ? creditBillsPath(windowEnd, today) : null;
  const creditQuery = useQuery<CreditBillsResponse>({
    queryKey: queryKeys.transactions.creditBills(billsPath ?? ""),
    queryFn: () => api.get(billsPath!),
    enabled: billsPath !== null,
  });

  const goals = useMemo(() => (goalsQuery.data?.goals ?? []).filter((goal) => !goal.is_archived), [goalsQuery.data]);
  const debts = useMemo(() => debtsQuery.data?.obligations ?? [], [debtsQuery.data]);
  // Active is today's rule (`isArchived`): not archived and something left; the rest are paid off.
  const activeDebts = useMemo(
    () =>
      debts
        .filter((debt) => !debt.is_archived && debt.remaining_amount > 0)
        .sort((a, b) => (a.due_date ?? "9999-12-31").localeCompare(b.due_date ?? "9999-12-31") || a.name.localeCompare(b.name)),
    [debts],
  );
  const paidDebts = useMemo(() => debts.filter((debt) => debt.is_archived || debt.remaining_amount <= 0), [debts]);
  const accounts = useMemo(() => (accountsQuery.data?.accounts ?? []).filter((account) => !account.is_archived), [accountsQuery.data]);
  const creditAccounts = useMemo(
    () => accounts.filter((account): account is AccountItem => account.type === "credit" && !account.parent_id).sort((a, b) => a.name.localeCompare(b.name)),
    [accounts],
  );
  const planLines = useMemo(
    () => (summaryQuery.data?.scheduled.lines ?? []).filter((line): line is PlanLine => line.kind === "plan"),
    [summaryQuery.data],
  );
  const flagged = goals.filter((goal) => goal.is_emergency);

  return {
    today,
    goalsQuery,
    debtsQuery,
    accountsQuery,
    netWorthQuery,
    summaryQuery,
    chargesQuery,
    rulesQuery,
    creditQuery,
    goals,
    /** The one flagged goal that leaves the list (D7); null with none or several. */
    emergencyGoal: flagged.length === 1 ? flagged[0] : null,
    flaggedCount: flagged.length,
    debts,
    activeDebts,
    paidDebts,
    accounts,
    creditAccounts,
    planLines,
  };
}

export type GoalData = ReturnType<typeof useGoalData>;
