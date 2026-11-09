"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";
import { MaskedMoney } from "@/components/workspace/money";
import { formatNumberWithDots, stripGrouping } from "@/lib/utils";

export interface DebtChoice {
  id: string;
  name: string;
  remaining_amount: number;
  is_archived: boolean;
}

export interface DebtAllocationValue {
  obligation_id: string;
  amount: string;
  obligation_name?: string;
}

export function totalDebtPayments(rows: DebtAllocationValue[]): number {
  return rows.reduce((sum, row) => sum + (Number(stripGrouping(row.amount)) || 0), 0);
}

/** What is wrong with a debt split, as a code its caller words from `transactions.debts.errors`. */
export type AllocationIssue = { code: "positive" } | { code: "total" } | { code: "over"; name: string };

export function allocationIssue(
  rows: DebtAllocationValue[],
  total: number,
  debts: DebtChoice[],
  originalAmounts: Record<string, number> = {},
): AllocationIssue | null {
  if (rows.length === 0) return null;
  const values = rows.map((row) => Number(stripGrouping(row.amount)));
  if (values.some((value) => !Number.isSafeInteger(value) || value <= 0)) return { code: "positive" };
  if (values.reduce((sum, value) => sum + value, 0) !== total) return { code: "total" };
  for (const [index, row] of rows.entries()) {
    const debt = debts.find((item) => item.id === row.obligation_id);
    if (debt && values[index] > debt.remaining_amount + (originalAmounts[row.obligation_id] || 0)) return { code: "over", name: debt.name };
  }
  return null;
}

interface Props {
  idPrefix: string;
  debts: DebtChoice[];
  rows: DebtAllocationValue[];
  onChange: (rows: DebtAllocationValue[]) => void;
  total: number;
  currency: (amount: number) => string;
  error?: string;
  originalAmounts?: Record<string, number>;
}

export function DebtAllocationEditor({
  idPrefix,
  debts,
  rows,
  onChange,
  total,
  currency,
  error,
  originalAmounts = {},
}: Props) {
  const t = useTranslations("transactions.debts");
  const [editingAmounts, setEditingAmounts] = useState(false);
  const allocated = totalDebtPayments(rows);
  const hasIncompleteAmount = rows.some((row) => {
    const amount = Number(stripGrouping(row.amount));
    return Boolean(row.obligation_id) && (!Number.isSafeInteger(amount) || amount <= 0);
  });
  const canAdd = rows.every((row) => row.obligation_id) && debts.some(
    (debt) => !debt.is_archived && debt.remaining_amount > 0 && !rows.some((row) => row.obligation_id === debt.id),
  );

  const addDebt = () => {
    if (rows.length === 0) setEditingAmounts(false);
    onChange([...rows, { obligation_id: "", amount: "" }]);
  };

  const updateDebt = (index: number, obligationId: string) => {
    const debt = debts.find((item) => item.id === obligationId);
    const availableAmount = debt ? debt.remaining_amount + (originalAmounts[obligationId] || 0) : 0;
    onChange(rows.map((row, rowIndex) => rowIndex === index
      ? { obligation_id: obligationId, amount: debt ? formatNumberWithDots(availableAmount) : "" }
      : row));
  };

  const updateAmount = (index: number, amount: string) => {
    const formatted = formatNumberWithDots(amount.replace(/[^0-9]/g, ""));
    onChange(rows.map((row, rowIndex) => rowIndex === index
      ? { ...row, amount: formatted }
      : row));
  };

  return (
    <fieldset className="min-w-0 space-y-2 rounded-xl border border-[var(--line)] p-3" aria-describedby={`${idPrefix}-debts-hint`}>
      <legend className="px-1 text-xs font-semibold text-[var(--text)]">{t("legend")}</legend>
      <p id={`${idPrefix}-debts-hint`} className="text-xs text-[var(--muted)]">{t("hint")}</p>
      {rows.map((row, index) => {
        const selected = debts.find((debt) => debt.id === row.obligation_id);
        const selectId = `${idPrefix}-debt-${index}`;
        const choices = debts.filter((debt) => {
          if (debt.id === row.obligation_id) return true;
          if (debt.is_archived || debt.remaining_amount <= 0) return false;
          return !rows.some((other, otherIndex) => otherIndex !== index && other.obligation_id === debt.id);
        });
        return (
          <div key={index} className="min-w-0 space-y-2">
            <div className="flex min-w-0 items-end gap-2">
              <div className="min-w-0 flex-1">
                <label htmlFor={selectId} className="mb-1 block text-xs text-[var(--muted)]">{t("bill", { n: String(index + 1) })}</label>
                <select id={selectId} name={`${idPrefix}-obligation-${index}`} autoComplete="off" value={row.obligation_id} onChange={(event) => updateDebt(index, event.target.value)} className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 text-sm text-[var(--text)]">
                  <option value="">{t("choose")}</option>
                  {choices.map((debt) => (
                    <option key={debt.id} value={debt.id}>
                      {debt.is_archived
                        ? t("archivedOption", { name: debt.name })
                        : t("availableOption", { name: debt.name, amount: currency(debt.remaining_amount + (originalAmounts[debt.id] || 0)) })}
                    </option>
                  ))}
                  {selected === undefined && row.obligation_id && <option value={row.obligation_id}>{row.obligation_name || t("saved")}</option>}
                </select>
              </div>
              <button type="button" onClick={() => {
                if (rows.length === 1) setEditingAmounts(false);
                onChange(rows.filter((_, itemIndex) => itemIndex !== index));
              }} className="min-h-11 shrink-0 rounded-md px-3 text-xs font-semibold text-[var(--muted)] hover:text-[var(--text)]" aria-label={t("removeName", { n: String(index + 1) })}>{t("remove")}</button>
            </div>
            {row.obligation_id && (editingAmounts ? (
              <div className="min-w-0">
                <label htmlFor={`${selectId}-amount`} className="mb-1 block text-xs text-[var(--muted)]">{t("paidFor", { n: String(index + 1) })}</label>
                <input id={`${selectId}-amount`} name={`${idPrefix}-payment-${index}`} type="text" inputMode="numeric" autoComplete="off" value={row.amount} onChange={(event) => updateAmount(index, event.target.value)} className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 text-sm tabular-nums text-[var(--text)]" />
              </div>
            ) : (
              <p className="text-sm tabular-nums text-[var(--text)]">{t.rich("paid", { amount: () => <MaskedMoney text={currency(Number(stripGrouping(row.amount)) || 0)} /> })}</p>
            ))}
          </div>
        );
      })}
      {canAdd && <button type="button" onClick={addDebt} className="min-h-11 rounded-md border border-[var(--line)] px-3 text-xs font-semibold text-[var(--text)]">{t("add")}</button>}
      {rows.some((row) => row.obligation_id) && (
        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-[var(--line)] pt-2">
          <div className="text-xs tabular-nums text-[var(--muted)]" aria-live="polite">
            <p>{t.rich("total", { amount: () => <MaskedMoney text={currency(allocated)} /> })}</p>
            {allocated !== total && <p>{t("totalMismatch")}</p>}
            {hasIncompleteAmount && <p>{t("incomplete")}</p>}
          </div>
          <button id={`${idPrefix}-edit-amounts`} type="button" onClick={() => setEditingAmounts(!editingAmounts)} className="min-h-11 rounded-md px-3 text-xs font-semibold text-[var(--text)] hover:bg-[var(--surface-2)]" aria-expanded={editingAmounts}>
            {editingAmounts ? t("doneAmounts") : t("editAmounts")}
          </button>
        </div>
      )}
      {error && <p role="alert" className="text-xs text-out-fg">{error}</p>}
    </fieldset>
  );
}
