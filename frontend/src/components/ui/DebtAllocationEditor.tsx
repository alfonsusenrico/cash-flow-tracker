"use client";

import { useState } from "react";
import { formatNumberWithDots } from "@/lib/utils";
import { InfoHelp } from "@/components/ui/InfoHelp";

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
  return rows.reduce((sum, row) => sum + (Number(row.amount.replace(/\./g, "")) || 0), 0);
}

export function allocationError(
  rows: DebtAllocationValue[],
  total: number,
  debts: DebtChoice[],
  originalAmounts: Record<string, number> = {},
): string | null {
  if (rows.length === 0) return null;
  const values = rows.map((row) => Number(row.amount.replace(/\./g, "")));
  if (values.some((value) => !Number.isSafeInteger(value) || value <= 0)) {
    return "Isi nominal positif untuk setiap tagihan.";
  }
  if (values.reduce((sum, value) => sum + value, 0) !== total) {
    return "Jumlah pembagian harus sama dengan nominal transaksi.";
  }
  for (const [index, row] of rows.entries()) {
    const debt = debts.find((item) => item.id === row.obligation_id);
    if (debt && values[index] > debt.remaining_amount + (originalAmounts[row.obligation_id] || 0)) {
      return `Pembagian ${debt.name} melebihi sisa tagihan.`;
    }
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
  const [editingAmounts, setEditingAmounts] = useState(false);
  const allocated = totalDebtPayments(rows);
  const hasIncompleteAmount = rows.some((row) => {
    const amount = Number(row.amount.replace(/\./g, ""));
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
    <fieldset className="min-w-0 space-y-2 rounded-xl border border-[var(--border)] p-3">
      <legend className="px-1 text-xs font-semibold text-[var(--text)]">Bayar tagihan / utang (opsional) <InfoHelp label="Pembagian pembayaran tagihan">Jumlah bayar mengikuti sisa tagihan. Pilih Ubah jumlah untuk pembayaran sebagian.</InfoHelp></legend>
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
                <label htmlFor={selectId} className="mb-1 block text-xs text-[var(--muted)]">Tagihan {index + 1}</label>
                <select id={selectId} name={`${idPrefix}-obligation-${index}`} autoComplete="off" value={row.obligation_id} onChange={(event) => updateDebt(index, event.target.value)} className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-sm text-[var(--text)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]">
                  <option value="">Pilih tagihan</option>
                  {choices.map((debt) => (
                    <option key={debt.id} value={debt.id}>{debt.name}{debt.is_archived ? " (tersimpan)" : ` · ${currency(debt.remaining_amount + (originalAmounts[debt.id] || 0))} tersedia`}</option>
                  ))}
                  {selected === undefined && row.obligation_id && <option value={row.obligation_id}>{row.obligation_name || "Tagihan tersimpan"}</option>}
                </select>
              </div>
              <button type="button" onClick={() => {
                if (rows.length === 1) setEditingAmounts(false);
                onChange(rows.filter((_, itemIndex) => itemIndex !== index));
              }} className="min-h-11 shrink-0 rounded-xl px-3 text-xs font-semibold text-[var(--muted)] hover:text-[var(--text)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]" aria-label={`Hapus tagihan ${index + 1}`}>Hapus</button>
            </div>
            {row.obligation_id && (editingAmounts ? (
              <div className="min-w-0">
                <label htmlFor={`${selectId}-amount`} className="mb-1 block text-xs text-[var(--muted)]">Dibayar untuk tagihan {index + 1} (IDR)</label>
                <input id={`${selectId}-amount`} name={`${idPrefix}-payment-${index}`} type="text" inputMode="numeric" autoComplete="off" value={row.amount} onChange={(event) => updateAmount(index, event.target.value)} className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-sm tabular-nums text-[var(--text)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]" />
              </div>
            ) : (
              <p className="text-sm tabular-nums text-[var(--text)]">Dibayar {currency(Number(row.amount.replace(/\./g, "")) || 0)}</p>
            ))}
          </div>
        );
      })}
      {canAdd && <button type="button" onClick={addDebt} className="min-h-11 rounded-xl border border-[var(--border)] px-3 text-xs font-semibold text-[var(--text)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]">+ Tambah tagihan</button>}
      {rows.some((row) => row.obligation_id) && (
        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-[var(--border)] pt-2">
          <div className="text-xs tabular-nums text-[var(--muted)]" aria-live="polite">
            <p>Total bayar {currency(allocated)}</p>
            {allocated !== total && <p>Jumlah bayar belum sesuai nominal transaksi.</p>}
            {hasIncompleteAmount && <p>Isi jumlah bayar positif sebelum menyimpan.</p>}
          </div>
          <button id={`${idPrefix}-edit-amounts`} type="button" onClick={() => setEditingAmounts(!editingAmounts)} className="min-h-11 rounded-xl px-3 text-xs font-semibold text-[var(--text)] hover:bg-[var(--surface-raised)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]" aria-expanded={editingAmounts}>
            {editingAmounts ? "Selesai ubah jumlah" : "Ubah jumlah"}
          </button>
        </div>
      )}
      {error && <p role="alert" className="text-xs text-rose-600">{error}</p>}
    </fieldset>
  );
}
