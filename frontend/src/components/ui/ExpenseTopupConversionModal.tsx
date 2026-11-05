"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { InvestmentAccount, listAmountInvestmentProducts } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import type { UndoFields } from "@/lib/reviewUndo";
import { fmtIDR } from "@/lib/utils";
import { Modal } from "./Modal";

export interface ExpenseForTopup {
  id: string;
  amount: number;
  account_name?: string | null;
}

interface Props {
  expense: ExpenseForTopup | null;
  accounts: InvestmentAccount[];
  onClose: () => void;
  /** Receives the conversion response, whose undo token lets the caller offer "Urungkan". */
  onConverted: (response?: UndoFields) => void;
}

export function ExpenseTopupConversionModal({ expense, accounts, onClose, onConverted }: Props) {
  const qc = useQueryClient();
  const products = useMemo(() => listAmountInvestmentProducts(accounts), [accounts]);
  const [productId, setProductId] = useState("");
  const [error, setError] = useState("");
  useEffect(() => {
    setError("");
    setProductId(products.length === 1 ? products[0].id : "");
  }, [expense?.id, products]);
  const mutation = useMutation({
    mutationFn: ({ transactionId, targetId }: { transactionId: string; targetId: string }) =>
      api.post<UndoFields>("/investment-topups/from-transaction", { transaction_id: transactionId, target_account_id: targetId }),
    onSuccess: (response) => {
      [queryKeys.accounts, queryKeys.transactions.all, queryKeys.dashboard.all, queryKeys.pulse, queryKeys.insights]
        .forEach((key) => qc.invalidateQueries({ queryKey: key }));
      onConverted(response);
    },
    onError: (failure: Error) => setError(failure.message || "Top up gagal dicatat. Coba lagi."),
  });
  if (!expense) return null;
  const busy = mutation.isPending;
  const parentName = (product: InvestmentAccount) => accounts.find((account) => account.id === product.parent_id)?.name;

  return (
    <Modal open onClose={() => { if (!busy) onClose(); }} title="Jadikan Top up Investasi">
      <form
        className="space-y-4"
        onSubmit={(event) => {
          event.preventDefault();
          setError("");
          if (!productId) {
            setError("Pilih produk reksadana tujuan.");
            document.getElementById("expense-topup-product")?.focus();
            return;
          }
          mutation.mutate({ transactionId: expense.id, targetId: productId });
        }}
      >
        <p className="text-sm">
          Debit <span className="font-semibold tabular-nums">{fmtIDR(expense.amount)}</span>
          {expense.account_name ? <> dari <span className="font-semibold">{expense.account_name}</span></> : null} dicatat sebagai top up ke produk yang dipilih. Saldo rekening sumber tidak berubah karena debit ini sudah tercatat.
        </p>
        {products.length === 0 ? (
          <p className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)] p-3 text-sm">
            Belum ada produk reksadana yang dicatat berdasarkan nominal. Buka Rekening, pilih menu produk, lalu pilih &quot;Ubah ke pelacakan nominal&quot;.
          </p>
        ) : (
          <div>
            <label htmlFor="expense-topup-product" className="mb-1 block text-xs font-medium">Produk tujuan</label>
            <select
              id="expense-topup-product"
              data-autofocus
              value={productId}
              onChange={(event) => setProductId(event.target.value)}
              disabled={busy}
              aria-invalid={Boolean(error) && !productId}
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
            >
              <option value="">Pilih produk…</option>
              {products.map((product) => (
                <option key={product.id} value={product.id}>
                  {parentName(product) ? `${parentName(product)} · ${product.name}` : product.name}
                </option>
              ))}
            </select>
          </div>
        )}
        {error && <p role="alert" className="text-sm text-out-fg">{error}</p>}
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-[var(--line)] pt-3">
          <button type="button" onClick={onClose} disabled={busy} className="min-h-11 rounded-md border border-[var(--line)] px-3 text-sm">Batal</button>
          <button type="submit" disabled={busy || products.length === 0} className="ws-btn ws-btn-accent disabled:opacity-50">
            {busy ? "Menyimpan…" : "Jadikan top up"}
          </button>
        </div>
      </form>
    </Modal>
  );
}
