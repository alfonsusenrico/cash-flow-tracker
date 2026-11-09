"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { InvestmentAccount, listAmountInvestmentProducts } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import type { UndoFields } from "@/lib/reviewUndo";

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

const FORM_ID = "expense-topup-form";

/**
 * "Catat sebagai top up investasi" (apply-markas-transaksi D17): an expense recorded as a top-up into an
 * amount-tracked product, on the shell dialog. The conversion request and its eligibility are unchanged.
 */
export function ExpenseTopupConversionModal({ expense, accounts, onClose, onConverted }: Props) {
  const t = useTranslations("transactions.topupConversion");
  const common = useTranslations("common");
  const money = useMoney();
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
    onError: (failure: Error) => setError(failure.message || t("failed")),
  });
  if (!expense) return null;
  const busy = mutation.isPending;
  const parentName = (product: InvestmentAccount) => accounts.find((account) => account.id === product.parent_id)?.name;
  const amount = () => <MaskedMoney text={money(expense.amount)} />;

  const footer = (
    <>
      <button type="button" className="ws-btn" onClick={onClose} disabled={busy}>{common("cancel")}</button>
      <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink" disabled={busy || products.length === 0}>
        {busy ? t("saving") : t("action")}
      </button>
    </>
  );

  return (
    <ShellDialog open onClose={() => { if (!busy) onClose(); }} title={t("title")} width={520} closeLabel={t("close")} footer={footer}>
      <form
        id={FORM_ID}
        className="form-stack"
        noValidate
        onSubmit={(event) => {
          event.preventDefault();
          setError("");
          if (!productId) {
            setError(t("chooseProduct"));
            document.getElementById("expense-topup-product")?.focus();
            return;
          }
          mutation.mutate({ transactionId: expense.id, targetId: productId });
        }}
      >
        {products.length === 0 ? (
          <p className="t-body">{t("noProducts")}</p>
        ) : (
          <div className="field">
            <label htmlFor="expense-topup-product">{t("product")}</label>
            <select
              id="expense-topup-product"
              value={productId}
              onChange={(event) => setProductId(event.target.value)}
              disabled={busy}
              aria-invalid={Boolean(error) && !productId}
              aria-describedby="expense-topup-hint"
              className="field-control"
            >
              <option value="">{t("productPlaceholder")}</option>
              {products.map((product) => (
                <option key={product.id} value={product.id}>
                  {parentName(product) ? `${parentName(product)} · ${product.name}` : product.name}
                </option>
              ))}
            </select>
            <p id="expense-topup-hint" className="hint">
              {expense.account_name ? t.rich("body", { amount, account: expense.account_name }) : t.rich("bodyNoAccount", { amount })}
            </p>
          </div>
        )}
        {error && <p role="alert" className="form-alert">{error}</p>}
      </form>
    </ShellDialog>
  );
}
