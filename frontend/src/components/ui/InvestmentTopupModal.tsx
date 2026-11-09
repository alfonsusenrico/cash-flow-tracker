"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { InvestmentAccount, InvestmentScheduleDraft, InvestmentTopup } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, localDatetimeToISO, parseNumberFromDots, toDatetimeLocal } from "@/lib/utils";
import { useTranslations } from "next-intl";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { ConfirmActionButton } from "./ConfirmActionButton";

interface Props {
  open: boolean;
  onClose: () => void;
  product: InvestmentAccount | null;
  allAccounts: InvestmentAccount[];
  editingTopup?: InvestmentTopup | null;
  onSchedule?: (draft: InvestmentScheduleDraft) => void;
}

const FORM_ID = "investment-topup-form";

/**
 * Top up into an amount-tracked product (apply-markas-rekening D10, handed over by apply-markas-transaksi): a 520 px
 * shell dialog with the 56 px amount, the funding account, the debit time and the note. Opened from Rekening and
 * from Transaksi; the create, correction and delete requests are unchanged.
 */
export function InvestmentTopupModal({ open, onClose, product, allAccounts, editingTopup, onSchedule }: Props) {
  const qc = useQueryClient();
  const t = useTranslations("accounts.topup");
  const common = useTranslations("common");
  const [sourceId, setSourceId] = useState("");
  const [amount, setAmount] = useState("");
  const [date, setDate] = useState("");
  const [notes, setNotes] = useState("");
  const [error, setError] = useState("");
  const retryKey = useRef("");
  const initialized = useRef<string | null>(null);
  const funding = useMemo(
    () => listLiquidAccountChoices(allAccounts).filter((account) => !account.is_archived),
    [allAccounts],
  );

  useEffect(() => {
    if (!open) {
      initialized.current = null;
      return;
    }
    if (!product || initialized.current === (editingTopup?.id ?? product.id)) return;
    initialized.current = editingTopup?.id ?? product.id;
    retryKey.current = crypto.randomUUID();
    setAmount(editingTopup ? formatNumberWithDots(editingTopup.amount) : "");
    setDate(toDatetimeLocal(editingTopup?.date ?? new Date().toISOString()));
    setNotes(editingTopup?.notes ?? "");
    setError("");
    const parent = allAccounts.find((account) => account.id === product.parent_id);
    const preferred = [product.default_funding_account_id, parent?.default_funding_account_id]
      .find((id) => funding.some((account) => account.id === id));
    setSourceId(editingTopup?.source_account_id ?? preferred ?? "");
  }, [open, product, allAccounts, funding, editingTopup]);

  const refresh = () => {
    [queryKeys.accounts, queryKeys.transactions.all, queryKeys.dashboard.all, queryKeys.pulse,
      queryKeys.insights, queryKeys.recurring.all].forEach((key) => qc.invalidateQueries({ queryKey: key }));
  };
  const mutation = useMutation({
    mutationFn: async (remove: boolean) => {
      if (remove && editingTopup) return api.del(`/investment-topups/${editingTopup.id}`);
      const parsedAmount = parseNumberFromDots(amount);
      const isoDate = localDatetimeToISO(date);
      if (!Number.isSafeInteger(parsedAmount) || parsedAmount <= 0) throw new Error(t("errors.amount"));
      if (!isoDate) throw new Error(t("errors.date"));
      if (!product || !sourceId) throw new Error(t("errors.source"));
      const correction = { amount: parsedAmount, date: isoDate, notes: notes.trim() };
      return editingTopup
        ? api.patch(`/investment-topups/${editingTopup.id}`, correction)
        : api.post(`/investment-topups`, {
          ...correction, source_account_id: sourceId, target_account_id: product.id, idempotency_key: retryKey.current,
        });
    },
    onSuccess: () => { refresh(); onClose(); },
    onError: (failure: Error) => setError(failure.message || t("errors.failed")),
  });
  if (!product) return null;
  const busy = mutation.isPending;
  const close = () => { if (!busy) onClose(); };

  const footer = (
    <>
      {editingTopup && (
        <ConfirmActionButton
          label={t("delete")}
          confirmation={t("deleteConfirm")}
          onConfirm={() => mutation.mutate(true)}
          disabled={busy}
          className="ws-btn ws-btn-quiet c-expense"
        >
          {t("deleteShort")}
        </ConfirmActionButton>
      )}
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose} disabled={busy}>{common("cancel")}</button>
      <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink" disabled={busy}>
        {busy ? t("saving") : editingTopup ? t("saveChanges") : t("submit")}
      </button>
    </>
  );

  return (
    <ShellDialog
      open={open}
      onClose={close}
      title={editingTopup ? t("editTitle") : t("title")}
      meta={product.name}
      width={520}
      closeLabel={common("close")}
      footer={footer}
    >
      <form
        id={FORM_ID}
        className="form-stack"
        noValidate
        onSubmit={(event) => { event.preventDefault(); setError(""); mutation.mutate(false); }}
      >
        <div className="field">
          <label htmlFor="topup-amount">{t("amount")}</label>
          <input
            id="topup-amount"
            name="amount"
            type="text"
            inputMode="numeric"
            className="field-control ws-money-input h-14 text-xl leading-5 font-semibold tabular-nums tracking-[-0.01em]"
            value={amount}
            onChange={(event) => setAmount(formatNumberWithDots(event.target.value))}
            placeholder="112.590"
            aria-describedby="topup-amount-hint"
            required
            disabled={busy}
          />
          <p id="topup-amount-hint" className="hint">{t("amountHint")}</p>
        </div>
        <div className="field">
          {editingTopup ? (
            <>
              <p className="flabel">{t("from")}</p>
              <p className="t-body">{allAccounts.find((account) => account.id === sourceId)?.name ?? t("sourceFallback")}</p>
            </>
          ) : (
            <>
              <label htmlFor="topup-source">{t("from")}</label>
              <select id="topup-source" className="field-control" value={sourceId} onChange={(event) => setSourceId(event.target.value)} required disabled={busy}>
                <option value="">{t("chooseSource")}</option>
                {funding.map((account) => <option key={account.id} value={account.id}>{account.name}</option>)}
              </select>
            </>
          )}
        </div>
        <div className="form-grid">
          <div className="field">
            <label htmlFor="topup-date">{t("date")}</label>
            <input id="topup-date" type="datetime-local" step="1" className="field-control tabular-nums" value={date} onChange={(event) => setDate(event.target.value)} required disabled={busy} />
          </div>
          <div className="field">
            <label htmlFor="topup-notes">{t("note")}</label>
            <input id="topup-notes" type="text" maxLength={500} className="field-control" value={notes} onChange={(event) => setNotes(event.target.value)} disabled={busy} />
          </div>
        </div>
        {onSchedule && !editingTopup && (
          <button
            type="button"
            className="ws-btn ws-btn-quiet"
            disabled={busy || !sourceId || parseNumberFromDots(amount) <= 0}
            onClick={() => {
              onSchedule({ sourceAccountId: sourceId, targetAccountId: product.id, amount: parseNumberFromDots(amount), productName: product.name });
            }}
          >
            {t("plan")}
          </button>
        )}
        <p className="t-meta">{editingTopup ? t("editNote") : t("createNote")}</p>
        {error && <p role="alert" className="form-alert">{error}</p>}
      </form>
    </ShellDialog>
  );
}
