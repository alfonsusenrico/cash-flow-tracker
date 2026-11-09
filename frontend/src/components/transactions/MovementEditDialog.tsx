"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { useMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { api, splitMovement, updateMovement } from "@/lib/api";
import { isCreditAccountChoice, listCreditAccountChoices, listLiquidAccountChoices } from "@/lib/accountOptions";
import { movementFormSchema, type MovementIssueCode } from "@/lib/movementFormSchema";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { routeText, useAccountOptionLabels, type MovementRoute } from "./rows";
import type { LedgerRowDetail } from "./types";

const FORM_ID = "tx-movement-form";

type MovementIssueKey = "chooseSource" | "chooseTarget" | "amountValid" | "chooseDate" | "notesTooLong" | "sameAccount";

/** The schema's issue code as this dialog's catalogue key: the rules stay in `movementFormSchema`, which Catat shares (D16). */
const ISSUE_KEY: Record<MovementIssueCode, MovementIssueKey> = {
  source_required: "chooseSource",
  target_required: "chooseTarget",
  amount_invalid: "amountValid",
  date_required: "chooseDate",
  notes_too_long: "notesTooLong",
  same_account: "sameAccount",
};

function accountId(account: { id?: unknown; account_id?: unknown }): string {
  return String(account.id ?? account.account_id ?? "");
}

/**
 * "Ubah pindah saldo" (apply-markas-transaksi D16): a transfer's both legs edited at once through `PATCH
 * /movements/{id}`, or split back into two transactions. It replaces `InternalMovementModal`; delete stays in the
 * transaction dialog. Server refusals arrive as codes and are shown in the interface language.
 */
export function MovementEditDialog({
  row,
  route,
  onClose,
  onSaved,
  onSplit,
}: {
  row: LedgerRowDetail;
  route: MovementRoute;
  onClose: () => void;
  onSaved: () => void;
  onSplit: () => void;
}) {
  const t = useTranslations("transactions.movement");
  const common = useTranslations("common");
  const edit = useTranslations("transactions.edit");
  const cancelSplitRef = useRef<HTMLButtonElement>(null);
  const money = useMoney();
  const accountLabels = useAccountOptionLabels();
  const accountsData = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") }).data?.accounts;
  const accounts = useMemo(() => accountsData ?? [], [accountsData]);
  const liquid = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);
  // A credit account may be either side of a movement, with cash on the other side (add-credit-accounts D6).
  const movementAccounts = useMemo(() => [...liquid, ...listCreditAccountChoices(accounts)], [accounts, liquid]);
  const isCreditId = (id: string) => movementAccounts.some((account) => accountId(account) === id && isCreditAccountChoice(account));

  const inbound = row.movement_role === "inbound";
  const [sourceId, setSourceId] = useState(inbound ? row.transfer_target_account_id ?? "" : row.account_id);
  const [targetId, setTargetId] = useState(inbound ? row.account_id : row.transfer_target_account_id ?? "");
  const [amount, setAmount] = useState(formatNumberWithDots(row.amount));
  const [date, setDate] = useState(toDatetimeLocal(row.date));
  const [notes, setNotes] = useState(row.notes ?? "");
  const [error, setError] = useState("");
  const [confirmSplit, setConfirmSplit] = useState(false);

  // A side the list no longer offers (archived, or the other side) falls back as the old modal did.
  useEffect(() => {
    if (!movementAccounts.length) return;
    setSourceId((current) => (movementAccounts.some((account) => accountId(account) === current) ? current : accountId(liquid[0] ?? {})));
  }, [movementAccounts, liquid]);
  // A credit source pays into cash only, so its target falls back to the first other liquid account (D16).
  useEffect(() => {
    if (!movementAccounts.length) return;
    const offered = isCreditId(sourceId) ? liquid : movementAccounts;
    setTargetId((current) => (current !== sourceId && offered.some((account) => accountId(account) === current)
      ? current
      : accountId(liquid.find((account) => accountId(account) !== sourceId) ?? {})));
    // isCreditId reads the same lists as the dependencies.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceId, movementAccounts, liquid]);

  useEffect(() => {
    if (confirmSplit) cancelSplitRef.current?.focus();
  }, [confirmSplit]);

  const parsedAmount = Number(amount.replace(/[^0-9]/g, "")) || 0;

  const save = useMutation({
    mutationFn: async () => {
      const validation = movementFormSchema.safeParse({ sourceAccountId: sourceId, targetAccountId: targetId, amount: parsedAmount, date, notes: notes.trim() });
      if (!validation.success) {
        throw new Error(t(`errors.${ISSUE_KEY[validation.error.issues[0].message as MovementIssueCode]}`));
      }
      if (isCreditId(validation.data.sourceAccountId) && isCreditId(validation.data.targetAccountId)) throw new Error(t("errors.creditBothSides"));
      return updateMovement(row.movement_id!, {
        source_account_id: validation.data.sourceAccountId,
        target_account_id: validation.data.targetAccountId,
        amount: validation.data.amount,
        notes: validation.data.notes || null,
        date: localDatetimeToISO(validation.data.date) || new Date().toISOString(),
      });
    },
    onSuccess: onSaved,
    onError: (failure: Error) => setError(failure.message || t("errors.saveFailed")),
  });

  const split = useMutation({
    mutationFn: () => splitMovement(row.movement_id!),
    onSuccess: onSplit,
    onError: (failure: Error) => setError(failure.message || t("errors.splitFailed")),
  });

  const footer = confirmSplit ? (
    <>
      <p className="t-body grow-text">{t("splitQuestion")}</p>
      <button ref={cancelSplitRef} type="button" className="ws-btn" onClick={() => setConfirmSplit(false)}>{common("cancel")}</button>
      <button type="button" className="ws-btn ws-btn-ink" disabled={split.isPending} onClick={() => split.mutate()}>
        {split.isPending ? t("splitting") : t("splitConfirm")}
      </button>
    </>
  ) : (
    <>
      <button type="button" className="ws-btn ws-btn-quiet" onClick={() => { setError(""); setConfirmSplit(true); }}>{t("split")}</button>
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>{common("cancel")}</button>
      <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink" disabled={parsedAmount <= 0 || save.isPending}>
        {save.isPending ? edit("saving") : edit("save")}
      </button>
    </>
  );

  return (
    <UntranslatedRegion active={false}>
      <ShellDialog open onClose={onClose} title={t("title")} meta={routeText(route)} width={600} closeLabel={t("close")} footer={footer}>
        <form id={FORM_ID} className="form-stack" noValidate onSubmit={(event) => { event.preventDefault(); setError(""); save.mutate(); }}>
          {error && <p role="alert" className="form-alert">{error}</p>}
          <div className="form-grid">
            <div className="field">
              <label htmlFor="movement-amount">{t("amount")}</label>
              <input id="movement-amount" type="text" inputMode="numeric" autoComplete="off" value={amount}
                onChange={(event) => setAmount(formatNumberWithDots(event.target.value))} className="field-control tabular-nums" />
            </div>
            <div className="field">
              <label htmlFor="movement-date">{t("date")}</label>
              <input id="movement-date" type="datetime-local" step={1} value={date} onChange={(event) => setDate(event.target.value)} className="field-control" />
            </div>
            <div className="field">
              <label htmlFor="movement-source-account">{t("from")}</label>
              <select id="movement-source-account" value={sourceId} onChange={(event) => setSourceId(event.target.value)} className="field-control">
                <AccountSelectOptions labels={accountLabels} accounts={accounts} formatBalance={money} allowParentSelection={true} liquidOnly includeCredit />
              </select>
            </div>
            <div className="field">
              <label htmlFor="movement-target-account">{t("to")}</label>
              <select id="movement-target-account" value={targetId} onChange={(event) => setTargetId(event.target.value)} className="field-control">
                <AccountSelectOptions labels={accountLabels} accounts={accounts} formatBalance={money} allowParentSelection={true} excludeAccountId={sourceId} liquidOnly includeCredit={!isCreditId(sourceId)} />
              </select>
            </div>
            <div className="field full-row">
              <label htmlFor="movement-notes">{t("notes")}</label>
              <input id="movement-notes" type="text" maxLength={500} autoComplete="off" value={notes} onChange={(event) => setNotes(event.target.value)}
                placeholder={t("notesPlaceholder")} className="field-control" />
            </div>
          </div>
        </form>
      </ShellDialog>
    </UntranslatedRegion>
  );
}
