"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { DebtAllocationEditor, allocationIssue, totalDebtPayments, type DebtAllocationValue } from "@/components/ui/DebtAllocationEditor";
import { useMoney } from "@/components/workspace/money";
import { RadioSegments } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api, uploadTransactionReceipt } from "@/lib/api";
import { isCreditAccountChoice } from "@/lib/accountOptions";
import { useFormat } from "@/lib/format";
import { canConvertExpenseToTopup } from "@/lib/investmentTopups";
import { KAKEIBO_PILLARS, pillarOrUnclassified, type KakeiboPillar } from "@/lib/kakeibo";
import { ownRowNote } from "@/lib/placeholderNotes";
import { MERCHANT_MAX_LENGTH, categoryOffsetsSpending, offsetCategoryOptions } from "@/lib/provenance";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, localDatetimeToISO, stripGrouping, toDatetimeLocal } from "@/lib/utils";
import { ReimbursementLinksField, linksRequest, linksValue } from "./ReimbursementLinksField";
import { bucketName, useAccountOptionLabels, useRowLabels } from "./rows";
import type { LedgerRowDetail } from "./types";

type EditKind = "expense" | "income";
type TypeChoice = EditKind | "movement" | "topup";
type PlanChoice = "1" | "3" | "6" | "12" | "other";

const FORM_ID = "tx-edit-form";
const PLAN_COUNTS = ["3", "6", "12"] as const;
const MIN_INSTALLMENTS = 2;
const MAX_INSTALLMENTS = 36;
const PILLAR_HELP = {
  need: "kakeibo.needHelp",
  want: "kakeibo.wantHelp",
  culture: "kakeibo.cultureHelp",
  unexpected: "kakeibo.unexpectedHelp",
  saving: "kakeibo.savingHelp",
} as const satisfies Record<KakeiboPillar, string>;

function initialDebtRows(tx: LedgerRowDetail): DebtAllocationValue[] {
  if (tx.type !== "expense") return [];
  const rows = tx.obligation_allocations
    ?? (tx.obligation_id ? [{ obligation_id: tx.obligation_id, obligation_name: tx.obligation_name || "", amount: tx.amount }] : []);
  return rows.map((item) => ({ obligation_id: item.obligation_id, obligation_name: item.obligation_name, amount: formatNumberWithDots(item.amount) }));
}

function initialPlan(count: number): PlanChoice {
  if (count <= 1) return "1";
  return (PLAN_COUNTS as readonly string[]).includes(String(count)) ? (String(count) as PlanChoice) : "other";
}

/**
 * "Ubah transaksi" (apply-markas-transaksi D15, D30): the edit of an ordinary row in its own 600 px dialog, with the
 * fields, checks and `PATCH` of the inline form it replaces. The Jenis control also offers "Pindah saldo" (pick the
 * pair in the list) and "Top up Investasi" (the existing conversion). A credit purchase adds how it is paid, sent to
 * `…/credit-terms` only when it changed.
 */
export function TransactionEditDialog({
  tx,
  onSaved,
  onClose,
  onPickPair,
  onConvertToTopup,
}: {
  tx: LedgerRowDetail;
  onSaved: () => void;
  onClose: () => void;
  /** Absent where the page has no list to pick the other side from. */
  onPickPair?: () => void;
  onConvertToTopup: () => void;
}) {
  const t = useTranslations("transactions.edit");
  const terms = useTranslations();
  const money = useMoney();
  const format = useFormat();
  const accountLabels = useAccountOptionLabels();
  const accounts = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") }).data?.accounts ?? [];
  const categories = useQuery<{ categories: any[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") }).data?.categories ?? [];
  const goals = useQuery<{ goals: any[] }>({ queryKey: queryKeys.goals, queryFn: () => api.get("/goals") }).data?.goals ?? [];
  const obligations = useQuery<{ obligations: any[] }>({ queryKey: queryKeys.obligations, queryFn: () => api.get("/obligations") }).data?.obligations ?? [];

  const originalCategory = categories.find((category) => category.id === tx.category_id);
  const [kind, setKind] = useState<EditKind>(tx.type);
  const [amount, setAmount] = useState(formatNumberWithDots(tx.amount));
  // "" is an unclassified row or category; saving keeps it unclassified instead of storing "need".
  const [pillar, setPillar] = useState<KakeiboPillar | "">(pillarOrUnclassified(tx.kakeibo_type));
  const [pillarOverride, setPillarOverride] = useState(Boolean(
    tx.kakeibo_type && originalCategory?.kakeibo_type && tx.kakeibo_type !== originalCategory.kakeibo_type,
  ));
  const [receipt, setReceipt] = useState<File | null>(null);
  const [receiptRetry, setReceiptRetry] = useState(false);
  const [notes, setNotes] = useState(tx.notes || "");
  const [merchant, setMerchant] = useState(tx.merchant || "");
  const [offsetCategoryId, setOffsetCategoryId] = useState(tx.offset_category_id || "");
  const [accountId, setAccountId] = useState(tx.account_id);
  const [categoryId, setCategoryId] = useState(tx.category_id || "");
  const [goalId, setGoalId] = useState(tx.goal_id || "");
  const [obligationId, setObligationId] = useState(tx.obligation_id || "");
  const [debtRows, setDebtRows] = useState<DebtAllocationValue[]>(initialDebtRows(tx));
  const [date, setDate] = useState(tx.date ? toDatetimeLocal(tx.date) : "");
  const originalCount = tx.credit?.role === "purchase" ? tx.credit.installment_count ?? 1 : 1;
  const [plan, setPlan] = useState<PlanChoice>(initialPlan(originalCount));
  const [otherCount, setOtherCount] = useState(initialPlan(originalCount) === "other" ? String(originalCount) : "");
  // A first due date the owner set is kept unless the owner clears it; a derived one is never stored from here.
  const ownerFirstDue = tx.credit?.role === "purchase" && tx.credit.first_due_date_set_by === "owner" ? tx.credit.first_due_date ?? null : null;
  // A date the system kept after the account's days changed is not shown in the field, but a terms change sends it
  // back so an already-due bill stays where it was (apply-markas-cleanup F1, owner 2026-11-09). Moving the purchase
  // date clears it on the server, so then it is not sent.
  const keptFirstDue = tx.credit?.role === "purchase" && tx.credit.first_due_date_set_by === "system" ? tx.credit.first_due_date ?? null : null;
  const [firstDue, setFirstDue] = useState(ownerFirstDue ?? "");
  const [error, setError] = useState("");
  const errorRef = useRef<HTMLParagraphElement>(null);

  const offsetsSpending = kind === "income" && categoryOffsetsSpending(categories, categoryId);
  const offsetCategories = offsetCategoryOptions(categories);
  const rowLabels = useRowLabels();
  // The expenses this reimbursement repays (link-reimbursements-to-expenses D8); a link replaces the category link.
  const [links, setLinks] = useState(() => linksValue(tx.reimbursement, rowLabels.undescribed));
  const hadLinks = (tx.reimbursement?.repays.length ?? 0) > 0;
  const restCategories = categories.filter((category: any) => category.kind === "income" && !category.offsets_spending && !category.is_archived && !category.system_key);
  const originalAmounts = Object.fromEntries((tx.obligation_allocations ?? []).map((item) => [item.obligation_id, item.amount]));
  const debtLinked = kind === "expense" && debtRows.some((row) => row.obligation_id);
  const creditIds = new Set(accounts.filter(isCreditAccountChoice).map((account) => String(account.id)));
  const onCredit = creditIds.has(accountId);
  // Credit accounts pay expenses; an income keeps credit only when it already sits there, as a refund does.
  const offerCredit = kind === "expense" || creditIds.has(tx.account_id);
  const convertible = canConvertExpenseToTopup(tx, accounts);
  const creditPurchase = kind === "expense" && onCredit;
  const chosenCount = plan === "1" ? 1 : plan === "other" ? Number(otherCount) : Number(plan);

  const fail = (message: string, fieldId?: string) => {
    setError(message);
    window.setTimeout(() => (fieldId ? document.getElementById(fieldId) : errorRef.current)?.focus(), 0);
  };

  /** The terms request a saved credit purchase needs, or null when its terms did not change (H5). */
  const termsRequest = (): { method: "put" | "del"; body?: { installment_count: number; first_due_date: string | null } } | null => {
    if (!creditPurchase) return null;
    const dateMoved = date !== (tx.date ? toDatetimeLocal(tx.date) : "");
    const kept = dateMoved ? null : keptFirstDue;
    const firstDueDate = firstDue || kept;
    if (chosenCount === originalCount && firstDueDate === (ownerFirstDue ?? kept)) return null;
    // Paid in full following the account's days is the default, which the server keeps as no stored terms.
    if (chosenCount === 1 && !firstDueDate) return originalCount > 1 || ownerFirstDue ? { method: "del" } : null;
    return { method: "put", body: { installment_count: chosenCount, first_due_date: firstDueDate } };
  };

  const save = useMutation({
    mutationFn: async () => {
      const value = parseInt(amount.replace(/[^0-9]/g, ""), 10);
      if (!value || value <= 0) throw new Error(t("errors.amountPositive"));
      if (!accountId) throw new Error(t("errors.chooseAccount"));
      const isoDate = date ? (localDatetimeToISO(date) || new Date(date).toISOString()) : undefined;
      const terms = termsRequest();
      await api.patch(`/transactions/${tx.id}`, {
        type: kind,
        amount: value,
        notes: notes.trim() || null,
        account_id: accountId,
        category_id: categoryId || null,
        goal_id: goalId || null,
        obligation_id: kind === "income" ? (obligationId || null) : debtRows.length === 1 ? debtRows[0].obligation_id : null,
        ...(kind === "expense" && debtRows.length > 1
          ? { obligation_allocations: debtRows.map((row) => ({ obligation_id: row.obligation_id, amount: Number(stripGrouping(row.amount)) })) }
          : {}),
        kakeibo_type: kind === "expense" && pillar ? pillar : null,
        merchant: merchant.trim() || null,
        ...(offsetsSpending ? { offset_category_id: links.expenses.length > 0 ? null : offsetCategoryId || null } : {}),
        date: isoDate,
      });
      const linking = offsetsSpending ? linksRequest(links) : null;
      try {
        if (linking) await api.put(`/transactions/${tx.id}/reimburses`, linking);
        // Leaving the reimbursement category drops the links on the server; only an emptied list unlinks here.
        else if (hadLinks && offsetsSpending) await api.del(`/transactions/${tx.id}/reimburses`);
      } catch (linkError) {
        throw new Error(t("linksFailed", { error: linkError instanceof Error ? linkError.message : "" }));
      }
      if (receipt) {
        try {
          await uploadTransactionReceipt(tx.id, receipt);
        } catch (uploadError) {
          setReceiptRetry(true);
          throw new Error(t("errors.receiptFailed", { error: uploadError instanceof Error ? uploadError.message : "" }));
        }
      }
      if (terms) {
        try {
          if (terms.method === "put") await api.put(`/transactions/${tx.id}/credit-terms`, terms.body);
          else await api.del(`/transactions/${tx.id}/credit-terms`);
        } catch (termsError) {
          throw new Error(t("errors.termsFailed", { error: termsError instanceof Error ? termsError.message : "" }));
        }
      }
    },
    onSuccess: onSaved,
    onError: (failure: Error) => fail(failure.message || t("errors.saveFailed")),
  });

  const retryReceipt = useMutation({
    mutationFn: async () => {
      if (!receipt) throw new Error(t("errors.chooseReceipt"));
      return uploadTransactionReceipt(tx.id, receipt);
    },
    onSuccess: onSaved,
    onError: (failure: Error) => fail(t("errors.receiptRetryFailed", { error: failure.message })),
  });

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    if (kind === "expense") {
      if (debtRows.some((row) => !row.obligation_id || debtRows.filter((item) => item.obligation_id === row.obligation_id).length > 1)) {
        fail(t("errors.distinctDebts"), "tx-edit-debt-0");
        return;
      }
      const issue = allocationIssue(debtRows, Number(stripGrouping(amount)), obligations, originalAmounts);
      if (issue) {
        const message = issue.code === "over"
          ? terms("transactions.debts.errors.over", { name: issue.name })
          : issue.code === "total" ? terms("transactions.debts.errors.total") : terms("transactions.debts.errors.positive");
        fail(message, document.getElementById("tx-edit-debt-0-amount") ? "tx-edit-debt-0-amount" : "tx-edit-amount");
        return;
      }
    }
    if (creditPurchase && plan === "other" && (!Number.isInteger(chosenCount) || chosenCount < MIN_INSTALLMENTS || chosenCount > MAX_INSTALLMENTS)) {
      fail(t("errors.creditCountRange"), "tx-edit-credit-count");
      return;
    }
    if (!save.isPending) save.mutate();
  };

  const switchKind = (next: TypeChoice) => {
    if (next === "movement") {
      onPickPair?.();
      return;
    }
    if (next === "topup") {
      onConvertToTopup();
      return;
    }
    setKind(next);
    setCategoryId("");
    setPillarOverride(false);
    if (next === "income") {
      setGoalId("");
      setDebtRows([]);
      // The income picker has no credit accounts unless the row was already on one; return to its account.
      if (creditIds.has(accountId) && !creditIds.has(tx.account_id)) setAccountId(tx.account_id);
    }
  };

  const typeOptions: { value: TypeChoice; label: string }[] = [
    { value: "expense", label: terms("terms.spending") },
    { value: "income", label: terms("terms.income") },
    ...(onPickPair ? [{ value: "movement" as const, label: terms("terms.transfer") }] : []),
    ...(convertible ? [{ value: "topup" as const, label: terms("terms.investmentTopUp") }] : []),
  ];

  const footer = (
    <>
      <button type="button" className="ws-btn" onClick={onClose}>{terms("common.cancel")}</button>
      <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink" disabled={save.isPending}>
        {save.isPending ? t("saving") : t("save")}
      </button>
    </>
  );

  return (
    <ShellDialog open onClose={onClose} title={t("title")} meta={ownRowNote(tx.notes) || tx.merchant || tx.notes?.trim() || undefined} width={600} closeLabel={t("close")} footer={footer}>
      <form id={FORM_ID} className="form-stack" onSubmit={submit} noValidate>
        {error && <p ref={errorRef} tabIndex={-1} role="alert" className="form-alert">{error}</p>}
        <div className="field">
          <span className="flabel" id="tx-edit-type">{t("type")}</span>
          <RadioSegments labelledBy="tx-edit-type" options={typeOptions} value={kind} onChange={switchKind} />
          <p className="hint">{convertible ? t("typeHintTopup") : t("typeHint")}</p>
        </div>
        <div className="form-grid">
          <div className="field">
            <label htmlFor="tx-edit-amount">{debtLinked ? t("total") : t("amount")}</label>
            <input id="tx-edit-amount" type="text" inputMode="numeric" required value={amount} readOnly={debtLinked}
              onChange={(event) => setAmount(formatNumberWithDots(event.target.value))} className="field-control tabular-nums" />
          </div>
          <div className="field">
            <label htmlFor="tx-edit-date">{t("date")}</label>
            <input id="tx-edit-date" type="datetime-local" step={1} required value={date} onChange={(event) => setDate(event.target.value)} className="field-control" />
          </div>
          <div className="field">
            <label htmlFor="tx-edit-account">{kind === "expense" ? t("payFrom") : t("paidInto")}</label>
            <select id="tx-edit-account" required value={accountId} className="field-control" onChange={(event) => {
              setAccountId(event.target.value);
              // A debt is paid from cash, so a row moved to credit drops its debt link.
              if (creditIds.has(event.target.value)) {
                setDebtRows([]);
                setObligationId("");
              }
            }}>
              <AccountSelectOptions labels={accountLabels} accounts={accounts} formatBalance={money} allowParentSelection={true} liquidOnly includeCredit={offerCredit} />
            </select>
          </div>
          <div className="field">
            <label htmlFor="tx-edit-category">{t("category")}</label>
            <select id="tx-edit-category" value={categoryId} className="field-control" onChange={(event) => {
              setCategoryId(event.target.value);
              if (kind === "expense" && !pillarOverride) {
                setPillar(pillarOrUnclassified(categories.find((item) => item.id === event.target.value)?.kakeibo_type));
              }
            }}>
              <option value="">{terms("terms.noCategory")}</option>
              {categories.filter((category) => category.kind === kind && !category.is_archived).map((category) => (
                <option key={category.id} value={category.id}>{category.name}</option>
              ))}
            </select>
          </div>
          {creditPurchase && (
            <>
              <div className="field full-row">
                <span className="flabel" id="tx-edit-credit">{t("creditPayment")}</span>
                <RadioSegments
                  labelledBy="tx-edit-credit"
                  options={[
                    { value: "1" as PlanChoice, label: t("creditInFull") },
                    ...PLAN_COUNTS.map((count) => ({ value: count as PlanChoice, label: t("creditTimes", { times: count }) })),
                    { value: "other" as PlanChoice, label: t("creditOther") },
                  ]}
                  value={plan}
                  onChange={setPlan}
                />
              </div>
              {plan === "other" && (
                <div className="field">
                  <label htmlFor="tx-edit-credit-count">{t("creditCount")}</label>
                  <input id="tx-edit-credit-count" type="number" inputMode="numeric" min={MIN_INSTALLMENTS} max={MAX_INSTALLMENTS} value={otherCount}
                    onChange={(event) => setOtherCount(event.target.value)} className="field-control tabular-nums" />
                </div>
              )}
              <div className="field">
                <label htmlFor="tx-edit-credit-due">{t("creditFirstDue")}</label>
                <input id="tx-edit-credit-due" type="date" value={firstDue} onChange={(event) => setFirstDue(event.target.value)}
                  className="field-control" aria-describedby="tx-edit-credit-due-hint" />
                <p id="tx-edit-credit-due-hint" className="hint">
                  {!firstDue && tx.credit?.installments?.[0]
                    ? t("creditFirstDueHintDate", { date: format.shortDate(tx.credit.installments[0].due_date) })
                    : t("creditFirstDueHint")}
                </p>
              </div>
            </>
          )}
          {offsetsSpending && (
            <ReimbursementLinksField idPrefix="tx-edit" value={links} onChange={setLinks} incomeCategories={restCategories} />
          )}
          {offsetsSpending && links.expenses.length === 0 && (
            <div className="field full-row">
              <label htmlFor="tx-edit-offset">{t("offset")}</label>
              <select id="tx-edit-offset" value={offsetCategoryId} onChange={(event) => setOffsetCategoryId(event.target.value)} aria-describedby="tx-edit-offset-help" className="field-control">
                <option value="">{t("offsetNone")}</option>
                {offsetCategories.map((category) => <option key={category.id} value={category.id}>{category.name}</option>)}
              </select>
              <p id="tx-edit-offset-help" className="hint">{t("offsetHelp")}</p>
            </div>
          )}
          {kind === "expense" && (
            <div className="field full-row">
              <span className="flabel" id="tx-edit-pillar">{t("pillar")}</span>
              <RadioSegments
                labelledBy="tx-edit-pillar"
                options={KAKEIBO_PILLARS.map((item) => ({ value: item.value, label: bucketName(terms, item.value) }))}
                value={pillar || null}
                onChange={(next) => { setPillar(next); setPillarOverride(true); }}
              />
              <p className="hint">{pillar ? terms(PILLAR_HELP[pillar]) : t("pillarChoose")}</p>
            </div>
          )}
          <div className="field">
            <label htmlFor="tx-edit-goal">{t("goal")}</label>
            <select id="tx-edit-goal" value={goalId} className="field-control" onChange={(event) => {
              setGoalId(event.target.value);
              if (event.target.value) { setObligationId(""); setDebtRows([]); }
            }} aria-describedby={goalId ? "tx-edit-goal-help" : undefined}>
              <option value="">{t("goalNone")}</option>
              {goals.filter((goal) => !goal.is_archived).map((goal) => (
                <option key={goal.id} value={goal.id}>{t("goalOption", { name: goal.name, current: money(goal.current_amount), target: money(goal.target_amount) })}</option>
              ))}
            </select>
            {goalId && <p id="tx-edit-goal-help" className="hint">{t("goalHelp")}</p>}
          </div>
          {onCredit ? null : kind === "expense" ? (
            <div className="full-row">
              <DebtAllocationEditor idPrefix="tx-edit" debts={obligations} rows={debtRows} total={Number(stripGrouping(amount)) || 0} currency={money}
                originalAmounts={originalAmounts} onChange={(rows) => {
                  setDebtRows(rows);
                  if (rows.length) setGoalId("");
                  if (rows.some((row) => row.obligation_id)) setAmount(formatNumberWithDots(totalDebtPayments(rows)));
                  setError("");
                }} />
            </div>
          ) : (
            <div className="field">
              <label htmlFor="tx-edit-obligation">{t("bill")}</label>
              <select id="tx-edit-obligation" value={obligationId} className="field-control" onChange={(event) => {
                setObligationId(event.target.value);
                if (event.target.value) setGoalId("");
              }} aria-describedby={obligationId ? "tx-edit-obligation-help" : undefined}>
                <option value="">{t("billNone")}</option>
                {obligations.filter((item) => !item.is_archived && item.remaining_amount > 0).map((item) => (
                  <option key={item.id} value={item.id}>{t("billOption", { name: item.name, amount: money(item.remaining_amount) })}</option>
                ))}
              </select>
              {obligationId && <p id="tx-edit-obligation-help" className="hint">{t("billHelp")}</p>}
            </div>
          )}
          <div className="field">
            <label htmlFor="tx-edit-merchant">{t("merchant")}</label>
            <input id="tx-edit-merchant" type="text" autoComplete="off" maxLength={MERCHANT_MAX_LENGTH} value={merchant}
              onChange={(event) => setMerchant(event.target.value)} placeholder={t("merchantPlaceholder")} className="field-control" />
          </div>
          <div className="field">
            <label htmlFor="tx-edit-notes">{t("notes")}</label>
            <input id="tx-edit-notes" type="text" autoComplete="off" value={notes} onChange={(event) => setNotes(event.target.value)} placeholder={t("notesPlaceholder")} className="field-control" />
          </div>
          <div className="field full-row">
            <label htmlFor="tx-edit-receipt">{t("receipt")}</label>
            <input id="tx-edit-receipt" type="file" accept="image/jpeg,image/png,image/webp,application/pdf" onChange={(event) => setReceipt(event.target.files?.[0] ?? null)}
              className="field-control" aria-describedby={tx.receipt_path ? "tx-edit-receipt-help" : undefined} />
            {tx.receipt_path && <p id="tx-edit-receipt-help" className="hint">{t("receiptHelp")}</p>}
            {receiptRetry && (
              <button type="button" className="ws-btn" disabled={retryReceipt.isPending || !receipt} onClick={() => retryReceipt.mutate()}>
                {retryReceipt.isPending ? t("receiptUploading") : t("receiptRetry")}
              </button>
            )}
          </div>
        </div>
      </form>
    </ShellDialog>
  );
}
