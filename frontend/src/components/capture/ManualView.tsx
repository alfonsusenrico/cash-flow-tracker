"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { bucketName, maskedTag, useAccountOptionLabels } from "@/components/transactions/rows";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { DebtAllocationEditor, allocationIssue, totalDebtPayments, type DebtAllocationValue } from "@/components/ui/DebtAllocationEditor";
import { Icon } from "@/components/ui/Icon";
import { useMoney } from "@/components/workspace/money";
import { useToast } from "@/components/workspace/overlays";
import { RadioSegments } from "@/components/workspace/primitives";
import { api, ApiError, createMovement, uploadTransactionReceipt, type CreateMovementPayload } from "@/lib/api";
import { defaultMovementTargetId, listCreditAccountChoices, listLiquidAccountChoices } from "@/lib/accountOptions";
import { useFormat } from "@/lib/format";
import { KAKEIBO_PILLARS, type KakeiboPillar, pillarOrNeed } from "@/lib/kakeibo";
import { movementFormSchema, type MovementIssueCode } from "@/lib/movementFormSchema";
import { categoryOffsetsSpending, offsetCategoryOptions } from "@/lib/provenance";
import { queryKeys } from "@/lib/queryKeys";
import { NO_LINKS, ReimbursementLinksField, linksRequest } from "@/components/transactions/ReimbursementLinksField";
import { formatNumberWithDots, localDatetimeToISO, stripGrouping, toDatetimeLocal } from "@/lib/utils";
import { isExpression, parseAmountExpression } from "./amount";
import { jakartaDate, jakartaToday } from "./creditTerms";
import {
  CreditTermsFields, INITIAL_CREDIT_TERMS, creditAccountFacts, creditTermsIssue, creditTermsRequest, type CreditTermsValue,
} from "./CreditTermsFields";
import type { CaptureMode, FooterState } from "./types";

type FieldKey =
  | "amount" | "account" | "time" | "notes" | "goal" | "debt" | "source" | "target" | "fee" | "count" | "firstDue";
type FieldErrors = Partial<Record<FieldKey, string>>;
type Submission = { kind: "movement"; payload: CreateMovementPayload } | { kind: "transaction" };

const QUICK_ADD = [10_000, 20_000, 50_000, 100_000, 500_000];
const PILLAR_HELP = {
  need: "kakeibo.needHelp",
  want: "kakeibo.wantHelp",
  culture: "kakeibo.cultureHelp",
  unexpected: "kakeibo.unexpectedHelp",
  saving: "kakeibo.savingHelp",
} as const satisfies Record<KakeiboPillar, string>;
type MovementErrorKey = "sourceRequired" | "targetRequired" | "amountRequired" | "transferTimeRequired" | "noteTooLong" | "sameAccount";
const MOVEMENT_ISSUE: Record<MovementIssueCode, { field: FieldKey; key: MovementErrorKey }> = {
  source_required: { field: "source", key: "sourceRequired" },
  target_required: { field: "target", key: "targetRequired" },
  amount_invalid: { field: "amount", key: "amountRequired" },
  date_required: { field: "time", key: "transferTimeRequired" },
  notes_too_long: { field: "notes", key: "noteTooLong" },
  same_account: { field: "target", key: "sameAccount" },
};
const FIELD_ID: Record<FieldKey, string> = {
  amount: "capture-amount",
  account: "capture-account",
  time: "capture-date",
  notes: "capture-notes",
  goal: "capture-goal",
  debt: "capture-debt-0",
  source: "capture-source",
  target: "capture-target",
  fee: "capture-fee",
  count: "capture-credit-count",
  firstDue: "capture-credit-due",
};
const SPLIT_ERROR = { positive: "errors.positive", total: "errors.total" } as const;
/** Fields inside "Detail tambahan", which opens itself when one of them holds an error (D7). */
const DETAIL_FIELDS = new Set<FieldKey>(["time", "notes", "goal", "debt"]);

function accountId(account: { id?: unknown; account_id?: unknown }): string {
  return String(account.id ?? account.account_id ?? "");
}

function apiCode(failure: unknown): string | undefined {
  if (failure instanceof ApiError && typeof failure.detail === "object") return failure.detail.code;
  return undefined;
}

/**
 * "Catat transaksi" (apply-markas-catat D6 to D12): Pengeluaran, Pemasukan and Perpindahan with the 56 px amount,
 * "Tambah cepat", each mode's fields, the credit block under "Bayar dari" and the credit bill parts of Perpindahan.
 * Every request equals the former form's for the same input; an expense on a credit account adds `credit_terms`.
 */
export function ManualView({
  active,
  formId,
  initialMode,
  initialSourceAccountId,
  initialTargetAccountId,
  initialAmount,
  onFooter,
  onMeta,
  onSaved,
}: {
  active: boolean;
  formId: string;
  initialMode: CaptureMode;
  initialSourceAccountId?: string;
  initialTargetAccountId?: string;
  initialAmount?: number | null;
  onFooter: (state: FooterState) => void;
  /** The dialog's meta line: the credit bill of Perpindahan into a credit account (D12), or nothing. */
  onMeta: (meta: React.ReactNode) => void;
  onSaved: () => void;
}) {
  const t = useTranslations("capture");
  const words = useTranslations();
  const debtWords = useTranslations("transactions.debts");
  const { hideBalances } = useAppCtx();
  const money = useMoney();
  const format = useFormat();
  const accountLabels = useAccountOptionLabels();
  const qc = useQueryClient();
  const { showToast } = useToast();
  // The success pause outlives a fast unmount (navigation, tests); clearing it keeps setState off an unmounted form.
  const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => {
    if (closeTimerRef.current) clearTimeout(closeTimerRef.current);
  }, []);

  const [type, setType] = useState<CaptureMode>(initialMode);
  const [amountStr, setAmountStr] = useState(initialAmount && initialAmount > 0 ? formatNumberWithDots(String(initialAmount)) : "");
  const [selectedAccount, setSelectedAccount] = useState("");
  const [sourceId, setSourceId] = useState(initialSourceAccountId ?? "");
  const [targetId, setTargetId] = useState(initialTargetAccountId ?? "");
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null);
  const [offsetCategoryId, setOffsetCategoryId] = useState("");
  // The expenses a new reimbursement repays (link-reimbursements-to-expenses D8).
  const [links, setLinks] = useState(NO_LINKS);
  const [kakeiboType, setKakeiboType] = useState<KakeiboPillar>("need");
  const [hasKakeiboOverride, setHasKakeiboOverride] = useState(false);
  const [selectedGoalId, setSelectedGoalId] = useState("");
  const [debtRows, setDebtRows] = useState<DebtAllocationValue[]>([]);
  const [notes, setNotes] = useState("");
  const [feeStr, setFeeStr] = useState("");
  const [receipt, setReceipt] = useState<File | null>(null);
  const [savedTransactionId, setSavedTransactionId] = useState<string | null>(null);
  const [txDate, setTxDate] = useState(() => toDatetimeLocal(new Date()));
  const [credit, setCredit] = useState<CreditTermsValue>(INITIAL_CREDIT_TERMS);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [fieldErrors, setFieldErrors] = useState<FieldErrors>({});
  const [err, setErr] = useState("");
  const [isSuccess, setIsSuccess] = useState(false);

  const accountsData = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") }).data;
  const categoriesData = useQuery<{ categories: any[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") }).data;
  const goalsData = useQuery<{ goals: any[] }>({ queryKey: queryKeys.goals, queryFn: () => api.get("/goals") }).data;
  const obligationsData = useQuery<{ obligations: any[] }>({ queryKey: queryKeys.obligations, queryFn: () => api.get("/obligations") }).data;

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const activeAccounts = useMemo(() => accounts.filter((account: any) => !account.is_archived), [accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(activeAccounts), [activeAccounts]);
  const liquidAccountIds = useMemo(() => liquidAccounts.map(accountId), [liquidAccounts]);
  // Credit accounts may pay an expense and be either side of a movement, never an income (add-credit-accounts D7).
  const creditAccounts = useMemo(() => listCreditAccountChoices(activeAccounts), [activeAccounts]);
  const creditAccountIds = useMemo(() => creditAccounts.map(accountId), [creditAccounts]);
  const movementAccountIds = useMemo(() => [...liquidAccountIds, ...creditAccountIds], [liquidAccountIds, creditAccountIds]);
  const accountChoices = useMemo(
    () => (type === "expense" ? [...liquidAccounts, ...creditAccounts] : liquidAccounts),
    [creditAccounts, liquidAccounts, type],
  );
  const isCreditId = (id: string) => creditAccountIds.includes(id);
  const sourceIsCredit = isCreditId(sourceId);
  const targetIsCredit = isCreditId(targetId);
  const accountIsCredit = type === "expense" && isCreditId(selectedAccount);
  const categories = useMemo(() => categoriesData?.categories ?? [], [categoriesData?.categories]);
  const availableGoals = useMemo(() => (goalsData?.goals ?? []).filter((goal: any) => !goal.is_archived), [goalsData?.goals]);
  const availableObligations = useMemo(
    () => (obligationsData?.obligations ?? []).filter((obligation: any) => !obligation.is_archived && obligation.remaining_amount > 0),
    [obligationsData?.obligations],
  );
  const availableCategories = useMemo(
    () => (type === "movement" ? [] : categories.filter((category: any) => category.kind === type && !category.is_archived)),
    [categories, type],
  );

  // The first account the form offers when none is chosen yet, as the former form did.
  useEffect(() => {
    if (accountChoices.length > 0 && !accountChoices.some((account) => accountId(account) === selectedAccount)) {
      setSelectedAccount(accountId(accountChoices[0]));
    }
  }, [accountChoices, selectedAccount]);

  useEffect(() => {
    if (liquidAccountIds.length === 0) return;
    setSourceId((current) => {
      if (movementAccountIds.includes(current)) return current;
      if (initialSourceAccountId && movementAccountIds.includes(initialSourceAccountId)) return initialSourceAccountId;
      return liquidAccountIds[0];
    });
  }, [initialSourceAccountId, liquidAccountIds, movementAccountIds]);

  useEffect(() => {
    if (liquidAccountIds.length === 0) return;
    let source = liquidAccountIds[0];
    if (movementAccountIds.includes(sourceId)) source = sourceId;
    else if (initialSourceAccountId && movementAccountIds.includes(initialSourceAccountId)) source = initialSourceAccountId;
    // One credit account cannot pay another: a credit source keeps only cash targets (D6).
    const targets = creditAccountIds.includes(source) ? liquidAccountIds : movementAccountIds;
    setTargetId((current) => {
      if (targets.includes(current) && current !== source) return current;
      if (initialTargetAccountId && targets.includes(initialTargetAccountId) && initialTargetAccountId !== source) return initialTargetAccountId;
      return defaultMovementTargetId(source, liquidAccounts);
    });
  }, [creditAccountIds, initialSourceAccountId, initialTargetAccountId, liquidAccountIds, liquidAccounts, movementAccountIds, sourceId]);

  const parsedAmount = parseAmountExpression(amountStr);
  const parsedFee = parseAmountExpression(feeStr);
  const offsetsSpending = type === "income" && categoryOffsetsSpending(categories, selectedCategory);
  const offsetCategories = useMemo(() => offsetCategoryOptions(categories), [categories]);
  const hasSelectedDebt = type === "expense" && debtRows.some((row) => row.obligation_id);
  const purchaseDate = jakartaDate(localDatetimeToISO(txDate) || new Date());
  const creditAccount = accountIsCredit ? activeAccounts.find((account: any) => accountId(account) === selectedAccount) : null;
  const targetAccount = type === "movement" && targetIsCredit ? activeAccounts.find((account: any) => accountId(account) === targetId) : null;
  const nextBill = targetAccount?.next_due ?? null;
  const showsAmountHint = isExpression(amountStr) && parsedAmount > 0;

  const clearError = (field: FieldKey) => setFieldErrors((current) => (current[field] ? { ...current, [field]: undefined } : current));

  const fail = (field: FieldKey, message: string) => {
    setFieldErrors({ [field]: message });
    if (DETAIL_FIELDS.has(field)) setDetailsOpen(true);
    window.setTimeout(() => document.getElementById(FIELD_ID[field])?.focus(), 0);
  };

  const handleCategoryChange = (categoryId: string | null) => {
    setSelectedCategory(categoryId);
    setOffsetCategoryId("");
    setLinks(NO_LINKS);
    const category = categories.find((item: any) => item.id === categoryId);
    if (type === "expense" && !hasKakeiboOverride) setKakeiboType(pillarOrNeed(category?.kakeibo_type));
  };

  const handleTypeChange = (nextType: CaptureMode) => {
    setType(nextType);
    setSelectedCategory(null);
    setOffsetCategoryId("");
    setLinks(NO_LINKS);
    setHasKakeiboOverride(false);
    setErr("");
    setFieldErrors({});
    setFeeStr("");
    if (nextType !== "expense") {
      setSelectedGoalId("");
      setDebtRows([]);
    }
    setKakeiboType("need");
  };

  const mutation = useMutation({
    mutationFn: async (submission: Submission) => {
      if (submission.kind === "movement") {
        const movement = await createMovement(submission.payload);
        return { kind: "movement" as const, movement };
      }
      if (savedTransactionId && receipt) {
        await uploadTransactionReceipt(savedTransactionId, receipt);
        return { kind: "transaction" as const, transaction: { ok: true, transaction_id: savedTransactionId, over_limit: false }, receiptWarning: undefined };
      }
      const created = await api.post<{ ok: boolean; transaction_id: string; over_limit?: boolean }>("/transactions", {
        type,
        amount: parsedAmount,
        account_id: selectedAccount,
        category_id: selectedCategory || null,
        kakeibo_type: type !== "income" ? kakeiboType : null,
        goal_id: selectedGoalId || null,
        obligation_id: type === "expense" && debtRows.length === 1 ? debtRows[0].obligation_id : null,
        ...(type === "expense" && debtRows.length > 1
          ? { obligation_allocations: debtRows.map((row) => ({ obligation_id: row.obligation_id, amount: Number(stripGrouping(row.amount)) })) }
          : {}),
        notes: notes.trim() || null,
        ...(offsetsSpending ? { offset_category_id: links.expenses.length > 0 ? null : offsetCategoryId || null } : {}),
        date: localDatetimeToISO(txDate) || new Date().toISOString(),
        ...(accountIsCredit ? { credit_terms: creditTermsRequest(credit) } : {}),
      });
      const linking = offsetsSpending ? linksRequest(links) : null;
      if (linking) {
        try {
          await api.put(`/transactions/${created.transaction_id}/reimburses`, linking);
        } catch (linkError) {
          // The row is saved; saying so keeps a second save from recording it twice.
          showToast(words("transactions.edit.linksFailed", { error: linkError instanceof Error ? linkError.message : "" }));
        }
      }
      if (receipt) {
        try {
          await uploadTransactionReceipt(created.transaction_id, receipt);
        } catch (uploadError) {
          return {
            kind: "transaction" as const,
            transaction: created,
            receiptWarning: uploadError instanceof Error && uploadError.message ? uploadError.message : t("errors.receiptReason"),
          };
        }
      }
      return { kind: "transaction" as const, transaction: created, receiptWarning: undefined };
    },
    onSuccess: (result) => {
      setIsSuccess(true);
      for (const key of [queryKeys.pulse, queryKeys.accounts, queryKeys.insights, queryKeys.transactions.all, queryKeys.dashboard.all, queryKeys.goals, queryKeys.obligations]) {
        qc.invalidateQueries({ queryKey: key });
      }
      if (result.kind === "transaction" && result.transaction.over_limit) {
        // A purchase over the limit is saved; the owner is only told (add-credit-accounts D5).
        const name = creditAccounts.find((account: any) => accountId(account) === selectedAccount)?.name;
        showToast(t("credit.overLimitToast", { account: name ?? t("credit.creditAccount") }));
      }
      if (result.kind === "transaction" && result.receiptWarning) {
        setIsSuccess(false);
        setSavedTransactionId(result.transaction.transaction_id);
        setErr(t("errors.receiptFailed", { reason: result.receiptWarning }));
        return;
      }
      closeTimerRef.current = setTimeout(() => {
        closeTimerRef.current = null;
        setIsSuccess(false);
        onSaved();
      }, 450);
    },
    onError: (failure: Error) => {
      const code = apiCode(failure);
      // A pair the browser could not see as one account (a stale list) is refused under "Ke" (D13).
      if (code === "same_account") return fail("target", failure.message);
      if (code === "credit_terms_due_before_purchase") return fail("firstDue", failure.message);
      if (code === "credit_terms_count_range") return fail("count", failure.message);
      setErr(failure.message || (type === "movement" ? t("errors.moveFailed") : t("errors.saveFailed")));
      document.getElementById(type === "movement" ? FIELD_ID.source : FIELD_ID.account)?.focus();
    },
  });

  const submitMovement = () => {
    const validation = movementFormSchema.safeParse({ sourceAccountId: sourceId, targetAccountId: targetId, amount: parsedAmount, date: txDate, notes: notes.trim() });
    if (!validation.success) {
      const issue = MOVEMENT_ISSUE[validation.error.issues[0].message as MovementIssueCode];
      return fail(issue.field, t(`errors.${issue.key}`));
    }
    if (liquidAccountIds.length === 0 || movementAccountIds.length < 2) return fail("target", words("capture.hints.needAnotherAccount"));
    if (!movementAccountIds.includes(validation.data.sourceAccountId)) return fail("source", t("errors.sourceInactive"));
    if (!movementAccountIds.includes(validation.data.targetAccountId)) return fail("target", t("errors.targetInactive"));
    if (sourceIsCredit && targetIsCredit) return fail("target", t("errors.bothCredit"));
    const fee = targetIsCredit && parsedFee > 0 ? parsedFee : null;
    if (fee !== null && fee > validation.data.amount) return fail("fee", t("errors.feeOverAmount"));
    mutation.mutate({
      kind: "movement",
      payload: {
        source_account_id: validation.data.sourceAccountId,
        target_account_id: validation.data.targetAccountId,
        amount: validation.data.amount,
        notes: validation.data.notes || null,
        date: localDatetimeToISO(validation.data.date) || new Date().toISOString(),
        ...(fee !== null ? { fee } : {}),
      },
    });
  };

  const submitTransaction = () => {
    if (!(savedTransactionId && receipt)) {
      if (parsedAmount <= 0) return fail("amount", amountStr.trim() ? t("errors.amountInvalid") : t("errors.amountRequired"));
      if (!selectedAccount) return fail("account", t("errors.accountRequired"));
      if (!txDate) return fail("time", t("errors.timeRequired"));
      if (notes.trim().length > 500) return fail("notes", t("errors.noteTooLong"));
      if (selectedGoalId && debtRows.some((row) => row.obligation_id)) return fail("goal", t("errors.goalOrDebt"));
      if (debtRows.some((row) => !row.obligation_id || debtRows.filter((item) => item.obligation_id === row.obligation_id).length > 1)) {
        return fail("debt", t("errors.distinctDebts"));
      }
      const split = allocationIssue(debtRows, parsedAmount, availableObligations);
      if (split) {
        setFieldErrors({ debt: split.code === "over" ? debtWords("errors.over", { name: split.name }) : debtWords(SPLIT_ERROR[split.code]) });
        setDetailsOpen(true);
        window.setTimeout(() => (document.getElementById("capture-debt-0-amount") ?? document.getElementById("capture-edit-amounts"))?.focus(), 0);
        return;
      }
      if (accountIsCredit) {
        const issue = creditTermsIssue(credit, purchaseDate);
        if (issue) return fail(issue.field, t(`errors.${issue.key}`));
      }
    }
    mutation.mutate({ kind: "transaction" });
  };

  const handleSubmit = (event?: React.FormEvent) => {
    event?.preventDefault();
    if (mutation.isPending || isSuccess) return;
    setErr("");
    setFieldErrors({});
    if (type === "movement") submitMovement();
    else submitTransaction();
  };

  let label = type === "movement" ? t("manual.move") : t("manual.save");
  if (isSuccess) label = type === "movement" ? t("saved.transfer") : t("saved.transaction");
  else if (mutation.isPending) label = type === "movement" ? t("manual.moving") : t("manual.saving");
  else if (savedTransactionId && receipt) label = t("manual.retryUpload");
  const disabled = parsedAmount <= 0 || isSuccess || mutation.isPending;
  useEffect(() => {
    if (active) onFooter({ label, disabled });
  }, [active, disabled, label, onFooter]);

  // The bill line in the dialog's meta (D12), rebuilt only when one of its parts changes.
  const billAccount = targetAccount ? String(targetAccount.name ?? "") : null;
  const billLeft = nextBill?.left_to_pay ?? null;
  const billDue = nextBill?.due_date ?? null;
  const billOverdue = nextBill?.status === "overdue";
  useEffect(() => {
    if (!active) return;
    if (billAccount === null) return onMeta(null);
    if (billDue === null || billLeft === null) return onMeta(t("bill.none", { account: billAccount }));
    const amount = maskedTag(money(billLeft));
    if (billOverdue) {
      onMeta(<span className="c-warning">{t.rich("bill.overdue", { account: billAccount, amount, date: format.shortDate(billDue) })}</span>);
      return;
    }
    let when = format.shortDate(billDue);
    if (billDue === jakartaToday()) when = t("bill.today");
    else if (billDue === jakartaToday(1)) when = t("bill.tomorrow");
    onMeta(t.rich("bill.meta", { account: billAccount, amount, when }));
    // `money` and `format` change only with the region or hiding, which `hideBalances` covers.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, billAccount, billDue, billLeft, billOverdue, hideBalances, onMeta, t]);

  const formatBalance = hideBalances ? undefined : money;
  const errorOf = (field: FieldKey) => fieldErrors[field];
  const described = (field: FieldKey, ...extra: (string | false | undefined)[]) =>
    [errorOf(field) ? `${FIELD_ID[field]}-error` : "", ...extra].filter(Boolean).join(" ") || undefined;
  const errorLine = (field: FieldKey) =>
    errorOf(field) ? <p id={`${FIELD_ID[field]}-error`} className="err" role="alert">{errorOf(field)}</p> : null;

  const expenseGroups = KAKEIBO_PILLARS.map((pillar) => ({
    key: pillar.value,
    label: bucketName(words, pillar.value),
    items: availableCategories.filter((category: any) => category.kakeibo_type === pillar.value),
  })).filter((group) => group.items.length > 0);
  const ungrouped = availableCategories.filter((category: any) => !KAKEIBO_PILLARS.some((pillar) => pillar.value === category.kakeibo_type));

  const amountField = (
    <div className="field">
      <label htmlFor={FIELD_ID.amount}>{hasSelectedDebt ? t("fields.amountTotal") : t("fields.amount")}</label>
      <input
        id={FIELD_ID.amount}
        name="amount"
        type="text"
        inputMode="numeric"
        autoComplete="off"
        placeholder="0"
        value={amountStr}
        readOnly={hasSelectedDebt}
        aria-invalid={errorOf("amount") ? true : undefined}
        aria-describedby={described("amount", showsAmountHint && "capture-amount-hint")}
        onChange={(event) => {
          clearError("amount");
          const value = event.target.value;
          setAmountStr(isExpression(value) ? value : formatNumberWithDots(value));
        }}
        className="field-control input-money"
      />
      {errorLine("amount")}
      {showsAmountHint && <p id="capture-amount-hint" className="hint tabular-nums">{t("fields.equals", { amount: format.money(parsedAmount) })}</p>}
    </div>
  );

  const timeField = (labelKey: "time" | "transferTime") => (
    <div className="field">
      <label htmlFor={FIELD_ID.time}>{t(`fields.${labelKey}`)}</label>
      <input id={FIELD_ID.time} name="date" type="datetime-local" step={1} value={txDate}
        aria-invalid={errorOf("time") ? true : undefined} aria-describedby={described("time")}
        onChange={(event) => { setTxDate(event.target.value); clearError("time"); }} className="field-control" />
      {errorLine("time")}
    </div>
  );

  const noteField = (
    <div className="field">
      <label htmlFor={FIELD_ID.notes}>{t("fields.note")}</label>
      <input id={FIELD_ID.notes} name="notes" type="text" maxLength={500} autoComplete="off" placeholder={t("fields.notePlaceholder")} value={notes}
        aria-invalid={errorOf("notes") ? true : undefined} aria-describedby={described("notes")}
        onChange={(event) => { setNotes(event.target.value); clearError("notes"); }} className="field-control" />
      {errorLine("notes")}
    </div>
  );

  const receiptField = (
    <div className="field">
      <label htmlFor="capture-receipt">{t("fields.receipt")}</label>
      <input id="capture-receipt" type="file" accept="image/jpeg,image/png,image/webp,application/pdf" aria-describedby="capture-receipt-hint"
        onChange={(event) => setReceipt(event.target.files?.[0] ?? null)} className="field-control" />
      <p id="capture-receipt-hint" className="hint">{t("hints.receipt")}</p>
    </div>
  );

  // "Bayar nanti" is not stored: it is on exactly when the chosen account is a credit account (ease-credit-entry D2).
  const offersPayLater = type === "expense" && creditAccounts.length > 0 && liquidAccounts.length > 0;
  const choosePayment = (later: boolean) => {
    const next = later ? creditAccountIds[0] : liquidAccountIds[0];
    if (!next || later === accountIsCredit) return;
    setSelectedAccount(next);
    clearError("account");
    // A debt is paid from cash; a credit purchase keeps no debt link (add-credit-accounts D7).
    if (later) setDebtRows([]);
  };
  let accountOptions = (
    <AccountSelectOptions labels={accountLabels} accounts={activeAccounts} formatBalance={formatBalance} allowParentSelection liquidOnly includeCredit={type === "expense"} />
  );
  if (offersPayLater && accountIsCredit) {
    accountOptions = <AccountSelectOptions labels={accountLabels} accounts={activeAccounts} formatBalance={formatBalance} creditOnly />;
  } else if (offersPayLater) {
    accountOptions = <AccountSelectOptions labels={accountLabels} accounts={activeAccounts} formatBalance={formatBalance} allowParentSelection liquidOnly />;
  }

  const accountField = (
    <div className="field">
      <label htmlFor={FIELD_ID.account}>{type === "expense" ? t("fields.payFrom") : t("fields.incomeTo")}</label>
      <select id={FIELD_ID.account} name="account_id" value={selectedAccount} aria-invalid={errorOf("account") ? true : undefined} aria-describedby={described("account")}
        onChange={(event) => {
          setSelectedAccount(event.target.value);
          clearError("account");
          if (isCreditId(event.target.value)) setDebtRows([]);
        }} className="field-control">
        {accountOptions}
      </select>
      {errorLine("account")}
    </div>
  );

  return (
    <form id={formId} hidden={!active} onSubmit={handleSubmit} noValidate className="form-stack capture-manual">
      <RadioSegments
        label={t("manual.kindLabel")}
        className="capture-seg capture-modes"
        options={[
          { value: "expense" as CaptureMode, label: t("modes.expense"), disabled: mutation.isPending || isSuccess },
          { value: "income" as CaptureMode, label: t("modes.income"), disabled: mutation.isPending || isSuccess },
          { value: "movement" as CaptureMode, label: t("modes.transfer"), disabled: mutation.isPending || isSuccess },
        ]}
        value={type}
        onChange={handleTypeChange}
      />

      {amountField}

      {type === "movement" && nextBill && nextBill.left_to_pay > 0 && (
        <div className="quick-chips">
          <button type="button" className="ws-btn" onClick={() => { setAmountStr(formatNumberWithDots(String(nextBill.left_to_pay))); clearError("amount"); }}>
            {t.rich("bill.payLeft", { amount: maskedTag(money(nextBill.left_to_pay)) })}
          </button>
        </div>
      )}

      {!hasSelectedDebt && (
        <div className="quick-chips" role="group" aria-label={t("fields.quickAdd")}>
          {QUICK_ADD.map((increment) => (
            <button key={increment} type="button" className="ws-btn tabular-nums"
              onClick={() => { setAmountStr(formatNumberWithDots(String((parsedAmount || 0) + increment))); clearError("amount"); }}>
              +{increment / 1_000}k
            </button>
          ))}
        </div>
      )}

      {type === "expense" && (
        <>
          {offersPayLater && (
            <div className="field">
              <span className="flabel" id="capture-payment-label">{t("fields.whenToPay")}</span>
              <RadioSegments
                labelledBy="capture-payment-label"
                className="capture-seg"
                options={[
                  { value: "now", label: t("fields.payNow"), disabled: mutation.isPending || isSuccess },
                  { value: "later", label: t("fields.payLater"), disabled: mutation.isPending || isSuccess },
                ]}
                value={accountIsCredit ? "later" : "now"}
                onChange={(value) => choosePayment(value === "later")}
              />
            </div>
          )}
          {accountField}
          {creditAccount && (
            <CreditTermsFields
              account={creditAccountFacts(creditAccount)}
              amount={parsedAmount}
              purchaseDate={purchaseDate}
              value={credit}
              onChange={(next) => { setCredit(next); clearError("count"); clearError("firstDue"); }}
              errors={{ count: errorOf("count"), firstDue: errorOf("firstDue") }}
              money={money}
            />
          )}
          <div className="field">
            <label htmlFor="capture-category">{t("fields.category")}</label>
            <select id="capture-category" name="category_id" value={selectedCategory || ""} onChange={(event) => handleCategoryChange(event.target.value || null)} className="field-control">
              <option value="">{t("fields.categoryNone")}</option>
              {expenseGroups.map((group) => (
                <optgroup key={group.key} label={group.label}>
                  {group.items.map((category: any) => <option key={category.id} value={category.id}>{category.name}</option>)}
                </optgroup>
              ))}
              {ungrouped.length > 0 && (
                <optgroup label={words("terms.noPillarYet")}>
                  {ungrouped.map((category: any) => <option key={category.id} value={category.id}>{category.name}</option>)}
                </optgroup>
              )}
            </select>
          </div>
          <div className="field">
            <span className="flabel" id="capture-pillar-label">{t("fields.pillar")}</span>
            <RadioSegments
              labelledBy="capture-pillar-label"
              className="capture-seg"
              options={KAKEIBO_PILLARS.map((pillar) => ({ value: pillar.value, label: bucketName(words, pillar.value) }))}
              value={kakeiboType}
              onChange={(next) => { setKakeiboType(next); setHasKakeiboOverride(true); }}
            />
            <p className="hint" id="capture-pillar-hint">{words(PILLAR_HELP[kakeiboType])}</p>
          </div>
          <details className="capture-details" open={detailsOpen} onToggle={(event) => setDetailsOpen((event.currentTarget as HTMLDetailsElement).open)}>
            <summary className="ws-btn ws-btn-quiet">
              <span>{t("fields.details")}</span>
              <Icon name="chevD" className="capture-details-chev h-4 w-4" />
            </summary>
            <div className="form-stack">
              {timeField("time")}
              {(availableGoals.length > 0 || selectedGoalId) && (
                <div className="field">
                  <label htmlFor={FIELD_ID.goal}>{t("fields.goal")}</label>
                  <select id={FIELD_ID.goal} name="goal_id" value={selectedGoalId} aria-invalid={errorOf("goal") ? true : undefined}
                    aria-describedby={described("goal", "capture-goal-hint")} className="field-control"
                    onChange={(event) => {
                      setSelectedGoalId(event.target.value);
                      clearError("goal");
                      if (event.target.value) setDebtRows([]);
                    }}>
                    <option value="">{t("fields.goalNone")}</option>
                    {availableGoals.map((goal: any) => <option key={goal.id} value={goal.id}>{goal.name}</option>)}
                  </select>
                  {errorLine("goal")}
                  <p id="capture-goal-hint" className="hint">{t("hints.goal")}</p>
                </div>
              )}
              {!accountIsCredit && availableObligations.length > 0 && (
                <div className="capture-debts">
                  <DebtAllocationEditor idPrefix="capture" debts={availableObligations} rows={debtRows} total={parsedAmount} currency={money}
                    error={errorOf("debt")}
                    onChange={(rows) => {
                      setDebtRows(rows);
                      if (rows.length) setSelectedGoalId("");
                      if (rows.some((row) => row.obligation_id)) setAmountStr(formatNumberWithDots(totalDebtPayments(rows)));
                      setErr("");
                      clearError("debt");
                    }} />
                </div>
              )}
              {noteField}
              {receiptField}
            </div>
          </details>
        </>
      )}

      {type === "income" && (
        <>
          {accountField}
          <div className="field">
            <label htmlFor="capture-category">{t("fields.incomeCategory")}</label>
            <select id="capture-category" name="category_id" value={selectedCategory || ""} onChange={(event) => handleCategoryChange(event.target.value || null)} className="field-control">
              <option value="">{t("fields.categoryNone")}</option>
              {availableCategories.map((category: any) => <option key={category.id} value={category.id}>{category.name}</option>)}
            </select>
          </div>
          {offsetsSpending && (
            <ReimbursementLinksField idPrefix="capture" value={links} onChange={setLinks}
              incomeCategories={categories.filter((category: any) => category.kind === "income" && !category.offsets_spending && !category.is_archived && !category.system_key)} />
          )}
          {offsetsSpending && links.expenses.length === 0 && (
            <div className="field">
              <label htmlFor="capture-offset">{t("fields.offset")}</label>
              <select id="capture-offset" name="offset_category_id" value={offsetCategoryId} onChange={(event) => setOffsetCategoryId(event.target.value)}
                aria-describedby="capture-offset-hint" className="field-control">
                <option value="">{t("fields.offsetNone")}</option>
                {offsetCategories.map((category: any) => <option key={category.id} value={category.id}>{category.name}</option>)}
              </select>
              <p id="capture-offset-hint" className="hint">{t("hints.offset")}</p>
            </div>
          )}
          {timeField("time")}
          {noteField}
          {receiptField}
        </>
      )}

      {type === "movement" && (
        <>
          <fieldset className="field capture-direction">
            <legend className="flabel">{t("fields.direction")}</legend>
            <div className="capture-direction-row">
              <select id={FIELD_ID.source} name="source_account_id" aria-label={t("fields.from")} value={sourceId}
                aria-invalid={errorOf("source") ? true : undefined} aria-describedby={described("source")}
                onChange={(event) => { setSourceId(event.target.value); clearError("source"); }} className="field-control">
                <option value="">{t("fields.chooseAccount")}</option>
                <AccountSelectOptions labels={accountLabels} accounts={activeAccounts} formatBalance={formatBalance} allowParentSelection liquidOnly includeCredit />
              </select>
              <span className="capture-direction-arrow" aria-hidden="true">
                <Icon name="arrowRight" className="h-4 w-4" />
              </span>
              <select id={FIELD_ID.target} name="target_account_id" aria-label={t("fields.to")} value={targetId}
                aria-invalid={errorOf("target") ? true : undefined} aria-describedby={described("target")}
                onChange={(event) => { setTargetId(event.target.value); clearError("target"); }} className="field-control">
                <option value="">{t("fields.chooseAccount")}</option>
                <AccountSelectOptions labels={accountLabels} accounts={activeAccounts} formatBalance={formatBalance} allowParentSelection
                  excludeAccountId={sourceId} liquidOnly includeCredit={!sourceIsCredit} />
              </select>
            </div>
            {errorLine("source")}
            {errorLine("target")}
            {(liquidAccountIds.length === 0 || movementAccountIds.length < 2) && !errorOf("target") && (
              <p className="hint">{t("hints.needAnotherAccount")}</p>
            )}
          </fieldset>
          {targetIsCredit && (
            <div className="field">
              <label htmlFor={FIELD_ID.fee}>{t("fields.fee")}</label>
              <input id={FIELD_ID.fee} name="fee" type="text" inputMode="numeric" autoComplete="off" placeholder="0" value={feeStr}
                aria-invalid={errorOf("fee") ? true : undefined} aria-describedby={described("fee", "capture-fee-hint")}
                onChange={(event) => { setFeeStr(formatNumberWithDots(event.target.value)); clearError("fee"); }} className="field-control tabular-nums" />
              {errorLine("fee")}
              <p id="capture-fee-hint" className="hint">{t("hints.fee")}</p>
            </div>
          )}
          {timeField("transferTime")}
          {noteField}
        </>
      )}

      {err && <p role="alert" className="form-alert">{err}</p>}
      {/* Enter in a field submits; the visible button sits in the dialog's footer. */}
      <button type="submit" hidden tabIndex={-1} aria-hidden="true" />
    </form>
  );
}
