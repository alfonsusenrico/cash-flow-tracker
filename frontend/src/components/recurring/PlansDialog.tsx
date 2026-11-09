"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { listCreditAccountChoices, listLiquidAccountChoices } from "@/lib/accountOptions";
import { parseRecurringRuleForm } from "@/lib/recurringForm";
import { type InvestmentScheduleDraft, listAmountInvestmentProducts } from "@/lib/investmentTopups";
import { cn } from "@/lib/utils";
import type { DueExpectation, ExpensePlanDraft, RecurringRule } from "@/types/recurring";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { jakartaDay, useAccountOptionLabels } from "@/components/transactions/rows";
import { ListRow } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { RadioSegments } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { PHONE_QUERY, useMediaQuery } from "@/components/workspace/layers";
import { DuePlanActions } from "./DuePlanActions";

interface Props {
  open: boolean;
  onClose: () => void;
  investmentDraft?: InvestmentScheduleDraft | null;
  expenseDraft?: ExpensePlanDraft | null;
  /** The plan shown in the detail when the dialog opens, such as a bill row's plan on Target & Tagihan. */
  planId?: string | null;
}

type Filter = "all" | "debt" | "routine";
type ScheduleType = RecurringRule["schedule_type"];
/** The detail column: nothing, a new plan, or an existing plan by id. */
type Selection = null | "new" | string;

const WEEKDAYS = ["1", "2", "3", "4", "5", "6", "7"] as const;
const FORM_ID = "plan-form";

function accountId(account: { id?: string; account_id?: string } | undefined): string {
  return String(account?.id ?? account?.account_id ?? "");
}

function errorText(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

/**
 * "Rencana & Pengingat" (apply-markas-target D20): the plans as a list beside the chosen plan's form, an 880 px
 * dialog and a full-height sheet on the phone. Plans are reminders: nothing here posts a transaction, and a due
 * occurrence is linked, skipped or recorded only by the owner through `DuePlanActions`. Its callers (Target &
 * Tagihan, Rekening, Analisis, Beranda and the shell) keep their props; the requests are those of the old modal.
 */
export function PlansDialog({ open, onClose, investmentDraft, expenseDraft, planId }: Props) {
  const t = useTranslations("plans");
  const format = useFormat();
  const accountLabels = useAccountOptionLabels();
  const qc = useQueryClient();
  const phone = useMediaQuery(PHONE_QUERY);
  const detailHeading = useRef<HTMLHeadingElement>(null);

  const [filter, setFilter] = useState<Filter>("all");
  const [selection, setSelection] = useState<Selection>(null);

  const [name, setName] = useState("");
  const [type, setType] = useState<RecurringRule["type"]>("transfer");
  const [amountText, setAmountText] = useState("");
  const [sourceAccountId, setSourceAccountId] = useState("");
  const [targetAccountId, setTargetAccountId] = useState("");
  const [categoryId, setCategoryId] = useState("");
  const [obligationId, setObligationId] = useState("");
  const [scheduleType, setScheduleType] = useState<ScheduleType>("monthly_day");
  const [scheduleDay, setScheduleDay] = useState<number>(25);
  // Only an existing plan may keep the retired fixed-payday schedule.
  const [legacyPaydaySchedule, setLegacyPaydaySchedule] = useState(false);
  const [isActive, setIsActive] = useState(true);
  const [notes, setNotes] = useState("");
  const [formError, setFormError] = useState("");
  const [listError, setListError] = useState("");

  const editingRuleId = selection !== null && selection !== "new" ? selection : null;
  const detailOpen = selection !== null;

  const { data: rulesData } = useQuery<{ ok: boolean; rules: RecurringRule[] }>({
    queryKey: queryKeys.recurring.all,
    queryFn: () => api.get("/recurring"),
    enabled: open,
  });
  const { data: pendingData } = useQuery<{ rules: DueExpectation[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    enabled: open,
  });
  const { data: accountsData } = useQuery<{ accounts?: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });
  const { data: categoriesData } = useQuery<{ categories?: any[] }>({
    queryKey: queryKeys.categories,
    queryFn: () => api.get("/categories"),
    enabled: open,
  });
  // Plans pay active debts only, so the picker keeps reading the active list (D9 gives Target its own key).
  const { data: obligationsData } = useQuery<{ obligations?: any[] }>({
    queryKey: queryKeys.obligations,
    queryFn: () => api.get("/obligations"),
    enabled: open,
  });

  const rules = useMemo(() => rulesData?.rules ?? [], [rulesData?.rules]);
  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);
  // A recurring expense may be paid on credit; incomes, transfers and top-ups stay on cash (add-credit-accounts D7).
  const creditAccounts = useMemo(() => listCreditAccountChoices(accounts), [accounts]);
  const sourceChoices = useMemo(
    () => (type === "expense" ? [...liquidAccounts, ...creditAccounts] : liquidAccounts),
    [creditAccounts, liquidAccounts, type],
  );
  const sourceIsCredit = creditAccounts.some((account: any) => accountId(account) === sourceAccountId);
  const investmentProducts = useMemo(() => listAmountInvestmentProducts(accounts), [accounts]);
  const targetChoices = type === "investment_topup" ? investmentProducts : liquidAccounts;
  const categories = categoriesData?.categories ?? [];
  const obligations = (obligationsData?.obligations ?? []).filter((obligation: any) => !obligation.is_archived);
  const editingRule = editingRuleId ? rules.find((rule) => rule.id === editingRuleId) ?? null : null;
  const dueOccurrence = editingRuleId ? (pendingData?.rules ?? []).find((rule) => rule.id === editingRuleId) ?? null : null;
  const today = jakartaDay(new Date());

  const resetForm = () => {
    setName("");
    setType("transfer");
    setAmountText("");
    setSourceAccountId("");
    setTargetAccountId("");
    setCategoryId("");
    setObligationId("");
    setScheduleType("monthly_day");
    setScheduleDay(25);
    setLegacyPaydaySchedule(false);
    setIsActive(true);
    setNotes("");
    setFormError("");
  };

  const choose = (rule: RecurringRule) => {
    setSelection(rule.id);
    setName(rule.name);
    setType(rule.type);
    setAmountText(format.groupDigits(rule.amount));
    setSourceAccountId(rule.source_account_id);
    setTargetAccountId(rule.target_account_id ?? "");
    setCategoryId(rule.category_id ?? "");
    setObligationId(rule.obligation_id ?? "");
    setScheduleType(rule.schedule_type);
    setScheduleDay(rule.schedule_day ?? (rule.schedule_type === "weekly" ? 1 : 25));
    setLegacyPaydaySchedule(rule.schedule_type === "payday");
    setIsActive(rule.is_active);
    setNotes(rule.notes ?? "");
    setFormError("");
  };

  const startNewRule = () => {
    resetForm();
    setSelection("new");
  };

  // On the phone the detail sits under the list, so focus follows the choice there.
  useEffect(() => {
    if (phone && detailOpen) detailHeading.current?.focus();
  }, [phone, detailOpen, selection]);

  // The chosen plan, once the list has loaded; asked for once per opening.
  const planChosen = useRef(false);
  useEffect(() => {
    if (!open) {
      planChosen.current = false;
      return;
    }
    if (!planId || planChosen.current) return;
    const rule = rules.find((item) => item.id === planId);
    if (!rule) return;
    planChosen.current = true;
    choose(rule);
    // `choose` only sets state; the list decides when the plan can be chosen.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, planId, rules]);

  // A draft opens the new plan's form directly, filled in; the owner still saves it.
  const draftInitialized = useRef(false);
  useEffect(() => {
    if (!open) {
      draftInitialized.current = false;
      setSelection(null);
      setListError("");
      return;
    }
    if (draftInitialized.current) return;
    if (investmentDraft) {
      draftInitialized.current = true;
      resetForm();
      setSelection("new");
      setType("investment_topup");
      setName(t("form.topupName", { product: investmentDraft.productName }).slice(0, 100));
      setAmountText(format.groupDigits(investmentDraft.amount));
      setSourceAccountId(investmentDraft.sourceAccountId);
      setTargetAccountId(investmentDraft.targetAccountId);
      setScheduleDay(0);
    } else if (expenseDraft) {
      draftInitialized.current = true;
      resetForm();
      setSelection("new");
      setType("expense");
      setName(expenseDraft.name.slice(0, 100));
      setAmountText(format.groupDigits(expenseDraft.amount));
      setSourceAccountId(expenseDraft.source_account_id);
      setCategoryId(expenseDraft.category_id ?? "");
      setScheduleType(expenseDraft.schedule_type);
      setScheduleDay(expenseDraft.schedule_day);
    }
    // The drafts are read once per opening; later renders must not refill an edited form.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, investmentDraft, expenseDraft]);

  // Keeps the account pickers on choices that exist, as the old form did.
  useEffect(() => {
    if (!open || liquidAccounts.length === 0) return;
    if ((investmentDraft || expenseDraft) && !detailOpen) return;
    const firstSourceId = accountId(sourceChoices[0]);
    const effectiveSourceId = sourceChoices.some((account: any) => accountId(account) === sourceAccountId) ? sourceAccountId : firstSourceId;
    setSourceAccountId((current) => (sourceChoices.some((account: any) => accountId(account) === current) ? current : firstSourceId));
    setTargetAccountId((current) => {
      if (targetChoices.some((account) => accountId(account) === current && current !== effectiveSourceId)) return current;
      const target = targetChoices.find((account) => accountId(account) !== effectiveSourceId);
      return target ? accountId(target) : "";
    });
  }, [liquidAccounts, sourceChoices, targetChoices, open, sourceAccountId, investmentDraft, expenseDraft, detailOpen]);

  const handleTypeChange = (nextType: RecurringRule["type"]) => {
    setType(nextType);
    setCategoryId("");
    setObligationId("");
    if (nextType !== "transfer") setTargetAccountId("");
    if (nextType === "investment_topup") {
      setScheduleType("monthly_day");
      setScheduleDay(0);
    }
  };

  const handleScheduleTypeChange = (nextSchedule: ScheduleType) => {
    setScheduleType(nextSchedule);
    if (nextSchedule === "weekly" && (scheduleDay < 1 || scheduleDay > 7)) setScheduleDay(1);
  };

  /** The form's first problem in the interface language, from the field the shared rule names. */
  const issueText = (field: string | number | undefined, rule: { targetAccountId: string | null; sourceAccountId: string }) => {
    switch (field) {
      case "name":
        return name.trim() ? t("errors.nameLength") : t("errors.name");
      case "amount":
        return t("errors.amount");
      case "sourceAccountId":
        return t("errors.source");
      case "targetAccountId":
        return rule.targetAccountId && rule.targetAccountId === rule.sourceAccountId ? t("errors.sameAccount") : t("errors.target");
      case "categoryId":
        return type === "investment_topup" ? t("errors.topupLinks") : t("errors.category");
      case "obligationId":
        return t("errors.debtExpenseOnly");
      case "scheduleDay":
        if (scheduleType === "payday") return t("errors.payday");
        return scheduleType === "weekly" ? t("errors.weekday") : t("errors.monthDay");
      case "notes":
        return t("errors.notesLength");
      default:
        return t("errors.check");
    }
  };

  const saveMutation = useMutation({
    mutationFn: async () => {
      setFormError("");
      const input = {
        name,
        type,
        amount: Number(amountText.replace(/[^0-9]/g, "")) || 0,
        sourceAccountId,
        targetAccountId: type === "transfer" || type === "investment_topup" ? targetAccountId || null : null,
        categoryId: type === "transfer" || type === "investment_topup" ? null : categoryId || null,
        // A debt is paid from cash; the hidden debt choice of a credit source is not sent.
        obligationId: sourceIsCredit ? null : obligationId || null,
        scheduleType,
        scheduleDay: scheduleType === "payday" ? null : Number(scheduleDay),
        isActive,
        notes: notes.trim(),
      };
      const validation = parseRecurringRuleForm(input);
      if (!validation.success) throw new Error(issueText(validation.error.issues[0]?.path[0], input));
      const rule = validation.data;
      const payload = {
        name: rule.name,
        type: rule.type,
        amount: rule.amount,
        source_account_id: rule.sourceAccountId,
        target_account_id: rule.targetAccountId,
        category_id: rule.categoryId,
        obligation_id: rule.obligationId,
        schedule_type: rule.scheduleType,
        schedule_day: rule.scheduleDay,
        is_active: rule.isActive,
        notes: rule.notes || null,
      };
      return editingRuleId ? api.patch(`/recurring/${editingRuleId}`, payload) : api.post("/recurring", payload);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      // Plans feed dashboard commitments and the "sudah ada rencana" marks on detected charges.
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      // An edited plan stays open; a new one returns to the list, as the old form did.
      if (!editingRuleId) {
        resetForm();
        setSelection(null);
      }
    },
    onError: (error: unknown) => setFormError(errorText(error, t("errors.saveFailed"))),
  });

  const toggleMutation = useMutation({
    mutationFn: async (id: string) => api.post(`/recurring/${id}/toggle`),
    onSuccess: () => {
      setListError("");
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
    },
    onError: (error: unknown) => setListError(errorText(error, t("errors.toggleFailed"))),
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => api.del(`/recurring/${id}`),
    onSuccess: (_result, id) => {
      setListError("");
      if (selection === id) setSelection(null);
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
    },
    onError: (error: unknown) => setListError(errorText(error, t("errors.deleteFailed"))),
  });

  const filteredRules = rules.filter((rule) => {
    if (filter === "debt") return Boolean(rule.obligation_id);
    if (filter === "routine") return !rule.obligation_id;
    return true;
  });

  const statusOf = (rule: RecurringRule) => {
    if (!rule.is_active) return { text: t("status.inactive"), tone: "" };
    if (rule.next_due_date < today) return { text: t("status.overdue"), tone: "c-expense" };
    if (rule.next_due_date === today) return { text: t("status.dueToday"), tone: "c-warning" };
    return { text: t("status.scheduled"), tone: "" };
  };
  const scheduleOf = (rule: RecurringRule) => {
    if (rule.schedule_type === "payday") return t("schedule.payday");
    if (rule.schedule_type === "weekly") return t("schedule.weekly");
    return t("schedule.monthly", { day: String(rule.schedule_day ?? "") });
  };
  const rowMeta = (rule: RecurringRule) => {
    const status = statusOf(rule);
    return t.rich("row.meta", {
      statusText: status.text,
      date: format.shortDate(rule.next_due_date),
      schedule: scheduleOf(rule),
      status: (chunks) => <span className={status.tone || undefined}>{chunks}</span>,
    });
  };

  const list = (
    <div className="rc-list">
      {listError && (
        <p role="alert" className="form-alert">
          {listError}
        </p>
      )}
      {filteredRules.length === 0 ? (
        <p className="t-body muted">{rules.length === 0 ? t("dialog.empty") : t("dialog.emptyFilter")}</p>
      ) : (
        <ul className="list" aria-label={t("dialog.listLabel")}>
          {filteredRules.map((rule) => (
            <li key={rule.id}>
              <ListRow
                onClick={() => choose(rule)}
                current={rule.id === editingRuleId}
                rowKey={`plan-${rule.id}`}
                title={rule.name}
                meta={rowMeta(rule)}
                amount={<MaskedMoney text={format.money(rule.amount)} className="t-amt" />}
              />
            </li>
          ))}
        </ul>
      )}
    </div>
  );

  const isTransfer = type === "transfer" || type === "investment_topup";
  const statusLine = editingRule && (
    <>
      <p className="t-meta">
        {statusOf(editingRule).text} · {t("row.next", { date: format.shortDate(editingRule.next_due_date) })} · {scheduleOf(editingRule)}
      </p>
      <p className="t-meta">
        {editingRule.type === "transfer" || editingRule.type === "investment_topup"
          ? t("row.route", { from: editingRule.source_account_name ?? "", to: editingRule.target_account_name ?? "" })
          : t("row.spending", { account: editingRule.source_account_name ?? "", category: editingRule.category_name || t("row.general") })}
        {editingRule.type === "investment_topup" && ` · ${t("mark.topup")}`}
        {editingRule.obligation_id && ` · ${t("mark.installment", { name: editingRule.obligation_name ?? "" })}`}
        {editingRule.is_payroll_allocation && ` · ${t("mark.payroll")}`}
        {editingRule.legacy_auto_post && ` · ${t("mark.legacyAuto")}`}
      </p>
    </>
  );

  const detail = !detailOpen ? (
    <div className="ws-empty rc-empty">
      <h3 className="t-strong">{t("dialog.pick")}</h3>
      <p>{t("dialog.pickLine")}</p>
      <button type="button" className="ws-btn" onClick={startNewRule}>
        {t("dialog.add")}
      </button>
    </div>
  ) : (
    <div className="rc-detail">
      <div className="rc-head">
        <h3 ref={detailHeading} tabIndex={-1} className="t-strong">
          {editingRule ? editingRule.name : t("dialog.newTitle")}
        </h3>
        {statusLine}
      </div>

      <form
        id={FORM_ID}
        className="form-grid"
        noValidate
        onSubmit={(event) => {
          event.preventDefault();
          if (!saveMutation.isPending) saveMutation.mutate();
        }}
      >
        {formError && (
          <p role="alert" aria-live="assertive" className="form-alert full-row">
            {formError}
          </p>
        )}
        <div className="field full-row">
          <label htmlFor="recurring-name">{t("form.name")}</label>
          <input
            id="recurring-name"
            name="name"
            type="text"
            maxLength={100}
            autoComplete="off"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder={t("form.namePlaceholder")}
            className="field-control"
          />
        </div>
        <div className="field">
          <label htmlFor="recurring-type">{t("form.kind")}</label>
          <select id="recurring-type" name="type" value={type} onChange={(event) => handleTypeChange(event.target.value as RecurringRule["type"])} className="field-control">
            <option value="transfer">{t("form.kindTransfer")}</option>
            <option value="expense">{t("form.kindExpense")}</option>
            <option value="income">{t("form.kindIncome")}</option>
            <option value="investment_topup">{t("form.kindTopup")}</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="recurring-amount">{t("form.amount")}</label>
          <input
            id="recurring-amount"
            name="amount"
            type="text"
            inputMode="numeric"
            value={amountText}
            onChange={(event) => setAmountText(format.groupDigits(event.target.value))}
            placeholder={t("form.amountPlaceholder", { amount: format.groupDigits(1_500_000) })}
            className="field-control tabular-nums"
          />
        </div>
        <div className="field">
          <label htmlFor="recurring-source">{type === "income" ? t("form.into") : t("form.from")}</label>
          <select id="recurring-source" name="source_account_id" value={sourceAccountId} onChange={(event) => setSourceAccountId(event.target.value)} className="field-control">
            <option value="">{t("form.chooseAccount")}</option>
            <AccountSelectOptions accounts={accounts} allowParentSelection liquidOnly includeCredit={type === "expense"} labels={accountLabels} />
          </select>
        </div>
        {isTransfer ? (
          <div className="field">
            <label htmlFor="recurring-target">{type === "investment_topup" ? t("form.targetProduct") : t("form.target")}</label>
            <select id="recurring-target" name="target_account_id" value={targetAccountId} onChange={(event) => setTargetAccountId(event.target.value)} className="field-control">
              <option value="">{t("form.chooseTarget")}</option>
              {type === "investment_topup" ? (
                investmentProducts.map((product) => (
                  <option key={product.id} value={product.id}>
                    {product.name}
                  </option>
                ))
              ) : (
                <AccountSelectOptions accounts={accounts} allowParentSelection excludeAccountId={sourceAccountId} liquidOnly labels={accountLabels} />
              )}
            </select>
          </div>
        ) : (
          <div className="field">
            <label htmlFor="recurring-category">{type === "expense" ? t("form.categoryExpense") : t("form.categoryIncome")}</label>
            <select id="recurring-category" name="category_id" value={categoryId} onChange={(event) => setCategoryId(event.target.value)} className="field-control">
              <option value="">{t("form.chooseCategory")}</option>
              {categories
                .filter((category: any) => category.kind === type && !category.is_archived)
                .map((category: any) => (
                  <option key={category.id} value={category.id}>
                    {category.name}
                  </option>
                ))}
            </select>
          </div>
        )}
        {type === "transfer" && <p className="hint t-meta full-row">{t("form.transferHint")}</p>}

        {type === "expense" && !sourceIsCredit && obligations.length > 0 && (
          <div className="field full-row">
            <label htmlFor="recurring-obligation">{t("form.debt")}</label>
            <select
              id="recurring-obligation"
              name="obligation_id"
              value={obligationId}
              onChange={(event) => setObligationId(event.target.value)}
              aria-describedby="recurring-obligation-hint"
              className="field-control"
            >
              <option value="">{t("form.noDebt")}</option>
              {obligations.map((obligation: any) => (
                <option key={obligation.id} value={obligation.id}>
                  {t("form.debtOption", { name: obligation.name, amount: format.money(obligation.remaining_amount) })}
                </option>
              ))}
            </select>
            <p id="recurring-obligation-hint" className="hint">
              {t("form.debtHint")}
            </p>
          </div>
        )}

        <div className="field">
          <label htmlFor="recurring-schedule">{t("form.schedule")}</label>
          <select
            id="recurring-schedule"
            name="schedule_type"
            value={scheduleType}
            onChange={(event) => handleScheduleTypeChange(event.target.value as ScheduleType)}
            className="field-control"
          >
            {legacyPaydaySchedule && <option value="payday">{t("form.schedulePayday")}</option>}
            <option value="monthly_day">{t("form.scheduleMonthly")}</option>
            <option value="weekly">{t("form.scheduleWeekly")}</option>
          </select>
        </div>
        {scheduleType !== "payday" &&
          (scheduleType === "weekly" ? (
            <div className="field">
              <label htmlFor="recurring-schedule-day">{t("form.weekday")}</label>
              <select
                id="recurring-schedule-day"
                name="schedule_day"
                value={scheduleDay}
                onChange={(event) => setScheduleDay(Number(event.target.value))}
                className="field-control"
              >
                {WEEKDAYS.map((day) => (
                  <option key={day} value={Number(day)}>
                    {t(`weekday.${day}`)}
                  </option>
                ))}
              </select>
            </div>
          ) : (
            <div className="field">
              <label htmlFor="recurring-schedule-day">{t("form.monthDay")}</label>
              <input
                id="recurring-schedule-day"
                name="schedule_day"
                type="number"
                min={1}
                max={31}
                required
                value={scheduleDay}
                onChange={(event) => setScheduleDay(Number(event.target.value))}
                className="field-control tabular-nums"
              />
            </div>
          ))}

        <div className="field full-row">
          <label htmlFor="recurring-notes">{t("form.notes")}</label>
          <input id="recurring-notes" name="notes" type="text" value={notes} onChange={(event) => setNotes(event.target.value)} className="field-control" />
        </div>
        {editingRuleId && (
          <label className="field-check full-row">
            <input type="checkbox" name="is_active" checked={isActive} onChange={(event) => setIsActive(event.target.checked)} />
            {t("form.active")}
          </label>
        )}
        <p className="t-meta full-row">{type === "investment_topup" ? t("form.noteTopup") : t("form.noteReminder")}</p>
      </form>

      {editingRule && (
        <div className="rc-actions">
          <button type="button" className="ws-btn" disabled={toggleMutation.isPending} onClick={() => toggleMutation.mutate(editingRule.id)}>
            {editingRule.is_active ? t("form.deactivate") : t("form.activate")}
          </button>
          <ConfirmActionButton
            label={t("form.deleteLabel", { name: editingRule.name })}
            confirmation={t("form.deleteConfirm", { name: editingRule.name })}
            onConfirm={() => deleteMutation.mutate(editingRule.id)}
            className="ws-btn ws-btn-quiet c-expense"
          >
            {t("form.delete")}
          </ConfirmActionButton>
        </div>
      )}

      {dueOccurrence && (
        <section className="rc-due" aria-labelledby="plan-due-title">
          <h3 id="plan-due-title" className="t-label">
            {t("due.title")}
          </h3>
          <div className="min-w-0 space-y-2 text-xs">
            <DuePlanActions rule={dueOccurrence} withName={false} />
          </div>
        </section>
      )}
    </div>
  );

  const footer = (
    <>
      <button type="button" className="ws-btn ws-btn-quiet rc-add" onClick={startNewRule}>
        {t("dialog.add")}
      </button>
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>
        {t("dialog.close")}
      </button>
      {detailOpen && (
        <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink" disabled={saveMutation.isPending}>
          {saveMutation.isPending ? t("dialog.saving") : t("dialog.save")}
        </button>
      )}
    </>
  );

  return (
    <ShellDialog
      open={open}
      onClose={onClose}
      title={t("dialog.title")}
      meta={t("dialog.meta", { count: rules.length })}
      width={880}
      closeLabel={t("dialog.closeLabel")}
      className="plans-dialog"
      fullHeight
      initialFocus={planId ? `[data-row="plan-${planId}"]` : undefined}
      footer={footer}
    >
      <p className="t-body muted">{t("dialog.intro")}</p>
      <RadioSegments
        label={t("filter.label")}
        className="rc-filter"
        value={filter}
        onChange={setFilter}
        options={[
          { value: "all", label: t("filter.all") },
          { value: "debt", label: t("filter.installments") },
          { value: "routine", label: t("filter.routine") },
        ]}
      />
      <div className={cn("rc-grid", detailOpen && "has-detail")}>
        {list}
        {detail}
      </div>
    </ShellDialog>
  );
}
