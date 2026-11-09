"use client";

import { useId } from "react";
import { useTranslations } from "next-intl";
import { maskedTag } from "@/components/transactions/rows";
import { RadioSegments } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import { MAX_INSTALLMENTS, derivedFirstDueDate, instalmentAmounts } from "./creditTerms";

export type CreditPlan = "1" | "3" | "6" | "12" | "other";

export interface CreditTermsValue {
  plan: CreditPlan;
  /** The count typed under "Lainnya". */
  otherCount: string;
  /** The first due date the owner typed; "" while the derived date is shown. */
  ownerDate: string;
  /** Whether the owner changed the date field; only then is a date sent (D9). */
  edited: boolean;
}

export const INITIAL_CREDIT_TERMS: CreditTermsValue = { plan: "1", otherCount: "", ownerDate: "", edited: false };

const PLAN_COUNTS = ["3", "6", "12"] as const;
const MIN_OTHER_COUNT = 2;

export interface CreditAccountFacts {
  name: string;
  creditLimit: number | null;
  limitLeft: number | null;
  statementDay: number | null;
  paymentDay: number | null;
}

/** The facts the credit block reads from a `GET /api/accounts` row. */
export function creditAccountFacts(account: any): CreditAccountFacts {
  const creditLimit = typeof account?.credit_limit === "number" ? account.credit_limit : null;
  const owed = typeof account?.credit_owed === "number" ? account.credit_owed : Math.max(0, -(account?.balance ?? 0));
  return {
    name: String(account?.name ?? account?.account_name ?? ""),
    creditLimit,
    limitLeft: creditLimit === null ? null : typeof account?.limit_left === "number" ? account.limit_left : creditLimit - owed,
    statementDay: account?.statement_day ?? null,
    paymentDay: account?.payment_day ?? null,
  };
}

/** The instalment count chosen, or NaN while "Lainnya" holds no number. */
export function chosenCount(value: CreditTermsValue): number {
  if (value.plan === "other") return value.otherCount.trim() ? Number(value.otherCount) : Number.NaN;
  return Number(value.plan);
}

/** What is wrong with the terms before saving, as a `capture.errors` key and the field it belongs to. */
export function creditTermsIssue(value: CreditTermsValue, purchaseDate: string): { key: "countRange" | "dueBeforePurchase"; field: "count" | "firstDue" } | null {
  const count = chosenCount(value);
  if (value.plan === "other" && (!Number.isInteger(count) || count < MIN_OTHER_COUNT || count > MAX_INSTALLMENTS)) {
    return { key: "countRange", field: "count" };
  }
  if (value.edited && value.ownerDate && value.ownerDate < purchaseDate) return { key: "dueBeforePurchase", field: "firstDue" };
  return null;
}

/**
 * The `credit_terms` of `POST /api/transactions` for an expense on a credit account (D9): always the count (1 for
 * "Sekali bayar"), and the first due date only when the owner set or edited it. A derived date is never sent.
 */
export function creditTermsRequest(value: CreditTermsValue): { installment_count: number; first_due_date?: string } {
  const terms: { installment_count: number; first_due_date?: string } = { installment_count: chosenCount(value) };
  if (value.edited && value.ownerDate) terms.first_due_date = value.ownerDate;
  return terms;
}

/**
 * "Bayar pakai kredit" (add-credit-spending-flow H11, apply-markas-catat D9 to D11): the limit left, how the purchase
 * is paid, the first due date and one line saying that no cash left yet. The purchase still counts as Pengeluaran on
 * its date; the browser's date and instalments are a preview of the server's rules.
 */
export function CreditTermsFields({
  account,
  amount,
  purchaseDate,
  value,
  onChange,
  errors,
  money,
}: {
  account: CreditAccountFacts;
  amount: number;
  purchaseDate: string;
  value: CreditTermsValue;
  onChange: (value: CreditTermsValue) => void;
  errors: { count?: string; firstDue?: string };
  money: (amount: number) => string;
}) {
  const t = useTranslations("capture");
  const format = useFormat();
  const id = useId();
  const derived = derivedFirstDueDate(purchaseDate, account.statementDay, account.paymentDay);
  const shownDate = value.edited ? value.ownerDate : derived ?? "";
  const count = chosenCount(value);
  const validCount = Number.isInteger(count) && count >= 1 && count <= MAX_INSTALLMENTS;
  const over = account.limitLeft !== null && (account.limitLeft < 0 || amount > account.limitLeft);

  let limitLine: React.ReactNode = null;
  if (account.limitLeft !== null) {
    if (account.limitLeft < 0) limitLine = <span className="c-warning">{t("credit.overLimit")}</span>;
    else if (over) limitLine = <span className="c-warning">{t.rich("credit.overLimitBy", { amount: maskedTag(money(amount - account.limitLeft)) })}</span>;
    else limitLine = t.rich("credit.limitLeft", { amount: maskedTag(money(account.limitLeft)) });
  }

  let line: React.ReactNode;
  if (!shownDate) {
    line = t("credit.lineNoDate", { account: account.name });
  } else if (!validCount || count === 1 || amount <= 0) {
    line = t("credit.lineFull", { account: account.name, date: format.shortDate(shownDate) });
  } else {
    const [first, rest] = instalmentAmounts(amount, count);
    line = first === rest
      ? t.rich("credit.lineInstallments", { count, date: format.shortDate(shownDate), first: maskedTag(money(first)) })
      : t.rich("credit.lineInstallmentsRemainder", { count, date: format.shortDate(shownDate), first: maskedTag(money(first)), rest: maskedTag(money(rest)) });
  }

  const countId = "capture-credit-count";
  const dueId = "capture-credit-due";
  return (
    <fieldset className="capture-credit form-stack">
      <legend className="capture-credit-legend">
        <span className="t-strong" id={`${id}-legend`}>{t("credit.legend")}</span>
        {limitLine && <span className="t-meta capture-credit-limit">{limitLine}</span>}
      </legend>
      <div className="field">
        <span className="flabel" id={`${id}-plan`}>{t("credit.planLabel")}</span>
        <RadioSegments
          labelledBy={`${id}-legend ${id}-plan`}
          className="capture-seg"
          options={[
            { value: "1" as CreditPlan, label: t("credit.inFull") },
            ...PLAN_COUNTS.map((times) => ({ value: times as CreditPlan, label: t("credit.times", { times }) })),
            { value: "other" as CreditPlan, label: t("credit.other") },
          ]}
          value={value.plan}
          onChange={(plan) => onChange({ ...value, plan })}
        />
      </div>
      {value.plan === "other" && (
        <div className="field">
          <label htmlFor={countId}>{t("credit.count")}</label>
          <input id={countId} type="number" inputMode="numeric" min={MIN_OTHER_COUNT} max={MAX_INSTALLMENTS} value={value.otherCount}
            onChange={(event) => onChange({ ...value, otherCount: event.target.value })}
            aria-invalid={errors.count ? true : undefined} aria-describedby={errors.count ? `${countId}-error` : undefined}
            className="field-control tabular-nums capture-count" />
          {errors.count && <p id={`${countId}-error`} className="err" role="alert">{errors.count}</p>}
        </div>
      )}
      <div className="field">
        <label htmlFor={dueId}>{t("credit.firstDue")}</label>
        <div className="capture-due-row">
          <input id={dueId} type="date" value={shownDate} min={purchaseDate}
            onChange={(event) => onChange({ ...value, ownerDate: event.target.value, edited: true })}
            aria-invalid={errors.firstDue ? true : undefined}
            aria-describedby={[errors.firstDue ? `${dueId}-error` : "", `${dueId}-hint`, !derived ? `${dueId}-setup` : ""].filter(Boolean).join(" ")}
            className="field-control tabular-nums" />
          {value.edited && derived && (
            <button type="button" className="ws-btn ws-btn-quiet" onClick={() => onChange({ ...value, ownerDate: "", edited: false })}>
              {t("credit.useBillDate")}
            </button>
          )}
        </div>
        {errors.firstDue && <p id={`${dueId}-error`} className="err" role="alert">{errors.firstDue}</p>}
        {!derived && <p id={`${dueId}-setup`} className="hint">{t("credit.noPaymentDay")}</p>}
        <p id={`${dueId}-hint`} className="hint capture-pay-later">{line}</p>
      </div>
    </fieldset>
  );
}
