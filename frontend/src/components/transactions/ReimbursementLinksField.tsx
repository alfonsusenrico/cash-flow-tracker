"use client";

import { useId, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { RadioSegments } from "@/components/workspace/primitives";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { rowTitle, useRowLabels } from "./rows";
import type { LedgerRow, ReimbursementLinks } from "./types";

/** How far back the picker looks for the expenses a reimbursement repays (link-reimbursements-to-expenses D8). */
const PICKER_DAYS = 90;
const PICKER_LIMIT = 20;

export interface PickedExpense {
  id: string;
  title: string;
  date: string;
  amount: number;
}

export interface ReimbursementLinksValue {
  expenses: PickedExpense[];
  /** "none" leaves the rest uncounted; otherwise the income category that records it (D4). */
  rest: "none" | "income";
  restCategoryId: string;
}

export const NO_LINKS: ReimbursementLinksValue = { expenses: [], rest: "none", restCategoryId: "" };

/** The value a saved reimbursement starts from: the expenses its links name. */
export function linksValue(links: ReimbursementLinks | undefined, undescribed: string): ReimbursementLinksValue {
  if (!links || links.repays.length === 0) return NO_LINKS;
  return {
    ...NO_LINKS,
    expenses: links.repays.map((line) => ({
      id: line.transaction_id,
      title: line.notes?.trim() || line.merchant || undescribed,
      date: line.date,
      amount: line.expense_amount ?? line.amount,
    })),
  };
}

/** The request for `PUT /transactions/{id}/reimburses`, or null when nothing is linked. */
export function linksRequest(value: ReimbursementLinksValue): { expense_ids: string[]; rest_category_id?: string } | null {
  if (value.expenses.length === 0) return null;
  return {
    expense_ids: value.expenses.map((expense) => expense.id),
    ...(value.rest === "income" && value.restCategoryId ? { rest_category_id: value.restCategoryId } : {}),
  };
}

function since(days: number): string {
  const day = new Date(Date.now() - days * 86_400_000).toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
  return `${day}T00:00:00+07:00`;
}

/**
 * "Mengganti transaksi": the expenses a reimbursement repays, chosen from the last 90 days by search, and what
 * happens to the rest (D4, D8). Every amount the server allocates is shown after saving, in the transaction dialog.
 */
export function ReimbursementLinksField({
  idPrefix,
  value,
  onChange,
  incomeCategories,
  error,
}: {
  idPrefix: string;
  value: ReimbursementLinksValue;
  onChange: (next: ReimbursementLinksValue) => void;
  incomeCategories: { id: string; name: string }[];
  error?: string;
}) {
  const t = useTranslations("transactions.reimbursementLinks");
  const money = useMoney();
  const format = useFormat();
  const labels = useRowLabels();
  const headingId = useId();
  const [query, setQuery] = useState("");
  const search = query.trim();
  const path = `/transactions?${new URLSearchParams({
    type: "expense", from_date: since(PICKER_DAYS), limit: String(PICKER_LIMIT), ...(search ? { q: search } : {}),
  }).toString()}`;
  const results = useQuery<{ transactions: LedgerRow[] }>({
    queryKey: [...queryKeys.transactions.all, "reimbursement-picker", search],
    queryFn: () => api.get(path),
  });
  const chosen = new Set(value.expenses.map((expense) => expense.id));
  const options = (results.data?.transactions ?? []).filter((row) => !chosen.has(row.id) && !row.movement_id);
  const add = (row: LedgerRow) =>
    onChange({ ...value, expenses: [...value.expenses, { id: row.id, title: rowTitle(row, labels), date: row.date, amount: row.amount }] });
  const remove = (id: string) => onChange({ ...value, expenses: value.expenses.filter((expense) => expense.id !== id) });
  const restId = `${idPrefix}-rest-category`;

  return (
    <fieldset className="field full-row reimburse-links" aria-labelledby={headingId}>
      <legend id={headingId} className="flabel">{t("label")}</legend>
      <p className="hint" id={`${idPrefix}-links-hint`}>{t("hint")}</p>
      {value.expenses.length > 0 && (
        <ul className="reimburse-chosen" aria-label={t("chosen")}>
          {value.expenses.map((expense) => (
            <li key={expense.id} className="reimburse-row">
              <span className="reimburse-main">
                <span className="reimburse-title">{expense.title}</span>
                <span className="t-meta">{format.shortDateAt(expense.date)}</span>
              </span>
              <MaskedMoney text={money(expense.amount)} className="t-amt" />
              <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm" onClick={() => remove(expense.id)}
                aria-label={t("remove", { title: expense.title })}>
                {t("removeShort")}
              </button>
            </li>
          ))}
        </ul>
      )}
      <label htmlFor={`${idPrefix}-links-search`} className="sr-only">{t("search")}</label>
      <input id={`${idPrefix}-links-search`} type="search" autoComplete="off" value={query} placeholder={t("search")}
        onChange={(event) => setQuery(event.target.value)} className="field-control" aria-describedby={`${idPrefix}-links-hint`} />
      {results.isError ? (
        <p className="err" role="alert">{t("loadFailed")}</p>
      ) : options.length > 0 ? (
        <ul className="reimburse-options" aria-label={t("results")}>
          {options.map((row) => {
            const title = rowTitle(row, labels);
            return (
              <li key={row.id}>
                <button type="button" className="reimburse-row is-option" onClick={() => add(row)} aria-label={t("add", { title })}>
                  <span className="reimburse-main">
                    <span className="reimburse-title">{title}</span>
                    <span className="t-meta">{format.shortDateAt(row.date)}</span>
                  </span>
                  <MaskedMoney text={money(row.amount)} className="t-amt" />
                </button>
              </li>
            );
          })}
        </ul>
      ) : results.data ? (
        <p className="t-meta">{search ? t("noMatch") : t("noRecent")}</p>
      ) : null}
      {value.expenses.length > 0 && (
        <div className="reimburse-rest">
          <span className="flabel" id={`${idPrefix}-rest-label`}>{t("restLabel")}</span>
          <RadioSegments
            labelledBy={`${idPrefix}-rest-label`}
            options={[
              { value: "none" as const, label: t("restNone") },
              { value: "income" as const, label: t("restIncome") },
            ]}
            value={value.rest}
            onChange={(rest) => onChange({ ...value, rest, restCategoryId: value.restCategoryId || incomeCategories[0]?.id || "" })}
          />
          {value.rest === "income" && (
            <>
              <label htmlFor={restId}>{t("restCategory")}</label>
              <select id={restId} value={value.restCategoryId} className="field-control"
                onChange={(event) => onChange({ ...value, restCategoryId: event.target.value })}>
                {incomeCategories.map((category) => <option key={category.id} value={category.id}>{category.name}</option>)}
              </select>
            </>
          )}
          <p className="hint">{value.rest === "income" ? t("restIncomeHint") : t("restNoneHint")}</p>
        </div>
      )}
      {error && <p className="err" role="alert">{error}</p>}
    </fieldset>
  );
}
