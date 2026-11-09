"use client";

import { useState, type ReactNode } from "react";
import { useTranslations } from "next-intl";
import { KindAmount } from "@/components/transactions/rows";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { ErrorState } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import type { ExpensePlanDraft } from "@/types/recurring";
import { billRows, billTotals, creditBillRows, mergeBillRows, type BillRow, type BillStatus } from "./bills";
import { RowsSkeleton } from "./skeletons";
import type { GoalData } from "./useGoalData";

/** Rows shown before "dan N tagihan lain" (D5). */
const SHOWN_ROWS = 5;

const STATUS_CLASS: Record<BillStatus, string | undefined> = { overdue: "c-expense", dueToday: "c-warning", scheduled: undefined };

/**
 * "Tagihan 30 hari ke depan" (apply-markas-target D5, D14): today's plan and detected-charge rows by date with their
 * count and total, then, beside them but outside that count and total, the unpaid credit bills that are already in
 * Pengeluaran. Five rows show until the toggle; a plan row opens it in "Rencana & Pengingat", a detected charge opens
 * the new-plan form with its draft, and a credit bill opens its account's detail.
 */
export function BillsSection({
  data,
  onManage,
  onOpenPlan,
  onOpenDraft,
  onOpenCredit,
}: {
  data: GoalData;
  onManage: () => void;
  onOpenPlan: (ruleId: string) => void;
  onOpenDraft: (draft: ExpensePlanDraft) => void;
  onOpenCredit: (accountId: string) => void;
}) {
  const t = useTranslations("goals.bills");
  const tc = useTranslations("common");
  const format = useFormat();
  const [all, setAll] = useState(false);
  const summary = data.summaryQuery.data;
  const basis = summary?.scheduled.window_basis ?? "next_30_days";
  const title = basis === "next_30_days" ? t("title30") : t("titleSalary");

  const header = (aside?: ReactNode) => (
    <SectionHeader
      id="tg-bills"
      label={title}
      aside={
        <>
          {aside}
          <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm" aria-haspopup="dialog" onClick={onManage}>
            {t("manage")}
          </button>
        </>
      }
    />
  );

  if (data.summaryQuery.isLoading) {
    return (
      <section aria-labelledby="tg-bills" aria-busy="true">
        {header()}
        <RowsSkeleton rows={3} />
      </section>
    );
  }
  if (data.summaryQuery.isError || !summary) {
    return (
      <section aria-labelledby="tg-bills">
        {header()}
        <ErrorState message={t("error")} onRetry={() => data.summaryQuery.refetch()} />
      </section>
    );
  }

  const windowEnd = summary.scheduled.window_end;
  const plain = billRows(data.planLines, data.chargesQuery.data?.charges ?? [], data.today, windowEnd);
  const credit = creditBillRows(data.creditQuery.data?.bills ?? [], data.planLines, data.today);
  const rows = mergeBillRows(plain, credit);
  const { count, total } = billTotals(plain);
  const shown = all ? rows : rows.slice(0, SHOWN_ROWS);
  const schedules = new Map((data.rulesQuery.data?.rules ?? []).map((rule) => [rule.id, rule.schedule_type]));
  const scheduleWord = (ruleId: string) => {
    const schedule = schedules.get(ruleId);
    if (schedule === "weekly") return t("weekly");
    if (schedule === "monthly_day" || schedule === "payday") return t("monthly");
    return null;
  };
  const status = (value: BillStatus, word: string) => <span className={STATUS_CLASS[value]}>{word}</span>;
  const statusWord = (value: BillStatus) => (value === "overdue" ? t("overdue") : value === "dueToday" ? t("dueToday") : t("scheduled"));

  const metaOf = (row: BillRow): ReactNode => {
    if (row.kind === "credit") {
      return (
        <>
          {status(row.status, row.status === "overdue" ? t("creditOverdue") : statusWord(row.status))} · {t("countedInSpending")}
        </>
      );
    }
    if (row.kind === "charge") return `${row.cadence === "weekly" ? t("weekly") : t("monthly")} · ${t("estimate")}`;
    const schedule = scheduleWord(row.ruleId);
    return (
      <>
        {status(row.status, statusWord(row.status))}
        {schedule && ` · ${schedule}`}
        {row.countedElsewhere > 0 && (
          <>
            {" · "}
            {t.rich("planCounted", { amount: () => <MaskedMoney text={format.money(row.countedElsewhere)} /> })}
          </>
        )}
      </>
    );
  };
  const open = (row: BillRow) => {
    if (row.kind === "plan") return () => onOpenPlan(row.ruleId);
    if (row.kind === "credit") return () => onOpenCredit(row.accountId);
    const draft = row.draft;
    return draft ? () => onOpenDraft(draft) : onManage;
  };

  return (
    <section aria-labelledby="tg-bills">
      {header(
        count > 0 || credit.length > 0 ? (
          <span className="aside">
            {count > 0 && t.rich("aside", { countText: format.integer(count), total: () => <MaskedMoney text={format.money(total)} /> })}
            {count > 0 && credit.length > 0 && " · "}
            {credit.length > 0 && t("creditAside", { count: credit.length })}
          </span>
        ) : undefined,
      )}
      {rows.length === 0 ? (
        <p className="t-body muted">{basis === "next_30_days" ? t("none30") : t("noneSalary")}</p>
      ) : (
        <>
          <ul className="list" aria-label={t("listLabel")}>
            {shown.map((row) => {
              const meta = metaOf(row);
              return (
                <li key={row.key}>
                  <ListRow
                    onClick={open(row)}
                    rowKey={row.key}
                    time={format.shortDate(row.date)}
                    timeIsDate
                    title={row.kind === "credit" ? t("creditTitle", { account: row.label }) : row.label}
                    meta={meta}
                    phoneMeta={
                      <>
                        {format.shortDate(row.date)} · {meta}
                      </>
                    }
                    amount={<KindAmount kind="plain" amount={row.shown} money={format.money} />}
                  />
                </li>
              );
            })}
          </ul>
          {rows.length > SHOWN_ROWS && (
            <div className="tg-more">
              <button type="button" className="ws-btn ws-btn-quiet" aria-expanded={all} onClick={() => setAll((value) => !value)}>
                {all ? t("less") : t("more", { count: rows.length - SHOWN_ROWS })}
              </button>
            </div>
          )}
        </>
      )}
      {data.chargesQuery.isError && <p className="t-meta">{t("chargesError")}</p>}
      {data.creditQuery.isError && (
        <div className="tg-note">
          <p className="t-meta">{t("creditError")}</p>
          <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm" onClick={() => data.creditQuery.refetch()}>
            {tc("tryAgain")}
          </button>
        </div>
      )}
      {(data.creditQuery.data?.due_dates_unavailable ?? []).map((account) => (
        <p key={account.account_id} className="t-meta tg-note">
          <button type="button" className="ws-linkbtn" onClick={() => onOpenCredit(account.account_id)}>
            {t("noDueDates", { account: account.account_name })}
          </button>
        </p>
      ))}
    </section>
  );
}
