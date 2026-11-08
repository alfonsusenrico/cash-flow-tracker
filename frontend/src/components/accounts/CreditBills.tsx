"use client";

import { useId, useState } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { ErrorState } from "@/components/workspace/primitives";
import { cn } from "@/lib/utils";
import { useAccountText } from "./accountText";
import { BILL_STATUS_KEY, billStatusTone } from "./credit";
import type { CreditBillStatus, CreditNextDue } from "./types";

/** `GET /api/credit/accounts/{id}` as the credit detail reads it (add-credit-spending-flow D4, H8). */
export interface CreditBillItem {
  kind: "purchase" | "fee" | "cash_advance" | "opening";
  transaction_id: string | null;
  date: string;
  due_date: string;
  installment: { number: number; count: number } | null;
  amount: number;
  left_to_pay: number;
  settled_on: string | null;
  days_to_settle: number | null;
  settled_on_time: boolean | null;
}

export interface CreditBill {
  due_date: string;
  statement_date: string;
  amount: number;
  paid: number;
  left_to_pay: number;
  status: CreditBillStatus;
  settled_on: string | null;
  items: CreditBillItem[];
}

export interface CreditOverview {
  due_dates_unavailable: boolean;
  next_due: CreditNextDue | null;
  bills: CreditBill[];
  totals: { spent_on_credit: number; paid_to_credit: number; items_settled: number; items_settled_on_time: number };
}

/**
 * The credit detail's bills (D8, H8): one disclosure row per due date with its status word, what was paid, the
 * amount and what is left; opened, its items with the instalment, and once settled the day, the days it took and
 * whether it was on time. A failed request or a missing due day says so and leaves the rest of the detail usable.
 */
export function CreditBills({
  accountId,
  overview,
  failed,
  onRetry,
  onEditCredit,
}: {
  accountId: string;
  overview: CreditOverview | undefined;
  failed: boolean;
  onRetry: () => void;
  onEditCredit: () => void;
}) {
  const { t } = useAccountText();
  const headingId = useId();
  const bills = overview?.bills ?? [];
  return (
    <section aria-labelledby={headingId}>
      <SectionHeader id={headingId} label={t("detail.bills")} aside={overview ? t("detail.billsCount", { count: bills.length }) : undefined} />
      {failed ? (
        <ErrorState message={t("detail.billsFailed")} onRetry={onRetry} />
      ) : overview?.due_dates_unavailable ? (
        <div className="box">
          <Icon name="alert-triangle" className="h-5 w-5" />
          <div className="grow">
            <p className="t-body">{t("detail.noDueDates")}</p>
            <button type="button" className="ws-btn" onClick={onEditCredit}>{t("actions.editCredit")}</button>
          </div>
        </div>
      ) : overview && !bills.length ? (
        <p className="t-meta">{t("detail.noBills")}</p>
      ) : (
        <ul className="list">
          {bills.map((bill) => <BillRow key={bill.due_date} bill={bill} />)}
        </ul>
      )}
      <Link href={`/ledger?account=${encodeURIComponent(accountId)}`} className="ws-linkbtn">
        {t("detail.viewInLedger")}
      </Link>
    </section>
  );
}

function BillRow({ bill }: { bill: CreditBill }) {
  const { t, format } = useAccountText();
  const terms = useTranslations("terms.billStatus");
  const [open, setOpen] = useState(false);
  const itemsId = useId();
  const tone = billStatusTone(bill.status);
  return (
    <li>
      <button type="button" className="row is-line" aria-expanded={open} aria-controls={open ? itemsId : undefined} onClick={() => setOpen((value) => !value)}>
        <span className="r-main">
          <span className="r-title">{t("detail.billDue", { date: format.shortDate(bill.due_date) })}</span>
          <span className="r-meta">
            <span className={cn(tone === "warning" && "c-warning")}>{terms(BILL_STATUS_KEY[bill.status])}</span>
            {" · "}
            {t.rich("detail.billPaid", { amount: () => <MaskedMoney text={format.money(bill.paid)} /> })}
          </span>
        </span>
        <span className="r-amt">
          <MaskedMoney text={format.money(bill.amount)} className="t-amt" />
          {bill.left_to_pay > 0 && (
            <span className="t-meta">{t.rich("detail.billLeft", { amount: () => <MaskedMoney text={format.money(bill.left_to_pay)} /> })}</span>
          )}
        </span>
        <Icon name="chevD" className="chev h-4 w-4" />
      </button>
      {open && (
        <ul id={itemsId} className="rk-bill-items">
          {bill.items.map((item, index) => (
            <li key={`${item.transaction_id ?? item.kind}-${item.installment?.number ?? 0}-${index}`}>
              <span>
                {t("detail.item.title", { kind: t(`detail.item.${item.kind}`), date: format.shortDate(item.date) })}
                {item.installment && item.installment.count > 1 && ` · ${t("detail.item.installment", { number: String(item.installment.number), total: String(item.installment.count) })}`}
              </span>
              <MaskedMoney text={format.money(item.amount)} className="t-amt" />
              <span className={cn("muted", item.settled_on && item.settled_on_time === false && "c-warning")}>
                {item.settled_on
                  ? t("detail.item.settled", {
                    date: format.shortDate(item.settled_on),
                    days: item.days_to_settle ?? 0,
                    onTime: item.settled_on_time ? "true" : "false",
                  })
                  : t.rich("detail.item.left", { amount: () => <MaskedMoney text={format.money(item.left_to_pay)} /> })}
              </span>
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}
