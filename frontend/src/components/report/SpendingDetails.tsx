"use client";

import { useState } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { essentialBasisText } from "@/components/dashboard/EmergencyCoverage";
import { Icon } from "@/components/ui/Icon";
import { KindAmount, jakartaDay, maskedTag } from "@/components/transactions/rows";
import { TransactionDialog } from "@/components/transactions/TransactionDialog";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { ListRow } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { type MonthlyReport, reportLedgerHref } from "@/lib/monthlyReport";

type Money = (amount: number) => string;

/**
 * "Rincian pengeluaran" (apply-markas-laporan D9): a closed disclosure box with the unexpected items, which open
 * the transaction dialog, the recurring charges seen in the period and today's essential living cost with the
 * server's method. The reimbursement caption above it is the part subtracted from Pengeluaran (D29).
 */
export function SpendingDetails({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report.details");
  const terms = useTranslations("terms");
  const translate = useTranslations();
  const format = useFormat();
  const { record } = useTransactionUndo();
  const [openId, setOpenId] = useState<string | null>(null);
  const { period, spending, flow } = report;
  const { unexpected, recurring, essential } = spending;

  return (
    <div>
      {flow.spending_reimbursement_offset > 0 && (
        <p className="caption lp-reimbursed">{t.rich("reimbursed", { amount: maskedTag(money(flow.spending_reimbursement_offset)) })}</p>
      )}
      <details className="box lp-details">
        <summary className="row">
          <span className="r-main lp-summary-text">
            <span className="r-title t-strong">{t("title")}</span>
            <span className="r-meta">{t("meta")}</span>
          </span>
          <Icon name="chevron-down" className="h-4 w-4 lp-chev" />
        </summary>
        <div className="lp-details-body">
          <section aria-labelledby="lp-unexpected">
            <h3 className="t-label" id="lp-unexpected">{t("unexpected")}</h3>
            {unexpected.items.length === 0 ? (
              <p className="t-meta lp-none">{t("none")}</p>
            ) : (
              <ul className="list">
                {unexpected.items.map((item) => (
                  <li key={item.id}>
                    <ListRow
                      onClick={() => setOpenId(item.id)}
                      rowKey={item.id}
                      title={item.merchant || item.notes || item.category_name || terms("noDescription")}
                      meta={format.shortDate(jakartaDay(item.date))}
                      amount={<KindAmount kind="expense" amount={item.amount} money={money} />}
                    />
                  </li>
                ))}
              </ul>
            )}
            {unexpected.count > unexpected.items.length && (
              <Link className="btn-link lp-more" href={reportLedgerHref(period, { type: "expense", kakeibo: "unexpected" })}>
                {t("allUnexpected", { count: unexpected.count })}
              </Link>
            )}
          </section>
          <section aria-labelledby="lp-recurring">
            <h3 className="t-label" id="lp-recurring">{t("recurring")}</h3>
            {recurring.charges.length === 0 ? (
              <p className="t-meta lp-none">{t("none")}</p>
            ) : (
              <ul className="list">
                {recurring.charges.map((charge) => (
                  <li key={charge.search_text}>
                    <ListRow
                      href={reportLedgerHref(period, { type: "expense", q: charge.search_text })}
                      title={charge.merchant}
                      meta={[
                        charge.cadence === "weekly" ? t("weekly") : t("monthly"),
                        charge.existing_plan ? t("hasPlan") : null,
                        charge.amount_changed ? t("amountChanged") : null,
                      ].filter(Boolean).join(" · ")}
                      amount={<KindAmount kind="expense" amount={charge.last_amount} money={money} />}
                    />
                  </li>
                ))}
              </ul>
            )}
          </section>
          <section aria-labelledby="lp-essential">
            <h3 className="t-label" id="lp-essential">{t("essential")}</h3>
            {essential.amount === null ? (
              <p className="t-meta lp-none">{t("essentialUnavailable")}</p>
            ) : (
              <p className="lp-essential">
                <MaskedMoney text={money(essential.amount)} className="t-amt" />{" "}
                <span className="t-meta">{t("perMonth", { method: essentialBasisText(essential, translate) })}</span>
              </p>
            )}
          </section>
        </div>
      </details>
      <TransactionDialog transactionId={openId} onClose={() => setOpenId(null)} record={record} />
    </div>
  );
}
