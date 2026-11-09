"use client";

import { useRef, useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { PlansDialog } from "@/components/recurring/PlansDialog";
import { ListRow, PanelBlock, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { ErrorState } from "@/components/workspace/primitives";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { ledgerHref } from "@/lib/ledgerFilters";
import { queryKeys } from "@/lib/queryKeys";
import type { ExpensePlanDraft } from "@/types/recurring";

type Money = (amount: number) => string;

export interface RecurringCharge {
  key: string;
  merchant: string;
  search_text: string;
  cadence: "monthly" | "weekly";
  occurrences: number;
  typical_amount: number;
  last_amount: number;
  previous_amount: number;
  amount_changed: boolean;
  last_date: string;
  next_expected_date: string;
  monthly_equivalent: number;
  account_name: string;
  existing_plan: { id: string; name: string } | null;
  plan_draft: ExpensePlanDraft;
}

export interface RecurringChargesData {
  window: { start: string; end: string; start_date: string; end_date: string; months: number };
  charges: RecurringCharge[];
  monthly_equivalent_total: number;
  monthly_equivalent_without_plan: number;
}

/**
 * "Tagihan & langganan" in Ringkasan's panel (apply-markas-analisis D10): the detected charges' count and monthly
 * equivalent, the four largest as ledger links, "Lihat semua" with every charge and its plan state, and "Jadikan
 * rencana", which opens the plan form prefilled; nothing is saved until the owner confirms it there.
 */
export function BillsPanel({ money }: { money: Money }) {
  const t = useTranslations("analysis.bills");
  const format = useFormat();
  const [allOpen, setAllOpen] = useState(false);
  const [draft, setDraft] = useState<ExpensePlanDraft | null>(null);
  const allRef = useRef<HTMLButtonElement>(null);
  const planRef = useRef<HTMLButtonElement>(null);
  const query = useQuery<RecurringChargesData>({
    queryKey: queryKeys.dashboard.recurringCharges,
    queryFn: () => api.get("/dashboard/recurring-charges"),
  });
  const data = query.data;
  const charges = [...(data?.charges ?? [])].sort((left, right) => right.monthly_equivalent - left.monthly_equivalent || left.merchant.localeCompare(right.merchant));
  const withoutPlan = charges.find((charge) => !charge.existing_plan) ?? null;
  const rowHref = (charge: RecurringCharge) => (data ? ledgerHref({ from: data.window.start, to: data.window.end, type: "expense", q: charge.search_text }) : "#");
  const cadence = (charge: RecurringCharge) =>
    charge.cadence === "weekly" ? t("weeklyNext", { date: format.shortDate(charge.next_expected_date) }) : t("monthlyNext", { date: format.shortDate(charge.next_expected_date) });

  let body;
  if (query.isPending) {
    body = <div role="status" aria-busy="true"><span className="skel is-line" /><span className="skel is-line" /></div>;
  } else if (query.isError) {
    body = <ErrorState message={t("loadFailed")} onRetry={() => query.refetch()} />;
  } else if (charges.length === 0) {
    body = <p className="t-body muted">{t("empty")}</p>;
  } else {
    body = (
      <>
        <ul className="list">
          {charges.slice(0, 4).map((charge) => (
            <li key={charge.key}>
              <ListRow
                href={rowHref(charge)}
                layout="stacked"
                title={charge.merchant}
                meta={cadence(charge)}
                amount={<MaskedMoney text={money(charge.typical_amount)} className="t-amt" />}
              />
            </li>
          ))}
        </ul>
        <div className="panel-actions">
          <button ref={allRef} type="button" className="ws-btn" aria-haspopup="dialog" onClick={() => setAllOpen(true)}>{t("seeAll")}</button>
          {withoutPlan && (
            <button ref={planRef} type="button" className="ws-btn ws-btn-quiet" aria-haspopup="dialog" onClick={() => setDraft(withoutPlan.plan_draft)}>{t("makePlan")}</button>
          )}
        </div>
      </>
    );
  }

  return (
    <PanelBlock>
      <SectionHeader
        id="an-bills"
        label={t("title")}
        aside={data && charges.length > 0 ? (
          <span className="aside">{t.rich("aside", { n: format.integer(charges.length), amount: () => <MaskedMoney text={money(data.monthly_equivalent_total)} /> })}</span>
        ) : undefined}
      />
      {body}
      {data && (
        <ShellDialog
          open={allOpen}
          onClose={() => {
            setAllOpen(false);
            allRef.current?.focus();
          }}
          title={t("dialogTitle")}
          width={560}
          closeLabel={t("close")}
        >
          <ul className="list bills-all">
            {charges.map((charge) => (
              <li key={charge.key} className="bill">
                <div className="bill-top">
                  <Link href={rowHref(charge)} className="bill-name">{charge.merchant}</Link>
                  <MaskedMoney text={money(charge.typical_amount)} className="t-amt" />
                </div>
                <p className="t-meta">{t("facts", { cadence: charge.cadence, count: charge.occurrences, account: charge.account_name })}</p>
                <p className="t-meta">{t("dates", { last: format.shortDate(charge.last_date), next: format.shortDate(charge.next_expected_date) })}</p>
                {charge.amount_changed && (
                  <p className="t-meta c-warning">{t.rich("amountChanged", { from: () => <MaskedMoney text={money(charge.previous_amount)} />, to: () => <MaskedMoney text={money(charge.last_amount)} /> })}</p>
                )}
                {charge.existing_plan ? (
                  <p className="t-meta">{t("hasPlan", { name: charge.existing_plan.name })}</p>
                ) : (
                  <button
                    type="button"
                    className="ws-btn ws-btn-sm"
                    aria-label={t("makePlanFor", { name: charge.merchant })}
                    onClick={() => {
                      setAllOpen(false);
                      setDraft(charge.plan_draft);
                    }}
                  >
                    {t("makePlan")}
                  </button>
                )}
              </li>
            ))}
          </ul>
        </ShellDialog>
      )}
      <PlansDialog
        open={draft !== null}
        onClose={() => {
          setDraft(null);
          (planRef.current ?? allRef.current)?.focus();
        }}
        expenseDraft={draft}
      />
    </PanelBlock>
  );
}

/** The latest closed report as one whole-block link, while one is ready (apply-markas-analisis D10). */
export function ReportLink({ label, url }: { label: string; url: string }) {
  const t = useTranslations("analysis.report");
  return (
    <PanelBlock heading={t("title")} href={url}>
      <p className="t-body">{t("ready", { period: label })}</p>
      <p className="t-meta">{t("about")}</p>
    </PanelBlock>
  );
}
