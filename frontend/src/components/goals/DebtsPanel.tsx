"use client";

import { useState, type ReactNode } from "react";
import { useTranslations } from "next-intl";
import { creditFacts } from "@/components/accounts/credit";
import type { AccountItem } from "@/components/accounts/types";
import { ProgressBar } from "@/components/workspace/figures";
import { ListRow, Panel, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { ErrorState } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import { RowsSkeleton } from "./skeletons";
import type { ObligationItem } from "./types";
import type { GoalData } from "./useGoalData";

/** What a debt or credit account still owes, in phase 8's Utang style: red, with a minus and a hidden "Utang". */
function OwedAmount({ amount }: { amount: number }) {
  const terms = useTranslations("terms");
  const format = useFormat();
  return (
    <span className="t-amt c-expense">
      <span className="sr-only">{terms("debt")} </span>
      <MaskedMoney text={amount > 0 ? `−${format.money(amount)}` : format.money(0)} />
    </span>
  );
}

function CreditRow({ account, onOpen }: { account: AccountItem; onOpen: () => void }) {
  const t = useTranslations("goals.credit");
  const terms = useTranslations("terms");
  const format = useFormat();
  const facts = creditFacts(account);
  const due = facts.nextDue && facts.nextDue.left_to_pay > 0 ? facts.nextDue : null;
  const amount = () => <MaskedMoney text={format.money(due!.left_to_pay)} />;
  let next: ReactNode = <span />;
  if (due?.status === "overdue") next = <span className="c-expense">{t.rich("overdue", { amount })}</span>;
  else if (due?.status === "due_today") next = <span className="c-warning">{t.rich("dueToday", { amount })}</span>;
  else if (due) next = <span>{t.rich("dueOn", { date: format.shortDate(due.due_date), amount })}</span>;
  const limit: ReactNode = facts.overLimit ? (
    <span className="c-expense">{terms("overLimit")}</span>
  ) : facts.limitLeft != null ? (
    <span>{t.rich("limitLeft", { amount: () => <MaskedMoney text={format.money(facts.limitLeft!)} /> })}</span>
  ) : null;
  return (
    <button type="button" className="row ws-select-row stacked" data-row={`credit-${account.id}`} aria-haspopup="dialog" onClick={onOpen}>
      <span className="line">
        <span className="t-body">{t("rowTitle", { name: account.name })}</span>
        {facts.overpaid > 0 ? (
          <span className="t-amt c-income">{t.rich("overpaid", { amount: () => <MaskedMoney text={format.money(facts.overpaid)} /> })}</span>
        ) : (
          <OwedAmount amount={facts.owed} />
        )}
      </span>
      {facts.limit ? <ProgressBar value={facts.owed} max={facts.limit} color="var(--out-solid)" /> : null}
      <span className="line t-meta">
        {next}
        {limit}
      </span>
    </button>
  );
}

/**
 * "Utang & kredit" (apply-markas-target D9, D10, D15): one stacked row per active debt with its paid bar, then one
 * per credit account (H9) opening its detail in place, and "Sudah lunas · N" listing the paid-off debts, which
 * `include_archived=true` now returns. No total and no payoff time are drawn.
 */
export function DebtsPanel({
  data,
  onOpenDebt,
  onOpenCredit,
}: {
  data: GoalData;
  onOpenDebt: (debt: ObligationItem) => void;
  onOpenCredit: (accountId: string) => void;
}) {
  const t = useTranslations("goals");
  const format = useFormat();
  const [paidOpen, setPaidOpen] = useState(false);
  const active = data.activeDebts;
  const credit = data.creditAccounts;
  const label = t("debts.title");
  const header = (aside?: ReactNode) => <SectionHeader id="tg-debts" label={label} aside={aside} />;

  let body: ReactNode;
  if (data.debtsQuery.isLoading) {
    body = (
      <>
        {header()}
        <RowsSkeleton rows={2} />
      </>
    );
  } else if (data.debtsQuery.isError) {
    body = (
      <>
        {header()}
        <ErrorState message={t("states.debtsError")} onRetry={() => data.debtsQuery.refetch()} />
      </>
    );
  } else {
    const count = active.length + credit.length;
    body = (
      <>
        {header(<span className="aside">{t("debts.active", { count })}</span>)}
        {count === 0 ? (
          <p className="t-body muted">{t("debts.empty")}</p>
        ) : (
          <ul className="list">
            {active.map((debt) => {
              const pct = Math.max(0, Math.round(debt.payoff_percentage));
              return (
                <li key={debt.id}>
                  <button type="button" className="row ws-select-row stacked" data-row={`debt-${debt.id}`} aria-haspopup="dialog" onClick={() => onOpenDebt(debt)}>
                    <span className="line">
                      <span className="t-body">{debt.name}</span>
                      <OwedAmount amount={debt.remaining_amount} />
                    </span>
                    <ProgressBar value={debt.payoff_percentage} max={100} color="var(--text)" />
                    <span className="line t-meta">
                      <span>{debt.due_date ? format.shortDate(debt.due_date) : ""}</span>
                      <span>{t("debts.left", { pct: String(pct) })}</span>
                    </span>
                  </button>
                </li>
              );
            })}
            {credit.map((account) => (
              <li key={account.id}>
                <CreditRow account={account} onOpen={() => onOpenCredit(account.id)} />
              </li>
            ))}
          </ul>
        )}
        {data.paidDebts.length > 0 && (
          <>
            <div className="tg-more">
              <button type="button" className="ws-btn ws-btn-quiet" aria-expanded={paidOpen} onClick={() => setPaidOpen((value) => !value)}>
                {t("debts.paidOff", { countText: format.integer(data.paidDebts.length) })}
              </button>
            </div>
            {paidOpen && (
              <ul className="list">
                {data.paidDebts.map((debt) => (
                  <li key={debt.id}>
                    <ListRow
                      onClick={() => onOpenDebt(debt)}
                      rowKey={`debt-${debt.id}`}
                      title={debt.name}
                      meta={t("debts.paidOffMeta")}
                      amount={<MaskedMoney text={format.money(debt.total_amount)} className="t-amt muted" />}
                    />
                  </li>
                ))}
              </ul>
            )}
          </>
        )}
      </>
    );
  }
  return (
    <Panel label={label}>
      <section aria-labelledby="tg-debts" aria-busy={data.debtsQuery.isLoading || undefined} tabIndex={-1}>
        {body}
      </section>
    </Panel>
  );
}
