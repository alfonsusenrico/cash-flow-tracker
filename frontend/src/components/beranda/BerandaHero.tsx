"use client";

import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { AccountsResponse } from "@/components/accounts/types";
import { ExplanationDialog } from "@/components/workspace/ExplanationDialog";
import { FigureBlock, HeroFigure, SplitBar } from "@/components/workspace/figures";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { api } from "@/lib/api";
import type { Words } from "@/lib/i18n/types";
import { queryKeys } from "@/lib/queryKeys";
import { CreditBillsDialog, creditAccountsOf, creditFigure } from "./CreditBillsDialog";
import { type Money, periodBasis, safeAmountRincian, unavailableReason } from "./explanations";
import type { ActivityRow, CycleSummary } from "./types";
import { useWords } from "./words";

/** The hero's one line (D3): the days to the expected salary, by how much the period is over, or why not known. */
export function heroLine(summary: CycleSummary, money: Money, words: Words): string {
  const { t, format } = words;
  const { safe_to_spend: safe, remaining, expected_salary: expected } = summary;
  if (remaining.total < 0) return t("home.over", { period: periodBasis(summary), amount: money(-remaining.total) });
  if (safe.status === "available" && safe.days_left !== null && expected.date) {
    return t("home.daysUntilPayday", { days: safe.days_left, date: format.shortDate(expected.date) });
  }
  return unavailableReason(summary, words).line;
}

/** The supporting figures' captions (D4, owner decisions 9 and 10), each short enough for one line. */
export function legendCaptions(summary: CycleSummary, { t, format }: Words) {
  const cycle = summary.period.basis === "cycle";
  const start = format.shortDate(summary.period.start_date);
  const monthStart = t("home.captionSinceMonth", { month: format.monthName(summary.period.start_date) });
  const savingTotal = summary.tabungan ? summary.tabungan.total : summary.fresh_saving.amount;
  return {
    masuk: cycle ? t("home.captionSalary", { start }) : monthStart,
    spending: cycle ? t("home.captionSinceSalary", { start }) : monthStart,
    saving: savingTotal < 0 ? t("home.captionSavingsDown") : t("home.captionSavingsUp"),
  };
}

/**
 * "Aman dipakai sampai gajian" (D3 to D5): the page's one 44 px figure with its line and "Rincian", the split bar
 * and the supporting figures, each opening its detail, with "Tagihan kredit" for an owner with credit accounts
 * (open-beranda-figures). Every figure is a server field; the client only orders and formats them.
 */
/** Where each supporting figure leads (open-beranda-figures D2): Transaksi with Beranda's period and its type. */
export function figureLinks(summary: CycleSummary): Record<"masuk" | "spending" | "saving", string> {
  const period = summary.period.basis === "cycle" ? "cycle" : "month";
  const ledger = (type: string) => `/ledger?${new URLSearchParams({ period, type }).toString()}`;
  return { masuk: ledger("income"), spending: ledger("expense"), saving: ledger("saving") };
}

/** "jatuh tempo 20 Nov", or why no bill is named (D3); the amount per bill is in the dialog, so the caption fits one line. */
function creditCaption(figure: ReturnType<typeof creditFigure>, { t, format }: Words): string {
  if (figure.nearest) return t("home.creditBills.caption", { date: format.shortDate(figure.nearest.due_date) });
  return figure.noDueDay ? t("home.creditBills.noDueDay") : t("home.creditBills.nothingOwed");
}

export function BerandaHero({ summary, onSelectRow }: { summary: CycleSummary; onSelectRow: (row: ActivityRow) => void }) {
  const words = useWords();
  const { t } = words;
  const money: Money = useMoney();
  const [open, setOpen] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);
  const [creditOpen, setCreditOpen] = useState(false);
  const creditOpener = useRef<HTMLButtonElement>(null);
  const accountsQuery = useQuery<AccountsResponse>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });
  const creditAccounts = creditAccountsOf(accountsQuery.data?.accounts ?? []);
  const credit = creditFigure(creditAccounts);
  const links = figureLinks(summary);
  const closeCredit = () => {
    setCreditOpen(false);
    creditOpener.current?.focus();
  };
  const { masuk, spending, fresh_saving: saving, scheduled, remaining } = summary;
  const free = Math.max(0, remaining.total);
  const captions = legendCaptions(summary, words);
  const period = periodBasis(summary);
  const close = () => {
    setOpen(false);
    opener.current?.focus();
  };
  const segments = [
    { key: "spending", label: t("terms.spending"), amount: spending.amount, color: "var(--out-solid)" },
    // A savings decrease takes no width in the bar; its caption names the direction in words.
    { key: "saving", label: t("terms.savedInvested"), amount: Math.max(0, saving.amount), color: "var(--inv-solid)" },
    { key: "scheduled", label: t("terms.scheduled"), amount: scheduled.total, color: "var(--warn-solid)" },
    { key: "safe", label: t("terms.safeToSpend"), amount: free, color: "var(--in-solid)" },
  ];
  return (
    <HeroFigure
      id="beranda-safe"
      label={t("terms.safeToSpendUntilPayday")}
      mark="var(--in-solid)"
      action={
        <button
          ref={opener}
          type="button"
          className="ws-btn ws-btn-quiet ws-btn-sm"
          aria-haspopup="dialog"
          aria-expanded={open}
          aria-label={t("home.breakdownButton")}
          onClick={() => setOpen(true)}
        >
          {t("terms.breakdown")}
        </button>
      }
      figure={<MaskedMoney text={money(free)} />}
      line={heroLine(summary, money, words)}
    >
      <SplitBar
        label={t("home.splitName", { period, income: money(masuk.total) })}
        emptyLabel={t("home.splitEmpty", { period })}
        total={masuk.total}
        segments={segments}
      />
      <div className={creditAccounts.length > 0 ? "legend has-credit" : "legend"}>
        <FigureBlock label={t("terms.income")} figure={<MaskedMoney text={money(masuk.total)} />} caption={captions.masuk} phoneHidden
          href={links.masuk} action={t("home.openInLedger")} />
        <FigureBlock label={t("terms.spending")} mark="var(--out-solid)" figure={<MaskedMoney text={money(spending.amount)} />} caption={captions.spending}
          href={links.spending} action={t("home.openInLedger")} />
        <FigureBlock
          label={t("terms.savedInvested")}
          mark="var(--inv-solid)"
          figure={<MaskedMoney text={money(Math.abs(saving.amount))} />}
          caption={captions.saving}
          href={links.saving}
          action={t("home.openInLedger")}
        />
        {creditAccounts.length > 0 && (
          <FigureBlock
            label={t("terms.creditBills")}
            mark="var(--credit-fg)"
            figure={<MaskedMoney text={money(credit.owed)} />}
            caption={creditCaption(credit, words)}
            onOpen={() => setCreditOpen(true)}
            openRef={creditOpener}
            action={t("home.creditBills.open")}
          />
        )}
      </div>
      <ExplanationDialog open={open} onClose={close} data={open ? safeAmountRincian(summary, money, words) : null} />
      <CreditBillsDialog
        open={creditOpen}
        onClose={closeCredit}
        accounts={creditAccounts}
        failed={accountsQuery.isError}
        onRetry={() => accountsQuery.refetch()}
        money={money}
        onSelect={onSelectRow}
      />
    </HeroFigure>
  );
}
