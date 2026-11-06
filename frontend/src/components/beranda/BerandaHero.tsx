"use client";

import { useRef, useState } from "react";
import { ExplanationDialog } from "@/components/workspace/ExplanationDialog";
import { FigureBlock, HeroFigure, SplitBar } from "@/components/workspace/figures";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import type { Words } from "@/lib/i18n/types";
import { type Money, periodBasis, safeAmountRincian, unavailableReason } from "./explanations";
import type { CycleSummary } from "./types";
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
  const setAsides = summary.scheduled.lines.some((line) => line.kind === "goal" && line.amount > 0);
  const windowEnd = format.shortDate(summary.scheduled.window_end);
  return {
    masuk: cycle ? t("home.captionSalary", { start }) : monthStart,
    spending: cycle ? t("home.captionSinceSalary", { start }) : monthStart,
    saving: savingTotal < 0 ? t("home.captionSavingsDown") : t("home.captionSavingsUp"),
    scheduled: setAsides ? t("home.captionWithDeposits", { date: windowEnd }) : t("home.captionBillsUntil", { date: windowEnd }),
  };
}

/**
 * "Aman dipakai sampai gajian" (D3 to D5): the page's one 44 px figure with its line and "Rincian", the split bar
 * and the four supporting figures. Every figure is a server field; the client only orders and formats them.
 */
export function BerandaHero({ summary }: { summary: CycleSummary }) {
  const words = useWords();
  const { t } = words;
  const money: Money = useMoney();
  const [open, setOpen] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);
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
      <div className="legend">
        <FigureBlock label={t("terms.income")} figure={<MaskedMoney text={money(masuk.total)} />} caption={captions.masuk} phoneHidden />
        <FigureBlock label={t("terms.spending")} mark="var(--out-solid)" figure={<MaskedMoney text={money(spending.amount)} />} caption={captions.spending} />
        <FigureBlock
          label={t("terms.savedInvested")}
          mark="var(--inv-solid)"
          figure={<MaskedMoney text={money(Math.abs(saving.amount))} />}
          caption={captions.saving}
        />
        <FigureBlock label={t("terms.scheduled")} mark="var(--warn-solid)" figure={<MaskedMoney text={money(scheduled.total)} />} caption={captions.scheduled} phoneHidden />
      </div>
      <ExplanationDialog open={open} onClose={close} data={open ? safeAmountRincian(summary, money, words) : null} />
    </HeroFigure>
  );
}
