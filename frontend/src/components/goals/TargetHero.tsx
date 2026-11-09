"use client";

import { useRef, useState } from "react";
import { useTranslations } from "next-intl";
import type { EmergencyRunway } from "@/components/dashboard/EmergencyCoverage";
import { ExplanationDialog } from "@/components/workspace/ExplanationDialog";
import { FigureBlock, HeroFigure, ProgressBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import { safetyNetRincian } from "./safetyNet";
import type { GoalItem } from "./types";

const HERO_ID = "tg-hero";

interface RunwayQuery {
  data?: { runway: EmergencyRunway };
  isLoading: boolean;
  isError: boolean;
  refetch: () => unknown;
}

/** The hero while the net worth loads (D11): label, figure, line, bar and four figure placeholders, marked busy. */
function TargetHeroSkeleton() {
  const terms = useTranslations("terms");
  return (
    <section className="hero hero-skel" aria-busy="true" aria-labelledby={HERO_ID}>
      <h2 id={HERO_ID} className="sr-only">
        {terms("safetyNet")}
      </h2>
      <Skeleton style={{ width: 120, height: 12 }} />
      <Skeleton style={{ width: 240, height: 44, marginTop: 12 }} />
      <Skeleton style={{ width: 280, height: 12, marginTop: 12 }} />
      <Skeleton style={{ width: "100%", height: 8, marginTop: 24 }} />
      <div className="supports" style={{ "--n": 4 } as React.CSSProperties}>
        {[0, 1, 2, 3].map((index) => (
          <div key={index} className={index > 1 ? "fig-block is-phone-hide" : "fig-block"}>
            <Skeleton style={{ width: 80, height: 12 }} />
            <Skeleton style={{ width: "70%", height: 20, marginTop: 8 }} />
          </div>
        ))}
      </div>
    </section>
  );
}

/**
 * "Ketahanan Dana" (apply-markas-target D3): `coverage_months` in the page's one 44 px figure, the basis line, a
 * thick bar of the server's `coverage_pct` and four supporting figures, the last two left out on a phone. With an
 * unknown essential cost the figure is words and no months, bar or days appear. Every value is the server's.
 */
export function TargetHero({ query, goals }: { query: RunwayQuery; goals: GoalItem[] }) {
  const t = useTranslations("goals.hero");
  const tAll = useTranslations();
  const terms = useTranslations("terms");
  const format = useFormat();
  // Amounts are formatted plainly; `MaskedMoney` and the Rincian hide them while balances are hidden.
  const money = format.money;
  const [open, setOpen] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);

  if (query.isLoading) return <TargetHeroSkeleton />;
  const runway = query.data?.runway;
  if (query.isError || !runway) {
    return (
      <section className="hero" aria-labelledby={HERO_ID}>
        <h2 id={HERO_ID} className="t-label">
          {terms("safetyNet")}
        </h2>
        <ErrorState message={tAll("goals.states.safetyNetError")} onRetry={() => query.refetch()} />
      </section>
    );
  }

  const months = runway.coverage_months ?? runway.runway_months;
  const known = months != null && runway.runway_days != null;
  const targetMonths = runway.target_months ?? 6;
  const targetMonthsText = format.decimal(targetMonths, 1);
  const allCash = runway.balance_basis === "all_liquid";
  const line = allCash
    ? t("lineCash", { months: targetMonths, monthsText: targetMonthsText })
    : t("lineGoals", { months: targetMonths, monthsText: targetMonthsText });
  const pct = runway.coverage_pct ?? null;
  const pctText = pct != null ? format.integer(pct) : null;
  const cost = runway.essential_cost;
  const firstPeriod = cost?.periods_used?.[0];
  const essentialCaption = !cost || cost.method === "unknown"
    ? undefined
    : cost.method === "need_budget"
      ? t("essentialBudget")
      : cost.limited_history
        ? t("essentialLimited")
        : firstPeriod?.kind === "cycle"
          ? t("essentialMedianCycles", { count: cost.period_count })
          : t("essentialMedian", { count: cost.period_count });

  return (
    <>
      <HeroFigure
        id={HERO_ID}
        label={terms("safetyNet")}
        action={
          <button ref={opener} type="button" className="ws-btn ws-btn-quiet ws-btn-sm" aria-haspopup="dialog" onClick={() => setOpen(true)}>
            {terms("breakdown")}
          </button>
        }
        figure={known ? t("months", { months: months!, monthsText: format.decimal(months!, 1) }) : <span className="is-words">{t("unknown")}</span>}
        line={known ? line : tAll("breakdown.essentialUnknown")}
      >
        {known && pct != null && (
          <div className="tg-bar" role="img" aria-label={t("barLabel", { pct: pctText ?? "" })}>
            <ProgressBar value={Math.min(100, pct)} max={100} thick color="var(--in-solid)" />
          </div>
        )}
        <div className="supports tg-supports" style={{ "--n": 4 } as React.CSSProperties}>
          <FigureBlock
            bar={false}
            label={allCash ? t("cash") : t("fund")}
            figure={<MaskedMoney text={money(runway.emergency_fund_balance)} />}
            caption={pctText != null ? t("fundCaption", { pct: pctText }) : undefined}
          />
          <FigureBlock
            bar={false}
            label={t("target")}
            figure={runway.target_amount != null ? <MaskedMoney text={money(runway.target_amount)} /> : t("noFigure")}
            caption={t("targetCaption", { monthsText: targetMonthsText })}
          />
          <FigureBlock
            bar={false}
            phoneHidden
            label={t("essential")}
            figure={runway.monthly_primary_expense != null ? <MaskedMoney text={money(runway.monthly_primary_expense)} /> : t("essentialUnknown")}
            caption={essentialCaption}
          />
          <FigureBlock
            bar={false}
            phoneHidden
            label={t("ifIncomeStops")}
            figure={known ? t("days", { days: runway.runway_days!, daysText: format.integer(runway.runway_days!) }) : t("noFigure")}
            caption={known ? t("daysCaption") : undefined}
          />
        </div>
      </HeroFigure>
      <ExplanationDialog
        open={open}
        data={safetyNetRincian(runway, goals, { t: tAll, format, money })}
        onClose={() => {
          setOpen(false);
          requestAnimationFrame(() => opener.current?.focus());
        }}
      />
    </>
  );
}
