"use client";

import type { ReactNode } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { ProgressBar } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { ErrorState } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import { RowsSkeleton } from "./skeletons";
import type { GoalItem } from "./types";
import type { GoalData } from "./useGoalData";

/** A goal's whole-number percentage as the server computes it, for display only. */
export function goalPercent(goal: GoalItem): number {
  return Math.max(0, Math.round(goal.percentage_completed));
}

/**
 * "Target tabungan" (apply-markas-target D6, D7): one whole-row button per active goal with its status in words, a
 * 4 px bar and "Rp … dari Rp … · N%". The one goal flagged as Dana Darurat leaves the list and is reached from "…";
 * with several flagged goals each stays here with "Dana darurat" in its meta.
 */
export function GoalsSection({ data, onOpen, onAdd }: { data: GoalData; onOpen: (goal: GoalItem) => void; onAdd: () => void }) {
  const t = useTranslations("goals");
  const format = useFormat();
  const listed = data.emergencyGoal ? data.goals.filter((goal) => goal.id !== data.emergencyGoal!.id) : data.goals;
  const several = data.flaggedCount > 1;

  const header = (
    <SectionHeader
      id="tg-goals"
      label={t("list.title")}
      aside={data.goalsQuery.isSuccess && listed.length > 0 ? <span className="aside">{t("list.count", { count: listed.length })}</span> : undefined}
    />
  );
  if (data.goalsQuery.isLoading) {
    return (
      <section aria-labelledby="tg-goals" aria-busy="true">
        {header}
        <RowsSkeleton rows={2} />
      </section>
    );
  }
  if (data.goalsQuery.isError) {
    return (
      <section aria-labelledby="tg-goals">
        {header}
        <ErrorState message={t("states.goalsError")} onRetry={() => data.goalsQuery.refetch()} />
      </section>
    );
  }

  const status = (goal: GoalItem): ReactNode => {
    if (goal.remaining_amount <= 0) return <span className="c-income">{t("goal.reached")}</span>;
    if (!goal.target_date) return t("goal.noDate");
    if (goal.monthly_target_pace == null) return t("goal.paceUnknown");
    return t.rich("goal.until", {
      date: format.yearDate(goal.target_date),
      pace: () => <MaskedMoney text={format.money(goal.monthly_target_pace!)} />,
    });
  };
  const sources = (goal: GoalItem) => {
    const count = goal.account_ids?.length ?? goal.linked_accounts?.length ?? 0;
    return count > 0 ? t("goal.pockets", { count }) : t("goal.manual");
  };

  return (
    <section aria-labelledby="tg-goals">
      {header}
      {listed.length === 0 ? (
        <div className="tg-empty">
          <p className="t-body muted">{t("list.empty")}</p>
          <button type="button" className="ws-btn" onClick={onAdd}>
            <Icon name="plus" className="h-4 w-4" />
            {t("page.addGoal")}
          </button>
        </div>
      ) : (
        <ul className="list">
          {listed.map((goal) => {
            const reached = goal.remaining_amount <= 0;
            return (
              <li key={goal.id}>
                <button type="button" className="row ws-select-row goal-row" data-row={`goal-${goal.id}`} onClick={() => onOpen(goal)}>
                  <span className="r-main col">
                    <span className="r-title">{goal.name}</span>
                    <span className="r-meta">
                      {status(goal)} · {sources(goal)}
                      {several && goal.is_emergency && ` · ${t("goal.emergency")}`}
                    </span>
                    <ProgressBar value={goal.percentage_completed} max={100} color={reached ? "var(--in-solid)" : "var(--text)"} />
                  </span>
                  <span className="r-amt">
                    <MaskedMoney text={format.money(goal.current_amount)} className="t-amt" />
                    <span className="t-meta">
                      {t.rich("goal.of", { target: () => <MaskedMoney text={format.money(goal.target_amount)} />, pct: String(goalPercent(goal)) })}
                    </span>
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
