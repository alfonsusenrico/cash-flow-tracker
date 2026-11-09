"use client";

import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { DisclosureSummary } from "@/components/ui/DisclosureSummary";
import { SectionHeader } from "@/components/workspace/lists";

export const QUESTION_RATE_WEEKS = 8;

export interface QuestionRateWeek {
  week_start: string;
  week_end: string;
  in_progress: boolean;
  recorded: number;
  asked: number;
  answered: number;
  by_memory: number;
  by_habit: number;
  by_ai: number;
  by_other: number;
  corrections: number;
  habit_corrections: number;
}

export interface QuestionRateData {
  weeks: QuestionRateWeek[];
  has_data: boolean;
}

function share(part: number, whole: number): string {
  return `${Math.round((100 * part) / whole)}%`;
}

function Count({ label, value, whole }: { label: string; value: number; whole: number }) {
  return (
    <div>
      <dt className="text-[var(--muted)]">{label}</dt>
      <dd className="tabular-nums">
        {value} ({share(value, whole)})
      </dd>
    </div>
  );
}

/**
 * "Seberapa sering ditanya" in Kebiasaan (apply-markas-analisis D18, apply-markas-pengaturan D10): this week's share
 * as a `t-fig` with its meta line, then per week how many recorded payments were asked and who recorded the others.
 */
export function QuestionRate() {
  const t = useTranslations("settings.questionRate");
  const format = useFormat();
  const { data, isLoading, isError } = useQuery<QuestionRateData>({
    queryKey: queryKeys.dashboard.questionRate(QUESTION_RATE_WEEKS),
    queryFn: () => api.get(`/dashboard/question-rate?weeks=${QUESTION_RATE_WEEKS}`),
  });
  const weeks = data ? [...data.weeks].reverse() : [];
  const thisWeek = weeks[0];
  return (
    <section aria-labelledby="question-rate-title">
      <SectionHeader id="question-rate-title" label={t("title")} />
      {isError && <p className="t-body">{t("loadFailed")}</p>}
      {!isError && isLoading && <p className="t-meta">{t("loading")}</p>}
      {data && !data.has_data && <p className="t-body text-[var(--muted)]">{t("empty", { weeks: String(QUESTION_RATE_WEEKS) })}</p>}
      {thisWeek && thisWeek.recorded > 0 && (
        <p className="qrate-fig" data-testid="question-rate-headline">
          <span className="t-fig">{share(thisWeek.asked, thisWeek.recorded)}</span>
          <span className="t-meta">{t("thisWeek", { asked: format.integer(thisWeek.asked), recorded: format.integer(thisWeek.recorded) })}</span>
        </p>
      )}
      {data?.has_data && (
        <details className="set-note">
          <DisclosureSummary>{t("details")}</DisclosureSummary>
          <ol className="qrate-weeks">
            {weeks.map((week) => (
              <li key={week.week_start}>
                <div className="set-actions">
                  <h3 className="t-strong">
                    {format.shortDate(week.week_start)} – {format.shortDate(week.week_end)}
                  </h3>
                  {week.in_progress && <span className="ws-badge">{t("running")}</span>}
                  {week.recorded > 0 && (
                    <p className="t-body tabular-nums ml-auto">{t("asked", { asked: format.integer(week.asked), recorded: format.integer(week.recorded), share: share(week.asked, week.recorded) })}</p>
                  )}
                </div>
                {week.recorded === 0 ? (
                  <p className="t-meta">{t("none")}</p>
                ) : (
                  <>
                    <span className="prog" aria-hidden="true">
                      <span style={{ width: share(week.asked, week.recorded) }} />
                    </span>
                    <dl className="t-meta">
                      <Count label={t("memory")} value={week.by_memory} whole={week.recorded} />
                      <Count label={t("habit")} value={week.by_habit} whole={week.recorded} />
                      <Count label={t("ai")} value={week.by_ai} whole={week.recorded} />
                      <Count label={t("other")} value={week.by_other} whole={week.recorded} />
                      <Count label={t("answered")} value={week.answered} whole={week.recorded} />
                    </dl>
                  </>
                )}
                {(week.recorded > 0 || week.corrections > 0) && (
                  <p className="t-meta tabular-nums set-note">
                    {week.habit_corrections > 0
                      ? t("correctionsWithHabit", { total: format.integer(week.corrections), habit: format.integer(week.habit_corrections) })
                      : t("corrections", { total: format.integer(week.corrections) })}
                  </p>
                )}
              </li>
            ))}
          </ol>
        </details>
      )}
    </section>
  );
}
