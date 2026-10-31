"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";

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

function day(isoDate: string): string {
  return new Date(`${isoDate}T00:00:00`).toLocaleDateString("id-ID", { day: "numeric", month: "short" });
}

function share(part: number, whole: number): string {
  return `${Math.round((100 * part) / whole)}%`;
}

function Count({ label, value, whole }: { label: string; value: number; whole: number }) {
  return (
    <div className="inline-flex items-baseline gap-1">
      <dt className="text-[var(--muted)]">{label}</dt>
      <dd className="font-semibold tabular-nums text-[var(--text)]">
        {value} <span className="font-normal text-[var(--text-secondary)]">({share(value, whole)})</span>
      </dd>
    </div>
  );
}

/** Per week: how many recorded payments the owner was asked about, and who recorded the others. */
export function QuestionRate() {
  const { data, isLoading, isError } = useQuery<QuestionRateData>({
    queryKey: queryKeys.dashboard.questionRate(QUESTION_RATE_WEEKS),
    queryFn: () => api.get(`/dashboard/question-rate?weeks=${QUESTION_RATE_WEEKS}`),
  });
  const weeks = data ? [...data.weeks].reverse() : [];
  return (
    <section aria-labelledby="question-rate-title" className="cockpit-card space-y-3 border border-[var(--border)] p-4 text-xs shadow-xs sm:p-6">
      <div>
        <h2 id="question-rate-title" className="text-base font-bold tracking-tight text-[var(--text)]">Seberapa sering ditanya</h2>
        <p className="mt-0.5 text-[var(--text-secondary)]">
          Transaksi dari notifikasi dan ketikan per minggu: berapa yang ditanyakan ke Anda, dan siapa yang mencatat sisanya.
        </p>
      </div>
      {isError && <p className="text-[var(--text-secondary)]">Hitungan belum dapat dimuat.</p>}
      {!isError && isLoading && <p className="text-[var(--text-secondary)]">Memuat…</p>}
      {data && !data.has_data && (
        <p className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3 text-[var(--text-secondary)]">
          Belum ada transaksi dari notifikasi atau ketikan dalam {QUESTION_RATE_WEEKS} minggu terakhir.
        </p>
      )}
      {data?.has_data && (
        <ol className="divide-y divide-[var(--border)]">
          {weeks.map((week) => (
            <li key={week.week_start} className="space-y-1.5 py-2.5">
              <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
                <h3 className="font-semibold text-[var(--text)]">
                  {day(week.week_start)} – {day(week.week_end)}
                </h3>
                {week.in_progress && (
                  <span className="shrink-0 rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 font-semibold text-amber-800 dark:text-amber-200">
                    berjalan
                  </span>
                )}
                {week.recorded > 0 && (
                  <p className="ml-auto shrink-0 tabular-nums text-[var(--text)]">
                    Ditanya <span className="font-bold">{week.asked}</span> dari {week.recorded} ({share(week.asked, week.recorded)})
                  </p>
                )}
              </div>
              {week.recorded === 0 ? (
                <p className="text-[var(--muted)]">Tidak ada transaksi tercatat.</p>
              ) : (
                <>
                  <div aria-hidden="true" className="h-1.5 overflow-hidden rounded-full bg-[var(--border)]">
                    <div className="h-full rounded-full bg-[var(--text-secondary)]" style={{ width: share(week.asked, week.recorded) }} />
                  </div>
                  <dl className="flex flex-wrap gap-x-3 gap-y-1">
                    <Count label="Memori" value={week.by_memory} whole={week.recorded} />
                    <Count label="Kebiasaan" value={week.by_habit} whole={week.recorded} />
                    <Count label="AI" value={week.by_ai} whole={week.recorded} />
                    <Count label="Aturan & lainnya" value={week.by_other} whole={week.recorded} />
                    <Count label="Dijawab Anda" value={week.answered} whole={week.recorded} />
                  </dl>
                </>
              )}
              {(week.recorded > 0 || week.corrections > 0) && (
                <p className="tabular-nums text-[var(--text-secondary)]">
                  Koreksi: {week.corrections}
                  {week.habit_corrections > 0 && ` (${week.habit_corrections} dari kebiasaan)`}
                </p>
              )}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
