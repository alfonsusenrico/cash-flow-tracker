"use client";

import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { isKakeiboPillar } from "@/lib/kakeibo";
import { habitStatus, type HabitList, type HabitState } from "@/lib/habits";
import { PILLAR_TERM } from "@/components/analysis/KakeiboRows";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { EmptyState, ErrorState, Skeleton } from "@/components/workspace/primitives";
import { QuestionRate } from "@/components/settings/QuestionRate";
import { SettingRow } from "@/components/settings/SettingRow";
import { SettingsSection } from "@/components/settings/SettingsSection";

function StatusWord({ state, confirmations, needed, autoOn }: { state: HabitState; confirmations: number; needed: number; autoOn: boolean }) {
  const t = useTranslations("settings.habits.status");
  const status = habitStatus(state, autoOn);
  // Only "Dicatat otomatis" reads in ink; the other words are muted (D10).
  return <span className={status === "auto" ? "r-end is-ink" : "r-end"}>{status === "learning" ? t("learning", { done: String(confirmations), needed: String(needed) }) : t(status)}</span>;
}

/**
 * Kebiasaan (apply-markas-pengaturan D10): the server's two auto-record states as read-only status rows, then the
 * payment habits and the investment top-ups as plain rows, then "Seberapa sering ditanya" (apply-markas-analisis D18).
 */
export function HabitsSettings() {
  const t = useTranslations("settings.habits");
  const terms = useTranslations("terms");
  const money = useMoney();
  const query = useQuery<HabitList>({ queryKey: queryKeys.habits.list, queryFn: () => api.get("/habits") });
  const data = query.data;
  const needed = data?.thresholds.min_confirmations ?? 3;

  return (
    <SettingsSection section="kebiasaan">
      {query.isLoading ? (
        <div aria-busy="true">
          <Skeleton style={{ height: 112 }} />
        </div>
      ) : query.isError || !data ? (
        <ErrorState message={t("loadFailed")} onRetry={() => void query.refetch()} />
      ) : (
        <>
          <div>
            <SettingRow
              titleId="habit-auto-record"
              title={t("autoRecord")}
              meta={t("rule", { count: needed, pct: String(Math.round(data.thresholds.min_agreement * 100)) })}
              end={<span className="tg-end">{data.auto_record.habits ? t("on") : t("off")}</span>}
            />
            <SettingRow
              titleId="habit-auto-topup"
              title={t("autoTopup")}
              end={<span className="tg-end">{data.auto_record.topups ? t("on") : t("off")}</span>}
            />
          </div>
          {data.habits.length === 0 && data.topups.length === 0 ? (
            <EmptyState title={t("empty")}>{t("emptyLine")}</EmptyState>
          ) : (
            <>
              {data.habits.length > 0 && (
                <section aria-labelledby="habits-payments">
                  <SectionHeader id="habits-payments" label={t("payments")} aside={<span className="aside">{data.habits.length}</span>} />
                  <ul className="list set-list" aria-labelledby="habits-payments">
                    {data.habits.map((habit) => {
                      const pillar = isKakeiboPillar(habit.kakeibo) ? terms(PILLAR_TERM[habit.kakeibo]) : null;
                      const meta = pillar
                        ? t("habitMeta", { category: habit.category.name, pillar, times: String(habit.confirmations) })
                        : t("habitMetaNoPillar", { category: habit.category.name, times: String(habit.confirmations) });
                      return (
                        <li key={habit.key}>
                          <ListRow
                            layout="stacked"
                            title={`${habit.label} · ${habit.account.name ?? t("accountFallback")}`}
                            meta={meta}
                            actions={<StatusWord state={habit.state} confirmations={habit.confirmations} needed={needed} autoOn={data.auto_record.habits} />}
                          />
                        </li>
                      );
                    })}
                  </ul>
                </section>
              )}
              {data.topups.length > 0 && (
                <section aria-labelledby="habits-topups">
                  <SectionHeader id="habits-topups" label={t("topups")} aside={<span className="aside">{data.topups.length}</span>} />
                  <ul className="list set-list" aria-labelledby="habits-topups">
                    {data.topups.map((topup) => {
                      const product = topup.product.parent_name ? `${topup.product.parent_name} · ${topup.product.name}` : topup.product.name;
                      return (
                        <li key={topup.key}>
                          <ListRow
                            layout="stacked"
                            title={
                              <>
                                <MaskedMoney text={money(topup.amount)} /> {t("fromSource", { source: topup.source_account.name })}
                              </>
                            }
                            meta={t("topupMeta", { product, times: String(topup.confirmations) })}
                            actions={<StatusWord state={topup.state} confirmations={topup.confirmations} needed={needed} autoOn={data.auto_record.topups} />}
                          />
                        </li>
                      );
                    })}
                  </ul>
                </section>
              )}
              {data.truncated && <p className="t-meta">{t("truncated")}</p>}
            </>
          )}
        </>
      )}
      <QuestionRate />
    </SettingsSection>
  );
}
