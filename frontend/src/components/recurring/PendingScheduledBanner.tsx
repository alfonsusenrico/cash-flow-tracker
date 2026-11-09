"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { useFormat, type Format } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { Icon } from "@/components/ui/Icon";
import { useInterfaceLang } from "@/components/workspace/untranslated";
import { AutoLinkedOccurrence, DueExpectation } from "@/types/recurring";
import { DuePlanActions } from "./DuePlanActions";

interface Props {
  onOpenRulesManager?: () => void;
}

type DuePlansText = ReturnType<typeof useTranslations<"shell.duePlans">>;

function autoLinkLabel(link: AutoLinkedOccurrence, t: DuePlansText, format: Format) {
  const evidence = link.merchant || link.notes || link.account_name || t("recordedTransaction");
  const day = link.transaction_date ? format.shortDateAt(link.transaction_date) : t("transactionDeleted");
  return `${day} · ${format.money(link.amount)} · ${evidence}`;
}

/**
 * Due plans are reminders: the owner links an actual payment, skips the occurrence, or records a missed one.
 * "Rencana jatuh tempo" opens from Beranda and the bell, so it is translated with them (add-language-system D10).
 */
export function PendingScheduledBanner({ onOpenRulesManager }: Props = {}) {
  const t = useTranslations("shell.duePlans");
  const format = useFormat();
  const lang = useInterfaceLang();
  const planDay = format.shortDate;
  const qc = useQueryClient();

  const { data } = useQuery<{
    ok: boolean;
    pending_count: number;
    rules: DueExpectation[];
    recent_auto_links?: AutoLinkedOccurrence[];
  }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    refetchInterval: 30000,
  });
  const dueRules = data?.rules ?? [];
  const autoLinks = data?.recent_auto_links ?? [];
  const [unlinkErrors, setUnlinkErrors] = useState<Record<string, string>>({});

  const unlink = useMutation({
    mutationFn: async (link: AutoLinkedOccurrence) =>
      api.post(`/recurring/${link.rule_id}/occurrences/unlink`, { scheduled_for: link.scheduled_for }),
    onMutate: (link) => setUnlinkErrors((current) => ({ ...current, [linkKey(link)]: "" })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
    },
    onError: (error: Error, link) => {
      setUnlinkErrors((current) => ({ ...current, [linkKey(link)]: error.message || t("unlinkFailed") }));
    },
  });

  if (dueRules.length === 0 && autoLinks.length === 0) return null;

  return (
    <div className="space-y-3" lang={lang}>
      {autoLinks.length > 0 && (
        <section aria-labelledby="auto-linked-title" className="space-y-2 rounded-xl border border-[var(--line)] bg-[var(--surface)] p-4 text-xs text-[var(--text)] sm:p-5">
          <div>
            <h2 id="auto-linked-title" className="text-xs font-semibold sm:text-sm">{t("autoLinkedTitle")}</h2>
            <p className="mt-0.5 text-[var(--text-2)]">{t("autoLinkedRule")}</p>
          </div>
          <ul className="space-y-2 border-t border-[var(--line)] pt-2">
            {autoLinks.map((link) => {
              const key = linkKey(link);
              const busy = unlink.isPending && unlink.variables && linkKey(unlink.variables) === key;
              return (
                <li key={key} className="flex min-w-0 flex-wrap items-center justify-between gap-2">
                  <div className="min-w-0">
                    <p className="truncate font-semibold">
                      {link.name}
                      {link.obligation_name && link.obligation_name !== link.name ? ` · ${link.obligation_name}` : ""}
                    </p>
                    <p className="text-[var(--text-2)]">
                      <span className="mr-1 inline-flex rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-1.5 font-semibold text-[var(--text)]">
                        {t("autoLinkedMark")}
                      </span>
                      <span className="tabular-nums">
                        {t("dueOn", { day: planDay(link.scheduled_for) })} · {autoLinkLabel(link, t, format)}
                      </span>
                    </p>
                    {unlinkErrors[key] && (
                      <p role="alert" className="font-medium text-out-fg">{unlinkErrors[key]}</p>
                    )}
                  </div>
                  <button
                    type="button"
                    disabled={Boolean(busy)}
                    onClick={() => unlink.mutate(link)}
                    aria-label={t("unlinkLabel", { name: link.name, day: planDay(link.scheduled_for) })}
                    className="min-h-11 shrink-0 rounded-md border border-[var(--line)] px-3 font-semibold disabled:opacity-50"
                  >
                    {t("unlink")}
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
      )}
      {dueRules.length > 0 && (
      <section
        aria-labelledby="due-plans-title"
        className="space-y-3 rounded-xl border border-[var(--line)] bg-[var(--surface-2)] p-4 text-[var(--text)] sm:p-5"
      >
        <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
          <div className="flex items-start gap-2.5">
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-[var(--surface-2)] text-warn-fg">
              <Icon name="clock" className="h-4 w-4" />
            </div>
            <div>
              <h2 id="due-plans-title" className="text-xs font-semibold sm:text-sm">
                {t("dueCount", { count: dueRules.length })}
              </h2>
              <p className="mt-0.5 text-xs text-[var(--text-2)]">{t("dueRule")}</p>
            </div>
          </div>
          {onOpenRulesManager && (
            <button
              type="button"
              onClick={onOpenRulesManager}
              className="min-h-11 self-start rounded-md border border-[var(--line)] px-3 text-xs font-semibold text-warn-fg hover:bg-[var(--surface-2)]"
            >
              {t("managePlans")}
            </button>
          )}
        </div>

        <ul className="space-y-2 border-t border-[var(--line)] pt-2">
          {dueRules.map((rule) => (
            <li key={rule.id} className="min-w-0 space-y-2 rounded-xl border border-[var(--line)] bg-[var(--surface)]/80 p-3 text-xs">
              <DuePlanActions rule={rule} />
            </li>
          ))}
        </ul>
      </section>
      )}
    </div>
  );
}

function linkKey(link: AutoLinkedOccurrence) {
  return `${link.rule_id}:${link.scheduled_for}`;
}
