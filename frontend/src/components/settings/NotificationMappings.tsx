"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { deleteNotificationAlias, getNotificationAliases } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import { sectionHeadingId } from "@/components/settings/sections";
import { SettingsAlert, SettingsSection } from "@/components/settings/SettingsSection";

/**
 * Pemetaan notifikasi (apply-markas-pengaturan D14): account names learned from notifications as plain rows with a
 * trash button and the inline confirmation; removing one affects only future notifications.
 */
export function NotificationMappings() {
  const t = useTranslations("settings.mappings");
  const qc = useQueryClient();
  const { data, isError, isPending, refetch } = useQuery({ queryKey: queryKeys.notificationAliases, queryFn: getNotificationAliases });
  const [error, setError] = useState("");

  const remove = useMutation({
    mutationFn: deleteNotificationAlias,
    onSuccess: () => {
      setError("");
      document.getElementById(sectionHeadingId("pemetaan"))?.focus();
      void qc.invalidateQueries({ queryKey: queryKeys.notificationAliases });
    },
    onError: (e: Error) => setError(e.message || t("removeFailed")),
  });

  return (
    <SettingsSection section="pemetaan">
      <section aria-labelledby="mappings-list">
        <SectionHeader id="mappings-list" label={t("list")} aside={data ? <span className="aside">{data.aliases.length}</span> : undefined} />
        {error && <SettingsAlert>{error}</SettingsAlert>}
        {isError ? (
          <ErrorState message={t("loadFailed")} onRetry={() => void refetch()} />
        ) : isPending ? (
          <div aria-busy="true">
            <Skeleton style={{ height: 56 }} />
          </div>
        ) : data.aliases.length === 0 ? (
          <p className="t-body text-[var(--muted)]">{t("empty")}</p>
        ) : (
          <ul className="list set-list" aria-labelledby="mappings-list">
            {data.aliases.map((alias) => (
              <li key={alias.id}>
                <ListRow
                  layout="stacked"
                  title={`“${alias.name}”`}
                  meta={
                    <>
                      <span aria-hidden="true">→ </span>
                      <span className="sr-only">{t("mappedTo")} </span>
                      {alias.account} · {alias.institution.toUpperCase()} · {alias.source === "owner" ? t("owner") : t("auto")}
                    </>
                  }
                  actions={
                    <ConfirmRemove
                      icon
                      subject={t("subject", { text: `“${alias.name}”` })}
                      triggerLabel={t("remove", { text: alias.name })}
                      disabled={remove.isPending}
                      pending={remove.isPending && remove.variables === alias.id}
                      onConfirm={() => remove.mutate(alias.id)}
                    />
                  }
                />
              </li>
            ))}
          </ul>
        )}
      </section>
    </SettingsSection>
  );
}
