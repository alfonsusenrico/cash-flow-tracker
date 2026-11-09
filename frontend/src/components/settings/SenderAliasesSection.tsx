"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { deleteSenderAlias, getSenderAliases, updateSenderAlias, type SenderAlias } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Icon } from "@/components/ui/Icon";
import { Field } from "@/components/workspace/Field";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import { sectionHeadingId } from "@/components/settings/sections";
import { SettingsAlert, SettingsSection } from "@/components/settings/SettingsSection";

/** Above this many names the list gets a search field. */
export const SENDER_SEARCH_THRESHOLD = 8;

function fold(value: string) {
  return value.normalize("NFKD").replace(/\p{M}/gu, "").toLocaleLowerCase("id-ID");
}

function matches(alias: SenderAlias, query: string) {
  const needle = fold(query.trim());
  if (!needle) return true;
  return [alias.mask, alias.name, alias.institution, alias.account].some((value) => fold(value).includes(needle));
}

/**
 * Nama pengirim (apply-markas-pengaturan D14): confirmed sender names as plain rows with edit and delete icon buttons;
 * editing opens the name field in the row. Above eight names a search filters the list.
 */
export function SenderAliasesSection() {
  const t = useTranslations("settings.senders");
  const common = useTranslations("common");
  const queryClient = useQueryClient();
  const { data, isError, isPending, refetch } = useQuery({ queryKey: queryKeys.senderAliases, queryFn: getSenderAliases });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");
  const returnFocus = useRef<HTMLButtonElement | null>(null);

  function finishEditing() {
    setEditingId(null);
    setError("");
    requestAnimationFrame(() => returnFocus.current?.focus());
  }

  const save = useMutation({
    mutationFn: ({ id, value }: { id: string; value: string }) => updateSenderAlias(id, value),
    onSuccess: () => {
      setError("");
      finishEditing();
      queryClient.invalidateQueries({ queryKey: queryKeys.senderAliases });
    },
    onError: () => setError(t("saveFailed")),
  });
  const remove = useMutation({
    mutationFn: deleteSenderAlias,
    onSuccess: () => {
      setError("");
      queryClient.invalidateQueries({ queryKey: queryKeys.senderAliases });
      document.getElementById(sectionHeadingId("pengirim"))?.focus();
    },
    onError: () => setError(t("removeFailed")),
  });
  const busy = save.isPending || remove.isPending;
  const aliases = data?.aliases ?? [];
  const searchable = aliases.length > SENDER_SEARCH_THRESHOLD;
  const shown = searchable ? aliases.filter((alias) => matches(alias, query)) : aliases;

  return (
    <SettingsSection section="pengirim">
      <section aria-labelledby="senders-list">
        <SectionHeader id="senders-list" label={t("list")} aside={data ? <span className="aside">{aliases.length}</span> : undefined} />
        {error && <SettingsAlert id="sender-alias-error">{error}</SettingsAlert>}
        {isError ? (
          <ErrorState message={t("loadFailed")} onRetry={() => void refetch()} />
        ) : isPending ? (
          <div aria-busy="true">
            <Skeleton style={{ height: 56 }} />
            <p role="status" className="sr-only">
              {t("loading")}
            </p>
          </div>
        ) : aliases.length === 0 ? (
          <p className="t-body text-[var(--muted)]">{t("empty")}</p>
        ) : (
          <>
            {searchable && (
              <div className="form-grid set-search">
                <Field id="sender-search" className="full-row" label={t("search")} hint={<span role="status" className="tabular-nums">{t("matches", { shown: String(shown.length), total: String(aliases.length) })}</span>}>
                  {(field) => (
                    <input
                      {...field}
                      type="search"
                      value={query}
                      onChange={(event) => setQuery(event.target.value)}
                      placeholder={t("searchPlaceholder")}
                      className="field-control border"
                    />
                  )}
                </Field>
              </div>
            )}
            {shown.length === 0 ? (
              <p className="t-body text-[var(--muted)]">{t("noMatch")}</p>
            ) : (
              <ul className="list set-list" aria-labelledby="senders-list">
                {shown.map((alias) => (
                  <li key={alias.id}>
                    <ListRow
                      layout="stacked"
                      title={alias.name}
                      meta={
                        <>
                          {alias.mask} · {alias.institution.toUpperCase()} · {alias.account}
                          {alias.state === "ambiguous" && <span className="block c-warning">{t("ambiguous")}</span>}
                        </>
                      }
                      actions={
                        <>
                          <button
                            type="button"
                            className="ws-icon-btn"
                            aria-label={t("edit", { mask: alias.mask })}
                            disabled={busy || editingId !== null}
                            onClick={(event) => {
                              returnFocus.current = event.currentTarget;
                              setEditingId(alias.id);
                              setName(alias.name);
                              setError("");
                            }}
                          >
                            <Icon name="edit" className="h-4 w-4" />
                          </button>
                          <ConfirmRemove
                            icon
                            subject={`${alias.mask} → ${alias.name}`}
                            triggerLabel={t("remove", { mask: alias.mask })}
                            disabled={busy || editingId !== null}
                            pending={remove.isPending && remove.variables === alias.id}
                            onConfirm={() => remove.mutate(alias.id)}
                          />
                        </>
                      }
                    />
                    {editingId === alias.id && (
                      <form
                        className="set-inline form-stack"
                        onSubmit={(event) => {
                          event.preventDefault();
                          const value = name.normalize("NFKC").trim();
                          if (!value || Array.from(value).length > 80 || /\p{C}/u.test(value)) {
                            setError(t("nameInvalid"));
                            return;
                          }
                          save.mutate({ id: alias.id, value });
                        }}
                      >
                        <Field id={`sender-name-${alias.id}`} label={t("nameLabel")} describedBy={error ? "sender-alias-error" : undefined}>
                          {(field) => (
                            <input
                              {...field}
                              autoFocus
                              value={name}
                              maxLength={80}
                              disabled={busy}
                              aria-invalid={error ? true : undefined}
                              onChange={(event) => setName(event.target.value)}
                              className="field-control border"
                            />
                          )}
                        </Field>
                        <div className="set-actions">
                          <button type="submit" className="ws-btn ws-btn-accent" disabled={busy}>
                            {t("saveName")}
                          </button>
                          <button type="button" className="ws-btn" disabled={busy} onClick={finishEditing}>
                            {common("cancel")}
                          </button>
                        </div>
                      </form>
                    )}
                  </li>
                ))}
              </ul>
            )}
          </>
        )}
      </section>
    </SettingsSection>
  );
}
