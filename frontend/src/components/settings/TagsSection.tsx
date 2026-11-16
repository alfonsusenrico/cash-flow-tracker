"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { Field } from "@/components/workspace/Field";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { sectionHeadingId } from "@/components/settings/sections";
import { SettingsAlert, SettingsSection } from "@/components/settings/SettingsSection";
import { queryKeys } from "@/lib/queryKeys";
import { TAG_NAME_MAX_LENGTH, getTags, mergeTag, tagDisplayName, tagErrorCode, tagNameValid, updateTag, type Tag } from "@/lib/tags";

/**
 * Tag (transaction-tags D5): the owner's tags with their row counts. A tag is renamed in its row (Enter saves, Esc
 * cancels), merged into another after a confirmation that states the result, and archived or restored. Archived
 * tags are listed apart; they stay on their rows and in the ledger filter.
 */
export function TagsSection() {
  const t = useTranslations("settings.tags");
  const common = useTranslations("common");
  const queryClient = useQueryClient();
  const { data, isError, isPending, refetch } = useQuery({ queryKey: queryKeys.tags.list(true), queryFn: () => getTags(true) });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [nameError, setNameError] = useState("");
  const [mergingId, setMergingId] = useState<string | null>(null);
  const [mergeInto, setMergeInto] = useState("");
  const [mergeError, setMergeError] = useState("");
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  // The button that opened the rename or merge form, which takes focus back when the form closes.
  const [returnFocusId, setReturnFocusId] = useState("");

  const tags = data?.tags ?? [];
  const active = tags.filter((tag) => !tag.archived);
  const archived = tags.filter((tag) => tag.archived);
  const busy = editingId !== null || mergingId !== null;

  // A rename or merge changes the names rows show, and every tag change changes the pickers.
  const refreshTags = () => {
    queryClient.invalidateQueries({ queryKey: queryKeys.tags.all });
    queryClient.invalidateQueries({ queryKey: queryKeys.transactions.all });
  };
  const focusHeading = () => requestAnimationFrame(() => document.getElementById(sectionHeadingId("tag"))?.focus());

  const finishEditing = () => {
    setEditingId(null);
    setNameError("");
    requestAnimationFrame(() => document.getElementById(returnFocusId)?.focus());
  };
  const finishMerging = () => {
    setMergingId(null);
    setMergeInto("");
    setMergeError("");
    requestAnimationFrame(() => document.getElementById(returnFocusId)?.focus());
  };

  const rename = useMutation({
    mutationFn: ({ id, value }: { id: string; value: string }) => updateTag(id, { name: value }),
    onSuccess: () => {
      refreshTags();
      finishEditing();
    },
    onError: (failure: Error) => setNameError(tagErrorCode(failure) && failure.message ? failure.message : t("saveFailed")),
  });
  const archive = useMutation({
    mutationFn: ({ id, value }: { id: string; value: boolean }) => updateTag(id, { archived: value }),
    onSuccess: () => {
      setError("");
      refreshTags();
      focusHeading();
    },
    onError: () => setError(t("saveFailed")),
  });
  const merge = useMutation({
    mutationFn: ({ id, into }: { id: string; into: string }) => mergeTag(id, into),
    onSuccess: (_result, { id, into }) => {
      const source = tags.find((tag) => tag.id === id);
      const target = tags.find((tag) => tag.id === into);
      setStatus(t("merged", { name: source?.name ?? "", into: target?.name ?? "" }));
      setMergingId(null);
      setMergeInto("");
      setMergeError("");
      refreshTags();
      focusHeading();
    },
    onError: (failure: Error) => setMergeError(tagErrorCode(failure) && failure.message ? failure.message : t("mergeFailed")),
  });
  const pending = rename.isPending || archive.isPending || merge.isPending;

  const startEditing = (tag: Tag) => {
    setReturnFocusId(`tag-rename-button-${tag.id}`);
    setEditingId(tag.id);
    setName(tag.name);
    setNameError("");
    setStatus("");
  };
  const startMerging = (tag: Tag) => {
    setReturnFocusId(`tag-merge-button-${tag.id}`);
    setMergingId(tag.id);
    setMergeInto("");
    setMergeError("");
    setStatus("");
  };

  const renameForm = (tag: Tag) => {
    const fieldId = `tag-name-${tag.id}`;
    return (
      <form
        className="set-inline form-stack"
        onSubmit={(event) => {
          event.preventDefault();
          if (!tagNameValid(name)) {
            setNameError(t("nameInvalid", { max: String(TAG_NAME_MAX_LENGTH) }));
            return;
          }
          const value = tagDisplayName(name);
          if (value === tag.name) {
            finishEditing();
            return;
          }
          rename.mutate({ id: tag.id, value });
        }}
      >
        <Field id={fieldId} label={t("nameLabel")} error={nameError || undefined} hint={t("nameHint")}>
          {(field) => (
            <input
              {...field}
              autoFocus
              value={name}
              maxLength={TAG_NAME_MAX_LENGTH}
              disabled={rename.isPending}
              autoComplete="off"
              onChange={(event) => {
                setName(event.target.value);
                setNameError("");
              }}
              onKeyDown={(event) => {
                if (event.key !== "Escape") return;
                // Esc cancels the rename only; the page around it stays.
                event.preventDefault();
                event.stopPropagation();
                finishEditing();
              }}
              className="field-control border"
            />
          )}
        </Field>
        <div className="set-actions">
          <button type="submit" className="ws-btn ws-btn-accent" disabled={rename.isPending}>
            {t("saveName")}
          </button>
          <button type="button" className="ws-btn" disabled={rename.isPending} onClick={finishEditing}>
            {common("cancel")}
          </button>
        </div>
      </form>
    );
  };

  const mergeForm = (tag: Tag) => {
    const fieldId = `tag-merge-into-${tag.id}`;
    const target = tags.find((item) => item.id === mergeInto);
    return (
      <form
        className="set-inline form-stack"
        onKeyDown={(event) => {
          if (event.key !== "Escape") return;
          event.preventDefault();
          event.stopPropagation();
          finishMerging();
        }}
        onSubmit={(event) => {
          event.preventDefault();
          if (!target) {
            setMergeError(t("mergeChooseFirst"));
            document.getElementById(fieldId)?.focus();
            return;
          }
          merge.mutate({ id: tag.id, into: target.id });
        }}
      >
        <Field id={fieldId} label={t("mergeInto", { name: tag.name })} error={mergeError || undefined}>
          {(field) => (
            <select
              {...field}
              autoFocus
              value={mergeInto}
              disabled={merge.isPending}
              onChange={(event) => {
                setMergeInto(event.target.value);
                setMergeError("");
              }}
              className="field-control border"
            >
              <option value="">{t("mergeChoose")}</option>
              {tags.filter((item) => item.id !== tag.id).map((item) => (
                <option key={item.id} value={item.id}>{item.archived ? `${item.name} · ${t("archivedList")}` : item.name}</option>
              ))}
            </select>
          )}
        </Field>
        {target && (
          <p className="t-body" role="status">
            {t("mergeResult", { count: tag.transaction_count, into: target.name, name: tag.name })}
          </p>
        )}
        <div className="set-actions">
          <button type="submit" className="ws-btn ws-btn-accent" disabled={merge.isPending}>
            {t("mergeYes")}
          </button>
          <button type="button" className="ws-btn" disabled={merge.isPending} onClick={finishMerging}>
            {common("cancel")}
          </button>
        </div>
      </form>
    );
  };

  const rows = (list: Tag[], labelledBy: string) => (
    <ul className="list set-list" aria-labelledby={labelledBy}>
      {list.map((tag) => (
        <li key={tag.id}>
          <ListRow
            layout="stacked"
            icon="tag"
            title={tag.name}
            meta={<span className="tabular-nums">{t("count", { count: tag.transaction_count })}</span>}
            actions={
              <>
                {!tag.archived && (
                  <button
                    id={`tag-rename-button-${tag.id}`}
                    type="button"
                    className="ws-icon-btn"
                    aria-label={t("rename", { name: tag.name })}
                    disabled={busy || pending}
                    onClick={() => startEditing(tag)}
                  >
                    <Icon name="edit" className="h-4 w-4" />
                  </button>
                )}
                {tags.length > 1 && (
                  <button
                    id={`tag-merge-button-${tag.id}`}
                    type="button"
                    className="ws-btn ws-btn-quiet"
                    aria-label={t("merge", { name: tag.name })}
                    disabled={busy || pending}
                    onClick={() => startMerging(tag)}
                  >
                    {t("mergeVerb")}
                  </button>
                )}
                <button
                  type="button"
                  className="ws-btn ws-btn-quiet"
                  aria-label={tag.archived ? t("restore", { name: tag.name }) : t("archive", { name: tag.name })}
                  disabled={busy || pending}
                  onClick={() => archive.mutate({ id: tag.id, value: !tag.archived })}
                >
                  {tag.archived ? t("restoreVerb") : t("archiveVerb")}
                </button>
              </>
            }
          />
          {editingId === tag.id && renameForm(tag)}
          {mergingId === tag.id && mergeForm(tag)}
        </li>
      ))}
    </ul>
  );

  return (
    <SettingsSection section="tag">
      {error && <SettingsAlert>{error}</SettingsAlert>}
      {isError ? (
        <ErrorState message={t("loadFailed")} onRetry={() => void refetch()} />
      ) : isPending ? (
        <div aria-busy="true">
          <Skeleton style={{ height: 56 }} />
          <p role="status" className="sr-only">{t("loading")}</p>
        </div>
      ) : tags.length === 0 ? (
        <p className="t-body text-[var(--muted)]">{t("empty")}</p>
      ) : (
        <>
          <section aria-labelledby="tags-active">
            <SectionHeader id="tags-active" label={t("list")} aside={<span className="aside tabular-nums">{active.length}</span>} />
            {active.length > 0 && rows(active, "tags-active")}
          </section>
          {archived.length > 0 && (
            <section aria-labelledby="tags-archived">
              <SectionHeader id="tags-archived" label={t("archivedList")} aside={<span className="aside tabular-nums">{archived.length}</span>} />
              <p className="t-meta">{t("archivedHint")}</p>
              {rows(archived, "tags-archived")}
            </section>
          )}
        </>
      )}
      <p className="sr-only" role="status">{status}</p>
    </SettingsSection>
  );
}
