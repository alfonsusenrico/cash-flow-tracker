"use client";

import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { Icon } from "@/components/ui/Icon";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { useMoney } from "@/components/workspace/money";
import { api } from "@/lib/api";
import type { KakeiboPillar } from "@/lib/kakeibo";
import { queryKeys } from "@/lib/queryKeys";
import { isNonReportableCategory } from "@/lib/systemCategories";
import { CATEGORY_FORM_ID, CategoryForm, type CategoryPayload, type ManagedCategory } from "./CategoryForm";

type View = { mode: "list" } | { mode: "form"; category: ManagedCategory | null; kind: "expense" | "income" };

const PILLAR_GROUPS: { key: KakeiboPillar | "none"; term: "needs" | "wants" | "culture" | "unexpected" | "savings" | "noPillarYet"; rail: string }[] = [
  { key: "need", term: "needs", rail: "var(--k-need)" },
  { key: "want", term: "wants", rail: "var(--k-want)" },
  { key: "culture", term: "culture", rail: "var(--k-culture)" },
  { key: "unexpected", term: "unexpected", rail: "var(--k-unexpected)" },
  { key: "saving", term: "savings", rail: "var(--k-saving)" },
  { key: "none", term: "noPillarYet", rail: "var(--k-none)" },
];

function pillarKey(value: string | null | undefined): KakeiboPillar | "none" {
  return PILLAR_GROUPS.some((group) => group.key === value) ? (value as KakeiboPillar) : "none";
}

/**
 * "Kelola kategori" (apply-markas-analisis D9): the categories by pillar, the income categories and the archived ones
 * in one 640 px dialog; the form replaces the list inside the same dialog. Every request is the one the former
 * Analisis section sent: `POST /categories`, `PATCH /categories/{id}`, `DELETE /categories/{id}` (archive) and
 * `PATCH {is_archived: false}` (restore).
 */
export function CategoryManagerDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const t = useTranslations("analysis.manager");
  const terms = useTranslations("terms");
  const common = useTranslations("common");
  const money = useMoney();
  const queryClient = useQueryClient();
  const [view, setView] = useState<View>({ mode: "list" });
  const [error, setError] = useState("");
  // Where focus goes back to when the form closes: the row's edit button, or the add button.
  const returnTo = useRef<string | null>(null);

  useEffect(() => {
    if (!open) {
      setView({ mode: "list" });
      setError("");
    }
  }, [open]);

  useEffect(() => {
    if (view.mode !== "list" || !returnTo.current) return;
    const key = returnTo.current;
    returnTo.current = null;
    const target = document.querySelector<HTMLElement>(`[data-focus-key="${CSS.escape(key)}"]`);
    target?.focus();
  }, [view]);

  const query = useQuery<{ categories: ManagedCategory[] }>({
    queryKey: queryKeys.categoryManagement,
    queryFn: () => api.get("/categories?include_archived=true"),
    enabled: open,
  });
  const refresh = () => {
    queryClient.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    queryClient.invalidateQueries({ queryKey: queryKeys.categories });
    queryClient.invalidateQueries({ queryKey: queryKeys.reports.all });
  };

  const save = useMutation({
    mutationFn: async ({ payload, id }: { payload: CategoryPayload; id: string | null }) =>
      id ? api.patch(`/categories/${id}`, payload) : api.post("/categories", payload),
    onSuccess: () => {
      refresh();
      setError("");
      setView({ mode: "list" });
    },
    onError: (failure: Error) => setError(failure.message || t("saveFailed")),
  });
  const archive = useMutation({
    mutationFn: async (id: string) => api.del(`/categories/${id}`),
    onSuccess: refresh,
    onError: (failure: Error) => setError(failure.message || t("archiveFailed")),
  });
  const restore = useMutation({
    mutationFn: async (id: string) => api.patch(`/categories/${id}`, { is_archived: false }),
    onSuccess: refresh,
    onError: (failure: Error) => setError(failure.message || t("restoreFailed")),
  });

  const all = (query.data?.categories ?? []).filter((category) => !isNonReportableCategory(category.system_key));
  const active = all.filter((category) => !category.is_archived);
  const expense = active.filter((category) => category.kind !== "income");
  const income = active.filter((category) => category.kind === "income");
  const archived = all.filter((category) => category.is_archived);
  const byName = (left: ManagedCategory, right: ManagedCategory) => left.name.localeCompare(right.name);

  const openForm = (category: ManagedCategory | null, kind: "expense" | "income", focusKey: string) => {
    returnTo.current = focusKey;
    setError("");
    setView({ mode: "form", category, kind });
  };
  const back = () => {
    setError("");
    setView({ mode: "list" });
  };

  const row = (category: ManagedCategory) => {
    const meta = category.kind === "income"
      ? null
      : category.monthly_budget && category.monthly_budget > 0
        ? t("budget", { amount: money(category.monthly_budget) })
        : t("noBudget");
    return (
      <li key={category.id}>
        <div className="row is-plain mgr-row">
          <span className="r-main">
            <span className="r-title">{category.name}</span>
            {meta && <span className="r-meta">{meta}</span>}
          </span>
          <span className="r-actions">
            <button
              type="button"
              className="ws-icon-btn"
              data-focus-key={`edit-${category.id}`}
              aria-label={t("edit", { name: category.name })}
              title={t("edit", { name: category.name })}
              onClick={() => openForm(category, category.kind === "income" ? "income" : "expense", `edit-${category.id}`)}
            >
              <Icon name="edit" className="h-4 w-4" />
            </button>
            <ConfirmActionButton
              label={t("archive", { name: category.name })}
              confirmation={t("archiveConfirm", { name: category.name })}
              onConfirm={() => archive.mutate(category.id)}
              disabled={archive.isPending}
              className="ws-icon-btn mgr-archive"
            >
              <Icon name="archive" className="h-4 w-4" />
            </ConfirmActionButton>
          </span>
        </div>
      </li>
    );
  };

  const group = (key: string, label: string, rail: string, rows: ManagedCategory[], render: (category: ManagedCategory) => React.ReactNode) => (
    rows.length > 0 && (
      <div key={key} className="grp" style={{ "--rail": rail } as React.CSSProperties}>
        <div className="grp-h">
          <h3 className="t-label">{label}</h3>
          <span className="count">{t("count", { count: rows.length })}</span>
        </div>
        <ul className="list">{[...rows].sort(byName).map(render)}</ul>
      </div>
    )
  );

  let body: React.ReactNode;
  if (view.mode === "form") {
    body = (
      <>
        <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm mgr-back" onClick={back}>
          <Icon name="chevron-left" className="h-4 w-4" />
          {t("back")}
        </button>
        <CategoryForm
          key={view.category?.id ?? `new-${view.kind}`}
          category={view.category}
          kind={view.kind}
          error={error}
          onSubmit={(payload) => save.mutate({ payload, id: view.category?.id ?? null })}
        />
      </>
    );
  } else if (query.isPending) {
    body = <div role="status" aria-busy="true"><Skeleton style={{ height: 160 }} /></div>;
  } else if (query.isError) {
    body = <ErrorState message={t("loadFailed")} onRetry={() => query.refetch()} />;
  } else {
    body = (
      <div className="mgr-groups">
        {error && <p role="alert" className="form-alert">{error}</p>}
        {PILLAR_GROUPS.map((pillar) => group(pillar.key, terms(pillar.term), pillar.rail, expense.filter((category) => pillarKey(category.kakeibo_type) === pillar.key), row))}
        {group("income", t("income"), "var(--in-solid)", income, row)}
        {group("archived", t("archived"), "var(--k-none)", archived, (category) => (
          <li key={category.id}>
            <div className="row is-plain mgr-row">
              <span className="r-main">
                <span className="r-title">{category.name}</span>
                <span className="r-meta">{category.kind === "income" ? terms("income") : terms("spending")}</span>
              </span>
              <span className="r-actions">
                <button
                  type="button"
                  className="ws-btn ws-btn-sm"
                  aria-label={t("restoreNamed", { name: category.name })}
                  disabled={restore.isPending}
                  onClick={() => restore.mutate(category.id)}
                >
                  {t("restore")}
                </button>
              </span>
            </div>
          </li>
        ))}
      </div>
    );
  }

  const footer = view.mode === "form" ? (
    <>
      <button type="button" className="ws-btn" onClick={back}>{common("cancel")}</button>
      <button type="submit" form={CATEGORY_FORM_ID} className="ws-btn ws-btn-ink" disabled={save.isPending}>
        {save.isPending ? t("saving") : view.category ? t("save") : t("create")}
      </button>
    </>
  ) : (
    <>
      <button type="button" className="ws-btn" data-focus-key="add-income" onClick={() => openForm(null, "income", "add-income")}>{t("addIncome")}</button>
      <button type="button" className="ws-btn ws-btn-ink" data-focus-key="add-expense" onClick={() => openForm(null, "expense", "add-expense")}>{t("add")}</button>
    </>
  );

  const title = view.mode === "form" ? (view.category ? t("edit", { name: view.category.name }) : view.kind === "income" ? t("addIncome") : t("add")) : t("title");
  return (
    <ShellDialog open={open} onClose={onClose} title={title} meta={view.mode === "list" ? t("meta") : undefined} width={640} closeLabel={common("close")} footer={footer}>
      {body}
    </ShellDialog>
  );
}
