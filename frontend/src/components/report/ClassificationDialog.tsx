"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { PILLAR_TERM } from "@/components/analysis/KakeiboRows";
import { jakartaDay } from "@/components/transactions/rows";
import { useMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { KAKEIBO_PILLARS, type KakeiboPillar } from "@/lib/kakeibo";
import { queryKeys } from "@/lib/queryKeys";

interface Gaps {
  categories: { id: string; name: string; unclassified_count: number; unclassified_amount: number }[];
  transactions: {
    id: string;
    date: string;
    amount: number;
    merchant: string | null;
    notes: string | null;
    category_name: string | null;
    account_name: string;
  }[];
  transaction_count: number;
  transaction_limit: number;
}

function PillarSelect({ id, label, value, onChange }: { id: string; label: string; value: KakeiboPillar | ""; onChange: (value: KakeiboPillar | "") => void }) {
  const t = useTranslations();
  return (
    <>
      <label htmlFor={id} className="sr-only">{label}</label>
      <select id={id} value={value} onChange={(event) => onChange(event.target.value as KakeiboPillar | "")} className="field-control is-select ws-select lp-select">
        <option value="">{t("report.classify.choosePillar")}</option>
        {KAKEIBO_PILLARS.map((pillar) => (
          <option key={pillar.value} value={pillar.value}>{t(`terms.${PILLAR_TERM[pillar.value]}`)}</option>
        ))}
      </select>
    </>
  );
}

/**
 * "Lengkapi klasifikasi" (apply-markas-laporan D15): the shell dialog at 560 px, a sheet below 768 px. A pillar per
 * category without one, or one pillar for several unclassified rows at once, with today's requests and statuses.
 */
export function ClassificationDialog({ open, onClose, scope }: { open: boolean; onClose: () => void; scope: { start: string; end: string } | null }) {
  const t = useTranslations("report.classify");
  const common = useTranslations("common");
  const terms = useTranslations("terms");
  const money = useMoney();
  const format = useFormat();
  const queryClient = useQueryClient();
  const params = scope ? `?from_date=${encodeURIComponent(scope.start)}&to_date=${encodeURIComponent(scope.end)}` : "";
  const { data, isLoading, isError } = useQuery<Gaps>({
    queryKey: queryKeys.classificationGaps(scope?.start ?? null, scope?.end ?? null),
    queryFn: () => api.get(`/classification/gaps${params}`),
    enabled: open,
  });
  const [categoryPillars, setCategoryPillars] = useState<Record<string, KakeiboPillar | "">>({});
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [rowPillar, setRowPillar] = useState<KakeiboPillar | "">("");
  const [status, setStatus] = useState("");
  useEffect(() => {
    if (!open) return;
    setCategoryPillars({});
    setSelected(new Set());
    setRowPillar("");
    setStatus("");
  }, [open]);

  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["classification"] }),
      queryClient.invalidateQueries({ queryKey: queryKeys.reports.all }),
      queryClient.invalidateQueries({ queryKey: queryKeys.dashboard.all }),
      queryClient.invalidateQueries({ queryKey: queryKeys.transactions.all }),
      queryClient.invalidateQueries({ queryKey: queryKeys.categories }),
    ]);
  };
  const classifyCategory = useMutation({
    mutationFn: ({ id, pillar }: { id: string; name: string; pillar: KakeiboPillar }) =>
      api.post<{ corrections: number }>(`/classification/categories/${id}`, { kakeibo_type: pillar }),
    onSuccess: async (result, variables) => {
      setStatus(t("savedCategory", { name: variables.name, count: result.corrections }));
      await refresh();
    },
    onError: (error: Error) => setStatus(error.message || t("saveFailed")),
  });
  const classifyRows = useMutation({
    mutationFn: ({ ids, pillar }: { ids: string[]; pillar: KakeiboPillar }) =>
      api.post<{ transaction_count: number }>("/classification/transactions", { transaction_ids: ids, kakeibo_type: pillar }),
    onSuccess: async (result) => {
      setStatus(t("appliedRows", { count: result.transaction_count }));
      setSelected(new Set());
      await refresh();
    },
    onError: (error: Error) => setStatus(error.message || t("saveFailed")),
  });

  const rows = data?.transactions ?? [];
  const toggle = (id: string) =>
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const allSelected = rows.length > 0 && rows.every((row) => selected.has(row.id));

  return (
    <ShellDialog
      open={open}
      onClose={onClose}
      title={t("title")}
      width={560}
      closeLabel={common("close")}
      footer={<button type="button" className="ws-btn" onClick={onClose}>{t("done")}</button>}
    >
      <div className="form-stack lp-classify">
        <p className="t-body muted">{t("intro")}</p>
        <p role="status" className="t-strong lp-classify-status">{status}</p>
        {isLoading && <p className="t-meta">{common("loading")}</p>}
        {isError && <p role="alert" className="form-alert">{t("loadFailed")}</p>}
        {data && data.categories.length === 0 && data.transaction_count === 0 && <p className="t-body">{t("allDone")}</p>}

        {data && data.categories.length > 0 && (
          <section aria-labelledby="classify-categories">
            <h3 id="classify-categories" className="t-label">{t("categories")}</h3>
            <p className="t-meta">{t("categoriesHelp")}</p>
            <ul className="list lp-classify-list">
              {data.categories.map((category) => {
                const value = categoryPillars[category.id] ?? "";
                return (
                  <li key={category.id} className="row is-plain lp-classify-row">
                    <span className="r-main">
                      <span className="r-title">{category.name}</span>
                      <span className="r-meta">{t("categoryMeta", { count: category.unclassified_count, amount: money(category.unclassified_amount) })}</span>
                    </span>
                    <span className="lp-classify-actions">
                      <PillarSelect
                        id={`classify-category-${category.id}`}
                        label={t("pillarFor", { name: category.name })}
                        value={value}
                        onChange={(next) => setCategoryPillars((current) => ({ ...current, [category.id]: next }))}
                      />
                      <button
                        type="button"
                        className="ws-btn"
                        disabled={!value || classifyCategory.isPending}
                        aria-label={t("saveFor", { name: category.name })}
                        onClick={() => value && classifyCategory.mutate({ id: category.id, name: category.name, pillar: value })}
                      >
                        {t("save")}
                      </button>
                    </span>
                  </li>
                );
              })}
            </ul>
          </section>
        )}

        {rows.length > 0 && (
          <section aria-labelledby="classify-rows">
            <h3 id="classify-rows" className="t-label">{t("rows", { count: data?.transaction_count ?? rows.length })}</h3>
            {data && data.transaction_count > rows.length && <p className="t-meta">{t("showingLatest", { count: rows.length })}</p>}
            <label className="field-check">
              <input type="checkbox" checked={allSelected} onChange={() => setSelected(allSelected ? new Set() : new Set(rows.map((row) => row.id)))} />
              {t("selectAll")}
            </label>
            <ul className="list lp-classify-rows">
              {rows.map((row) => (
                <li key={row.id}>
                  <label className="row is-selectable lp-classify-pick">
                    <input type="checkbox" className="r-check" checked={selected.has(row.id)} onChange={() => toggle(row.id)} />
                    <span className="r-main">
                      <span className="r-title">{row.merchant || row.notes || terms("noDescription")}</span>
                      <span className="r-meta">
                        {t("rowMeta", { date: format.shortDate(jakartaDay(row.date)), category: row.category_name ?? terms("noCategory"), account: row.account_name })}
                      </span>
                    </span>
                    <span className="r-amt t-amt">{money(row.amount)}</span>
                  </label>
                </li>
              ))}
            </ul>
            <div className="lp-classify-apply">
              <PillarSelect id="classify-rows-pillar" label={t("rowsPillar")} value={rowPillar} onChange={setRowPillar} />
              <button
                type="button"
                className="ws-btn ws-btn-ink"
                disabled={!rowPillar || selected.size === 0 || classifyRows.isPending}
                onClick={() => rowPillar && classifyRows.mutate({ ids: [...selected], pillar: rowPillar })}
              >
                {t("apply", { count: selected.size })}
              </button>
            </div>
          </section>
        )}
      </div>
    </ShellDialog>
  );
}
