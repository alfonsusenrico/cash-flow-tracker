"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { isNonReportableCategory } from "@/lib/systemCategories";

interface CategoryRow {
  id: string;
  name: string;
  kind: string;
  monthly_budget: number | null;
  is_archived: boolean;
  is_excluded_from_budget: boolean;
  system_key: string | null;
}

const FORM_ID = "budget-dialog-form";
const digits = (value: string) => value.replace(/[^0-9]/g, "");

/**
 * "Atur anggaran" (apply-markas-analisis D9): one monthly amount per expense category that reports count, saved
 * with one `PATCH /categories/{id}` per changed category, as the former Laporan editor did. Translated, so it is
 * announced in the interface language on a page not translated yet too.
 */
export function BudgetDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const t = useTranslations("analysis.budget");
  const common = useTranslations("common");
  const format = useFormat();
  const queryClient = useQueryClient();
  const { data, isLoading, isError } = useQuery<{ categories: CategoryRow[] }>({
    queryKey: [...queryKeys.categories, "expense"],
    queryFn: () => api.get("/categories?kind=expense"),
    enabled: open,
  });
  // Reports leave these categories out of spending, so a budget on them would never be compared.
  const categories = useMemo(
    () => (data?.categories ?? []).filter(
      (row) => row.kind === "expense" && !row.is_archived && !row.is_excluded_from_budget && !isNonReportableCategory(row.system_key),
    ),
    [data],
  );
  const grouped = (value: number | null) => (value && value > 0 ? format.groupDigits(value) : "");
  const [values, setValues] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  useEffect(() => {
    if (open) setValues(Object.fromEntries(categories.map((row) => [row.id, row.monthly_budget && row.monthly_budget > 0 ? format.groupDigits(row.monthly_budget) : ""])));
  }, [open, categories, format]);

  const save = useMutation({
    mutationFn: async () => {
      const changed = categories.filter((row) => (Number(digits(values[row.id] ?? "")) || null) !== (row.monthly_budget || null));
      for (const row of changed) {
        const amount = Number(digits(values[row.id] ?? "")) || null;
        await api.patch(`/categories/${row.id}`, { monthly_budget: amount });
      }
      return changed.length;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.categories });
      queryClient.invalidateQueries({ queryKey: queryKeys.reports.all });
      queryClient.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (failure: Error) => setError(failure.message || t("saveFailed")),
  });

  const footer = (
    <>
      <button type="button" className="ws-btn" onClick={onClose}>{common("cancel")}</button>
      <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink" disabled={save.isPending || isLoading}>
        {save.isPending ? t("saving") : t("save")}
      </button>
    </>
  );

  return (
    <UntranslatedRegion active={false}>
      <ShellDialog open={open} onClose={onClose} title={t("title")} meta={t("help")} width={560} closeLabel={common("close")} footer={footer}>
        <form
          id={FORM_ID}
          className="form-stack"
          onSubmit={(event) => {
            event.preventDefault();
            setError("");
            save.mutate();
          }}
        >
          {error && <p role="alert" className="form-alert">{error}</p>}
          {isLoading && <p className="t-meta" role="status">{t("loading")}</p>}
          {isError && <p role="alert" className="form-alert">{t("loadFailed")}</p>}
          <ul className="budget-list">
            {categories.map((row) => (
              <li key={row.id} className="budget-row">
                <label htmlFor={`budget-${row.id}`} className="budget-name">{row.name}</label>
                <span className="budget-input">
                  {/* Budgets are stored in rupiah whatever the display currency. */}
                  <span aria-hidden="true" className="budget-unit">{t("unit")}</span>
                  <input
                    id={`budget-${row.id}`}
                    inputMode="numeric"
                    autoComplete="off"
                    value={values[row.id] ?? ""}
                    onChange={(event) => setValues((current) => ({ ...current, [row.id]: grouped(Number(digits(event.target.value)) || null) }))}
                    placeholder={t("none")}
                    className="field-control tabular-nums"
                  />
                </span>
              </li>
            ))}
          </ul>
        </form>
      </ShellDialog>
    </UntranslatedRegion>
  );
}
