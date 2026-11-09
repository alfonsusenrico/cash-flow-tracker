"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { RadioSegments } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { KAKEIBO_BUCKETS, type KakeiboBucket } from "@/lib/kakeibo";
import { UNCATEGORIZED, type LedgerFilters, type LedgerPeriod, type LedgerReviewFilter, type LedgerSourceFilter } from "@/lib/ledgerFilters";
import { bucketName, useAccountOptionLabels } from "./rows";

const FORM_ID = "ledger-filter-form";

/** The fields the dialog edits; type, search and the date range stay where they are set. */
export type FilterDraft = Pick<LedgerFilters, "account" | "category" | "kakeibo" | "review" | "source" | "waiting" | "dup" | "period">;

function draftOf(filters: LedgerFilters): FilterDraft {
  const { account, category, kakeibo, review, source, waiting, dup, period } = filters;
  return { account, category, kakeibo, review, source, waiting, dup, period };
}

/**
 * "Filter transaksi" (apply-markas-transaksi D8): a draft of every filter, applied with one URL write by
 * "Terapkan"; "Batal", Esc and the close button discard it. "Atur ulang" clears all but the period.
 */
export function LedgerFilterDialog({
  open,
  filters,
  accounts,
  categories,
  onApply,
  onClose,
}: {
  open: boolean;
  filters: LedgerFilters;
  accounts: any[];
  categories: { id: string; name: string; kind: string }[];
  onApply: (draft: FilterDraft) => void;
  onClose: () => void;
}) {
  const t = useTranslations("transactions.filter");
  const terms = useTranslations();
  const accountLabels = useAccountOptionLabels();
  const [draft, setDraft] = useState<FilterDraft>(() => draftOf(filters));
  useEffect(() => {
    if (open) setDraft(draftOf(filters));
  }, [open, filters]);
  const set = (patch: Partial<FilterDraft>) => setDraft((current) => ({ ...current, ...patch }));

  const footer = (
    <>
      <button
        type="button"
        className="ws-btn ws-btn-quiet"
        onClick={() => set({ account: "all", category: "all", kakeibo: "all", review: "all", source: "all", waiting: false, dup: false })}
      >
        {t("reset")}
      </button>
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>{terms("common.cancel")}</button>
      <button type="submit" form={FORM_ID} className="ws-btn ws-btn-ink">{t("apply")}</button>
    </>
  );

  return (
    <UntranslatedRegion active={false}>
      <ShellDialog open={open} onClose={onClose} title={t("title")} width={640} closeLabel={t("close")} footer={footer}>
        <form id={FORM_ID} className="form-stack" onSubmit={(event) => { event.preventDefault(); onApply(draft); }}>
          <div className="field">
            <label htmlFor="ledger-filter-account">{t("account")}</label>
            <select id="ledger-filter-account" className="field-control" value={draft.account} onChange={(event) => set({ account: event.target.value })}>
              <option value="all">{t("allAccounts")}</option>
              <AccountSelectOptions labels={accountLabels} accounts={accounts} allowParentSelection={true} includeCredit />
            </select>
          </div>
          <div className="field">
            <label htmlFor="ledger-filter-category">{t("category")}</label>
            <select id="ledger-filter-category" className="field-control" value={draft.category} onChange={(event) => set({ category: event.target.value })}>
              <option value="all">{t("allCategories")}</option>
              <option value={UNCATEGORIZED}>{terms("terms.noCategory")}</option>
              <optgroup label={terms("terms.spending")}>
                {categories.filter((category) => category.kind !== "income").map((category) => (
                  <option key={category.id} value={category.id}>{category.name}</option>
                ))}
              </optgroup>
              <optgroup label={terms("terms.income")}>
                {categories.filter((category) => category.kind === "income").map((category) => (
                  <option key={category.id} value={category.id}>{category.name}</option>
                ))}
              </optgroup>
            </select>
          </div>
          <div className="field">
            <label htmlFor="ledger-filter-pillar">{t("pillar")}</label>
            <select id="ledger-filter-pillar" className="field-control" value={draft.kakeibo} onChange={(event) => set({ kakeibo: event.target.value as KakeiboBucket | "all" })}>
              <option value="all">{t("allPillars")}</option>
              {KAKEIBO_BUCKETS.map((bucket) => <option key={bucket.value} value={bucket.value}>{bucketName(terms, bucket.value)}</option>)}
            </select>
          </div>
          <div className="field">
            <span className="flabel" id="ledger-filter-status">{t("status")}</span>
            <RadioSegments<LedgerReviewFilter>
              labelledBy="ledger-filter-status"
              options={[
                { value: "all", label: t("statusAll") },
                { value: "needs_review", label: t("needsReview") },
                { value: "auto", label: t("auto") },
                { value: "reviewed", label: t("reviewed") },
              ]}
              value={draft.review}
              onChange={(review) => set({ review })}
            />
          </div>
          <div className="field">
            <span className="flabel" id="ledger-filter-source">{t("source")}</span>
            <RadioSegments<LedgerSourceFilter>
              labelledBy="ledger-filter-source"
              options={[
                { value: "all", label: t("sourceAll") },
                { value: "notification", label: terms("transactions.chips.phone") },
                { value: "capture", label: terms("transactions.chips.typed") },
              ]}
              value={draft.source}
              onChange={(source) => set({ source })}
            />
          </div>
          <label className="field-check">
            <input type="checkbox" checked={draft.waiting} onChange={(event) => set({ waiting: event.target.checked })} />
            {t("waitingOnly")}
          </label>
          {(filters.dup || draft.dup) && (
            <label className="field-check">
              <input type="checkbox" checked={draft.dup} onChange={(event) => set({ dup: event.target.checked })} />
              {t("duplicates")}
            </label>
          )}
          <div className="field">
            <span className="flabel" id="ledger-filter-period">{t("period")}</span>
            <RadioSegments<LedgerPeriod>
              labelledBy="ledger-filter-period"
              options={[
                { value: "cycle", label: terms("terms.payCycle") },
                { value: "month", label: terms("terms.calendarMonth") },
                { value: "all", label: terms("transactions.periods.all") },
              ]}
              value={draft.period}
              onChange={(period) => set({ period })}
            />
          </div>
        </form>
      </ShellDialog>
    </UntranslatedRegion>
  );
}
