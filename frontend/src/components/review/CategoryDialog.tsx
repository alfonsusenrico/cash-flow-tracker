"use client";

import { useEffect, useId, useState, type FormEvent } from "react";
import { useTranslations } from "next-intl";
import { bucketName } from "@/components/beranda/words";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { Segmented } from "@/components/workspace/primitives";
import { KAKEIBO_PILLARS } from "@/lib/kakeibo";
import { MERCHANT_MAX_LENGTH } from "@/lib/provenance";
import { isMovementCategory } from "@/lib/systemCategories";
import type { SaveResult } from "./triage/types";

export interface CategoryOption {
  id: string;
  name: string;
  kind: string;
  kakeibo_type?: string | null;
  is_archived?: boolean;
  /** Read-only system key (investment, internal_movement, balance_adjustment); null for an ordinary category. */
  system_key?: string | null;
}

/** Whether the categories could be listed; a failure shows its line with "Coba lagi", never an empty list. */
export interface CategoriesState {
  categories: CategoryOption[];
  failed: boolean;
  retry: () => void;
}

/** "category" and "spending" send `recategorise`, "name" sends `rename`; each exactly one request as before. */
export type CategoryDialogMode = "category" | "name" | "spending";

const NOTES_MAX_LENGTH = 500;
const FOLLOW_CATEGORY = "";

/**
 * "Ubah kategori", "Ganti nama" and "Catat sebagai pengeluaran" (apply-markas-tinjau D10): one centered 560 px
 * dialog, a bottom sheet on the phone. It closes only when its request succeeds; a failure stays in it as text.
 */
export function CategoryDialog({
  open,
  mode,
  onClose,
  label,
  count,
  type,
  merchant,
  remembered,
  categories,
  onSave,
}: {
  open: boolean;
  mode: CategoryDialogMode;
  onClose: () => void;
  label: string;
  count: number;
  type: "expense" | "income";
  /** The store name a merchant group starts with in "Ganti nama". */
  merchant?: string;
  /** The line under the fields in "Ubah kategori": what the choice is remembered for. */
  remembered?: string;
  categories: CategoriesState;
  onSave: (fields: Record<string, string>) => Promise<SaveResult>;
}) {
  const t = useTranslations("review.dialog.category");
  const terms = useTranslations();
  const common = useTranslations("common");
  const formId = useId();
  const [categoryId, setCategoryId] = useState("");
  const [pillar, setPillar] = useState(FOLLOW_CATEGORY);
  const [store, setStore] = useState(merchant ?? "");
  const [notes, setNotes] = useState("");
  const [invalid, setInvalid] = useState("");
  const [failure, setFailure] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setCategoryId("");
    setPillar(FOLLOW_CATEGORY);
    setStore(merchant ?? "");
    setNotes("");
    setInvalid("");
    setFailure(null);
  }, [open, merchant]);

  const naming = mode === "name";
  const choices = categories.categories.filter(
    (category) => category.kind === type && !category.is_archived && (mode !== "spending" || !isMovementCategory(category.system_key)),
  );
  const pillars = [
    { value: FOLLOW_CATEGORY, label: t("followCategory") },
    ...KAKEIBO_PILLARS.map((entry) => ({ value: entry.value, label: bucketName(terms, entry.value) })),
  ];
  const title = mode === "category" ? t("title") : mode === "name" ? t("renameTitle") : t("spendingTitle");

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const fields: Record<string, string> = {};
    if (naming) {
      if (store.trim()) fields.merchant = store.trim();
      if (notes.trim()) fields.notes = notes.trim();
      if (!Object.keys(fields).length) {
        setInvalid(t("needName"));
        return;
      }
    } else {
      if (!categoryId) {
        setInvalid(mode === "spending" ? t("needSpendingCategory") : t("needCategory"));
        return;
      }
      fields.category_id = categoryId;
      if (type === "expense" && pillar) fields.kakeibo_type = pillar;
    }
    setInvalid("");
    setFailure(null);
    setSaving(true);
    const result = await onSave(fields);
    setSaving(false);
    if (!result.saved) setFailure(result.error);
  };

  const errorId = `${formId}-error`;
  return (
    <ShellDialog
      open={open}
      onClose={onClose}
      title={title}
      width={560}
      closeLabel={common("close")}
      meta={t("meta", { label, count })}
      footer={
        <>
          <button type="button" className="ws-btn" onClick={onClose}>
            {common("cancel")}
          </button>
          <button type="submit" form={formId} className="ws-btn ws-btn-ink" disabled={saving}>
            {saving ? t("saving") : t("save")}
          </button>
          {failure && (
            <p role="alert" className="dlg-error t-meta c-expense">
              {failure}
            </p>
          )}
        </>
      }
    >
      <form id={formId} noValidate onSubmit={submit} className="form-stack">
        {naming ? (
          <>
            <div className="field">
              <label htmlFor={`${formId}-store`}>{t("store")}</label>
              <input
                id={`${formId}-store`}
                className="border field-control"
                value={store}
                maxLength={MERCHANT_MAX_LENGTH}
                aria-invalid={invalid ? true : undefined}
                aria-describedby={invalid ? errorId : undefined}
                onChange={(event) => setStore(event.target.value)}
              />
            </div>
            <div className="field">
              <label htmlFor={`${formId}-notes`}>{t("notes")}</label>
              <input
                id={`${formId}-notes`}
                className="border field-control"
                value={notes}
                maxLength={NOTES_MAX_LENGTH}
                placeholder={t("keepEmpty")}
                onChange={(event) => setNotes(event.target.value)}
              />
              {invalid && (
                <p id={errorId} className="err">
                  {invalid}
                </p>
              )}
            </div>
          </>
        ) : (
          <>
            <div className="field">
              <label htmlFor={`${formId}-category`}>{t("category")}</label>
              <select
                id={`${formId}-category`}
                className="border field-control"
                value={categoryId}
                aria-invalid={invalid ? true : undefined}
                aria-describedby={invalid || categories.failed ? errorId : undefined}
                onChange={(event) => setCategoryId(event.target.value)}
              >
                <option value="">{t("choose")}</option>
                {choices.map((category) => (
                  <option key={category.id} value={category.id}>
                    {category.name}
                  </option>
                ))}
              </select>
              {categories.failed && (
                <p id={errorId} className="err">
                  {t("loadError")}{" "}
                  <button type="button" className="ws-linkbtn" onClick={categories.retry}>
                    {common("tryAgain")}
                  </button>
                </p>
              )}
              {invalid && !categories.failed && (
                <p id={errorId} className="err">
                  {invalid}
                </p>
              )}
            </div>
            {type === "expense" && (
              <div className="field">
                <span className="flabel" aria-hidden="true">
                  {t("pillar")}
                </span>
                <Segmented label={t("pillar")} options={pillars} value={pillar} onChange={setPillar} />
              </div>
            )}
            {(mode === "spending" || remembered) && <p className="t-meta">{mode === "spending" ? t("spendingHint") : remembered}</p>}
          </>
        )}
      </form>
    </ShellDialog>
  );
}
