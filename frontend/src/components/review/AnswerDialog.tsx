"use client";

import { useEffect, useId, useState, type FormEvent } from "react";
import { useTranslations } from "next-intl";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import type { SaveResult } from "./triage/types";

const DEFAULT_MAX_LENGTH = 200;

/**
 * "Jawab sendiri" (apply-markas-tinjau D11): a centered 520 px dialog with one field of the question's length limit.
 * The field is a typing target, so digits never press an answer; an empty answer is not sent.
 */
export function AnswerDialog({
  open,
  onClose,
  question,
  maxLength,
  onSave,
}: {
  open: boolean;
  onClose: () => void;
  question: string;
  maxLength?: number;
  onSave: (text: string) => Promise<SaveResult>;
}) {
  const t = useTranslations("review.dialog.answer");
  const common = useTranslations("common");
  const formId = useId();
  const [text, setText] = useState("");
  const [empty, setEmpty] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setText("");
    setEmpty(false);
    setFailure(null);
  }, [open]);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = text.trim();
    if (!trimmed) {
      setEmpty(true);
      return;
    }
    setEmpty(false);
    setFailure(null);
    setSaving(true);
    const result = await onSave(trimmed);
    setSaving(false);
    if (!result.saved) setFailure(result.error);
  };

  const errorId = `${formId}-error`;
  return (
    <ShellDialog
      open={open}
      onClose={onClose}
      title={t("title")}
      width={520}
      closeLabel={common("close")}
      meta={question}
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
      <form id={formId} noValidate onSubmit={submit} className="field">
        <label htmlFor={`${formId}-text`}>{t("field")}</label>
        <input
          id={`${formId}-text`}
          className="border field-control"
          value={text}
          maxLength={maxLength ?? DEFAULT_MAX_LENGTH}
          placeholder={t("placeholder")}
          aria-invalid={empty ? true : undefined}
          aria-describedby={empty ? errorId : undefined}
          onChange={(event) => setText(event.target.value)}
        />
        {empty && (
          <p id={errorId} className="err">
            {t("empty")}
          </p>
        )}
      </form>
    </ShellDialog>
  );
}
