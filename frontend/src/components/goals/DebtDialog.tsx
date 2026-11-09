"use client";

import { useRef, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { Field } from "@/components/workspace/Field";
import { ProgressBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api, ApiError } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import type { ObligationItem } from "./types";

type DebtField = "name" | "total" | "remaining" | "due";

class FieldError extends Error {
  constructor(public field: DebtField, message: string) {
    super(message);
  }
}

/** The field a refused save points at: the form's own checks name it, a server error by its code or message. */
function fieldOf(failure: Error): DebtField {
  if (failure instanceof FieldError) return failure.field;
  const code = failure instanceof ApiError && typeof failure.detail !== "string" ? failure.detail.code ?? "" : "";
  const text = `${code} ${failure.message}`.toLowerCase();
  if (text.includes("remaining") || text.includes("sisa")) return "remaining";
  if (text.includes("total")) return "total";
  if (text.includes("due") || text.includes("date") || text.includes("tempo")) return "due";
  return "name";
}

/**
 * "Ubah utang" and "Tambah utang" (apply-markas-target D9, D19): a 560 px dialog with what is left and the paid bar,
 * the lifecycle note of a paid-off debt, today's debt form and validation (focus on the failing field), "Kaitkan
 * atau catat pembayaran" for an active debt, and "Arsipkan" confirmed in the footer. Requests are today's.
 */
export function DebtDialog({ debt, onClose, onPay }: { debt: ObligationItem | null; onClose: () => void; onPay: (debt: ObligationItem) => void }) {
  const t = useTranslations("goals");
  const tc = useTranslations("common");
  const format = useFormat();
  const qc = useQueryClient();
  const [name, setName] = useState(debt?.name ?? "");
  const [total, setTotal] = useState(debt ? formatNumberWithDots(debt.total_amount) : "");
  const [remaining, setRemaining] = useState(debt ? formatNumberWithDots(debt.remaining_amount) : "");
  const [due, setDue] = useState(debt?.due_date ?? "");
  const [minimum, setMinimum] = useState(debt?.minimum_payment ? formatNumberWithDots(debt.minimum_payment) : "");
  const [notes, setNotes] = useState(debt?.notes ?? "");
  const [error, setError] = useState("");
  const [confirmArchive, setConfirmArchive] = useState(false);
  const archiveButton = useRef<HTMLButtonElement>(null);
  const refs = {
    name: useRef<HTMLInputElement>(null),
    total: useRef<HTMLInputElement>(null),
    remaining: useRef<HTMLInputElement>(null),
    due: useRef<HTMLInputElement>(null),
  };

  const totalValue = parseNumberFromDots(total);
  const remainingValue = remaining.trim() ? parseNumberFromDots(remaining) : totalValue;
  const exceeds = Boolean(remaining.trim()) && totalValue > 0 && remainingValue > totalValue;
  const paidOff = debt !== null && (debt.is_archived || debt.remaining_amount <= 0);

  const save = useMutation({
    mutationFn: async () => {
      const trimmed = name.trim();
      if (!trimmed) throw new FieldError("name", t("debtForm.nameRequired"));
      if (totalValue <= 0) throw new FieldError("total", t("debtForm.totalPositive"));
      const left = remaining ? parseNumberFromDots(remaining) : totalValue;
      const minPay = minimum ? parseNumberFromDots(minimum) : null;
      if (left < 0) throw new FieldError("remaining", t("debtForm.remainingNegative"));
      if (left > totalValue) throw new FieldError("remaining", t("debtForm.remainingOverTotal"));
      const body = { name: trimmed, total_amount: totalValue, remaining_amount: left, minimum_payment: minPay, notes: notes.trim() || null };
      return debt
        ? api.patch(`/obligations/${debt.id}`, { ...body, due_date: due || "" })
        : api.post("/obligations", { ...body, due_date: due || null });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (failure: Error) => {
      setError(failure.message || t("debtForm.failed"));
      refs[fieldOf(failure)].current?.focus();
    },
  });
  const archive = useMutation({
    mutationFn: (id: string) => api.del(`/obligations/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    },
  });
  const submit = (event: FormEvent) => {
    event.preventDefault();
    setError("");
    save.mutate();
  };

  const footer = confirmArchive && debt ? (
    <>
      <p className="grow-text t-body" role="alert">
        {t("debtForm.archiveConfirm", { name: debt.name })}
      </p>
      <button
        type="button"
        className="ws-btn"
        autoFocus
        onClick={() => {
          setConfirmArchive(false);
          requestAnimationFrame(() => archiveButton.current?.focus());
        }}
      >
        {t("dialog.cancel")}
      </button>
      <button
        type="button"
        className="ws-btn ws-btn-danger"
        onClick={() => {
          archive.mutate(debt.id);
          onClose();
        }}
      >
        {t("dialog.archiveYes")}
      </button>
    </>
  ) : (
    <>
      {debt && (
        <button ref={archiveButton} type="button" className="ws-btn ws-btn-quiet c-expense" onClick={() => setConfirmArchive(true)}>
          {t("dialog.archive")}
        </button>
      )}
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>
        {t("dialog.cancel")}
      </button>
      <button type="submit" form="debt-form" className="ws-btn ws-btn-ink" disabled={save.isPending}>
        {save.isPending ? t("dialog.saving") : t("dialog.save")}
      </button>
    </>
  );
  const amountPlaceholder = (value: number) => t("debtForm.amountPlaceholder", { amount: format.integer(value) });
  const dueLine = (() => {
    if (!debt || paidOff) return null;
    if (debt.due_date && debt.minimum_payment != null) return t.rich("debtForm.dueLine", { date: format.yearDate(debt.due_date), amount: () => <MaskedMoney text={format.money(debt.minimum_payment!)} /> });
    if (debt.due_date) return t("debtForm.dueOnly", { date: format.yearDate(debt.due_date) });
    if (debt.minimum_payment != null) return t.rich("debtForm.minimumOnly", { amount: () => <MaskedMoney text={format.money(debt.minimum_payment!)} /> });
    return null;
  })();

  return (
    <ShellDialog
      open
      onClose={onClose}
      title={debt ? t("debtForm.editTitle") : t("debtForm.newTitle")}
      meta={debt?.name}
      width={560}
      closeLabel={tc("close")}
      className="tg-dialog"
      footer={footer}
    >
      {debt && (
        <div className="dlg-fig">
          <span className="t-label">{paidOff ? t("debtForm.paidOff") : t("debtForm.left")}</span>
          <p>
            <MaskedMoney text={format.money(debt.remaining_amount)} className="t-fig-l" />{" "}
            <span className="t-meta">
              {t.rich("debtForm.ofPaid", { total: () => <MaskedMoney text={format.money(debt.total_amount)} />, pct: String(Math.max(0, Math.round(debt.payoff_percentage))) })}
            </span>
          </p>
          <ProgressBar value={debt.payoff_percentage} max={100} thick color="var(--text)" />
          {paidOff ? <p className="t-meta">{t("debtForm.lifecycle")}</p> : dueLine && <p className="t-meta">{dueLine}</p>}
        </div>
      )}
      <form id="debt-form" className="form-stack" noValidate onSubmit={submit}>
        {error && (
          <p role="alert" className="form-alert">
            {error}
          </p>
        )}
        {!debt && <p className="t-body muted">{t("debtForm.intro")}</p>}
        <div className="form-grid">
          <Field id="obligation-name" label={t("debtForm.name")} className="full-row">
            {(aria) => (
              <input {...aria} ref={refs.name} name="name" type="text" value={name} onChange={(event) => setName(event.target.value)}
                placeholder={t("debtForm.namePlaceholder")} className="field-control" />
            )}
          </Field>
          <Field id="obligation-total" label={t("debtForm.total")}>
            {(aria) => (
              <input {...aria} ref={refs.total} name="total_amount" type="text" inputMode="numeric" value={total}
                onChange={(event) => setTotal(formatNumberWithDots(event.target.value))} placeholder={amountPlaceholder(15_000_000)}
                className="field-control tabular-nums" />
            )}
          </Field>
          <Field id="obligation-remaining" label={t("debtForm.remaining")} hint={t("debtForm.remainingHint")} error={exceeds ? t("debtForm.remainingOverTotal") : undefined}>
            {(aria) => (
              <input {...aria} ref={refs.remaining} name="remaining_amount" type="text" inputMode="numeric" value={remaining}
                onChange={(event) => setRemaining(formatNumberWithDots(event.target.value))} className="field-control tabular-nums" />
            )}
          </Field>
          {totalValue > 0 && !exceeds && (
            <p role="status" className="t-meta full-row">
              {t.rich("debtForm.paidPreview", { paid: () => <MaskedMoney text={format.money(Math.max(0, totalValue - remainingValue))} />, left: () => <MaskedMoney text={format.money(remainingValue)} /> })}
            </p>
          )}
          <Field id="obligation-minimum-payment" label={t("debtForm.minimum")}>
            {(aria) => (
              <input {...aria} name="minimum_payment" type="text" inputMode="numeric" value={minimum}
                onChange={(event) => setMinimum(formatNumberWithDots(event.target.value))} placeholder={amountPlaceholder(1_000_000)}
                className="field-control tabular-nums" />
            )}
          </Field>
          <Field id="obligation-due-date" label={t("debtForm.due")}>
            {(aria) => <input {...aria} ref={refs.due} name="due_date" type="date" value={due} onChange={(event) => setDue(event.target.value)} className="field-control" />}
          </Field>
          <Field id="obligation-notes" label={t("debtForm.notes")} className="full-row">
            {(aria) => <textarea {...aria} name="notes" rows={2} value={notes} onChange={(event) => setNotes(event.target.value)} className="field-control tg-notes" />}
          </Field>
        </div>
        {debt && !paidOff && (
          <button type="button" className="ws-btn" aria-haspopup="dialog" onClick={() => onPay(debt)}>
            {t("debtForm.payEntry")}
          </button>
        )}
      </form>
    </ShellDialog>
  );
}
