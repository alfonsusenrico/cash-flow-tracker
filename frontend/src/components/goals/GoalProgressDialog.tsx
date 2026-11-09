"use client";

import { useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Field } from "@/components/workspace/Field";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import type { GoalItem } from "./types";

/**
 * "Tambah progres" for a standalone goal (apply-markas-target D18): a 480 px dialog that records progress only, with
 * today's request `POST /goals/{id}/adjust`. No transaction is made and no account changes.
 */
export function GoalProgressDialog({ goal, onClose }: { goal: GoalItem; onClose: () => void }) {
  const t = useTranslations("goals.progress");
  const tc = useTranslations("common");
  const format = useFormat();
  const { currency } = useAppCtx();
  const qc = useQueryClient();
  const [amount, setAmount] = useState("");
  const [error, setError] = useState("");
  const value = parseNumberFromDots(amount);

  const adjust = useMutation({
    mutationFn: async () => {
      if (value <= 0) throw new Error(t("invalid"));
      if (goal.linked_accounts?.length) throw new Error(t("linked"));
      return api.post(`/goals/${goal.id}/adjust`, { amount: value });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (failure: Error) => setError(failure.message || t("failed")),
  });
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!adjust.isPending) adjust.mutate();
  };

  return (
    <ShellDialog
      open
      onClose={onClose}
      title={t("title")}
      meta={goal.name}
      width={480}
      closeLabel={tc("close")}
      footer={
        <>
          <button type="button" className="ws-btn" onClick={onClose}>
            {tc("cancel")}
          </button>
          <button type="submit" form="goal-progress-form" className="ws-btn ws-btn-ink" disabled={adjust.isPending}>
            {adjust.isPending ? tc("loading") : t("save")}
          </button>
        </>
      }
    >
      <form id="goal-progress-form" className="form-stack" noValidate onSubmit={submit}>
        {error && (
          <p role="alert" className="form-alert">
            {error}
          </p>
        )}
        <p className="t-body muted">{t("note")}</p>
        <Field
          id="goal-progress-adjustment"
          label={t("amount")}
          hint={currency === "USD" && value > 0 ? <MaskedMoney text={format.money(value)} /> : undefined}
        >
          {(aria) => (
            <input {...aria} name="amount" type="text" inputMode="numeric" value={amount}
              onChange={(event) => setAmount(formatNumberWithDots(event.target.value))}
              placeholder={t("placeholder", { amount: format.integer(500_000) })} className="field-control tg-amount-input tabular-nums" />
          )}
        </Field>
      </form>
    </ShellDialog>
  );
}
