"use client";

import { useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import type { AccountItem } from "@/components/accounts/types";
import { useAccountOptionLabels } from "@/components/transactions/rows";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { Field } from "@/components/workspace/Field";
import { ListRow } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import type { ObligationItem } from "./types";

/** The note a manual payment is stored with; it is data, so it stays as the server has always received it. */
const STORED_PAYMENT_NOTE = "Bayar tagihan: ";

interface Candidate {
  id: string;
  amount: number;
  date: string;
  notes: string | null;
  merchant: string | null;
  account_name: string;
  exact_amount: boolean;
}

/**
 * "Pembayaran: {name}" (apply-markas-target D19): first the recorded spending that may be this debt's payment, each
 * linked only when the owner chooses it, then "Tidak tertangkap ponsel? Catat manual" with today's manual record.
 * Requests are today's: `GET …/payment-candidates`, `POST …/payments`, `POST /transactions` with `obligation_id`.
 */
export function DebtPaymentDialog({ debt, accounts, onClose }: { debt: ObligationItem; accounts: AccountItem[]; onClose: () => void }) {
  const t = useTranslations("goals.pay");
  const tc = useTranslations("common");
  const format = useFormat();
  const masked = useMoney();
  const accountLabels = useAccountOptionLabels();
  const qc = useQueryClient();
  const [account, setAccount] = useState("");
  const [amount, setAmount] = useState(debt.minimum_payment ? formatNumberWithDots(debt.minimum_payment) : "");
  const [error, setError] = useState("");

  const candidatesQuery = useQuery<{ match_state: "none" | "single" | "ambiguous"; window_days: number; candidates: Candidate[] }>({
    queryKey: ["obligations", "payment-candidates", debt.id],
    queryFn: () => api.get(`/obligations/${debt.id}/payment-candidates`),
  });
  const done = () => {
    qc.invalidateQueries({ queryKey: queryKeys.obligations });
    qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    onClose();
  };
  const link = useMutation({
    mutationFn: (transactionId: string) => api.post(`/obligations/${debt.id}/payments`, { transaction_id: transactionId }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      done();
    },
    onError: (failure: Error) => setError(failure.message || t("linkFailed")),
  });
  const record = useMutation({
    mutationFn: async () => {
      const value = parseNumberFromDots(amount);
      if (value <= 0) throw new Error(t("invalid"));
      const source = account || accounts[0]?.id;
      if (!source) throw new Error(t("chooseAccount"));
      return api.post("/transactions", {
        account_id: source,
        type: "expense",
        amount: value,
        obligation_id: debt.id,
        notes: `${STORED_PAYMENT_NOTE}${debt.name}`,
        date: new Date().toISOString(),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      done();
    },
    onError: (failure: Error) => setError(failure.message || t("failed")),
  });
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!record.isPending) record.mutate();
  };

  const data = candidatesQuery.data;
  const candidateText = (candidate: Candidate) =>
    `${format.shortDateAt(candidate.date)} · ${masked(candidate.amount)} · ${candidate.merchant || candidate.notes || candidate.account_name}`;

  return (
    <ShellDialog
      open
      onClose={onClose}
      title={t("title", { name: debt.name })}
      width={560}
      closeLabel={tc("close")}
      className="tg-dialog"
      footer={
        <>
          <button type="button" className="ws-btn" onClick={onClose}>
            {tc("cancel")}
          </button>
          <button type="submit" form="debt-payment-form" className="ws-btn ws-btn-ink" disabled={record.isPending}>
            {record.isPending ? t("recording") : t("record")}
          </button>
        </>
      }
    >
      {error && (
        <p role="alert" className="form-alert">
          {error}
        </p>
      )}
      <section aria-labelledby="debt-payment-candidates" className="tg-pay-part">
        <h3 id="debt-payment-candidates" className="t-label">
          {t("linkTitle")}
        </h3>
        <p className="t-meta">{t("linkRule", { days: data?.window_days ?? 45 })}</p>
        {candidatesQuery.isLoading ? (
          <p role="status" className="t-body muted">
            {t("loading")}
          </p>
        ) : !data?.candidates.length ? (
          <p className="t-body muted">{t("none")}</p>
        ) : (
          <>
            {data.match_state === "ambiguous" && <p className="t-body c-warning">{t("ambiguous")}</p>}
            <ul className="list">
              {data.candidates.map((candidate) => (
                <li key={candidate.id}>
                  <ListRow
                    time={format.shortDateAt(candidate.date)}
                    title={candidate.merchant || candidate.notes || candidate.account_name}
                    meta={candidate.exact_amount ? `${candidate.account_name} · ${t("exact")}` : candidate.account_name}
                    amount={<MaskedMoney text={format.money(candidate.amount)} className="t-amt" />}
                    actions={
                      <button
                        type="button"
                        className="ws-btn ws-btn-sm"
                        disabled={link.isPending}
                        aria-label={t("linkLabel", { candidate: candidateText(candidate) })}
                        onClick={() => link.mutate(candidate.id)}
                      >
                        {t("link")}
                      </button>
                    }
                  />
                </li>
              ))}
            </ul>
          </>
        )}
      </section>
      <form id="debt-payment-form" className="form-stack tg-pay-part" noValidate onSubmit={submit} aria-labelledby="debt-payment-manual">
        <h3 id="debt-payment-manual" className="t-label">
          {t("manualTitle")}
        </h3>
        <div className="form-grid">
          <Field id="obligation-payment-account" label={t("from")}>
            {(aria) => (
              <select {...aria} name="account_id" value={account} onChange={(event) => setAccount(event.target.value)} className="field-control">
                <AccountSelectOptions accounts={accounts} formatBalance={masked} allowParentSelection labels={accountLabels} />
              </select>
            )}
          </Field>
          <Field id="obligation-payment-amount" label={t("amount")}>
            {(aria) => (
              <input {...aria} name="amount" type="text" inputMode="numeric" value={amount}
                onChange={(event) => setAmount(formatNumberWithDots(event.target.value))} className="field-control tabular-nums" />
            )}
          </Field>
        </div>
      </form>
    </ShellDialog>
  );
}
