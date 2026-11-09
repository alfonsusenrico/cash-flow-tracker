"use client";

import Link from "next/link";
import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { ListRow } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat, type Format } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { parseNumberFromDots } from "@/lib/utils";
import { MonetaryInput } from "@/components/ui/FormField";
import type { AccountItem } from "./types";

type CauseKind = "duplicate" | "unpaired" | "held" | "skipped" | "unrecorded" | "pending";

export interface BalanceCause {
  key: string;
  kind: CauseKind;
  effect: number | null;
  amount: number | null;
  direction: "income" | "expense" | null;
  at: string;
  label: string | null;
  account: string | null;
  app: string | null;
  link: { kind: "transaction"; id: string } | { kind: "review"; folder: string | null };
  twin: { transaction_id: string; account: string | null; app: string | null; at: string } | null;
}

export interface BalanceCheckResult {
  account: { id: string; name: string; is_parent: boolean };
  actual_balance: number;
  recorded_balance: number;
  difference: number;
  since: { at: string; basis: "matched" | "adjusted" | "window" };
  candidates: BalanceCause[];
  unattributed: BalanceCause[];
  explanation: string[] | null;
  last_report_at: string | null;
  capture_gap: boolean;
  can_adjust: boolean;
}

/** Liquid accounts and pockets, and parents whose pockets are all liquid (manual-balance-check D2). */
export function canCheckBalance(account: AccountItem): boolean {
  // Credit is left out too: its balance is what is owed, not money in a bank app (add-credit-accounts D8).
  const liquid = (item: AccountItem) => (
    item.type !== "investment"
    && item.type !== "credit"
    && !item.instrument_type
    && item.investment_tracking_mode !== "amount"
  );
  return liquid(account) && (account.children ?? []).every(liquid);
}

type CheckWords = ReturnType<typeof useTranslations<"accounts.check">>;

function basisText(since: BalanceCheckResult["since"], t: CheckWords, format: Format): string {
  const day = format.shortDateAt(since.at);
  if (since.basis === "matched") return t("sinceMatched", { day });
  if (since.basis === "adjusted") return t("sinceAdjusted", { day });
  return t("sinceWindow");
}

function when(value: string, format: Format): string {
  return `${format.shortDateAt(value)} ${format.time(value)}`;
}

function causeDetail(cause: BalanceCause, t: CheckWords, format: Format): string {
  const parts = [cause.label, cause.account, cause.app, when(cause.at, format)].filter(Boolean);
  if (cause.kind === "duplicate" && cause.twin) {
    const twin = [cause.twin.account, cause.twin.app].filter(Boolean).join(", ");
    parts.push(t("twin", { where: twin, when: when(cause.twin.at, format) }));
  }
  return parts.join(" · ");
}

function signedAmount(cause: BalanceCause, money: (value: number) => string, t: CheckWords): string {
  if (cause.amount == null) return t("amountUnknown");
  if (cause.direction === "income") return `+${money(cause.amount)}`;
  if (cause.direction === "expense") return `−${money(cause.amount)}`;
  return money(cause.amount);
}

function causeHref(cause: BalanceCause): string {
  if (cause.link.kind === "transaction") return `/ledger?edit=${cause.link.id}`;
  return cause.link.folder ? `/review?folder=${cause.link.folder}` : "/review";
}

function CauseList({ causes, money, highlighted }: { causes: BalanceCause[]; money: (value: number) => string; highlighted: Set<string> }) {
  const t = useTranslations("accounts.check");
  const format = useFormat();
  return (
    <ul className="list">
      {causes.map((cause) => (
        <li key={cause.key} data-explains={highlighted.has(cause.key) || undefined}>
          <ListRow
            layout="stacked"
            icon="alert-triangle"
            title={t(`kind.${cause.kind}`)}
            meta={causeDetail(cause, t, format)}
            amount={<span className="t-amt">{signedAmount(cause, money, t)}</span>}
            actions={
              <Link href={causeHref(cause)} className="ws-linkbtn">
                {cause.link.kind === "transaction" ? t("openTransaction") : t("openReview")}
              </Link>
            }
          />
        </li>
      ))}
    </ul>
  );
}

const FORM_ID = "balance-check-form";

/**
 * Cocokkan saldo (manual-balance-check, apply-markas-rekening D10): a 560 px shell dialog that compares the balance in
 * the bank app with the record, names the likely causes of a difference with links to their rows, and offers
 * "Sesuaikan saldo" with the typed amount. Every request is unchanged.
 */
export function BalanceCheckDialog({
  account,
  money,
  onClose,
  onAdjust,
}: {
  account: AccountItem;
  money: (value: number) => string;
  onClose: () => void;
  /** Opens "Sesuaikan saldo" with the typed balance; `onReconciled` runs after it succeeds. */
  onAdjust: (actual: number, onReconciled: (actual: number) => void) => void;
}) {
  const qc = useQueryClient();
  const t = useTranslations("accounts.check");
  const common = useTranslations("common");
  const format = useFormat();
  const [actualText, setActualText] = useState("");
  const [inputError, setInputError] = useState("");
  const [result, setResult] = useState<BalanceCheckResult | null>(null);
  const [matchedAt, setMatchedAt] = useState<string | null>(null);

  const checkMutation = useMutation({
    mutationFn: (actual: number) => api.post<BalanceCheckResult>(`/accounts/${account.id}/balance-check`, { actual_balance: actual }),
    onSuccess: (data) => { setResult(data); setMatchedAt(null); },
    onError: (error: Error) => setInputError(error.message || t("failed")),
  });
  const matchMutation = useMutation({
    mutationFn: (actual: number) => api.post<{ matched_at: string }>(`/accounts/${account.id}/balance-matches`, { actual_balance: actual }),
    onSuccess: (data) => {
      setMatchedAt(data.matched_at);
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
    },
  });

  const runCheck = () => {
    const clean = actualText.trim().replace(/[^0-9]/g, "");
    if (!clean) {
      setInputError(t("enterBalance"));
      document.getElementById("balance-check-actual")?.focus();
      return;
    }
    setInputError("");
    checkMutation.mutate(parseNumberFromDots(actualText));
  };

  const afterAdjust = (actual: number) => {
    matchMutation.mutate(actual, { onSuccess: () => checkMutation.mutate(actual) });
  };

  const highlighted = new Set(result?.explanation ?? []);
  const explaining = result ? result.candidates.filter((cause) => highlighted.has(cause.key)) : [];
  const difference = result?.difference ?? 0;
  const differs = Boolean(result) && difference !== 0;

  const footer = (
    <>
      {differs && (
        <button type="button" className="ws-btn ws-btn-quiet" disabled={checkMutation.isPending} onClick={() => checkMutation.mutate(result!.actual_balance)}>
          {t("again")}
        </button>
      )}
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>{t("close")}</button>
      {differs && result!.can_adjust && (
        <button type="button" className="ws-btn ws-btn-ink" onClick={() => onAdjust(result!.actual_balance, afterAdjust)}>
          {t("adjust")}
        </button>
      )}
    </>
  );

  return (
    <ShellDialog open onClose={onClose} title={t("title")} meta={t("subtitle", { name: account.name })} width={560} closeLabel={common("close")} footer={footer}>
      <form
        id={FORM_ID}
        className="form-stack"
        noValidate
        onSubmit={(event) => {
          event.preventDefault();
          if (!checkMutation.isPending) runCheck();
        }}
      >
        <div className="form-grid">
          <MonetaryInput
            id="balance-check-actual"
            name="actual_balance"
            label={t("bankBalance")}
            value={actualText}
            onChange={(value) => { setActualText(value); setInputError(""); }}
            placeholder="1.250.000"
            error={inputError}
          />
          <div className="field">
            <p className="flabel">{t("recorded")}</p>
            <p className="t-fig"><MaskedMoney text={money(account.balance)} /></p>
          </div>
        </div>
        <button type="submit" className="ws-btn ws-btn-ink" disabled={checkMutation.isPending}>
          {checkMutation.isPending ? t("running") : t("run")}
        </button>
      </form>

      <div aria-live="polite">
        {result && (
          <section className="form-stack" aria-labelledby="balance-check-result">
            <div>
              <h3 id="balance-check-result" className="t-label">{t("result")}</h3>
              <p className={difference > 0 ? "t-fig c-income" : difference < 0 ? "t-fig c-expense" : "t-fig"}>
                {difference === 0 ? t("noDifference") : `${difference > 0 ? "+" : "−"}${money(Math.abs(difference))}`}
              </p>
              {difference !== 0 && <p className="t-meta">{difference > 0 ? t("more") : t("less")}</p>}
            </div>
            <dl className="dl">
              <dt>{t("recordedShort")}</dt>
              <dd className="tabular-nums">{money(result.recorded_balance)}</dd>
              <dt>{t("inBankApp")}</dt>
              <dd className="tabular-nums">{money(result.actual_balance)}</dd>
            </dl>
            <p className="t-meta">
              {result.account.is_parent ? t("basisWithPockets", { basis: basisText(result.since, t, format) }) : basisText(result.since, t, format)}
            </p>
            {result.capture_gap && result.last_report_at && (
              <p className="t-body">{t("captureGap", { when: when(result.last_report_at, format) })}</p>
            )}

            {difference === 0 ? (
              <div className="box">
                <div className="grow">
                  <p className="t-strong">{t("matched")}</p>
                  {matchedAt ? (
                    <p className="t-body" role="status">{t("matchedSaved")}</p>
                  ) : (
                    <>
                      <p className="t-body">{t("matchedLine")}</p>
                      <button type="button" className="ws-btn ws-btn-ink" disabled={matchMutation.isPending} onClick={() => matchMutation.mutate(result.actual_balance)}>
                        {matchMutation.isPending ? t("saving") : t("markMatched")}
                      </button>
                    </>
                  )}
                </div>
              </div>
            ) : (
              <>
                {explaining.length > 0 && (
                  <div>
                    <p className="t-strong">{t("explains", { count: explaining.length })}</p>
                    <CauseList causes={explaining} money={money} highlighted={highlighted} />
                  </div>
                )}
                {result.candidates.length > explaining.length && (
                  <section aria-labelledby="balance-check-causes">
                    <h3 id="balance-check-causes" className="t-label">
                      {explaining.length ? t("otherCauses") : t("causes")}
                    </h3>
                    <CauseList causes={result.candidates.filter((cause) => !highlighted.has(cause.key))} money={money} highlighted={highlighted} />
                  </section>
                )}
                {result.candidates.length === 0 && <p className="t-body">{t("noCauses")}</p>}
                {result.unattributed.length > 0 && (
                  <section aria-labelledby="balance-check-unattributed">
                    <h3 id="balance-check-unattributed" className="t-label">{t("unattributed")}</h3>
                    <CauseList causes={result.unattributed} money={money} highlighted={highlighted} />
                  </section>
                )}
                {matchMutation.isError && <p role="alert" className="form-alert">{t("matchFailed")}</p>}
              </>
            )}
          </section>
        )}
      </div>
    </ShellDialog>
  );
}
