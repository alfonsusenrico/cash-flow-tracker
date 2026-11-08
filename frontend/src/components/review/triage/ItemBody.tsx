"use client";

import Link from "next/link";
import { Fragment, type ReactNode } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { transaksiHref, type FolderKey } from "@/lib/triage";
import type { ItemParts } from "./types";

export const HEADING_ID = "triage-item-heading";

/** The review reasons the catalogue names; any other code is shown as sent rather than guessed. */
const REASONS = [
  "uncertain_category",
  "model_fallback",
  "low_confidence",
  "model_flagged",
  "ambiguous_movement",
  "purpose_question",
  "awaiting_counterpart",
] as const;
type Reason = (typeof REASONS)[number];

function isKnownReason(reason: string): reason is Reason {
  return (REASONS as readonly string[]).includes(reason);
}

/**
 * An amount with the SPEC row signs and colours: spending "−Rp" in the expense red (owner, 2026-11-06), income
 * "+Rp" in the income colour, and a transfer's leg "Rp" in the transfer colour. Hidden balances mask it.
 */
export function SignedAmount({ amount, direction }: { amount: number; direction: "expense" | "income" | "transfer" }) {
  const money = useMoney();
  if (direction === "income") return <MaskedMoney text={`+${money(amount)}`} className="t-amt c-income" />;
  if (direction === "expense") return <MaskedMoney text={`−${money(amount)}`} className="t-amt c-expense" />;
  return <MaskedMoney text={money(amount)} className="t-amt c-transfer" />;
}

export interface Failure {
  message: string;
  transactionId?: string | null;
}

/** The polite notice and the failure line (D7, D16); the notice's live region stays mounted while the pane is. */
export function StatusLine({ notice, failure, folder }: { notice: string; failure: Failure | null; folder: FolderKey }) {
  const t = useTranslations("review.notice");
  return (
    <>
      <div className="tj-status" aria-live="polite">
        {notice && <p className="t-meta">{notice}</p>}
      </div>
      {failure && (
        <p role="alert" className="t-meta c-expense tj-line">
          {failure.message}
          {failure.transactionId && (
            <>
              {" "}
              <Link className="ws-linkbtn" href={transaksiHref(failure.transactionId, folder)}>
                {t("openInLedger")}
              </Link>
            </>
          )}
        </p>
      )}
    </>
  );
}

/** The kind label with "N dari M" and ‹ › (D7); the phone's sheet carries the kind in its title instead. */
export function ItemHeadRow({
  kind,
  index,
  total,
  onMove,
}: {
  kind: string;
  index: number;
  total: number;
  onMove: (step: 1 | -1) => void;
}) {
  const t = useTranslations("review.detail");
  const format = useFormat();
  return (
    <div className="sec-h is-flush tj-head">
      <p className="t-label">{kind}</p>
      {index >= 0 && (
        <div className="tj-nav">
          <button type="button" className="ws-icon-btn" aria-label={t("previous")} aria-keyshortcuts="K" disabled={index <= 0} onClick={() => onMove(-1)}>
            <Icon name="chevL" className="h-4 w-4" />
          </button>
          <span className="pos">{t("position", { index: format.integer(index + 1), total: format.integer(total) })}</span>
          <button
            type="button"
            className="ws-icon-btn"
            aria-label={t("next")}
            aria-keyshortcuts="J"
            disabled={index >= total - 1}
            onClick={() => onMove(1)}
          >
            <Icon name="chevR" className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
}

/** The question as the item's heading and focus target, then one meta line, the lines, answers, rows and text. */
export function ItemBody({ parts, status }: { parts: ItemParts; status: ReactNode }) {
  const t = useTranslations("review");
  const meta = parts.meta.filter(Boolean);
  const reasons = (parts.reasons ?? []).map((reason) => (isKnownReason(reason) ? t(`reasons.${reason}`) : reason));
  return (
    <>
      <h2 id={HEADING_ID} className="t-lead tj-q" tabIndex={-1} data-item-heading="">
        {parts.question}
      </h2>
      {meta.length > 0 && (
        <p className="t-meta tj-meta">
          {meta.map((part, index) => (
            <Fragment key={index}>
              {index > 0 && " · "}
              {part}
            </Fragment>
          ))}
        </p>
      )}
      {parts.warning && <p className="t-meta c-warning tj-line">{parts.warning}</p>}
      {parts.note && <p className="t-meta tj-line">{parts.note}</p>}
      {reasons.length > 0 && <p className="t-meta tj-line">{reasons.join(" · ")}</p>}
      {status}
      {parts.answers}
      {parts.rows}
      {parts.raw && (
        <p className="raw">
          <span className="sr-only">{t("detail.rawPrefix")} </span>
          {parts.raw}
        </p>
      )}
    </>
  );
}
