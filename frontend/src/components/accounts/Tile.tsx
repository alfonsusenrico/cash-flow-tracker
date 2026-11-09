"use client";

import { useId, type DragEvent, type KeyboardEvent, type ReactNode } from "react";
import { Icon } from "@/components/ui/Icon";
import { GainText, ProgressBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { cn } from "@/lib/utils";
import { initials, isCredit } from "./accountFacts";
import { useAccountText } from "./accountText";
import { creditFacts } from "./credit";
import { badgeInk } from "./displayColours";
import type { Holding } from "./wallet";
import type { AccountItem, LayoutGrouping } from "./types";

export const tileDomId = (id: string) => `rk-tile-${id}`;

/**
 * The institution mark (D6): the rekening's initials on its display colour, 24 × 24. The initials take white or
 * ink, whichever contrasts more (`badgeInk`), so they reach 4.5:1 on the owner's colours. Decorative: the name is
 * always written beside it.
 */
export function InstitutionMark({ name, colour }: { name: string; colour: string }) {
  return (
    <span className="inst" style={{ background: colour, color: badgeInk(colour) }} aria-hidden="true">
      {initials(name)}
    </span>
  );
}

export interface TileReorder {
  /** Below 768 px the mode moves tiles with buttons instead of dragging. */
  first: boolean;
  last: boolean;
  grabbed: boolean;
  dropBefore: boolean;
  onMove: (step: number, keyboard: boolean) => void;
  onDragStart: (event: DragEvent<HTMLElement>) => void;
  onDragEnd: () => void;
  onDragOver: (event: DragEvent<HTMLElement>) => void;
  onDrop: (event: DragEvent<HTMLElement>) => void;
}

/** The meta line under a tile's name (D6). */
function useMetaLine(holding: Holding, grouping: LayoutGrouping, amountProducts: Set<string>): string {
  const { t, typeLabel, holdingLine, roleLabel, kindLabel } = useAccountText();
  const { account, rekening } = holding;
  if (holding.ownBalance) return t("tile.outsidePockets");
  if (holding.gain) {
    const holdingText = holdingLine(account, amountProducts);
    const lead = grouping === "rekening" && holding.kind ? kindLabel(holding.kind) : rekening.name;
    if (!account.parent_id && grouping === "use") return holdingText || typeLabel(account);
    return holdingText ? t("tile.productLine", { platform: lead, holding: holdingText }) : lead;
  }
  if (account.parent_id) {
    const pocket = t("tile.pocketOf", { parent: rekening.name });
    const withDefault = rekening.default_pocket_id === account.id ? t("tile.withDefault", { meta: pocket, default: t("tile.default") }) : pocket;
    return grouping === "rekening" ? t("tile.inUse", { use: roleLabel(holding.role), meta: withDefault }) : withDefault;
  }
  return grouping === "rekening" ? t("tile.inUse", { use: roleLabel(holding.role), meta: typeLabel(account) }) : typeLabel(account);
}

/**
 * One tile (D6, D7): a whole button that opens the account's detail, with the mark, the name, one meta line, the
 * balance and, for a product, its gain; at most one note line. With an actionable "Perlu dicocokkan" the tile is a
 * `div` holding two buttons, so no button is nested. In Atur urutan the tile is a dashed, draggable card whose
 * grip takes Alt + arrows, plus move buttons on a phone (D12).
 */
export function Tile({
  holding,
  grouping,
  colour,
  amountProducts,
  awaiting,
  reorder,
  onOpen,
  onReconcile,
  className,
}: {
  holding: Holding;
  grouping: LayoutGrouping;
  colour: string;
  amountProducts: Set<string>;
  awaiting: number;
  reorder: TileReorder | null;
  onOpen: () => void;
  onReconcile: (account: AccountItem) => void;
  className?: string;
}) {
  const { t, format } = useAccountText();
  const meta = useMetaLine(holding, grouping, amountProducts);
  const factsId = useId();
  const { account, rekening } = holding;
  const credit = isCredit(account);
  const facts = credit ? creditFacts(account) : null;
  const canReconcile = !holding.ownBalance && !credit && !account.children?.length && account.investment_tracking_mode !== "amount";
  const flagAction = !reorder && Boolean(account.reconciliation_required) && canReconcile && !facts?.overLimit;

  const name = holding.ownBalance ? rekening.name : account.name;
  const metaNode = facts ? <CreditMeta facts={facts} /> : <span className="t-meta">{meta}</span>;

  let note: ReactNode = null;
  if (facts?.overLimit) note = null;
  else if (account.reconciliation_required && !holding.ownBalance) {
    note = flagAction ? (
      <button type="button" className="tl-note c-warning" aria-label={t("tile.needsCheckAction", { name: account.name })} onClick={() => onReconcile(account)}>
        <Icon name="alert-triangle" className="h-3.5 w-3.5" />
        {t("tile.needsCheck")}
      </button>
    ) : (
      <span className="tl-note c-warning">
        <Icon name="alert-triangle" className="h-3.5 w-3.5" />
        {t("tile.needsCheck")}
      </span>
    );
  } else if (awaiting > 0) note = <span className="tl-note muted">{t("tile.awaiting", { count: awaiting })}</span>;

  const body = (
    <>
      <span className="tl-top">
        <InstitutionMark name={rekening.name} colour={colour} />
        <span className="names">
          <span className="tl-name">{name}</span>
          {metaNode}
        </span>
        {reorder && <Icon name="grip-vertical" className="tl-grip h-4 w-4" />}
      </span>
      {facts?.limit ? <ProgressBar value={facts.owed} max={facts.limit} color="var(--out-solid)" /> : null}
      <span className="tl-bottom" id={factsId}>
        {facts ? <CreditAmount facts={facts} /> : <MaskedMoney text={format.money(holding.amount)} className="tl-amt t-amt" />}
        {holding.gain && <GainText gain={holding.gain.known ? { amount: holding.gain.gain, cost: holding.gain.cost } : "unknown"} />}
        {facts && <CreditDue facts={facts} />}
      </span>
    </>
  );

  if (reorder) {
    return (
      <div
        className={cn("tile", reorder.grabbed && "is-grabbed", reorder.dropBefore && "is-drop", className)}
        data-account-row={holding.id}
        draggable
        onDragStart={reorder.onDragStart}
        onDragEnd={reorder.onDragEnd}
        onDragOver={reorder.onDragOver}
        onDrop={reorder.onDrop}
      >
        <button
          type="button"
          id={tileDomId(holding.id)}
          className="tl-open"
          aria-label={name}
          aria-describedby="rk-card-move-hint"
          onKeyDown={(event: KeyboardEvent<HTMLButtonElement>) => {
            if (!event.altKey || (event.key !== "ArrowLeft" && event.key !== "ArrowRight")) return;
            event.preventDefault();
            reorder.onMove(event.key === "ArrowLeft" ? -1 : 1, true);
          }}
        />
        {body}
        <span className="rk-moves">
          <button type="button" className="ws-icon-btn" aria-label={t("reorder.cardEarlier", { name })} disabled={reorder.first} onClick={() => reorder.onMove(-1, false)}>
            <Icon name="chevron-left" className="h-4 w-4" />
          </button>
          <button type="button" className="ws-icon-btn" aria-label={t("reorder.cardLater", { name })} disabled={reorder.last} onClick={() => reorder.onMove(1, false)}>
            <Icon name="chevron-right" className="h-4 w-4" />
          </button>
        </span>
      </div>
    );
  }

  if (flagAction) {
    return (
      <div className={cn("tile has-flag", className)} data-account-row={holding.id}>
        <button type="button" id={tileDomId(holding.id)} className="tl-open" aria-label={`${name}, ${meta}`} aria-describedby={factsId} onClick={onOpen} />
        {body}
        {note}
      </div>
    );
  }
  return (
    <button type="button" id={tileDomId(holding.id)} className={cn("tile", className)} data-account-row={holding.id} onClick={onOpen}>
      {body}
      {note}
    </button>
  );
}

function CreditMeta({ facts }: { facts: ReturnType<typeof creditFacts> }) {
  const { t, format } = useAccountText();
  if (facts.overLimit) {
    return (
      <span className="t-meta c-expense">
        <Icon name="alert-triangle" className="mr-1 inline h-3.5 w-3.5 align-[-2px]" />
        {t("credit.overLimit")}
      </span>
    );
  }
  if (facts.limitLeft === null) return <span className="t-meta">{t("credit.noLimit")}</span>;
  return (
    <span className="t-meta">
      {t.rich("credit.limitLeft", { amount: () => <MaskedMoney text={format.money(facts.limitLeft!)} /> })}
    </span>
  );
}

function CreditAmount({ facts }: { facts: ReturnType<typeof creditFacts> }) {
  const { t, format } = useAccountText();
  if (facts.overpaid > 0) {
    return <span className="tl-amt t-amt c-income">{t.rich("credit.overpaid", { amount: () => <MaskedMoney text={format.money(facts.overpaid)} /> })}</span>;
  }
  return (
    <span className="tl-amt t-amt c-expense">
      <span className="sr-only">{t("detail.debt")} </span>
      <MaskedMoney text={facts.owed > 0 ? `−${format.money(facts.owed)}` : format.money(0)} />
    </span>
  );
}

function CreditDue({ facts }: { facts: ReturnType<typeof creditFacts> }) {
  const { t, format } = useAccountText();
  if (!facts.paymentDay) return <span className="tl-due t-meta">{t("credit.noDueDay")}</span>;
  const due = facts.nextDue;
  if (!due) return null;
  const warn = due.status === "due_today" || due.status === "overdue";
  const date = due.status === "due_today" ? t("credit.dueToday") : format.shortDate(due.due_date);
  return (
    <span className={cn("tl-due t-meta", warn && "c-warning")}>
      {t.rich("credit.nextBill", { date, amount: () => <MaskedMoney text={format.money(due.left_to_pay)} /> })}
      {due.status === "overdue" && ` · ${t("credit.overdue")}`}
    </span>
  );
}
