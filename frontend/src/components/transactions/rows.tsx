"use client";

import { useMemo } from "react";
import { useTranslations } from "next-intl";
import type { AccountOptionLabels } from "@/components/ui/AccountSelectOptions";
import { MaskedMoney } from "@/components/workspace/money";
import type { Format } from "@/lib/format";
import type { Translate } from "@/lib/i18n/types";
import { KAKEIBO_TERMS, kakeiboBucketInfo, type KakeiboBucket } from "@/lib/kakeibo";
import { isPlaceholderNote } from "@/lib/placeholderNotes";
import { isMovementCategory } from "@/lib/systemCategories";
import type { RowFacts } from "./types";

/**
 * How a ledger row describes itself on Transaksi, Beranda, the transaction dialog and the pages that list rows
 * (apply-markas-transaksi D11, D22, D27, D29, D30). The words come from the caller's labels, read from the catalogue.
 */
export interface RowLabels {
  investmentTopUp: string;
  topupTitle: (product: string) => string;
  undescribed: string;
  noCategory: string;
  noCategoryYet: string;
  waiting: string;
  income: string;
  transfer: string;
  categoryTopup: string;
  unpaired: string;
  inFrom: (other: string) => string;
  outTo: (other: string) => string;
  /** A Kakeibo bucket's name; the bucket key itself when unknown. */
  bucket: (bucket: string) => string;
  flow: { saved: string; taken: string; invested: string; debtPayment: string };
  none: string;
  paidFromSavings: string;
  payLater: string;
  due: (date: string) => string;
  installment: (k: number, n: number) => string;
  paid: (date: string) => string;
  billPayment: string;
}

/** A Kakeibo bucket's glossary name; the key itself for a value the app does not know. */
export function bucketName(t: Translate, bucket: string): string {
  const entry = KAKEIBO_TERMS[bucket as KakeiboBucket];
  return entry ? t(`terms.${entry.term}`) : bucket;
}

export function rowLabelsFor(t: Translate): RowLabels {
  return {
    investmentTopUp: t("terms.investmentTopUp"),
    topupTitle: (product) => t("transactions.row.topupTitle", { product }),
    undescribed: t("terms.noDescription"),
    noCategory: t("terms.noCategory"),
    noCategoryYet: t("transactions.row.noCategoryYet"),
    waiting: t("transactions.row.waiting"),
    income: t("transactions.row.income"),
    transfer: t("terms.transfer"),
    categoryTopup: t("transactions.row.categoryTopup"),
    unpaired: t("transactions.row.pairNotFound"),
    inFrom: (other) => t("transactions.row.inFrom", { other }),
    outTo: (other) => t("transactions.row.outTo", { other }),
    bucket: (bucket) => bucketName(t, bucket),
    flow: {
      saved: t("transactions.flow.saved"),
      taken: t("transactions.flow.taken"),
      invested: t("transactions.flow.invested"),
      debtPayment: t("transactions.flow.debtPayment"),
    },
    none: t("transactions.flow.none"),
    paidFromSavings: t("transactions.row.paidFromSavings"),
    payLater: t("transactions.row.payLater"),
    due: (date) => t("transactions.row.due", { date }),
    installment: (k, n) => t("transactions.row.installment", { k: String(k), n: String(n) }),
    paid: (date) => t("transactions.row.paid", { date }),
    billPayment: t("transactions.row.billPayment"),
  };
}

/** The account picker's words from the catalogue, for Transaksi's dialogs. */
export function useAccountOptionLabels(): AccountOptionLabels {
  const t = useTranslations("transactions.accountOptions");
  return useMemo(() => ({
    creditGroup: t("creditGroup"),
    parent: (name) => t("parent", { name }),
    parentDefault: (name, pocket) => t("parentDefault", { name, pocket }),
    groupTotal: (name, amount) => t("groupTotal", { name, amount }),
    owed: (amount) => t("owed", { amount }),
    noOther: t("noOther"),
    noLiquid: t("noLiquid"),
  }), [t]);
}

/** The interface language's row labels. */
export function useRowLabels(): RowLabels {
  const t = useTranslations();
  return useMemo(() => rowLabelsFor(t), [t]);
}

export type RowKind = "topup" | "movement" | "income" | "expense";

export function rowKind(row: RowFacts): RowKind {
  if (row.investment_topup_id) return "topup";
  if (row.is_consolidated_transfer || row.movement_id || isMovementCategory(row.category_system_key) || row.is_excluded_from_budget) {
    return "movement";
  }
  return row.type === "income" ? "income" : "expense";
}

export function isWaiting(row: RowFacts): boolean {
  return row.review_status === "needs_review";
}

/** "Bank · Pocket" for a pocket, the account's own name otherwise; null when the side is unknown. */
export function accountLabel(name: string | null | undefined, parent: string | null | undefined): string | null {
  if (!name) return null;
  return parent ? `${parent} · ${name}` : name;
}

export interface MovementRoute {
  source: string | null;
  target: string | null;
}

/** Whether the row's own account received the money: an inbound leg, or an unpaired income. */
function ownLegIsTarget(row: RowFacts): boolean {
  if (row.is_consolidated_transfer || row.is_inferred_transfer) return false;
  return row.movement_role ? row.movement_role === "inbound" : row.type === "income";
}

/**
 * Where a movement's money came from and where it went. A merged row is always its outbound leg; a linked leg
 * follows its role; a leg nobody paired takes its side from its type, the other side unknown.
 */
export function movementRoute(row: RowFacts): MovementRoute {
  const own = accountLabel(row.account_name, row.account_parent_name);
  if (row.is_consolidated_transfer || row.is_inferred_transfer) {
    return { source: own, target: accountLabel(row.target_account_name, row.target_account_parent_name) };
  }
  const partner = accountLabel(row.transfer_target_account_name, row.transfer_target_account_parent_name);
  return ownLegIsTarget(row) ? { source: partner, target: own } : { source: own, target: partner };
}

export function routeText(route: MovementRoute): string {
  return `${route.source ?? "?"} → ${route.target ?? "?"}`;
}

export function routeIncomplete(row: RowFacts): boolean {
  if (rowKind(row) !== "movement") return false;
  const route = movementRoute(row);
  return !route.source || !route.target;
}

/**
 * A movement seen from the account a list is filtered to: "Masuk dari …" when the money came in, "Keluar ke …" when
 * it left. Merged rows only exist in unfiltered lists, so the row's own leg is the viewed one.
 */
export function movementDirection(row: RowFacts, labels: RowLabels): string {
  const route = movementRoute(row);
  const ownIsTarget = ownLegIsTarget(row);
  const other = ownIsTarget ? route.source : route.target;
  if (!other) return labels.unpaired;
  return ownIsTarget ? labels.inFrom(other) : labels.outTo(other);
}

/** A transfer's or top-up's own note, as stored; the server's "no note" placeholder counts as none (D11). */
export function movementNote(row: RowFacts): string | null {
  return isPlaceholderNote(row.notes) ? null : (row.notes as string).trim();
}

/** The product a top-up went into, by its own name; null when the list does not know it. */
function topupProduct(row: RowFacts): string | null {
  if (row.is_consolidated_transfer || row.is_inferred_transfer) return row.target_account_name ?? null;
  return (ownLegIsTarget(row) ? row.account_name : row.transfer_target_account_name) ?? null;
}

/** The row's title: a transfer or top-up by its note (owner feedback 2), else its route or product (D11). */
export function rowTitle(row: RowFacts, labels: RowLabels): string {
  const kind = rowKind(row);
  if (kind === "topup") {
    const product = topupProduct(row);
    return movementNote(row) ?? (product ? labels.topupTitle(product) : labels.investmentTopUp);
  }
  if (kind === "movement") return movementNote(row) ?? routeText(movementRoute(row));
  return row.merchant || row.notes || row.category_name || labels.undescribed;
}

/** Whether the title is the row's own words rather than its route: the route then goes to the meta. */
function titledByNote(row: RowFacts): boolean {
  return rowKind(row) === "movement" ? movementNote(row) !== null : false;
}

export function rowCategory(row: RowFacts, labels: RowLabels): string {
  const kind = rowKind(row);
  if (kind === "movement") return labels.transfer;
  if (kind === "topup") return labels.categoryTopup;
  if (row.category_name) return row.category_name;
  return isWaiting(row) ? labels.noCategoryYet : labels.noCategory;
}

/** A move's saving direction (D27): only what changes "Ditabung & investasi", and "Bayar utang" as information. */
export function savingFlowLabel(row: RowFacts, labels: RowLabels): string | null {
  switch (row.saving_flow) {
    case "saved":
      return labels.flow.saved;
    case "taken":
      return labels.flow.taken;
    case "invested":
      return labels.flow.invested;
    case "debt_payment":
      return labels.flow.debtPayment;
    default:
      return null;
  }
}

export interface RowStatusFacts {
  text: string;
  /** The pillar's bucket for the dot; null when the column holds words only (D11). */
  bucket: string | null;
  color?: string;
  warning?: boolean;
}

/** The pillar column: a waiting row says so, income is "Uang masuk", a move names its saving direction or "—". */
export function rowStatus(row: RowFacts, labels: RowLabels): RowStatusFacts {
  const kind = rowKind(row);
  if (isWaiting(row)) return { text: labels.waiting, bucket: null, warning: true };
  if (kind === "income") return { text: labels.income, bucket: null };
  if (kind === "movement" || kind === "topup") return { text: savingFlowLabel(row, labels) ?? labels.none, bucket: null };
  const bucket = row.kakeibo_type || "unclassified";
  return { text: labels.bucket(bucket), bucket, color: kakeiboBucketInfo(bucket)?.color };
}

export function RowStatus({ row, labels }: { row: RowFacts; labels: RowLabels }) {
  const status = rowStatus(row, labels);
  if (status.bucket === null) return <span className={status.warning ? "c-warning" : undefined}>{status.text}</span>;
  return (
    <>
      <span className={status.bucket === "unclassified" ? "dot is-unclassified" : "dot"} style={status.color ? { background: status.color } : undefined} aria-hidden="true" />
      <span>{status.text}</span>
    </>
  );
}

export interface RowLine {
  text: string;
  warning?: boolean;
}

/** A credit purchase's facts (H5): "dibayar nanti · SPayLater · cicilan 2/3 · jatuh tempo 11 Des", or "SPayLater · lunas 4 Des". */
export function creditPurchaseLine(row: RowFacts, labels: RowLabels, format: Format): string | null {
  const credit = row.credit;
  if (!credit || credit.role !== "purchase") return null;
  if (credit.settled_on) return `${credit.account_name} · ${labels.paid(format.shortDate(credit.settled_on))}`;
  const parts = [labels.payLater, credit.account_name];
  const count = credit.installment_count ?? 1;
  if (count > 1) parts.push(labels.installment(Math.min(count, (credit.installments_settled ?? 0) + 1), count));
  if (credit.next_due_date) parts.push(labels.due(format.shortDate(credit.next_due_date)));
  return parts.join(" · ");
}

function isCreditPayment(row: RowFacts): boolean {
  return row.credit?.role === "payment";
}

/**
 * The desktop meta beside the title (D11, D30): the account with its bank; for a move its route when the note is
 * the title, its direction in an account's own list, or the missing side; a credit purchase's due date.
 */
export function rowMeta(row: RowFacts, labels: RowLabels, format: Format, accountView = false): RowLine {
  const kind = rowKind(row);
  if (kind === "movement" || kind === "topup") {
    if (kind === "movement" && routeIncomplete(row)) return { text: labels.unpaired, warning: true };
    const parts: string[] = [];
    if (accountView) parts.push(movementDirection(row, labels));
    else if (kind === "topup" || titledByNote(row)) parts.push(routeText(movementRoute(row)));
    if (isCreditPayment(row)) parts.push(labels.billPayment);
    return { text: parts.join(" · ") };
  }
  const credit = creditPurchaseLine(row, labels, format);
  if (credit) return { text: credit };
  const account = accountLabel(row.account_name, row.account_parent_name) ?? "";
  if (row.saving_flow === "paid_from_savings") return { text: [account, labels.paidFromSavings].filter(Boolean).join(" · ") };
  return { text: account };
}

/** The phone's second line after the time: the pillar, the income's category, or what the move was. */
export function rowPhoneLine(row: RowFacts, labels: RowLabels, format: Format, accountView = false): RowLine {
  const kind = rowKind(row);
  if (isWaiting(row)) return { text: labels.waiting, warning: true };
  if (kind === "income") return { text: row.category_name ?? labels.income };
  if (kind === "movement" || kind === "topup") {
    if (kind === "movement" && routeIncomplete(row)) return { text: labels.unpaired, warning: true };
    if (accountView) return { text: movementDirection(row, labels) };
    if (isCreditPayment(row)) return { text: labels.billPayment };
    if (titledByNote(row)) return { text: routeText(movementRoute(row)) };
    return { text: savingFlowLabel(row, labels) ?? (kind === "topup" ? labels.categoryTopup : labels.transfer) };
  }
  const credit = row.credit?.role === "purchase" ? row.credit : null;
  if (credit) return { text: credit.settled_on ? labels.paid(format.shortDate(credit.settled_on)) : labels.payLater };
  return { text: rowStatus(row, labels).text };
}

/** A rich-text tag that shows an amount and hides it with "Sembunyikan saldo", for messages with `<amount></amount>`. */
export function maskedTag(text: string) {
  return function MaskedAmount() {
    return <MaskedMoney text={text} />;
  };
}

export type AmountKind = RowKind | "plain";

const AMOUNT_CLASS: Record<AmountKind, string> = {
  expense: "t-amt c-expense",
  income: "t-amt c-income",
  movement: "t-amt c-transfer",
  topup: "t-amt c-invest",
  plain: "t-amt",
};

/** A signed amount in its kind's colour (D29): "−Rp" in the expense red, "+Rp" green, transfers sky, top-ups lavender. */
export function KindAmount({ kind, amount, money, className }: { kind: AmountKind; amount: number; money: (amount: number) => string; className?: string }) {
  const sign = kind === "expense" ? "−" : kind === "income" ? "+" : "";
  // A size class (the dialog's 32 px `t-fig-l`) replaces the row's `t-amt`; the kind's colour stays.
  const classes = className ? AMOUNT_CLASS[kind].replace("t-amt", className) : AMOUNT_CLASS[kind];
  return <MaskedMoney text={`${sign}${money(amount)}`} className={classes} />;
}

/** The one row amount of Transaksi, Beranda, the dialog and the Rekening detail (D22, D29). */
export function RowAmount({ row, money, className }: { row: RowFacts; money: (amount: number) => string; className?: string }) {
  return <KindAmount kind={rowKind(row)} amount={row.amount} money={money} className={className} />;
}

const JAKARTA_DAY = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });

/** The row's Asia/Jakarta day as `YYYY-MM-DD`, a key rather than display text. */
export function jakartaDay(value: Date | string): string {
  return JAKARTA_DAY.format(typeof value === "string" ? new Date(value) : value);
}
