/* The Rekening page's model (organize-accounts-by-use D6, D7): cards for every kantong and produk, rows per use
   or per rekening, sums for the rows and gains summed only over products with a known cost basis. Net worth,
   Saldo tersedia, Dana investasi, Investasi and Utang come from the server; nothing here stands in for them. */

import { isCredit, isHolding, owedOn } from "./accountFacts";
import type { AccountItem, AccountLayout, AccountRole, InvestmentKind, LayoutGrouping } from "./types";

export type AssetUse = Exclude<AccountRole, "utang">;

export interface UseInfo {
  key: AccountRole;
  label: string;
  colour: string;
  note?: string;
}

export const USES: (UseInfo & { key: AssetUse })[] = [
  { key: "harian", label: "Uang harian", colour: "var(--use-harian)" },
  { key: "tabungan", label: "Tabungan", colour: "var(--use-tabungan)" },
  { key: "dana_investasi", label: "Dana investasi", colour: "var(--use-dana)", note: "belum dibelikan" },
  { key: "investasi", label: "Investasi", colour: "var(--use-investasi)" },
];

export const DEBT_USE: UseInfo = { key: "utang", label: "Utang", colour: "var(--out-solid)" };

export const KINDS: { key: InvestmentKind; label: string; colour: string }[] = [
  { key: "emas", label: "Emas", colour: "var(--kind-emas)" },
  { key: "reksa_dana", label: "Reksa dana", colour: "var(--kind-reksa-dana)" },
  { key: "saham", label: "Saham", colour: "var(--kind-saham)" },
  { key: "lainnya", label: "Lainnya", colour: "var(--use-investasi)" },
];

export function roleInfo(role: AccountRole): UseInfo {
  return USES.find((use) => use.key === role) ?? DEBT_USE;
}

export function kindInfo(kind: InvestmentKind) {
  return KINDS.find((entry) => entry.key === kind) ?? KINDS[KINDS.length - 1];
}

/** The account's role from the server, or the role V51 would backfill when an older API sends none. */
export function roleOf(account: AccountItem): AccountRole {
  if (account.role) return account.role;
  if (isCredit(account)) return "utang";
  if (isHolding(account)) return "investasi";
  return "harian";
}

const KIND_OF_INSTRUMENT: Record<string, InvestmentKind> = { gold: "emas", mutual_fund: "reksa_dana", stock: "saham" };

export function kindOf(account: AccountItem, role: AccountRole): InvestmentKind | null {
  if (role !== "investasi") return null;
  if (account.kind) return account.kind;
  return KIND_OF_INSTRUMENT[account.instrument_type ?? ""] ?? "lainnya";
}

/** A product's gain, or unknown when its cost basis is missing. */
export type Gain = { known: true; gain: number; cost: number } | { known: false };

/** One card: a kantong, a produk, a stand-alone rekening, or a parent's balance outside its pockets. */
export interface Holding {
  id: string;
  /** The account the card opens. */
  account: AccountItem;
  /** The top-level rekening the card belongs to (itself for a stand-alone rekening). */
  rekening: AccountItem;
  role: AccountRole;
  kind: InvestmentKind | null;
  /** The value of an asset, or what a debt owes, as a positive amount. */
  amount: number;
  /** A parent's own balance beside its pockets (D5: cards are leaves plus such a balance). */
  ownBalance: boolean;
  /** Set for products only. */
  gain: Gain | null;
}

function productGain(account: AccountItem): Gain {
  if (account.cost_basis == null) return { known: false };
  // The same figure as the server's group totals: value minus cost basis.
  return { known: true, gain: account.balance - account.cost_basis, cost: account.cost_basis };
}

/** A product holds an instrument or has a cost basis; an investment platform without products is neither. */
function isProduct(account: AccountItem): boolean {
  return Boolean(account.instrument_type) || account.cost_basis != null;
}

function holdingOf(account: AccountItem, rekening: AccountItem, ownBalance: boolean, amount: number): Holding {
  const role = roleOf(account);
  const debt = role === "utang";
  return {
    id: account.id,
    account,
    rekening,
    role,
    kind: kindOf(account, role),
    amount: debt ? owedOn(account) : amount,
    ownBalance,
    gain: role === "investasi" && !ownBalance && isProduct(account) ? productGain(account) : null,
  };
}

/** Every card of the top-level accounts, in the API's order (display_order). */
export function buildHoldings(topLevel: AccountItem[]): Holding[] {
  const holdings: Holding[] = [];
  for (const rekening of topLevel) {
    const pockets = (rekening.children ?? []).filter((pocket) => !pocket.is_archived);
    if (!pockets.length) {
      holdings.push(holdingOf(rekening, rekening, false, rekening.balance));
      continue;
    }
    const inPockets = pockets.reduce((sum, pocket) => sum + pocket.balance, 0);
    const own = rekening.own_balance ?? rekening.balance - inPockets;
    if (own !== 0) holdings.push(holdingOf(rekening, rekening, true, own));
    for (const pocket of pockets) holdings.push(holdingOf(pocket, rekening, false, pocket.balance));
  }
  return holdings;
}

export const isDebt = (holding: Holding) => holding.role === "utang";

/** Assets minus what the debts owe; the figure of a row header. */
export function netOf(holdings: Holding[]): number {
  return holdings.reduce((sum, holding) => sum + (isDebt(holding) ? -holding.amount : holding.amount), 0);
}

export function assetSum(holdings: Holding[]): number {
  return holdings.filter((holding) => !isDebt(holding)).reduce((sum, holding) => sum + holding.amount, 0);
}

/** The gain of a set of holdings: null without products, unknown when any product lacks a cost basis (D6). */
export function gainOf(holdings: Holding[]): Gain | null {
  const products = holdings.filter((holding) => holding.gain);
  if (!products.length) return null;
  if (products.some((holding) => !holding.gain!.known)) return { known: false };
  let gain = 0;
  let cost = 0;
  for (const holding of products) {
    const known = holding.gain as { gain: number; cost: number };
    gain += known.gain;
    cost += known.cost;
  }
  return { known: true, gain, cost };
}

/** Each asset use's sum and share of the positive asset total, for the share bar, rings and legends. */
export function sharesByUse(holdings: Holding[]): { use: (typeof USES)[number]; amount: number; share: number }[] {
  const amounts = USES.map((use) => ({ use, amount: assetSum(holdings.filter((holding) => holding.role === use.key)) }));
  const total = amounts.reduce((sum, entry) => sum + Math.max(0, entry.amount), 0);
  return amounts.map((entry) => ({ ...entry, share: total > 0 ? Math.max(0, entry.amount) / total : 0 }));
}

export interface Section {
  /** A use key under Per kegunaan, a top-level account id under Per rekening. */
  key: string;
  title: string;
  colour?: string;
  note?: string;
  rekening?: AccountItem;
  holdings: Holding[];
}

export const SECTION_USES: UseInfo[] = [...USES, DEBT_USE];

/** Every row of a grouping in the default order, empty rows included so a saved layout can name them. */
export function sectionsFor(grouping: LayoutGrouping, topLevel: AccountItem[], holdings: Holding[]): Section[] {
  if (grouping === "use") {
    return SECTION_USES.map((use) => ({
      key: use.key,
      title: use.label,
      colour: use.colour,
      note: use.note,
      holdings: holdings.filter((holding) => holding.role === use.key),
    }));
  }
  return topLevel.map((rekening) => ({
    key: rekening.id,
    title: rekening.name,
    rekening,
    holdings: holdings.filter((holding) => holding.rekening.id === rekening.id),
  }));
}

function byPosition<T>(items: T[], idOf: (item: T) => string, saved: string[] | undefined): T[] {
  if (!saved?.length) return items;
  const position = (item: T, index: number) => {
    const at = saved.indexOf(idOf(item));
    // Items the layout does not name keep their default order after the named ones.
    return at < 0 ? saved.length + index : at;
  };
  return items
    .map((item, index) => ({ item, rank: position(item, index) }))
    .sort((left, right) => left.rank - right.rank)
    .map((entry) => entry.item);
}

/** The rows and cards in the owner's saved order; anything the layout does not name follows in default order. */
export function applyLayout(sections: Section[], layout: AccountLayout | null | undefined): Section[] {
  if (!layout || !Array.isArray(layout.sections)) return sections;
  const cards = layout.cards ?? {};
  return byPosition(sections, (section) => section.key, layout.sections).map((section) => ({
    ...section,
    holdings: byPosition(section.holdings, (holding) => holding.id, cards[section.key]),
  }));
}

/** The full layout of the sections as shown, the body of `PUT /accounts/layout`. */
export function layoutOf(grouping: LayoutGrouping, sections: Section[]): AccountLayout {
  return {
    grouping,
    sections: sections.map((section) => section.key),
    cards: Object.fromEntries(sections.map((section) => [section.key, section.holdings.map((holding) => holding.id)])),
  };
}

/** A copy of the list with one item moved to the destination index; the same list when the move is impossible. */
export function moved<T>(items: T[], from: number, to: number): T[] {
  if (from < 0 || to < 0 || from >= items.length || to >= items.length || from === to) return items;
  const next = [...items];
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}

export function formatPercent(value: number): string {
  return value.toLocaleString("id-ID", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

/** An active debt from Target & Tagihan (an obligation); it has no rekening and no place in the saved layout. */
export interface Bill {
  id: string;
  name: string;
  /** What is still owed, as a positive amount. */
  amount: number;
}

/** The row key of the bills under Per rekening, after the rows the owner orders. */
export const BILLS_ROW = "tagihan";

/** Active obligations from the net worth response; the same set its `total_liabilities` counts. */
export function billsOf(obligations: { id: string; name: string; remaining_amount: number }[] | undefined): Bill[] {
  return (obligations ?? [])
    .filter((obligation) => obligation.remaining_amount > 0)
    .map((obligation) => ({ id: obligation.id, name: obligation.name, amount: obligation.remaining_amount }));
}

/** The bills a row shows: Utang holds them under Per kegunaan. */
export function billsIn(section: Section, grouping: LayoutGrouping, bills: Bill[]): Bill[] {
  return grouping === "use" && section.key === "utang" ? bills : [];
}

/** A row shows when it has cards, Utang also with bills alone. */
export function isShown(section: Section, grouping: LayoutGrouping, bills: Bill[]): boolean {
  return section.holdings.length > 0 || billsIn(section, grouping, bills).length > 0;
}
