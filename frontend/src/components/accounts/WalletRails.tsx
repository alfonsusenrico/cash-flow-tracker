"use client";

import { useState, type DragEvent, type KeyboardEvent } from "react";
import Link from "next/link";
import { Icon } from "@/components/ui/Icon";
import { ReconciliationFlag } from "@/components/ui/ReconciliationFlag";
import { cn } from "@/lib/utils";
import { KIND, holdingLine, isCredit } from "./accountFacts";
import { GainChip } from "./GainChip";
import { RekeningLogo, Swatch } from "./walletParts";
import {
  BILLS_ROW, DEBT_USE, KINDS, USES, assetSum, billsIn, gainOf, isDebt, kindInfo, netOf, roleInfo, type Bill, type Holding, type Section,
} from "./wallet";
import type { AccountItem, LayoutGrouping } from "./types";
import styles from "./wallet.module.css";

type Money = (value: number) => string;

export const MOVE_HINT_ID = "wallet-move-hint";
export const cardDomId = (id: string) => `wallet-card-${id}`;
export const railHandleId = (key: string) => `wallet-rail-${key}`;
export const railHeadingId = (key: string) => `wallet-heading-${key}`;

type Drag = { kind: "card"; section: string; id: string } | { kind: "rail"; key: string } | null;

export interface RailActions {
  onOpen: (id: string) => void;
  onReconcile: (account: AccountItem) => void;
  /** Moves a card within its row; `keyboard` keeps the focus on the moved card. */
  onMoveCard: (section: string, id: string, destination: number, keyboard: boolean) => void;
  onMoveSection: (key: string, destination: number, keyboard: boolean) => void;
}

/** The Kartu tab's rows: one row of equal cards per use or per rekening (D7). */
export function WalletRails({
  sections,
  grouping,
  money,
  colours,
  draggable,
  amountProducts,
  awaiting,
  bills,
  actions,
}: {
  sections: Section[];
  grouping: LayoutGrouping;
  money: Money;
  colours: Map<string, string>;
  draggable: boolean;
  amountProducts: Set<string>;
  awaiting: Map<string, number>;
  /** Active obligations: after the credit cards in Utang, or in a last Tagihan row under Per rekening. */
  bills: Bill[];
  actions: RailActions;
}) {
  const [drag, setDrag] = useState<Drag>(null);
  const [over, setOver] = useState<string | null>(null);
  const endDrag = () => {
    setDrag(null);
    setOver(null);
  };

  return (
    <div className={styles.rails}>
      {draggable && <p id={MOVE_HINT_ID} hidden>Alt dan panah kiri atau kanan untuk memindah.</p>}
      {sections.map((section, sectionIndex) => {
        const sectionBills = billsIn(section, grouping, bills);
        const railDrop = (event: DragEvent) => {
          if (drag?.kind !== "rail" || drag.key === section.key) return;
          event.preventDefault();
          if (event.type === "dragover") setOver(section.key);
          else {
            actions.onMoveSection(drag.key, sectionIndex, false);
            endDrag();
          }
        };
        return (
          <section
            key={section.key}
            className={cn(styles.rail, over === section.key && drag?.kind === "rail" && styles.railDrop)}
            aria-labelledby={railHeadingId(section.key)}
            data-rail={section.key}
            onDragOver={railDrop}
            onDrop={railDrop}
          >
            <RailHead
              section={section}
              bills={sectionBills}
              grouping={grouping}
              money={money}
              colour={colours.get(section.key) ?? "var(--muted)"}
              handle={draggable ? {
                onDragStart: (event) => {
                  setDrag({ kind: "rail", key: section.key });
                  event.dataTransfer.effectAllowed = "move";
                  event.dataTransfer.setData("text/plain", section.key);
                },
                onDragEnd: endDrag,
                onKeyDown: (event) => {
                  if (!event.altKey || (event.key !== "ArrowUp" && event.key !== "ArrowDown")) return;
                  event.preventDefault();
                  actions.onMoveSection(section.key, sectionIndex + (event.key === "ArrowUp" ? -1 : 1), true);
                },
              } : null}
              onOpen={actions.onOpen}
            />
            <RailExtra section={section} grouping={grouping} money={money} />
            <ul
              className={styles.cards}
              aria-label={`Kartu ${section.title}`}
              onDragOver={(event) => {
                if (drag?.kind === "card" && drag.section === section.key) event.preventDefault();
              }}
              onDrop={(event) => {
                if (drag?.kind !== "card" || drag.section !== section.key) return;
                event.preventDefault();
                actions.onMoveCard(section.key, drag.id, section.holdings.length - 1, false);
                endDrag();
              }}
            >
              {section.holdings.map((holding, index) => (
                <li
                  key={holding.id}
                  className={cn(
                    styles.card,
                    isDebt(holding) && styles.cardDebt,
                    drag?.kind === "card" && drag.id === holding.id && styles.dragging,
                    drag?.kind === "card" && over === holding.id && styles.dropBefore,
                  )}
                  style={{ ["--card-accent" as string]: roleInfo(holding.role).colour }}
                  data-account-row={holding.id}
                  draggable={draggable || undefined}
                  onDragStart={(event) => {
                    setDrag({ kind: "card", section: section.key, id: holding.id });
                    event.dataTransfer.effectAllowed = "move";
                    event.dataTransfer.setData("text/plain", holding.id);
                  }}
                  onDragEnd={endDrag}
                  onDragOver={(event) => {
                    if (drag?.kind !== "card" || drag.section !== section.key || drag.id === holding.id) return;
                    event.preventDefault();
                    event.stopPropagation();
                    setOver(holding.id);
                  }}
                  onDrop={(event) => {
                    if (drag?.kind !== "card" || drag.section !== section.key) return;
                    event.preventDefault();
                    event.stopPropagation();
                    if (drag.id !== holding.id) actions.onMoveCard(section.key, drag.id, index, false);
                    endDrag();
                  }}
                >
                  <WalletCard
                    holding={holding}
                    grouping={grouping}
                    money={money}
                    colour={colours.get(holding.rekening.id) ?? "var(--muted)"}
                    amountProducts={amountProducts}
                    waiting={awaiting.get(holding.account.id) ?? 0}
                    movable={draggable}
                    onOpen={() => actions.onOpen(holding.account.id)}
                    onReconcile={actions.onReconcile}
                    onKeyDown={(event) => {
                      if (!event.altKey || (event.key !== "ArrowLeft" && event.key !== "ArrowRight")) return;
                      event.preventDefault();
                      actions.onMoveCard(section.key, holding.id, index + (event.key === "ArrowLeft" ? -1 : 1), true);
                    }}
                  />
                </li>
              ))}
              {sectionBills.map((bill) => <BillCard key={bill.id} bill={bill} money={money} />)}
            </ul>
          </section>
        );
      })}
      {grouping === "rekening" && bills.length > 0 && <BillsRow bills={bills} money={money} />}
    </div>
  );
}

const billsRow: Section = { key: BILLS_ROW, title: "Tagihan", colour: DEBT_USE.colour, holdings: [] };

/** Obligations have no rekening, so Per rekening shows them in one fixed last row. */
function BillsRow({ bills, money }: { bills: Bill[]; money: Money }) {
  return (
    <section className={styles.rail} aria-labelledby={railHeadingId(BILLS_ROW)} data-rail={BILLS_ROW}>
      <RailHead section={billsRow} bills={bills} grouping="rekening" money={money} colour={DEBT_USE.colour} handle={null} onOpen={() => undefined} />
      <ul className={styles.cards} aria-label="Kartu Tagihan">
        {bills.map((bill) => <BillCard key={bill.id} bill={bill} money={money} />)}
      </ul>
    </section>
  );
}

/** A debt from Target & Tagihan; it opens that page, where it is paid and edited. */
function BillCard({ bill, money }: { bill: Bill; money: Money }) {
  const factsId = `wallet-bill-${bill.id}-facts`;
  return (
    <li className={cn(styles.card, styles.cardDebt)} data-bill-row={bill.id}>
      <Link href="/goals" className={styles.cardOpen} aria-label={`${bill.name}, tagihan. Buka Target & Tagihan`} aria-describedby={factsId}>
        <span className={styles.cardTop}>
          <span className={styles.useTag}>
            <Swatch colour={DEBT_USE.colour} />
            Tagihan
          </span>
        </span>
        <span className={styles.cardBody}>
          <span className={styles.cardName}>{bill.name}</span>
          <span className={styles.cardExtra}>di Target & Tagihan</span>
        </span>
        <span id={factsId} className={styles.cardFoot}>
          <span className={cn(styles.cardAmount, styles.debtFigure)}>−{money(bill.amount)}</span>
        </span>
      </Link>
    </li>
  );
}

function countText(section: Section, bills: Bill[]): string {
  if (section.note) return section.note;
  if (section.rekening && !section.rekening.children?.length) return "";
  const count = section.holdings.length;
  if (!section.rekening && (section.key === "utang" || section.key === BILLS_ROW)) {
    return [count ? `${count} rekening` : "", bills.length ? `${bills.length} tagihan` : ""].filter(Boolean).join(" · ");
  }
  const products = section.holdings.filter((holding) => holding.gain).length;
  if (products === count) return `${count} produk`;
  if (products === 0) return `${count} kantong`;
  return `${count} kantong & produk`;
}

interface HandleProps {
  onDragStart: (event: DragEvent<HTMLButtonElement>) => void;
  onDragEnd: () => void;
  onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => void;
}

function RailHead({
  section,
  bills,
  grouping,
  money,
  colour,
  handle,
  onOpen,
}: {
  section: Section;
  bills: Bill[];
  grouping: LayoutGrouping;
  money: Money;
  colour: string;
  handle: HandleProps | null;
  onOpen: (id: string) => void;
}) {
  const total = netOf(section.holdings) - bills.reduce((sum, bill) => sum + bill.amount, 0);
  const count = countText(section, bills);
  const rekening = section.rekening;
  // A rekening with pockets has no card of its own, so its name opens its detail (Tambah kantong, Ubah rekening).
  const opensRekening = grouping === "rekening" && rekening && Boolean(rekening.children?.length);
  const flaggedPockets = opensRekening ? section.holdings.filter((holding) => holding.account.parent_id && holding.account.reconciliation_required).length : 0;
  return (
    <div className={styles.railHead}>
      <div className={styles.railTitle}>
        {handle && (
          <button
            type="button"
            id={railHandleId(section.key)}
            className={styles.handle}
            draggable
            aria-label={`Pindah baris ${section.title}. Alt dan panah atas atau bawah.`}
            onDragStart={handle.onDragStart}
            onDragEnd={handle.onDragEnd}
            onKeyDown={handle.onKeyDown}
          >
            <Icon name="grip-vertical" className="h-4 w-4" />
          </button>
        )}
        <h2 id={railHeadingId(section.key)} tabIndex={-1}>
          {rekening ? <RekeningLogo rekening={rekening} colour={colour} /> : <Swatch colour={section.colour ?? "var(--muted)"} />}
          {opensRekening ? (
            <button type="button" className={styles.railOpen} onClick={() => onOpen(rekening!.id)}>{section.title}</button>
          ) : (
            <span>{section.title}</span>
          )}
          {count && (
            <>
              {" "}
              <span className={styles.railNote}>{count}</span>
            </>
          )}
        </h2>
      </div>
      <span className={styles.railFigures}>
        {flaggedPockets > 0 && <ReconciliationFlag name={section.title} pocketCount={flaggedPockets} />}
        <GainChip gain={gainOf(section.holdings)} money={money} />
        <span className={cn(styles.railTotal, total < 0 && styles.debtFigure)}>{total < 0 ? `−${money(-total)}` : money(total)}</span>
      </span>
    </div>
  );
}

/** Investasi's sub-totals per kind (Per kegunaan), or a rekening's composition by use (Per rekening). */
function RailExtra({ section, grouping, money }: { section: Section; grouping: LayoutGrouping; money: Money }) {
  if (grouping === "use" && section.key === "investasi") {
    const kinds = KINDS.map((kind) => ({ kind, holdings: section.holdings.filter((holding) => holding.kind === kind.key) })).filter((entry) => entry.holdings.length);
    return (
      <ul className={styles.railLegend} aria-label="Investasi per jenis">
        {kinds.map(({ kind, holdings }) => (
          <li key={kind.key}>
            <Swatch colour={kind.colour} />
            {kind.label} <b>{money(assetSum(holdings))}</b>
            <GainChip gain={gainOf(holdings)} money={money} />
          </li>
        ))}
      </ul>
    );
  }
  if (grouping !== "rekening") return null;
  const total = assetSum(section.holdings);
  if (total <= 0) return null;
  const parts = USES.map((use) => ({ use, amount: assetSum(section.holdings.filter((holding) => holding.role === use.key)) })).filter((part) => part.amount > 0);
  return (
    <div className={styles.railCompo}>
      <span className={styles.stack} aria-hidden="true">
        {parts.map((part) => (
          <span key={part.use.key} style={{ flexGrow: part.amount, background: part.use.colour }} />
        ))}
      </span>
      <ul className={styles.railLegend} aria-label={`Isi ${section.title}`}>
        {parts.map((part) => (
          <li key={part.use.key}>
            <Swatch colour={part.use.colour} />
            {part.use.label} <b>{money(part.amount)}</b>
          </li>
        ))}
      </ul>
    </div>
  );
}

function cardName(holding: Holding): string {
  const { account, rekening } = holding;
  if (holding.ownBalance) return `${account.name}, saldo di luar kantong`;
  if (account.parent_id) return `${account.name}, ${holding.gain ? "produk" : "kantong"} di ${rekening.name}`;
  return account.name;
}

function extraLine(holding: Holding, grouping: LayoutGrouping, amountProducts: Set<string>): string {
  const { account } = holding;
  if (holding.ownBalance) return "saldo di luar kantong";
  if (isCredit(account)) return account.payment_day ? `dibayar tiap tgl ${account.payment_day}` : KIND.credit;
  if (holding.gain) {
    const kind = grouping === "rekening" && holding.kind ? kindInfo(holding.kind).label : "";
    return [kind, holdingLine(account, amountProducts)].filter(Boolean).join(" · ");
  }
  return account.parent_id ? "" : KIND[account.type];
}

function DebtAmount({ holding, money }: { holding: Holding; money: Money }) {
  const { account } = holding;
  if (account.balance > 0) return <>Lebih bayar {money(account.balance)}</>;
  return <>{holding.amount > 0 ? `−${money(holding.amount)}` : money(0)}</>;
}

function WalletCard({
  holding,
  grouping,
  money,
  colour,
  amountProducts,
  waiting,
  movable,
  onOpen,
  onReconcile,
  onKeyDown,
}: {
  holding: Holding;
  grouping: LayoutGrouping;
  money: Money;
  colour: string;
  amountProducts: Set<string>;
  waiting: number;
  movable: boolean;
  onOpen: () => void;
  onReconcile: (account: AccountItem) => void;
  onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => void;
}) {
  const { account, rekening } = holding;
  const debt = isDebt(holding);
  const use = roleInfo(holding.role);
  const extra = extraLine(holding, grouping, amountProducts);
  const factsId = `${cardDomId(holding.id)}-facts`;
  const limit = debt ? account.credit_limit ?? null : null;
  const used = limit ? Math.min(100, Math.round((holding.amount / limit) * 100)) : 0;
  const canReconcile = !holding.ownBalance && !debt && account.investment_tracking_mode !== "amount";
  return (
    <>
      <button
        type="button"
        id={cardDomId(holding.id)}
        className={styles.cardOpen}
        aria-label={cardName(holding)}
        aria-describedby={movable ? `${factsId} ${MOVE_HINT_ID}` : factsId}
        onClick={onOpen}
        onKeyDown={onKeyDown}
      >
        <span className={styles.cardTop}>
          {grouping === "use" ? (
            <>
              <span className={styles.prov}>
                <RekeningLogo rekening={rekening} colour={colour} />
                {rekening.name}
              </span>
              {holding.kind && <span className={styles.useTag}>{kindInfo(holding.kind).label}</span>}
            </>
          ) : (
            <span className={styles.useTag}>
              <Swatch colour={use.colour} />
              {use.label}
            </span>
          )}
        </span>
        <span className={styles.cardBody}>
          <span className={styles.cardName}>{account.name}</span>
          {extra && <span className={styles.cardExtra}>{extra}</span>}
        </span>
        <span id={factsId} className={styles.cardFoot}>
          <span className={cn(styles.cardAmount, debt && account.balance <= 0 && holding.amount > 0 && styles.debtFigure)}>
            {debt ? <DebtAmount holding={holding} money={money} /> : money(holding.amount)}
          </span>{" "}
          <GainChip gain={holding.gain} money={money} />
        </span>
      </button>
      {limit ? (
        <div className={styles.cardLimit}>
          <div
            className={styles.meter}
            role="progressbar"
            aria-label={`Limit terpakai ${account.name}`}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={used}
            aria-valuetext={`${money(holding.amount)} dari ${money(limit)}, ${used}%`}
          >
            <i style={{ width: `${used}%` }} data-over={account.over_limit ? "" : undefined} />
          </div>
          <span className={styles.cardExtra}>{used}% dari limit {money(limit)}</span>
        </div>
      ) : null}
      {(account.over_limit || account.reconciliation_required || waiting > 0) && (
        <div className={styles.cardNotes}>
          {account.over_limit && (
            <span className="ws-pill ws-tone-warn">
              <Icon name="alert-triangle" className="h-3.5 w-3.5" />
              Melewati limit
            </span>
          )}
          {account.reconciliation_required && (
            <ReconciliationFlag name={account.name} onReconcile={canReconcile ? () => onReconcile(account) : undefined} />
          )}
          {waiting > 0 && <span className="ws-caption">{waiting} pindah saldo belum lengkap</span>}
        </div>
      )}
    </>
  );
}
