"use client";

import { useState, type DragEvent } from "react";
import Link from "next/link";
import { Icon } from "@/components/ui/Icon";
import { GainText } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";
import { useAccountText } from "./accountText";
import { InstitutionMark, Tile, type TileReorder } from "./Tile";
import { KINDS, USES, assetSum, gainOf, isDebt, netOf, type Gain, type Holding, type Section } from "./wallet";
import type { AccountItem, LayoutGrouping } from "./types";

export const groupHandleId = (key: string) => `rk-handle-${key}`;
export const groupHeadingId = (key: string) => `rk-heading-${key}`;

type Drag = { kind: "card"; section: string; id: string } | { kind: "group"; key: string } | null;

export interface GroupActions {
  onOpen: (id: string) => void;
  onReconcile: (account: AccountItem) => void;
  /** Moves a tile within its group; `keyboard` keeps the focus on the moved tile. */
  onMoveCard: (section: string, id: string, destination: number, keyboard: boolean) => void;
  onMoveSection: (key: string, destination: number, keyboard: boolean) => void;
}

const gainFigure = (gain: Gain | null) => (gain ? (gain.known ? { amount: gain.gain, cost: gain.cost } : ("unknown" as const)) : null);

/**
 * Kartu (D5, D6): one group per use or per rekening, each with a 2 px straight rail, an uppercase label, a muted
 * count, its gain where it exists and its total, then its tiles. Utang lists credit accounts only and ends with
 * one link to the obligations in Target & Tagihan (owner request 2026-11-05).
 */
export function RekeningGroups({
  sections,
  grouping,
  colours,
  amountProducts,
  awaiting,
  obligations,
  reordering,
  phone,
  actions,
}: {
  sections: Section[];
  grouping: LayoutGrouping;
  colours: Map<string, string>;
  amountProducts: Set<string>;
  awaiting: Map<string, number>;
  /** Active obligations outside Rekening: their count and `total_liabilities − credit_owed`. */
  obligations: { count: number; amount: number };
  reordering: boolean;
  phone: boolean;
  actions: GroupActions;
}) {
  const { t, format } = useAccountText();
  const [drag, setDrag] = useState<Drag>(null);
  const [over, setOver] = useState<string | null>(null);
  const endDrag = () => {
    setDrag(null);
    setOver(null);
  };
  const hasUtang = sections.some((section) => section.key === "utang");
  const billsRow = obligations.count > 0 && obligations.amount > 0;
  // Under Per kegunaan the row closes Utang; when Utang has no credit tile it shows as Utang's only row.
  const utangOnlyBills = grouping === "use" && billsRow && !hasUtang;

  return (
    <div className={cn("rk-groups", reordering && "reorder")}>
      {reordering && <p id="rk-card-move-hint" hidden>{t("reorder.cardMove")}</p>}
      {sections.map((section, sectionIndex) => {
        const groupDrop = (event: DragEvent) => {
          if (!reordering || drag?.kind !== "group" || drag.key === section.key) return;
          event.preventDefault();
          if (event.type === "dragover") setOver(section.key);
          else {
            actions.onMoveSection(drag.key, sectionIndex, false);
            endDrag();
          }
        };
        return (
          <Group
            key={section.key}
            section={section}
            grouping={grouping}
            colours={colours}
            amountProducts={amountProducts}
            awaiting={awaiting}
            bills={grouping === "use" && section.key === "utang" && billsRow ? obligations : null}
            dropTarget={over === section.key && drag?.kind === "group"}
            reorder={reordering ? {
              first: sectionIndex === 0,
              last: sectionIndex === sections.length - 1,
              onMove: (step, keyboard) => actions.onMoveSection(section.key, sectionIndex + step, keyboard),
              onDragStart: (event) => {
                setDrag({ kind: "group", key: section.key });
                event.dataTransfer.effectAllowed = "move";
                event.dataTransfer.setData("text/plain", section.key);
              },
              onDragEnd: endDrag,
            } : null}
            phone={phone}
            onGroupDrag={groupDrop}
            cardReorder={(holding, index) => reordering ? {
              first: index === 0,
              last: index === section.holdings.length - 1,
              grabbed: drag?.kind === "card" && drag.id === holding.id,
              dropBefore: drag?.kind === "card" && over === holding.id,
              onMove: (step, keyboard) => actions.onMoveCard(section.key, holding.id, index + step, keyboard),
              onDragStart: (event) => {
                event.stopPropagation();
                setDrag({ kind: "card", section: section.key, id: holding.id });
                event.dataTransfer.effectAllowed = "move";
                event.dataTransfer.setData("text/plain", holding.id);
              },
              onDragEnd: endDrag,
              onDragOver: (event) => {
                if (drag?.kind !== "card" || drag.section !== section.key || drag.id === holding.id) return;
                event.preventDefault();
                event.stopPropagation();
                setOver(holding.id);
              },
              onDrop: (event) => {
                if (drag?.kind !== "card" || drag.section !== section.key) return;
                event.preventDefault();
                event.stopPropagation();
                if (drag.id !== holding.id) actions.onMoveCard(section.key, drag.id, index, false);
                endDrag();
              },
            } : null}
            actions={actions}
          />
        );
      })}
      {utangOnlyBills && (
        <section className="grp" style={{ "--rail": "var(--out-solid)" } as React.CSSProperties} aria-labelledby={groupHeadingId("utang")}>
          <div className="grp-h">
            <h2 className="t-label" id={groupHeadingId("utang")}>{t("groups.utang")}</h2>
            <span className="count">{`${t("groups.credit", { count: 0 })} · ${t("debt.billsCount", { count: obligations.count })}`}</span>
            <MaskedMoney text={`−${format.money(obligations.amount)}`} className="total t-amt c-expense" />
          </div>
          <ObligationsLink obligations={obligations} />
        </section>
      )}
      {grouping === "rekening" && billsRow && (
        <div className="rk-bills is-last">
          <ObligationsLink obligations={obligations} />
        </div>
      )}
    </div>
  );
}

/** One whole-row link to the obligations, named with their amount: "Tagihan −Rp X di Target & Tagihan". */
function ObligationsLink({ obligations }: { obligations: { count: number; amount: number } }) {
  const { t, format } = useAccountText();
  const { hideBalances } = useAppCtx();
  const amount = `−${format.money(obligations.amount)}`;
  const name = hideBalances ? `${t("debt.billsLink")}, ${t("debt.billsCount", { count: obligations.count })}` : t("debt.billsName", { amount });
  return (
    <div className="list rk-bills">
      <Link href="/goals" className="row ws-select-row" aria-label={name}>
        <span className="r-main">
          <span className="r-title">{t("debt.billsLink")}</span>
          <span className="r-meta">{t("debt.billsCount", { count: obligations.count })}</span>
        </span>
        <span className="r-amt">
          <MaskedMoney text={amount} className="t-amt c-expense" />
        </span>
        <span className="chev">
          <Icon name="chevron-right" className="h-4 w-4" />
        </span>
      </Link>
    </div>
  );
}

interface GroupReorder {
  first: boolean;
  last: boolean;
  onMove: (step: number, keyboard: boolean) => void;
  onDragStart: (event: DragEvent<HTMLButtonElement>) => void;
  onDragEnd: () => void;
}

function Group({
  section,
  grouping,
  colours,
  amountProducts,
  awaiting,
  bills,
  dropTarget,
  reorder,
  phone,
  onGroupDrag,
  cardReorder,
  actions,
}: {
  section: Section;
  grouping: LayoutGrouping;
  colours: Map<string, string>;
  amountProducts: Set<string>;
  awaiting: Map<string, number>;
  bills: { count: number; amount: number } | null;
  dropTarget: boolean;
  reorder: GroupReorder | null;
  phone: boolean;
  onGroupDrag: (event: DragEvent) => void;
  cardReorder: (holding: Holding, index: number) => TileReorder | null;
  actions: GroupActions;
}) {
  const { t, format, roleLabel, kindLabel } = useAccountText();
  const rekening = section.rekening;
  const title = rekening ? rekening.name : t(`groups.${section.use!.labelKey}`);
  const rail = rekening ? colours.get(rekening.id) ?? "var(--line-strong)" : section.use!.colour;
  const debtGroup = section.key === "utang";
  const total = netOf(section.holdings);
  const gain = gainOf(section.holdings);
  const products = section.holdings.filter((holding) => holding.gain).length;
  const count = section.holdings.length;
  const opensRekening = grouping === "rekening" && rekening && Boolean(rekening.children?.length);
  const flaggedPockets = opensRekening ? section.holdings.filter((holding) => holding.account.parent_id && holding.account.reconciliation_required).length : 0;

  let countText: string;
  if (debtGroup) countText = t("groups.credit", { count }) + (bills ? ` · ${t("debt.billsCount", { count: bills.count })}` : "");
  else if (section.key === "dana_investasi") countText = `${t("groups.accounts", { count })} · ${t("groups.danaNote")}`;
  else if (products === count) countText = t("groups.products", { count });
  else if (products === 0) countText = t("groups.pockets", { count });
  else countText = t("groups.pocketsAndProducts", { count });

  // Per rekening names each use's subtotal in words, in role order (D5; replaces the stacked composition bar).
  const subtotals = grouping === "rekening" && rekening?.children?.length
    ? USES.map((use) => ({ use, amount: assetSum(section.holdings.filter((holding) => holding.role === use.key)) })).filter((part) => part.amount > 0)
    : [];
  const subtotalsShown = subtotals.length > 1 || (subtotals.length === 1 && products === 0);
  const wallet = phone && grouping === "use" && section.key === "harian";
  const tilesLabel = t("groups.cards", { title });

  const tiles = (className?: string) => section.holdings.map((holding, index) => (
    <li key={holding.id}>
      <Tile
        holding={holding}
        grouping={grouping}
        colour={colours.get(holding.rekening.id) ?? "var(--muted)"}
        amountProducts={amountProducts}
        awaiting={awaiting.get(holding.account.id) ?? 0}
        reorder={cardReorder(holding, index)}
        onOpen={() => actions.onOpen(holding.account.id)}
        onReconcile={actions.onReconcile}
        className={className}
      />
    </li>
  ));

  return (
    <section
      className={cn("grp", rekening && "parent-block", dropTarget && "is-drop")}
      style={{ "--rail": rail } as React.CSSProperties}
      aria-labelledby={groupHeadingId(section.key)}
      data-group={section.key}
      onDragOver={onGroupDrag}
      onDrop={onGroupDrag}
    >
      <div className="grp-h">
        {reorder && (
          <button
            type="button"
            id={groupHandleId(section.key)}
            className="ws-icon-btn rk-handle"
            draggable
            aria-label={t("reorder.rowHandle", { title })}
            onDragStart={reorder.onDragStart}
            onDragEnd={reorder.onDragEnd}
            onKeyDown={(event) => {
              if (!event.altKey || (event.key !== "ArrowUp" && event.key !== "ArrowDown")) return;
              event.preventDefault();
              reorder.onMove(event.key === "ArrowUp" ? -1 : 1, true);
            }}
          >
            <Icon name="grip-vertical" className="h-4 w-4" />
          </button>
        )}
        <h2 className="t-label" id={groupHeadingId(section.key)} tabIndex={-1}>
          {rekening && <InstitutionMark name={rekening.name} colour={rail} />}
          {opensRekening && !reorder ? (
            <button type="button" className="ws-linkbtn" onClick={() => actions.onOpen(rekening!.id)}>{title}</button>
          ) : (
            <span>{title}</span>
          )}
        </h2>
        <span className="count">
          {wallet ? <span className="sr-only">{`${t("groups.swipe")}. `}</span> : null}
          {countText}
          {subtotalsShown && subtotals.map((part) => (
            <span key={part.use.key}>
              {" · "}
              {t.rich("groups.subtotal", { use: roleLabel(part.use.key), amount: () => <MaskedMoney text={format.money(part.amount)} /> })}
            </span>
          ))}
          {flaggedPockets > 0 && (
            <>
              {" · "}
              <span className="c-warning">
                <Icon name="alert-triangle" className="h-3.5 w-3.5" />
                {t("groups.needCheck", { count: flaggedPockets })}
              </span>
            </>
          )}
        </span>
        <GainText gain={gainFigure(gain)} unknownLabel={rekening ? t("tile.costIncomplete") : undefined} />
        <MaskedMoney
          text={total < 0 ? `−${format.money(Math.abs(total))}` : format.money(total)}
          className={cn("total t-amt", total < 0 && "c-expense")}
        />
        {reorder && (
          <span className="rk-moves">
            <button type="button" className="ws-icon-btn" aria-label={t("reorder.rowUp", { name: title })} disabled={reorder.first} onClick={() => reorder.onMove(-1, false)}>
              <Icon name="chevron-up" className="h-4 w-4" />
            </button>
            <button type="button" className="ws-icon-btn" aria-label={t("reorder.rowDown", { name: title })} disabled={reorder.last} onClick={() => reorder.onMove(1, false)}>
              <Icon name="chevron-down" className="h-4 w-4" />
            </button>
          </span>
        )}
      </div>
      {grouping === "use" && section.key === "investasi" && <KindLines holdings={section.holdings} kindLabel={kindLabel} />}
      {wallet && !reorder ? (
        <ul className="wallet" aria-label={`${tilesLabel}, ${t("groups.swipe")}`}>{tiles()}</ul>
      ) : (
        <ul className="tiles" aria-label={tilesLabel}>{tiles()}</ul>
      )}
      {bills && <ObligationsLink obligations={bills} />}
    </section>
  );
}

/** Investasi per jenis (D5): one quiet line per kind with its count, gain and total, before the product tiles. */
function KindLines({ holdings, kindLabel }: { holdings: Holding[]; kindLabel: (kind: Holding["kind"] & string) => string }) {
  const { t, format } = useAccountText();
  const kinds = KINDS.map((kind) => ({ kind, holdings: holdings.filter((holding) => holding.kind === kind.key && !isDebt(holding)) })).filter((entry) => entry.holdings.length);
  if (!kinds.length) return null;
  return (
    <ul className="rk-kinds" aria-label={t("groups.perKind")}>
      {kinds.map(({ kind, holdings: items }) => (
        <li key={kind.key}>
          <span className="lbl">
            <span className="sw-sq" style={{ background: kind.colour }} aria-hidden="true" />
            {kindLabel(kind.key)}
          </span>
          <span className="t-meta">{t("groups.products", { count: items.length })}</span>
          <GainText gain={gainFigure(gainOf(items))} />
          <MaskedMoney text={format.money(assetSum(items))} className="amt t-amt" />
        </li>
      ))}
    </ul>
  );
}
