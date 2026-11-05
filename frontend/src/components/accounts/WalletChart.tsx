"use client";

import { useState } from "react";
import { cn } from "@/lib/utils";
import { GainChip } from "./GainChip";
import { arcPath, pressOnKey, segmentAngles, SEGMENT_GAP } from "./rings";
import { RekeningLogo, Swatch } from "./walletParts";
import { KINDS, assetSum, gainOf, kindInfo, sharesByUse, type AssetUse, type Holding } from "./wallet";
import type { AccountItem, LayoutGrouping } from "./types";
import styles from "./wallet.module.css";

type Money = (value: number) => string;

const percentOf = (share: number) => `${Math.round(share * 100)}%`;

interface FocusGroup {
  key: string;
  title: string | null;
  colour?: string;
  rekening?: AccountItem;
  holdings: Holding[];
}

/** The focus list's groups: by kind for Investasi under Per kegunaan, by rekening under Per rekening (D7). */
function focusGroups(use: AssetUse, holdings: Holding[], grouping: LayoutGrouping): FocusGroup[] {
  if (grouping === "rekening") {
    const order: AccountItem[] = [];
    for (const holding of holdings) if (!order.includes(holding.rekening)) order.push(holding.rekening);
    return order.map((rekening) => ({ key: rekening.id, title: rekening.name, rekening, holdings: holdings.filter((holding) => holding.rekening === rekening) }));
  }
  if (use === "investasi") {
    return KINDS.map((kind) => ({ key: kind.key, title: kind.label, colour: kind.colour, holdings: holdings.filter((holding) => holding.kind === kind.key) }))
      .filter((group) => group.holdings.length);
  }
  return [{ key: use, title: null, holdings }];
}

/** The Grafik tab: a two-ring chart (uses inside, kantong and produk outside) beside the chosen use's list. */
export function WalletChart({
  holdings,
  grouping,
  money,
  colours,
  total,
  onOpen,
}: {
  /** Cards in the order of the current grouping's rows. */
  holdings: Holding[];
  grouping: LayoutGrouping;
  money: Money;
  colours: Map<string, string>;
  total?: number;
  onOpen: (id: string) => void;
}) {
  const shares = sharesByUse(holdings);
  const present = shares.filter((entry) => entry.share > 0);
  const [chosen, setChosen] = useState<AssetUse>("investasi");
  const focus = present.some((entry) => entry.use.key === chosen) ? chosen : present[0]?.use.key ?? chosen;
  const focusUse = shares.find((entry) => entry.use.key === focus)!;
  const assetTotal = present.reduce((sum, entry) => sum + entry.amount, 0);

  const useAngles = segmentAngles(present.map((entry) => entry.share));
  const inner: React.ReactNode[] = [];
  const outer: React.ReactNode[] = [];
  present.forEach((entry, index) => {
    const { start, end } = useAngles[index];
    const dim = entry.use.key !== focus;
    inner.push(
      <path
        key={entry.use.key}
        className={cn(styles.segment, dim && styles.dim)}
        d={arcPath(160, 78, 112, start + SEGMENT_GAP, end - SEGMENT_GAP)}
        fill={entry.use.colour}
        role="button"
        tabIndex={0}
        aria-pressed={!dim}
        aria-label={`${entry.use.label} ${money(entry.amount)}, ${percentOf(entry.share)} aset`}
        onClick={() => setChosen(entry.use.key)}
        onKeyDown={(event) => pressOnKey(event, () => setChosen(entry.use.key))}
      />,
    );
    // The outer ring splits each use into its kantong and produk, in the rows' order.
    const parts = holdings.filter((holding) => holding.role === entry.use.key && holding.amount > 0);
    const partAngles = segmentAngles(parts.map((holding) => holding.amount / assetTotal), start);
    parts.forEach((holding, partIndex) => {
      outer.push(
        <path
          key={holding.id}
          className={cn(dim && styles.dim, !holding.kind && styles.outerPlain)}
          d={arcPath(160, 116, 150, partAngles[partIndex].start + SEGMENT_GAP, partAngles[partIndex].end - SEGMENT_GAP)}
          fill={holding.kind ? kindInfo(holding.kind).colour : entry.use.colour}
        >
          <title>{`${holding.account.name} · ${holding.rekening.name} · ${money(holding.amount)}`}</title>
        </path>,
      );
    });
  });

  const rows = holdings.filter((holding) => holding.role === focus);
  const largest = Math.max(1, ...rows.map((holding) => holding.amount));
  return (
    <section className={cn(styles.panel, styles.chart)} aria-labelledby="wallet-chart-title">
      <div className={styles.ringColumn}>
        <svg className={styles.ring} viewBox="0 0 320 320" role="group" aria-label="Porsi aset, pilih kegunaan">
          {inner}
          <g aria-hidden="true">{outer}</g>
          <text x="160" y="150" textAnchor="middle" className={styles.ringCaption} aria-hidden="true">Aset</text>
          <text x="160" y="174" textAnchor="middle" className={styles.ringTotalLarge} aria-hidden="true">{money(total ?? assetTotal)}</text>
        </svg>
        <div className={styles.ringLegend} role="group" aria-label="Pilih kegunaan">
          {shares.map((entry) => (
            <button key={entry.use.key} type="button" className="ws-select-row" aria-pressed={entry.use.key === focus} disabled={entry.share === 0} onClick={() => setChosen(entry.use.key)}>
              <Swatch colour={entry.use.colour} />
              <b>{entry.use.label}</b>{" "}
              <span>
                {money(entry.amount)} <span className={styles.muted}>{percentOf(entry.share)}</span>
              </span>
            </button>
          ))}
        </div>
      </div>
      <div className={styles.focus}>
        <h2 id="wallet-chart-title" className={styles.focusTitle}>
          <Swatch colour={focusUse.use.colour} />
          {focusUse.use.label}
          {grouping === "rekening" && <span className={styles.railNote}>per rekening</span>}
          <span className={styles.focusTotal}>{money(assetSum(rows))}</span>
        </h2>
        {focusGroups(focus, rows, grouping).map((group) => (
          <div key={group.key}>
            {group.title && (
              <h3 className={styles.focusGroup}>
                <span className={styles.prov}>
                  {group.rekening ? <RekeningLogo rekening={group.rekening} colour={colours.get(group.rekening.id) ?? "var(--muted)"} /> : <Swatch colour={group.colour!} />}
                  {group.title}
                </span>
                <span className={styles.railFigures}>
                  <GainChip gain={gainOf(group.holdings)} money={money} />
                  <span>{money(assetSum(group.holdings))}</span>
                </span>
              </h3>
            )}
            <ul className={styles.focusList}>
              {group.holdings.map((holding) => (
                <li key={holding.id}>
                  <button type="button" className={styles.focusItem} onClick={() => onOpen(holding.account.id)}>
                    <span>
                      <b>{holding.account.name}</b> <span className={styles.muted}>· {holding.rekening.name}</span>
                    </span>
                    <span className={styles.railFigures}>
                      <b>{money(holding.amount)}</b>
                      <GainChip gain={holding.gain} money={money} />
                    </span>
                    <span className={styles.sizeBar} aria-hidden="true">
                      <span style={{ width: `${(Math.max(0, holding.amount) / largest) * 100}%`, background: holding.kind ? kindInfo(holding.kind).colour : focusUse.use.colour }} />
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </section>
  );
}
