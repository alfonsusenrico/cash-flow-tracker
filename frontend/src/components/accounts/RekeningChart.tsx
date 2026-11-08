"use client";

import { useState } from "react";
import { GainText } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { cn } from "@/lib/utils";
import { useAccountText } from "./accountText";
import { arcPath, segmentAngles, SEGMENT_GAP } from "./rings";
import { InstitutionMark } from "./Tile";
import { KINDS, assetSum, gainOf, sharesByUse, type AssetUse, type Gain, type Holding } from "./wallet";
import type { AccountItem, LayoutGrouping } from "./types";

const gainFigure = (gain: Gain | null) => (gain ? (gain.known ? { amount: gain.gain, cost: gain.cost } : ("unknown" as const)) : null);

interface Bar {
  key: string;
  label: string;
  amount: number;
  colour: string;
  /** A gain for products and kinds; otherwise the share of assets is shown. */
  gain?: Gain | null;
  onOpen?: () => void;
}

/**
 * Grafik (D11): the owner's ring of the four asset uses as a selector beside its legend, which carries every value,
 * then the chosen use's contents as horizontal bars with their gains. The ring is decorative; the legend is its
 * accessible form. The outer ring of kantong and products is gone: their sizes are the bars.
 */
export function RekeningChart({
  holdings,
  grouping,
  colours,
  totalAssets,
  onOpen,
}: {
  holdings: Holding[];
  grouping: LayoutGrouping;
  colours: Map<string, string>;
  totalAssets?: number;
  onOpen: (id: string) => void;
}) {
  const { t, format, roleLabel, kindLabel } = useAccountText();
  const money = useMoney();
  const shares = sharesByUse(holdings);
  const hasProducts = holdings.some((holding) => holding.role === "investasi" && holding.gain);
  const largest = [...shares].sort((left, right) => right.amount - left.amount)[0]?.use.key ?? "harian";
  const [chosen, setChosen] = useState<AssetUse | null>(null);
  const active: AssetUse = chosen ?? (hasProducts ? "investasi" : largest);
  const ringTotal = totalAssets ?? shares.reduce((sum, entry) => sum + Math.max(0, entry.amount), 0);
  const drawn = shares.filter((entry) => entry.share > 0);
  const angles = segmentAngles(drawn.map((entry) => entry.share));
  const percent = (share: number) => `${Math.round(share * 100)}%`;
  const assetTotal = shares.reduce((sum, entry) => sum + Math.max(0, entry.amount), 0);
  const shareOf = (amount: number) => t("grafik.shareOfAssets", { percent: assetTotal > 0 ? percent(Math.max(0, amount) / assetTotal) : "0%" });

  const inUse = holdings.filter((holding) => holding.role === active);
  const productBar = (holding: Holding): Bar => ({
    key: holding.id,
    label: holding.ownBalance ? `${holding.rekening.name} · ${t("tile.outsidePockets")}` : holding.account.name,
    amount: holding.amount,
    colour: active === "investasi" ? "var(--inv-solid)" : shares.find((entry) => entry.use.key === active)!.use.colour,
    gain: active === "investasi" ? holding.gain : undefined,
    onOpen: () => onOpen(holding.account.id),
  });
  const byRekening = () => {
    const order: AccountItem[] = [];
    for (const holding of inUse) if (!order.includes(holding.rekening)) order.push(holding.rekening);
    return order.map((rekening) => ({ rekening, holdings: inUse.filter((holding) => holding.rekening === rekening) }));
  };

  return (
    <div className="rk-grafik">
      <section className="rk-bars-section" aria-labelledby="rk-grafik-use">
        <SectionHeader id="rk-grafik-use" label={t("grafik.byUse")} />
        <div className="rk-chart">
          <svg className="rk-ring" viewBox="0 0 200 200" aria-hidden="true">
            {drawn.map((entry, index) => (
              <path
                key={entry.use.key}
                d={arcPath(100, 72, 100, angles[index].start + SEGMENT_GAP, angles[index].end - SEGMENT_GAP)}
                fill={entry.use.colour}
                opacity={entry.use.key === active ? 1 : 0.45}
              />
            ))}
            <text x="100" y="94" textAnchor="middle" className="cap">{t("grafik.assets")}</text>
            <text x="100" y="116" textAnchor="middle" className="tot t-amt">
              {money(ringTotal)}
            </text>
          </svg>
          <div className="rk-legend" role="group" aria-label={t("grafik.pick")}>
            {shares.map((entry) => (
              <button
                key={entry.use.key}
                type="button"
                className="ws-select-row" aria-pressed={entry.use.key === active}
                disabled={entry.amount <= 0}
                onClick={() => setChosen(entry.use.key)}
              >
                <span className="sw-sq" style={{ background: entry.use.colour }} aria-hidden="true" />
                <span className="lbl">{roleLabel(entry.use.key)}</span>
                <MaskedMoney text={format.money(entry.amount)} className="amt t-amt" />
                <span className="t-meta">{t("grafik.shareOfAssets", { percent: percent(entry.share) })}</span>
              </button>
            ))}
          </div>
        </div>
      </section>
      {active === "investasi" ? (
        <>
          <BarsSection
            id="rk-grafik-kinds"
            label={t("grafik.perKind")}
            aside={<GainText gain={gainFigure(gainOf(inUse))} />}
            bars={KINDS.map((kind) => {
              const items = inUse.filter((holding) => holding.kind === kind.key);
              return { key: kind.key, label: kindLabel(kind.key), amount: assetSum(items), colour: "var(--inv-solid)", gain: gainOf(items), items };
            }).filter((bar) => bar.items.length)}
            shareOf={shareOf}
          />
          {grouping === "rekening" ? (
            <GroupedBars id="rk-grafik-products" label={t("grafik.perProduct")} groups={byRekening()} colours={colours} toBar={productBar} shareOf={shareOf} unknownLabel={t("tile.costIncomplete")} />
          ) : (
            <BarsSection id="rk-grafik-products" label={t("grafik.perProduct")} bars={inUse.map(productBar)} shareOf={shareOf} />
          )}
        </>
      ) : grouping === "rekening" ? (
        <GroupedBars id="rk-grafik-contents" label={t("grafik.contents", { use: roleLabel(active) })} groups={byRekening()} colours={colours} toBar={productBar} shareOf={shareOf} />
      ) : (
        <BarsSection id="rk-grafik-contents" label={t("grafik.contents", { use: roleLabel(active) })} bars={inUse.map(productBar)} shareOf={shareOf} />
      )}
    </div>
  );
}

function BarsSection({ id, label, aside, bars, shareOf }: { id: string; label: string; aside?: React.ReactNode; bars: Bar[]; shareOf: (amount: number) => string }) {
  const { t } = useAccountText();
  return (
    <section className="rk-bars-section" aria-labelledby={id}>
      <SectionHeader id={id} label={label} aside={aside} />
      {bars.length ? <BarList bars={bars} shareOf={shareOf} /> : <p className="t-meta">{t("grafik.empty")}</p>}
    </section>
  );
}

/** Products or kantong grouped under each rekening: a quiet label, its colour once, its total and gain, a 2 px rail. */
function GroupedBars({
  id, label, groups, colours, toBar, shareOf, unknownLabel,
}: {
  id: string;
  label: string;
  groups: { rekening: AccountItem; holdings: Holding[] }[];
  colours: Map<string, string>;
  toBar: (holding: Holding) => Bar;
  shareOf: (amount: number) => string;
  unknownLabel?: string;
}) {
  const { format } = useAccountText();
  return (
    <section className="rk-bars-section" aria-labelledby={id}>
      <SectionHeader id={id} label={label} />
      <div className="rk-bars">
        {groups.map(({ rekening, holdings }) => {
          const colour = colours.get(rekening.id) ?? "var(--line-strong)";
          return (
            <div key={rekening.id} className="ws-group-rail" style={{ "--rail": colour } as React.CSSProperties}>
              <div className="rk-bar-label">
                <InstitutionMark name={rekening.name} colour={colour} />
                <span className="t-label grow">{rekening.name}</span>
                <GainText gain={gainFigure(gainOf(holdings))} unknownLabel={unknownLabel} />
                <MaskedMoney text={format.money(assetSum(holdings))} className="t-meta t-amt" />
              </div>
              <BarList bars={holdings.map(toBar)} shareOf={shareOf} />
            </div>
          );
        })}
      </div>
    </section>
  );
}

function BarList({ bars, shareOf }: { bars: Bar[]; shareOf: (amount: number) => string }) {
  const { format } = useAccountText();
  const longest = Math.max(1, ...bars.map((bar) => bar.amount));
  return (
    <ul className="rk-bars">
      {bars.map((bar) => {
        const content = (
          <>
            <span className="lbl">{bar.label}</span>
            <span className="bar" aria-hidden="true">
              {bar.amount > 0 && <span style={{ width: `${(bar.amount / longest) * 100}%`, background: bar.colour }} />}
            </span>
            <MaskedMoney text={format.money(bar.amount)} className="amt t-amt" />
            {bar.gain !== undefined ? (
              <span className="side">
                <GainText gain={gainFigure(bar.gain)} />
              </span>
            ) : (
              <span className="side is-share t-meta">{shareOf(bar.amount)}</span>
            )}
          </>
        );
        return (
          <li key={bar.key}>
            {bar.onOpen ? (
              <button type="button" className="hbar" onClick={bar.onOpen}>{content}</button>
            ) : (
              <div className={cn("hbar")}>{content}</div>
            )}
          </li>
        );
      })}
    </ul>
  );
}
