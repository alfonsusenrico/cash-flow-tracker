"use client";

import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { ExplainableFigure, type ExplanationData } from "@/components/workspace/explain";
import { cn } from "@/lib/utils";
import { GainChip } from "./GainChip";
import { arcPath, pressOnKey, segmentAngles, SEGMENT_GAP } from "./rings";
import { RekeningLogo, Swatch } from "./walletParts";
import { USES, assetSum, gainOf, sharesByUse, type Holding, type Section } from "./wallet";
import type { AssetUse } from "./wallet";
import type { LayoutGrouping, NetWorthResponse } from "./types";
import styles from "./wallet.module.css";

type Money = (value: number) => string;

interface NetWorthQuery {
  data?: NetWorthResponse;
  isLoading: boolean;
  isError: boolean;
  refetch: () => unknown;
}

function lineLabel(line: { key: string; label: string }): string {
  if (line.key === "liquid") return "Saldo tersedia";
  if (line.key === "dana_investasi") return "Dana investasi";
  if (line.key === "investment") return "Investasi";
  if (line.key.startsWith("credit:")) return `Utang ${line.label}`;
  return line.label;
}

const percentOf = (share: number) => `${Math.round(share * 100)}%`;

/** Kekayaan bersih and its parts from the server, the gain over products, and the share of each use (D7). */
export function WalletSummary({
  netWorth,
  holdings,
  grouping,
  money,
  colours,
  rekeningSections,
  side,
  onJump,
}: {
  netWorth: NetWorthQuery;
  holdings: Holding[];
  grouping: LayoutGrouping;
  money: Money;
  colours: Map<string, string>;
  /** The shown rows; their rekening composition is drawn under Per rekening. */
  rekeningSections: Section[];
  side: boolean;
  onJump: (use: AssetUse) => void;
}) {
  const shares = sharesByUse(holdings);
  return (
    <section className={cn(styles.panel, styles.summary, !side && styles.summaryAlone)} aria-label="Ringkasan">
      <div className={styles.summaryMain}>
        <Figures netWorth={netWorth} holdings={holdings} money={money} />
        <div className={styles.shareBar} aria-hidden="true">
          {shares.filter((entry) => entry.share > 0).map((entry) => (
            <span key={entry.use.key} style={{ flexGrow: entry.share, background: entry.use.colour }} />
          ))}
        </div>
        <ul className={styles.legend} aria-label="Porsi aset per kegunaan">
          {shares.map((entry) => (
            <li key={entry.use.key}>
              <Swatch colour={entry.use.colour} />
              {entry.use.label} <b>{percentOf(entry.share)}</b>
            </li>
          ))}
        </ul>
      </div>
      {side && (
        <div className={styles.summarySide}>
          {grouping === "use" ? (
            <MiniRing holdings={holdings} money={money} total={netWorth.data?.total_assets} onJump={onJump} />
          ) : (
            <RekeningComposition sections={rekeningSections} colours={colours} money={money} />
          )}
        </div>
      )}
    </section>
  );
}

function Figures({ netWorth, holdings, money }: { netWorth: NetWorthQuery; holdings: Holding[]; money: Money }) {
  if (netWorth.isLoading) return <Skeleton style={{ height: 96 }} />;
  const data = netWorth.data;
  if (netWorth.isError || !data) return <ErrorState message="Kekayaan bersih belum dapat dihitung." onRetry={() => netWorth.refetch()} />;
  const explanation: ExplanationData = {
    title: "Kekayaan bersih",
    rule: "Saldo tersedia, Dana investasi dan Investasi, dikurangi utang dan saldo minus yang perlu dicocokkan. Saldo kantong dihitung sekali. Angka yang sama dengan Beranda.",
    lines: (data.lines ?? []).map((line) => ({ label: lineLabel(line), amount: money(line.amount) })),
    total: { label: "Kekayaan bersih", amount: money(data.net_worth) },
  };
  const gain = gainOf(holdings);
  const debt = data.total_liabilities;
  return (
    <>
      <div className={styles.label}>Kekayaan bersih</div>
      <div className={styles.big}>
        <ExplainableFigure label="Kekayaan bersih" amountText={money(data.net_worth)} explanation={explanation} />
      </div>
      <dl className={styles.kpis}>
        <div>
          <dt>Saldo tersedia</dt>
          <dd>{money(data.liquid)}</dd>
        </div>
        <div>
          <dt>Dana investasi</dt>
          <dd>{money(data.dana_investasi ?? 0)}</dd>
        </div>
        <div>
          <dt>Investasi</dt>
          <dd>{money(data.investment)}</dd>
        </div>
        <div>
          <dt>Untung/Rugi</dt>
          <dd>{gain ? <GainChip gain={gain} money={money} /> : <span className={styles.muted}>Belum ada investasi</span>}</dd>
        </div>
        <div>
          <dt>Utang</dt>
          <dd className={debt > 0 ? styles.debtFigure : undefined}>{debt > 0 ? `−${money(debt)}` : money(0)}</dd>
        </div>
      </dl>
    </>
  );
}

/** The small ring of uses; a segment scrolls to its row (Per kegunaan). */
function MiniRing({ holdings, money, total, onJump }: { holdings: Holding[]; money: Money; total?: number; onJump: (use: AssetUse) => void }) {
  const shares = sharesByUse(holdings).filter((entry) => entry.share > 0);
  const angles = segmentAngles(shares.map((entry) => entry.share));
  const segments = shares.map((entry, index) => ({
    ...entry,
    path: arcPath(120, 72, 108, angles[index].start + SEGMENT_GAP, angles[index].end - SEGMENT_GAP),
  }));
  const centre = total ?? shares.reduce((sum, entry) => sum + entry.amount, 0);
  return (
    <svg className={styles.miniRing} viewBox="0 0 240 240" role="group" aria-label="Porsi aset per kegunaan">
      {segments.map((segment) => (
        <path
          key={segment.use.key}
          className={styles.segment}
          d={segment.path}
          fill={segment.use.colour}
          role="button"
          tabIndex={0}
          aria-label={`${segment.use.label} ${money(segment.amount)}, ${percentOf(segment.share)} aset, lompat ke baris`}
          onClick={() => onJump(segment.use.key)}
          onKeyDown={(event) => pressOnKey(event, () => onJump(segment.use.key))}
        >
          <title>{`${segment.use.label} ${money(segment.amount)} · ${percentOf(segment.share)} aset`}</title>
        </path>
      ))}
      <text x="120" y="114" textAnchor="middle" className={styles.ringCaption} aria-hidden="true">Aset</text>
      <text x="120" y="136" textAnchor="middle" className={styles.ringTotal} aria-hidden="true">{money(centre)}</text>
    </svg>
  );
}

/** Each rekening's assets as one bar, split by use, longest first in the rows' order (Per rekening). */
function RekeningComposition({ sections, colours, money }: { sections: Section[]; colours: Map<string, string>; money: Money }) {
  const rows = sections
    .map((section) => ({ section, total: assetSum(section.holdings) }))
    .filter((row) => row.total > 0 && row.section.rekening);
  const longest = Math.max(1, ...rows.map((row) => row.total));
  return (
    <ul className={styles.compo} aria-label="Isi tiap rekening">
      {rows.map(({ section, total }) => {
        const parts = USES.map((use) => ({ use, amount: assetSum(section.holdings.filter((holding) => holding.role === use.key)) })).filter((part) => part.amount > 0);
        const described = parts.map((part) => `${part.use.label} ${percentOf(part.amount / total)}`).join(", ");
        return (
          <li key={section.key}>
            <span className={styles.prov}>
              <RekeningLogo rekening={section.rekening!} colour={colours.get(section.key) ?? "var(--muted)"} />
              {section.title}
            </span>
            <span className={styles.stack} style={{ width: `${(total / longest) * 100}%` }} role="img" aria-label={`Isi ${section.title}: ${described}`}>
              {parts.map((part) => (
                <span key={part.use.key} style={{ flexGrow: part.amount, background: part.use.colour }} />
              ))}
            </span>
            <span className={styles.compoTotal}>{money(total)}</span>
          </li>
        );
      })}
    </ul>
  );
}
