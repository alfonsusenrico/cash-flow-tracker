"use client";

import Link from "next/link";
import { compactMoney } from "@/components/analysis/format";
import { Icon, type IconName } from "@/components/ui/Icon";
import { Card, CardHeader } from "@/components/workspace/primitives";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { type ComparisonFact, type ReportBasis, type ReportComparison, refHref } from "@/lib/monthlyReport";
import styles from "./report.module.css";

type Money = (amount: number) => string;

/** "rata-rata 3 bulan" or "rata-rata 2 siklus". */
export function baselineName(comparison: ReportComparison): string | null {
  if (!comparison.baseline) return null;
  return `rata-rata ${comparison.baseline.period_count} ${comparison.basis === "cycle" ? "siklus" : "bulan"}`;
}

/** The previous period's short name for chips: "Sep" for a month, the cycle label otherwise. */
export function previousShort(comparison: ReportComparison): string | null {
  const previous = comparison.previous;
  if (!previous) return null;
  if (comparison.basis === "month") return previous.label.split(" ")[0].slice(0, 3);
  return previous.label;
}

/**
 * A signed change with its arrow. A rise of spending is bad, a rise of income or saving is good (D11).
 * The sign and the arrow carry the meaning; colour only repeats it.
 */
export function Change({ change, kind = "spending" }: { change: number | null; kind?: "spending" | "income" }) {
  if (change === null) return null;
  if (change === 0) return <span className={styles.change} data-tone="neutral">sama</span>;
  const up = change > 0;
  const bad = kind === "spending" ? up : !up;
  return (
    <span className={styles.change} data-tone={`${up ? "up" : "down"}-${bad ? "bad" : "good"}`}>
      {up ? "▲" : "▼"} {up ? "+" : "−"}
      <MoneyDigits amount={Math.abs(change)} />
    </span>
  );
}

function MoneyDigits({ amount }: { amount: number }) {
  return <>{new Intl.NumberFormat("id-ID").format(amount)}</>;
}

/** The change chip beside a KPI: "▲ Rp 1.583.500 vs Sep", or "belum lengkap" for a partial previous period. */
export function ChangeLine({ change, against, money, kind = "spending", partial = false }: {
  change: number | null; against: string; money: Money; kind?: "spending" | "income"; partial?: boolean;
}) {
  if (partial) return <span className={styles.chipLine}><span className={styles.change} data-tone="neutral">belum lengkap</span> vs {against}</span>;
  if (change === null) return null;
  if (change === 0) return <span className={styles.chipLine}><span className={styles.change} data-tone="neutral">sama</span> dengan {against}</span>;
  const up = change > 0;
  const bad = kind === "spending" ? up : !up;
  return (
    <span className={styles.chipLine}>
      <span className={styles.change} data-tone={`${up ? "up" : "down"}-${bad ? "bad" : "good"}`}>
        {up ? "▲" : "▼"} {money(Math.abs(change))}
      </span>
      vs {against}
    </span>
  );
}

/** "30% dari total Oktober 2026" with a bar; no arrow and no verdict (D4). */
export function Progress({ share, label }: { share: number | null; label: string | null }) {
  if (share === null || !label) return null;
  return (
    <div className={styles.progress}>
      <div className={styles.progressTrack} aria-hidden="true">
        <div className={styles.progressFill} style={{ width: `${Math.min(100, share)}%` }} />
      </div>
      <span className={styles.progressText}>{share}% dari total {label}</span>
    </div>
  );
}

function Amount({ href, money, amount, label }: { href: string; money: Money; amount: number; label: string }) {
  return (
    <Link href={href} className={styles.amountLink} aria-label={`${money(amount)}, lihat transaksi ${label}`}>
      {money(amount)}
    </Link>
  );
}

/** "Pengeluaran per pilar dan kategori": pillar rows, then their categories sorted by amount (D8). */
export function PillarTable({ comparison, money }: { comparison: ReportComparison; money: Money }) {
  const running = comparison.running;
  const previous = previousShort(comparison);
  const progressByPillar = new Map((comparison.progress?.pillars ?? []).map((item) => [item.pillar, item]));
  const lastHeading = running
    ? comparison.progress?.previous_label ? `dari ${previousShort(comparison)}` : null
    : previous ? `vs ${previous}` : null;
  return (
    <Card labelledBy="report-pillars">
      <CardHeader id="report-pillars" icon="layers" title="Pengeluaran per pilar dan kategori" caption={money(comparison.totals.spending)} />
      {comparison.pillars.length === 0 ? (
        <p className={styles.progressText}>Belum ada pengeluaran pada periode ini.</p>
      ) : (
        <table className={styles.table}>
          <caption className={styles.srOnly}>Pengeluaran {comparison.period.label} per pilar Kakeibo dan kategori</caption>
          <thead>
            <tr>
              <th scope="col">Pilar dan kategori</th>
              <th scope="col" className="num">Jumlah</th>
              <th scope="col" className={`num ${styles.hideNarrow}`}>Porsi</th>
              {lastHeading && <th scope="col" className="num">{lastHeading}</th>}
            </tr>
          </thead>
          <tbody>
            {comparison.pillars.map((pillar) => {
              const info = kakeiboBucketInfo(pillar.pillar);
              const name = info?.label ?? pillar.pillar;
              const progress = progressByPillar.get(pillar.pillar);
              return [
                <tr key={pillar.pillar} className={styles.pillarRow}>
                  <th scope="row" style={{ textAlign: "left", fontWeight: 600, padding: 8 }}>
                    <span aria-hidden="true" className={styles.dot} style={{ background: info?.color }} />
                    {name}
                  </th>
                  <td className="num"><Amount href={refHref(pillar.ref)} money={money} amount={pillar.amount} label={name} /></td>
                  <td className={`num ${styles.hideNarrow}`}>{pillar.share !== null ? `${pillar.share}%` : ""}</td>
                  {lastHeading && (
                    <td className="num">
                      {running
                        ? progress?.share !== null && progress?.share !== undefined ? <span className={styles.progressText}>{progress.share}%</span> : null
                        : comparison.previous?.partial ? <span className={styles.change} data-tone="neutral">belum lengkap</span> : <Change change={pillar.change} />}
                    </td>
                  )}
                </tr>,
                ...pillar.categories.map((category) => (
                  <tr key={`${pillar.pillar}-${category.category_id ?? "none"}`} className={styles.categoryRow}>
                    <td>{category.name}</td>
                    <td className="num"><Amount href={refHref(category.ref)} money={money} amount={category.amount} label={category.name} /></td>
                    <td className={`num ${styles.share} ${styles.hideNarrow}`}>{category.share !== null ? `${category.share}%` : ""}</td>
                    {lastHeading && <td className="num">{running ? null : <Change change={category.change} />}</td>}
                  </tr>
                )),
              ];
            })}
          </tbody>
        </table>
      )}
    </Card>
  );
}

const FACT_ICON: Record<ComparisonFact["kind"], IconName> = {
  category_above_average: "trending-up",
  category_new: "sparkles",
  want_share: "buckets",
  top_merchant: "receipt",
  recurring: "repeat",
  unexpected: "alert-triangle",
};

/** "Yang bisa diperbaiki", or "Sejauh ini" while the period runs (D6, OD88). */
export function ImprovementFacts({ comparison, money }: { comparison: ReportComparison; money: Money }) {
  const title = comparison.running ? "Sejauh ini" : "Yang bisa diperbaiki";
  return (
    <Card labelledBy="report-facts">
      <CardHeader id="report-facts" icon="target" title={title} caption={comparison.period.label} />
      {comparison.facts.length === 0 ? (
        <p className={styles.progressText}>Belum ada yang menonjol pada periode ini.</p>
      ) : (
        <ul className={styles.facts}>
          {comparison.facts.map((fact) => {
            const figure = fact.difference !== undefined && fact.kind === "category_above_average"
              ? `+${money(fact.difference)}`
              : fact.kind === "recurring" ? `${money(fact.amount)}/bln` : money(fact.amount);
            return (
              <li key={`${fact.kind}-${fact.label}`} className={styles.fact}>
                <span className={styles.factIcon} aria-hidden="true"><Icon name={FACT_ICON[fact.kind]} /></span>
                <span className={styles.factLabel}>{fact.label}</span>
                <span className={styles.factSide}>
                  <span className={styles.factFigure}>{figure}</span>
                  {fact.ref ? (
                    <Link className={styles.factLink} href={refHref(fact.ref)} aria-label={`Lihat transaksi: ${fact.label}`}>Lihat transaksi</Link>
                  ) : fact.kind === "recurring" ? (
                    <Link className={styles.factLink} href="/insights?tab=ringkasan" aria-label="Lihat daftar tagihan dan langganan berulang">Lihat daftar</Link>
                  ) : null}
                </span>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}

const STACK_ORDER = ["need", "want", "culture", "unexpected", "unclassified"] as const;

/** "Pengeluaran per bulan": stacked pillar bars, the viewed period outlined, the running one marked (D8, OD93). */
export function MonthlyHistory({ comparison, money, basis }: { comparison: ReportComparison; money: Money; basis: ReportBasis }) {
  const history = comparison.history;
  if (history.length === 0) return null;
  const peak = Math.max(1, ...history.map((item) => item.spending));
  const unit = basis === "cycle" ? "siklus" : "bulan";
  const average = comparison.baseline ? `${baselineName(comparison)} ${money(comparison.baseline.totals.spending)}` : null;
  return (
    <Card labelledBy="report-history">
      <CardHeader id="report-history" icon="analysis" title={`Pengeluaran per ${unit}`} caption={average ?? undefined} />
      <div className={styles.bars} aria-hidden="true">
        {history.map((item) => (
          <div key={item.start} className={styles.bar}>
            <span className={styles.barValue}>{compactMoney(item.spending, money)}</span>
            <div
              className={styles.barStack}
              data-viewed={item.viewed}
              data-running={!item.complete}
              style={{ height: `${Math.max(2, Math.round((item.spending / peak) * 100))}%` }}
            >
              {STACK_ORDER.map((pillar) => {
                const amount = item.pillars[pillar] ?? 0;
                if (amount <= 0 || item.spending <= 0) return null;
                return <span key={pillar} style={{ height: `${(amount / item.spending) * 100}%`, background: kakeiboBucketInfo(pillar)?.color }} />;
              })}
            </div>
            <span className={styles.barLabel} data-viewed={item.viewed}>
              {basis === "month" ? item.label.split(" ")[0].slice(0, 3) : item.label.replace("Siklus ", "")}
              {!item.complete && <><br />berjalan</>}
            </span>
          </div>
        ))}
      </div>
      <ul className={styles.srOnly}>
        {history.map((item) => (
          <li key={item.start}>{item.label}{item.complete ? "" : " (berjalan)"}: {money(item.spending)}</li>
        ))}
      </ul>
      <p className={styles.progressText} style={{ marginTop: 10 }}>
        {unit === "bulan" ? "Bulan" : "Siklus"} berjalan tidak dibandingkan sampai {unit}nya selesai.
      </p>
    </Card>
  );
}

/** "Belum ada rata-rata": shown where a baseline would sit but too few whole periods exist (D2). */
export function NoBaseline({ basis }: { basis: ReportBasis }) {
  return <span className={styles.chipLine}>Rata-rata muncul setelah 2 {basis === "cycle" ? "siklus" : "bulan"} lengkap</span>;
}
