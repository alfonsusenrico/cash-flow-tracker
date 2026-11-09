"use client";

import Link from "next/link";
import { useTranslations } from "next-intl";
import { ChangeFigure } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { type ReportComparison, type SpendingBucket, refHref } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";

type Money = (amount: number) => string;

type PillarTerm = "needs" | "wants" | "culture" | "unexpected" | "savings" | "noPillarYet";

export const PILLAR_TERM: Record<SpendingBucket | "saving", PillarTerm> = {
  need: "needs", want: "wants", culture: "culture", unexpected: "unexpected", saving: "savings", unclassified: "noPillarYet",
};

export const PILLAR_COLOR: Record<SpendingBucket | "saving", string> = {
  need: "var(--k-need)", want: "var(--k-want)", culture: "var(--k-culture)", unexpected: "var(--k-unexpected)", saving: "var(--k-saving)",
  unclassified: "var(--k-none)",
};

/** The 8 px pillar mark; "Belum diklasifikasi" is a dashed ring (tactile-pulse-ui "Kakeibo Pillar Marks"). */
export function PillarDot({ pillar }: { pillar: SpendingBucket | "saving" }) {
  return <span className={cn("dot is-square", pillar === "unclassified" && "is-unclassified")} style={pillar === "unclassified" ? undefined : { background: PILLAR_COLOR[pillar] }} aria-hidden="true" />;
}

/**
 * "Kakeibo · per pilar" (apply-markas-analisis D6): amount, share of Pengeluaran and, while running, the previous
 * period's amount; once complete, the change against the average. Each row opens the ledger for its pillar and
 * window; "Belum diklasifikasi" keeps its "Perlu dipilih" beside the amount link.
 */
export function KakeiboRows({ comparison, money }: { comparison: ReportComparison; money: Money }) {
  const t = useTranslations("analysis.kakeibo");
  const terms = useTranslations("terms");
  const format = useFormat();
  const { running, progress, baseline, previous } = comparison;
  const mode: "running" | "complete" | "basic" = running && previous && !previous.partial ? "running" : !running && baseline ? "complete" : "basic";
  const previousOf = new Map((progress?.pillars ?? []).map((pillar) => [pillar.pillar, pillar.previous]));
  const rows = comparison.pillars.filter((pillar) => {
    const other = mode === "running" ? previousOf.get(pillar.pillar) ?? 0 : mode === "complete" ? pillar.baseline ?? 0 : 0;
    return pillar.amount > 0 || other > 0;
  });
  const aside = mode === "running" ? t("asideRunning") : mode === "complete" ? t("asideComplete") : t("asideBasic");
  const classifyHref = `/laporan?lengkapi=1&basis=${comparison.basis}&start=${comparison.period.start_date}`;

  return (
    <section aria-labelledby="an-kakeibo">
      <SectionHeader id="an-kakeibo" label={t("title")} aside={<span className="aside">{aside}</span>} />
      <div className="cmp">
        {rows.map((pillar) => {
          const name = terms(PILLAR_TERM[pillar.pillar]);
          const third = mode === "running" ? (
            <MaskedMoney text={money(previousOf.get(pillar.pillar) ?? 0)} className="t-meta cmp-num ph-hide" />
          ) : mode === "complete" ? (
            <span className="cmp-num ph-hide">{pillar.baseline_change === null ? <span className="t-meta">—</span> : <ChangeFigure change={pillar.baseline_change} money={money} />}</span>
          ) : (
            <span className="ph-hide" aria-hidden="true" />
          );
          const share = <span className="t-meta cmp-num">{pillar.share === null ? "—" : `${format.integer(pillar.share)}%`}</span>;
          if (pillar.pillar === "unclassified") {
            return (
              <div key={pillar.pillar} className="cmp-row is-pillar">
                <span className="t-body kk-name">
                  <PillarDot pillar={pillar.pillar} />
                  <span>{name}</span>
                  {pillar.amount > 0 && <Link href={classifyHref} className="btn-link kk-fix">{t("needsPillar")}</Link>}
                </span>
                <Link href={refHref(pillar.ref)} className="cmp-num kk-amount-link">
                  <MaskedMoney text={money(pillar.amount)} className="t-amt" />
                </Link>
                {share}
                {third}
              </div>
            );
          }
          return (
            <Link key={pillar.pillar} href={refHref(pillar.ref)} className="cmp-row is-pillar is-link">
              <span className="t-body kk-name">
                <PillarDot pillar={pillar.pillar} />
                <span>{name}</span>
              </span>
              <MaskedMoney text={money(pillar.amount)} className="t-amt cmp-num" />
              {share}
              {third}
            </Link>
          );
        })}
      </div>
    </section>
  );
}
