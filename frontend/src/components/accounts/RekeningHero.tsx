"use client";

import { useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { ExplanationDialog, type RincianData } from "@/components/workspace/ExplanationDialog";
import { HeroFigure, SplitBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { useAccountText } from "./accountText";
import { sharesByUse, type Holding } from "./wallet";
import type { NetWorthResponse } from "./types";

interface NetWorthQuery {
  data?: NetWorthResponse;
  isLoading: boolean;
  isError: boolean;
  refetch: () => unknown;
}

const HERO_ID = "rk-hero";

/** The hero while the accounts load (D14): label, figure, line and bar placeholders, marked busy. */
export function RekeningHeroSkeleton() {
  return (
    <section className="hero hero-skel" aria-busy="true" aria-labelledby={HERO_ID}>
      <h2 id={HERO_ID} className="sr-only">
        {useTranslations("terms")("netWorth")}
      </h2>
      <Skeleton style={{ width: 96, height: 12 }} />
      <Skeleton style={{ width: 240, height: 32, marginTop: 12 }} />
      <Skeleton style={{ width: 160, height: 12, marginTop: 12 }} />
      <Skeleton style={{ width: "100%", height: 8, marginTop: 24 }} />
    </section>
  );
}

/**
 * "Kekayaan bersih" (D3): the server's `net_worth` in the page's one 44 px figure, the page's one "Rincian", the
 * line "dari aset {total_assets}" and the split bar of the four asset uses with its % legend. The shares are
 * today's: each use's sum of the account list over the positive asset total. When the net worth request fails
 * the hero is an error box with a retry, and no Rp 0 is shown.
 */
export function RekeningHero({ netWorth, holdings }: { netWorth: NetWorthQuery; holdings: Holding[] }) {
  const { t, format, roleLabel } = useAccountText();
  const terms = useTranslations("terms");
  const [open, setOpen] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);
  const data = netWorth.data;

  if (netWorth.isLoading) return <RekeningHeroSkeleton />;
  if (netWorth.isError || !data) {
    return (
      <section className="hero" aria-labelledby={HERO_ID}>
        <h2 id={HERO_ID} className="t-label">{terms("netWorth")}</h2>
        <ErrorState message={t("hero.failed")} onRetry={() => netWorth.refetch()} />
      </section>
    );
  }

  const shares = sharesByUse(holdings);
  const total = shares.reduce((sum, entry) => sum + Math.max(0, entry.amount), 0);
  const percent = (share: number) => `${Math.round(share * 100)}%`;
  const lineLabel = (line: { key: string; label: string }) => {
    if (line.key === "liquid") return t("rincian.liquid");
    if (line.key === "dana_investasi") return terms("investmentCash");
    if (line.key === "investment") return terms("investments");
    if (line.key === "reconciliation_pending") return terms("negativeBalanceToMatch");
    if (line.key.startsWith("debt:")) return t("rincian.billLine", { name: line.label });
    if (line.key.startsWith("credit:")) return t("rincian.debtLine", { name: line.label });
    return line.label;
  };
  const signed = (amount: number) => (amount < 0 ? `−${format.money(-amount)}` : format.money(amount));
  const hasObligations = (data.lines ?? []).some((line) => line.key.startsWith("debt:"));
  const rincian: RincianData = {
    title: terms("netWorth"),
    rule: t("rincian.rule"),
    lines: (data.lines ?? []).map((line) => ({ key: line.key, label: lineLabel(line), amount: signed(line.amount) })),
    total: { label: terms("netWorth"), amount: signed(data.net_worth) },
    notes: [],
    link: hasObligations ? { href: "/goals", label: t("rincian.openGoals") } : undefined,
  };

  return (
    <>
      <HeroFigure
        id={HERO_ID}
        label={terms("netWorth")}
        action={
          <button ref={opener} type="button" className="ws-btn ws-btn-quiet ws-btn-sm" aria-haspopup="dialog" onClick={() => setOpen(true)}>
            {terms("breakdown")}
          </button>
        }
        figure={<MaskedMoney text={signed(data.net_worth)} />}
        line={t.rich("hero.fromAssets", { amount: () => <MaskedMoney text={format.money(data.total_assets)} /> })}
      >
        <SplitBar
          label={t("hero.splitLabel")}
          emptyLabel={t("hero.splitEmpty")}
          total={total}
          segments={shares.map((entry) => ({ key: entry.use.key, label: roleLabel(entry.use.key), amount: Math.max(0, entry.amount), color: entry.use.colour }))}
        />
        {total > 0 && (
          <ul className="chart-foot" aria-label={t("hero.legend")}>
            {shares.filter((entry) => entry.amount > 0).map((entry) => (
              <li key={entry.use.key}>
                <span className="sw-sq" style={{ background: entry.use.colour }} aria-hidden="true" />
                {roleLabel(entry.use.key)} <b>{percent(entry.share)}</b>
              </li>
            ))}
          </ul>
        )}
      </HeroFigure>
      <ExplanationDialog
        open={open}
        data={rincian}
        onClose={() => {
          setOpen(false);
          requestAnimationFrame(() => opener.current?.focus());
        }}
      />
    </>
  );
}
