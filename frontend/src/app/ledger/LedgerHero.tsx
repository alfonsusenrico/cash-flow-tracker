"use client";

import { useTranslations } from "next-intl";
import { FigureBlock, HeroFigure } from "@/components/workspace/figures";
import { maskedTag } from "@/components/transactions/rows";
import { MaskedMoney } from "@/components/workspace/money";
import type { LedgerTypeFilter } from "@/lib/ledgerFilters";

/** The summary block of `GET /transactions` (`summary.period` or `summary.cumulative`); every figure is the server's. */
export interface CashFlowMetrics {
  inflow: number;
  outflow: number;
  net: number;
  /** Pengeluaran and the saving-pillar part of the outflow; `spending + saving = outflow`. Absent on an older API. */
  spending?: number;
  saving?: number;
  /** Masuk less Pengeluaran: Transaksi's "Bersih". */
  spending_net?: number;
  gross_outflow?: number;
  reimbursement_offset?: number;
  /** "Ditabung & investasi" for the window, only for the savings type without other row filters (D28). */
  saving_net?: number | null;
}

function signed(amount: number, money: (amount: number) => string): string {
  if (amount > 0) return `+${money(amount)}`;
  if (amount < 0) return `−${money(-amount)}`;
  return money(0);
}

/**
 * Transaksi's hero (apply-markas-transaksi D4, D28, D30): "Pengeluaran" in 44 px with "Masuk" and "Bersih" beside it;
 * for "Tabungan & investasi" Beranda's "Ditabung & investasi" alone. While loading it shows skeleton figures only.
 */
export function LedgerHero({
  figures,
  loading,
  type,
  periodLabel,
  money,
}: {
  figures: CashFlowMetrics | null;
  loading: boolean;
  type: LedgerTypeFilter;
  /** "semua waktu" without a period, the range of a link with `from`/`to`, or null inside a period. */
  periodLabel: { kind: "all" } | { kind: "range"; range: string } | null;
  money: (amount: number) => string;
}) {
  const t = useTranslations();
  const saving = type === "saving";
  const base = saving ? t("terms.savedInvested") : t("terms.spending");
  const label = periodLabel?.kind === "all"
    ? t("transactions.hero.allTime", { label: base })
    : periodLabel?.kind === "range" ? t("transactions.hero.range", { label: base, range: periodLabel.range }) : base;

  if (loading || !figures) {
    return (
      <HeroFigure
        id="tx-hero"
        label={label}
        busy={loading}
        figure={<span className="skel is-hero" style={{ width: 240 }} aria-hidden="true" />}
        aside={saving ? undefined : (
          <>
            <FigureBlock bar={false} label={t("terms.income")} figure={<span className="skel is-fig" aria-hidden="true" />} />
            <FigureBlock bar={false} label={t("transactions.hero.netLabel")} figure={<span className="skel is-fig" aria-hidden="true" />} />
          </>
        )}
      />
    );
  }

  if (saving) {
    const net = figures.saving_net;
    const amount = (value: number) => maskedTag(money(value));
    const figure = net === undefined
      ? <span className="is-words">{t("transactions.hero.unavailable")}</span>
      : net === null ? <span className="is-words">{t("transactions.hero.savingUnavailable")}</span> : <MaskedMoney text={net < 0 ? `−${money(-net)}` : money(net)} />;
    const line = typeof net === "number" && net !== 0
      ? net > 0 ? t.rich("transactions.hero.savingUp", { amount: amount(net) }) : t.rich("transactions.hero.savingDown", { amount: amount(-net) })
      : undefined;
    return <HeroFigure id="tx-hero" label={label} figure={figure} line={line} />;
  }

  const offset = figures.reimbursement_offset ?? 0;
  const savedPart = figures.saving ?? 0;
  const amount = (value: number) => maskedTag(money(value));
  const qualifier = offset > 0
    ? t.rich("transactions.hero.reimbursed", { amount: amount(offset) })
    : savedPart > 0 ? t.rich("transactions.hero.withoutSaved", { amount: amount(savedPart) }) : null;
  const line = type === "credit"
    ? <>{qualifier}{qualifier ? " " : null}{t("transactions.hero.creditLine")}</>
    : qualifier ?? undefined;
  return (
    <HeroFigure
      id="tx-hero"
      label={label}
      figure={figures.spending === undefined ? <span className="is-words">{t("transactions.hero.unavailable")}</span> : <MaskedMoney text={money(figures.spending)} />}
      line={line}
      aside={
        <>
          <FigureBlock bar={false} label={t("terms.income")} className="c-income" figure={<MaskedMoney text={figures.inflow > 0 ? `+${money(figures.inflow)}` : money(figures.inflow)} />} />
          <FigureBlock
            bar={false}
            label={t("transactions.hero.netLabel")}
            figure={figures.spending_net === undefined ? <span className="is-words">{t("transactions.hero.unavailable")}</span> : <MaskedMoney text={signed(figures.spending_net, money)} />}
          />
        </>
      }
    />
  );
}
