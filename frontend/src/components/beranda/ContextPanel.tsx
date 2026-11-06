"use client";

import Link from "next/link";
import { useTranslations } from "next-intl";
import { type EmergencyRunway, essentialBasisText } from "@/components/dashboard/EmergencyCoverage";
import { Panel, PanelBlock, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { balance, lineLabel, periodBasis } from "./explanations";
import type { CycleSummary } from "./types";

export interface Overview {
  kpis?: EmergencyRunway & {
    runway_status?: EmergencyRunway["status"];
    emergency_balance_basis?: EmergencyRunway["balance_basis"];
  };
}

function NetWorthBlock({ summary, bal }: { summary: CycleSummary; bal: (amount: number) => string }) {
  const net = summary.net_worth;
  const assets = net.liquid + (net.dana_investasi ?? 0) + net.investment - (net.reconciliation_pending ?? 0);
  const debts = net.debts + (net.credit_owed ?? 0);
  const t = useTranslations();
  return (
    <PanelBlock heading={t("terms.netWorth")} href="/accounts">
      <p className="t-fig">
        <MaskedMoney text={balance(net.total, bal)} />
      </p>
      <p className="t-meta">
        {t.rich("home.assetsDebts", {
          assets: () => <MaskedMoney text={bal(assets)} />,
          debts: () => <MaskedMoney text={bal(debts)} />,
        })}
      </p>
    </PanelBlock>
  );
}

function EmergencyBlock({ overview, error, bal }: { overview: Overview | undefined; error: boolean; bal: (amount: number) => string }) {
  const t = useTranslations();
  const format = useFormat();
  const kpis = overview?.kpis;
  const months = kpis ? kpis.coverage_months ?? kpis.runway_months : null;
  const status = kpis?.runway_status ?? kpis?.status;
  const unknown = !kpis || months == null || status === "unknown";
  const basis = kpis?.emergency_balance_basis === "all_liquid" ? "cash" : "emergency";
  const amount = () => <MaskedMoney text={bal(kpis?.emergency_fund_balance ?? 0)} />;
  return (
    <PanelBlock heading={t("terms.safetyNet")} href="/goals">
      {error && <p className="t-meta">{t("home.safetyNetLoadError")}</p>}
      {kpis && unknown && (
        <>
          <p className="t-strong">{t("home.cannotCalculate")}</p>
          <p className="t-meta">{essentialBasisText(kpis.essential_cost, t)}</p>
        </>
      )}
      {kpis && !unknown && (
        <>
          <p className="t-strong">{t("home.safetyNetMonths", { months: months!, monthsText: format.decimal(months!) })}</p>
          <p className="t-meta">
            {kpis.target_months
              ? t.rich("home.safetyNetBalanceTarget", {
                  basis,
                  amount,
                  months: kpis.target_months,
                  monthsText: format.decimal(kpis.target_months),
                })
              : t.rich("home.safetyNetBalance", { basis, amount })}
          </p>
        </>
      )}
    </PanelBlock>
  );
}

function KakeiboBlock({ summary, bal }: { summary: CycleSummary; bal: (amount: number) => string }) {
  const lines = summary.spending.lines;
  // Bar lengths are display only: each bucket against the largest one; the amounts come from the server.
  const largest = Math.max(1, ...lines.map((line) => line.amount));
  const t = useTranslations();
  return (
    <PanelBlock>
      <SectionHeader
        label={t("home.kakeiboPeriod", { period: periodBasis(summary) })}
        aside={
          <Link href="/insights" className="ws-linkbtn">
            {t("nav.analysis")}
          </Link>
        }
      />
      {lines.map((line) => {
        const info = kakeiboBucketInfo(line.key);
        const unclassified = line.key === "unclassified";
        return (
          <div key={line.key} className="kk-row">
            <span className={unclassified ? "dot is-unclassified" : "dot"} style={unclassified ? undefined : { background: info?.color }} aria-hidden="true" />
            {line.href ? (
              <Link href={line.href} className="t-body kk-label">
                {lineLabel(line, t)}
              </Link>
            ) : (
              <span className="t-body">{lineLabel(line, t)}</span>
            )}
            <MaskedMoney text={bal(line.amount)} className="t-amt" />
            {unclassified && line.amount > 0 ? (
              <Link href="/review" className="t-meta c-accent kk-note">
                {t("home.chooseInReview")}
              </Link>
            ) : (
              <span className="prog" aria-hidden="true">
                <span style={{ width: `${(line.amount / largest) * 100}%`, background: info?.color }} />
              </span>
            )}
          </div>
        );
      })}
    </PanelBlock>
  );
}

/**
 * The context panel (D8): Kekayaan bersih and Ketahanan Dana as whole-block links, then Kakeibo for the period.
 * Without the summary only Ketahanan Dana, which has its own data, is shown.
 */
export function ContextPanel({
  summary,
  overview,
  overviewError,
  bal,
}: {
  summary: CycleSummary | undefined;
  overview: Overview | undefined;
  overviewError: boolean;
  bal: (amount: number) => string;
}) {
  const t = useTranslations("home");
  return (
    <Panel label={t("moneySummary")}>
      {summary && <NetWorthBlock summary={summary} bal={bal} />}
      <EmergencyBlock overview={overview} error={overviewError} bal={bal} />
      {summary && <KakeiboBlock summary={summary} bal={bal} />}
    </Panel>
  );
}
