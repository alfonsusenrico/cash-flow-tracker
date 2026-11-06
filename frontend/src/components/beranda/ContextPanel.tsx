"use client";

import Link from "next/link";
import { type EmergencyRunway, essentialBasisText } from "@/components/dashboard/EmergencyCoverage";
import { Panel, PanelBlock, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { balance, periodWord } from "./explanations";
import type { CycleSummary } from "./types";

export interface Overview {
  kpis?: EmergencyRunway & {
    runway_status?: EmergencyRunway["status"];
    emergency_balance_basis?: EmergencyRunway["balance_basis"];
  };
}

const MONTHS = { maximumFractionDigits: 1 } as const;

function NetWorthBlock({ summary, bal }: { summary: CycleSummary; bal: (amount: number) => string }) {
  const net = summary.net_worth;
  const assets = net.liquid + (net.dana_investasi ?? 0) + net.investment - (net.reconciliation_pending ?? 0);
  const debts = net.debts + (net.credit_owed ?? 0);
  return (
    <PanelBlock heading="Kekayaan bersih" href="/accounts">
      <p className="t-fig">
        <MaskedMoney text={balance(net.total, bal)} />
      </p>
      <p className="t-meta">
        aset <MaskedMoney text={bal(assets)} /> · utang <MaskedMoney text={bal(debts)} />
      </p>
    </PanelBlock>
  );
}

function EmergencyBlock({ overview, error, bal }: { overview: Overview | undefined; error: boolean; bal: (amount: number) => string }) {
  const kpis = overview?.kpis;
  const months = kpis ? kpis.coverage_months ?? kpis.runway_months : null;
  const status = kpis?.runway_status ?? kpis?.status;
  const unknown = !kpis || months == null || status === "unknown";
  const basisWord = kpis?.emergency_balance_basis === "all_liquid" ? "saldo kas" : "dana darurat";
  return (
    <PanelBlock heading="Ketahanan Dana" href="/goals">
      {error && <p className="t-meta">Ketahanan Dana belum bisa dimuat.</p>}
      {kpis && unknown && (
        <>
          <p className="t-strong">Belum bisa dihitung</p>
          <p className="t-meta">{essentialBasisText(kpis.essential_cost)}</p>
        </>
      )}
      {kpis && !unknown && (
        <>
          <p className="t-strong">{months!.toLocaleString("id-ID", MONTHS)} bulan biaya pokok</p>
          <p className="t-meta">
            {basisWord} <MaskedMoney text={bal(kpis.emergency_fund_balance)} />
            {kpis.target_months ? ` · target ${kpis.target_months.toLocaleString("id-ID", MONTHS)} bulan` : null}
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
  return (
    <PanelBlock>
      <SectionHeader
        label={`Kakeibo ${periodWord(summary)}`}
        aside={
          <Link href="/insights" className="ws-linkbtn">
            Analisis
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
                {line.label}
              </Link>
            ) : (
              <span className="t-body">{line.label}</span>
            )}
            <MaskedMoney text={bal(line.amount)} className="t-amt" />
            {unclassified && line.amount > 0 ? (
              <Link href="/review" className="t-meta c-accent kk-note">
                Perlu dipilih di Tinjau
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
  return (
    <Panel label="Ringkasan simpanan">
      {summary && <NetWorthBlock summary={summary} bal={bal} />}
      <EmergencyBlock overview={overview} error={overviewError} bal={bal} />
      {summary && <KakeiboBlock summary={summary} bal={bal} />}
    </Panel>
  );
}
