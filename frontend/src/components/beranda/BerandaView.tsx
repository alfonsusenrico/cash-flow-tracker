"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { EmergencyRunway, essentialBasisText } from "@/components/dashboard/EmergencyCoverage";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { usePageHeader } from "@/components/workspace/context";
import { ExplainableFigure } from "@/components/workspace/explain";
import { Drawer } from "@/components/workspace/overlays";
import { Card, CardHeader, ErrorState, MoneyBar, Skeleton } from "@/components/workspace/primitives";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { api } from "@/lib/api";
import { KAKEIBO_BUCKETS } from "@/lib/kakeibo";
import { consolidateLedgerMovements } from "@/lib/ledgerMovements";
import { queryKeys } from "@/lib/queryKeys";
import type { ReviewQueue } from "@/lib/review";
import type { AutoLinkedOccurrence, DueExpectation, RecurringRule } from "@/types/recurring";
import { ACTIVITY_LIMIT, ActivityCard, jakartaDay } from "./activity";
import {
  type Money,
  balance,
  longDate,
  masukExplanation,
  netWorthExplanation,
  periodWord,
  remainingExplanation,
  savingExplanation,
  scheduledExplanation,
  shortDate,
  spendingExplanation,
  unavailableReason,
} from "./explanations";
import { TodoCard, todoItems } from "./TodoCard";
import { TransactionDrawer } from "@/components/transactions/TransactionDrawer";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import type { ActivityRow, CycleSummary } from "./types";

const SUMMARY_REFRESH_MS = 30_000;
// Plans due today, overdue, or due within this many days appear in "Perlu tindakan".
const PLAN_NOTICE_DAYS = 2;

const WEEKDAY_DATE = new Intl.DateTimeFormat("id-ID", { weekday: "long", day: "numeric", month: "long", timeZone: "Asia/Jakarta" });
const MONTH_NAME = new Intl.DateTimeFormat("id-ID", { month: "long", timeZone: "UTC" });

interface Overview {
  kpis?: EmergencyRunway & {
    runway_status?: EmergencyRunway["status"];
    target_amount?: number | null;
    coverage_pct?: number | null;
    emergency_balance_basis?: EmergencyRunway["balance_basis"];
  };
}

function useDisplayMoney(): Money {
  const { bal } = useAppCtx();
  return (amount: number) => bal(amount);
}

function AnimatedMoney({ value }: { value: number }) {
  const shown = useAnimatedCounter(value);
  const { bal } = useAppCtx();
  return <>{bal(shown)}</>;
}

/* ------------------------------------------------------------------------------------------------
 * KPI cards (refine-workspace-layout D5): the period's safe amount first, then the four money figures.
 * Every figure is a server field with its "Rincian hitungan"; no sentence summary and no same-date chip.
 * ---------------------------------------------------------------------------------------------- */

function KpiCard({
  label,
  tone,
  hero,
  value,
  valueIsWords,
  caption,
}: {
  label: string;
  tone: "in" | "out" | "save" | "net" | "safe";
  hero?: boolean;
  value: React.ReactNode;
  valueIsWords?: boolean;
  caption: React.ReactNode;
}) {
  return (
    <div className="ws-kcard" data-hero={hero || undefined} data-tone={tone}>
      <div className="ws-kcard-l">
        {!hero && <span className="ws-kcard-dot" aria-hidden="true" />}
        {label}
      </div>
      <div className={valueIsWords ? "ws-kcard-v is-words" : "ws-kcard-v"}>{value}</div>
      <div className="ws-kcard-c">{caption}</div>
    </div>
  );
}

function sinceCaption(summary: CycleSummary): string {
  return summary.period.basis === "cycle" ? `sejak gaji ${shortDate(summary.period.start_date)}` : `sejak 1 ${MONTH_NAME.format(new Date(`${summary.period.start_date}T00:00:00Z`))}`;
}

function masukCaption(summary: CycleSummary, money: Money): string {
  const top = [...summary.masuk.lines].sort((a, b) => b.amount - a.amount)[0];
  if (!top) return "belum ada uang masuk";
  return summary.masuk.lines.length > 1 ? `terbesar ${top.label} ${money(top.amount)}` : top.label;
}

export function BerandaKpis({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const { safe_to_spend: safe, remaining, expected_salary: expected } = summary;
  const word = periodWord(summary);
  const known = safe.status === "available" && safe.days_left !== null;
  const free = Math.max(0, remaining.total);
  const days = known
    ? `${safe.days_left} hari lagi${expected.date ? ` · gaji ± ${shortDate(expected.date)}` : ""}`
    : unavailableReason(summary).short === "tanggal gaji belum jelas"
      ? "Tanggal gaji berikutnya belum diketahui"
      : unavailableReason(summary).short;
  const net = summary.net_worth;
  return (
    <section className="ws-kcards" aria-label={`Ringkasan ${word}`}>
      <KpiCard
        hero
        tone="safe"
        label="Aman dipakai sampai gajian"
        value={
          <ExplainableFigure label="Aman dipakai sampai gajian" amountText={balance(remaining.total, money)} explanation={remainingExplanation(summary, money)}>
            <AnimatedMoney value={free} />
          </ExplainableFigure>
        }
        caption={remaining.total < 0 ? `lebih ${money(-remaining.total)} dari uang masuk` : days}
      />
      <KpiCard
        tone="in"
        label="Masuk"
        value={
          <ExplainableFigure label={`Masuk ${word}`} amountText={money(summary.masuk.total)} explanation={masukExplanation(summary, money)}>
            <AnimatedMoney value={summary.masuk.total} />
          </ExplainableFigure>
        }
        caption={masukCaption(summary, money)}
      />
      <KpiCard
        tone="out"
        label="Pengeluaran"
        value={
          <ExplainableFigure label={`Pengeluaran ${word}`} amountText={money(summary.spending.amount)} explanation={spendingExplanation(summary, money)}>
            <AnimatedMoney value={summary.spending.amount} />
          </ExplainableFigure>
        }
        caption={sinceCaption(summary)}
      />
      <KpiCard
        tone="save"
        label="Ditabung & investasi"
        value={
          <ExplainableFigure label="Ditabung & investasi" amountText={money(summary.fresh_saving.amount)} explanation={savingExplanation(summary, money)}>
            <AnimatedMoney value={summary.fresh_saving.amount} />
          </ExplainableFigure>
        }
        caption="kantong tabungan dan investasi"
      />
      <KpiCard
        tone="net"
        label="Kekayaan bersih"
        value={
          <ExplainableFigure label="Kekayaan bersih" amountText={money(net.total)} explanation={netWorthExplanation(summary, money)}>
            <AnimatedMoney value={net.total} />
          </ExplainableFigure>
        }
        caption={`aset ${money(net.liquid + net.investment - (net.reconciliation_pending ?? 0))} · utang ${money(net.debts)}`}
      />
    </section>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Uang siklus ini: four segments that add up to Masuk, and one formula line.
 * ---------------------------------------------------------------------------------------------- */

export function MoneyCard({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const { spending, fresh_saving: saving, scheduled, remaining, masuk } = summary;
  const word = periodWord(summary);
  const free = Math.max(0, remaining.total);
  const legend = [
    { key: "spending", label: "Pengeluaran", amount: spending.amount, color: "var(--out-solid)", explanation: spendingExplanation(summary, money) },
    { key: "saving", label: "Ditabung & investasi", amount: saving.amount, color: "var(--k-saving)", explanation: savingExplanation(summary, money) },
    { key: "scheduled", label: "Terjadwal", amount: scheduled.total, color: "var(--warn-solid)", explanation: scheduledExplanation(summary, money) },
    { key: "safe", label: "Aman dipakai", amount: free, color: "var(--in-solid)", explanation: remainingExplanation(summary, money) },
  ];
  return (
    <Card labelledBy="beranda-money">
      <CardHeader
        id="beranda-money"
        title={`Uang ${word}`}
        extra={<span className="text-[13px] font-normal text-[var(--muted)]">dari {money(masuk.total)} masuk</span>}
        action={
          <Link href="/goals" className="ws-linkbtn ws-accent-link">
            Tagihan
          </Link>
        }
      />
      <div className="mt-4">
        <MoneyBar
          legend={false}
          segments={legend.map((item) => ({ key: item.key, label: item.label, amount: item.amount, display: money(item.amount), className: `ws-seg-${item.key}` }))}
        />
        <ul className="ws-money-legend" aria-label={`Pembagian uang ${word}`}>
          {legend.map((item) => (
            <li key={item.key} style={{ ["--sc" as string]: item.color }}>
              <span className="ws-money-legend-h">{item.label}</span>
              <b>
                <ExplainableFigure label={item.label} amountText={money(item.amount)} explanation={item.explanation} />
              </b>
            </li>
          ))}
        </ul>
      </div>
      {remaining.total < 0 ? (
        <p className="ws-caption mt-3 font-medium text-[var(--out-fg)]">
          {word[0].toUpperCase() + word.slice(1)} sudah lebih {money(-remaining.total)} dari uang masuk.
        </p>
      ) : (
        <p className="ws-caption mt-3">Aman dipakai = masuk − pengeluaran − ditabung − terjadwal.</p>
      )}
    </Card>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Kakeibo and Ketahanan Dana
 * ---------------------------------------------------------------------------------------------- */

export function KakeiboCard({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const word = periodWord(summary);
  const rows = [...summary.spending.lines, ...summary.fresh_saving.lines];
  // Bar lengths are display only: each bucket against the largest one; the amounts come from the server.
  const largest = Math.max(1, ...rows.map((line) => line.amount));
  const row = (line: (typeof rows)[number]) => {
    const info = KAKEIBO_BUCKETS.find((bucket) => bucket.value === line.key);
    return (
      <li key={line.key} style={{ ["--kc" as string]: info?.color }}>
        <span className="ws-kk" data-bucket={line.key}>
          {line.href ? <Link href={line.href}>{line.label}</Link> : line.label}
        </span>
        <span className="ws-kk-bar" aria-hidden="true">
          <i style={{ width: `${(line.amount / largest) * 100}%` }} />
        </span>
        <span className="ws-kk-amount">{money(line.amount)}</span>
      </li>
    );
  };
  return (
    <Card labelledBy="beranda-kakeibo">
      <CardHeader
        id="beranda-kakeibo"
        title={`Kakeibo ${word}`}
        extra={
          <span className="text-[13px] font-normal text-[var(--muted)]">
            Pengeluaran <ExplainableFigure label={`Pengeluaran ${word}`} amountText={money(summary.spending.amount)} explanation={spendingExplanation(summary, money)} />
          </span>
        }
        action={
          <Link href="/insights" className="ws-linkbtn ws-accent-link">
            Analisis
          </Link>
        }
      />
      <ul className="ws-kk-rows">{summary.spending.lines.map(row)}</ul>
      <ul className="ws-kk-rows ws-kk-saving" aria-label="Ditabung">
        {summary.fresh_saving.lines.map(row)}
      </ul>
    </Card>
  );
}

export function DanaCard({ overview, error }: { overview: Overview | undefined; error: boolean }) {
  const { bal } = useAppCtx();
  const kpis = overview?.kpis;
  const months = kpis ? kpis.coverage_months ?? kpis.runway_months : null;
  const status = kpis?.runway_status ?? kpis?.status;
  const unknown = !kpis || months == null || status === "unknown";
  const basisWord = kpis?.emergency_balance_basis === "all_liquid" ? "saldo kas" : "dana darurat";
  return (
    <Card labelledBy="beranda-dana">
      <CardHeader
        id="beranda-dana"
        icon="shield"
        title="Ketahanan Dana"
        action={
          <Link href="/goals" className="ws-linkbtn ws-accent-link">
            Detail
          </Link>
        }
      />
      {error && <p className="ws-caption mt-3">Ketahanan Dana belum bisa dimuat.</p>}
      {kpis && unknown && (
        <p className="ws-caption mt-3">
          Belum bisa dihitung: biaya pokok bulanan belum diketahui. {essentialBasisText(kpis.essential_cost)}
        </p>
      )}
      {kpis && !unknown && (
        <>
          <div className="ws-dana-big">
            <b>{months!.toLocaleString("id-ID", { maximumFractionDigits: 1 })}</b> bulan biaya pokok
          </div>
          {kpis.coverage_pct != null && (
            <div className="ws-bar mt-3" aria-hidden="true">
              <i style={{ width: `${Math.min(100, Math.max(0, kpis.coverage_pct))}%` }} />
            </div>
          )}
          <div className="ws-dana-foot">
            <span>
              <b>{bal(kpis.emergency_fund_balance)}</b> {basisWord}
            </span>
            {kpis.target_amount ? (
              <span>
                target <b>{bal(kpis.target_amount)}</b>
              </span>
            ) : null}
          </div>
        </>
      )}
    </Card>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Page
 * ---------------------------------------------------------------------------------------------- */

export function headerFor(summary: CycleSummary | undefined, now: Date) {
  const title = WEEKDAY_DATE.format(now);
  if (!summary) return { title, subtitle: "" };
  const { period } = summary;
  if (period.basis === "cycle") {
    return { title, subtitle: `Siklus sejak ${shortDate(period.start_date)} · hari ke-${period.day_index}` };
  }
  const month = MONTH_NAME.format(new Date(`${period.start_date}T00:00:00Z`));
  return { title, subtitle: `${month} · siklus gaji belum tersedia` };
}

function startOfYesterdayIso(now: Date): string {
  const today = jakartaDay(now);
  const start = Date.parse(`${today}T00:00:00+07:00`) - 86_400_000;
  return new Date(start).toISOString();
}

export default function BerandaView() {
  const { bal } = useAppCtx();
  const [now] = useState(() => new Date());
  const today = jakartaDay(now);
  const [selectedTx, setSelectedTx] = useState<ActivityRow | null>(null);
  const { record } = useTransactionUndo();
  const [plansOpen, setPlansOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const gridRef = useRef<HTMLDivElement>(null);
  // Same width as `@container main (max-width: 900px)`, where the grid becomes one column. The cards are
  // then put in the phone order (D5), so reading and focus order follow the layout.
  const singleColumn = useWidthAtMost(gridRef, 900);

  const summaryQuery = useQuery<CycleSummary>({
    queryKey: queryKeys.dashboard.cycleSummary,
    queryFn: () => api.get("/dashboard/cycle-summary"),
    refetchInterval: SUMMARY_REFRESH_MS,
    refetchOnWindowFocus: true,
  });
  const summary = summaryQuery.data;
  const basis = summary?.period.cycle_offset_basis ?? "cycle";

  const overviewQuery = useQuery<Overview>({
    queryKey: queryKeys.dashboard.overview(basis, 0),
    queryFn: () => api.get(`/dashboard/overview?timeframe=${basis}&cycle_offset=0`),
    enabled: !summaryQuery.isLoading,
  });

  const queueQuery = useQuery<ReviewQueue>({ queryKey: queryKeys.review.queue, queryFn: () => api.get("/review/queue") });
  const rulesQuery = useQuery<{ rules: RecurringRule[] }>({ queryKey: queryKeys.recurring.all, queryFn: () => api.get("/recurring") });
  const pendingQuery = useQuery<{ rules: DueExpectation[]; recent_auto_links?: AutoLinkedOccurrence[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
  });

  const from = startOfYesterdayIso(now);
  const activityQuery = useQuery<{ transactions: ActivityRow[] }>({
    queryKey: queryKeys.transactions.recent(from),
    queryFn: () => api.get(`/transactions?from_date=${encodeURIComponent(from)}&limit=60`),
    refetchInterval: SUMMARY_REFRESH_MS,
  });

  const activity = useMemo(() => {
    const rows = activityQuery.data?.transactions;
    return rows ? consolidateLedgerMovements(rows, true) : null;
  }, [activityQuery.data]);
  const recent = activity ? activity.slice(0, ACTIVITY_LIMIT) : null;
  // A row deleted from the drawer leaves the list once Beranda refreshes; focus then goes to the main region.
  const [deletedId, setDeletedId] = useState<string | null>(null);
  useEffect(() => {
    if (!deletedId || activity?.some((row) => row.id === deletedId)) return;
    if (document.activeElement === document.body) document.getElementById("main")?.focus();
    setDeletedId(null);
  }, [deletedId, activity]);
  const todayRows = activity ? activity.filter((row) => jakartaDay(row.date) === today) : [];
  const autoToday = todayRows.filter((row) => row.review_status === "auto").length;

  const horizon = jakartaDay(new Date(now.getTime() + PLAN_NOTICE_DAYS * 86_400_000));
  const plans = (rulesQuery.data?.rules ?? [])
    .filter((rule) => rule.is_active && rule.type !== "income" && rule.next_due_date <= horizon)
    .sort((a, b) => a.next_due_date.localeCompare(b.next_due_date));
  const items = todoItems({
    queue: queueQuery.data,
    plans,
    autoLinks: pendingQuery.data?.recent_auto_links ?? [],
    today,
    bal,
    onOpenPlans: () => setPlansOpen(true),
  });
  const attention = queueQuery.data?.counts.attention ?? 0;

  usePageHeader(headerFor(summary, now));

  const summaryFailed = summaryQuery.isError;
  const retry = () => summaryQuery.refetch();

  const noCycleNote =
    summary?.period.basis === "month" ? (
      <p className="ws-caption mb-4">
        Siklus gaji belum tersedia, jadi Beranda memakai bulan kalender.{" "}
        <Link href="/settings/gaji" className="ws-linkbtn">
          Atur gaji di Pengaturan
        </Link>
      </p>
    ) : null;

  const todo = (
    <TodoCard
      attention={attention}
      items={items}
      autoToday={autoToday}
      error={queueQuery.isError}
      waitingAmount={summary && summary.waiting.count > 0 ? bal(summary.waiting.total) : null}
    />
  );
  const money = summary ? <MoneyCard summary={summary} /> : null;
  const activityCard = <ActivityCard rows={recent} bal={bal} onSelect={setSelectedTx} error={activityQuery.isError} />;
  const kakeibo = summary ? <KakeiboCard summary={summary} /> : null;
  const dana = <DanaCard overview={overviewQuery.data} error={overviewQuery.isError} />;

  return (
    // Measured here, not on the grid: the grid only exists once the figures have loaded.
    <div ref={gridRef}>
      {summaryQuery.isLoading && <BerandaSkeleton />}
      {summaryFailed && (
        <div className="my-4">
          <ErrorState message="Angka Beranda belum dapat dimuat. Saldo dan transaksi tetap aman; coba muat lagi." onRetry={retry} />
        </div>
      )}
      {summary && (
        <>
          {noCycleNote}
          <BerandaKpis summary={summary} />
        </>
      )}
      {(summary || summaryFailed) && (
        <div className="ws-home-grid">
          {singleColumn ? (
            <div className="ws-stack">
              {todo}
              {money}
              {activityCard}
              {kakeibo}
              {dana}
            </div>
          ) : (
            <>
              <div className="ws-stack">
                {money}
                {activityCard}
              </div>
              <div className="ws-stack">
                {todo}
                {kakeibo}
                {dana}
              </div>
            </>
          )}
        </div>
      )}

      <TransactionDrawer transactionId={selectedTx?.id ?? null} onClose={() => setSelectedTx(null)} record={record} onDeleted={(row) => setDeletedId(row.id)} />
      <Drawer open={plansOpen} onClose={() => setPlansOpen(false)} title="Rencana jatuh tempo" subtitle="Tautkan, lewati, atau catat manual">
        <PendingScheduledBanner onOpenRulesManager={() => setRulesOpen(true)} />
        {!(pendingQuery.data?.rules ?? []).length && !(pendingQuery.data?.recent_auto_links ?? []).length && (
          <p className="ws-caption">Tidak ada rencana yang jatuh tempo hari ini.</p>
        )}
      </Drawer>
      <RecurringRulesModal open={rulesOpen} onClose={() => setRulesOpen(false)} />
    </div>
  );
}

function BerandaSkeleton() {
  return (
    <div aria-busy="true" aria-label="Memuat Beranda">
      <Skeleton className="h-[132px] rounded-[18px]" />
      <div className="ws-home-grid">
        <div className="ws-stack">
          <Skeleton className="h-[160px] rounded-[18px]" />
          <Skeleton className="h-[420px] rounded-[18px]" />
        </div>
        <div className="ws-stack">
          <Skeleton className="h-[140px] rounded-[18px]" />
          <Skeleton className="h-[260px] rounded-[18px]" />
        </div>
      </div>
    </div>
  );
}

export { longDate };
