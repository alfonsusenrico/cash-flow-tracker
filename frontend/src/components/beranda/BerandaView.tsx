"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { TransactionDrawer } from "@/components/transactions/TransactionDrawer";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { Icon } from "@/components/ui/Icon";
import { usePageHeader } from "@/components/workspace/context";
import { Drawer } from "@/components/workspace/overlays";
import { api } from "@/lib/api";
import { consolidateLedgerMovements } from "@/lib/ledgerMovements";
import { queryKeys } from "@/lib/queryKeys";
import type { ReviewQueue } from "@/lib/review";
import type { AutoLinkedOccurrence, DueExpectation, RecurringRule } from "@/types/recurring";
import { ActivitySection } from "./ActivitySection";
import { BerandaHero } from "./BerandaHero";
import { ContextPanel, type Overview } from "./ContextPanel";
import { TodoSection, todoItems } from "./TodoSection";
import { jakartaDay, recentActivity } from "./activity";
import { shortDate } from "./explanations";
import type { ActivityRow, CycleSummary } from "./types";

const SUMMARY_REFRESH_MS = 30_000;
// Plans due today, overdue, or due within this many days appear in "Perlu tindakan".
const PLAN_NOTICE_DAYS = 2;

const WEEKDAY_DATE = new Intl.DateTimeFormat("id-ID", { weekday: "long", day: "numeric", month: "long", timeZone: "Asia/Jakarta" });
const MONTH_NAME = new Intl.DateTimeFormat("id-ID", { month: "long", timeZone: "UTC" });

/** The title block (D2): today's date and the period line, with the cycle's span when the salary date is known. */
export function headerFor(summary: CycleSummary | undefined, now: Date) {
  const title = WEEKDAY_DATE.format(now);
  if (!summary) return { title, subtitle: "" };
  const { period, scheduled } = summary;
  if (period.basis === "cycle") {
    const span =
      scheduled.window_basis === "expected_salary"
        ? `${shortDate(period.start_date)} – ${shortDate(scheduled.window_end)}`
        : `sejak ${shortDate(period.start_date)}`;
    return { title, subtitle: `Siklus gaji ${span} · hari ke-${period.day_index}` };
  }
  const month = MONTH_NAME.format(new Date(`${period.start_date}T00:00:00Z`));
  return { title, subtitle: `${month} · siklus gaji belum tersedia` };
}

function dayBefore(day: string): string {
  return jakartaDay(new Date(Date.parse(`${day}T12:00:00+07:00`) - 86_400_000));
}

function startOfYesterdayIso(now: Date): string {
  const today = jakartaDay(now);
  const start = Date.parse(`${today}T00:00:00+07:00`) - 86_400_000;
  return new Date(start).toISOString();
}

/** Static placeholders in the final layout (D13): the SPEC skeleton, no pulse and no amount. */
function HeroSkeleton() {
  return (
    <div className="hero-skel" aria-hidden="true">
      <span className="skel" style={{ width: 168 }} />
      <span className="skel is-hero" style={{ width: "40%" }} />
      <span className="skel is-line" style={{ width: 240 }} />
      <span className="skel is-split" />
      <div className="legend">
        {[0, 1, 2, 3].map((index) => (
          <div key={index} className="fig-block">
            <span className="skel" style={{ width: 64 }} />
            <span className="skel is-fig" style={{ width: "72%" }} />
          </div>
        ))}
      </div>
    </div>
  );
}

function RowsSkeleton() {
  return (
    <div aria-hidden="true">
      <span className="skel is-label" style={{ width: 120 }} />
      {[46, 38, 52].map((width) => (
        <div key={width} className="skel-row">
          <span className="skel" style={{ width: 40 }} />
          <span className="skel" style={{ width: `${width}%` }} />
          <span className="grow" />
          <span className="skel" style={{ width: 96 }} />
        </div>
      ))}
    </div>
  );
}

function PanelSkeleton() {
  return (
    <div className="panel" aria-hidden="true">
      {[0, 1, 2].map((index) => (
        <div key={index} className="panel-block">
          <span className="skel" style={{ width: 120 }} />
          <span className="skel is-fig" style={{ width: "60%" }} />
          <span className="skel" style={{ width: "80%" }} />
        </div>
      ))}
    </div>
  );
}

/** The summary's failure in the hero's place (D13): the SPEC error box, no amount, and a retry. */
function SummaryError({ onRetry }: { onRetry: () => void }) {
  return (
    <div className="error-box" role="alert">
      <Icon name="alert" className="h-5 w-5" />
      <div className="grow">
        <p className="t-strong">Angka Beranda belum dapat dimuat</p>
        <p className="t-meta">Saldo dan transaksi tetap aman; coba muat lagi.</p>
      </div>
      <button type="button" className="ws-btn" onClick={onRetry}>
        Coba lagi
      </button>
    </div>
  );
}

export default function BerandaView() {
  const { bal } = useAppCtx();
  const [now] = useState(() => new Date());
  const today = jakartaDay(now);
  const [selectedTx, setSelectedTx] = useState<ActivityRow | null>(null);
  const { record } = useTransactionUndo();
  const [plansOpen, setPlansOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);

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
  const recent = activity ? recentActivity(activity) : null;
  // A row deleted from the drawer leaves the list once Beranda refreshes; focus then goes to the main region.
  const [deletedId, setDeletedId] = useState<string | null>(null);
  useEffect(() => {
    if (!deletedId || activity?.some((row) => row.id === deletedId)) return;
    if (document.activeElement === document.body) document.getElementById("main")?.focus();
    setDeletedId(null);
  }, [deletedId, activity]);

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

  usePageHeader(headerFor(summary, now));

  const loading = summaryQuery.isLoading;
  const todoLoading = queueQuery.isLoading || rulesQuery.isLoading || pendingQuery.isLoading;

  return (
    <>
      {summary?.period.basis === "month" && (
        <p className="t-meta beranda-note">
          Siklus gaji belum tersedia, jadi Beranda memakai bulan kalender.{" "}
          <Link href="/settings/gaji" className="ws-linkbtn">
            Atur gaji di Pengaturan
          </Link>
        </p>
      )}
      {/* DOM order is the visual and phone order at every width (D1): hero, Perlu tindakan, Aktivitas, panel. */}
      <div className="cols" aria-busy={loading || undefined}>
        <div className="stack">
          {loading && <HeroSkeleton />}
          {summaryQuery.isError && <SummaryError onRetry={() => summaryQuery.refetch()} />}
          {summary && <BerandaHero summary={summary} />}
          {todoLoading ? <RowsSkeleton /> : <TodoSection attention={queueQuery.data?.counts.attention ?? 0} items={items} error={queueQuery.isError} />}
          {activityQuery.isLoading ? (
            <RowsSkeleton />
          ) : (
            <ActivitySection rows={recent} today={today} yesterday={dayBefore(today)} bal={bal} onSelect={setSelectedTx} error={activityQuery.isError} />
          )}
        </div>
        {loading ? <PanelSkeleton /> : <ContextPanel summary={summary} overview={overviewQuery.data} overviewError={overviewQuery.isError} bal={bal} />}
      </div>

      <TransactionDrawer transactionId={selectedTx?.id ?? null} onClose={() => setSelectedTx(null)} record={record} onDeleted={(row) => setDeletedId(row.id)} />
      <Drawer open={plansOpen} onClose={() => setPlansOpen(false)} title="Rencana jatuh tempo" subtitle="Tautkan, lewati, atau catat manual">
        <PendingScheduledBanner onOpenRulesManager={() => setRulesOpen(true)} />
        {!(pendingQuery.data?.rules ?? []).length && !(pendingQuery.data?.recent_auto_links ?? []).length && (
          <p className="ws-caption">Tidak ada rencana yang jatuh tempo hari ini.</p>
        )}
      </Drawer>
      <RecurringRulesModal open={rulesOpen} onClose={() => setRulesOpen(false)} />
    </>
  );
}
