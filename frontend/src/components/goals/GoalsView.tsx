"use client";

import { useState } from "react";
import dynamic from "next/dynamic";
import { usePathname, useSearchParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import { replaceSearch } from "@/lib/urlState";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { Card, CardHeader, ErrorState, Pill, Skeleton, type Tone } from "@/components/workspace/primitives";
import { Modal } from "@/components/ui/Modal";
import { PageActions } from "@/components/workspace/context";
import type { CycleSummary, PlanLine } from "@/components/beranda/types";
import type { NetWorthResponse } from "@/components/accounts/types";
import styles from "@/components/accounts/money.module.css";
import { useGoalDialogs } from "./GoalDialogs";
import type { GoalItem, ObligationItem } from "./types";

const ALL_LIQUID_NOTICE = "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas";

// The plans editor loads when it is opened, so the page's first load does not carry it (gate D18).
const RecurringRulesModal = dynamic(() => import("@/components/recurring/RecurringRulesModal").then((module) => module.RecurringRulesModal), { ssr: false });

const DAY_MONTH = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });
// en-CA formats a date as YYYY-MM-DD, comparable with the API's local dates.
const JAKARTA_DATE = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });
const MONTH_YEAR = new Intl.DateTimeFormat("id-ID", { month: "short", year: "numeric", timeZone: "Asia/Jakarta" });

function shortDate(iso: string): string {
  return DAY_MONTH.format(new Date(`${iso.slice(0, 10)}T00:00:00+07:00`));
}

/** The goal's status in words, from the server's figures only; no pace is invented for a goal without a date. */
function goalStatus(goal: GoalItem, money: (value: number) => string): { tone: Tone; text: string } {
  if (goal.remaining_amount <= 0) return { tone: "ok", text: "Tercapai" };
  if (!goal.target_date) return { tone: "neutral", text: "Tanpa tanggal" };
  if (goal.monthly_target_pace == null) return { tone: "neutral", text: "Belum dapat dihitung" };
  return { tone: "warn", text: `Perlu ${money(goal.monthly_target_pace)}/bulan` };
}

function setParam(pathname: string, search: URLSearchParams, key: "target" | "utang", value: string | null): string {
  const next = new URLSearchParams(search.toString());
  next.delete("target");
  next.delete("utang");
  if (value) next.set(key, value);
  const query = next.toString();
  return query ? `${pathname}?${query}` : pathname;
}

export function GoalsView() {
  const { bal } = useAppCtx();
  const pathname = usePathname();
  const search = useSearchParams();
  const dialogs = useGoalDialogs();
  const [plansOpen, setPlansOpen] = useState(false);
  const summaryQuery = useQuery<CycleSummary>({ queryKey: queryKeys.dashboard.cycleSummary, queryFn: () => api.get("/dashboard/cycle-summary") });
  const netWorthQuery = useQuery<NetWorthResponse>({ queryKey: queryKeys.dashboard.netWorth, queryFn: () => api.get("/dashboard/net-worth") });

  const goalId = search.get("target");
  const debtId = search.get("utang");
  const selectedGoal = goalId ? dialogs.goals.find((goal) => goal.id === goalId) ?? null : null;
  const selectedDebt = debtId ? dialogs.obligations.find((debt) => debt.id === debtId) ?? null : null;
  const open = (key: "target" | "utang", id: string | null) => replaceSearch(setParam(pathname, search, key, id));
  const active = dialogs.obligations.filter((debt) => !debt.is_archived && debt.remaining_amount > 0);
  const archived = dialogs.obligations.filter((debt) => debt.is_archived || debt.remaining_amount <= 0);

  // The title block's actions in the prototype's order (apply-markas-shell D4). Tambah target is secondary
  // because "+ Catat" is the only primary button in the frame.
  const toolbar = (
    <PageActions more={[{ key: "utang", label: "Tambah utang", icon: "plus", onSelect: dialogs.openNewObligation }]}>
      <button type="button" className="ws-btn" onClick={() => setPlansOpen(true)}>
        <Icon name="calendar" className="h-4 w-4" />
        Rencana
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openNewGoal}>
        <Icon name="plus" className="h-4 w-4" />
        Tambah target
      </button>
    </PageActions>
  );

  if (dialogs.loading) {
    return (
      <div className={styles.root}>
        {toolbar}
        <div className={styles.grid}><Skeleton style={{ height: 320 }} /><Skeleton style={{ height: 260 }} /></div>
        {dialogs.dialogs}
      </div>
    );
  }
  if (dialogs.failed) {
    return (
      <div className={styles.root}>
        {toolbar}
        <ErrorState message="Target dan utang gagal dimuat." onRetry={dialogs.retry} />
        {dialogs.dialogs}
      </div>
    );
  }

  const goalsCard = (
    <Card labelledBy="goals-list">
      <CardHeader id="goals-list" icon="target" title="Target tabungan" count={dialogs.goals.length} countLabel={`${dialogs.goals.length} target`} />
      {dialogs.goals.length === 0 ? (
        <div className={styles.emptyBlock}>
          <p className="ws-caption">Belum ada target tabungan.</p>
          <button type="button" className="ws-btn" onClick={dialogs.openNewGoal}>Buat target pertama</button>
        </div>
      ) : (
        <ul className={styles.goalTiles}>
          {dialogs.goals.map((goal) => {
            const status = goalStatus(goal, bal);
            const percent = Math.max(0, Math.min(100, Math.round(goal.percentage_completed)));
            const sources = goal.linked_accounts ?? [];
            return (
              <li key={goal.id}>
                <button type="button" className={styles.goalTile} onClick={() => open("target", goal.id)} aria-label={goal.name}>
                  <span className={styles.goalTop}>
                    <b>{goal.name}</b>
                    <Pill tone={status.tone}>{status.text}</Pill>
                    <span className={styles.goalPct}>{percent}%</span>
                  </span>
                  <span className={styles.progress} aria-hidden="true">
                    <i style={{ width: `${percent}%`, background: percent >= 100 ? "var(--in-solid)" : "var(--tr-fg)" }} />
                  </span>
                  <span className={styles.goalMeta}>
                    <span><b>{bal(goal.current_amount)}</b> dari {bal(goal.target_amount)}{goal.target_date ? ` · sampai ${MONTH_YEAR.format(new Date(goal.target_date))}` : ""}</span>
                    <span>{sources.length ? `${sources.length} kantong` : "Dicatat manual"}</span>
                  </span>
                  {sources.length > 0 && <span className={styles.goalSources}>{sources.map((account) => account.name).join(" · ")}</span>}
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );

  const billsCard = <BillsCard query={summaryQuery} money={bal} onPlans={() => setPlansOpen(true)} />;
  const runwayCard = <RunwayCard query={netWorthQuery} money={bal} />;

  const debtsCard = <DebtsCard active={active} archived={archived} money={bal} onOpen={(id) => open("utang", id)} />;

  return (
    <div className={styles.root}>
      {toolbar}
      {/* Three equal cards, then the goals at full width; on phones Ketahanan Dana comes first (D11). */}
      <div className={styles.trio}>
        {runwayCard}
        {debtsCard}
        {billsCard}
      </div>
      {goalsCard}
      <GoalDrawer goal={selectedGoal} money={bal} dialogs={dialogs} onClose={() => open("target", null)} />
      <DebtDrawer debt={selectedDebt} money={bal} dialogs={dialogs} onClose={() => open("utang", null)} />
      {plansOpen && <RecurringRulesModal open onClose={() => setPlansOpen(false)} />}
      {dialogs.dialogs}
    </div>
  );
}

/** A charge the server saw recur with a steady amount; only the fields this page reads. */
interface DetectedCharge {
  key: string;
  merchant: string;
  cadence: "weekly" | "monthly";
  typical_amount: number;
  next_expected_date: string | null;
  existing_plan: { id: string } | null;
}

interface BillRow {
  key: string;
  label: string;
  date: string;
  amount: number;
  note: string;
}

const CADENCE: Record<DetectedCharge["cadence"], string> = { weekly: "mingguan", monthly: "bulanan" };

/** Planned bills of the window, then charges seen recurring that no plan covers yet, from today to the window end,
    marked as estimates (OD97). */
export function billRows(lines: PlanLine[], charges: DetectedCharge[], today: string, windowEnd: string): BillRow[] {
  const planned = lines.map((line) => ({
    key: `plan-${line.rule_id}-${line.due_date}`,
    label: line.label,
    date: line.due_date,
    amount: line.amount,
    note: line.overdue ? "terlewat" : "terjadwal",
  }));
  const detected = charges
    .filter((charge) => !charge.existing_plan && charge.next_expected_date != null)
    .filter((charge) => charge.next_expected_date! >= today && charge.next_expected_date! <= windowEnd)
    .map((charge) => ({
      key: `seen-${charge.key}`,
      label: charge.merchant,
      date: charge.next_expected_date!,
      amount: charge.typical_amount,
      note: `${CADENCE[charge.cadence]} · perkiraan`,
    }));
  return [...planned, ...detected].sort((a, b) => a.date.localeCompare(b.date));
}

function BillsCard({ query, money, onPlans }: { query: { data?: CycleSummary; isLoading: boolean; isError: boolean; refetch: () => unknown }; money: (value: number) => string; onPlans: () => void }) {
  const chargesQuery = useQuery<{ charges: DetectedCharge[] }>({ queryKey: queryKeys.dashboard.recurringCharges, queryFn: () => api.get("/dashboard/recurring-charges") });
  const data = query.data;
  const title = data?.scheduled.window_basis === "next_30_days" ? "Tagihan 30 hari ke depan" : "Tagihan sampai gaji berikutnya";
  const plans = (data?.scheduled.lines ?? []).filter((line): line is PlanLine => line.kind === "plan");
  const rows = data ? billRows(plans, chargesQuery.data?.charges ?? [], JAKARTA_DATE.format(new Date()), data.scheduled.window_end) : [];
  const total = rows.reduce((sum, row) => sum + row.amount, 0);
  return (
    <Card labelledBy="goals-bills">
      <CardHeader id="goals-bills" icon="calendar" title={title}
        extra={rows.length ? <span className="ws-caption">{rows.length} · {money(total)}</span> : undefined}
        action={<button type="button" className="ws-linkbtn ws-accent-link" onClick={onPlans}>Kelola</button>} />
      {query.isLoading ? (
        <Skeleton style={{ height: 120 }} />
      ) : query.isError || !data ? (
        <ErrorState message="Tagihan belum dapat dimuat." onRetry={() => query.refetch()} />
      ) : rows.length === 0 ? (
        <p className="ws-caption">Tidak ada tagihan sampai {data.scheduled.window_basis === "next_30_days" ? "30 hari ke depan" : "gaji berikutnya"}.</p>
      ) : (
        <>
          <ul className={styles.billList} aria-label="Tagihan berikutnya">
            {rows.slice(0, 3).map((row) => (
              <li key={row.key}>
                <span>
                  <b>{row.label}</b>
                  <span>{shortDate(row.date)} · {row.note}</span>
                </span>
                <b>{money(row.amount)}</b>
              </li>
            ))}
          </ul>
          {rows.length > 3 && <p className="ws-caption">dan {rows.length - 3} tagihan lain</p>}
        </>
      )}
      {chargesQuery.isError && <p className="ws-caption">Tagihan yang terdeteksi belum dapat dimuat.</p>}
    </Card>
  );
}

function DebtsCard({ active, archived, money, onOpen }: { active: ObligationItem[]; archived: ObligationItem[]; money: (value: number) => string; onOpen: (id: string) => void }) {
  const [archivedOpen, setArchivedOpen] = useState(false);
  const remaining = active.reduce((sum, debt) => sum + debt.remaining_amount, 0);
  const total = active.reduce((sum, debt) => sum + debt.total_amount, 0);
  const paid = active.reduce((sum, debt) => sum + debt.paid_amount, 0);
  const percent = total > 0 ? Math.max(0, Math.min(100, Math.round((paid / total) * 100))) : 0;
  // The minimum shown is the one due first; debts without a due date come last.
  const next = [...active].filter((debt) => debt.minimum_payment != null).sort((a, b) => (a.due_date ?? "9999").localeCompare(b.due_date ?? "9999"))[0];
  return (
    <Card labelledBy="goals-debts">
      <CardHeader id="goals-debts" icon="credit-card" title="Utang" extra={<span className="ws-caption">{active.length} aktif</span>} />
      {active.length === 0 ? (
        <p className="ws-caption">Belum ada utang aktif.</p>
      ) : (
        <>
          <div className="ws-dana-big"><b className={styles.debtBig}>{money(remaining)}</b> sisa</div>
          <div className={cn(styles.progress, "mt-3")} aria-hidden="true"><i style={{ width: `${percent}%`, background: "var(--tr-fg)" }} /></div>
          <dl className={cn(styles.detailFacts, styles.factsTwo)}>
            <div><dt>Terbayar</dt><dd>{percent}%</dd></div>
            {next && <div><dt>Minimal{next.due_date ? ` · ${shortDate(next.due_date)}` : ""}</dt><dd>{money(next.minimum_payment!)}</dd></div>}
          </dl>
          <ul className={styles.debtNames} aria-label="Utang aktif">
            {active.map((debt) => (
              <li key={debt.id}>
                <button type="button" className="ws-rowbtn" onClick={() => onOpen(debt.id)}>{debt.name}</button>
                <span> · total {money(debt.total_amount)}</span>
              </li>
            ))}
          </ul>
        </>
      )}
      {archived.length > 0 && (
        <div className={styles.archived}>
          <button type="button" className="ws-linkbtn" aria-expanded={archivedOpen} onClick={() => setArchivedOpen((value) => !value)}>
            {archivedOpen ? "Sembunyikan" : "Tampilkan"} {archived.length} utang lunas
          </button>
          {archivedOpen && (
            <ul className={styles.archivedList}>
              {archived.map((debt) => (
                <li key={debt.id}><span>{debt.name}</span><span>{money(debt.total_amount)}</span></li>
              ))}
            </ul>
          )}
        </div>
      )}
    </Card>
  );
}

function RunwayCard({ query, money }: { query: { data?: NetWorthResponse; isLoading: boolean; isError: boolean; refetch: () => unknown }; money: (value: number) => string }) {
  const runway = query.data?.runway;
  const months = runway ? runway.coverage_months ?? runway.runway_months : null;
  const known = runway != null && months != null && runway.runway_days != null;
  const target = runway?.target_months != null && runway.monthly_primary_expense != null ? runway.target_months * runway.monthly_primary_expense : null;
  const coverage = target ? Math.max(0, Math.min(100, ((runway!.emergency_fund_balance ?? 0) / target) * 100)) : null;
  const basisWord = runway?.balance_basis === "all_liquid" ? "saldo kas" : "dana darurat";
  return (
    <Card labelledBy="goals-runway">
      <CardHeader id="goals-runway" icon="shield" title="Ketahanan Dana" />
      {query.isLoading ? (
        <Skeleton style={{ height: 120 }} />
      ) : query.isError || !runway ? (
        <ErrorState message="Ketahanan Dana belum dapat dihitung." onRetry={() => query.refetch()} />
      ) : (
        <>
          <div className="ws-dana-big">
            {known ? <><b>{months!.toLocaleString("id-ID", { maximumFractionDigits: 1 })}</b> bulan biaya pokok</> : <b className="!text-base">Belum dapat dihitung</b>}
          </div>
          {known && coverage != null && (
            <div className="ws-bar mt-3" aria-hidden="true"><i style={{ width: `${coverage}%` }} /></div>
          )}
          <div className="ws-dana-foot">
            <span><b>{money(runway.emergency_fund_balance ?? 0)}</b> {basisWord}</span>
            {target != null && <span>target <b>{money(target)}</b></span>}
          </div>
          <dl className={cn(styles.detailFacts, styles.factsTwo)}>
            <div><dt>Biaya pokok / bulan</dt><dd>{runway.monthly_primary_expense != null ? money(runway.monthly_primary_expense) : "Belum diketahui"}</dd></div>
            <div><dt>Jika pemasukan berhenti</dt><dd>{known ? `${runway.runway_days!.toLocaleString("id-ID")} hari` : "—"}</dd></div>
          </dl>
          {!known && <p className="ws-caption">Biaya pokok bulanan belum dapat diperkirakan dari riwayat.</p>}
          {runway.balance_basis === "all_liquid" && <p className="ws-caption">{ALL_LIQUID_NOTICE}</p>}
        </>
      )}
    </Card>
  );
}

function GoalDrawer({ goal, money, dialogs, onClose }: { goal: GoalItem | null; money: (value: number) => string; dialogs: ReturnType<typeof useGoalDialogs>; onClose: () => void }) {
  if (!goal) return null;
  const standalone = !goal.linked_accounts?.length;
  return (
    <Modal open onClose={onClose} title={goal.name}>
      <div className={styles.drawerBody}>
        <p className="ws-caption">Target tabungan</p>
        <div className={styles.drawerBalance}><span className="ws-caption">Terkumpul</span><b>{money(goal.current_amount)}</b></div>
        <dl className={styles.drawerFacts}>
          <dt>Target</dt><dd>{money(goal.target_amount)}</dd>
          <dt>Sisa</dt><dd>{money(goal.remaining_amount)}</dd>
          {goal.target_date && (<><dt>Sampai</dt><dd>{shortDate(goal.target_date)}</dd></>)}
          {goal.monthly_target_pace != null && (<><dt>Perlu per bulan</dt><dd>{money(goal.monthly_target_pace)}</dd></>)}
          <dt>Sumber progres</dt><dd>{standalone ? "Dicatat manual" : goal.linked_accounts!.map((account) => account.name).join(", ")}</dd>
        </dl>
        <div className={styles.drawerActions} role="group" aria-label={`Tindakan ${goal.name}`}>
          {standalone && (
            <button type="button" className="ws-btn ws-btn-ink" onClick={() => dialogs.openDeposit(goal)}>Tambah progres</button>
          )}
          {standalone && <p className="ws-caption">Progres manual tidak membuat transaksi di rekening.</p>}
          <button type="button" className="ws-btn" onClick={() => dialogs.openEditGoal(goal)}>Ubah target</button>
          <ConfirmActionButton
            label={`Arsipkan target ${goal.name}`}
            confirmation={`Arsipkan target "${goal.name}"? Saldo rekening tidak berubah.`}
            onConfirm={() => { dialogs.archiveGoal.mutate(goal.id); onClose(); }}
            className="ws-btn ws-btn-danger"
          >
            Arsipkan target
          </ConfirmActionButton>
        </div>
      </div>
    </Modal>
  );
}

function DebtDrawer({ debt, money, dialogs, onClose }: { debt: ObligationItem | null; money: (value: number) => string; dialogs: ReturnType<typeof useGoalDialogs>; onClose: () => void }) {
  if (!debt) return null;
  return (
    <Modal open onClose={onClose} title={debt.name}>
      <div className={styles.drawerBody}>
        <p className="ws-caption">Utang</p>
        <div className={styles.drawerBalance}><span className="ws-caption">Sisa</span><b>{money(debt.remaining_amount)}</b></div>
        <dl className={styles.drawerFacts}>
          <dt>Total</dt><dd>{money(debt.total_amount)}</dd>
          <dt>Terbayar</dt><dd>{money(debt.paid_amount)}</dd>
          {debt.minimum_payment != null && (<><dt>Minimal per bulan</dt><dd>{money(debt.minimum_payment)}</dd></>)}
          {debt.due_date && (<><dt>Jatuh tempo</dt><dd>{shortDate(debt.due_date)}</dd></>)}
          {debt.notes && (<><dt>Catatan</dt><dd>{debt.notes}</dd></>)}
        </dl>
        <div className={styles.drawerActions} role="group" aria-label={`Tindakan ${debt.name}`}>
          <button type="button" className="ws-btn ws-btn-ink" onClick={() => dialogs.openPay(debt)}>Kaitkan atau catat pembayaran</button>
          <button type="button" className="ws-btn" onClick={() => dialogs.openEditObligation(debt)}>Ubah utang</button>
          <ConfirmActionButton
            label={`Arsipkan utang ${debt.name}`}
            confirmation={`Arsipkan utang "${debt.name}"?`}
            onConfirm={() => { dialogs.archiveObligation.mutate(debt.id); onClose(); }}
            className="ws-btn ws-btn-danger"
          >
            Arsipkan utang
          </ConfirmActionButton>
        </div>
      </div>
    </Modal>
  );
}
