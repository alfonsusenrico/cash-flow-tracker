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
import { coverageText, essentialBasisText, ALL_LIQUID_NOTICE } from "@/components/dashboard/EmergencyCoverage";
import { Card, CardHeader, ErrorState, Pill, Skeleton, type Tone } from "@/components/workspace/primitives";
import { Drawer } from "@/components/workspace/overlays";
import type { CycleSummary, PlanLine } from "@/components/beranda/types";
import type { NetWorthResponse } from "@/components/accounts/types";
import styles from "@/components/accounts/money.module.css";
import { useGoalDialogs } from "./GoalDialogs";
import type { GoalItem, ObligationItem } from "./types";

// The plans editor loads when it is opened, so the page's first load does not carry it (gate D18).
const RecurringRulesModal = dynamic(() => import("@/components/recurring/RecurringRulesModal").then((module) => module.RecurringRulesModal), { ssr: false });

const DAY_MONTH = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });
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
  const [archivedOpen, setArchivedOpen] = useState(false);
  const summaryQuery = useQuery<CycleSummary>({ queryKey: queryKeys.dashboard.cycleSummary, queryFn: () => api.get("/dashboard/cycle-summary") });
  const netWorthQuery = useQuery<NetWorthResponse>({ queryKey: queryKeys.dashboard.netWorth, queryFn: () => api.get("/dashboard/net-worth") });

  const goalId = search.get("target");
  const debtId = search.get("utang");
  const selectedGoal = goalId ? dialogs.goals.find((goal) => goal.id === goalId) ?? null : null;
  const selectedDebt = debtId ? dialogs.obligations.find((debt) => debt.id === debtId) ?? null : null;
  const open = (key: "target" | "utang", id: string | null) => replaceSearch(setParam(pathname, search, key, id));
  const active = dialogs.obligations.filter((debt) => !debt.is_archived && debt.remaining_amount > 0);
  const archived = dialogs.obligations.filter((debt) => debt.is_archived || debt.remaining_amount <= 0);

  const toolbar = (
    <div className={styles.toolbar}>
      <span className={styles.grow} />
      <button type="button" className="ws-btn" onClick={() => setPlansOpen(true)}>
        <Icon name="calendar" className="h-4 w-4" />
        Rencana
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openNewObligation}>
        <Icon name="plus" className="h-4 w-4" />
        Tambah utang
      </button>
      <button type="button" className="ws-btn ws-btn-ink" onClick={dialogs.openNewGoal}>
        <Icon name="plus" className="h-4 w-4" />
        Tambah target
      </button>
    </div>
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
      <CardHeader id="goals-list" icon="target" title="Target tabungan" count={dialogs.goals.length} countLabel={`${dialogs.goals.length} target`}
        caption="Setoran target yang bertanggal sudah dihitung dalam Terjadwal di Beranda." />
      {dialogs.goals.length === 0 ? (
        <div className={styles.emptyBlock}>
          <p className="ws-caption">Belum ada target tabungan.</p>
          <button type="button" className="ws-btn" onClick={dialogs.openNewGoal}>Buat target pertama</button>
        </div>
      ) : (
        <ul className={styles.goalList}>
          {dialogs.goals.map((goal) => {
            const status = goalStatus(goal, bal);
            const percent = Math.max(0, Math.min(100, Math.round(goal.percentage_completed)));
            return (
              <li key={goal.id} className={styles.goal}>
                <div className={styles.goalTop}>
                  <button type="button" className="ws-rowbtn" onClick={() => open("target", goal.id)}>{goal.name}</button>
                  <Pill tone={status.tone}>{status.text}</Pill>
                </div>
                <div className={styles.progress} aria-hidden="true">
                  <i style={{ width: `${percent}%`, background: goal.color || "var(--lime)" }} />
                </div>
                <div className={styles.goalMeta}>
                  <span>{bal(goal.current_amount)} dari {bal(goal.target_amount)} · {percent}%</span>
                  <span>
                    {goal.linked_accounts?.length
                      ? `Dari ${goal.linked_accounts.map((account) => account.name).join(", ")}`
                      : "Dicatat manual"}
                    {goal.target_date ? ` · sampai ${MONTH_YEAR.format(new Date(goal.target_date))}` : ""}
                  </span>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );

  const billsCard = <BillsCard query={summaryQuery} money={bal} onPlans={() => setPlansOpen(true)} />;
  const runwayCard = <RunwayCard query={netWorthQuery} money={bal} />;

  const debtsCard = (
    <Card labelledBy="goals-debts">
      <CardHeader id="goals-debts" icon="credit-card" title="Utang" count={active.length} countLabel={`${active.length} utang aktif`} />
      {active.length === 0 ? (
        <p className="ws-caption">Belum ada utang aktif.</p>
      ) : (
        <ul className={styles.goalList}>
          {active.map((debt) => {
            const percent = Math.max(0, Math.min(100, Math.round(debt.payoff_percentage)));
            return (
              <li key={debt.id} className={styles.goal}>
                <div className={styles.goalTop}>
                  <button type="button" className="ws-rowbtn" onClick={() => open("utang", debt.id)}>{debt.name}</button>
                  {debt.due_date && <Pill tone="warn">Jatuh tempo {shortDate(debt.due_date)}</Pill>}
                </div>
                <div className={styles.progress} aria-hidden="true"><i style={{ width: `${percent}%`, background: "var(--tr-fg)" }} /></div>
                <div className={styles.goalMeta}>
                  <span>Terbayar {bal(debt.paid_amount)} dari {bal(debt.total_amount)} · {percent}%</span>
                  <span>Sisa {bal(debt.remaining_amount)}{debt.minimum_payment ? ` · minimal ${bal(debt.minimum_payment)}` : ""}</span>
                </div>
              </li>
            );
          })}
        </ul>
      )}
      <p className="ws-caption">Utang yang lunas otomatis pindah ke arsip. Jika pembayarannya dibatalkan, utang aktif kembali.</p>
      {archived.length > 0 && (
        <div className={styles.archived}>
          <button type="button" className="ws-linkbtn" aria-expanded={archivedOpen} onClick={() => setArchivedOpen((value) => !value)}>
            {archivedOpen ? "Sembunyikan" : "Tampilkan"} {archived.length} utang lunas
          </button>
          {archivedOpen && (
            <ul className={styles.archivedList}>
              {archived.map((debt) => (
                <li key={debt.id}><span>{debt.name}</span><span>{bal(debt.total_amount)}</span></li>
              ))}
            </ul>
          )}
        </div>
      )}
    </Card>
  );

  return (
    <div className={styles.root}>
      {toolbar}
      <div className={styles.grid}>
        <div className={cn(styles.stack, styles.summaryFirst)}>
          {runwayCard}
          {debtsCard}
        </div>
        <div className={styles.stack}>
          {goalsCard}
          {billsCard}
        </div>
      </div>
      <GoalDrawer goal={selectedGoal} money={bal} dialogs={dialogs} onClose={() => open("target", null)} />
      <DebtDrawer debt={selectedDebt} money={bal} dialogs={dialogs} onClose={() => open("utang", null)} />
      {plansOpen && <RecurringRulesModal open onClose={() => setPlansOpen(false)} />}
      {dialogs.dialogs}
    </div>
  );
}

function BillsCard({ query, money, onPlans }: { query: { data?: CycleSummary; isLoading: boolean; isError: boolean; refetch: () => unknown }; money: (value: number) => string; onPlans: () => void }) {
  const data = query.data;
  const title = data?.scheduled.window_basis === "next_30_days" ? "Tagihan 30 hari ke depan" : "Tagihan sampai gaji berikutnya";
  const plans = (data?.scheduled.lines ?? []).filter((line): line is PlanLine => line.kind === "plan");
  return (
    <Card labelledBy="goals-bills">
      <CardHeader id="goals-bills" icon="repeat" title={title} count={data ? plans.length : undefined} countLabel={`${plans.length} tagihan`}
        action={<button type="button" className="ws-btn ws-btn-sm" onClick={onPlans}>Kelola rencana</button>} />
      {query.isLoading ? (
        <Skeleton style={{ height: 120 }} />
      ) : query.isError || !data ? (
        <ErrorState message="Tagihan belum dapat dimuat." onRetry={() => query.refetch()} />
      ) : plans.length === 0 ? (
        <p className="ws-caption">Tidak ada tagihan sampai {data.scheduled.window_basis === "next_30_days" ? "30 hari ke depan" : "gaji berikutnya"}.</p>
      ) : (
        <table className={cn("ws-tbl", styles.table)}>
          <caption className="sr-only">{title}</caption>
          <thead><tr><th scope="col">Tagihan</th><th scope="col">Jatuh tempo</th><th scope="col" className="r">Jumlah</th></tr></thead>
          <tbody>
            {plans.map((line) => (
              <tr key={`${line.rule_id}-${line.due_date}`}>
                <td>{line.label}</td>
                <td>{shortDate(line.due_date)}{line.overdue ? " · terlewat" : ""}</td>
                <td className={cn("r", styles.amount)}>{money(line.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><th scope="row" colSpan={2}>Total</th><td className={cn("r", styles.amount)}>{money(data.scheduled.plans_total ?? 0)}</td></tr>
          </tfoot>
        </table>
      )}
    </Card>
  );
}

function RunwayCard({ query, money }: { query: { data?: NetWorthResponse; isLoading: boolean; isError: boolean; refetch: () => unknown }; money: (value: number) => string }) {
  const runway = query.data?.runway;
  const months = runway ? runway.coverage_months ?? runway.runway_months : null;
  const known = runway != null && months != null && runway.runway_days != null;
  return (
    <Card labelledBy="goals-runway">
      <h2 id="goals-runway" className={styles.plainTitle}>Ketahanan Dana</h2>
      {query.isLoading ? (
        <Skeleton style={{ height: 120 }} />
      ) : query.isError || !runway ? (
        <ErrorState message="Ketahanan Dana belum dapat dihitung." onRetry={() => query.refetch()} />
      ) : (
        <>
          <div className="ws-bigline">
            <span>{known ? `${months!.toLocaleString("id-ID")} bulan` : "Belum dapat dihitung"}</span>
          </div>
          <p className="ws-caption">{known ? `${coverageText(runway)} jika pemasukan berhenti.` : "Biaya pokok bulanan belum dapat diperkirakan dari riwayat."}</p>
          {known && (
            <div className={styles.runway} aria-hidden="true">
              {Array.from({ length: 6 }, (_, index) => (
                <i key={index} className={cn(index + 1 <= months! && styles.on)} />
              ))}
            </div>
          )}
          <dl className={styles.facts}>
            <dt>Dana yang dihitung</dt>
            <dd>{money(runway.emergency_fund_balance ?? 0)}</dd>
          </dl>
          <p className="ws-caption">{runway.balance_basis === "all_liquid" ? ALL_LIQUID_NOTICE : essentialBasisText(runway.essential_cost)}</p>
        </>
      )}
    </Card>
  );
}

function GoalDrawer({ goal, money, dialogs, onClose }: { goal: GoalItem | null; money: (value: number) => string; dialogs: ReturnType<typeof useGoalDialogs>; onClose: () => void }) {
  if (!goal) return null;
  const standalone = !goal.linked_accounts?.length;
  return (
    <Drawer open onClose={onClose} title={goal.name} subtitle="Target tabungan">
      <div className={styles.drawerBody}>
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
    </Drawer>
  );
}

function DebtDrawer({ debt, money, dialogs, onClose }: { debt: ObligationItem | null; money: (value: number) => string; dialogs: ReturnType<typeof useGoalDialogs>; onClose: () => void }) {
  if (!debt) return null;
  return (
    <Drawer open onClose={onClose} title={debt.name} subtitle="Utang">
      <div className={styles.drawerBody}>
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
    </Drawer>
  );
}
