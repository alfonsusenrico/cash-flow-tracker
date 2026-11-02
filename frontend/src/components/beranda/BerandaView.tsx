"use client";

import Link from "next/link";
import { useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { EmergencyRunway, essentialBasisText } from "@/components/dashboard/EmergencyCoverage";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { Icon } from "@/components/ui/Icon";
import { usePageHeader } from "@/components/workspace/context";
import { ExplainableFigure } from "@/components/workspace/explain";
import { Drawer } from "@/components/workspace/overlays";
import { Card, CardHeader, DeltaChip, ErrorState, Kpi, KpiStrip, MoneyBar, Pill, Skeleton } from "@/components/workspace/primitives";
import { CompanionStatus } from "@/components/workspace/SideNav";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { api } from "@/lib/api";
import { KAKEIBO_BUCKETS } from "@/lib/kakeibo";
import { consolidateLedgerMovements } from "@/lib/ledgerMovements";
import { queryKeys } from "@/lib/queryKeys";
import type { ReviewQueue } from "@/lib/review";
import { greetingName } from "@/lib/utils";
import type { AutoLinkedOccurrence, DueExpectation, RecurringRule } from "@/types/recurring";
import { ACTIVITY_LIMIT, ActivityCard, TodayList, jakartaDay } from "./activity";
import {
  type Money,
  keluarExplanation,
  longDate,
  masukExplanation,
  netWorthExplanation,
  periodWord,
  remainingExplanation,
  safeExplanation,
  scheduledExplanation,
  shortDate,
  signed,
  balance,
  unavailableReason,
  waitingExplanation,
} from "./explanations";
import { AutoRowsNote, TodoCard, TodoList, todoItems } from "./TodoCard";
import { TransactionDetailDialog } from "./TransactionDetailDialog";
import type { ActivityRow, CycleSummary } from "./types";

const SUMMARY_REFRESH_MS = 30_000;
// Plans due today, overdue, or due within this many days appear in "Kerjakan dulu".
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

interface PeriodComparison {
  compared_days: number;
  previous_label: string | null;
  previous_has_data: boolean;
  current_to_date: { outflow: number };
  categories: { increases: { name: string; change: number }[]; decreases: { name: string; change: number }[] };
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
 * Summary sentences: exactly three explainable numbers (keluar, safe-to-spend, waiting).
 * ---------------------------------------------------------------------------------------------- */

export function Lede({ summary, name }: { summary: CycleSummary; name: string | null }) {
  const money = useDisplayMoney();
  const { period, keluar, safe_to_spend: safe, waiting } = summary;
  const who = name ? `, ${name}` : "";
  const keluarText = money(keluar.total);
  const keluarFigure = (
    <ExplainableFigure label={`Keluar ${periodWord(summary)}`} amountText={keluar.total ? keluarText : "belum ada"} explanation={keluarExplanation(summary, money)}>
      {keluar.total ? keluarText : "belum ada yang keluar"}
    </ExplainableFigure>
  );
  const first =
    period.basis === "cycle" ? (
      <>
        Hari ke-{period.day_index} siklus gajimu{who}. Sejak gajian{keluar.total ? " kamu keluar " : " "}
        {keluarFigure}.
      </>
    ) : (
      <>
        Hari ke-{period.day_index} bulan {MONTH_NAME.format(new Date(`${period.start_date}T00:00:00Z`))}
        {who}. Bulan ini{keluar.total ? " kamu keluar " : " "}
        {keluarFigure}.
      </>
    );

  let second: React.ReactNode;
  if (safe.status === "available" && safe.per_day !== null && safe.per_day > 0) {
    const text = money(safe.per_day);
    second = (
      <>
        Hari ini aman pakai{" "}
        <ExplainableFigure label="Aman dipakai hari ini" amountText={text} explanation={safeExplanation(summary, money)} />.
      </>
    );
  } else if (safe.status === "available") {
    second = (
      <>
        Hari ini{" "}
        <ExplainableFigure label="Aman dipakai hari ini" amountText="nggak ada" explanation={safeExplanation(summary, money)}>
          belum ada yang aman dipakai
        </ExplainableFigure>
        , uang {periodWord(summary)} sudah terpakai semua.
      </>
    );
  } else {
    second = (
      <>
        Angka aman per hari{" "}
        <ExplainableFigure label="Aman dipakai hari ini" amountText="belum tersedia" explanation={safeExplanation(summary, money)}>
          belum bisa aku hitung
        </ExplainableFigure>
        : {unavailableReason(summary).short}.
      </>
    );
  }

  const third = waiting.count ? (
    <>
      {waiting.count} hal nunggu jawabanmu (
      <ExplainableFigure label="Nunggu jawabanmu" amountText={money(waiting.total)} explanation={waitingExplanation(summary, money)} />
      ).
    </>
  ) : (
    <>Semua transaksi udah beres, nggak ada yang nunggu jawaban.</>
  );

  return (
    <p className="ws-lede">
      {first} {second} {third}
    </p>
  );
}

/* ------------------------------------------------------------------------------------------------
 * KPI strip
 * ---------------------------------------------------------------------------------------------- */

function comparisonChip(summary: CycleSummary, key: "keluar" | "masuk", money: Money) {
  const { comparison } = summary;
  const change = comparison[key].change;
  if (comparison.status !== "available" || change === null) return undefined;
  const higherIsGood = key === "masuk";
  const tone = change === 0 ? "neutral" : change > 0 === higherIsGood ? "ok" : "warn";
  return (
    <DeltaChip tone={tone} title={`Dibanding ${comparison.compared_days} hari pertama ${comparison.previous_label ?? "periode lalu"}`}>
      {signed(change, money)}
    </DeltaChip>
  );
}

function comparisonCaption(summary: CycleSummary): string {
  const { comparison } = summary;
  if (comparison.status === "too_short") return "perbandingan muncul setelah 3 hari";
  if (comparison.status === "no_previous") return "belum ada data periode lalu";
  return `vs ${comparison.compared_days} hari pertama periode lalu`;
}

export function BerandaKpis({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const safe = summary.safe_to_spend;
  const available = safe.status === "available" && safe.per_day !== null;
  const word = periodWord(summary);
  return (
    <KpiStrip label={`Ringkasan ${word}`}>
      <Kpi
        icon="shield"
        label="Aman dipakai hari ini"
        valueIsWords={!available}
        value={
          <ExplainableFigure label="Aman dipakai hari ini" amountText={available ? money(safe.per_day!) : "belum tersedia"} explanation={safeExplanation(summary, money)}>
            {available ? <AnimatedMoney value={safe.per_day!} /> : "Belum bisa dihitung"}
          </ExplainableFigure>
        }
        caption={available ? `${safe.days_left} hari lagi sampai gajian` : unavailableReason(summary).short}
      />
      <Kpi
        icon="arrow-up-right"
        label={`Keluar ${word}`}
        value={
          <ExplainableFigure label={`Keluar ${word}`} amountText={money(summary.keluar.total)} explanation={keluarExplanation(summary, money)}>
            <AnimatedMoney value={summary.keluar.total} />
          </ExplainableFigure>
        }
        chip={comparisonChip(summary, "keluar", money)}
        caption={comparisonCaption(summary)}
      />
      <Kpi
        icon="arrow-down-left"
        label={`Masuk ${word}`}
        value={
          <ExplainableFigure label={`Masuk ${word}`} amountText={money(summary.masuk.total)} explanation={masukExplanation(summary, money)}>
            <AnimatedMoney value={summary.masuk.total} />
          </ExplainableFigure>
        }
        chip={comparisonChip(summary, "masuk", money)}
        caption={comparisonCaption(summary)}
      />
      <Kpi
        icon="accounts"
        label="Kekayaan bersih"
        value={
          <ExplainableFigure label="Kekayaan bersih" amountText={money(summary.net_worth.total)} explanation={netWorthExplanation(summary, money)}>
            <AnimatedMoney value={summary.net_worth.total} />
          </ExplainableFigure>
        }
        caption="aset dikurangi utang aktif"
      />
    </KpiStrip>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Uang siklus ini
 * ---------------------------------------------------------------------------------------------- */

export function MoneyCard({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const { keluar, scheduled, remaining, masuk, safe_to_spend: safe, waiting } = summary;
  const word = periodWord(summary);
  const plans = scheduled.lines.filter((line) => line.kind === "plan").length;
  const goals = scheduled.lines.length - plans;
  const saving = keluar.lines.find((line) => line.key === "saving");
  const over = remaining.total < 0;
  const free = over ? 0 : remaining.total;
  const remainingFigure = (
    <ExplainableFigure label="Sisa aman sampai gajian" amountText={balance(remaining.total, money)} explanation={remainingExplanation(summary, money)} />
  );
  return (
    <Card labelledBy="beranda-money">
      <CardHeader
        id="beranda-money"
        icon="layers"
        title={`Uang ${word}`}
        caption={
          <>
            Masuk {money(masuk.total)}. Setelah tagihan dan setoran target, sisa {remainingFigure}
            {safe.days_left ? ` buat ${safe.days_left} hari` : ""}.
          </>
        }
        action={
          <Link href="/goals" className="ws-btn ws-hide-narrow">
            <Icon name="calendar" className="h-4 w-4" />
            Lihat tagihan
            <span className="ws-cnt">{plans}</span>
          </Link>
        }
      />
      <div className="mt-5">
        <MoneyBar
          legend={false}
          segments={[
            { key: "used", label: "Udah dipakai", amount: keluar.total, display: money(keluar.total), className: "ws-seg-used" },
            { key: "scheduled", label: "Terjadwal", amount: scheduled.total, display: money(scheduled.total), className: "ws-seg-sched" },
            { key: "safe", label: "Aman dipakai", amount: free, display: money(free), className: "ws-seg-safe" },
          ]}
        />
        <ul className="ws-legend" aria-label={`Pembagian uang ${word}`}>
          <li>
            <span className="ws-legend-h" style={{ ["--sc" as string]: "var(--muted)" }}>
              Udah dipakai
            </span>
            <div className="ws-legend-v">
              <ExplainableFigure label="Udah dipakai" amountText={money(keluar.total)} explanation={keluarExplanation(summary, money)} />
            </div>
            {saving && saving.amount > 0 && (
              <div className="ws-legend-f">
                termasuk <b>{money(saving.amount)}</b> ditabung
              </div>
            )}
          </li>
          <li>
            <span className="ws-legend-h" style={{ ["--sc" as string]: "var(--warn-fg)" }}>
              Terjadwal
            </span>
            <div className="ws-legend-v">
              <ExplainableFigure label="Terjadwal" amountText={money(scheduled.total)} explanation={scheduledExplanation(summary, money)} />
            </div>
            <Pill tone="warn">
              {plans} tagihan + {goals} target
            </Pill>
          </li>
          <li>
            <span className="ws-legend-h" style={{ ["--sc" as string]: "var(--lime-text)" }}>
              Aman dipakai
            </span>
            <div className="ws-legend-v">{remainingFigure}</div>
            {safe.status === "available" && safe.per_day !== null && <Pill tone="ok">{money(safe.per_day)}/hari</Pill>}
            <div className="ws-legend-f">
              {safe.days_left ? (
                <>
                  <b>{safe.days_left} hari</b> lagi sampai gajian
                </>
              ) : (
                unavailableReason(summary).short
              )}
            </div>
          </li>
        </ul>
      </div>
      {over && (
        <p className="ws-caption mt-3 font-medium text-[var(--out-fg)]">
          {word[0].toUpperCase() + word.slice(1)} sudah lebih {money(-remaining.total)} dari uang masuk.
        </p>
      )}
      {waiting.count > 0 && (
        <p className="ws-caption mt-3 flex items-start gap-2">
          <Icon name="info" className="mt-0.5 h-4 w-4 shrink-0 text-[var(--warn-fg)]" />
          <span>
            {waiting.count} hal masih nunggu jawabanmu di Tinjau. {money(waiting.in_keluar)} di antaranya sudah ikut di Udah dipakai; jawabanmu bisa
            mengubah angka aman.
          </span>
        </p>
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
  const unclassified = summary.keluar.lines.find((line) => line.key === "unclassified");
  return (
    <Card labelledBy="beranda-kakeibo">
      <CardHeader
        id="beranda-kakeibo"
        title={`Kakeibo ${word}`}
        action={
          <Link href="/insights" className="ws-btn ws-btn-sm">
            Analisis
          </Link>
        }
      />
      <div className="ws-bigline">
        <ExplainableFigure label={`Keluar ${word}`} amountText={money(summary.keluar.total)} explanation={keluarExplanation(summary, money)} />
      </div>
      <div className="ws-stackbar mt-3.5" aria-hidden="true">
        {summary.keluar.lines
          .filter((line) => line.amount > 0)
          .map((line) => {
            const info = KAKEIBO_BUCKETS.find((bucket) => bucket.value === line.key);
            return <i key={line.key} style={{ flex: line.amount, background: info?.color }} />;
          })}
      </div>
      <ul className="ws-kk-legend">
        {summary.keluar.lines.map((line) => {
          const info = KAKEIBO_BUCKETS.find((bucket) => bucket.value === line.key);
          // Share of Keluar for display only; the amounts themselves come from the server.
          const pct = summary.keluar.total > 0 ? Math.round((line.amount / summary.keluar.total) * 100) : 0;
          return (
            <li key={line.key}>
              <span className="ws-kk" data-bucket={line.key} style={{ ["--kc" as string]: info?.color }}>
                {line.href ? <Link href={line.href}>{line.label}</Link> : line.label}
              </span>
              <span>{money(line.amount)}</span>
              <span className="pct">{pct}%</span>
            </li>
          );
        })}
      </ul>
      <p className="ws-caption mt-2">
        {unclassified && unclassified.amount > 0
          ? `${money(unclassified.amount)} belum diklasifikasi. Yang belum jelas aku tanya di Tinjau, nggak aku masukkan diam-diam ke Kebutuhan.`
          : "Belum diklasifikasi Rp 0. Yang belum jelas aku tanya di Tinjau, nggak aku masukkan diam-diam ke Kebutuhan."}
      </p>
    </Card>
  );
}

export function DanaCard({ overview, error }: { overview: Overview | undefined; error: boolean }) {
  const { bal } = useAppCtx();
  const kpis = overview?.kpis;
  const months = kpis ? kpis.coverage_months ?? kpis.runway_months : null;
  const status = kpis?.runway_status ?? kpis?.status;
  const unknown = !kpis || months == null || status === "unknown";
  const target = kpis?.target_months ?? null;
  return (
    <Card labelledBy="beranda-dana">
      <div className="ws-card-h">
        <div className="ws-tile ws-tile-sm" aria-hidden="true">
          <Icon name="shield" />
        </div>
        <div className="ws-card-t">
          <h2 id="beranda-dana" className="!text-base">
            Ketahanan Dana
          </h2>
        </div>
      </div>
      {error && <p className="ws-caption mt-3">Ketahanan Dana belum bisa dimuat.</p>}
      {kpis && unknown && (
        <p className="ws-caption mt-3">
          Belum bisa dihitung: biaya kebutuhan bulananmu belum diketahui. {essentialBasisText(kpis.essential_cost)}
        </p>
      )}
      {kpis && !unknown && (
        <>
          <div className="ws-ministats">
            <div className="ws-ministat">
              <div className="k">Bertahan</div>
              <div className="v">{months!.toLocaleString("id-ID", { maximumFractionDigits: 1 })} bulan</div>
              {target ? <div className="c">target {target} bulan</div> : null}
            </div>
            <div className="ws-ministat">
              <div className="k">{kpis.emergency_balance_basis === "all_liquid" ? "Saldo kas" : "Dana Darurat"}</div>
              <div className="v">{bal(kpis.emergency_fund_balance)}</div>
              {kpis.target_amount ? <div className="c">dari {bal(kpis.target_amount)}</div> : null}
            </div>
          </div>
          {kpis.coverage_pct != null && (
            <div className="ws-bar mt-3.5" aria-hidden="true" style={{ ["--bc" as string]: "var(--warn-fg)" }}>
              <i style={{ width: `${Math.min(100, Math.max(0, kpis.coverage_pct))}%` }} />
            </div>
          )}
          <p className="ws-caption mt-2.5">
            Kalau gaji berhenti, {kpis.emergency_balance_basis === "all_liquid" ? "saldo kasmu" : "Dana Darurat"} cukup buat{" "}
            {months!.toLocaleString("id-ID", { maximumFractionDigits: 1 })} bulan kebutuhan
            {kpis.monthly_primary_expense ? ` (${bal(kpis.monthly_primary_expense)}/bulan)` : ""}.
          </p>
        </>
      )}
    </Card>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Insight: the fair comparison, called "belanja" because it leaves saving moves out.
 * ---------------------------------------------------------------------------------------------- */

export function InsightBanner({
  summary,
  comparison,
  error,
  compact = false,
}: {
  summary: CycleSummary;
  comparison: PeriodComparison | undefined;
  error: boolean;
  compact?: boolean;
}) {
  const money = useDisplayMoney();
  let tone: "neutral" | "ok" | "warn" = "neutral";
  let head: string;
  let body: string | null = null;
  const status = summary.comparison.status;
  if (error) {
    head = "Perbandingan dengan periode lalu belum bisa dimuat.";
  } else if (status === "too_short") {
    head = `Baru ${summary.period.day_index} hari berjalan.`;
    body = "Perbandingan dengan periode lalu muncul setelah 3 hari.";
  } else if (status === "no_previous") {
    head = "Belum ada data periode lalu untuk dibandingkan.";
    body = comparison ? `Belanja sejauh ini ${money(comparison.current_to_date.outflow)}, di luar tabungan.` : null;
  } else {
    const change = summary.comparison.belanja.change ?? 0;
    const days = summary.comparison.compared_days;
    tone = change > 0 ? "warn" : change < 0 ? "ok" : "neutral";
    head =
      change === 0
        ? `Belanja ${days} hari pertama sama dengan periode lalu.`
        : `Belanja ${days} hari pertama ${money(Math.abs(change))} ${change > 0 ? "lebih banyak" : "lebih hemat"} dari periode lalu di hari yang sama.`;
    const up = comparison?.categories.increases[0];
    const down = comparison?.categories.decreases[0];
    const parts = [
      up ? `Naik paling besar: ${up.name} ${signed(up.change, money)}.` : null,
      down ? `Turun paling besar: ${down.name} ${signed(down.change, money)}.` : null,
    ].filter(Boolean);
    body = parts.length ? parts.join(" ") : null;
  }
  return (
    <section className={`ws-banner ws-banner-${tone}`} aria-label="Wawasan periode ini">
      <div className="ws-tile" aria-hidden="true">
        <Icon name="trending-up" />
      </div>
      <div className="flex-1">
        <b>{head}</b>
        {body && (
          <>
            {compact ? " " : <br />}
            {body}
          </>
        )}
      </div>
      {!compact && (
        <Link href="/insights" className="ws-btn ws-btn-sm ws-hide-narrow">
          Lihat Analisis
        </Link>
      )}
    </section>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Phone Beranda
 * ---------------------------------------------------------------------------------------------- */

export function PhoneHero({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const { safe_to_spend: safe, keluar, scheduled, remaining, waiting } = summary;
  const available = safe.status === "available" && safe.per_day !== null;
  const free = remaining.total > 0 ? remaining.total : 0;
  return (
    <section className="ws-ph-hero" aria-labelledby="ph-safe">
      <h2 id="ph-safe">
        <Icon name="shield" className="h-4 w-4" />
        Aman dipakai hari ini
      </h2>
      <div className={available ? "ws-ph-big" : "ws-ph-big is-words"}>
        <ExplainableFigure label="Aman dipakai hari ini" amountText={available ? money(safe.per_day!) : "belum tersedia"} explanation={safeExplanation(summary, money)}>
          {available ? money(safe.per_day!) : "Belum bisa dihitung"}
        </ExplainableFigure>
      </div>
      <p className="ws-ph-sub">
        Sisa{" "}
        <ExplainableFigure label="Sisa aman sampai gajian" amountText={balance(remaining.total, money)} explanation={remainingExplanation(summary, money)} />
        {safe.days_left ? ` buat ${safe.days_left} hari` : `, ${unavailableReason(summary).short}`}
      </p>
      <div className="ws-ph-bar" aria-hidden="true">
        {keluar.total > 0 && <i style={{ flex: keluar.total, background: "var(--surface-3)" }} />}
        {scheduled.total > 0 && <i style={{ flex: scheduled.total, background: "var(--warn-solid)" }} />}
        {free > 0 && <i style={{ flex: free, background: "var(--lime)" }} />}
      </div>
      <ul className="ws-ph-leg">
        <li style={{ ["--sc" as string]: "var(--muted)" }}>
          Dipakai
          <b>
            <ExplainableFigure label="Udah dipakai" amountText={money(keluar.total)} explanation={keluarExplanation(summary, money)} />
          </b>
        </li>
        <li style={{ ["--sc" as string]: "var(--warn-fg)" }}>
          Terjadwal
          <b>
            <ExplainableFigure label="Terjadwal" amountText={money(scheduled.total)} explanation={scheduledExplanation(summary, money)} />
          </b>
        </li>
        <li style={{ ["--sc" as string]: "var(--lime-text)" }}>
          Aman
          <b>{money(free)}</b>
        </li>
      </ul>
      <p className="ws-caption mt-3">
        {waiting.count ? (
          <>
            <ExplainableFigure label="Nunggu jawabanmu" amountText={money(waiting.total)} explanation={waitingExplanation(summary, money)} /> nunggu
            jawabanmu ({waiting.count} hal).
          </>
        ) : (
          "Nggak ada yang nunggu jawaban."
        )}
      </p>
    </section>
  );
}

function PhoneFigures({ summary }: { summary: CycleSummary }) {
  const money = useDisplayMoney();
  const word = periodWord(summary);
  const chip = (key: "keluar" | "masuk") => comparisonChip(summary, key, money);
  return (
    <div className="ws-ph-kpis">
      <div className="ws-card">
        <div className="ws-ph-k">Keluar</div>
        <div className="ws-ph-v">
          <ExplainableFigure label={`Keluar ${word}`} amountText={money(summary.keluar.total)} explanation={keluarExplanation(summary, money)} />
        </div>
        {chip("keluar")}
      </div>
      <div className="ws-card">
        <div className="ws-ph-k">Masuk</div>
        <div className="ws-ph-v">
          <ExplainableFigure label={`Masuk ${word}`} amountText={money(summary.masuk.total)} explanation={masukExplanation(summary, money)} />
        </div>
        {chip("masuk")}
      </div>
      <div className="ws-card ws-span-2">
        <div className="ws-ph-k">Kekayaan bersih</div>
        <div className="ws-ph-v">
          <ExplainableFigure label="Kekayaan bersih" amountText={money(summary.net_worth.total)} explanation={netWorthExplanation(summary, money)} />
        </div>
        <span className="ws-caption">aset dikurangi utang aktif</span>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------------------------------------
 * Page
 * ---------------------------------------------------------------------------------------------- */

export function headerFor(summary: CycleSummary | undefined, now: Date) {
  const title = WEEKDAY_DATE.format(now);
  if (!summary) return { title, subtitle: "" };
  const { period, expected_salary: expected } = summary;
  if (period.basis === "cycle") {
    const next = expected.date && expected.status === "estimated" ? ` · gajian berikutnya ± ${shortDate(expected.date)}` : "";
    return { title, subtitle: `Siklus gaji hari ke-${period.day_index} · sejak ${shortDate(period.start_date)}${next}` };
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
  const { user, bal } = useAppCtx();
  const [now] = useState(() => new Date());
  const today = jakartaDay(now);
  const [selectedTx, setSelectedTx] = useState<ActivityRow | null>(null);
  const [plansOpen, setPlansOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const deskRef = useRef<HTMLDivElement>(null);
  // Same width as `@container main (max-width: 900px)`, where the grid becomes one column.
  // Kerjakan dulu then moves up in the DOM, so reading and focus order follow the layout.
  const singleColumn = useWidthAtMost(deskRef, 900);

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

  const comparisonQuery = useQuery<PeriodComparison>({
    queryKey: queryKeys.dashboard.periodComparison(basis, 0),
    queryFn: () => api.get(`/dashboard/period-comparison?timeframe=${basis}&cycle_offset=0`),
    enabled: Boolean(summary),
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
  const name = greetingName(user);

  const summaryFailed = summaryQuery.isError;
  const retry = () => summaryQuery.refetch();

  const noCycleNote =
    summary?.period.basis === "month" ? (
      <p className="ws-caption -mt-2 mb-4">
        Siklus gaji belum tersedia, jadi Beranda pakai bulan kalender.{" "}
        <Link href="/settings/gaji" className="ws-linkbtn">
          Atur gaji di Pengaturan
        </Link>
      </p>
    ) : null;

  const todo = <TodoCard attention={attention} items={items} autoToday={autoToday} error={queueQuery.isError} />;
  const dana = <DanaCard overview={overviewQuery.data} error={overviewQuery.isError} />;

  return (
    <div>
      {/* Desktop and tablet */}
      <div className="ws-only-desk" ref={deskRef}>
        {summaryQuery.isLoading && <DesktopSkeleton />}
        {summaryFailed && (
          <div className="my-4">
            <ErrorState message="Angka Beranda belum bisa dimuat. Saldo dan transaksimu aman; coba muat lagi." onRetry={retry} />
          </div>
        )}
        {summary && (
          <>
            <Lede summary={summary} name={name} />
            {noCycleNote}
            <BerandaKpis summary={summary} />
          </>
        )}
        {(summary || summaryFailed) && (
          <div className="ws-home-grid">
            <div className="ws-stack">
              {summary && <MoneyCard summary={summary} />}
              {singleColumn && todo}
              <ActivityCard rows={recent} today={today} bal={bal} onSelect={setSelectedTx} error={activityQuery.isError} />
              {summary && <InsightBanner summary={summary} comparison={comparisonQuery.data} error={comparisonQuery.isError} />}
            </div>
            <div className="ws-stack">
              {!singleColumn && todo}
              {summary && <KakeiboCard summary={summary} />}
              {dana}
            </div>
          </div>
        )}
      </div>

      {/* Phones: a single column built for one hand */}
      <div className="ws-only-phone">
        <div className="ws-ph-greet">
          <CompanionStatus />
        </div>
        {summaryQuery.isLoading && <Skeleton className="h-[230px] rounded-[22px]" />}
        {summaryFailed && <ErrorState message="Angka Beranda belum bisa dimuat." onRetry={retry} />}
        {summary && <PhoneHero summary={summary} />}
        {noCycleNote && <div className="mt-4">{noCycleNote}</div>}

        <div className="ws-ph-sect">
          <h2>Kerjakan dulu</h2>
          {attention > 0 && <Link href="/review">Tinjau {attention}</Link>}
        </div>
        {items.length ? (
          <div className="ws-ph-list">
            <TodoList items={items} />
          </div>
        ) : (
          <div className="ws-ph-list">
            <p className="py-3 text-[14px]">Tinjau udah beres. Nggak ada yang perlu dijawab.</p>
          </div>
        )}
        <AutoRowsNote count={autoToday} />

        {summary && (
          <>
            <div className="ws-ph-sect">
              <h2>{periodWord(summary) === "siklus ini" ? "Siklus ini" : "Bulan ini"}</h2>
              <Link href="/insights">Analisis</Link>
            </div>
            <PhoneFigures summary={summary} />
          </>
        )}

        <div className="ws-ph-sect">
          <h2>Hari ini</h2>
          <Link href="/ledger">Semua</Link>
        </div>
        {activityQuery.isError ? (
          <p className="ws-caption">Aktivitas belum bisa dimuat.</p>
        ) : (
          <TodayList rows={todayRows.slice(0, ACTIVITY_LIMIT)} today={today} bal={bal} onSelect={setSelectedTx} />
        )}

        {summary && (
          <div className="mt-4">
            <InsightBanner summary={summary} comparison={comparisonQuery.data} error={comparisonQuery.isError} compact />
          </div>
        )}
      </div>

      <TransactionDetailDialog selectedTx={selectedTx} onClose={() => setSelectedTx(null)} />
      <Drawer open={plansOpen} onClose={() => setPlansOpen(false)} title="Rencana jatuh tempo" subtitle="Tautkan, lewati, atau catat manual">
        <PendingScheduledBanner onOpenRulesManager={() => setRulesOpen(true)} />
        {!(pendingQuery.data?.rules ?? []).length && !(pendingQuery.data?.recent_auto_links ?? []).length && (
          <p className="ws-caption">Nggak ada rencana yang jatuh tempo hari ini.</p>
        )}
      </Drawer>
      <RecurringRulesModal open={rulesOpen} onClose={() => setRulesOpen(false)} />
    </div>
  );
}

function DesktopSkeleton() {
  return (
    <div aria-busy="true" aria-label="Memuat Beranda">
      <Skeleton className="my-3 h-[86px] max-w-[880px]" />
      <Skeleton className="h-[110px] rounded-[20px]" />
      <div className="ws-home-grid">
        <div className="ws-stack">
          <Skeleton className="h-[300px] rounded-[20px]" />
          <Skeleton className="h-[420px] rounded-[20px]" />
        </div>
        <div className="ws-stack">
          <Skeleton className="h-[320px] rounded-[20px]" />
          <Skeleton className="h-[300px] rounded-[20px]" />
        </div>
      </div>
    </div>
  );
}

export { longDate };
