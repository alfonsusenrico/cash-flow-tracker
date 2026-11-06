"use client";

import { useRef, useState } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { ExplanationDialog } from "@/components/workspace/ExplanationDialog";
import { FigureBlock, HeroFigure, SplitBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { type Money, safeAmountRincian, shortDate, unavailableReason } from "./explanations";
import type { CycleSummary } from "./types";

const MONTH_NAME = new Intl.DateTimeFormat("id-ID", { month: "long", timeZone: "UTC" });

function monthStart(summary: CycleSummary): string {
  return `sejak 1 ${MONTH_NAME.format(new Date(`${summary.period.start_date}T00:00:00Z`))}`;
}

/** The hero's one line (D3): the days to the expected salary, by how much the period is over, or why not known. */
export function heroLine(summary: CycleSummary, money: Money): string {
  const { safe_to_spend: safe, remaining, expected_salary: expected } = summary;
  if (remaining.total < 0) {
    const period = summary.period.basis === "cycle" ? "Siklus ini" : "Bulan ini";
    return `${period} sudah lebih ${money(-remaining.total)} dari uang masuk`;
  }
  if (safe.status === "available" && safe.days_left !== null && expected.date) {
    return `${safe.days_left} hari lagi sampai gajian ${shortDate(expected.date)}`;
  }
  return unavailableReason(summary).line;
}

/** The supporting figures' captions (D4, owner decisions 9 and 10). */
export function legendCaptions(summary: CycleSummary) {
  const cycle = summary.period.basis === "cycle";
  const start = shortDate(summary.period.start_date);
  const savingTotal = summary.tabungan ? summary.tabungan.total : summary.fresh_saving.amount;
  const setAsides = summary.scheduled.lines.some((line) => line.kind === "goal" && line.amount > 0);
  return {
    masuk: cycle ? `Gaji ${start}` : monthStart(summary),
    spending: cycle ? `sejak gaji ${start}` : monthStart(summary),
    saving: savingTotal < 0 ? "Tabungan berkurang" : "Tabungan bertambah",
    scheduled: `${setAsides ? "tagihan & setoran" : "tagihan"} sampai ${shortDate(summary.scheduled.window_end)}`,
  };
}

/**
 * "Aman dipakai sampai gajian" (D3 to D5): the page's one 44 px figure with its line and "Rincian", the split bar
 * and the four supporting figures. Every figure is a server field; the client only orders and formats them.
 */
export function BerandaHero({ summary }: { summary: CycleSummary }) {
  const { bal } = useAppCtx();
  const money: Money = (amount) => bal(amount);
  const [open, setOpen] = useState(false);
  const opener = useRef<HTMLButtonElement>(null);
  const { masuk, spending, fresh_saving: saving, scheduled, remaining } = summary;
  const free = Math.max(0, remaining.total);
  const captions = legendCaptions(summary);
  const word = summary.period.basis === "cycle" ? "siklus ini" : "bulan ini";
  const close = () => {
    setOpen(false);
    opener.current?.focus();
  };
  const segments = [
    { key: "spending", label: "Pengeluaran", amount: spending.amount, color: "var(--out-solid)" },
    // A savings decrease takes no width in the bar; its caption names the direction in words.
    { key: "saving", label: "Ditabung & investasi", amount: Math.max(0, saving.amount), color: "var(--inv-solid)" },
    { key: "scheduled", label: "Terjadwal", amount: scheduled.total, color: "var(--warn-solid)" },
    { key: "safe", label: "Aman dipakai", amount: free, color: "var(--in-solid)" },
  ];
  const uang = `Uang ${word}`;
  return (
    <HeroFigure
      id="beranda-safe"
      label="Aman dipakai sampai gajian"
      mark="var(--in-solid)"
      action={
        <button
          ref={opener}
          type="button"
          className="ws-btn ws-btn-quiet ws-btn-sm"
          aria-haspopup="dialog"
          aria-expanded={open}
          aria-label="Rincian aman dipakai sampai gajian"
          onClick={() => setOpen(true)}
        >
          Rincian
        </button>
      }
      figure={<MaskedMoney text={money(free)} />}
      line={heroLine(summary, money)}
    >
      <SplitBar label={`${uang} dari ${money(masuk.total)} masuk`} emptyLabel={`${uang}: belum ada uang masuk`} total={masuk.total} segments={segments} />
      <div className="legend">
        <FigureBlock label="Masuk" figure={<MaskedMoney text={money(masuk.total)} />} caption={captions.masuk} phoneHidden />
        <FigureBlock label="Pengeluaran" mark="var(--out-solid)" figure={<MaskedMoney text={money(spending.amount)} />} caption={captions.spending} />
        <FigureBlock
          label="Ditabung & investasi"
          mark="var(--inv-solid)"
          figure={<MaskedMoney text={money(Math.abs(saving.amount))} />}
          caption={captions.saving}
        />
        <FigureBlock label="Terjadwal" mark="var(--warn-solid)" figure={<MaskedMoney text={money(scheduled.total)} />} caption={captions.scheduled} phoneHidden />
      </div>
      <ExplanationDialog open={open} onClose={close} data={open ? safeAmountRincian(summary, money) : null} />
    </HeroFigure>
  );
}
