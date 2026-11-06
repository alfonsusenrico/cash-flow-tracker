import type { ExplanationData, ExplanationLine } from "@/components/workspace/explain";
import type { RincianData } from "@/components/workspace/ExplanationDialog";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import type { CycleSummary, ExpectedReason, SummaryLine, WaitingLine, WaitingSection } from "./types";

/**
 * "Rincian hitungan" content for the Beranda hero. Everything here formats the server's figures and lines;
 * nothing is added up on the client (design D13), so the web and any later view cannot drift.
 */
export type Money = (amount: number) => string;

const SHORT = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "UTC" });
const LONG = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "long", timeZone: "UTC" });

export function shortDate(iso: string): string {
  return SHORT.format(new Date(`${iso.slice(0, 10)}T00:00:00Z`));
}

export function longDate(iso: string): string {
  return LONG.format(new Date(`${iso.slice(0, 10)}T00:00:00Z`));
}

export function signed(amount: number, money: Money): string {
  if (amount > 0) return `+${money(amount)}`;
  if (amount < 0) return `−${money(-amount)}`;
  return money(0);
}

/** A balance: no plus sign, a minus only when below zero. */
export function balance(amount: number, money: Money): string {
  return amount < 0 ? `−${money(-amount)}` : money(amount);
}

function lineOf(line: SummaryLine, money: Money, withHref = true): ExplanationLine {
  return { label: line.label, note: line.note ?? null, amount: money(line.amount), href: withHref ? line.href ?? null : null };
}

export function periodWord(summary: CycleSummary): string {
  return summary.period.basis === "cycle" ? "siklus ini" : "bulan ini";
}

function ledgerPeriod(summary: CycleSummary): string {
  return `/ledger?period=${summary.period.cycle_offset_basis}`;
}

const SECTION_LABELS: Record<WaitingSection, string> = {
  questions: "Belum dikenali",
  ambiguous: "Pasangan pindah saldo belum pasti",
  awaiting: "Pindah saldo belum lengkap",
  groups: "Perlu jawaban",
};

export function spendingExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const notes = ["Pindah saldo antar rekening sendiri dan uang yang ditabung atau diinvestasikan tidak dihitung pengeluaran."];
  if (summary.keluar.offsets_total > 0) notes.push(`Sudah dikurangi ganti uang ${money(summary.keluar.offsets_total)}.`);
  if (summary.waiting.in_keluar > 0) {
    notes.push(`${money(summary.waiting.in_keluar)} dari yang menunggu jawaban sudah termasuk di sini. Jawabannya hanya memindahkan pilarnya.`);
  }
  return {
    title: `Pengeluaran ${periodWord(summary)}`,
    rule: "Semua pengeluaran periode ini menurut pilar Kakeibo, tanpa yang ditabung, dikurangi ganti uang.",
    lines: summary.spending.lines.map((line) => lineOf(line, money)),
    total: { label: "Total pengeluaran", amount: money(summary.spending.amount) },
    notes,
    link: { href: `${ledgerPeriod(summary)}&type=expense`, label: "Lihat pengeluaran" },
  };
}

export function savingExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const parts = summary.tabungan;
  if (parts) {
    const lines: ExplanationLine[] = [
      { label: "Masuk ke tabungan", amount: signed(parts.masuk, money) },
      { label: "Diambil dari tabungan", amount: signed(-parts.diambil, money) },
      { label: "Pengeluaran dari tabungan", note: "tetap dihitung sebagai pengeluaran", amount: signed(-parts.belanja, money) },
    ];
    if (parts.pilar_tabungan) lines.push({ label: "Pengeluaran berpilar Tabungan", amount: signed(parts.pilar_tabungan, money) });
    return {
      title: "Ditabung & investasi",
      rule: "Uang yang masuk ke kantong tabungan atau investasi, dikurangi yang diambil dan yang dibelanjakan dari tabungan.",
      lines,
      total: { label: parts.total < 0 ? "Tabungan berkurang" : "Tabungan bertambah", amount: money(Math.abs(parts.total)) },
      notes: [`Bersama pengeluaran ${money(summary.spending.amount)}, totalnya ${money(summary.keluar.total)}.`],
      link: { href: `${ledgerPeriod(summary)}&kakeibo=saving`, label: "Lihat yang ditabung" },
    };
  }
  return {
    title: "Ditabung & investasi",
    rule: "Pengeluaran berpilar Tabungan ditambah uang bersih yang dipindah ke kantong tabungan atau diinvestasikan.",
    lines: summary.fresh_saving.lines.map((line) => lineOf(line, money)),
    total: { label: "Total ditabung", amount: money(summary.fresh_saving.amount) },
    notes: [`Bersama pengeluaran ${money(summary.spending.amount)}, totalnya ${money(summary.keluar.total)}.`],
    link: { href: `${ledgerPeriod(summary)}&kakeibo=saving`, label: "Lihat yang ditabung" },
  };
}

export function masukExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const notes = ["Pindah saldo antar rekening sendiri dan uang pengganti tidak dihitung masuk."];
  return {
    title: `Masuk ${periodWord(summary)}`,
    rule: "Uang yang masuk di periode ini, per kategori.",
    lines: summary.masuk.lines.map((line) => lineOf(line, money)),
    total: { label: "Total masuk", amount: money(summary.masuk.total) },
    notes,
    link: { href: `${ledgerPeriod(summary)}&type=income`, label: "Lihat uang masuk" },
  };
}

function scheduledDetailLines(summary: CycleSummary, money: Money): ExplanationLine[] {
  return summary.scheduled.lines.map((line) =>
    line.kind === "plan"
      ? { label: `Tagihan: ${line.label}`, note: `jatuh tempo ${shortDate(line.due_date)}${line.overdue ? ", lewat" : ""}`, amount: `−${money(line.amount)}` }
      : {
          label: `Setoran target: ${line.label}`,
          note: `${money(line.set_aside)} per bulan, sudah masuk ${money(line.funded)}`,
          amount: `−${money(line.amount)}`,
        },
  );
}

function expectedBasisNote(summary: CycleSummary): string | null {
  const expected = summary.expected_salary;
  if (!expected.date || expected.basis !== "regular_receipts") return null;
  const receipts = expected.receipt_dates.map(shortDate).join(", ");
  return `Gaji berikutnya diperkirakan ${longDate(expected.date)}, dari gaji yang dikonfirmasi tanggal ${receipts}. Ini perkiraan, bukan batas siklus.`;
}

function waitingNote(summary: CycleSummary, money: Money): string | null {
  const { waiting } = summary;
  if (!waiting.count) return null;
  return `${waiting.count} hal (${money(waiting.total)}) masih menunggu jawaban. ${money(waiting.in_keluar)} di antaranya sudah termasuk.`;
}

const EXPECTED_REASONS: Record<ExpectedReason, string> = {
  too_few_receipts: "tanggal gaji berikutnya belum dapat diperkirakan karena gaji yang terkonfirmasi belum ada tiga.",
  irregular_receipts: "tanggal tiga gaji terakhir tidak teratur, sehingga tanggal gaji berikutnya tidak ditebak.",
  no_salary_source: "sumber gaji belum diatur, sehingga gaji yang dipakai untuk perkiraan belum diketahui.",
  no_cycle: "belum ada siklus gaji yang terkonfirmasi, jadi Beranda pakai bulan kalender.",
};

/** Why the days to the next salary are unknown: the full reason for the Rincian and the hero's short line. */
export function unavailableReason(summary: CycleSummary): { reason: string; line: string } {
  const safe = summary.safe_to_spend;
  if (safe.reason === "salary_overdue") {
    const date = summary.expected_salary.date ? longDate(summary.expected_salary.date) : "sebelumnya";
    return { reason: `gaji diperkirakan masuk ${date}, tetapi gaji baru belum tercatat.`, line: "Gaji baru belum tercatat" };
  }
  if (safe.reason === "no_cycle") return { reason: EXPECTED_REASONS.no_cycle, line: "Siklus gaji belum ada" };
  const detail = safe.detail ?? summary.expected_salary.reason ?? "too_few_receipts";
  return {
    reason: EXPECTED_REASONS[detail],
    line: detail === "no_salary_source" ? "Sumber gaji belum diatur" : "Tanggal gaji berikutnya belum diketahui",
  };
}

export function scheduledExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const { scheduled } = summary;
  const window =
    scheduled.window_basis === "expected_salary"
      ? `Sampai sehari sebelum perkiraan gaji berikutnya (${shortDate(scheduled.window_end)}).`
      : `Tanggal gaji belum diketahui, sehingga dipakai 30 hari ke depan (sampai ${shortDate(scheduled.window_end)}).`;
  return {
    title: "Terjadwal",
    rule: "Tagihan yang belum dibayar dan setoran target yang belum masuk, sebelum gaji berikutnya.",
    lines: scheduledDetailLines(summary, money).map((line) => ({ ...line, amount: line.amount.replace("−", "") })),
    total: { label: "Total terjadwal", amount: money(scheduled.total) },
    notes: [window, "Rencana adalah pengingat, bukan pembayaran. Setelah pembayarannya ditautkan, angkanya pindah ke Pengeluaran."],
    link: { href: "/goals", label: "Buka Target & Tagihan" },
  };
}

/** Where a waiting row's amount already sits, in the words of the figures it is counted in. */
function waitingPlace(line: WaitingLine): string {
  if (!line.in_keluar) return "belum dihitung";
  if (line.in_keluar_as === "saving") return "sudah dihitung di Ditabung & investasi";
  return `sudah dihitung di Pengeluaran sebagai ${kakeiboBucketInfo(line.in_keluar_as)?.label ?? "Belum diklasifikasi"}`;
}

function waitingDisclosure(summary: CycleSummary, money: Money): RincianData["waiting"] {
  const { waiting } = summary;
  if (!waiting.count) return null;
  return {
    label: "Menunggu jawaban",
    amount: money(waiting.total),
    lines: waiting.lines.map((line) => ({ label: line.label, note: `${SECTION_LABELS[line.section]} · ${waitingPlace(line)}`, amount: money(line.amount) })),
    link: { href: "/review", label: "Jawab di Tinjau" },
  };
}

/** A capitalised sentence from a reason that is written to follow a colon. */
function sentence(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** The note under the Rincian's total: the expected-salary basis, or why the period is over or the date unknown. */
function basisNote(summary: CycleSummary, money: Money): string | null {
  if (summary.remaining.total < 0) {
    return `Periode ini sudah lebih ${money(-summary.remaining.total)} dari uang masuk, jadi belum ada yang aman dipakai.`;
  }
  if (summary.safe_to_spend.status !== "available") return sentence(unavailableReason(summary).reason);
  return expectedBasisNote(summary);
}

/** A cost line of the Rincian: "−Rp …", or Rp 0 without a sign. */
function cost(amount: number, money: Money): string {
  return amount > 0 ? `−${money(amount)}` : money(0);
}

/**
 * The Rincian of "Aman dipakai sampai gajian" (apply-markas-beranda D5): Masuk less Pengeluaran, Ditabung &
 * investasi and Terjadwal, each opening the server lines behind it, the total, its notes and the waiting rows.
 */
export function safeAmountRincian(summary: CycleSummary, money: Money): RincianData {
  const { masuk, spending, fresh_saving: saving, scheduled, remaining } = summary;
  const end = summary.period.basis === "cycle" ? "siklus" : "bulan";
  const savingLines = savingExplanation(summary, money);
  const scheduledLines = scheduledExplanation(summary, money);
  return {
    title: "Aman dipakai sampai gajian",
    rule: `Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir ${end}.`,
    lines: [
      { key: "masuk", label: "Masuk", amount: money(masuk.total), parts: masukExplanation(summary, money).lines },
      { key: "spending", label: "Pengeluaran", amount: cost(spending.amount, money), parts: spendingExplanation(summary, money).lines },
      saving.amount < 0
        ? { key: "saving", label: "Tabungan berkurang", amount: money(-saving.amount), parts: savingLines.lines }
        : { key: "saving", label: "Ditabung & investasi", amount: cost(saving.amount, money), parts: savingLines.lines },
      {
        key: "scheduled",
        label: `Terjadwal sampai ${shortDate(scheduled.window_end)}`,
        amount: cost(scheduled.total, money),
        parts: scheduledLines.lines,
        partNotes: scheduledLines.notes?.slice(0, 1),
      },
    ],
    // An over period ends in what it is over by, then the result Rp 0, so "Aman dipakai" is never negative.
    over: remaining.total < 0 ? { label: "Lebih dari uang masuk", amount: money(-remaining.total) } : null,
    total: { label: "Aman dipakai", amount: money(Math.max(0, remaining.total)) },
    notes: [basisNote(summary, money), waitingNote(summary, money)].filter((note): note is string => Boolean(note)),
    waiting: waitingDisclosure(summary, money),
    link: { href: ledgerPeriod(summary), label: `Lihat transaksi ${periodWord(summary)}` },
  };
}
