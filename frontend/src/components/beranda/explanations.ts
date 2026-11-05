import type { ExplanationData, ExplanationLine } from "@/components/workspace/explain";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import type { CycleSummary, ExpectedReason, SummaryLine, WaitingSection } from "./types";

/**
 * "Rincian hitungan" content for the Beranda figures. Everything here formats the server's figures and lines;
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

function remainingLines(summary: CycleSummary, money: Money): ExplanationLine[] {
  return [
    ...summary.remaining.lines.map((line) => ({ label: line.label, amount: signed(line.amount, money) })),
    { label: "Sisa", amount: balance(summary.remaining.total, money) },
  ];
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

function waitingNote(summary: CycleSummary, money: Money): string {
  const { waiting } = summary;
  if (!waiting.count) return "Semua transaksi sudah ditinjau, tidak ada yang menunggu jawaban.";
  return `${waiting.count} hal (${money(waiting.total)}) masih menunggu jawaban; ${money(waiting.in_keluar)} di antaranya sudah termasuk dalam keluar.`;
}

const EXPECTED_REASONS: Record<ExpectedReason, { reason: string; fix: string }> = {
  too_few_receipts: {
    reason: "tanggal gaji berikutnya belum dapat diperkirakan karena gaji yang terkonfirmasi belum ada tiga.",
    fix: "Begitu ada tiga gaji terkonfirmasi yang jaraknya teratur (26–35 hari), angka per hari muncul.",
  },
  irregular_receipts: {
    reason: "tanggal tiga gaji terakhir tidak teratur, sehingga tanggal gaji berikutnya tidak ditebak.",
    fix: "Jika gaji memang diterima setiap bulan pada tanggal yang sama, angka per hari muncul setelah tiga gaji yang teratur.",
  },
  no_salary_source: {
    reason: "sumber gaji belum diatur, sehingga gaji yang dipakai untuk perkiraan belum diketahui.",
    fix: "Pilih sumber gaji di Pengaturan › Gaji.",
  },
  no_cycle: {
    reason: "belum ada siklus gaji yang terkonfirmasi, jadi Beranda pakai bulan kalender.",
    fix: "Konfirmasi gaji di Pengaturan › Gaji.",
  },
};

export function unavailableReason(summary: CycleSummary): { reason: string; fix: string; short: string } {
  const safe = summary.safe_to_spend;
  if (safe.reason === "salary_overdue") {
    const date = summary.expected_salary.date ? longDate(summary.expected_salary.date) : "sebelumnya";
    return {
      reason: `gaji diperkirakan masuk ${date}, tetapi gaji baru belum tercatat.`,
      fix: "Setelah gaji masuk dan dikonfirmasi, siklus baru dimulai dan angka per hari muncul lagi.",
      short: "gaji baru belum tercatat",
    };
  }
  if (safe.reason === "no_cycle") return { ...EXPECTED_REASONS.no_cycle, short: "siklus gaji belum ada" };
  const detail = safe.detail ?? summary.expected_salary.reason ?? "too_few_receipts";
  return { ...EXPECTED_REASONS[detail], short: detail === "no_salary_source" ? "sumber gaji belum diatur" : "tanggal gaji belum jelas" };
}

export function safeExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const safe = summary.safe_to_spend;
  const base = {
    title: "Aman dipakai hari ini",
    rule: "Uang periode ini yang belum dipakai dan belum dijadwalkan, dibagi sisa hari sampai perkiraan gaji berikutnya.",
  };
  if (safe.status !== "available" || safe.per_day === null || safe.days_left === null) {
    const why = unavailableReason(summary);
    return {
      ...base,
      lines: [...summary.remaining.lines.map((line) => ({ label: line.label, amount: signed(line.amount, money) })), { label: "Sisa", amount: balance(summary.remaining.total, money) }],
      unavailable: { reason: `Angka per hari memerlukan tanggal gaji yang teratur: ${why.reason}`, fix: why.fix },
      notes: [waitingNote(summary, money)],
      link: { href: "/settings/gaji", label: "Buka Pengaturan › Gaji" },
    };
  }
  const notes: string[] = [];
  if (safe.over_by) notes.push(`Periode ini sudah lebih ${money(safe.over_by)} dari uang masuk, jadi belum ada yang aman dipakai hari ini.`);
  const basis = expectedBasisNote(summary);
  if (basis) notes.push(basis);
  notes.push(waitingNote(summary, money));
  return {
    ...base,
    lines: [
      ...summary.remaining.lines
        .filter((line) => line.key !== "scheduled")
        .map((line) => ({ label: line.label, amount: signed(line.amount, money) })),
      ...scheduledDetailLines(summary, money),
      { label: "Sisa", amount: balance(summary.remaining.total, money) },
      {
        label: `÷ ${safe.days_left} hari (${shortDate(summary.period.today)} – ${shortDate(summary.scheduled.window_end)})`,
        note: `dibulatkan ke bawah per ${money(safe.rounding)}, tidak pernah di bawah nol`,
        amount: "",
      },
    ],
    total: { label: "Aman hari ini", amount: money(safe.per_day) },
    notes,
    link: { href: `${ledgerPeriod(summary)}`, label: "Lihat transaksinya" },
  };
}

export function waitingExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const { waiting } = summary;
  if (!waiting.count) {
    return {
      title: "Menunggu jawaban",
      rule: "Tidak ada yang menunggu jawaban. Semua transaksi sudah ditinjau.",
      lines: [],
      link: { href: "/review", label: "Buka Tinjau" },
    };
  }
  return {
    title: "Menunggu jawaban",
    rule: "Transaksi yang sudah tercatat tetapi masih perlu dijawab di Tinjau. Semuanya tetap tercatat; jawabannya tidak ditebak.",
    lines: waiting.lines.map((line) => ({
      label: line.label,
      note: `${SECTION_LABELS[line.section]} · ${
        line.in_keluar ? `sudah dihitung sebagai ${kakeiboBucketInfo(line.in_keluar_as)?.label ?? "Belum diklasifikasi"}` : "belum masuk Keluar"
      }`,
      amount: money(line.amount),
    })),
    total: { label: "Total", amount: money(waiting.total) },
    notes: [`${money(waiting.in_keluar)} sudah ikut dihitung di angka periode ini, ${money(waiting.not_in_keluar)} belum.`],
    link: { href: "/review", label: "Jawab di Tinjau" },
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

export function remainingExplanation(summary: CycleSummary, money: Money): ExplanationData {
  return {
    title: "Aman dipakai sampai gajian",
    rule: "Masuk dikurangi pengeluaran, yang ditabung dan yang terjadwal, untuk seluruh periode sampai gaji berikutnya.",
    lines: [
      { label: "Masuk", amount: signed(summary.masuk.total, money) },
      { label: "Pengeluaran", amount: signed(-summary.spending.amount, money) },
      { label: "Ditabung & investasi", amount: signed(-summary.fresh_saving.amount, money) },
      { label: "Terjadwal", amount: signed(-summary.scheduled.total, money) },
    ],
    total: { label: "Sisa", amount: balance(summary.remaining.total, money) },
    notes: [waitingNote(summary, money)],
  };
}

export function netWorthExplanation(summary: CycleSummary, money: Money): ExplanationData {
  return {
    title: "Kekayaan bersih",
    rule: "Semua saldo dan investasi dikurangi utang yang masih aktif, termasuk utang kredit.",
    lines: summary.net_worth.lines.map((line) => ({
      label: line.key.startsWith("credit:") ? `Utang ${line.label}` : line.label,
      amount: signed(line.amount, money),
    })),
    total: { label: "Kekayaan bersih", amount: money(summary.net_worth.total) },
    notes: ["Kantong dihitung sekali, tidak ganda dengan rekening induknya. Investasi memakai nilai terakhir yang tercatat."],
    link: { href: "/accounts", label: "Buka Rekening & Kantong" },
  };
}

export { remainingLines };
