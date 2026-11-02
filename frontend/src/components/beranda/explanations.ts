import type { ExplanationData, ExplanationLine } from "@/components/workspace/explain";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import type { CycleSummary, ExpectedReason, SummaryLine, WaitingSection } from "./types";

/**
 * "Kok segini?" content for the Beranda figures. Everything here formats the server's figures and lines;
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
  questions: "Apa ini?",
  ambiguous: "Pasangan pindah saldo belum pasti",
  awaiting: "Pindah saldo belum lengkap",
  groups: "Perlu jawaban",
};

export function comparisonNote(summary: CycleSummary): string | null {
  const { comparison } = summary;
  if (comparison.status === "too_short") return "Perbandingan dengan periode lalu muncul setelah 3 hari.";
  if (comparison.status === "no_previous") return "Belum ada data periode lalu untuk dibandingkan.";
  return null;
}

export function keluarExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const notes = ["Pindah saldo antar rekeningmu sendiri nggak dihitung keluar."];
  if (summary.keluar.offsets_total > 0) notes.push(`Sudah dikurangi ganti uang ${money(summary.keluar.offsets_total)}.`);
  if (summary.waiting.in_keluar > 0) {
    notes.push(`${money(summary.waiting.in_keluar)} dari yang nunggu jawabanmu sudah ikut di sini. Jawabanmu cuma memindahkan pilarnya.`);
  }
  const comparison = comparisonNote(summary);
  if (comparison) notes.push(comparison);
  return {
    title: `Keluar ${periodWord(summary)}`,
    rule: "Semua yang keluar di periode ini menurut pilar Kakeibo, termasuk yang ditabung atau diinvestasikan, dikurangi ganti uang.",
    lines: summary.keluar.lines.map((line) => lineOf(line, money)),
    total: { label: "Total keluar", amount: money(summary.keluar.total) },
    notes,
    link: { href: `${ledgerPeriod(summary)}&type=expense`, label: "Lihat transaksi keluar" },
  };
}

export function masukExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const notes = ["Pindah saldo antar rekeningmu dan ganti uang nggak dihitung masuk."];
  const comparison = comparisonNote(summary);
  if (comparison) notes.push(comparison);
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
  return `Gajian berikutnya diperkirakan ${longDate(expected.date)}, dari gaji yang kamu konfirmasi tanggal ${receipts}. Ini perkiraan, bukan batas siklus.`;
}

function waitingNote(summary: CycleSummary, money: Money): string {
  const { waiting } = summary;
  if (!waiting.count) return "Semua transaksi udah beres, nggak ada yang nunggu jawaban.";
  return `${waiting.count} hal (${money(waiting.total)}) masih nunggu jawabanmu; ${money(waiting.in_keluar)} di antaranya sudah ikut dihitung keluar.`;
}

const EXPECTED_REASONS: Record<ExpectedReason, { reason: string; fix: string }> = {
  too_few_receipts: {
    reason: "tanggal gajian berikutnya belum bisa diperkirakan karena gaji yang terkonfirmasi belum ada tiga.",
    fix: "Begitu ada tiga gaji terkonfirmasi yang jaraknya teratur (26–35 hari), angka per hari muncul.",
  },
  irregular_receipts: {
    reason: "tanggal tiga gaji terakhirmu nggak teratur, jadi aku nggak mau nebak tanggal gajian berikutnya.",
    fix: "Kalau gajimu memang tiap bulan di tanggal yang sama, angka per hari muncul setelah tiga gaji yang teratur.",
  },
  no_salary_source: {
    reason: "sumber gajimu belum diatur, jadi aku belum tahu gaji mana yang dipakai buat perkiraan.",
    fix: "Pilih sumber gaji di Pengaturan › Gaji.",
  },
  no_cycle: {
    reason: "belum ada siklus gaji yang terkonfirmasi, jadi Beranda pakai bulan kalender.",
    fix: "Konfirmasi gajimu di Pengaturan › Gaji.",
  },
};

export function unavailableReason(summary: CycleSummary): { reason: string; fix: string; short: string } {
  const safe = summary.safe_to_spend;
  if (safe.reason === "salary_overdue") {
    const date = summary.expected_salary.date ? longDate(summary.expected_salary.date) : "sebelumnya";
    return {
      reason: `gajian diperkirakan ${date}, tapi gaji baru belum tercatat.`,
      fix: "Begitu gajinya masuk dan kamu konfirmasi, siklus baru mulai dan angka per hari muncul lagi.",
      short: "gaji baru belum tercatat",
    };
  }
  if (safe.reason === "no_cycle") return { ...EXPECTED_REASONS.no_cycle, short: "siklus gaji belum ada" };
  const detail = safe.detail ?? summary.expected_salary.reason ?? "too_few_receipts";
  return { ...EXPECTED_REASONS[detail], short: detail === "no_salary_source" ? "sumber gaji belum diatur" : "tanggal gajian belum jelas" };
}

export function safeExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const safe = summary.safe_to_spend;
  const base = {
    title: "Aman dipakai hari ini",
    rule: "Uang periode ini yang belum dipakai dan belum dijadwalkan, dibagi sisa hari sampai perkiraan gajian.",
  };
  if (safe.status !== "available" || safe.per_day === null || safe.days_left === null) {
    const why = unavailableReason(summary);
    return {
      ...base,
      lines: [...summary.remaining.lines.map((line) => ({ label: line.label, amount: signed(line.amount, money) })), { label: "Sisa", amount: balance(summary.remaining.total, money) }],
      unavailable: { reason: `Angka per hari butuh tanggal gajian yang teratur: ${why.reason}`, fix: why.fix },
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
        note: `dibulatkan ke bawah per ${money(safe.rounding)}, nggak pernah di bawah nol`,
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
      title: "Nunggu jawabanmu",
      rule: "Nggak ada yang nunggu jawaban. Semua transaksi udah beres.",
      lines: [],
      link: { href: "/review", label: "Buka Tinjau" },
    };
  }
  return {
    title: "Nunggu jawabanmu",
    rule: "Transaksi yang sudah tercatat tapi masih perlu kamu jawab di Tinjau. Semuanya tetap tercatat; aku nggak nebak jawabannya.",
    lines: waiting.lines.map((line) => ({
      label: line.label,
      note: `${SECTION_LABELS[line.section]} · ${
        line.in_keluar ? `sudah di Keluar sebagai ${kakeiboBucketInfo(line.in_keluar_as)?.label ?? "Belum diklasifikasi"}` : "belum masuk Keluar"
      }`,
      amount: money(line.amount),
    })),
    total: { label: "Total", amount: money(waiting.total) },
    notes: [`${money(waiting.in_keluar)} sudah ikut dihitung di Keluar, ${money(waiting.not_in_keluar)} belum.`],
    link: { href: "/review", label: "Jawab di Tinjau" },
  };
}

export function scheduledExplanation(summary: CycleSummary, money: Money): ExplanationData {
  const { scheduled } = summary;
  const window =
    scheduled.window_basis === "expected_salary"
      ? `Sampai sehari sebelum perkiraan gajian (${shortDate(scheduled.window_end)}).`
      : `Tanggal gajian belum diketahui, jadi aku pakai 30 hari ke depan (sampai ${shortDate(scheduled.window_end)}).`;
  return {
    title: "Terjadwal",
    rule: "Tagihan yang belum dibayar dan setoran target yang belum masuk, sebelum gajian berikutnya.",
    lines: scheduledDetailLines(summary, money).map((line) => ({ ...line, amount: line.amount.replace("−", "") })),
    total: { label: "Total terjadwal", amount: money(scheduled.total) },
    notes: [window, "Rencana itu pengingat, bukan pembayaran. Begitu pembayarannya kamu tautkan, angkanya pindah ke Keluar."],
    link: { href: "/goals", label: "Buka Target & Tagihan" },
  };
}

export function remainingExplanation(summary: CycleSummary, money: Money): ExplanationData {
  return {
    title: "Sisa aman sampai gajian",
    rule: "Masuk dikurangi yang udah dipakai dan yang terjadwal.",
    lines: summary.remaining.lines.map((line) => ({ label: line.label, amount: signed(line.amount, money) })),
    total: { label: "Sisa", amount: balance(summary.remaining.total, money) },
    notes: [waitingNote(summary, money)],
  };
}

export function netWorthExplanation(summary: CycleSummary, money: Money): ExplanationData {
  return {
    title: "Kekayaan bersih",
    rule: "Semua saldo dan investasi dikurangi utang yang masih aktif.",
    lines: summary.net_worth.lines.map((line) => ({ label: line.label, amount: signed(line.amount, money) })),
    total: { label: "Kekayaan bersih", amount: money(summary.net_worth.total) },
    notes: ["Kantong dihitung sekali, nggak dobel dengan rekening induknya. Investasi pakai nilai terakhir yang tercatat."],
    link: { href: "/accounts", label: "Buka Rekening & Kantong" },
  };
}

export { remainingLines };
