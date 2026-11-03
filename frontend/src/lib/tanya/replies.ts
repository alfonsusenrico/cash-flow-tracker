import type { CycleSummary } from "@/components/beranda/types";
import type { BillsRange } from "./intents";
import type { CardKind } from "./types";

/**
 * Everything the assistant says in Tanya, and nothing else. This is the one Tanya module that keeps the
 * casual assistant voice (copy register, add-tanya-conversation D10); labels, buttons, figure names and
 * notices live in the components and stay neutral. Amounts always go through `money`, so hidden balances
 * hide them here too.
 */

type Money = (amount: number) => string;

/** 04–10 pagi, 11–14 siang, 15–18 sore, 19–03 malam, by the device's local hour. */
export function partOfDay(hour: number): "Pagi" | "Siang" | "Sore" | "Malam" {
  if (hour >= 4 && hour <= 10) return "Pagi";
  if (hour >= 11 && hour <= 14) return "Siang";
  if (hour >= 15 && hour <= 18) return "Sore";
  return "Malam";
}

export function greeting(hour: number, name: string | null): string {
  const first = name?.trim().split(/\s+/)[0];
  return first ? `${partOfDay(hour)}, ${first}.` : `${partOfDay(hour)}.`;
}

/** The line under the greeting; waiting rows are counted, never "belum dihitung" (phase plan D9). */
export function leadLine(summary: CycleSummary, waiting: number | null, money: Money): string {
  const safe = summary.safe_to_spend;
  const first = safe.status === "available" && safe.per_day !== null
    ? `Aman dipakai hari ini ${money(safe.per_day)}`
    : `Sisa uang periode ini ${money(summary.remaining.total)}`;
  if (waiting === null) return `${first}.`;
  if (waiting === 0) return `${first} · nggak ada yang nunggu jawabanmu.`;
  return `${first} · ${waiting} hal nunggu jawabanmu.`;
}

export function intentIntro(kind: CardKind, range?: BillsRange): string {
  switch (kind) {
    case "cycle":
      return "Ini posisi uangmu periode ini:";
    case "kakeibo":
      return "Ini ke mana aja uangmu keluar periode ini, per pilar:";
    case "bills":
      return range === "week" ? "Ini tagihan dan setoran 7 hari ke depan:" : "Ini tagihan dan setoran yang udah dijadwalkan:";
    case "waiting":
      return "Ini yang lagi nunggu jawabanmu:";
    case "report":
      return "Ini laporan terbarumu:";
  }
}

export const HELP_TEXT =
  "Aku bisa catat transaksi dari satu kalimat, misalnya \"parkir 5rb jam 2\". Aku juga bisa kasih ringkasan siklus ini, " +
  "yang nunggu jawabanmu, tagihan, pembagian Kakeibo, dan laporan terbaru. Aku nggak nebak angka. Coba salah satu ini:";

export const UNKNOWN_TEXT = "Aku belum bisa jawab itu. Coba salah satu ini:";

export const ASK_AMOUNT_TEXT = "Jumlahnya berapa? Aku nggak nebak. Tulis juga angkanya, misalnya \"parkir 5rb jam 2\".";

export const SEVERAL_AMOUNTS_TEXT = "Ada lebih dari satu jumlah di situ. Tulis satu jumlah aja, biar aku nggak salah catat.";

export const MIXED_TEXT = "Kalau maksudmu mau dicatat, pilih \"Catat sebagai transaksi\".";

export function recordedText(toReview: boolean): string {
  return toReview
    ? "Udah aku catat. Ada yang belum pasti, jadi aku taruh juga di Tinjau buat kamu cek."
    : "Udah aku catat.";
}

export const DETAIL_LATER_TEXT = "Pertanyaannya aku simpan. Kamu bisa jawab nanti di Tinjau.";
