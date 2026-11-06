import { KAKEIBO_BUCKETS } from "@/lib/kakeibo";

/**
 * The words a ledger row describes itself with (add-language-system D10). Beranda passes them from its catalogue;
 * Transaksi, Rekening and Analisis are not translated yet and use these Indonesian ones until their phase.
 */
export interface RowLabels {
  investmentTopUp: string;
  undescribed: string;
  noCategory: string;
  waiting: string;
  notCounted: string;
  income: string;
  transfer: string;
  unpaired: string;
  inFrom: (other: string) => string;
  outTo: (other: string) => string;
  /** A Kakeibo bucket's name; the bucket key itself when unknown. */
  bucket: (bucket: string) => string;
}

export const INDONESIAN_ROW_LABELS: RowLabels = {
  investmentTopUp: "Top up Investasi",
  undescribed: "Tanpa keterangan",
  noCategory: "Tanpa kategori",
  waiting: "Menunggu jawaban",
  notCounted: "Tidak dihitung",
  income: "Uang masuk",
  transfer: "Pindah saldo",
  unpaired: "Pasangan belum ditemukan",
  inFrom: (other) => `Masuk dari ${other}`,
  outTo: (other) => `Keluar ke ${other}`,
  bucket: (bucket) => KAKEIBO_BUCKETS.find((item) => item.value === bucket)?.label ?? bucket,
};
