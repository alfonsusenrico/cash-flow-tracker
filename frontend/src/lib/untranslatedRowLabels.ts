import { KAKEIBO_BUCKETS } from "@/lib/kakeibo";
import type { RowLabels } from "@/components/transactions/rows";

/**
 * The Indonesian row words of the pages whose phase has not translated them yet: Analisis' Kalender (phase 6) and
 * the Rekening detail (phase 8). Their content stays Indonesian inside a `lang="id"` area (add-language-system D10);
 * each phase moves its page onto the catalogue's labels and this file goes with the last one.
 */
export const INDONESIAN_ROW_LABELS: RowLabels = {
  investmentTopUp: "Top up Investasi",
  topupTitle: (product) => `Top up ${product}`,
  undescribed: "Tanpa keterangan",
  noCategory: "Tanpa kategori",
  noCategoryYet: "Belum ada kategori",
  waiting: "Menunggu jawaban",
  income: "Uang masuk",
  transfer: "Pindah saldo",
  categoryTopup: "Top up investasi",
  unpaired: "Pasangan belum ditemukan",
  inFrom: (other) => `Masuk dari ${other}`,
  outTo: (other) => `Keluar ke ${other}`,
  bucket: (bucket) => KAKEIBO_BUCKETS.find((item) => item.value === bucket)?.label ?? bucket,
  flow: { saved: "Ditabung", taken: "Diambil dari tabungan", invested: "Investasi", debtPayment: "Bayar utang" },
  none: "—",
  paidFromSavings: "Dibayar dari tabungan",
  payLater: "dibayar nanti",
  due: (date) => `jatuh tempo ${date}`,
  installment: (k, n) => `cicilan ${k}/${n}`,
  paid: (date) => `lunas ${date}`,
  billPayment: "bayar tagihan",
};
