export type KakeiboPillar = "need" | "want" | "culture" | "unexpected" | "saving";

export interface KakeiboPillarInfo {
  value: KakeiboPillar;
  label: string;
  help: string;
  color: string;
  softColor: string;
}

export const KAKEIBO_PILLARS: readonly KakeiboPillarInfo[] = [
  {
    value: "need",
    label: "Kebutuhan",
    help: "Pokok sehari-hari: makan harian, belanja dapur, transport rutin, tagihan, cicilan.",
    color: "var(--k-need)",
    softColor: "var(--k-need-soft)",
  },
  {
    value: "want",
    label: "Keinginan",
    help: "Bisa ditunda: kafe, jajan, makan di luar karena ingin, belanja non-pokok, hiburan.",
    color: "var(--k-want)",
    softColor: "var(--k-want-soft)",
  },
  {
    value: "culture",
    label: "Budaya",
    help: "Pengembangan diri dan relasi: olahraga, kursus, buku, acara, persembahan, hadiah terencana.",
    color: "var(--k-culture)",
    softColor: "var(--k-culture-soft)",
  },
  {
    value: "unexpected",
    label: "Tak Terduga",
    help: "Tidak direncanakan bulan ini: perbaikan, berobat mendadak, kondangan mendadak, denda.",
    color: "var(--k-unexpected)",
    softColor: "var(--k-unexpected-soft)",
  },
  {
    value: "saving",
    label: "Tabungan",
    help: "Disisihkan, bukan dibelanjakan.",
    color: "var(--k-saving)",
    softColor: "var(--k-saving-soft)",
  },
];

/** Report bucket for spending with no transaction or category pillar. Rows carry `null`, never this value. */
export const UNCLASSIFIED = "unclassified";
export type KakeiboBucket = KakeiboPillar | typeof UNCLASSIFIED;

export const UNCLASSIFIED_BUCKET: Omit<KakeiboPillarInfo, "value"> & { value: typeof UNCLASSIFIED } = {
  value: UNCLASSIFIED,
  label: "Belum diklasifikasi",
  help: "Belum punya pilar, dari transaksi maupun kategorinya. Tidak dihitung sebagai Kebutuhan.",
  color: "var(--k-none)",
  softColor: "var(--k-none-soft)",
};

/** The five pillars and the unclassified bucket, in report order. */
export const KAKEIBO_BUCKETS: readonly (KakeiboPillarInfo | typeof UNCLASSIFIED_BUCKET)[] = [...KAKEIBO_PILLARS, UNCLASSIFIED_BUCKET];

export function isKakeiboBucket(value: unknown): value is KakeiboBucket {
  return value === UNCLASSIFIED || isKakeiboPillar(value);
}

export function kakeiboBucketInfo(value: unknown) {
  return KAKEIBO_BUCKETS.find((bucket) => bucket.value === value);
}

export function isKakeiboPillar(value: unknown): value is KakeiboPillar {
  return KAKEIBO_PILLARS.some((pillar) => pillar.value === value);
}

/** Pillar preselected for a new capture: a known value, otherwise "need", shown to the user before saving. */
export function pillarOrNeed(value: unknown): KakeiboPillar {
  return isKakeiboPillar(value) ? value : "need";
}

/** Pillar of an existing row or category for an edit form; "" keeps it unclassified when saved. */
export function pillarOrUnclassified(value: unknown): KakeiboPillar | "" {
  return isKakeiboPillar(value) ? value : "";
}

/** The glossary key of each bucket's name and the catalogue key of its description (add-language-system D4, D17). */
export const KAKEIBO_TERMS = {
  need: { term: "needs", help: "needHelp" },
  want: { term: "wants", help: "wantHelp" },
  culture: { term: "culture", help: "cultureHelp" },
  unexpected: { term: "unexpected", help: "unexpectedHelp" },
  saving: { term: "savings", help: "savingHelp" },
  unclassified: { term: "noPillarYet", help: "unclassifiedHelp" },
} as const satisfies Record<KakeiboBucket, { term: string; help: string }>;
