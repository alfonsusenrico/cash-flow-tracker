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
    color: "var(--color-kakeibo-need)",
    softColor: "var(--color-kakeibo-need-soft)",
  },
  {
    value: "want",
    label: "Keinginan",
    help: "Bisa ditunda: kafe, jajan, makan di luar karena ingin, belanja non-pokok, hiburan.",
    color: "var(--color-kakeibo-want)",
    softColor: "var(--color-kakeibo-want-soft)",
  },
  {
    value: "culture",
    label: "Budaya",
    help: "Pengembangan diri dan relasi: olahraga, kursus, buku, acara, persembahan, hadiah terencana.",
    color: "var(--color-kakeibo-culture)",
    softColor: "var(--color-kakeibo-culture-soft)",
  },
  {
    value: "unexpected",
    label: "Tak Terduga",
    help: "Tidak direncanakan bulan ini: perbaikan, berobat mendadak, kondangan mendadak, denda.",
    color: "var(--color-kakeibo-unexpected)",
    softColor: "var(--color-kakeibo-unexpected-soft)",
  },
  {
    value: "saving",
    label: "Tabungan",
    help: "Disisihkan, bukan dibelanjakan.",
    color: "var(--color-kakeibo-saving)",
    softColor: "var(--color-kakeibo-saving-soft)",
  },
];

export function isKakeiboPillar(value: unknown): value is KakeiboPillar {
  return KAKEIBO_PILLARS.some((pillar) => pillar.value === value);
}

/** Pillar for a form: a known value, otherwise "need", matching the backend's effective-pillar default. */
export function pillarOrNeed(value: unknown): KakeiboPillar {
  return isKakeiboPillar(value) ? value : "need";
}

export function kakeiboPillarInfo(value: unknown): KakeiboPillarInfo | undefined {
  return KAKEIBO_PILLARS.find((pillar) => pillar.value === value);
}

export function kakeiboLabel(value: string | null | undefined): string {
  return kakeiboPillarInfo(value)?.label ?? value ?? "";
}

/** The backend judges the 50/30/20 benchmark on groups and reports healthy, warning or elevated_needs. */
export function isKakeiboHealthy(status: string | null | undefined): boolean {
  return status === "healthy";
}

export function kakeiboStatusLabel(status: string | null | undefined): string {
  if (status === "healthy") return "Kakeibo sehat";
  if (status === "warning") return "Gaya hidup di atas 30%";
  if (status === "elevated_needs") return "Kebutuhan di atas 50%";
  return "Belum ada data";
}

/** Kakeibo section of the dashboard and insights responses; the culture, unexpected and group fields are newer. */
export interface KakeiboBreakdown {
  need_spent: number;
  need_pct: number;
  want_spent: number;
  want_pct: number;
  culture_spent?: number;
  culture_pct?: number;
  unexpected_spent?: number;
  unexpected_pct?: number;
  saving_spent: number;
  saving_expenses?: number;
  net_saving_transfers?: number;
  saving_pct: number;
  essentials_pct?: number;
  lifestyle_pct?: number;
  total_allocated?: number;
  kakeibo_status: string;
  want_status?: string;
}

export function pillarPct(kakeibo: KakeiboBreakdown | undefined, pillar: KakeiboPillar): number {
  return kakeibo?.[`${pillar}_pct`] ?? 0;
}

export function pillarSpent(kakeibo: KakeiboBreakdown | undefined, pillar: KakeiboPillar): number {
  return kakeibo?.[`${pillar}_spent`] ?? 0;
}

export interface KakeiboGroup {
  key: "essentials" | "lifestyle" | "saving";
  label: string;
  /** For tight spaces such as the dashboard metric tile. */
  shortLabel: string;
  pillars: readonly KakeiboPillar[];
  targetPct: number;
  /** Essentials and lifestyle are ceilings; saving is a floor. */
  targetIsMinimum: boolean;
  spent: number;
  pct: number;
  withinTarget: boolean;
}

const GROUP_DEFINITIONS = [
  { key: "essentials", label: "Kebutuhan pokok", shortLabel: "Pokok", pillars: ["need", "unexpected"], targetPct: 50, targetIsMinimum: false },
  { key: "lifestyle", label: "Gaya hidup", shortLabel: "Gaya hidup", pillars: ["want", "culture"], targetPct: 30, targetIsMinimum: false },
  { key: "saving", label: "Tabungan", shortLabel: "Tabungan", pillars: ["saving"], targetPct: 20, targetIsMinimum: true },
] as const;

/** The 50/30/20 benchmark groups: essentials = need + unexpected, lifestyle = want + culture. */
export function kakeiboGroups(kakeibo: KakeiboBreakdown | undefined): KakeiboGroup[] {
  const reportedGroupPct: Record<KakeiboGroup["key"], number | undefined> = {
    essentials: kakeibo?.essentials_pct,
    lifestyle: kakeibo?.lifestyle_pct,
    saving: kakeibo?.saving_pct,
  };
  return GROUP_DEFINITIONS.map((group) => {
    const spent = group.pillars.reduce((sum, pillar) => sum + pillarSpent(kakeibo, pillar), 0);
    const summedPct = group.pillars.reduce((sum, pillar) => sum + pillarPct(kakeibo, pillar), 0);
    const pct = reportedGroupPct[group.key] ?? Math.round(summedPct * 10) / 10;
    const withinTarget = group.targetIsMinimum ? pct >= group.targetPct : pct <= group.targetPct;
    return { ...group, spent, pct, withinTarget };
  });
}
