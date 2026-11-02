import type { IconName } from "@/components/ui/Icon";
import { SETTINGS_HOME } from "@/components/settings/sections";

/** Shown in the sidebar, the phone header and the document title. Swapping the name is this constant. */
export const PRODUCT_NAME = "CashFlow";

export type NavBadge = "review" | "report";

export interface NavLink {
  href: string;
  label: string;
  icon: IconName;
  badge?: NavBadge;
}

export interface NavGroup {
  label: string;
  links: NavLink[];
}

export const REVIEW_HREF = "/review";
export const LEDGER_HREF = "/ledger";

/**
 * One source for the sidebar, the phone tab bar and the "Lainnya" sheet. The AI group (Tanya) joins
 * once its page exists; an empty group is never shown.
 */
export const NAV_GROUPS: NavGroup[] = [
  {
    label: "Harian",
    links: [
      { href: "/", label: "Beranda", icon: "home" },
      { href: REVIEW_HREF, label: "Tinjau", icon: "inbox", badge: "review" },
      { href: LEDGER_HREF, label: "Transaksi", icon: "receipt" },
    ],
  },
  {
    label: "Uang",
    links: [
      { href: "/accounts", label: "Rekening & Kantong", icon: "wallet" },
      { href: "/goals", label: "Target & Tagihan", icon: "target" },
    ],
  },
  {
    label: "Laporan",
    links: [
      { href: "/insights", label: "Analisis", icon: "analysis" },
      { href: "/laporan", label: "Laporan", icon: "file-text", badge: "report" },
    ],
  },
];

export const SETTINGS_LINK: NavLink = { href: SETTINGS_HOME, label: "Pengaturan", icon: "sliders" };

/** Pages on the phone tab bar; every other page sits in "Lainnya". */
export const PHONE_TAB_HREFS = ["/", REVIEW_HREF, LEDGER_HREF] as const;

export function isActivePath(pathname: string, href: string): boolean {
  if (href === "/") return pathname === "/";
  if (href.startsWith("/settings")) return pathname.startsWith("/settings");
  return pathname === href || pathname.startsWith(`${href}/`);
}

/** Words for a count badge, so the number is never colour or position alone. */
export function badgeWords(kind: NavBadge, count: number): string {
  if (kind === "review") return `${count} perlu ditinjau`;
  return "laporan baru siap";
}

interface PageMeta {
  title: string;
  subtitle: string;
}

const PAGE_META: [string, PageMeta][] = [
  ["/review", { title: "Tinjau", subtitle: "Yang perlu jawabanmu dan yang dicatat otomatis" }],
  ["/ledger", { title: "Transaksi", subtitle: "Semua uang masuk, keluar, dan pindah saldo" }],
  ["/accounts", { title: "Rekening & Kantong", subtitle: "Bank, e-wallet, tunai, kantong, dan investasi" }],
  ["/goals", { title: "Target & Tagihan", subtitle: "Target tabungan, utang, dan rencana bayar" }],
  ["/insights", { title: "Analisis", subtitle: "Ke mana uangmu pergi, dibanding periode lalu" }],
  ["/laporan", { title: "Laporan", subtitle: "Laporan bulanan Kakeibo" }],
  ["/settings", { title: "Pengaturan", subtitle: "Profil, gaji, anggaran, HP, dan pengirim" }],
];

/** Title and subtitle of the top bar from the route; Beranda writes its own date and period line. */
export function pageMeta(pathname: string): PageMeta {
  const match = PAGE_META.find(([href]) => isActivePath(pathname, href));
  return match ? match[1] : { title: "Beranda", subtitle: "" };
}
