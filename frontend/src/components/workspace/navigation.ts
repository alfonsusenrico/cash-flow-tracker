import type { IconName } from "@/components/ui/Icon";
import { SETTINGS_HOME } from "@/components/settings/sections";

/** Shown in the sidebar, the phone header and the document title. Swapping the name is this constant. */
export const PRODUCT_NAME = "CashFlow";

export type NavBadge = "review" | "report";

export type NavGroup = "Harian" | "Ringkasan" | "Simpanan";

export interface NavLink {
  href: string;
  label: string;
  icon: IconName;
  badge?: NavBadge;
}

export interface GroupedNavLink extends NavLink {
  group: NavGroup;
}

export const REVIEW_HREF = "/review";
export const LEDGER_HREF = "/ledger";
/** What the Transaksi entry opens: this cycle (OD20). Plain `/ledger` and every other link keep all time. */
export const LEDGER_NAV_HREF = "/ledger?period=cycle";
/** Opens Transaksi with its own search focused; the page removes the parameter once it took effect (D5). */
export const LEDGER_SEARCH_HREF = "/ledger?fokus=cari";

/** One ordered page list for the sidebar, the rail, the phone tab bar and Lainnya (apply-markas-shell D1). */
export const NAV_LINKS: GroupedNavLink[] = [
  { href: "/", label: "Beranda", icon: "home", group: "Harian" },
  { href: REVIEW_HREF, label: "Tinjau", icon: "inbox", group: "Harian", badge: "review" },
  { href: LEDGER_NAV_HREF, label: "Transaksi", icon: "list", group: "Harian" },
  { href: "/insights", label: "Analisis", icon: "chart", group: "Ringkasan" },
  { href: "/laporan", label: "Laporan", icon: "file", group: "Ringkasan", badge: "report" },
  { href: "/accounts", label: "Rekening & Kantong", icon: "wallet", group: "Simpanan" },
  { href: "/goals", label: "Target & Tagihan", icon: "target", group: "Simpanan" },
];

const GROUP_ORDER: NavGroup[] = ["Harian", "Ringkasan", "Simpanan"];

export const NAV_GROUPS: { label: NavGroup; links: GroupedNavLink[] }[] = GROUP_ORDER.map((label) => ({
  label,
  links: NAV_LINKS.filter((link) => link.group === label),
}));

export const SETTINGS_LINK: NavLink = { href: SETTINGS_HOME, label: "Pengaturan", icon: "settings" };

/** Pages on the phone tab bar; every other page sits in "Lainnya". */
export const PHONE_TAB_HREFS = ["/", REVIEW_HREF, LEDGER_NAV_HREF] as const;

/** The "Lainnya" sheet: the pages outside the tab bar in the sidebar's order, then Pengaturan (D5). */
export const MORE_LINKS: NavLink[] = [
  ...NAV_LINKS.filter((link) => !(PHONE_TAB_HREFS as readonly string[]).includes(link.href)),
  SETTINGS_LINK,
];

export function isActivePath(pathname: string, href: string): boolean {
  href = href.split("?")[0];
  if (href === "/") return pathname === "/";
  if (href.startsWith("/settings")) return pathname.startsWith("/settings");
  return pathname === href || pathname.startsWith(`${href}/`);
}

/** Words for a count badge, so the number is never colour or position alone. */
export function badgeWords(kind: NavBadge, count: number): string {
  if (kind === "review") return `${count} perlu ditinjau`;
  return "laporan baru siap";
}

/** The visible count: muted text, capped so the column never widens (D1). */
export function countText(count: number): string {
  return count > 99 ? "99+" : String(count);
}

interface PageMeta {
  title: string;
  subtitle: string;
  /** The frame's maximum width in px: 1040 on Transaksi and Pengaturan, 1200 elsewhere (D4). */
  width: number;
}

const FRAME_WIDTH = 1200;
const NARROW_FRAME_WIDTH = 1040;

const PAGE_META: [string, PageMeta][] = [
  ["/review", { title: "Tinjau", subtitle: "Yang perlu dijawab dan yang dicatat otomatis", width: FRAME_WIDTH }],
  ["/ledger", { title: "Transaksi", subtitle: "Semua uang masuk, pengeluaran, dan pindah saldo", width: NARROW_FRAME_WIDTH }],
  ["/accounts", { title: "Rekening & Kantong", subtitle: "Saldo rekening, kantong, investasi dan utang", width: FRAME_WIDTH }],
  ["/goals", { title: "Target & Tagihan", subtitle: "Dana darurat, tagihan, utang dan target tabungan", width: FRAME_WIDTH }],
  ["/insights", { title: "Analisis", subtitle: "Ke mana uang digunakan, dibanding periode lalu", width: FRAME_WIDTH }],
  ["/laporan", { title: "Laporan", subtitle: "Laporan bulanan Kakeibo", width: FRAME_WIDTH }],
  ["/settings", { title: "Pengaturan", subtitle: "", width: NARROW_FRAME_WIDTH }],
  ["/komponen", { title: "Komponen", subtitle: "Lembar komponen Markas+, hanya di build lokal", width: FRAME_WIDTH }],
];

/** Title, subtitle and frame width of the title block from the route; Beranda writes its own date and period line. */
export function pageMeta(pathname: string): PageMeta {
  const match = PAGE_META.find(([href]) => isActivePath(pathname, href));
  return match ? match[1] : { title: "Beranda", subtitle: "", width: FRAME_WIDTH };
}
