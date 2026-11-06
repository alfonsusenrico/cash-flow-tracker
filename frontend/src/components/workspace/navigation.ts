import type { IconName } from "@/components/ui/Icon";
import { SETTINGS_HOME } from "@/components/settings/sections";

/** Shown in the sidebar, the phone header and the document title. Swapping the name is this constant. */
export const PRODUCT_NAME = "CashFlow";

export type NavBadge = "review" | "report";

/** A sidebar group; its words are `nav.groups.<key>` (add-language-system D3). */
export type NavGroup = "daily" | "overview" | "money";

/** A destination's words are `nav.<key>`. */
export type NavKey = "home" | "review" | "transactions" | "analysis" | "reports" | "accounts" | "goals" | "settings";

export interface NavLink {
  href: string;
  key: NavKey;
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
  { href: "/", key: "home", icon: "home", group: "daily" },
  { href: REVIEW_HREF, key: "review", icon: "inbox", group: "daily", badge: "review" },
  { href: LEDGER_NAV_HREF, key: "transactions", icon: "list", group: "daily" },
  { href: "/insights", key: "analysis", icon: "chart", group: "overview" },
  { href: "/laporan", key: "reports", icon: "file", group: "overview", badge: "report" },
  { href: "/accounts", key: "accounts", icon: "wallet", group: "money" },
  { href: "/goals", key: "goals", icon: "target", group: "money" },
];

const GROUP_ORDER: NavGroup[] = ["daily", "overview", "money"];

export const NAV_GROUPS: { key: NavGroup; links: GroupedNavLink[] }[] = GROUP_ORDER.map((key) => ({
  key,
  links: NAV_LINKS.filter((link) => link.group === key),
}));

export const SETTINGS_LINK: NavLink = { href: SETTINGS_HOME, key: "settings", icon: "settings" };

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

/** The visible count: muted text, capped so the column never widens (D1). */
export function countText(count: number): string {
  return count > 99 ? "99+" : String(count);
}

/** The frame's maximum width in px: 1040 on Transaksi and Pengaturan, 1200 elsewhere (D4). */
const FRAME_WIDTH = 1200;
const NARROW_FRAME_WIDTH = 1040;

const NARROW_ROUTES = ["/ledger", "/settings"];

export function frameWidth(pathname: string): number {
  return NARROW_ROUTES.some((href) => isActivePath(pathname, href)) ? NARROW_FRAME_WIDTH : FRAME_WIDTH;
}
