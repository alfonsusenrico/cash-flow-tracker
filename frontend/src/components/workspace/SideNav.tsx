"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Icon } from "@/components/ui/Icon";
import { sameRouteClick } from "@/lib/urlState";
import { AccountMenu } from "./AccountMenu";
import { useWorkspace } from "./context";
import { NAV_LINKS, PRODUCT_NAME, SETTINGS_LINK, badgeWords, isActivePath, type NavLink } from "./navigation";

function badgeCount(link: NavLink, reviewAttention: number | null, reportBadge: boolean): number {
  if (link.badge === "review") return reviewAttention ?? 0;
  if (link.badge === "report") return reportBadge ? 1 : 0;
  return 0;
}

export function SideNavLink({ link, count }: { link: NavLink; count: number }) {
  const pathname = usePathname();
  const active = isActivePath(pathname, link.href);
  const words = count && link.badge ? `, ${badgeWords(link.badge, count)}` : "";
  return (
    <Link
      href={link.href}
      onClick={sameRouteClick(link.href, pathname)}
      className="ws-nav-item"
      aria-current={active ? "page" : undefined}
      aria-label={`${link.label}${words}`}
    >
      <Icon name={link.icon} />
      <span className="ws-rail-hide">{link.label}</span>
      {count > 0 && (
        <>
          <span className="ws-count ws-rail-hide" aria-hidden="true">
            {count > 99 ? "99+" : count}
          </span>
          <span className="ws-rail-count" aria-hidden="true">
            {count > 99 ? "99+" : count}
          </span>
        </>
      )}
      <span className="ws-tip" aria-hidden="true">
        {link.label}
      </span>
    </Link>
  );
}

/** Seven pages without group headings; the foot holds Pengaturan and the account card (D3). */
export function SideNav() {
  const { reviewAttention, reportBadge, collapsed, toggleCollapsed } = useWorkspace();
  return (
    <aside className="ws-side" aria-label="Navigasi samping">
      <div className="ws-brand">
        <span className="ws-brand-mark" aria-hidden="true">
          <Icon name="layers" className="h-4 w-4" />
        </span>
        <span className="ws-brand-name ws-rail-hide">{PRODUCT_NAME}</span>
        <button
          type="button"
          className="ws-collapse"
          aria-label={collapsed ? "Lebarkan menu samping" : "Ciutkan menu samping"}
          aria-pressed={collapsed}
          title={collapsed ? "Lebarkan menu samping" : "Ciutkan menu samping"}
          onClick={toggleCollapsed}
        >
          <Icon name="panel-left" className="h-[18px] w-[18px]" />
        </button>
      </div>
      <nav className="ws-nav" aria-label="Halaman">
        {NAV_LINKS.map((link) => (
          <SideNavLink key={link.href} link={link} count={badgeCount(link, reviewAttention, reportBadge)} />
        ))}
      </nav>
      <div className="ws-side-foot">
        <SideNavLink link={SETTINGS_LINK} count={0} />
        <AccountMenu variant="card" />
      </div>
    </aside>
  );
}
