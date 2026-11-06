"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Icon } from "@/components/ui/Icon";
import { sameRouteClick } from "@/lib/urlState";
import { AccountMenu } from "./AccountMenu";
import { useWorkspace } from "./context";
import { NAV_GROUPS, PRODUCT_NAME, SETTINGS_LINK, badgeWords, countText, isActivePath, type NavLink } from "./navigation";

export function badgeCount(link: NavLink, reviewAttention: number | null, reportBadge: boolean): number {
  if (link.badge === "review") return reviewAttention ?? 0;
  if (link.badge === "report") return reportBadge ? 1 : 0;
  return 0;
}

/** A 36 px nav item; the current one has the neutral fill and the 2 × 20 px accent bar (D1). */
export function SideNavLink({ link, count }: { link: NavLink; count: number }) {
  const pathname = usePathname();
  const active = isActivePath(pathname, link.href);
  const words = count && link.badge ? `, ${badgeWords(link.badge, count)}` : "";
  return (
    <Link
      href={link.href}
      onClick={sameRouteClick(link.href, pathname)}
      className="nav-item ws-select-bar"
      aria-current={active ? "page" : undefined}
      aria-label={`${link.label}${words}`}
    >
      <Icon name={link.icon} className="h-4 w-4" />
      <span className="lbl">{link.label}</span>
      {count > 0 && (
        <span className="count" aria-hidden="true">
          {countText(count)}
        </span>
      )}
      {/* Rail label for sighted keyboard and pointer users; the accessible name already says it (decision 2). */}
      <span className="nav-tip" aria-hidden="true">
        {link.label}
      </span>
    </Link>
  );
}

/** Brand row, the three page groups and the foot with Pengaturan and the user button (D1, D2). */
export function SideNav() {
  const { reviewAttention, reportBadge, collapsed, toggleCollapsed } = useWorkspace();
  const toggleLabel = collapsed ? "Bentangkan menu samping" : "Ciutkan menu samping";
  return (
    <aside className="sb" aria-label="Navigasi samping">
      <div className="sb-brand">
        <span className="mark" aria-hidden="true">
          <Icon name="trend" className="h-4 w-4" />
        </span>
        <span className="name">{PRODUCT_NAME}</span>
        <button type="button" className="ws-icon-btn sb-toggle" aria-label={toggleLabel} aria-pressed={collapsed} onClick={toggleCollapsed}>
          <Icon name="panel" className="h-4 w-4" />
        </button>
      </div>
      <nav className="sb-nav" aria-label="Menu utama">
        {NAV_GROUPS.map((group) => (
          <div key={group.label} className="sb-group" role="group" aria-labelledby={`sb-group-${group.label}`}>
            <span id={`sb-group-${group.label}`} className="t-label">
              {group.label}
            </span>
            {group.links.map((link) => (
              <SideNavLink key={link.href} link={link} count={badgeCount(link, reviewAttention, reportBadge)} />
            ))}
          </div>
        ))}
      </nav>
      <div className="sb-foot">
        <SideNavLink link={SETTINGS_LINK} count={0} />
        <AccountMenu />
      </div>
    </aside>
  );
}
