"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useRef, useState } from "react";
import { Icon } from "@/components/ui/Icon";
import { THEME_CHOICES } from "@/lib/theme";
import { sameRouteClick } from "@/lib/urlState";
import { useWorkspace } from "./context";
import { closeEnclosingDialog } from "./layers";
import { MenuPanel } from "./Menu";
import {
  LEDGER_SEARCH_HREF,
  MORE_LINKS,
  NAV_LINKS,
  PHONE_TAB_HREFS,
  PRODUCT_NAME,
  REVIEW_HREF,
  badgeWords,
  countText,
  isActivePath,
  type NavLink,
} from "./navigation";
import { NotificationBell, NotificationsPanel } from "./Notifications";
import { ShellDialog } from "./ShellDialog";
import { HideBalancesButton, useThemeEntries } from "./TopBar";

function tabLink(href: string): NavLink {
  return NAV_LINKS.find((link) => link.href === href)!;
}

/** The sticky 56 px phone header below 768 px (D5): brand, search, Sembunyikan saldo and the bell. */
export function PhoneHeader() {
  return (
    <header className="ph-header">
      <Link href="/" className="brand">
        <span className="mark" aria-hidden="true">
          <Icon name="trend" className="h-4 w-4" />
        </span>
        {PRODUCT_NAME}
      </Link>
      <Link href={LEDGER_SEARCH_HREF} className="ws-icon-btn" aria-label="Cari transaksi">
        <Icon name="search" className="h-5 w-5" />
      </Link>
      <HideBalancesButton iconSize={20} />
      <NotificationBell iconSize={20} id="bell-ph" />
    </header>
  );
}

type PhoneLayer = "more" | "notifications" | "theme" | null;

/** The 64 px tab bar below 768 px: Beranda, Tinjau, Catat, Transaksi and Lainnya (D5). */
export function PhoneTabBar() {
  const pathname = usePathname();
  const { reviewAttention, openCapture } = useWorkspace();
  const [layer, setLayer] = useState<PhoneLayer>(null);
  const moreButton = useRef<HTMLButtonElement>(null);
  const themeEntries = useThemeEntries();
  const [home, review, ledger] = PHONE_TAB_HREFS.map(tabLink);
  const inMore = !PHONE_TAB_HREFS.some((href) => isActivePath(pathname, href));

  const renderTab = (link: NavLink) => {
    const active = isActivePath(pathname, link.href);
    const count = link.href === REVIEW_HREF ? reviewAttention ?? 0 : 0;
    return (
      <Link
        key={link.href}
        href={link.href}
        onClick={sameRouteClick(link.href, pathname)}
        aria-current={active ? "page" : undefined}
        aria-label={count ? `${link.label}, ${badgeWords("review", count)}` : undefined}
      >
        <Icon name={link.icon} className="h-5 w-5" />
        <span>{link.label}</span>
        {count > 0 && (
          <span className="tab-count" aria-hidden="true">
            {countText(count)}
          </span>
        )}
      </Link>
    );
  };

  const closeTo = () => {
    setLayer(null);
    moreButton.current?.focus();
  };

  return (
    <>
      <nav className="tabbar" aria-label="Navigasi ponsel">
        {renderTab(home)}
        {renderTab(review)}
        <button type="button" className="catat" onClick={openCapture} aria-label="Catat transaksi">
          <span className="plus" aria-hidden="true">
            <Icon name="plus" className="h-5 w-5" />
          </span>
          <span aria-hidden="true">Catat</span>
        </button>
        {renderTab(ledger)}
        <button
          ref={moreButton}
          type="button"
          aria-haspopup="dialog"
          aria-expanded={layer === "more"}
          aria-current={inMore ? "page" : undefined}
          onClick={() => setLayer("more")}
        >
          <Icon name="more" className="h-5 w-5" />
          <span>Lainnya</span>
        </button>
      </nav>
      <MoreSheet open={layer === "more"} onClose={closeTo} onOpenLayer={setLayer} />
      <NotificationsPanel open={layer === "notifications"} anchorRef={moreButton} onClose={() => setLayer(null)} />
      <MenuPanel
        open={layer === "theme"}
        anchorRef={moreButton}
        onClose={() => setLayer(null)}
        label="Tema"
        heading="Tema"
        entries={themeEntries}
        width={220}
      />
    </>
  );
}

/**
 * Lainnya (D5): the pages outside the tab bar in the sidebar's order, then Notifikasi, Tema and Keluar, which
 * stays here until phase 10 adds it to Pengaturan (owner decision 7). Modal: focus stays inside.
 */
export function MoreSheet({ open, onClose, onOpenLayer }: { open: boolean; onClose: () => void; onOpenLayer: (layer: PhoneLayer) => void }) {
  const pathname = usePathname();
  const { reportBadge, unreadIds, themeChoice, logout } = useWorkspace();
  const theme = THEME_CHOICES.find((choice) => choice.value === themeChoice)!;
  const unread = unreadIds.size;

  return (
    <ShellDialog open={open} onClose={onClose} title="Lainnya" width={480} closeLabel="Tutup Lainnya">
      <ul className="more-list">
        {MORE_LINKS.map((link) => {
          const active = isActivePath(pathname, link.href);
          const badge = link.badge === "report" && reportBadge;
          return (
            <li key={link.href}>
              <Link
                href={link.href}
                className="more-row"
                aria-current={active ? "page" : undefined}
                aria-label={badge ? `${link.label}, ${badgeWords("report", 1)}` : undefined}
                onClick={sameRouteClick(link.href, pathname, onClose)}
              >
                <Icon name={link.icon} className="h-5 w-5" />
                <span className="r-title t-strong">{link.label}</span>
                {badge && (
                  <span className="t-meta" aria-hidden="true">
                    1 baru
                  </span>
                )}
                <Icon name="chevron-right" className="h-4 w-4 chev" />
              </Link>
            </li>
          );
        })}
        <li>
          <button
            type="button"
            className="more-row"
            aria-haspopup="dialog"
            onClick={(event) => {
              closeEnclosingDialog(event.currentTarget);
              onOpenLayer("notifications");
            }}
          >
            <Icon name="bell" className="h-5 w-5" />
            <span className="r-title t-strong">Notifikasi</span>
            {unread > 0 && <span className="t-meta">{unread} baru</span>}
            <Icon name="chevron-right" className="h-4 w-4 chev" />
          </button>
        </li>
        <li>
          <button
            type="button"
            className="more-row"
            aria-haspopup="menu"
            onClick={(event) => {
              closeEnclosingDialog(event.currentTarget);
              onOpenLayer("theme");
            }}
          >
            <Icon name="sun" className="h-5 w-5" />
            <span className="r-title t-strong">Tema</span>
            <span className="t-meta">{theme.label}</span>
            <Icon name="chevron-right" className="h-4 w-4 chev" />
          </button>
        </li>
        <li>
          <button type="button" className="more-row" onClick={logout}>
            <Icon name="logout" className="h-5 w-5" />
            <span className="r-title t-strong">Keluar</span>
          </button>
        </li>
      </ul>
    </ShellDialog>
  );
}
