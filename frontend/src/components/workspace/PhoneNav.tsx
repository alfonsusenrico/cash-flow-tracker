"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useId, useRef, useState } from "react";
import { Icon } from "@/components/ui/Icon";
import { sameRouteClick } from "@/lib/urlState";
import { useWorkspace } from "./context";
import { syncDialog, useEscapeLayer, useFocusTrap } from "./layers";
import { NAV_GROUPS, PHONE_TAB_HREFS, REVIEW_HREF, SETTINGS_LINK, badgeWords, isActivePath } from "./navigation";

const TAB_LINKS = NAV_GROUPS.flatMap((group) => group.links);

function tabLink(href: string) {
  return TAB_LINKS.find((link) => link.href === href)!;
}

/** Bottom tab bar at 760px and below: Beranda, Tinjau, Catat, Transaksi and Lainnya. */
export function PhoneTabBar() {
  const pathname = usePathname();
  const { reviewAttention, openCapture } = useWorkspace();
  const [moreOpen, setMoreOpen] = useState(false);
  const moreButton = useRef<HTMLButtonElement>(null);
  const [home, review, ledger] = PHONE_TAB_HREFS.map(tabLink);
  const inMore = !PHONE_TAB_HREFS.some((href) => isActivePath(pathname, href));

  const renderTab = (link: typeof home) => {
    const active = isActivePath(pathname, link.href);
    const count = link.href === REVIEW_HREF ? reviewAttention ?? 0 : 0;
    return (
      <Link
        key={link.href}
        href={link.href}
        onClick={sameRouteClick(link.href, pathname)}
        className="ws-tab"
        aria-current={active ? "page" : undefined}
        aria-label={count ? `${link.label}, ${badgeWords("review", count)}` : link.label}
      >
        <Icon name={link.icon} />
        <span aria-hidden="true">{link.label}</span>
        {count > 0 && (
          <span className="ws-tab-badge" aria-hidden="true">
            {count > 99 ? "99+" : count}
          </span>
        )}
      </Link>
    );
  };

  return (
    <>
      <nav className="ws-tabbar" aria-label="Navigasi utama">
        {renderTab(home)}
        {renderTab(review)}
        <button type="button" className="ws-tab" onClick={openCapture} aria-label="Catat transaksi">
          <span className="ws-tab-catat" aria-hidden="true">
            <Icon name="plus" />
          </span>
          <span className="sr-only">Catat</span>
        </button>
        {renderTab(ledger)}
        <button
          ref={moreButton}
          type="button"
          className="ws-tab"
          aria-haspopup="dialog"
          aria-expanded={moreOpen}
          aria-current={inMore ? "page" : undefined}
          onClick={() => setMoreOpen(true)}
        >
          <Icon name="menu" />
          <span>Lainnya</span>
        </button>
      </nav>
      <MoreSheet
        open={moreOpen}
        onClose={() => {
          setMoreOpen(false);
          moreButton.current?.focus();
        }}
      />
    </>
  );
}

/** Every page outside the tab bar, plus notifications, Bantuan and Keluar. Modal: focus stays inside. */
export function MoreSheet({ open, onClose }: { open: boolean; onClose: () => void }) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  const pathname = usePathname();
  const { reportBadge, notifications, openHelp, logout } = useWorkspace();
  const links = [...TAB_LINKS.filter((link) => !PHONE_TAB_HREFS.includes(link.href as never)), SETTINGS_LINK];

  useEffect(() => {
    syncDialog(ref.current, open);
    if (open) ref.current?.querySelector<HTMLElement>("a, button")?.focus();
  }, [open]);
  useFocusTrap(ref, open);
  useEscapeLayer(open, onClose);

  return (
    <dialog
      ref={ref}
      className="ws-more"
      aria-labelledby={titleId}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      {open && (
        <>
          <div className="ws-grab" aria-hidden="true" />
          <div className="flex items-center justify-between gap-3">
            <h2 id={titleId} className="text-[17px] font-medium">
              Lainnya
            </h2>
            <button type="button" className="ws-close" aria-label="Tutup Lainnya" onClick={onClose}>
              <Icon name="x" className="h-4 w-4" />
            </button>
          </div>
          <nav aria-label="Halaman lain" className="mt-2 flex flex-col gap-0.5">
            {links.map((link) => {
              const active = isActivePath(pathname, link.href);
              const badge = link.badge === "report" && reportBadge;
              return (
                <Link
                  key={link.href}
                  href={link.href}
                  className="ws-menu-item"
                  aria-current={active ? "page" : undefined}
                  aria-label={badge ? `${link.label}, ${badgeWords("report", 1)}` : link.label}
                  onClick={sameRouteClick(link.href, pathname, onClose)}
                >
                  <Icon name={link.icon} className="h-5 w-5" />
                  <span className="flex-1">{link.label}</span>
                  {badge && (
                    <span className="ws-cnt" aria-hidden="true">
                      1
                    </span>
                  )}
                </Link>
              );
            })}
          </nav>
          {notifications.length > 0 && (
            <section aria-label="Notifikasi" className="mt-2 border-t border-[var(--line)] pt-2">
              <h3 className="ws-menu-h">Notifikasi</h3>
              {notifications.map((item) => (
                <Link key={item.id} href={item.href} className="ws-menu-item" onClick={onClose}>
                  <span>
                    {item.title}
                    <small>{item.detail}</small>
                  </span>
                </Link>
              ))}
            </section>
          )}
          <div className="mt-2 border-t border-[var(--line)] pt-2">
            <button
              type="button"
              className="ws-menu-item"
              onClick={() => {
                onClose();
                openHelp();
              }}
            >
              <Icon name="help-circle" className="h-5 w-5" />
              Bantuan
            </button>
            <button type="button" className="ws-menu-item" onClick={logout}>
              <Icon name="logout" className="h-5 w-5" />
              Keluar
            </button>
          </div>
        </>
      )}
    </dialog>
  );
}
