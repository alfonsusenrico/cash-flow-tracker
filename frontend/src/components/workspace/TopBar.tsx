"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useRef, useState } from "react";
import { Icon } from "@/components/ui/Icon";
import { THEME_CHOICES } from "@/lib/theme";
import { useWorkspace } from "./context";
import { AccountMenu } from "./AccountMenu";
import { Disclosure } from "./Disclosure";
import { LEDGER_HREF, pageMeta } from "./navigation";
import { useShortcut } from "./shortcuts";

/** The id of Transaksi's own search field, which ⌘K focuses while Transaksi is open (D12). */
export const LEDGER_SEARCH_ID = "ledger-search";

function SearchBox() {
  const router = useRouter();
  const pathname = usePathname();
  const input = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState("");
  const onLedger = pathname === LEDGER_HREF;
  useShortcut("mod+k", () => {
    const pageSearch = onLedger ? document.getElementById(LEDGER_SEARCH_ID) : null;
    if (pageSearch) pageSearch.focus();
    else if (input.current && input.current.offsetParent !== null) input.current.focus();
    else router.push(LEDGER_HREF);
  });
  // Transaksi has its own search in its toolbar; a second field would search the same rows.
  if (onLedger) return null;
  return (
    <form
      role="search"
      className="ws-search ws-desk-only"
      onSubmit={(event) => {
        event.preventDefault();
        const text = query.trim();
        router.push(text ? `${LEDGER_HREF}?q=${encodeURIComponent(text)}` : LEDGER_HREF);
      }}
    >
      <Icon name="search" className="h-4 w-4 shrink-0" />
      <input
        ref={input}
        type="search"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Cari transaksi…"
        aria-label="Cari transaksi"
        aria-keyshortcuts="Meta+K Control+K"
        autoComplete="off"
      />
      <kbd className="ws-kbd" aria-hidden="true">
        ⌘K
      </kbd>
    </form>
  );
}

function ThemeMenu() {
  const { themeChoice, resolvedTheme, setThemeChoice } = useWorkspace();
  const current = THEME_CHOICES.find((choice) => choice.value === themeChoice)!;
  return (
    <Disclosure
      label={`Tema: ${current.label}`}
      buttonClassName="ws-icon-btn"
      buttonContent={<Icon name={resolvedTheme === "dark" ? "moon" : "sun"} className="h-[18px] w-[18px]" />}
      panelLabel="Pilih tema"
    >
      {(close) => (
        <>
          <div className="ws-menu-h">Tema</div>
          {THEME_CHOICES.map((choice) => (
            <button
              key={choice.value}
              type="button"
              className="ws-menu-item"
              aria-pressed={choice.value === themeChoice}
              onClick={() => {
                setThemeChoice(choice.value);
                close();
              }}
            >
              <Icon name={choice.value === "dark" ? "moon" : choice.value === "light" ? "sun" : "monitor"} className="h-4 w-4" />
              <span className="flex-1">{choice.label}</span>
              {choice.value === themeChoice && <Icon name="check" className="h-4 w-4" />}
            </button>
          ))}
        </>
      )}
    </Disclosure>
  );
}

function NotificationsMenu() {
  const { notifications } = useWorkspace();
  const count = notifications.length;
  return (
    <div className="ws-desk-only">
      <Disclosure
        label={count ? `Notifikasi, ${count} baru` : "Notifikasi, tidak ada yang baru"}
        buttonClassName="ws-icon-btn"
        buttonContent={
          <>
            <Icon name="bell" className="h-[18px] w-[18px]" />
            {count > 0 && <span className="ws-pip" aria-hidden="true" />}
          </>
        }
        panelLabel="Notifikasi"
      >
        {(close) =>
          count ? (
            <>
              <div className="ws-menu-h">Notifikasi</div>
              {notifications.map((item) => (
                <Link key={item.id} href={item.href} className="ws-menu-item" onClick={close}>
                  <span>
                    {item.title}
                    <small>{item.detail}</small>
                  </span>
                </Link>
              ))}
            </>
          ) : (
            <p className="px-2.5 py-3 text-sm text-[var(--muted)]">Belum ada yang baru.</p>
          )
        }
      </Disclosure>
    </div>
  );
}

export function TopBar() {
  const pathname = usePathname();
  const { header, openCapture, setControlsSlot } = useWorkspace();
  const meta = header ?? pageMeta(pathname);
  return (
    <header className="ws-topbar">
      <div className="ws-title">
        <h1>{meta.title}</h1>
        {meta.subtitle && <p>{meta.subtitle}</p>}
      </div>
      <div className="ws-controls" ref={setControlsSlot} />
      <div className="ws-actions">
        <SearchBox />
        <ThemeMenu />
        <NotificationsMenu />
        <button type="button" className="ws-btn ws-btn-accent ws-desk-only" onClick={openCapture} aria-keyshortcuts="N">
          <Icon name="plus" className="h-4 w-4" />
          Catat
        </button>
        {/* On wider screens the account card sits in the sidebar foot (refine-workspace-layout D3). */}
        <div className="ws-only-phone">
          <AccountMenu variant="avatar" />
        </div>
      </div>
    </header>
  );
}
