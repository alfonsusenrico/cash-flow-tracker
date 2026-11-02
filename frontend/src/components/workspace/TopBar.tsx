"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { type ReactNode, useCallback, useId, useRef, useState } from "react";
import { Icon } from "@/components/ui/Icon";
import { THEME_CHOICES } from "@/lib/theme";
import { useWorkspace } from "./context";
import { useEscapeLayer, useOutsidePress } from "./layers";
import { LEDGER_HREF, SETTINGS_LINK, pageMeta } from "./navigation";
import { useShortcut } from "./shortcuts";

/** A button with a popover panel; Esc or a press outside closes it and Esc returns focus to the button. */
function Disclosure({
  label,
  buttonClassName,
  buttonContent,
  panelLabel,
  children,
}: {
  label: string;
  buttonClassName: string;
  buttonContent: ReactNode;
  panelLabel: string;
  children: (close: () => void) => ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const anchor = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const panelId = useId();
  const close = useCallback(() => setOpen(false), []);
  const closeAndReturn = useCallback(() => {
    setOpen(false);
    button.current?.focus();
  }, []);
  useEscapeLayer(open, closeAndReturn);
  useOutsidePress([anchor], open, close);
  return (
    <div className="ws-pop-anchor" ref={anchor}>
      <button
        ref={button}
        type="button"
        className={buttonClassName}
        aria-label={label}
        aria-expanded={open}
        aria-controls={open ? panelId : undefined}
        onClick={() => setOpen((value) => !value)}
      >
        {buttonContent}
      </button>
      {open && (
        <div id={panelId} className="ws-menu" role="region" aria-label={panelLabel}>
          {children(close)}
        </div>
      )}
    </div>
  );
}

function SearchBox() {
  const router = useRouter();
  const input = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState("");
  useShortcut("mod+k", () => {
    if (input.current && input.current.offsetParent !== null) input.current.focus();
    else router.push(LEDGER_HREF);
  });
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
            <p className="px-2.5 py-3 text-[13.5px] text-[var(--muted)]">Belum ada yang baru.</p>
          )
        }
      </Disclosure>
    </div>
  );
}

function AccountMenu() {
  const { username, hideBalances, toggleHideBalances, logout } = useWorkspace();
  const initials = (username ?? "?").slice(0, 1);
  return (
    <Disclosure
      label={`Akun ${username ?? ""}`.trim()}
      buttonClassName="ws-avatar"
      buttonContent={<span aria-hidden="true">{initials}</span>}
      panelLabel="Akun"
    >
      {(close) => (
        <>
          <div className="ws-menu-h">
            Masuk sebagai <strong className="font-medium text-[var(--text)]">{username}</strong>
          </div>
          <button
            type="button"
            className="ws-menu-item"
            onClick={() => {
              toggleHideBalances();
              close();
            }}
          >
            <Icon name={hideBalances ? "eye" : "eye-off"} className="h-4 w-4" />
            {hideBalances ? "Tampilkan saldo" : "Sembunyikan saldo"}
          </button>
          <Link href={SETTINGS_LINK.href} className="ws-menu-item" onClick={close}>
            <Icon name="sliders" className="h-4 w-4" />
            Pengaturan
          </Link>
          <div className="ws-menu-sep" />
          <button type="button" className="ws-menu-item" onClick={logout}>
            <Icon name="logout" className="h-4 w-4" />
            Keluar
          </button>
        </>
      )}
    </Disclosure>
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
        <button type="button" className="ws-btn ws-btn-ink ws-desk-only" onClick={openCapture} aria-keyshortcuts="N">
          <Icon name="plus" className="h-4 w-4" />
          Catat
        </button>
        <AccountMenu />
      </div>
    </header>
  );
}
