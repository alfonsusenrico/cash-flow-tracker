"use client";

import Link from "next/link";
import { Icon } from "@/components/ui/Icon";
import { useWorkspace } from "./context";
import { Disclosure } from "./Disclosure";
import { SETTINGS_LINK } from "./navigation";

function initialsOf(name: string): string {
  const words = name.trim().split(/[\s_.-]+/).filter(Boolean);
  const letters = words.length > 1 ? words[0][0] + words[1][0] : name.slice(0, 2);
  return letters.toUpperCase() || "?";
}

/**
 * The signed-in account and its menu: hide balances, Pengaturan, Keluar. On wider screens it is the card in
 * the sidebar foot; on phones the avatar in the header (refine-workspace-layout D3).
 */
export function AccountMenu({ variant }: { variant: "avatar" | "card" }) {
  const { username, displayName, hideBalances, toggleHideBalances, logout } = useWorkspace();
  const name = displayName || username || "";
  const initials = initialsOf(name || "?");
  return (
    <Disclosure
      label={`Akun ${name}`.trim()}
      buttonClassName={variant === "card" ? "ws-account-card" : "ws-avatar"}
      buttonContent={
        variant === "card" ? (
          <>
            <span className="ws-account-initials" aria-hidden="true">
              {initials}
            </span>
            <span className="ws-rail-hide min-w-0" aria-hidden="true">
              <strong>{name}</strong>
              {username && displayName && <small>{username}</small>}
            </span>
          </>
        ) : (
          <span aria-hidden="true">{initials}</span>
        )
      }
      panelLabel="Akun"
      anchorClassName={variant === "card" ? "ws-account-anchor" : undefined}
      panelClassName={variant === "card" ? "ws-menu-up" : undefined}
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
