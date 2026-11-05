"use client";

import { useId, useRef, useState } from "react";
import { useWorkspace } from "./context";
import { MenuPanel } from "./Menu";

export function initialsOf(name: string): string {
  const words = name.trim().split(/[\s_.-]+/).filter(Boolean);
  const letters = words.length > 1 ? words[0][0] + words[1][0] : name.slice(0, 2);
  return letters.toUpperCase() || "?";
}

/**
 * The user button in the sidebar foot and its account menu (D1, D6): the name and username, Profil and Keluar.
 * "Sembunyikan saldo" sits in the top bar and Pengaturan in the sidebar foot.
 */
export function AccountMenu() {
  const { username, displayName, logout } = useWorkspace();
  const [open, setOpen] = useState(false);
  const button = useRef<HTMLButtonElement>(null);
  const menuId = useId();
  const name = displayName || username || "";
  const second = displayName && username ? username : null;
  return (
    <>
      <button
        ref={button}
        type="button"
        className="user-btn"
        aria-label={`Akun ${name}`.trim()}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? menuId : undefined}
        onClick={() => setOpen((value) => !value)}
      >
        <span className="avatar" aria-hidden="true">
          {initialsOf(name || "?")}
        </span>
        <span className="who" aria-hidden="true">
          <span className="t-body">{name}</span>
          {second && <span className="t-meta">{second}</span>}
        </span>
      </button>
      <MenuPanel
        open={open}
        anchorRef={button}
        onClose={() => setOpen(false)}
        label="Akun"
        id={menuId}
        width={240}
        header={
          <>
            <div className="popover-who">
              <p className="t-body">{name}</p>
              {second && <p className="t-meta">{second}</p>}
            </div>
            <div className="menu-sep" role="separator" />
          </>
        }
        entries={[
          { key: "profil", label: "Profil", icon: "settings", href: "/settings/profil" },
          { key: "keluar", label: "Keluar", icon: "logout", onSelect: logout },
        ]}
      />
    </>
  );
}
