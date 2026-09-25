"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { Icon, type IconName } from "@/components/ui/Icon";
import { useAppCtx } from "@/components/layout/AppLayout";
import { api } from "@/lib/api";

export interface NavItem {
  href: string;
  label: string;
  icon: IconName;
  badge?: string;
}

export const MAIN_NAV: NavItem[] = [
  { href: "/", label: "Beranda", icon: "dashboard" },
  { href: "/ledger", label: "Transaksi", icon: "ledger" },
  { href: "/insights", label: "Analisis", icon: "analysis" },
  { href: "/accounts", label: "Rekening & Saldo", icon: "credit-card" },
  { href: "/goals", label: "Target & Tagihan", icon: "goals" },
];

interface SidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
  onQuickAdd?: () => void;
  onOpenSettings?: () => void;
}

export function Sidebar({
  collapsed,
  onToggleCollapse,
  onOpenSettings,
}: SidebarProps) {
  const pathname = usePathname();
  const { user } = useAppCtx();
  const [collapsedMenuOpen, setCollapsedMenuOpen] = useState(false);

  useEffect(() => {
    if (!collapsed) setCollapsedMenuOpen(false);
  }, [collapsed]);

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  async function logout() {
    try {
      await api.post("/auth/logout");
    } finally {
      window.location.replace("/auth/login");
    }
  }

  return (
    <aside
      className={cn(
        "fixed left-0 top-0 z-40 hidden h-screen flex-col border-r border-[var(--border)] bg-[var(--sidebar)] transition-[width] duration-200 ease-in-out lg:flex",
        collapsed ? "w-[72px]" : "w-[260px]"
      )}
    >
      {/* Primary Navigation List */}
      <nav className="flex-1 flex flex-col justify-between px-3 pt-4 pb-3 overflow-y-auto">
        <div className="space-y-1">
          {MAIN_NAV.map((item) => {
            const active = isActive(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                title={collapsed ? item.label : undefined}
                className={cn(
                  "flex items-center gap-3 rounded-xl transition-all pressable group",
                  collapsed ? "h-10 justify-center px-0" : "px-3 py-2.5 text-xs font-medium",
                  active
                    ? "bg-primary text-primary-contrast font-bold shadow-xs"
                    : "text-[var(--sidebar-text)] hover:bg-[var(--sidebar-hover)] hover:text-[var(--sidebar-text-active)]"
                )}
              >
                <Icon
                  name={item.icon}
                  className={cn(
                    "h-4 w-4 shrink-0 transition-transform group-hover:scale-110",
                    active ? "text-primary-contrast" : "text-[var(--sidebar-text)] group-hover:text-[var(--sidebar-text-active)]"
                  )}
                />
                {!collapsed && <span>{item.label}</span>}
              </Link>
            );
          })}
        </div>

        {/* Collapse Toggle */}
        <div className="pt-2">
          <button
            type="button"
            onClick={onToggleCollapse}
            title={collapsed ? "Tampilkan menu samping" : "Sembunyikan menu samping"}
            className={cn(
              "flex w-full items-center gap-3 rounded-xl transition-colors text-[var(--sidebar-text)] hover:bg-[var(--sidebar-hover)] hover:text-[var(--sidebar-text-active)]",
              collapsed ? "h-10 justify-center px-0" : "px-3 py-2 text-xs font-medium"
            )}
          >
            <Icon
              name={collapsed ? "chevron-right" : "chevron-left"}
              className="h-4 w-4 shrink-0"
            />
            {!collapsed && <span>Sembunyikan menu</span>}
          </button>
        </div>
      </nav>

      {/* Footer Profile with Integrated Logout */}
      <div className="border-t border-[var(--border)] p-3">
        {!collapsed ? (
          <div className="flex items-center justify-between p-2 rounded-xl bg-[var(--sidebar-hover)] border border-[var(--border)]/60 text-xs">
            <div className="flex items-center gap-2 min-w-0">
              <div className="h-7 w-7 rounded-lg bg-primary/20 text-primary font-bold flex items-center justify-center shrink-0 text-xs uppercase">
                {user?.username ? user.username.slice(0, 2) : "CF"}
              </div>
              <div className="min-w-0">
                <span className="block font-semibold text-[var(--sidebar-text-active)] truncate">
                  {user?.username || "User"}
                </span>
                <span className="block text-[10px] text-[var(--sidebar-text)] font-medium">
                  {user?.currency || "IDR"} • Gajian Tgl {user?.payday_day || 25}
                </span>
              </div>
            </div>
            <div className="flex items-center gap-0.5 shrink-0">
              {onOpenSettings && (
                <button
                  type="button"
                  onClick={onOpenSettings}
                  aria-label="Pengaturan"
                  title="Pengaturan"
                  className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg text-[var(--sidebar-text)] hover:bg-[var(--sidebar-active)] hover:text-[var(--sidebar-text-active)] focus-visible:outline-2 focus-visible:outline-[var(--primary)] transition-colors"
                >
                  <Icon name="settings" className="h-3.5 w-3.5" />
                </button>
              )}
              <button
                type="button"
                onClick={logout}
                title="Keluar dari akun"
                className="p-1.5 rounded-lg text-rose-500/80 hover:bg-rose-500/10 hover:text-rose-500 transition-colors"
              >
                <Icon name="logout" className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>
        ) : (
          <div className="relative flex justify-center">
            {/* Clickable Avatar Button */}
            <button
              type="button"
              onClick={() => setCollapsedMenuOpen((prev) => !prev)}
              title={`${user?.username || "Pengguna"} — Menu Akun`}
              className={cn(
                "h-9 w-9 rounded-xl font-bold flex items-center justify-center text-xs uppercase transition-all pressable shadow-2xs",
                collapsedMenuOpen
                  ? "bg-primary text-primary-contrast ring-2 ring-primary/40 shadow-xs"
                  : "bg-primary/20 text-primary hover:bg-primary/30"
              )}
            >
              {user?.username ? user.username.slice(0, 2) : "CF"}
            </button>

            {/* Floating Popover / FAB Menu */}
            {collapsedMenuOpen && (
              <>
                <div
                  className="fixed inset-0 z-40"
                  onClick={() => setCollapsedMenuOpen(false)}
                />
                <div className="absolute left-[60px] bottom-0 z-50 w-52 rounded-2xl bg-[var(--surface)] border border-[var(--border)] shadow-xl p-1.5 space-y-1 animate-in fade-in zoom-in-95 duration-150">
                  <div className="px-2.5 py-2 border-b border-[var(--border)]">
                    <span className="block text-xs font-bold text-[var(--text)] truncate">
                      {user?.username || "Pengguna"}
                    </span>
                    <span className="block text-[10px] text-[var(--muted)] font-medium">
                      {user?.currency || "IDR"} • Gajian Tgl {user?.payday_day || 25}
                    </span>
                  </div>

                  {onOpenSettings && (
                    <button
                      type="button"
                      onClick={() => {
                        setCollapsedMenuOpen(false);
                        onOpenSettings();
                      }}
                      className="flex w-full items-center gap-2.5 px-2.5 py-2 rounded-xl text-xs font-medium text-[var(--text)] hover:bg-[var(--surface-raised)] transition-colors text-left"
                    >
                      <Icon name="settings" className="h-4 w-4 text-[var(--muted)]" />
                      <span>Pengaturan</span>
                    </button>
                  )}

                  <button
                    type="button"
                    onClick={() => {
                      setCollapsedMenuOpen(false);
                      logout();
                    }}
                    className="flex w-full items-center gap-2.5 px-2.5 py-2 rounded-xl text-xs font-medium text-rose-500 hover:bg-rose-500/10 transition-colors text-left"
                  >
                    <Icon name="logout" className="h-4 w-4 text-rose-500" />
                    <span>Keluar dari Akun</span>
                  </button>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </aside>
  );
}

export function MobileDrawer({
  open,
  onClose,
  onOpenSettings,
}: {
  open: boolean;
  onClose: () => void;
  onQuickAdd?: () => void;
  onOpenSettings?: () => void;
}) {
  const pathname = usePathname();
  const { user } = useAppCtx();

  if (!open) return null;

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  async function logout() {
    try {
      await api.post("/auth/logout");
    } finally {
      window.location.replace("/auth/login");
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex lg:hidden">
      {/* Backdrop */}
      <button
        type="button"
        aria-label="Tutup menu navigasi"
        className="fixed inset-0 bg-black/50 backdrop-blur-xs transition-opacity"
        onClick={onClose}
      />

      {/* Drawer Panel */}
      <div className="relative flex w-72 max-w-[85vw] flex-col bg-[var(--surface)] border-r border-[var(--border)] shadow-xl">
        {/* Header */}
        <div className="flex h-14 items-center justify-between px-4 border-b border-[var(--border)]">
          <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">Menu Navigasi</span>
          <button
            type="button"
            onClick={onClose}
            aria-label="Tutup menu"
            className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg text-[var(--muted)] hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
          >
            <Icon name="close" className="h-5 w-5" />
          </button>
        </div>

        {/* Navigation List */}
        <nav className="flex-1 space-y-1.5 px-4 py-3 overflow-y-auto">
          {MAIN_NAV.map((item) => {
            const active = isActive(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={onClose}
                className={cn(
                  "flex items-center gap-3 rounded-xl px-3.5 py-2.5 text-xs font-medium transition-colors",
                  active
                    ? "bg-income/10 text-income font-semibold"
                    : "text-[var(--muted)] hover:bg-[var(--surface-raised)] hover:text-[var(--text)]"
                )}
              >
                <Icon
                  name={item.icon}
                  className={cn("h-4 w-4", active ? "text-income" : "text-[var(--muted)]")}
                />
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>

        {/* Footer with Integrated Logout */}
        <div className="border-t border-[var(--border)] p-4">
          <div className="flex items-center justify-between p-2.5 rounded-xl bg-[var(--surface-raised)] border border-[var(--border)]/60 text-xs">
            <div className="flex items-center gap-2.5 min-w-0">
              <div className="h-8 w-8 rounded-lg bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 font-bold flex items-center justify-center shrink-0 text-xs uppercase">
                {user?.username ? user.username.slice(0, 2) : "CF"}
              </div>
              <div className="min-w-0">
                <span className="block font-semibold text-[var(--text)] truncate">
                  {user?.username || "Pengguna"}
                </span>
                <span className="block text-[10px] text-[var(--muted)] font-medium">
                  {user?.currency || "IDR"} • Gajian Tgl {user?.payday_day || 25}
                </span>
              </div>
            </div>
            <div className="flex items-center gap-1 shrink-0">
              {onOpenSettings && (
                <button
                  type="button"
                  onClick={onOpenSettings}
                  aria-label="Pengaturan"
                  title="Pengaturan"
                  className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg text-[var(--muted)] hover:bg-[var(--surface)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)] transition-colors"
                >
                  <Icon name="settings" className="h-4 w-4" />
                </button>
              )}
              <button
                type="button"
                onClick={logout}
                title="Keluar dari akun"
                className="p-1.5 rounded-lg text-rose-500 hover:bg-rose-500/10 transition-colors"
              >
                <Icon name="logout" className="h-4 w-4" />
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
