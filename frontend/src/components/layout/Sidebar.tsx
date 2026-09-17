"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { Icon, type IconName } from "@/components/ui/Icon";
import { useAppCtx } from "@/components/layout/AppLayout";
import { api } from "@/lib/api";

export interface NavSection {
  title?: string;
  items: Array<{
    href: string;
    label: string;
    icon: IconName;
    badge?: string | number;
  }>;
}

export const NAV_SECTIONS: NavSection[] = [
  {
    title: "RINGKASAN",
    items: [
      { href: "/", label: "Beranda", icon: "dashboard" },
      { href: "/insights", label: "Analisis", icon: "analysis" },
    ],
  },
  {
    title: "KEUANGAN",
    items: [
      { href: "/ledger", label: "Transaksi", icon: "ledger" },
      { href: "/accounts", label: "Rekening & Saldo", icon: "credit-card" },
      { href: "/goals", label: "Target & Tagihan", icon: "goals" },
    ],
  },
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
        "hidden lg:flex flex-col border-r border-[var(--border-structural)] bg-[var(--canvas-app)] transition-[width] duration-200 ease-in-out shrink-0 select-none",
        collapsed ? "w-[72px]" : "w-[230px]"
      )}
    >
      {/* Brand Header */}
      <div className="h-16 flex items-center px-4 border-b border-[var(--border-structural)] gap-3 shrink-0">
        <div className="h-9 w-9 rounded-xl bg-[#1E201E] text-white flex items-center justify-center font-bold text-sm shadow-xs shrink-0">
          <span className="text-[#66CC55] font-black text-base leading-none">⚡</span>
        </div>
        {!collapsed && (
          <div className="min-w-0">
            <div className="font-bold text-xs tracking-tight text-[var(--text-primary)] flex items-center gap-1.5 leading-none">
              <span>Cash Flow</span>
              <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-[#66CC55]/15 text-[#1E7E34] dark:text-[#66CC55] font-bold">
                OS
              </span>
            </div>
            <div className="text-[10px] text-[var(--text-muted)] font-medium mt-1 truncate">
              {user?.username ? `@${user.username}` : "Personal Ledger"}
            </div>
          </div>
        )}
      </div>

      {/* Navigation Sections */}
      <nav className="flex-1 flex flex-col justify-between px-3 pt-4 pb-3 overflow-y-auto space-y-4">
        <div className="space-y-4">
          {NAV_SECTIONS.map((section, sIdx) => (
            <div key={sIdx} className="space-y-1">
              {!collapsed && section.title && (
                <div className="px-3 py-1 text-[10px] font-bold uppercase tracking-wider text-[var(--text-tertiary)]">
                  {section.title}
                </div>
              )}
              {section.items.map((item) => {
                const active = isActive(item.href);
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    title={collapsed ? item.label : undefined}
                    className={cn(
                      "flex items-center gap-2.5 rounded-xl transition-all select-none group",
                      collapsed ? "h-10 justify-center px-0" : "px-3 py-2 text-xs font-semibold",
                      active
                        ? "bg-[#66CC55] text-[#141814] shadow-xs"
                        : "text-[var(--text-secondary)] hover:bg-[#EFEFEA] dark:hover:bg-[#272B27] hover:text-[var(--text-primary)]"
                    )}
                  >
                    <Icon
                      name={item.icon}
                      className={cn(
                        "h-4 w-4 shrink-0 transition-colors",
                        active ? "text-[#141814] stroke-[2.5]" : "text-[var(--text-muted)] group-hover:text-[var(--text-primary)]"
                      )}
                    />
                    {!collapsed && (
                      <span className="truncate flex-1 font-medium">{item.label}</span>
                    )}
                    {!collapsed && item.badge && (
                      <span
                        className={cn(
                          "text-[10px] font-bold px-1.5 py-0.5 rounded-full",
                          active
                            ? "bg-black/15 text-[#141814]"
                            : "bg-[var(--canvas-subtle)] text-[var(--text-muted)]"
                        )}
                      >
                        {item.badge}
                      </span>
                    )}
                  </Link>
                );
              })}
            </div>
          ))}
        </div>

        {/* User Profile & Footer Actions */}
        <div className="pt-3 border-t border-[var(--border-structural)]">
          {!collapsed ? (
            <div className="flex items-center justify-between p-2 rounded-xl bg-[var(--canvas-card)] border border-[var(--border-structural)] text-xs shadow-xs">
              <div className="flex items-center gap-2.5 min-w-0">
                <div className="h-7 w-7 rounded-lg bg-[#EFEFEA] dark:bg-[#2A2E2A] text-[var(--text-primary)] font-bold flex items-center justify-center shrink-0 text-[11px] uppercase">
                  {user?.username ? user.username.slice(0, 2) : "CF"}
                </div>
                <div className="min-w-0">
                  <span className="block font-bold text-[var(--text-primary)] truncate text-xs leading-none">
                    {user?.username || "Enrico"}
                  </span>
                  <span className="block text-[10px] text-[var(--text-muted)] font-medium mt-0.5 truncate">
                    {user?.currency || "IDR"} • Gajian #{user?.payday_day || 25}
                  </span>
                </div>
              </div>

              <div className="flex items-center gap-1 shrink-0">
                {onOpenSettings && (
                  <button
                    type="button"
                    onClick={onOpenSettings}
                    title="Pengaturan"
                    className="p-1 rounded-lg text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)] transition-colors cursor-pointer"
                  >
                    <Icon name="settings" className="h-3.5 w-3.5" />
                  </button>
                )}
                <button
                  type="button"
                  onClick={logout}
                  title="Keluar dari akun"
                  className="p-1 rounded-lg text-rose-500/80 hover:bg-rose-500/10 hover:text-rose-500 transition-colors cursor-pointer"
                >
                  <Icon name="logout" className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-center gap-2">
              <div
                title={`${user?.username || "Pengguna"} (${user?.currency || "IDR"})`}
                className="h-8 w-8 rounded-lg bg-[#EFEFEA] text-[var(--text-primary)] font-bold flex items-center justify-center text-xs uppercase cursor-default"
              >
                {user?.username ? user.username.slice(0, 2) : "CF"}
              </div>
              {onOpenSettings && (
                <button
                  type="button"
                  onClick={onOpenSettings}
                  title="Pengaturan"
                  className="p-1.5 rounded-lg text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)] transition-colors cursor-pointer"
                >
                  <Icon name="settings" className="h-3.5 w-3.5" />
                </button>
              )}
              <button
                type="button"
                onClick={logout}
                title="Keluar dari akun"
                className="p-1.5 rounded-lg text-rose-500/80 hover:bg-rose-500/10 hover:text-rose-500 transition-colors cursor-pointer"
              >
                <Icon name="logout" className="h-3.5 w-3.5" />
              </button>
            </div>
          )}
        </div>
      </nav>
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
      <div
        className="fixed inset-0 bg-black/50 backdrop-blur-xs transition-opacity"
        onClick={onClose}
      />

      <div className="relative flex w-72 max-w-[85vw] flex-col bg-[var(--canvas-app)] border-r border-[var(--border-structural)] shadow-xl">
        <div className="flex h-14 items-center justify-between px-4 border-b border-[var(--border-structural)]">
          <span className="text-xs font-bold uppercase tracking-wider text-[var(--text-tertiary)]">
            Menu Navigasi
          </span>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)]"
          >
            <Icon name="close" className="h-5 w-5" />
          </button>
        </div>

        <nav className="flex-1 space-y-4 px-4 py-3 overflow-y-auto">
          {NAV_SECTIONS.map((section, sIdx) => (
            <div key={sIdx} className="space-y-1">
              {section.title && (
                <div className="px-2 py-1 text-[10px] font-bold uppercase tracking-wider text-[var(--text-tertiary)]">
                  {section.title}
                </div>
              )}
              {section.items.map((item) => {
                const active = isActive(item.href);
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    onClick={onClose}
                    className={cn(
                      "flex items-center gap-3 rounded-xl px-3 py-2 text-xs font-semibold transition-colors",
                      active
                        ? "bg-[#66CC55] text-[#141814]"
                        : "text-[var(--text-secondary)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)]"
                    )}
                  >
                    <Icon
                      name={item.icon}
                      className={cn("h-4 w-4", active ? "text-[#141814]" : "text-[var(--text-muted)]")}
                    />
                    <span>{item.label}</span>
                  </Link>
                );
              })}
            </div>
          ))}
        </nav>

        <div className="border-t border-[var(--border-structural)] p-4">
          <div className="flex items-center justify-between p-2.5 rounded-xl bg-[var(--canvas-card)] border border-[var(--border-structural)] text-xs">
            <div className="flex items-center gap-2.5 min-w-0">
              <div className="h-8 w-8 rounded-lg bg-[#EFEFEA] text-[var(--text-primary)] font-bold flex items-center justify-center shrink-0 text-xs uppercase">
                {user?.username ? user.username.slice(0, 2) : "CF"}
              </div>
              <div className="min-w-0">
                <span className="block font-semibold text-[var(--text-primary)] truncate">
                  {user?.username || "Pengguna"}
                </span>
                <span className="block text-[10px] text-[var(--text-muted)] font-medium">
                  {user?.currency || "IDR"} • Gajian Tgl {user?.payday_day || 25}
                </span>
              </div>
            </div>
            <div className="flex items-center gap-1 shrink-0">
              {onOpenSettings && (
                <button
                  type="button"
                  onClick={onOpenSettings}
                  title="Pengaturan"
                  className="p-1.5 rounded-lg text-[var(--text-muted)] hover:bg-[var(--canvas-subtle)] hover:text-[var(--text-primary)] transition-colors"
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
