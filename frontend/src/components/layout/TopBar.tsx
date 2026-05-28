"use client";
import { useState } from "react";
import { api } from "@/lib/api";
import { useAppCtx } from "@/components/layout/AppLayout";
import { SettingsModal } from "@/components/ui/SettingsModal";
import { Icon } from "@/components/ui/Icon";

interface TopBarProps {
  title: string;
  showDateRange?: boolean;
}

export function TopBar({ title, showDateRange = true }: TopBarProps) {
  const { hideBalances, setHideBalances, theme, setTheme, paydayDay, paydaySource, summaryRange, user } = useAppCtx();
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);

  async function logout() {
    try {
      await api.post("/auth/logout");
    } finally {
      window.location.replace("/auth/login");
    }
  }

  function toggleTheme() {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.classList.toggle("dark", next === "dark");
    localStorage.setItem("theme", next);
  }

  function toggleHideBalances() {
    const next = !hideBalances;
    setHideBalances(next);
    localStorage.setItem("hideBalances", next ? "1" : "0");
  }

  const paydayLabel = paydaySource === "override" ? `Cycle day ${paydayDay}` : "Set pay cycle";
  const paydayTitle = paydaySource === "override"
    ? "Open settings to change your payday cycle"
    : "Open settings to set your payday cycle";

  return (
    <header className="fixed left-0 right-0 top-0 z-40 flex h-[var(--topbar-height)] items-center justify-between gap-3 border-b border-[var(--border)] bg-[var(--surface)]/95 px-4 backdrop-blur lg:left-[var(--sidebar-width)] lg:px-6">
      <div className="flex min-w-0 items-center gap-3">
        <h1 className="truncate text-base font-bold tracking-[-0.01em] text-[var(--text)] lg:text-lg">{title}</h1>
        {showDateRange && summaryRange && (
          <div className="hidden items-center gap-2 md:flex">
            <button type="button" disabled title="Date range selection is coming soon" className="flex cursor-not-allowed items-center gap-1.5 rounded-[var(--radius-md)] border border-[var(--border)] px-3 py-1.5 text-xs font-medium opacity-80">
              <span className="text-[var(--muted)]">Cycle</span>
              <span className="tabular">{summaryRange.from} - {summaryRange.to}</span>
            </button>
            <button
              onClick={() => setSettingsOpen(true)}
              className="rounded-[var(--radius-md)] border border-[var(--primary)]/20 bg-[var(--primary-light)] px-2.5 py-1 text-xs font-semibold text-[var(--primary)] transition-colors hover:bg-[var(--primary)]/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus)]"
              title={paydayTitle}
            >
              {paydayLabel}
            </button>
          </div>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-2">
        <div className="hidden items-center gap-2 sm:flex">
          <span className="text-xs text-[var(--muted)]">Hide balances</span>
          <button
            onClick={toggleHideBalances}
            className={`relative h-5 w-9 rounded-full transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus)] ${hideBalances ? "bg-[var(--primary)]" : "bg-[var(--color-rule-strong)]"}`}
            aria-pressed={hideBalances}
          >
            <span className={`absolute left-0.5 top-0.5 h-4 w-4 rounded-full bg-white shadow transition-transform ${hideBalances ? "translate-x-4" : ""}`} />
          </button>
        </div>

        <button
          onClick={toggleTheme}
          className="inline-flex h-8 w-8 items-center justify-center rounded-[var(--radius-md)] text-[var(--muted)] transition-colors hover:bg-[var(--bg)] hover:text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus)]"
          title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
          aria-label={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
        >
          <Icon name={theme === "dark" ? "sun" : "moon"} className="h-4 w-4" />
        </button>

        <button
          onClick={() => setSettingsOpen(true)}
          className="inline-flex h-8 w-8 items-center justify-center rounded-[var(--radius-md)] text-[var(--muted)] transition-colors hover:bg-[var(--bg)] hover:text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus)]"
          title="Settings"
          aria-label="Open settings"
        >
          <Icon name="settings" className="h-4 w-4" />
        </button>

        <div className="relative">
          <button
            onClick={() => setUserMenuOpen((open) => !open)}
            className="flex h-8 w-8 items-center justify-center rounded-full bg-[var(--primary)] text-white transition-colors hover:bg-[var(--primary-hover)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-focus)]"
            title="Account menu"
            aria-label="Open account menu"
            aria-haspopup="menu"
            aria-expanded={userMenuOpen}
          >
            <Icon name="user" className="h-4 w-4" />
          </button>
          {userMenuOpen && (
            <div role="menu" className="absolute right-0 top-10 z-50 w-56 rounded-[var(--radius-md)] border border-[var(--border)] bg-[var(--surface)] py-2 shadow-[var(--shadow-md)]">
              <div className="border-b border-[var(--border)] px-3 pb-2">
                <p className="truncate text-sm font-semibold text-[var(--text)]">
                  {user?.full_name || user?.username || "Account"}
                </p>
                {user?.full_name && user?.username && (
                  <p className="truncate text-xs text-[var(--muted)]">{user.username}</p>
                )}
              </div>
              <button
                type="button"
                role="menuitem"
                onClick={logout}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-[var(--text)] transition-colors hover:bg-[var(--bg)]"
              >
                <Icon name="logout" className="h-4 w-4 text-[var(--muted)]" />
                Logout
              </button>
            </div>
          )}
        </div>
      </div>
      <SettingsModal
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        paydayDay={paydayDay}
        paydaySource={paydaySource}
        hideBalances={hideBalances}
        onHideBalancesToggle={toggleHideBalances}
      />
    </header>
  );
}
