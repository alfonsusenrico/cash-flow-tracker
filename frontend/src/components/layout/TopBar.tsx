"use client";
import { useState } from "react";
import { api } from "@/lib/api";
import { useAppCtx } from "@/components/layout/AppLayout";
import { SettingsModal } from "@/components/ui/SettingsModal";

interface TopBarProps {
  title: string;
  showDateRange?: boolean;
}

export function TopBar({ title, showDateRange = true }: TopBarProps) {
  const { hideBalances, setHideBalances, theme, setTheme, paydayDay, paydaySource, summaryRange, user } = useAppCtx();
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);

  const initials = user?.full_name
    ? user.full_name.split(" ").map((n: string) => n[0]).join("").slice(0, 2).toUpperCase()
    : user?.username?.slice(0, 2).toUpperCase() ?? "??";

  async function logout() {
    await api.post("/auth/logout");
    window.location.replace("/auth/login");
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

  const paydayLabel = paydaySource === "override"
    ? `Pay cycle override: day ${paydayDay}`
    : "Set pay cycle";
  const paydayTitle = paydaySource === "override"
    ? "Open settings to change your payday cycle"
    : "Open settings to set your payday cycle";

  return (
    <header
      className="fixed top-0 right-0 h-14 flex items-center justify-between px-6 z-40 border-b"
      style={{
        left: "200px",
        background: "var(--surface)",
        borderColor: "var(--border)",
      }}
    >
      {/* Left: title + date range */}
      <div className="flex items-center gap-4">
        <h1 className="text-lg font-bold text-[var(--text)]">{title}</h1>
        {showDateRange && summaryRange && (
          <div className="flex items-center gap-2">
            <button type="button" disabled title="Date range selection is coming soon" className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-sm font-medium opacity-80 cursor-not-allowed" style={{ borderColor: "var(--border)" }}>
              <span className="text-[var(--muted)]">📅</span>
              <span>{summaryRange.from} – {summaryRange.to}</span>
            </button>
            <button
              onClick={() => setSettingsOpen(true)}
              className="px-2.5 py-1 rounded-full text-xs font-semibold bg-primary-light text-primary border border-primary/20 hover:bg-primary/10 transition-colors"
              title={paydayTitle}
            >
              {paydayLabel}
            </button>
          </div>
        )}
      </div>

      {/* Right: controls */}
      <div className="flex items-center gap-3">
        {/* Hide balances */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-[var(--muted)]">🙈 Hide balances</span>
          <button
            onClick={toggleHideBalances}
            className={`relative w-10 h-5 rounded-full transition-colors ${hideBalances ? "bg-primary" : "bg-gray-300"}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${hideBalances ? "translate-x-5" : ""}`} />
          </button>
        </div>

        {/* Theme */}
        <button
          onClick={toggleTheme}
          className="p-1.5 rounded-lg hover:bg-[var(--bg)] text-[var(--muted)] transition-colors"
          title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
        >
          {theme === "dark" ? "☀️" : "🌙"}
        </button>

        {/* Settings */}
        <button
          onClick={() => setSettingsOpen(true)}
          className="p-1.5 rounded-lg hover:bg-[var(--bg)] text-[var(--muted)] transition-colors"
          title="Settings"
        >
          ⚙️
        </button>

        {/* User menu */}
        <div className="relative">
          <button
            onClick={() => setUserMenuOpen((open) => !open)}
            className="w-8 h-8 rounded-full bg-primary text-white text-xs font-bold flex items-center justify-center hover:bg-primary-hover transition-colors"
            title="Account menu"
            aria-haspopup="menu"
            aria-expanded={userMenuOpen}
          >
            {initials}
          </button>
          {userMenuOpen && (
            <div
              role="menu"
              className="absolute right-0 top-10 w-56 rounded-lg border shadow-lg py-2 z-50"
              style={{ background: "var(--surface)", borderColor: "var(--border)" }}
            >
              <div className="px-3 pb-2 border-b" style={{ borderColor: "var(--border)" }}>
                <p className="text-sm font-semibold text-[var(--text)] truncate">
                  {user?.full_name || user?.username || "Account"}
                </p>
                {user?.full_name && user?.username && (
                  <p className="text-xs text-[var(--muted)] truncate">{user.username}</p>
                )}
              </div>
              <button
                type="button"
                role="menuitem"
                onClick={logout}
                className="w-full text-left px-3 py-2 text-sm text-[var(--text)] hover:bg-[var(--bg)] transition-colors"
              >
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
