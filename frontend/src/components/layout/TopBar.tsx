"use client";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { useAppCtx } from "@/components/layout/AppLayout";

interface TopBarProps {
  title: string;
  showDateRange?: boolean;
}

export function TopBar({ title, showDateRange = true }: TopBarProps) {
  const router = useRouter();
  const { hideBalances, setHideBalances, theme, setTheme, paydayDay, summaryRange, user } = useAppCtx();

  const initials = user?.full_name
    ? user.full_name.split(" ").map((n: string) => n[0]).join("").slice(0, 2).toUpperCase()
    : user?.username?.slice(0, 2).toUpperCase() ?? "??";

  async function logout() {
    await api.post("/auth/logout");
    router.push("/auth/login");
  }

  function toggleTheme() {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.classList.toggle("dark", next === "dark");
    localStorage.setItem("theme", next);
  }

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
            <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-sm font-medium hover:bg-[var(--bg)] transition-colors" style={{ borderColor: "var(--border)" }}>
              <span className="text-[var(--muted)]">📅</span>
              <span>{summaryRange.from} – {summaryRange.to}</span>
              <span className="text-[var(--muted)]">▾</span>
            </button>
            <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-primary-light text-primary border border-primary/20">
              Payday in {paydayDay} days
            </span>
          </div>
        )}
      </div>

      {/* Right: controls */}
      <div className="flex items-center gap-3">
        {/* Hide balances */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-[var(--muted)]">🙈 Hide balances</span>
          <button
            onClick={() => {
              const next = !hideBalances;
              setHideBalances(next);
              localStorage.setItem("hideBalances", next ? "1" : "0");
            }}
            className={`relative w-10 h-5 rounded-full transition-colors ${hideBalances ? "bg-primary" : "bg-gray-300"}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${hideBalances ? "translate-x-5" : ""}`} />
          </button>
        </div>

        {/* Theme */}
        <button onClick={toggleTheme} className="p-1.5 rounded-lg hover:bg-[var(--bg)] text-[var(--muted)] transition-colors" title="Light mode">
          ☀️
        </button>
        <button onClick={toggleTheme} className="p-1.5 rounded-lg hover:bg-[var(--bg)] text-[var(--muted)] transition-colors" title="Dark mode">
          🌙
        </button>

        {/* Settings */}
        <button className="p-1.5 rounded-lg hover:bg-[var(--bg)] text-[var(--muted)] transition-colors">⚙️</button>

        {/* User avatar */}
        <button
          onClick={logout}
          className="w-8 h-8 rounded-full bg-primary text-white text-xs font-bold flex items-center justify-center hover:bg-primary-hover transition-colors"
          title="Logout"
        >
          {initials}
        </button>
      </div>
    </header>
  );
}
