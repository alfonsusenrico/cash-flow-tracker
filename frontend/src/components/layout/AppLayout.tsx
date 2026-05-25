"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useState, useEffect, createContext, useContext } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";
import { SettingsModal } from "@/components/ui/SettingsModal";
import type { Account, SummaryResponse } from "@/types/domain";

// Global context for hide-balances and theme
interface AppCtx {
  hideBalances: boolean;
  theme: "light" | "dark";
  accounts: Account[];
  paydayDay: number;
}
const AppContext = createContext<AppCtx>({ hideBalances: false, theme: "light", accounts: [], paydayDay: 25 });
export const useAppCtx = () => useContext(AppContext);

const NAV = [
  { href: "/dashboard", label: "Summary" },
  { href: "/ledger", label: "Transactions" },
  { href: "/analysis", label: "Analysis" },
  { href: "/accounts", label: "Accounts" },
  { href: "/buckets", label: "Buckets" },
  { href: "/allocation", label: "Allocation" },
  { href: "/strategy", label: "Strategy" },
  { href: "/goals", label: "Goals" },
  { href: "/assets", label: "Assets" },
  { href: "/net-worth", label: "Net Worth" },
  { href: "/categories", label: "Categories" },
];

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [hideBalances, setHideBalances] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark">("light");

  // Load persisted prefs
  useEffect(() => {
    const saved = localStorage.getItem("theme") as "light" | "dark" | null;
    const t = saved ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    setTheme(t);
    document.documentElement.classList.toggle("dark", t === "dark");
    setHideBalances(localStorage.getItem("hideBalances") === "1");
  }, []);

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

  const { data: accountsData } = useQuery<{ accounts: Account[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const { data: summaryData } = useQuery<SummaryResponse>({
    queryKey: ["summary"],
    queryFn: () => api.get("/summary"),
  });

  const accounts = accountsData?.accounts ?? [];
  const paydayDay = summaryData?.payday?.default_day ?? 25;

  async function handleLogout() {
    await api.post("/auth/logout");
    router.push("/auth/login");
  }

  return (
    <AppContext.Provider value={{ hideBalances, theme, accounts, paydayDay }}>
      <div className="min-h-screen flex flex-col">
        <header className="border-b border-[var(--border)] bg-[var(--surface)] px-4 py-2.5 flex items-center justify-between gap-2 sticky top-0 z-40">
          <span className="font-bold text-base shrink-0">💰 Cash Flow</span>

          {/* Desktop nav */}
          <nav className="hidden md:flex gap-0.5 flex-1 justify-center">
            {NAV.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "px-3 py-1.5 rounded text-sm font-medium transition-colors",
                  pathname.startsWith(item.href)
                    ? "bg-[var(--primary)] text-white"
                    : "text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--bg)]"
                )}
              >
                {item.label}
              </Link>
            ))}
          </nav>

          {/* Right actions */}
          <div className="flex items-center gap-1 shrink-0">
            <button
              onClick={toggleHideBalances}
              title={hideBalances ? "Show balances" : "Hide balances"}
              className="p-1.5 rounded text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--bg)] text-sm"
            >
              {hideBalances ? "👁️" : "🙈"}
            </button>
            <button
              onClick={toggleTheme}
              title="Toggle theme"
              className="p-1.5 rounded text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--bg)] text-sm"
            >
              {theme === "dark" ? "☀️" : "🌙"}
            </button>
            <button
              onClick={() => setSettingsOpen(true)}
              title="Settings"
              className="p-1.5 rounded text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--bg)] text-sm"
            >
              ⚙️
            </button>
            <button
              onClick={handleLogout}
              className="hidden md:block px-3 py-1.5 rounded text-sm text-[var(--muted)] hover:text-[var(--danger)] hover:bg-[var(--bg)] transition-colors"
            >
              Logout
            </button>
            {/* Mobile menu toggle */}
            <button
              className="md:hidden p-1.5 rounded text-[var(--muted)] hover:bg-[var(--bg)]"
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            >
              ☰
            </button>
          </div>
        </header>

        {/* Mobile nav */}
        {mobileMenuOpen && (
          <div className="md:hidden border-b border-[var(--border)] bg-[var(--surface)] px-4 py-2 flex flex-wrap gap-1">
            {NAV.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                onClick={() => setMobileMenuOpen(false)}
                className={cn(
                  "px-3 py-1.5 rounded text-sm font-medium",
                  pathname.startsWith(item.href)
                    ? "bg-[var(--primary)] text-white"
                    : "text-[var(--muted)] hover:bg-[var(--bg)]"
                )}
              >
                {item.label}
              </Link>
            ))}
            <button onClick={handleLogout} className="px-3 py-1.5 rounded text-sm text-[var(--danger)]">Logout</button>
          </div>
        )}

        <main className="flex-1">{children}</main>
      </div>

      <SettingsModal
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        paydayDay={paydayDay}
        theme={theme}
        onThemeToggle={toggleTheme}
        hideBalances={hideBalances}
        onHideBalancesToggle={toggleHideBalances}
      />
    </AppContext.Provider>
  );
}
