"use client";

import { createContext, useContext, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname } from "next/navigation";
import { Sidebar, MobileDrawer } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { BottomNav } from "@/components/layout/BottomNav";
import { QuickCaptureModal } from "@/components/ui/QuickCaptureModal";
import { SettingsModal } from "@/components/ui/SettingsModal";
import { api, ApiError } from "@/lib/api";
import { cn, setCurrencyConfig, fmtMoney as globalFmtMoney, convertAmount as globalConvertAmount } from "@/lib/utils";

interface AppCtxType {
  hideBalances: boolean;
  setHideBalances: (v: boolean) => void;
  theme: "light" | "dark";
  setTheme: (v: "light" | "dark") => void;
  timeframe: "cycle" | "30d" | "90d";
  setTimeframe: (v: "cycle" | "30d" | "90d") => void;
  cycleOffset: number;
  setCycleOffset: (v: number) => void;
  user: any;
  currency: "IDR" | "USD";
  usdidrRate: number;
  fmtMoney: (
    n: number,
    currency?: "IDR" | "USD",
    rate?: number,
    options?: { isAlreadyConverted?: boolean; maxFractionDigits?: number }
  ) => string;
  bal: (n: number, overrideCurrency?: "IDR" | "USD") => string;
  convertAmount: (amountInIDR: number) => number;
  openQuickAdd: (type?: "expense" | "income" | "transfer") => void;
}

const AppContext = createContext<AppCtxType>({
  hideBalances: false,
  setHideBalances: () => {},
  theme: "light",
  setTheme: () => {},
  timeframe: "cycle",
  setTimeframe: () => {},
  cycleOffset: 0,
  setCycleOffset: () => {},
  user: null,
  currency: "IDR",
  usdidrRate: 16500,
  fmtMoney: (n: number) => globalFmtMoney(n),
  bal: (n: number) => globalFmtMoney(n),
  convertAmount: (amountInIDR: number) => amountInIDR,
  openQuickAdd: () => {},
});

export const useAppCtx = () => useContext(AppContext);

interface AppLayoutProps {
  children: React.ReactNode;
  title?: string;
}

export default function AppLayout({ children }: AppLayoutProps) {
  const pathname = usePathname();
  const [hideBalances, setHideBalances] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark">("light");
  const [timeframe, setTimeframe] = useState<"cycle" | "30d" | "90d">("cycle");
  const [cycleOffset, setCycleOffset] = useState(0);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [quickAddOpen, setQuickAddOpen] = useState(false);
  const [quickAddType, setQuickAddType] = useState<"expense" | "income" | "transfer">("expense");
  const [settingsOpen, setSettingsOpen] = useState(false);

  const handleOpenQuickAdd = (type?: "expense" | "income" | "transfer") => {
    if (type) setQuickAddType(type);
    setQuickAddOpen(true);
  };

  useEffect(() => {
    const savedTheme = localStorage.getItem("theme") as "light" | "dark" | null;
    const t = savedTheme ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    setTheme(t);
    document.documentElement.classList.toggle("dark", t === "dark");

    // Clean up any stale data-preset from previewing
    document.documentElement.removeAttribute("data-preset");
    localStorage.removeItem("theme_preset");

    setHideBalances(localStorage.getItem("hideBalances") === "1");
    setSidebarCollapsed(localStorage.getItem("sidebarCollapsed") === "1");
  }, []);

  const handleToggleSidebar = () => {
    const next = !sidebarCollapsed;
    setSidebarCollapsed(next);
    localStorage.setItem("sidebarCollapsed", next ? "1" : "0");
  };

  // Global hotkey 'N' to trigger quick entry
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key.toLowerCase() === "n" && !e.ctrlKey && !e.metaKey && !e.altKey) {
        const target = e.target as HTMLElement;
        if (
          target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.tagName === "SELECT" ||
          target.isContentEditable
        ) {
          return;
        }
        e.preventDefault();
        setQuickAddOpen(true);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  const { data: userData, error: userError, isLoading: userLoading } = useQuery<{ ok: boolean; user: any }>({
    queryKey: ["auth-me"],
    queryFn: () => api.get("/auth/me"),
  });

  const { data: ratesData } = useQuery<{ ok: boolean; usdidr: number }>({
    queryKey: ["currency-rates"],
    queryFn: () => api.get("/auth/currency/rates"),
    staleTime: 60 * 60 * 1000,
  });

  const user = userData?.user;
  const currency: "IDR" | "USD" = user?.currency === "USD" ? "USD" : "IDR";
  const usdidrRate = ratesData?.usdidr && ratesData.usdidr > 0 ? ratesData.usdidr : 16500;

  useEffect(() => {
    setCurrencyConfig(currency, usdidrRate);
  }, [currency, usdidrRate]);

  const fmtMoney = (
    n: number,
    cur?: "IDR" | "USD",
    rate?: number,
    options?: { isAlreadyConverted?: boolean; maxFractionDigits?: number }
  ) => globalFmtMoney(n, cur || currency, rate || usdidrRate, options);

  const bal = (n: number, overrideCurrency?: "IDR" | "USD") => {
    const cur = overrideCurrency || currency;
    if (hideBalances) {
      return cur === "USD" ? "$ ••••••" : "Rp ••••••";
    }
    return fmtMoney(n, cur);
  };

  const convertAmount = (amountInIDR: number) =>
    globalConvertAmount(amountInIDR, currency, usdidrRate);

  useEffect(() => {
    if (!(userError instanceof ApiError) || userError.status !== 401) return;
    const query = window.location.search.replace(/^\?/, "");
    const next = `${pathname}${query ? `?${query}` : ""}`;
    window.location.replace(`/auth/login?next=${encodeURIComponent(next)}`);
  }, [pathname, userError]);

  if (userLoading || userError) {
    return <div className="min-h-screen bg-[var(--bg)]" />;
  }

  return (
    <AppContext.Provider
      value={{
        hideBalances,
        setHideBalances,
        theme,
        setTheme,
        timeframe,
        setTimeframe,
        cycleOffset,
        setCycleOffset,
        user,
        currency,
        usdidrRate,
        fmtMoney,
        bal,
        convertAmount,
        openQuickAdd: handleOpenQuickAdd,
      }}
    >
      <div className="flex min-h-screen bg-[var(--bg)] text-[var(--text)]">
        {/* Desktop Persistent Left Sidebar */}
        <Sidebar
          collapsed={sidebarCollapsed}
          onToggleCollapse={handleToggleSidebar}
          onQuickAdd={() => handleOpenQuickAdd()}
          onOpenSettings={() => setSettingsOpen(true)}
        />

        {/* Mobile Slide-Over Navigation Drawer */}
        <MobileDrawer
          open={mobileMenuOpen}
          onClose={() => setMobileMenuOpen(false)}
          onQuickAdd={() => {
            setMobileMenuOpen(false);
            handleOpenQuickAdd();
          }}
          onOpenSettings={() => {
            setMobileMenuOpen(false);
            setSettingsOpen(true);
          }}
        />

        {/* Main Fluid Workbench */}
        <div
          className={cn(
            "flex-1 flex flex-col min-w-0 transition-[margin] duration-200 ease-in-out",
            sidebarCollapsed ? "lg:ml-[72px]" : "lg:ml-[260px]"
          )}
        >
          <TopBar
            onToggleMobileMenu={() => setMobileMenuOpen(true)}
            onQuickAdd={() => handleOpenQuickAdd()}
            onOpenSettings={() => setSettingsOpen(true)}
          />

          {/* Fluid Full-Width Workspace Canvas (matching student-enrollment-analysis) */}
          <main className="flex-1 w-full px-4 sm:px-6 lg:px-8 py-6 pb-24 lg:pb-12 min-w-0">
            {children}
          </main>
        </div>

        {/* Mobile Bottom Bar */}
        <BottomNav onQuickAdd={() => handleOpenQuickAdd()} />

        {/* Global Modals */}
        <QuickCaptureModal
          open={quickAddOpen}
          onClose={() => setQuickAddOpen(false)}
          defaultType={quickAddType}
        />

        <SettingsModal
          open={settingsOpen}
          onClose={() => setSettingsOpen(false)}
        />
      </div>
    </AppContext.Provider>
  );
}
