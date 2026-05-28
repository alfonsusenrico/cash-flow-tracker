"use client";
import { createContext, useContext, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname } from "next/navigation";
import { Sidebar } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { BottomNav } from "@/components/layout/BottomNav";
import { api, ApiError } from "@/lib/api";
import type { Account, SummaryResponse } from "@/types/domain";

interface AppCtxType {
  hideBalances: boolean;
  setHideBalances: (v: boolean) => void;
  theme: "light" | "dark";
  setTheme: (v: "light" | "dark") => void;
  accounts: Account[];
  paydayDay: number;
  paydaySource: string | null;
  summaryRange: { from: string; to: string } | null;
  user: { username: string; full_name: string } | null;
}

const AppContext = createContext<AppCtxType>({
  hideBalances: false, setHideBalances: () => {},
  theme: "light", setTheme: () => {},
  accounts: [], paydayDay: 25, paydaySource: null,
  summaryRange: null, user: null,
});

export const useAppCtx = () => useContext(AppContext);

interface AppLayoutProps {
  children: React.ReactNode;
  title?: string;
  showDateRange?: boolean;
}

export default function AppLayout({ children, title = "Financial Manager", showDateRange = true }: AppLayoutProps) {
  const pathname = usePathname();
  const [hideBalances, setHideBalances] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    const saved = localStorage.getItem("theme") as "light" | "dark" | null;
    const t = saved ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    setTheme(t);
    document.documentElement.classList.toggle("dark", t === "dark");
    setHideBalances(localStorage.getItem("hideBalances") === "1");
  }, []);

  const { data: userData, error: userError, isLoading: userLoading } = useQuery<{ username: string; full_name: string }>({
    queryKey: ["me"],
    queryFn: () => api.get("/me"),
  });

  useEffect(() => {
    if (!(userError instanceof ApiError) || userError.status !== 401) return;
    const query = window.location.search.replace(/^\?/, "");
    const next = `${pathname}${query ? `?${query}` : ""}`;
    window.location.replace(`/auth/login?next=${encodeURIComponent(next)}`);
  }, [pathname, userError]);

  const isAuthenticated = !!userData;

  const { data: accountsData } = useQuery<{ accounts: Account[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
    enabled: isAuthenticated,
  });

  const { data: summaryData } = useQuery<SummaryResponse>({
    queryKey: ["summary"],
    queryFn: () => api.get("/summary"),
    enabled: isAuthenticated,
  });

  const accounts = accountsData?.accounts ?? [];
  const paydayDay = summaryData?.payday?.day ?? summaryData?.payday?.default_day ?? 25;
  const paydaySource = summaryData?.payday?.source ?? null;
  const summaryRange = summaryData?.range ?? null;

  if (userLoading || userError) {
    return <div className="min-h-screen bg-[var(--bg)]" />;
  }

  return (
    <AppContext.Provider value={{
      hideBalances, setHideBalances,
      theme, setTheme,
      accounts, paydayDay, paydaySource,
      summaryRange,
      user: userData ?? null,
    }}>
      <div className="app-shell">
        <Sidebar />
        <div className="app-main flex flex-col">
          <TopBar title={title} showDateRange={showDateRange} />
          <main className="app-content flex-1 overflow-auto pb-16 md:pb-0">
            {children}
          </main>
        </div>
        <BottomNav />
      </div>
    </AppContext.Provider>
  );
}
