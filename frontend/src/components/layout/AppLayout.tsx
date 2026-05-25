"use client";
import { createContext, useContext, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Sidebar } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { api } from "@/lib/api";
import type { Account, SummaryResponse } from "@/types/domain";

interface AppCtxType {
  hideBalances: boolean;
  setHideBalances: (v: boolean) => void;
  theme: "light" | "dark";
  setTheme: (v: "light" | "dark") => void;
  accounts: Account[];
  paydayDay: number;
  summaryRange: { from: string; to: string } | null;
  user: { username: string; full_name: string } | null;
}

const AppContext = createContext<AppCtxType>({
  hideBalances: false, setHideBalances: () => {},
  theme: "light", setTheme: () => {},
  accounts: [], paydayDay: 25,
  summaryRange: null, user: null,
});

export const useAppCtx = () => useContext(AppContext);

interface AppLayoutProps {
  children: React.ReactNode;
  title?: string;
  showDateRange?: boolean;
}

export default function AppLayout({ children, title = "Cash Flow", showDateRange = true }: AppLayoutProps) {
  const [hideBalances, setHideBalances] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    const saved = localStorage.getItem("theme") as "light" | "dark" | null;
    const t = saved ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    setTheme(t);
    document.documentElement.classList.toggle("dark", t === "dark");
    setHideBalances(localStorage.getItem("hideBalances") === "1");
  }, []);

  const { data: accountsData } = useQuery<{ accounts: Account[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const { data: summaryData } = useQuery<SummaryResponse>({
    queryKey: ["summary"],
    queryFn: () => api.get("/summary"),
  });

  const { data: userData } = useQuery<{ username: string; full_name: string }>({
    queryKey: ["me"],
    queryFn: () => api.get("/me"),
  });

  const accounts = accountsData?.accounts ?? [];
  const paydayDay = summaryData?.payday?.default_day ?? 25;
  const summaryRange = summaryData?.range ?? null;

  return (
    <AppContext.Provider value={{
      hideBalances, setHideBalances,
      theme, setTheme,
      accounts, paydayDay,
      summaryRange,
      user: userData ?? null,
    }}>
      <div className="flex min-h-screen">
        <Sidebar />
        <div className="flex-1 flex flex-col" style={{ marginLeft: "200px" }}>
          <TopBar title={title} showDateRange={showDateRange} />
          <main className="flex-1 pt-14 overflow-auto">
            {children}
          </main>
        </div>
      </div>
    </AppContext.Provider>
  );
}
