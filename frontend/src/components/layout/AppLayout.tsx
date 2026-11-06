"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";
import { useQuery } from "@tanstack/react-query";
import { usePathname } from "next/navigation";
import { TestSessionBanner } from "@/components/settings/TestSessionBanner";
import { QuickCaptureModal } from "@/components/ui/QuickCaptureModal";
import { api, ApiError } from "@/lib/api";
import { setCurrencyConfig, fmtMoney as globalFmtMoney, convertAmount as globalConvertAmount } from "@/lib/utils";
import { queryKeys } from "@/lib/queryKeys";
import { freshReport, type ReportReadiness } from "@/lib/reportReady";
import {
  applyTheme,
  readThemeChoice,
  storeThemeChoice,
  systemPrefersDark,
  watchSystemTheme,
  type ResolvedTheme,
  type ThemeChoice,
} from "@/lib/theme";
import type { AutoLinkedOccurrence, DueExpectation, RecurringRule } from "@/types/recurring";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { planDay } from "@/components/recurring/planDay";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { WorkspaceContext, type PageHeader, type WorkspaceNotification, type WorkspaceValue } from "@/components/workspace/context";
import { pageMeta } from "@/components/workspace/navigation";
import { useNotificationSeen } from "@/components/workspace/Notifications";
import { SideNav } from "@/components/workspace/SideNav";
import { TitleBlock } from "@/components/workspace/TitleBlock";
import { TopBar } from "@/components/workspace/TopBar";
import { PhoneHeader, PhoneTabBar } from "@/components/workspace/PhoneNav";
import { ShortcutProvider, useShortcut, useShortcuts } from "@/components/workspace/shortcuts";
import { ExplainProvider } from "@/components/workspace/explain";
import { Drawer, ToastProvider } from "@/components/workspace/overlays";

type QuickCaptureMode = "expense" | "income" | "movement";

// Loaded on first open and prefetched when the browser is idle, so the shell's first load stays small (D11).
const loadCatatDialog = () => import("@/components/capture/CatatDialog");
const CatatDialog = dynamic(loadCatatDialog, { ssr: false });

/** Report window basis: payday cycle or calendar month (both stepped by `cycleOffset`), or rolling days. */
export type ReportTimeframe = "cycle" | "month" | "30d" | "90d";

interface AppCtxType {
  hideBalances: boolean;
  setHideBalances: (v: boolean) => void;
  theme: "light" | "dark";
  setTheme: (v: "light" | "dark") => void;
  timeframe: ReportTimeframe;
  setTimeframe: (v: ReportTimeframe) => void;
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
  openQuickAdd: (type?: "expense" | "income") => void;
  openMovement: (sourceAccountId?: string, targetAccountId?: string) => void;
  /** Rows needing the owner's attention on the review page; null until known. */
  reviewAttention: number | null;
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
  openMovement: () => {},
  reviewAttention: null,
});

export const useAppCtx = () => useContext(AppContext);

interface AppLayoutProps {
  children: React.ReactNode;
}

export default function AppLayout({ children }: AppLayoutProps) {
  return (
    <ShortcutProvider>
      <AppLayoutInner>{children}</AppLayoutInner>
    </ShortcutProvider>
  );
}

// Plans that are due today, overdue, or due within this many days appear in the notifications.
const PLAN_NOTICE_DAYS = 2;

function localIsoDate(date: Date): string {
  return date.toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
}

interface PlanOccurrence {
  ruleId: string;
  name: string;
  amount: number;
  due: string;
}

/**
 * The bell's plan items (D7): the due and overdue occurrences of `/recurring/pending` (the former Transaksi
 * banner's source) merged with the two-day window of active expense plans, once per plan and date, by date.
 * Due and overdue items open "Rencana jatuh tempo"; later ones keep their Target & Tagihan link.
 */
export function planNotifications(
  rules: RecurringRule[],
  pending: DueExpectation[],
  autoLinks: AutoLinkedOccurrence[],
  now = new Date(),
): WorkspaceNotification[] {
  const today = localIsoDate(now);
  const horizon = localIsoDate(new Date(now.getTime() + PLAN_NOTICE_DAYS * 86_400_000));
  const tomorrow = localIsoDate(new Date(now.getTime() + 86_400_000));
  const byKey = new Map<string, PlanOccurrence>();
  const add = (rule: RecurringRule) => {
    const key = `${rule.id}-${rule.next_due_date}`;
    if (!byKey.has(key)) byKey.set(key, { ruleId: rule.id, name: rule.name, amount: rule.amount, due: rule.next_due_date });
  };
  pending.forEach(add);
  rules.filter((rule) => rule.is_active && rule.type !== "income" && rule.next_due_date <= horizon).forEach(add);
  const items: WorkspaceNotification[] = [...byKey.values()]
    .sort((a, b) => a.due.localeCompare(b.due))
    .map((plan) => {
      const when = plan.due < today ? "lewat jatuh tempo" : plan.due === today ? "hari ini" : plan.due === tomorrow ? "besok" : "lusa";
      const opensDuePlans = plan.due <= today;
      return {
        id: `plan-${plan.ruleId}-${plan.due}`,
        title: `${plan.name} ${globalFmtMoney(plan.amount)} ${when}`,
        detail: `Rencana · jatuh tempo ${planDay(plan.due)}`,
        ...(opensDuePlans ? { opensDuePlans } : { href: "/goals" }),
      };
    });
  if (autoLinks.length) {
    const occurrences = autoLinks.map((link) => `${link.rule_id}-${link.scheduled_for}`).sort();
    const newest = [...autoLinks].map((link) => link.scheduled_for).sort().at(-1)!;
    items.push({
      id: `autolink-${occurrences.join(",")}`,
      title: `${autoLinks.length} rencana ditautkan otomatis`,
      detail: `Rencana · jatuh tempo ${planDay(newest)}`,
      opensDuePlans: true,
    });
  }
  return items;
}

function AppLayoutInner({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { openHelp } = useShortcuts();
  const [hideBalances, setHideBalances] = useState(false);
  const [themeChoice, setThemeChoiceState] = useState<ThemeChoice>("system");
  const [theme, setThemeState] = useState<ResolvedTheme>("light");
  const [timeframe, setTimeframe] = useState<ReportTimeframe>("cycle");
  const [cycleOffset, setCycleOffset] = useState(0);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [quickAddOpen, setQuickAddOpen] = useState(false);
  const [quickAddType, setQuickAddType] = useState<"expense" | "income">("expense");
  const [quickAddDefaultType, setQuickAddDefaultType] = useState<QuickCaptureMode>("expense");
  const [movementDefaults, setMovementDefaults] = useState({ sourceAccountId: "", targetAccountId: "" });
  const [catatOpen, setCatatOpen] = useState(false);
  const [manualAmount, setManualAmount] = useState<number | null>(null);
  const [header, setHeader] = useState<PageHeader | null>(null);
  const [actionsSlot, setActionsSlot] = useState<HTMLElement | null>(null);
  const [duePlansOpen, setDuePlansOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);

  const handleOpenQuickAdd = (type?: "expense" | "income") => {
    const defaultType = type ?? quickAddType;
    if (type) setQuickAddType(type);
    setMovementDefaults({ sourceAccountId: "", targetAccountId: "" });
    setQuickAddDefaultType(defaultType);
    setManualAmount(null);
    setQuickAddOpen(true);
  };

  const handleOpenMovement = (sourceAccountId?: string, targetAccountId?: string) => {
    setMovementDefaults({
      sourceAccountId: sourceAccountId ?? "",
      targetAccountId: targetAccountId ?? "",
    });
    setQuickAddDefaultType("movement");
    setQuickAddOpen(true);
  };

  // "+ Catat", N and the phone tab open the text-first dialog; "Isi manual" there opens the form.
  const openExpenseCapture = useCallback(() => setCatatOpen(true), []);

  const openManualCapture = useCallback((amount: number | null) => {
    setCatatOpen(false);
    setMovementDefaults({ sourceAccountId: "", targetAccountId: "" });
    setQuickAddDefaultType("expense");
    setManualAmount(amount);
    setQuickAddOpen(true);
  }, []);

  useEffect(() => {
    const idle = (window as Window & { requestIdleCallback?: (callback: () => void) => number }).requestIdleCallback;
    if (idle) idle(() => void loadCatatDialog());
    else window.setTimeout(() => void loadCatatDialog(), 2000);
  }, []);

  // The pre-paint script already applied the theme; this only syncs React state with it.
  useEffect(() => {
    const choice = readThemeChoice();
    setThemeChoiceState(choice);
    setThemeState(applyTheme(choice));
    try {
      setHideBalances(localStorage.getItem("hideBalances") === "1");
      setSidebarCollapsed(localStorage.getItem("sidebarCollapsed") === "1");
    } catch {
      // Storage blocked: defaults apply for this visit.
    }
  }, []);

  useEffect(() => {
    if (themeChoice !== "system") return;
    return watchSystemTheme((prefersDark) => setThemeState(applyTheme("system", prefersDark)));
  }, [themeChoice]);

  const setThemeChoice = useCallback((choice: ThemeChoice) => {
    storeThemeChoice(choice);
    setThemeChoiceState(choice);
    setThemeState(applyTheme(choice, systemPrefersDark()));
  }, []);

  // Legacy pages may still set "light" or "dark" directly through the context.
  const setTheme = useCallback((value: "light" | "dark") => setThemeChoice(value), [setThemeChoice]);

  const persistFlag = (key: string, value: boolean) => {
    try {
      localStorage.setItem(key, value ? "1" : "0");
    } catch {
      // Not remembered, still applied.
    }
  };

  const updateHideBalances = useCallback((value: boolean) => {
    setHideBalances(value);
    persistFlag("hideBalances", value);
  }, []);

  const toggleCollapsed = useCallback(() => {
    setSidebarCollapsed((current) => {
      persistFlag("sidebarCollapsed", !current);
      return !current;
    });
  }, []);

  useShortcut("n", openExpenseCapture);

  const { data: userData, error: userError, isLoading: userLoading } = useQuery<{ ok: boolean; user: any }>({
    queryKey: queryKeys.auth.me,
    queryFn: () => api.get("/auth/me"),
  });

  const { data: ratesData } = useQuery<{ ok: boolean; usdidr: number }>({
    queryKey: queryKeys.auth.currencyRates,
    queryFn: () => api.get("/auth/currency/rates"),
    staleTime: 60 * 60 * 1000,
  });

  const signedIn = Boolean(userData?.user);
  const { data: reviewSummary } = useQuery<{ ok: boolean; attention: number }>({
    queryKey: queryKeys.review.summary,
    queryFn: () => api.get("/review/summary"),
    enabled: signedIn,
  });

  const { data: reportReadiness } = useQuery<ReportReadiness>({
    queryKey: queryKeys.reports.latest,
    queryFn: () => api.get("/reports/monthly/latest"),
    staleTime: 60 * 60 * 1000,
    enabled: signedIn,
  });

  const { data: recurringData } = useQuery<{ rules: RecurringRule[] }>({
    queryKey: queryKeys.recurring.all,
    queryFn: () => api.get("/recurring"),
    enabled: signedIn,
  });

  const { data: pendingData } = useQuery<{ ok: boolean; rules: DueExpectation[]; recent_auto_links?: AutoLinkedOccurrence[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    refetchInterval: 30000,
    enabled: signedIn,
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

  const logout = useCallback(async () => {
    try {
      await api.post("/auth/logout");
    } finally {
      window.location.replace("/auth/login");
    }
  }, []);

  const report = freshReport(reportReadiness);
  const notifications = useMemo<WorkspaceNotification[]>(() => {
    const items: WorkspaceNotification[] = [];
    if (report) {
      const ended = new Date(`${report.endDate}T00:00:00+07:00`).toLocaleDateString("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });
      items.push({ id: `report-${report.endDate}`, title: `Laporan ${report.label} siap dibaca`, detail: `Periodenya selesai ${ended}`, href: report.url });
    }
    return [...items, ...planNotifications(recurringData?.rules ?? [], pendingData?.rules ?? [], pendingData?.recent_auto_links ?? [])];
  }, [report?.endDate, report?.label, report?.url, recurringData, pendingData]); // eslint-disable-line react-hooks/exhaustive-deps
  const { unreadIds, markSeen } = useNotificationSeen(notifications);
  const openDuePlans = useCallback(() => setDuePlansOpen(true), []);

  const workspace: WorkspaceValue = {
    username: user?.username ?? null,
    displayName: user?.name ?? null,
    reviewAttention: reviewSummary?.attention ?? null,
    reportBadge: Boolean(report),
    notifications,
    unreadIds,
    markNotificationsSeen: markSeen,
    openDuePlans,
    themeChoice,
    resolvedTheme: theme,
    setThemeChoice,
    hideBalances,
    toggleHideBalances: () => updateHideBalances(!hideBalances),
    collapsed: sidebarCollapsed,
    toggleCollapsed,
    openCapture: openExpenseCapture,
    openHelp,
    logout,
    header,
    setHeader,
    actionsSlot,
    setActionsSlot,
  };

  if (userLoading || userError) {
    return <div className="min-h-screen bg-[var(--canvas)]" />;
  }

  return (
    <AppContext.Provider
      value={{
        hideBalances,
        setHideBalances: updateHideBalances,
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
        openMovement: handleOpenMovement,
        reviewAttention: reviewSummary?.attention ?? null,
      }}
    >
      <WorkspaceContext.Provider value={workspace}>
        <ToastProvider>
          <ExplainProvider>
            <a className="skip" href="#main">
              Lewati ke isi
            </a>
            {/* The frame (D4): sidebar | main column with the top bar, the phone header and the page. */}
            <div className="app" data-collapsed={sidebarCollapsed}>
              <SideNav />
              <div className="main-col">
                <TopBar />
                <PhoneHeader />
                <TestSessionBanner enabled={signedIn} />
                <main id="main" tabIndex={-1} className="page" style={{ "--page-w": `${pageMeta(pathname).width}px` } as React.CSSProperties}>
                  <div className="page-inner">
                    <TitleBlock />
                    {children}
                  </div>
                </main>
              </div>
            </div>
            <PhoneTabBar />
            {/* Due plans from the bell (D7): link, skip and record, as the Transaksi banner opened them. */}
            <Drawer open={duePlansOpen} onClose={() => setDuePlansOpen(false)} title="Rencana jatuh tempo" subtitle="Tautkan, lewati, atau catat manual">
              <PendingScheduledBanner
                onOpenRulesManager={() => {
                  setDuePlansOpen(false);
                  setRulesOpen(true);
                }}
              />
            </Drawer>
            <RecurringRulesModal open={rulesOpen} onClose={() => setRulesOpen(false)} />
            <QuickCaptureModal
              open={quickAddOpen}
              onClose={() => setQuickAddOpen(false)}
              defaultType={quickAddDefaultType}
              defaultSourceAccountId={movementDefaults.sourceAccountId}
              defaultTargetAccountId={movementDefaults.targetAccountId}
              defaultAmount={manualAmount}
            />
            {catatOpen && <CatatDialog open onClose={() => setCatatOpen(false)} onManual={openManualCapture} />}
          </ExplainProvider>
        </ToastProvider>
      </WorkspaceContext.Provider>
    </AppContext.Provider>
  );
}
