"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";
import { useQuery } from "@tanstack/react-query";
import { usePathname } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { parseRegion, DEFAULT_REGION } from "@/i18n/language";
import { TestSessionBanner } from "@/components/settings/TestSessionBanner";
import { QuickCaptureModal } from "@/components/ui/QuickCaptureModal";
import { api, ApiError } from "@/lib/api";
import { setCurrencyConfig, fmtMoney as globalFmtMoney, convertAmount as globalConvertAmount } from "@/lib/utils";
import { createFormat, RegionProvider } from "@/lib/format";
import { configureApiRegion } from "@/lib/i18n/runtime";
import { isTranslatedRoute } from "@/lib/i18n/translatedRoutes";
import type { Words } from "@/lib/i18n/types";
import { useAccountLanguage } from "@/lib/i18n/useAccountLanguage";
import { queryKeys } from "@/lib/queryKeys";
import { creditBillsPath, dueCreditBills, type CreditBill, type CreditBillsResponse } from "@/lib/creditBills";
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
import { PlansDialog } from "@/components/recurring/PlansDialog";
import { WorkspaceContext, type MovementCaptureDefaults, type PageHeader, type WorkspaceNotification, type WorkspaceValue } from "@/components/workspace/context";
import { frameWidth } from "@/components/workspace/navigation";
import { useNotificationSeen } from "@/components/workspace/Notifications";
import { SideNav } from "@/components/workspace/SideNav";
import { TitleBlock } from "@/components/workspace/TitleBlock";
import { TopBar } from "@/components/workspace/TopBar";
import { PhoneHeader, PhoneTabBar } from "@/components/workspace/PhoneNav";
import { ShortcutProvider, useShortcut, useShortcuts } from "@/components/workspace/shortcuts";
import { ExplainProvider } from "@/components/workspace/explain";
import { Drawer, ToastProvider } from "@/components/workspace/overlays";
import { UntranslatedRegion } from "@/components/workspace/untranslated";

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
  key: string;
  name: string;
  amount: number;
  due: string;
  /** A credit bill (apply-markas-target D16) rather than a plan. */
  bill?: CreditBill;
}

/**
 * The bell's plan items (D7): the due and overdue occurrences of `/recurring/pending` (the former Transaksi
 * banner's source) merged with the two-day window of active expense plans, once per plan and date, by date.
 * Due and overdue items open "Rencana jatuh tempo"; later ones keep their Target & Tagihan link. Unpaid credit
 * bills join them (apply-markas-target D16), once per account and due date and not when a listed plan pays the
 * same bill; choosing one opens Perpindahan prefilled through `onPayBill`.
 */
export function planNotifications(
  rules: RecurringRule[],
  pending: DueExpectation[],
  autoLinks: AutoLinkedOccurrence[],
  { t, format }: Words,
  now = new Date(),
  creditBills: CreditBill[] = [],
  onPayBill?: (bill: CreditBill) => void,
): WorkspaceNotification[] {
  const today = localIsoDate(now);
  const horizon = localIsoDate(new Date(now.getTime() + PLAN_NOTICE_DAYS * 86_400_000));
  const tomorrow = localIsoDate(new Date(now.getTime() + 86_400_000));
  const byKey = new Map<string, PlanOccurrence>();
  const listed: RecurringRule[] = [];
  const add = (rule: RecurringRule) => {
    const key = `plan-${rule.id}-${rule.next_due_date}`;
    if (byKey.has(key)) return;
    byKey.set(key, { key, name: rule.name, amount: rule.amount, due: rule.next_due_date });
    listed.push(rule);
  };
  pending.forEach(add);
  rules.filter((rule) => rule.is_active && rule.type !== "income" && rule.next_due_date <= horizon).forEach(add);
  for (const bill of dueCreditBills(creditBills.filter((item) => item.due_date <= horizon), listed)) {
    const key = `credit-${bill.account_id}-${bill.due_date}`;
    if (!byKey.has(key)) byKey.set(key, { key, name: t("home.creditBill", { account: bill.account_name }), amount: bill.left_to_pay, due: bill.due_date, bill });
  }
  const items: WorkspaceNotification[] = [...byKey.values()]
    .sort((a, b) => a.due.localeCompare(b.due) || (a.bill && b.bill ? a.name.localeCompare(b.name) : 0))
    .map((plan) => {
      const values = { name: plan.name, amount: format.money(plan.amount) };
      const title =
        plan.due < today
          ? t("shell.planOverdue", values)
          : plan.due === today
            ? t("shell.planDueToday", values)
            : plan.due === tomorrow
              ? t("shell.planDueTomorrow", values)
              : t("shell.planDueInTwoDays", values);
      const bill = plan.bill;
      if (bill) {
        return {
          id: plan.key,
          title,
          detail: t("shell.creditDetail", { day: format.shortDate(plan.due) }),
          ...(onPayBill ? { onSelect: () => onPayBill(bill) } : {}),
        };
      }
      const opensDuePlans = plan.due <= today;
      return {
        id: plan.key,
        title,
        detail: t("shell.planDetail", { day: format.shortDate(plan.due) }),
        ...(opensDuePlans ? { opensDuePlans } : { href: "/goals" }),
      };
    });
  if (autoLinks.length) {
    const occurrences = autoLinks.map((link) => `${link.rule_id}-${link.scheduled_for}`).sort();
    const newest = [...autoLinks].map((link) => link.scheduled_for).sort().at(-1)!;
    items.push({
      id: `autolink-${occurrences.join(",")}`,
      title: t("shell.plansAutoLinked", { count: autoLinks.length }),
      detail: t("shell.planDetail", { day: format.shortDate(newest) }),
      opensDuePlans: true,
    });
  }
  return items;
}

/** The bell's name for a ready report, from its basis and dates (add-language-system D7). */
export function reportNotification(report: NonNullable<ReturnType<typeof freshReport>>, { t, format }: Words): WorkspaceNotification {
  // An API older than the start date keeps naming the report with its own label.
  const period = !report.startDate
    ? report.label
    : report.basis === "cycle"
      ? t("shell.reportPeriodCycle", { start: format.shortDate(report.startDate), end: format.shortDate(report.endDate) })
      : format.monthYear(report.startDate);
  return {
    id: `report-${report.endDate}`,
    title: t("shell.reportReady", { period }),
    detail: t("shell.reportPeriodEnded", { ended: format.shortDate(report.endDate) }),
    href: report.url,
  };
}

function AppLayoutInner({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const t = useTranslations();
  const locale = useLocale();
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
    openMovementCapture({ sourceAccountId, targetAccountId });
  };

  function openMovementCapture({ sourceAccountId, targetAccountId, amount }: MovementCaptureDefaults) {
    setMovementDefaults({
      sourceAccountId: sourceAccountId ?? "",
      targetAccountId: targetAccountId ?? "",
    });
    setQuickAddDefaultType("movement");
    setManualAmount(amount && amount > 0 ? amount : null);
    setQuickAddOpen(true);
  }

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

  // Unpaid credit bills for the bell (apply-markas-target D16), the same request as Beranda's "Perlu tindakan".
  const [billsHorizon] = useState(() => localIsoDate(new Date(Date.now() + PLAN_NOTICE_DAYS * 86_400_000)));
  const billsPath = creditBillsPath(billsHorizon);
  const { data: creditBillsData } = useQuery<CreditBillsResponse>({
    queryKey: queryKeys.transactions.creditBills(billsPath),
    queryFn: () => api.get(billsPath),
    enabled: signedIn,
  });

  const user = userData?.user;
  const currency: "IDR" | "USD" = user?.currency === "USD" ? "USD" : "IDR";
  const usdidrRate = ratesData?.usdidr && ratesData.usdidr > 0 ? ratesData.usdidr : 16500;
  const region = parseRegion(user?.region) ?? DEFAULT_REGION;
  const regionSettings = useMemo(() => ({ region, currency, rate: usdidrRate }), [region, currency, usdidrRate]);
  const format = useMemo(() => createFormat({ language: locale, region, currency, rate: usdidrRate }), [locale, region, currency, usdidrRate]);
  useAccountLanguage(user?.language);

  useEffect(() => {
    setCurrencyConfig(currency, usdidrRate);
  }, [currency, usdidrRate]);

  useEffect(() => {
    configureApiRegion(region);
  }, [region]);

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
    const words = { t, format };
    const items: WorkspaceNotification[] = report ? [reportNotification(report, words)] : [];
    const payBill = (bill: CreditBill) => openMovementCapture({ targetAccountId: bill.account_id, amount: bill.left_to_pay });
    return [
      ...items,
      ...planNotifications(recurringData?.rules ?? [], pendingData?.rules ?? [], pendingData?.recent_auto_links ?? [], words, new Date(), creditBillsData?.bills ?? [], payBill),
    ];
  }, [report?.endDate, report?.startDate, report?.basis, report?.url, recurringData, pendingData, creditBillsData, t, format]); // eslint-disable-line react-hooks/exhaustive-deps
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
    openMovementCapture,
    openHelp,
    logout,
    header,
    setHeader,
    actionsSlot,
    setActionsSlot,
  };

  // Pages no phase has translated yet stay Indonesian, marked as such, inside the translated shell (D10).
  const untranslated = locale !== "id" && !isTranslatedRoute(pathname);

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
      <RegionProvider value={regionSettings}>
      <WorkspaceContext.Provider value={workspace}>
        <ToastProvider>
          <ExplainProvider>
            <a className="skip" href="#main">
              {t("shell.skipToContent")}
            </a>
            {/* The frame (D4): sidebar | main column with the top bar, the phone header and the page. */}
            <div className="app" data-collapsed={sidebarCollapsed}>
              <SideNav />
              <div className="main-col">
                <TopBar />
                <PhoneHeader />
                <TestSessionBanner enabled={signedIn} />
                <main
                  id="main"
                  tabIndex={-1}
                  className="page"
                  lang={untranslated ? "id" : undefined}
                  style={{ "--page-w": `${frameWidth(pathname)}px` } as React.CSSProperties}
                >
                  <UntranslatedRegion active={untranslated}>
                    <div className="page-inner">
                      <TitleBlock />
                      {children}
                    </div>
                  </UntranslatedRegion>
                </main>
              </div>
            </div>
            <PhoneTabBar />
            {/* Due plans from the bell (D7): link, skip and record, as the Transaksi banner opened them. */}
            <Drawer open={duePlansOpen} onClose={() => setDuePlansOpen(false)} title={t("shell.duePlans.title")} subtitle={t("shell.duePlans.subtitle")}>
              <PendingScheduledBanner
                onOpenRulesManager={() => {
                  setDuePlansOpen(false);
                  setRulesOpen(true);
                }}
              />
            </Drawer>
            <PlansDialog open={rulesOpen} onClose={() => setRulesOpen(false)} />
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
      </RegionProvider>
    </AppContext.Provider>
  );
}
