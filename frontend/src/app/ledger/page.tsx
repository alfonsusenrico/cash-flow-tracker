"use client";

import { Suspense, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useInfiniteQuery, useMutation, useQuery } from "@tanstack/react-query";
import { useRouter, useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { LedgerFilterDialog, type FilterDraft } from "@/components/transactions/LedgerFilterDialog";
import { LedgerList, type ListSelection } from "@/components/transactions/LedgerList";
import { bucketName, jakartaDay, useRowLabels } from "@/components/transactions/rows";
import { TransactionDialog } from "@/components/transactions/TransactionDialog";
import type { LedgerRow } from "@/components/transactions/types";
import { useRefreshTransactionViews, useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { Icon } from "@/components/ui/Icon";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { PageActions, WorkspaceContext } from "@/components/workspace/context";
import { PHONE_QUERY, useMediaQuery } from "@/components/workspace/layers";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { useToast } from "@/components/workspace/overlays";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { useShortcut, usePageShortcutGroup, type PageShortcutGroup } from "@/components/workspace/shortcuts";
import { LEDGER_SEARCH_ID } from "@/components/workspace/TopBar";
import { listCreditAccountChoices } from "@/lib/accountOptions";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { groupByDay, todayLocal, uniqueRows, type DailyTotal } from "@/lib/ledgerDays";
import {
  DEFAULT_LEDGER_FILTERS,
  UNCATEGORIZED,
  activeFilterCount,
  hasDateRange,
  ledgerApiParams,
  ledgerFilterQuery,
  listsLogicalMovements,
  parseLedgerFilters,
  type LedgerFilters,
  type LedgerTypeFilter,
} from "@/lib/ledgerFilters";
import { consolidateLedgerMovements } from "@/lib/ledgerMovements";
import { queryKeys } from "@/lib/queryKeys";
import type { UndoFields } from "@/lib/reviewUndo";
import { FOLDER_PARAMS } from "@/lib/triage";
import { pushSearch, replaceSearch } from "@/lib/urlState";
import { CreditHeader } from "./CreditHeader";
import { LedgerHero, type CashFlowMetrics } from "./LedgerHero";
import { LedgerRail, LedgerTypeControl, ProviderControl, type TypeCounts } from "./LedgerRail";
import { LedgerToolbar, type ActiveChip } from "./LedgerToolbar";
import { MergeBar } from "./MergeBar";

const PAGE_SIZE = 50;
const SEARCH_DELAY_MS = 300;
/** Focus goes back to a row once the list has settled after its dialog closed. */
const FOCUS_SETTLE_MS = 150;
const SKELETON_ROWS = [52, 30, 44, 38, 48, 34];
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

interface PeriodMetrics extends CashFlowMetrics {
  basis: "cycle" | "month" | null;
  label: string | null;
  start: string | null;
  end: string | null;
}

interface LedgerPage {
  ok: boolean;
  total: number;
  transactions: LedgerRow[];
  summary?: { cycle: CashFlowMetrics; cumulative: CashFlowMetrics; period?: PeriodMetrics | null };
  daily_totals?: DailyTotal[];
  type_counts?: TypeCounts;
}

export default function LedgerPage() {
  // The page reads its filters from the URL, which needs a suspense boundary for static rendering.
  return (
    <Suspense fallback={null}>
      <LedgerPageContent />
    </Suspense>
  );
}

/**
 * Transaksi (apply-markas-transaksi): the hero, then the type and period rail beside the list from 1200 px. Every
 * filter lives in the URL; every figure, count and day total is the server's.
 */
function LedgerPageContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchKey = searchParams?.toString() ?? "";
  const t = useTranslations();
  const { setTimeframe, setCycleOffset } = useAppCtx();
  const openCapture = useContext(WorkspaceContext)?.openCapture;
  const money = useMoney();
  const format = useFormat();
  const labels = useRowLabels();
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();
  const { record, undoNewest } = useTransactionUndo();
  const phone = useMediaQuery(PHONE_QUERY);
  const listRegionRef = useRef<HTMLDivElement>(null);
  const listHeadingRef = useRef<HTMLHeadingElement>(null);
  const filterButtonRef = useRef<HTMLButtonElement>(null);

  const params = useMemo(() => new URLSearchParams(searchKey), [searchKey]);
  const filters = useMemo(() => parseLedgerFilters(params), [params]);
  const rawEdit = params.get("edit");
  const editId = rawEdit && UUID_PATTERN.test(rawEdit) ? rawEdit : null;
  const returnTo = params.get("return") === "review" ? "review" : null;
  const returnFolder = params.get("folder");

  const [search, setSearch] = useState(filters.q);
  const [appliedSearch, setAppliedSearch] = useState(filters.q);
  useEffect(() => {
    setSearch(filters.q);
    setAppliedSearch(filters.q);
  }, [filters.q]);
  useEffect(() => {
    const timer = window.setTimeout(() => setAppliedSearch(search.trim()), SEARCH_DELAY_MS);
    return () => window.clearTimeout(timer);
  }, [search]);

  // The phone header's search and ⌘K on a phone open Transaksi with `fokus=cari`: focus the field once and
  // drop the parameter, so a reload or Back does not take focus again (apply-markas-shell D5).
  const focusRequested = params.get("fokus") === "cari";
  useEffect(() => {
    if (!focusRequested) return;
    document.getElementById(LEDGER_SEARCH_ID)?.focus();
    const next = new URLSearchParams(searchKey);
    next.delete("fokus");
    const query = next.toString();
    replaceSearch(query ? `/ledger?${query}` : "/ledger");
  }, [focusRequested, searchKey]);

  const [filterOpen, setFilterOpen] = useState(false);
  const [selecting, setSelecting] = useState(false);
  const [selected, setSelected] = useState<LedgerRow[]>([]);
  const [mergeError, setMergeError] = useState("");
  const [mergeAmounts, setMergeAmounts] = useState<{ expense: number; income: number } | null>(null);
  const [announcement, setAnnouncement] = useState("");

  /** Writes the filters into the URL without a navigation; the stepped period is shared with Analisis. */
  const updateFilters = (patch: Partial<LedgerFilters>) => {
    const next = { ...filters, ...patch };
    if (("period" in patch || "offset" in patch) && next.period !== "all") {
      setTimeframe?.(next.period);
      setCycleOffset?.(next.offset);
    }
    const query = ledgerFilterQuery(next);
    replaceSearch(query ? `/ledger?${query}` : "/ledger");
  };

  const apiQuery = useMemo(() => {
    const query = ledgerApiParams(filters);
    if (appliedSearch) query.set("q", appliedSearch);
    if (listsLogicalMovements(filters, appliedSearch)) query.set("logical_movements", "true");
    query.set("include", "daily_totals,type_counts");
    query.set("limit", String(PAGE_SIZE));
    return query.toString();
  }, [filters, appliedSearch]);

  const list = useInfiniteQuery<LedgerPage>({
    queryKey: queryKeys.transactions.ledger(apiQuery),
    queryFn: ({ pageParam }) => api.get(`/transactions?${apiQuery}&offset=${pageParam as number}`),
    initialPageParam: 0,
    getNextPageParam: (last, pages) => {
      const loaded = pages.reduce((sum, page) => sum + page.transactions.length, 0);
      return loaded < last.total && last.transactions.length > 0 ? loaded : undefined;
    },
  });

  const pages = useMemo(() => list.data?.pages ?? [], [list.data]);
  const first = pages[0];
  const loadedRows = useMemo(() => uniqueRows(pages.map((page) => page.transactions)), [pages]);
  const shownRows = useMemo(
    () => consolidateLedgerMovements(loadedRows, listsLogicalMovements(filters, appliedSearch)),
    [loadedRows, filters, appliedSearch],
  );
  const dayTotals = useMemo(() => {
    const merged = new Map<string, DailyTotal>();
    let any = false;
    for (const page of pages) {
      if (!page.daily_totals) continue;
      any = true;
      for (const total of page.daily_totals) merged.set(total.date, total);
    }
    return any ? [...merged.values()] : undefined;
  }, [pages]);
  const groups = useMemo(() => groupByDay(shownRows, loadedRows, dayTotals), [shownRows, loadedRows, dayTotals]);

  const periodScoped = filters.period !== "all" || hasDateRange(filters);
  const figures: CashFlowMetrics | null = first?.summary ? (periodScoped ? first.summary.period ?? null : first.summary.cumulative) : null;
  const counts = first?.type_counts ?? null;
  const total = first?.total ?? 0;

  const loadMore = list.hasNextPage && !list.isFetchingNextPage ? () => void list.fetchNextPage() : null;
  const previousCount = useRef(0);
  useEffect(() => {
    const added = loadedRows.length - previousCount.current;
    if (previousCount.current > 0 && added > 0) setAnnouncement(t("transactions.announce.loaded", { count: added }));
    if (previousCount.current > 0 && added > 0 && !list.hasNextPage) {
      document.querySelector<HTMLElement>(`[data-row="${loadedRows[previousCount.current]?.id}"]`)?.focus();
    }
    previousCount.current = loadedRows.length;
  }, [loadedRows, list.hasNextPage, t]);
  useEffect(() => {
    previousCount.current = 0;
  }, [apiQuery]);

  // Opening a row: a phone sheet adds a history entry so back closes it; wider screens replace the URL.
  const openRow = (row: LedgerRow) => {
    const next = new URLSearchParams(searchKey);
    next.set("edit", row.id);
    if (phone) pushSearch(`/ledger?${next.toString()}`, { ledgerSheet: row.id });
    else replaceSearch(`/ledger?${next.toString()}`);
  };

  const closeDialog = () => {
    if (returnTo === "review" && editId) {
      const folder = returnFolder && (FOLDER_PARAMS as readonly string[]).includes(returnFolder) ? `&folder=${returnFolder}` : "";
      router.push(`/review?item=${editId}${folder}`);
      return;
    }
    if (window.history.state?.ledgerSheet) {
      window.history.back();
      return;
    }
    const next = new URLSearchParams(searchKey);
    next.delete("edit");
    next.delete("return");
    next.delete("folder");
    const query = next.toString();
    replaceSearch(query ? `/ledger?${query}` : "/ledger");
  };

  // Closing the dialog returns focus to its row once the list has settled (D18).
  const lastEdit = useRef<string | null>(null);
  useEffect(() => {
    const closed = lastEdit.current;
    lastEdit.current = editId;
    if (!closed || editId || returnTo) return;
    const timer = window.setTimeout(() => document.querySelector<HTMLElement>(`[data-row="${closed}"]`)?.focus(), FOCUS_SETTLE_MS);
    return () => window.clearTimeout(timer);
  }, [editId, returnTo]);

  // After a delete, focus moves to the next row (or the list heading) once the refreshed list no longer has it.
  const [deleted, setDeleted] = useState<{ id: string; neighbour: string | null } | null>(null);
  const afterDelete = (row: { id: string }) => {
    const index = shownRows.findIndex((item) => item.id === row.id);
    setDeleted({ id: row.id, neighbour: (shownRows[index + 1] ?? shownRows[index - 1])?.id ?? null });
  };
  useEffect(() => {
    if (!deleted || shownRows.some((item) => item.id === deleted.id)) return;
    const target = deleted.neighbour ? document.querySelector<HTMLElement>(`[data-row="${deleted.neighbour}"]`) : null;
    (target ?? listHeadingRef.current)?.focus();
    setDeleted(null);
  }, [deleted, shownRows]);

  // Selection for "Gabungkan jadi pindah saldo": two unlinked rows, one out and one in (D9).
  const selectionDisabled = useCallback((row: LedgerRow) => {
    if (row.movement_id) return true;
    return selected.length >= 2 && !selected.some((item) => item.id === row.id);
  }, [selected]);
  const toggle = (row: LedgerRow) => {
    setMergeError("");
    setSelected((current) => (current.some((item) => item.id === row.id)
      ? current.filter((item) => item.id !== row.id)
      : current.length < 2 ? [...current, row] : current));
  };
  const startSelection = (row: LedgerRow | null) => {
    setSelecting(true);
    setMergeError("");
    setSelected(row && !row.movement_id ? [row] : []);
  };
  const stopSelection = () => {
    setSelecting(false);
    setSelected([]);
    setMergeError("");
  };
  const selection: ListSelection = {
    active: selecting,
    selectedIds: selected.map((item) => item.id),
    disabled: selectionDisabled,
    toggle,
    start: startSelection,
  };

  const merge = useMutation({
    mutationFn: async () => {
      const details = await Promise.all(selected.map((row) => api.get<{ transaction: LedgerRow }>(`/transactions/${row.id}`).then((r) => r.transaction)));
      const expense = details.find((row) => row.type === "expense");
      const income = details.find((row) => row.type === "income");
      if (!expense || !income) throw new Error(t("transactions.merge.needOneEach"));
      if (expense.amount !== income.amount) {
        setMergeAmounts({ expense: expense.amount, income: income.amount });
        return null;
      }
      if (expense.movement_id || income.movement_id || expense.account_id === income.account_id) {
        throw new Error(t("transactions.merge.needUnlinked"));
      }
      const response = await api.post<UndoFields>("/movements/merge", { expense_transaction_id: expense.id, income_transaction_id: income.id });
      return { response, expense };
    },
    onSuccess: (result) => {
      if (!result) return;
      refresh();
      stopSelection();
      record(t("transactions.toast.merged"), labels.transfer, result.response, result.expense.id);
    },
    onError: (error: Error) => setMergeError(error.message),
  });

  // Keys of the list region (D18): J and K move between rows, Z undoes the newest check, conversion or merge.
  const pageKeys = useMemo<PageShortcutGroup>(() => ({
    name: t("transactions.keys.group"),
    translated: true,
    entries: [
      { keys: ["J", "K"], text: t("transactions.keys.move") },
      { keys: ["Enter"], text: t("transactions.keys.open") },
      { keys: ["Z"], text: t("transactions.keys.undo") },
    ],
  }), [t]);
  usePageShortcutGroup(pageKeys);
  const moveFocus = (step: number) => {
    // While merging, a row that cannot be chosen has a disabled checkbox, which cannot take focus: J and K skip it.
    const rows = [...(listRegionRef.current?.querySelectorAll<HTMLElement>("[data-row]") ?? [])]
      .filter((row) => !row.querySelector("input:disabled"));
    if (!rows.length) return;
    const active = document.activeElement;
    const index = rows.findIndex((element) => element === active || element.contains(active));
    const next = index < 0 ? (step > 0 ? 0 : rows.length - 1) : Math.min(rows.length - 1, Math.max(0, index + step));
    const target = rows[next].querySelector<HTMLElement>("input") ?? rows[next];
    target.focus();
    target.scrollIntoView?.({ block: "nearest" });
  };
  useShortcut("j", () => moveFocus(1), listRegionRef);
  useShortcut("k", () => moveFocus(-1), listRegionRef);
  useShortcut("z", undoNewest, listRegionRef);

  const accountsData = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") }).data?.accounts;
  const accounts = useMemo(() => accountsData ?? [], [accountsData]);
  const categories = useQuery<{ categories: any[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") }).data?.categories ?? [];
  const providers = useMemo(
    () => listCreditAccountChoices(accounts).filter((account) => !account.is_archived).map((account) => ({ id: String(account.id), name: String(account.name) })),
    [accounts],
  );
  const provider = providers.find((item) => item.id === filters.account) ?? null;

  const accountName = (id: string) => accounts.flatMap((account) => [account, ...(account.children ?? [])]).find((account) => account.id === id)?.name ?? "…";
  const categoryName = (id: string) => categories.find((category) => category.id === id)?.name ?? "…";
  const rangeText = (from: string | null, to: string | null) => {
    if (from && to) return `${format.shortDateAt(from)} – ${format.shortDateAt(to)}`;
    return from ? t("transactions.chips.since", { date: format.shortDateAt(from) }) : t("transactions.chips.until", { date: format.shortDateAt(to ?? "") });
  };
  const reviewLabel = { needs_review: t("transactions.filter.needsReview"), auto: t("transactions.filter.auto"), reviewed: t("transactions.filter.reviewed") };

  const clearSearch = () => {
    setSearch("");
    setAppliedSearch("");
    if (filters.q) updateFilters({ q: "" });
  };
  const chips: ActiveChip[] = [
    filters.account !== "all" ? { key: "account", label: accountName(filters.account), remove: () => updateFilters({ account: "all" }) } : null,
    filters.category !== "all"
      ? { key: "category", label: filters.category === UNCATEGORIZED ? t("terms.noCategory") : categoryName(filters.category), remove: () => updateFilters({ category: "all" }) }
      : null,
    filters.kakeibo !== "all" ? { key: "pillar", label: bucketName(t, filters.kakeibo), remove: () => updateFilters({ kakeibo: "all" }) } : null,
    filters.review !== "all" ? { key: "review", label: reviewLabel[filters.review], remove: () => updateFilters({ review: "all" }) } : null,
    filters.source !== "all"
      ? { key: "source", label: filters.source === "notification" ? t("transactions.chips.phone") : t("transactions.chips.typed"), remove: () => updateFilters({ source: "all" }) }
      : null,
    filters.waiting ? { key: "waiting", label: t("transactions.chips.waiting"), remove: () => updateFilters({ waiting: false }) } : null,
    filters.dup ? { key: "dup", label: t("transactions.chips.duplicates"), remove: () => updateFilters({ dup: false }) } : null,
    hasDateRange(filters) ? { key: "range", label: rangeText(filters.from, filters.to), remove: () => updateFilters({ from: null, to: null }) } : null,
    appliedSearch ? { key: "q", label: t("transactions.chips.query", { text: appliedSearch }), remove: clearSearch } : null,
  ].filter((chip): chip is ActiveChip => chip !== null);

  const clearAll = () => {
    setSearch("");
    setAppliedSearch("");
    updateFilters({ ...DEFAULT_LEDGER_FILTERS, period: filters.period, offset: filters.offset, type: filters.type });
  };

  const applyFilters = (draft: FilterDraft) => {
    updateFilters({ ...draft, offset: draft.period === filters.period ? filters.offset : 0 });
    setFilterOpen(false);
    window.setTimeout(() => filterButtonRef.current?.focus(), 0);
  };
  const closeFilters = () => {
    setFilterOpen(false);
    window.setTimeout(() => filterButtonRef.current?.focus(), 0);
  };

  const setType = (type: LedgerTypeFilter) => updateFilters({ type });
  const period = first?.summary?.period;
  const heroPeriod = filters.period !== "all"
    ? null
    : hasDateRange(filters) ? { kind: "range" as const, range: rangeText(filters.from, filters.to) } : { kind: "all" as const };
  const creditWindow = filters.period !== "all"
    ? { from: period?.start ? jakartaDay(period.start) : null, to: period?.end ? jakartaDay(period.end) : null }
    : { from: filters.from ? jakartaDay(filters.from) : null, to: filters.to ? jakartaDay(filters.to) : null };
  const nothingSet = chips.length === 0 && filters.period === "all" && filters.type === "all";
  const loadingFirst = list.isLoading;
  const failed = list.isError && !first;
  const remainingCount = Math.max(0, total - loadedRows.length);

  return (
    <div className="tx-page">
      <PageActions
        moreLabel={t("transactions.page.more")}
        more={[
          {
            key: "merge",
            label: selecting ? t("transactions.page.mergeStop") : t("transactions.page.mergeStart"),
            icon: "swap",
            onSelect: () => (selecting ? stopSelection() : startSelection(null)),
          },
          { key: "filter", label: t("transactions.page.filter"), icon: "sliders", onSelect: () => setFilterOpen(true) },
        ]}
      >
        {filters.period !== "all" && (
          <PeriodStepper
            unit={filters.period === "month" ? "Bulan" : "Siklus"}
            label={period?.start ? period.label ?? `${format.shortDateAt(period.start)} – ${format.shortDateAt(period.end ?? period.start)}` : null}
            period={period?.start ? { start: period.start, end: period.end } : null}
            offset={filters.offset}
            onChange={(next) => updateFilters({ offset: next })}
          />
        )}
      </PageActions>

      {!failed && <LedgerHero figures={figures} loading={loadingFirst} type={filters.type} periodLabel={heroPeriod} money={money} />}

      <div className="tx">
        <LedgerRail
          type={filters.type}
          counts={loadingFirst ? null : counts}
          format={format}
          onType={setType}
          period={filters.period}
          onPeriod={(next) => updateFilters({ period: next, offset: 0 })}
        />
        <div ref={listRegionRef} className="tx-main">
          <LedgerTypeControl type={filters.type} counts={loadingFirst ? null : counts} format={format} onType={setType} />
          {filters.type === "credit" && <ProviderControl providers={providers} account={filters.account} onAccount={(account) => updateFilters({ account })} />}
          {provider && <CreditHeader accountId={provider.id} from={creditWindow.from} to={creditWindow.to} money={money} format={format} />}
          <LedgerToolbar
            ref={filterButtonRef}
            search={search}
            onSearch={setSearch}
            filterCount={activeFilterCount(filters)}
            onFilter={() => setFilterOpen(true)}
            chips={chips}
          />
          {selecting && <MergeBar count={selected.length} error={mergeError} pending={merge.isPending} onCancel={stopSelection} onMerge={() => merge.mutate()} />}
          <section className={selecting ? "tx-list is-selecting" : "tx-list"} aria-labelledby="tx-list-title">
            <h2 id="tx-list-title" ref={listHeadingRef} tabIndex={-1} className="sr-only">
              {t("transactions.list.heading", { count: total })}
            </h2>
            <p className="sr-only" role="status" aria-live="polite">{announcement}</p>
            {loadingFirst && (
              <div role="status" aria-busy="true" aria-label={t("transactions.states.loading")}>
                {SKELETON_ROWS.map((width, index) => (
                  <div key={index} className="skel-row" aria-hidden="true">
                    <span className="skel" style={{ width: 40 }} />
                    <span className="grow"><span className="skel" style={{ width: `${width}%` }} /></span>
                    <span className="skel" style={{ width: 96 }} />
                  </div>
                ))}
              </div>
            )}
            {failed && (
              <div className="error-box" role="alert">
                <Icon name="alert" className="h-5 w-5" />
                <div className="grow">
                  <p className="t-strong">{t("transactions.states.errorTitle")}</p>
                  <p className="t-meta">{t("transactions.states.errorLine")}</p>
                </div>
                <button type="button" className="ws-btn" onClick={() => list.refetch()}>{t("common.tryAgain")}</button>
              </div>
            )}
            {first && shownRows.length === 0 && (
              <div className="tx-empty">
                <p className="t-strong">{nothingSet ? t("transactions.states.emptyTitle") : t("transactions.states.noMatchTitle")}</p>
                <p className="t-body">{nothingSet ? t("transactions.states.emptyLine") : t("transactions.states.noMatchLine")}</p>
                {chips.length > 0 ? (
                  <button type="button" className="ws-btn" onClick={clearAll}>{t("transactions.states.clearAll")}</button>
                ) : openCapture ? (
                  <button type="button" className="ws-btn" onClick={openCapture}>{t("transactions.states.add")}</button>
                ) : null}
              </div>
            )}
            {shownRows.length > 0 && (
              <LedgerList
                groups={groups}
                openRow={openRow}
                selection={selection}
                loadMore={loadMore}
                openId={editId}
                accountView={filters.account !== "all"}
                today={todayLocal()}
                labels={labels}
                format={format}
                money={money}
              />
            )}
            {first && shownRows.length > 0 && (list.hasNextPage ? (
              <div className="list-foot">
                <button type="button" className="ws-btn" disabled={list.isFetchingNextPage} onClick={() => void list.fetchNextPage()}>
                  {list.isFetchingNextPage ? t("transactions.more.loading") : t("transactions.more.load", { next: format.integer(Math.min(PAGE_SIZE, remainingCount)) })}
                </button>
                {list.isFetchNextPageError && <span role="alert" className="err">{t("transactions.more.failed")}</span>}
              </div>
            ) : (
              <p className="list-end">{t("transactions.more.end", { count: total })}</p>
            ))}
          </section>
        </div>
      </div>

      <TransactionDialog
        transactionId={editId}
        onClose={closeDialog}
        record={record}
        onPickPair={(row) => {
          closeDialog();
          startSelection(row);
          showToast(t("transactions.merge.pickPair"));
        }}
        onDeleted={afterDelete}
      />

      <LedgerFilterDialog
        open={filterOpen}
        filters={filters}
        accounts={accounts}
        categories={categories}
        onApply={applyFilters}
        onClose={closeFilters}
      />

      <ShellDialog
        open={Boolean(mergeAmounts)}
        onClose={() => setMergeAmounts(null)}
        title={t("transactions.merge.mismatchTitle")}
        width={480}
        closeLabel={t("common.close")}
        footer={<button type="button" className="ws-btn ws-btn-ink" onClick={() => setMergeAmounts(null)}>{t("common.close")}</button>}
      >
        {mergeAmounts && (
          <p role="alert">
            {t.rich("transactions.merge.mismatchBody", {
              out: () => <MaskedMoney text={money(mergeAmounts.expense)} />,
              in: () => <MaskedMoney text={money(mergeAmounts.income)} />,
            })}
          </p>
        )}
      </ShellDialog>
    </div>
  );
}
