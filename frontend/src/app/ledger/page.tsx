"use client";

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useInfiniteQuery, useMutation, useQuery } from "@tanstack/react-query";
import { useRouter, useSearchParams } from "next/navigation";
import { useAppCtx } from "@/components/layout/AppLayout";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { LedgerFeed, LedgerTable, type ListSelection } from "@/components/transactions/LedgerList";
import { TransactionDrawer } from "@/components/transactions/TransactionDrawer";
import type { LedgerRow } from "@/components/transactions/types";
import { useRefreshTransactionViews, useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { TopBarControls } from "@/components/workspace/context";
import { useToast } from "@/components/workspace/overlays";
import { EmptyState, ErrorState, Kpi, KpiStrip, Segmented, Skeleton } from "@/components/workspace/primitives";
import { useShortcut, usePageShortcutGroup, type PageShortcutGroup } from "@/components/workspace/shortcuts";
import { LEDGER_SEARCH_ID } from "@/components/workspace/TopBar";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { api } from "@/lib/api";
import { KAKEIBO_BUCKETS, kakeiboBucketInfo } from "@/lib/kakeibo";
import { groupByDay, uniqueRows, type DailyTotal } from "@/lib/ledgerDays";
import {
  DEFAULT_LEDGER_FILTERS,
  LEDGER_CHIPS,
  UNCATEGORIZED,
  chipOf,
  hasDateRange,
  ledgerApiParams,
  ledgerFilterQuery,
  listsLogicalMovements,
  parseLedgerFilters,
  withChip,
  type LedgerFilters,
  type LedgerPeriod,
  type LedgerReviewFilter,
  type LedgerTypeFilter,
} from "@/lib/ledgerFilters";
import { consolidateLedgerMovements } from "@/lib/ledgerMovements";
import { queryKeys } from "@/lib/queryKeys";
import type { UndoFields } from "@/lib/reviewUndo";
import { FOLDER_PARAMS } from "@/lib/triage";
import { pushSearch, replaceSearch } from "@/lib/urlState";

const PAGE_SIZE = 50;
const SEARCH_DELAY_MS = 300;
const PHONE_LIST_WIDTH = 760;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

interface CashFlowMetrics {
  inflow: number;
  outflow: number;
  net: number;
  gross_outflow?: number;
  reimbursement_offset?: number;
}

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
  type_counts?: Record<"all" | "expense" | "income" | "movement" | "investment", number>;
}

const TABS: { value: LedgerTypeFilter; label: string; count: keyof NonNullable<LedgerPage["type_counts"]> }[] = [
  { value: "all", label: "Semua", count: "all" },
  { value: "expense", label: "Keluar", count: "expense" },
  { value: "income", label: "Masuk", count: "income" },
  { value: "movement", label: "Pindah saldo", count: "movement" },
  { value: "investment", label: "Investasi", count: "investment" },
];

const PERIOD_OPTIONS: { value: LedgerPeriod; label: string }[] = [
  { value: "cycle", label: "Siklus gaji" },
  { value: "month", label: "Bulan kalender" },
  { value: "all", label: "Semua waktu" },
];

const REVIEW_OPTIONS: { value: LedgerReviewFilter; label: string }[] = [
  { value: "all", label: "Semua status" },
  { value: "needs_review", label: "Perlu ditinjau" },
  { value: "auto", label: "Belum dicek" },
  { value: "reviewed", label: "Sudah dicek" },
];

interface RemovableFilter {
  key: string;
  label: string;
  clear: Partial<LedgerFilters>;
}

const SHORT_DATE = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });

function shortDate(value: string): string {
  return SHORT_DATE.format(new Date(value));
}

function periodLabel(period: PeriodMetrics | null | undefined): string {
  if (!period?.start || !period.end) return "Memuat…";
  return period.label ?? `${shortDate(period.start)} – ${shortDate(period.end)}`;
}

const PAGE_KEYS: PageShortcutGroup = {
  name: "Di Transaksi",
  caption: "J, K, dan Z di Transaksi",
  entries: [
    { keys: ["J", "K"], text: "Pindah ke transaksi berikutnya atau sebelumnya" },
    { keys: ["Enter"], text: "Buka detail transaksi" },
    { keys: ["Z"], text: "Urungkan tanda sudah dicek atau top up terakhir" },
  ],
};

export default function LedgerPage() {
  // The page reads its filters from the URL, which needs a suspense boundary for static rendering.
  return (
    <Suspense fallback={null}>
      <LedgerPageContent />
    </Suspense>
  );
}

function LedgerPageContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchKey = searchParams?.toString() ?? "";
  const { bal, setTimeframe, setCycleOffset } = useAppCtx();
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();
  const { record, undoNewest } = useTransactionUndo();
  const listRegionRef = useRef<HTMLElement>(null);
  const listHeadingRef = useRef<HTMLHeadingElement>(null);
  const loadMoreRef = useRef<HTMLButtonElement>(null);
  const phone = useWidthAtMost(listRegionRef, PHONE_LIST_WIDTH);

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

  const [filterOpen, setFilterOpen] = useState(false);
  const [recurringOpen, setRecurringOpen] = useState(false);
  const [selecting, setSelecting] = useState(false);
  const [selected, setSelected] = useState<LedgerRow[]>([]);
  const [mergeError, setMergeError] = useState("");
  const [mergeAmounts, setMergeAmounts] = useState<{ expense: number; income: number } | null>(null);
  const [announcement, setAnnouncement] = useState("");

  /** Writes the filters into the URL without a navigation (D13); the stepped period is shared with Analisis. */
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
  const counts = first?.type_counts;

  const loadMore = list.hasNextPage ? () => void list.fetchNextPage() : null;
  const previousCount = useRef(0);
  useEffect(() => {
    const added = loadedRows.length - previousCount.current;
    if (previousCount.current > 0 && added > 0) setAnnouncement(`${added} transaksi lagi dimuat.`);
    if (previousCount.current > 0 && added > 0 && !list.hasNextPage) {
      document.querySelector<HTMLElement>(`[data-tx-row="${loadedRows[previousCount.current]?.id}"]`)?.focus();
    }
    previousCount.current = loadedRows.length;
  }, [loadedRows, list.hasNextPage]);
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

  const closeDrawer = () => {
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

  // Closing the drawer returns focus to its row; the list may have re-rendered between table and feed.
  const lastEdit = useRef<string | null>(null);
  useEffect(() => {
    const closed = lastEdit.current;
    lastEdit.current = editId;
    if (!closed || editId || returnTo) return;
    const row = document.querySelector<HTMLElement>(`[data-tx-row="${closed}"]`);
    if (row) row.focus();
  }, [editId, returnTo]);

  // After a delete, focus moves to the next row (or the list heading) once the refreshed list no longer has it.
  const [deleted, setDeleted] = useState<{ id: string; neighbour: string | null } | null>(null);
  const afterDelete = (row: { id: string }) => {
    const index = shownRows.findIndex((item) => item.id === row.id);
    setDeleted({ id: row.id, neighbour: (shownRows[index + 1] ?? shownRows[index - 1])?.id ?? null });
  };
  useEffect(() => {
    if (!deleted || shownRows.some((item) => item.id === deleted.id)) return;
    const target = deleted.neighbour ? document.querySelector<HTMLElement>(`[data-tx-row="${deleted.neighbour}"]`) : null;
    (target ?? listHeadingRef.current)?.focus();
    setDeleted(null);
  }, [deleted, shownRows]);

  // Selection for "Jadikan Pindah Saldo": two unlinked rows, one out and one in (OD23).
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
      if (!expense || !income) throw new Error("Pilih satu transaksi keluar dan satu transaksi masuk.");
      if (expense.amount !== income.amount) {
        setMergeAmounts({ expense: expense.amount, income: income.amount });
        return null;
      }
      if (expense.movement_id || income.movement_id || expense.account_id === income.account_id) {
        throw new Error("Pilih transaksi yang belum tertaut dari dua rekening berbeda.");
      }
      const response = await api.post<UndoFields>("/movements/merge", { expense_transaction_id: expense.id, income_transaction_id: income.id });
      return { response, expense };
    },
    onSuccess: (result) => {
      if (!result) return;
      refresh();
      stopSelection();
      record("Digabung sebagai pindah saldo.", "Pindah saldo", result.response, result.expense.id);
    },
    onError: (error: Error) => setMergeError(error.message),
  });

  // Keys of the list region (D12): J and K move between rows, Z undoes the newest check or top-up.
  usePageShortcutGroup(PAGE_KEYS);
  const moveFocus = (step: number) => {
    const rows = [...(listRegionRef.current?.querySelectorAll<HTMLElement>("[data-tx-row]") ?? [])];
    if (!rows.length) return;
    const index = rows.findIndex((element) => element === document.activeElement);
    const next = index < 0 ? (step > 0 ? 0 : rows.length - 1) : Math.min(rows.length - 1, Math.max(0, index + step));
    rows[next].focus();
    rows[next].scrollIntoView?.({ block: "nearest" });
  };
  useShortcut("j", () => moveFocus(1), listRegionRef);
  useShortcut("k", () => moveFocus(-1), listRegionRef);
  useShortcut("z", undoNewest, listRegionRef);

  const accounts = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") }).data?.accounts ?? [];
  const categories = useQuery<{ categories: any[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") }).data?.categories ?? [];
  const chip = chipOf(filters);
  const removable = ([
    filters.account !== "all" ? { key: "account", label: `Rekening: ${accountName(filters.account)}`, clear: { account: "all" } } : null,
    filters.category !== "all" ? { key: "category", label: filters.category === UNCATEGORIZED ? "Tanpa kategori" : `Kategori: ${categoryName(filters.category)}`, clear: { category: "all" } } : null,
    filters.kakeibo !== "all" ? { key: "pillar", label: `Pilar: ${kakeiboBucketInfo(filters.kakeibo)?.label ?? filters.kakeibo}`, clear: { kakeibo: "all" } } : null,
    filters.review !== "all" && filters.review !== "auto" ? { key: "review", label: REVIEW_OPTIONS.find((option) => option.value === filters.review)?.label ?? "", clear: { review: "all" } } : null,
    hasDateRange(filters) ? { key: "range", label: `Rentang: ${rangeText(filters.from, filters.to)}`, clear: { from: null, to: null } } : null,
    filters.dup ? { key: "dup", label: "Kemungkinan duplikat", clear: { dup: false } } : null,
  ] as (RemovableFilter | null)[]).filter((item): item is RemovableFilter => Boolean(item));

  function accountName(id: string) {
    return accounts.flatMap((account) => [account, ...(account.children ?? [])]).find((account) => account.id === id)?.name ?? "…";
  }
  function categoryName(id: string) {
    return categories.find((category) => category.id === id)?.name ?? "…";
  }
  function rangeText(from: string | null, to: string | null) {
    if (from && to) return `${shortDate(from)} – ${shortDate(to)}`;
    return from ? `sejak ${shortDate(from)}` : `sampai ${shortDate(to ?? "")}`;
  }

  const clearAll = () => {
    setSearch("");
    setAppliedSearch("");
    updateFilters({ ...DEFAULT_LEDGER_FILTERS, period: filters.period, offset: filters.offset });
  };

  const listProps = { groups, bal, openRow, selection, loadMore, openId: editId };
  const offset = figures?.reimbursement_offset ?? 0;

  return (
    <div className="ws-tx space-y-4">
      {filters.period !== "all" && (
        <TopBarControls>
          <PeriodStepper
            unit={filters.period === "month" ? "Bulan" : "Siklus"}
            label={periodLabel(first?.summary?.period)}
            offset={filters.offset}
            onChange={(next) => updateFilters({ offset: next })}
          />
        </TopBarControls>
      )}

      <PendingScheduledBanner onOpenRulesManager={() => setRecurringOpen(true)} />

      <div className="ws-tx-toolbar">
        <div className="ws-seg ws-tx-tabs" role="group" aria-label="Jenis transaksi">
          {TABS.map((tab) => (
            <button key={tab.value} type="button" aria-pressed={filters.type === tab.value} onClick={() => updateFilters({ type: tab.value })}>
              {tab.label}
              {counts && <span className="ws-cnt tabular-nums">{counts[tab.count]}</span>}
            </button>
          ))}
        </div>
        <label className="ws-search ws-tx-search">
          <span className="sr-only">Cari transaksi</span>
          <Icon name="search" className="h-4 w-4 shrink-0" />
          <input
            id={LEDGER_SEARCH_ID}
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Cari catatan, merchant, kategori"
            autoComplete="off"
          />
        </label>
        <div className="ws-tx-tools">
          <Segmented label="Periode" options={PERIOD_OPTIONS} value={filters.period} onChange={(period) => updateFilters({ period, offset: 0 })} />
          <button type="button" className="ws-btn" onClick={() => setFilterOpen(true)} aria-haspopup="dialog">
            <Icon name="sliders" className="h-4 w-4" />
            Filter
          </button>
          <button type="button" className="ws-btn" onClick={() => (selecting ? stopSelection() : startSelection(null))} aria-pressed={selecting}>
            <Icon name="repeat" className="h-4 w-4" />
            Gabungkan
          </button>
          <button type="button" className="ws-btn" onClick={() => setRecurringOpen(true)}>
            <Icon name="calendar" className="h-4 w-4" />
            Rutin
          </button>
        </div>
      </div>

      <div className="ws-chips ws-tx-chips" role="group" aria-label="Saring transaksi">
        {LEDGER_CHIPS.map((option) => (
          <button key={option.value} type="button" className="ws-chip" aria-pressed={chip === option.value} onClick={() => updateFilters(withChip(filters, option.value))}>
            {option.label}
          </button>
        ))}
        {removable.map((item) => (
          <span key={item.key} className="ws-chip ws-tx-chip-static">
            {item.label}
            <button type="button" className="ws-tx-chip-x" aria-label={`Hapus filter ${item.label}`} onClick={() => updateFilters(item.clear)}>
              <Icon name="x" className="h-3.5 w-3.5" />
            </button>
          </span>
        ))}
      </div>

      {figures ? (
        <KpiStrip label={periodScoped ? `Total ${periodLabel(first?.summary?.period)}` : "Total semua waktu"}>
          <Kpi icon="arrow-down-left" label="Masuk" value={<span className="ws-amt-in tabular-nums">{bal(figures.inflow)}</span>} />
          <Kpi
            icon="arrow-up-right"
            label="Belanja"
            value={<span className="tabular-nums">{bal(figures.outflow)}</span>}
            caption={offset > 0 ? `sudah dikurangi penggantian ${bal(offset)}` : undefined}
          />
          <Kpi icon="arrows-right-left" label="Bersih" value={<span className="tabular-nums">{bal(figures.net)}</span>} />
        </KpiStrip>
      ) : list.isLoading ? (
        <Skeleton className="h-24" />
      ) : null}
      <p className="ws-caption">Pindah saldo antar rekening tidak dihitung masuk atau belanja. Di Beranda, Keluar adalah Belanja ditambah yang ditabung.</p>

      <section ref={listRegionRef} className="ws-card ws-tx-list" aria-labelledby="tx-list-title">
        <h2 id="tx-list-title" ref={listHeadingRef} tabIndex={-1} className="ws-tx-list-h">
          Daftar transaksi
          {first && <span className="ws-caption"> · {first.total} transaksi</span>}
        </h2>
        <p className="sr-only" role="status" aria-live="polite">{announcement}</p>
        {list.isLoading && (
          <div className="flex flex-col gap-2 py-3" aria-busy="true">
            <Skeleton className="h-10" />
            <Skeleton className="h-10" />
            <Skeleton className="h-10" />
          </div>
        )}
        {list.isError && !first && <ErrorState message="Transaksi belum bisa dimuat." onRetry={() => list.refetch()} />}
        {first && shownRows.length === 0 && (
          <EmptyState icon="search" title="Tidak ditemukan" action={<button type="button" className="ws-btn" onClick={clearAll}>Hapus filter</button>}>
            {appliedSearch ? `Tidak ada transaksi yang cocok dengan “${appliedSearch}”.` : "Tidak ada transaksi yang cocok dengan filter ini."}
          </EmptyState>
        )}
        {shownRows.length > 0 && (phone ? <LedgerFeed {...listProps} /> : <LedgerTable {...listProps} />)}
        {list.hasNextPage && (
          <div className="ws-tx-loadmore">
            <button ref={loadMoreRef} type="button" className="ws-btn" disabled={list.isFetchingNextPage} onClick={() => void list.fetchNextPage()}>
              {list.isFetchingNextPage ? "Memuat…" : `Muat ${Math.min(PAGE_SIZE, (first?.total ?? 0) - loadedRows.length)} lagi`}
            </button>
            {list.isFetchNextPageError && <span role="alert" className="ws-caption">Gagal memuat transaksi berikutnya. Coba lagi.</span>}
          </div>
        )}
      </section>

      {selecting && (
        <div className="ws-tx-selectbar" role="region" aria-label="Gabungkan jadi pindah saldo">
          {mergeError && <p role="alert" className="ws-notice ws-tx-error">{mergeError}</p>}
          <span className="tabular-nums">{selected.length}/2 dipilih · pilih satu keluar dan satu masuk</span>
          <button type="button" className="ws-btn ws-btn-ink" disabled={selected.length !== 2 || merge.isPending} onClick={() => merge.mutate()}>
            {merge.isPending ? "Menyimpan…" : "Jadikan Pindah Saldo"}
          </button>
          <button type="button" className="ws-btn" onClick={stopSelection}>Batal</button>
        </div>
      )}

      <TransactionDrawer
        transactionId={editId}
        onClose={closeDrawer}
        record={record}
        onPickPair={(row) => {
          closeDrawer();
          startSelection(row);
          showToast("Pilih pasangannya di daftar, lalu Jadikan Pindah Saldo.");
        }}
        onDeleted={afterDelete}
      />

      <Modal open={Boolean(mergeAmounts)} onClose={() => setMergeAmounts(null)} title="Nominal tidak sama" centered hideCloseButton>
        {mergeAmounts && (
          <div className="space-y-4 text-sm">
            <p role="alert" className="ws-notice">
              Nominal keluar ({bal(mergeAmounts.expense)}) berbeda dengan nominal masuk ({bal(mergeAmounts.income)}). Pindah saldo hanya dapat dibuat jika nominalnya sama.
            </p>
            <div className="flex justify-end">
              <button type="button" data-autofocus className="ws-btn ws-btn-ink" onClick={() => setMergeAmounts(null)}>Tutup</button>
            </div>
          </div>
        )}
      </Modal>

      <RecurringRulesModal open={recurringOpen} onClose={() => setRecurringOpen(false)} />

      <Modal open={filterOpen} onClose={() => setFilterOpen(false)} title="Filter transaksi">
        <div className="space-y-4 pt-1 text-sm">
          <label className="ws-field">
            Rekening
            <select value={filters.account} onChange={(event) => updateFilters({ account: event.target.value })}>
              <option value="all">Semua rekening</option>
              <AccountSelectOptions accounts={accounts} allowParentSelection={true} />
            </select>
          </label>
          <label className="ws-field">
            Kategori
            <select value={filters.category} onChange={(event) => updateFilters({ category: event.target.value })}>
              <option value="all">Semua kategori</option>
              <option value={UNCATEGORIZED}>Tanpa kategori</option>
              {categories.map((category) => (
                <option key={category.id} value={category.id}>{category.name} ({category.kind === "income" ? "Masuk" : "Keluar"})</option>
              ))}
            </select>
          </label>
          <label className="ws-field">
            Pilar Kakeibo
            <select value={filters.kakeibo} onChange={(event) => updateFilters({ kakeibo: event.target.value as LedgerFilters["kakeibo"] })}>
              <option value="all">Semua pilar</option>
              {KAKEIBO_BUCKETS.map((pillar) => <option key={pillar.value} value={pillar.value}>{pillar.label}</option>)}
            </select>
          </label>
          <label className="ws-field">
            Status
            <select value={filters.review} onChange={(event) => updateFilters({ review: event.target.value as LedgerReviewFilter })}>
              {REVIEW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <div className="flex gap-2 pt-2">
            <button type="button" className="ws-btn flex-1" onClick={() => updateFilters({ account: "all", category: "all", kakeibo: "all", review: "all" })}>Atur ulang</button>
            <button type="button" className="ws-btn ws-btn-ink flex-1" onClick={() => setFilterOpen(false)}>Selesai</button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
