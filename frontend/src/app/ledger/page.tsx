"use client";
import { Suspense, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";
import { FilterBar } from "@/components/ui/FilterBar";
import type { LedgerRow, LedgerResponse, Category } from "@/types/domain";

const FINANCIAL_QUERY_KEYS = [
  ["ledger"],
  ["summary"],
  ["accounts"],
  ["dashboard"],
  ["buckets"],
  ["goals"],
  ["allocation-plans"],
  ["net-worth"],
] as const;

// Fallback icon map keyed by category kind or well-known names
const KIND_ICONS: Record<string, string> = {
  income: "💵", expense: "🛍️", transfer: "⇄", adjustment: "⚙️",
};
const NAME_ICONS: Record<string, string> = {
  salary: "💵", bonus: "🎁", freelance: "💻",
  "food & dining": "🍽️", food: "🍽️", dining: "🍽️", groceries: "🛒",
  transport: "🚗", shopping: "🛍️", health: "❤️",
  utilities: "💡", bills: "💡", housing: "🏠",
  entertainment: "🎬", education: "📚", savings: "🏦",
  investment: "📈", "internal movement": "⇄", "transfer in": "⇄", "transfer out": "⇄",
  "interest income": "💰", giving: "❤️", church: "⛪",
};

function getCategoryIcon(category: Category | undefined, fallbackName: string): string {
  if (category?.icon) return category.icon;
  const byName = NAME_ICONS[category?.name?.toLowerCase() ?? ""] ?? NAME_ICONS[fallbackName.toLowerCase()];
  if (byName) return byName;
  return KIND_ICONS[category?.kind ?? "expense"] ?? "📋";
}

export default function LedgerPage() {
  return (
    <Suspense fallback={<div className="workbench-page text-sm text-[var(--muted)]">Loading ledger...</div>}>
      <LedgerContent />
    </Suspense>
  );
}

function LedgerContent() {
  const { accounts, hideBalances } = useAppCtx();
  const qc = useQueryClient();
  const searchParams = useSearchParams();
  const bal = (n: number) => hideBalances ? "Rp ••••" : fmtMoney(n);
  const invalidateFinancialQueries = () =>
    Promise.all(FINANCIAL_QUERY_KEYS.map((queryKey) => qc.invalidateQueries({ queryKey })));

  const { data: categoriesData } = useQuery<{ categories: Category[] }>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
  });
  // Build a fast lookup map: category_id -> Category
  const categoryById: Record<string, Category> = {};
  (categoriesData?.categories ?? []).forEach((c) => { categoryById[c.category_id] = c; });

  // Filters
  const [scope, setScope] = useState<"all" | "account">("all");
  const [accountId, setAccountId] = useState<string | null>(null);
  const [categoryFilter, setCategoryFilter] = useState("");
  const [typeFilter, setTypeFilter] = useState<"all" | "income" | "expense" | "transfer" | "payroll">("all");
  const [search, setSearch] = useState("");
  const perPage = 10;
  const loadMoreRef = useRef<HTMLTableRowElement | null>(null);

  // Modals
  const [txModal, setTxModal] = useState(false);
  const [editingRow, setEditingRow] = useState<LedgerRow | null>(null);
  const [selectedRow, setSelectedRow] = useState<LedgerRow | null>(null);
  const [movementModal, setMovementModal] = useState(false);
  const [editingMovement, setEditingMovement] = useState<any | null>(null);
  const [deleteErr, setDeleteErr] = useState("");
  const [deletingDetail, setDeletingDetail] = useState(false);
  const [receiptErr, setReceiptErr] = useState("");
  const [receiptBusy, setReceiptBusy] = useState(false);

  const categories = categoriesData?.categories ?? [];

  // Ledger data
  const {
    data: ledgerData,
    isLoading,
    isFetchingNextPage,
    fetchNextPage,
    hasNextPage,
  } = useInfiniteQuery<LedgerResponse>({
    queryKey: ["ledger", scope, accountId, categoryFilter, typeFilter, search, perPage],
    initialPageParam: 0,
    queryFn: ({ pageParam }) => {
      const params = new URLSearchParams({
        scope,
        limit: String(perPage),
        offset: String(pageParam ?? 0),
        order: "desc",
        include_switch: "true",
        include_summary: pageParam === 0 ? "true" : "false",
      });
      if (accountId) params.set("account_id", accountId);
      if (categoryFilter) params.set("category_id", categoryFilter);
      if (typeFilter !== "all") params.set("kind", typeFilter);
      if (search) params.set("q", search);
      return api.get(`/ledger?${params.toString()}`);
    },
    getNextPageParam: (lastPage) => lastPage.paging.has_more ? lastPage.paging.next_offset : undefined,
  });

  const ledgerPages = ledgerData?.pages ?? [];
  const rows = ledgerPages.flatMap((pageData) => pageData.rows);
  const cashFlowRows = rows.filter((r) => !r.is_transfer);
  const movementRows = rows.filter((r) => r.is_transfer);
  const totalIn = cashFlowRows.reduce((s, r) => s + r.debit, 0);
  const totalOut = cashFlowRows.reduce((s, r) => s + r.credit, 0);
  const movementIn = movementRows.reduce((s, r) => s + r.debit, 0);
  const movementOut = movementRows.reduce((s, r) => s + r.credit, 0);
  const filteredRows = rows;

  const selectedTxId = selectedRow?.transaction_id;
  const { data: receiptData } = useQuery<any | null>({
    queryKey: ["transaction-receipt", selectedTxId],
    enabled: !!selectedTxId && !selectedRow?.is_transfer,
    queryFn: async () => {
      try {
        return await api.get(`/transactions/${selectedTxId}/receipt`);
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return null;
        throw e;
      }
    },
  });
  const { data: auditData } = useQuery<{ audit: any[] }>({
    queryKey: ["transaction-audit", selectedTxId],
    enabled: !!selectedTxId && !selectedRow?.is_transfer,
    queryFn: () => api.get(`/transactions/audit?transaction_id=${selectedTxId}`),
  });
  const auditRows = auditData?.audit ?? [];
  const hasHistory = auditRows.length > 1;
  const selectedTransferId = selectedRow?.is_transfer ? selectedRow.transfer_id : null;
  const { data: movementDetail } = useQuery<any | null>({
    queryKey: ["account-movement", selectedTransferId],
    enabled: !!selectedTransferId,
    queryFn: () => api.get(`/account-movements/${selectedTransferId}`),
  });
  const movementSourceName = accounts.find((a: any) => a.account_id === movementDetail?.source_account_id)?.account_name ?? "Source account";
  const movementTargetName = accounts.find((a: any) => a.account_id === movementDetail?.target_account_id)?.account_name ?? "Target account";
  const movementLabel = movementDetail?.movement_type === "allocation_funding" ? "Allocation Funding" : "Internal Movement";

  useEffect(() => {
    const action = searchParams.get("action");
    if (!action) return;
    const params = new URLSearchParams(searchParams.toString());
    if (action === "add") {
      setEditingRow(null);
      setTxModal(true);
    } else if (action === "transfer" || action === "movement") {
      setMovementModal(true);
    }
    if (action) {
      params.delete("action");
      const nextUrl = params.toString() ? `${window.location.pathname}?${params.toString()}` : window.location.pathname;
      window.history.replaceState(null, "", nextUrl);
    }
  }, [searchParams]);

  useEffect(() => {
    const node = loadMoreRef.current;
    if (!node || !hasNextPage || isFetchingNextPage) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) fetchNextPage();
      },
      { root: null, rootMargin: "240px", threshold: 0.01 },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [fetchNextPage, hasNextPage, isFetchingNextPage, filteredRows.length]);

  async function deleteSelectedRow() {
    if (!selectedRow || selectedRow.is_transfer) return;
    if (!confirm(`Delete "${selectedRow.transaction_name}"?`)) return;
    setDeletingDetail(true);
    setDeleteErr("");
    try {
      await api.del(`/transactions/${selectedRow.transaction_id}`);
      setSelectedRow(null);
      await invalidateFinancialQueries();
    } catch (e: any) {
      setDeleteErr(e.message);
    } finally {
      setDeletingDetail(false);
    }
  }

  async function deleteSelectedMovement() {
    if (!selectedRow?.transfer_id) return;
    if (!confirm("Delete this account movement? This removes both paired ledger entries.")) return;
    setDeletingDetail(true);
    setDeleteErr("");
    try {
      await api.del(`/account-movements/${selectedRow.transfer_id}`);
      setSelectedRow(null);
      await invalidateFinancialQueries();
    } catch (e: any) {
      setDeleteErr(e.message);
    } finally {
      setDeletingDetail(false);
    }
  }

  async function uploadReceipt(file: File | null) {
    if (!file || !selectedRow) return;
    setReceiptBusy(true);
    setReceiptErr("");
    try {
      const form = new FormData();
      form.append("file", file);
      const res = await fetch(`/api/transactions/${selectedRow.transaction_id}/receipt`, {
        method: "POST",
        credentials: "include",
        body: form,
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? "Upload failed");
      }
      await qc.invalidateQueries({ queryKey: ["transaction-receipt", selectedRow.transaction_id] });
    } catch (e: any) {
      setReceiptErr(e.message);
    } finally {
      setReceiptBusy(false);
    }
  }

  async function deleteReceipt() {
    if (!selectedRow || !confirm("Delete this receipt?")) return;
    setReceiptBusy(true);
    setReceiptErr("");
    try {
      await api.del(`/transactions/${selectedRow.transaction_id}/receipt`);
      await qc.invalidateQueries({ queryKey: ["transaction-receipt", selectedRow.transaction_id] });
    } catch (e: any) {
      setReceiptErr(e.message);
    } finally {
      setReceiptBusy(false);
    }
  }

  const TYPE_PILLS = [
    { key: "all", label: "All" },
    { key: "income", label: "Cash In" },
    { key: "expense", label: "Cash Out" },
    { key: "transfer", label: "Movement" },
    { key: "payroll", label: "★ Payroll" },
  ];

  return (
    <div className="flex h-[calc(100vh-var(--topbar-height))] min-w-0">
      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Filter bar */}
        <FilterBar>
          <Select value={scope === "all" ? "all" : accountId ?? "all"} onChange={(e) => {
            if (e.target.value === "all") { setScope("all"); setAccountId(null); }
            else { setScope("account"); setAccountId(e.target.value); }
          }} className="text-xs py-1.5 w-40">
            <option value="all">All accounts</option>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
          <Select value={categoryFilter} onChange={(e) => setCategoryFilter(e.target.value)} className="text-xs py-1.5 w-36">
            <option value="">All categories</option>
            {categories.map((c) => <option key={c.category_id} value={c.category_id}>{c.name}</option>)}
          </Select>
          <div className="relative flex-1 min-w-48">
            <input
              type="search" placeholder="Search description, category, account..."
              value={search} onChange={(e) => setSearch(e.target.value)}
              className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-xs bg-[var(--surface)] text-[var(--text)] pr-8"
            />
            <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[var(--muted)] text-xs">🔍</span>
          </div>
          <div className="ml-auto flex gap-2">
            <Button size="sm" variant="secondary" onClick={() => setMovementModal(true)}>⇄ Move Accounts</Button>
            <Button size="sm" variant="primary" onClick={() => { setEditingRow(null); setTxModal(true); }}>+ Add Transaction</Button>
          </div>
        </FilterBar>

        {/* Type pills */}
        <div className="px-5 py-2 border-b border-[var(--border)] bg-[var(--surface)] flex gap-1.5 items-center">
          {TYPE_PILLS.map((p) => (
            <button
              key={p.key}
              onClick={() => setTypeFilter(p.key as typeof typeFilter)}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${typeFilter === p.key ? "bg-primary text-white" : "bg-[var(--bg)] text-[var(--muted)] hover:bg-[var(--border)]"}`}
            >
              {p.label}
            </button>
          ))}
        </div>

        {/* Stat cards */}
        <div className="px-5 py-3 border-b border-[var(--border)] grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-6 gap-3">
          {[
            { icon: "↑", label: "Total Cash In", value: totalIn, color: "text-primary" },
            { icon: "↓", label: "Total Cash Out", value: totalOut, color: "text-danger" },
            { icon: "↘", label: "Movement In", value: movementIn, color: "text-info" },
            { icon: "↗", label: "Movement Out", value: movementOut, color: "text-info" },
            { icon: "~", label: "Net Balance Change", value: totalIn - totalOut + movementIn - movementOut, color: totalIn - totalOut + movementIn - movementOut >= 0 ? "text-primary" : "text-danger" },
            { icon: "#", label: "Loaded Transactions", value: null, count: filteredRows.length, color: "text-[var(--text)]" },
          ].map((s) => (
            <div key={s.label} className="flex items-center gap-2.5">
              <div className="w-8 h-8 rounded-lg bg-[var(--bg)] flex items-center justify-center text-sm font-bold text-[var(--muted)]">{s.icon}</div>
              <div>
                <p className="text-xs text-[var(--muted)]">{s.label}</p>
                <p className={`text-sm font-bold tabular ${s.color}`}>
                  {s.count != null ? s.count : bal(s.value ?? 0)}
                </p>
              </div>
            </div>
          ))}
        </div>

        {/* Table */}
        <div className="flex-1 overflow-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-[var(--surface)] border-b border-[var(--border)]">
              <tr className="text-[var(--muted)]">
                <th className="text-left px-4 py-2.5 font-medium">Date ↓</th>
                <th className="text-left px-3 py-2.5 font-medium">Account</th>
                <th className="text-left px-3 py-2.5 font-medium">Category</th>
                <th className="text-left px-3 py-2.5 font-medium">Description</th>
                <th className="text-right px-3 py-2.5 font-medium">Cash In</th>
                <th className="text-right px-3 py-2.5 font-medium">Cash Out</th>
                <th className="text-right px-3 py-2.5 font-medium">Movement</th>
                <th className="text-right px-3 py-2.5 font-medium">Running Balance</th>
                <th className="px-3 py-2.5"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {isLoading && (
                <tr><td colSpan={9} className="text-center py-8 text-[var(--muted)]">Loading…</td></tr>
              )}
              {filteredRows.map((row) => (
                <tr
                  key={row.transaction_id}
                  onClick={() => { setSelectedRow(row); setEditingRow(null); }}
                  className={`hover:bg-[var(--bg)] cursor-pointer transition-colors ${selectedRow?.transaction_id === row.transaction_id ? "bg-primary/5" : ""}`}
                >
                  <td className="px-4 py-2.5">
                    <div className="flex items-center gap-1.5">
                      {row.is_cycle_topup && <span className="text-warning text-xs">★</span>}
                      <div>
                        <p className="font-medium text-[var(--text)]">{new Date(row.date).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" })}</p>
                        <p className="text-[var(--muted)]">{new Date(row.date).toLocaleDateString("en-GB", { weekday: "short" })}</p>
                      </div>
                    </div>
                  </td>
                  <td className="px-3 py-2.5">
                    {row.is_transfer ? (
                      <div className="flex items-center gap-1.5">
                        <span className="w-5 h-5 rounded bg-blue-100 dark:bg-blue-900/30 text-primary flex items-center justify-center text-xs">⇄</span>
                        <span className="font-medium text-[var(--text)] whitespace-normal break-words">{row.account_name}</span>
                      </div>
                    ) : (
                      <div className="flex items-center gap-1.5">
                        <div className="w-5 h-5 rounded bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center text-xs">🏦</div>
                        <div>
                          <p className="font-medium text-[var(--text)] whitespace-normal break-words">{row.account_name}</p>
                        </div>
                      </div>
                    )}
                  </td>
                  <td className="px-3 py-2.5">
                    <div className="flex items-center gap-1.5">
                      <span className="text-sm">{row.is_transfer ? "⇄" : getCategoryIcon(categoryById[row.category_id ?? ""], row.transaction_name)}</span>
                      <span className="text-[var(--muted)] whitespace-normal break-words">{row.is_transfer ? "Internal Movement" : categoryById[row.category_id ?? ""]?.name ?? "Uncategorized"}</span>
                    </div>
                  </td>
                  <td className="px-3 py-2.5">
                    <div className="flex items-start gap-1.5">
                      <span className="block max-w-[340px] whitespace-normal break-words text-[var(--text)]">{row.transaction_name}</span>
                      {row.is_transfer && <Badge variant="blue">Movement</Badge>}
                      {row.is_cycle_topup && <Badge variant="yellow">Payroll</Badge>}
                    </div>
                  </td>
                  <td className="px-3 py-2.5 text-right tabular font-medium text-primary">
                    {!row.is_transfer && row.debit > 0 ? bal(row.debit) : "—"}
                  </td>
                  <td className="px-3 py-2.5 text-right tabular font-medium text-danger">
                    {!row.is_transfer && row.credit > 0 ? bal(row.credit) : "—"}
                  </td>
                  <td className="px-3 py-2.5 text-right tabular font-medium text-info">
                    {row.is_transfer ? bal(Math.max(row.debit, row.credit)) : "—"}
                  </td>
                  <td className="px-3 py-2.5 text-right tabular text-[var(--text)]">{bal(row.balance)}</td>
                  <td className="px-3 py-2.5 text-[var(--muted)]">⋯</td>
                </tr>
              ))}
              {!isLoading && filteredRows.length === 0 && (
                <tr><td colSpan={9} className="text-center py-8 text-[var(--muted)]">No transactions found</td></tr>
              )}
              {(hasNextPage || isFetchingNextPage) && (
                <tr ref={loadMoreRef}>
                  <td colSpan={9} className="py-4 text-center text-[var(--muted)]">
                    <span className="inline-flex items-center gap-2 text-xs">
                      <span className="h-4 w-4 rounded-full border-2 border-[var(--border)] border-t-primary animate-spin" />
                      {isFetchingNextPage ? "Loading more transactions..." : "Scroll to load more"}
                    </span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Result count */}
        <div className="px-5 py-3 border-t border-[var(--border)] bg-[var(--surface)] flex items-center justify-between text-xs text-[var(--muted)]">
          <span>Showing latest {filteredRows.length} loaded transactions</span>
          <span>{hasNextPage ? "More results available" : "End of current results"}</span>
        </div>
      </div>

      {/* Right detail panel */}
      {selectedRow && !txModal && (
        <div className="w-80 border-l border-[var(--border)] bg-[var(--surface)] flex flex-col">
          <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--border)]">
            <h3 className="font-semibold text-sm">Transaction Details</h3>
            <button onClick={() => setSelectedRow(null)} className="text-[var(--muted)] hover:text-[var(--text)]">✕</button>
          </div>
          <div className="flex gap-4 px-4 py-2 border-b border-[var(--border)]">
            <button type="button" className="text-xs font-semibold text-primary border-b-2 border-primary pb-1">Details</button>
            {hasHistory && <button type="button" disabled title="Audit history is available for this transaction" className="text-xs text-[var(--muted)] cursor-not-allowed">History</button>}
          </div>
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            <div className={`p-3 rounded-xl ${selectedRow.is_transfer ? "bg-blue-50 dark:bg-blue-900/20" : selectedRow.debit > 0 ? "bg-green-50 dark:bg-green-900/20" : "bg-red-50 dark:bg-red-900/20"}`}>
              <Badge variant={selectedRow.is_transfer ? "blue" : selectedRow.debit > 0 ? "green" : "red"}>{selectedRow.is_transfer ? movementLabel : selectedRow.debit > 0 ? "Cash In" : "Cash Out"}</Badge>
              <p className={`text-2xl font-bold tabular mt-1 ${selectedRow.is_transfer ? "text-info" : selectedRow.debit > 0 ? "text-primary" : "text-danger"}`}>
                {bal(selectedRow.debit > 0 ? selectedRow.debit : selectedRow.credit)}
              </p>
              <p className="text-xs text-[var(--muted)] mt-0.5">{selectedRow.transaction_name}</p>
              {selectedRow.is_transfer && (
                <p className="text-xs text-[var(--muted)] mt-1">
                  {movementDetail ? `${movementSourceName} -> ${movementTargetName}` : "Loading movement pair..."}
                </p>
              )}
            </div>
            {!selectedRow.is_transfer && (
              <div className="rounded-xl border border-[var(--border)] p-3 text-sm">
                <div className="flex items-center justify-between gap-2">
                  <div>
                    <p className="text-xs text-[var(--muted)] mb-1">Transaction Media</p>
                    <p className="font-medium">{receiptData?.receipt ? receiptData.receipt.original_filename : "No receipt attached"}</p>
                  </div>
                  {receiptData?.receipt && (
                    <a
                      href={`/api/transactions/${selectedRow.transaction_id}/receipt/view`}
                      target="_blank"
                      rel="noreferrer"
                      className="text-xs font-semibold text-primary hover:underline"
                    >
                      View
                    </a>
                  )}
                </div>
                <div className="mt-3 flex gap-2">
                  <label className="flex-1">
                    <input
                      type="file"
                      className="hidden"
                      accept="image/*,application/pdf"
                      disabled={receiptBusy}
                      onChange={(e) => uploadReceipt(e.target.files?.[0] ?? null)}
                    />
                    <span className="block w-full text-center rounded-lg border border-[var(--border)] px-3 py-1.5 text-xs font-semibold cursor-pointer hover:bg-[var(--bg)]">
                      {receiptData?.receipt ? "Replace" : "Upload"}
                    </span>
                  </label>
                  {receiptData?.receipt && (
                    <button
                      type="button"
                      onClick={deleteReceipt}
                      disabled={receiptBusy}
                      className="rounded-lg border border-red-200 px-3 py-1.5 text-xs font-semibold text-danger hover:bg-red-50 dark:hover:bg-red-900/20"
                    >
                      Delete
                    </button>
                  )}
                </div>
                {receiptErr && <p className="mt-2 text-xs text-danger">{receiptErr}</p>}
              </div>
            )}
            <div className="space-y-3 text-sm">
              <div><p className="text-xs text-[var(--muted)] mb-1">Date</p><p className="font-medium">{new Date(selectedRow.date).toLocaleDateString("en-GB", { day: "2-digit", month: "long", year: "numeric" })}</p></div>
              {selectedRow.is_transfer && movementDetail ? (
                <>
                  <div><p className="text-xs text-[var(--muted)] mb-1">From Account</p><p className="font-medium">{movementSourceName}</p></div>
                  <div><p className="text-xs text-[var(--muted)] mb-1">To Account</p><p className="font-medium">{movementTargetName}</p></div>
                  <div><p className="text-xs text-[var(--muted)] mb-1">Category</p><p className="font-medium">{movementLabel}</p></div>
                  <div>
                    <p className="text-xs text-[var(--muted)] mb-1">Movement Pair</p>
                    <p className="text-xs text-[var(--muted)]">An account movement changes owned-account balances only. It is excluded from cash in, cash out, spending, and income analysis.</p>
                  </div>
                </>
              ) : (
                <>
                  <div><p className="text-xs text-[var(--muted)] mb-1">Account</p><p className="font-medium">{selectedRow.account_name}</p></div>
                  <div><p className="text-xs text-[var(--muted)] mb-1">Category</p><p className="font-medium">{categoryById[selectedRow.category_id ?? ""]?.name ?? "Uncategorized"}</p></div>
                </>
              )}
              {(selectedRow.tags ?? []).length > 0 && (
                <div>
                  <p className="text-xs text-[var(--muted)] mb-1">Tags</p>
                  <div className="flex flex-wrap gap-1">{(selectedRow.tags ?? []).map((tag) => <Badge key={tag} variant="blue">{tag}</Badge>)}</div>
                </div>
              )}
              {selectedRow.notes && <div><p className="text-xs text-[var(--muted)] mb-1">Notes</p><p className="font-medium whitespace-pre-wrap">{selectedRow.notes}</p></div>}
              <div><p className="text-xs text-[var(--muted)] mb-1">Running Balance</p><p className="font-medium tabular">{bal(selectedRow.balance)}</p></div>
            </div>
          </div>
          {!selectedRow.is_transfer && (
            <div className="p-4 border-t border-[var(--border)] space-y-2">
              {deleteErr && <p className="text-xs text-danger">{deleteErr}</p>}
              <div className="flex gap-2">
                <Button size="sm" variant="danger" className="flex-1" onClick={deleteSelectedRow} disabled={deletingDetail}>
                  {deletingDetail ? "Deleting..." : "Delete"}
                </Button>
                <Button size="sm" variant="secondary" className="flex-1" onClick={() => setSelectedRow(null)}>Cancel</Button>
                <Button size="sm" variant="primary" className="flex-1" onClick={() => { setEditingRow(selectedRow); setTxModal(true); }}>Edit</Button>
              </div>
            </div>
          )}
          {selectedRow.is_transfer && (
            <div className="p-4 border-t border-[var(--border)] space-y-2">
              {deleteErr && <p className="text-xs text-danger">{deleteErr}</p>}
              <div className="flex gap-2">
                <Button size="sm" variant="danger" className="flex-1" onClick={deleteSelectedMovement} disabled={deletingDetail}>
                  {deletingDetail ? "Deleting..." : "Delete"}
                </Button>
                <Button size="sm" variant="secondary" className="flex-1" onClick={() => setSelectedRow(null)}>Close</Button>
                <Button
                  size="sm"
                  variant="primary"
                  className="flex-1"
                  disabled={!movementDetail}
                  onClick={() => { setEditingMovement(movementDetail); setMovementModal(true); }}
                >
                  Edit
                </Button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Transaction modal */}
      {txModal && (
        <TxModal
          open={txModal}
          onClose={() => { setTxModal(false); setEditingRow(null); }}
          accounts={accounts}
          categories={categories}
          editing={editingRow}
          onSaved={async () => {
            await invalidateFinancialQueries();
            setTxModal(false);
            setEditingRow(null);
          }}
        />
      )}

      {/* Account movement modal */}
      {movementModal && (
        <AccountMovementModal
          open={movementModal}
          onClose={() => { setMovementModal(false); setEditingMovement(null); }}
          accounts={accounts}
          editing={editingMovement}
          onSaved={async () => {
            await invalidateFinancialQueries();
            if (editingMovement?.transfer_id) {
              await qc.invalidateQueries({ queryKey: ["account-movement", editingMovement.transfer_id] });
            }
            setMovementModal(false);
            setEditingMovement(null);
            setSelectedRow(null);
          }}
        />
      )}
    </div>
  );
}

// Transaction modal
function TxModal({ open, onClose, accounts, categories, editing, onSaved }: any) {
  const [type, setType] = useState<"debit" | "credit">(editing?.debit > 0 ? "debit" : "credit");
  const [accountId, setAccountId] = useState(editing?.account_id ?? accounts[0]?.account_id ?? "");
  const [name, setName] = useState(editing?.transaction_name ?? "");
  const [amount, setAmount] = useState(editing ? (editing.debit > 0 ? editing.debit : editing.credit) : 0);
  const [date, setDate] = useState(editing?.date?.slice(0, 16) ?? new Date().toISOString().slice(0, 16));
  const [categoryId, setCategoryId] = useState(editing?.category_id ?? "");
  const [notes, setNotes] = useState(editing?.notes ?? "");
  const [tagsText, setTagsText] = useState<string>((editing?.tags ?? []).join(", "));
  const [isTopup, setIsTopup] = useState(editing?.is_cycle_topup ?? false);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  const filteredCats = categories.filter((c: Category) => !c.is_archived && (type === "debit" ? c.kind === "income" : c.kind === "expense"));

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true); setErr("");
    try {
      const tags = tagsText.split(",").map((tag) => tag.trim()).filter(Boolean);
      const payload = { account_id: accountId, transaction_type: type, transaction_name: name, amount, date, is_cycle_topup: isTopup, category_id: categoryId || null, notes: notes || null, tags, is_reviewed: true };
      if (editing) await api.put(`/transactions/${editing.transaction_id}`, payload);
      else await api.post("/transactions", payload);
      await onSaved();
    } catch (e: any) { setErr(e.message); }
    finally { setLoading(false); }
  }

  async function handleDelete() {
    if (!editing || !confirm("Delete this transaction?")) return;
    setLoading(true);
    try { await api.del(`/transactions/${editing.transaction_id}`); await onSaved(); }
    catch (e: any) { setErr(e.message); setLoading(false); }
  }

  return (
    <Modal open={open} onClose={onClose} title={editing ? "Edit Transaction" : "Add Transaction"}>
      <form onSubmit={handleSubmit} className="space-y-3">
        <div className="flex rounded-lg overflow-hidden border border-[var(--border)]">
          {(["credit", "debit"] as const).map((t) => (
            <button key={t} type="button" onClick={() => setType(t)}
              className={`flex-1 py-2 text-sm font-medium transition-colors ${type === t ? (t === "debit" ? "bg-primary text-white" : "bg-danger text-white") : "bg-[var(--surface)] text-[var(--muted)]"}`}>
              {t === "debit" ? "Cash In" : "Cash Out"}
            </button>
          ))}
        </div>
        <Select label="Account" value={accountId} onChange={(e) => setAccountId(e.target.value)}>
          {accounts.map((a: any) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
        </Select>
        <Input label="Description" value={name} onChange={(e) => setName(e.target.value)} required placeholder="e.g. Lunch, Salary" />
        <MoneyInput label="Amount" value={amount} onChange={setAmount} required />
        <Input label="Date & Time" type="datetime-local" value={date} onChange={(e) => setDate(e.target.value)} required />
        {filteredCats.length > 0 && (
          <Select label="Category" value={categoryId} onChange={(e) => setCategoryId(e.target.value)}>
            <option value="">— none —</option>
            {filteredCats.map((c: any) => <option key={c.category_id} value={c.category_id}>{c.name}</option>)}
          </Select>
        )}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-[var(--muted)]">Notes</label>
          <textarea value={notes} onChange={(e) => setNotes(e.target.value)} rows={2} placeholder="Optional note" className="w-full border border-[var(--border)] rounded px-3 py-2 text-sm bg-[var(--surface)] text-[var(--text)] resize-none" />
          <p className="text-xs text-[var(--muted)] text-right">{notes.length}/250</p>
        </div>
        <Input label="Tags" value={tagsText} onChange={(e) => setTagsText(e.target.value)} placeholder="comma separated, e.g. reimbursable, recurring" />
        {type === "debit" && (
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={isTopup} onChange={(e) => setIsTopup(e.target.checked)} className="rounded" />
            Mark as Payroll / Top-up
          </label>
        )}
        {err && <p className="text-xs text-danger">{err}</p>}
        <div className="flex gap-2 pt-1">
          {editing && <Button type="button" variant="danger" size="sm" onClick={handleDelete} disabled={loading}>Delete</Button>}
          <Button type="button" variant="secondary" size="sm" onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" size="sm" className="flex-1" disabled={loading}>{loading ? "Saving…" : "Save Changes"}</Button>
        </div>
        {editing && <p className="text-xs text-[var(--muted)] text-center">Created {new Date(editing.date).toLocaleString()}</p>}
      </form>
    </Modal>
  );
}

function toDatetimeLocal(value?: string) {
  const source = value ? new Date(value) : new Date();
  const local = new Date(source.getTime() - source.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 16);
}

function AccountMovementModal({ open, onClose, accounts, onSaved, editing }: any) {
  const [fromId, setFromId] = useState(accounts[0]?.account_id ?? "");
  const [toId, setToId] = useState(accounts[1]?.account_id ?? "");
  const [amount, setAmount] = useState(0);
  const [date, setDate] = useState(toDatetimeLocal());
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!open) return;
    if (editing) {
      setFromId(editing.source_account_id ?? "");
      setToId(editing.target_account_id ?? "");
      setAmount(Number(editing.amount ?? 0));
      setDate(toDatetimeLocal(editing.date));
    } else {
      setFromId(accounts[0]?.account_id ?? "");
      setToId(accounts[1]?.account_id ?? "");
      setAmount(0);
      setDate(toDatetimeLocal());
    }
    setErr("");
    setLoading(false);
  }, [open, editing, accounts]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (fromId === toId) return setErr("Source and target must differ");
    if (!fromId || !toId) return setErr("Choose source and target accounts");
    if (!amount || amount <= 0) return setErr("Amount must be greater than zero");
    setLoading(true); setErr("");
    const payload = { source_account_id: fromId, target_account_id: toId, amount, date };
    try {
      if (editing) await api.put(`/account-movements/${editing.transfer_id}`, payload);
      else await api.post("/account-movements", payload);
      await onSaved();
    }
    catch (e: any) { setErr(e.message); setLoading(false); }
  }

  return (
    <Modal open={open} onClose={onClose} title={editing ? "Edit Account Movement" : "Move Between Accounts"}>
      <form onSubmit={handleSubmit} className="space-y-3">
        <Select label="From Account" value={fromId} onChange={(e) => setFromId(e.target.value)}>
          {accounts.map((a: any) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
        </Select>
        <Select label="To Account" value={toId} onChange={(e) => setToId(e.target.value)}>
          {accounts.map((a: any) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
        </Select>
        <MoneyInput label="Amount" value={amount} onChange={setAmount} required />
        <Input label="Date & Time" type="datetime-local" value={date} onChange={(e) => setDate(e.target.value)} required />
        <p className="text-xs text-[var(--muted)]">This only moves money between owned accounts. It does not count as cash in, cash out, spending, income, or allocation progress.</p>
        {err && <p className="text-xs text-danger">{err}</p>}
        <div className="flex gap-2 pt-1">
          <Button type="button" variant="secondary" className="flex-1" onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" className="flex-1" disabled={loading}>
            {loading ? "Saving..." : (editing ? "Save Movement" : "Move Money")}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
