"use client";
import { useState, useEffect, useRef, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { TransactionModal } from "@/components/ui/TransactionModal";
import { SwitchModal } from "@/components/ui/SwitchModal";
import { ExportModal } from "@/components/ui/ExportModal";
import { Button } from "@/components/ui/Button";
import type { LedgerResponse, LedgerRow, Category } from "@/types/domain";

export default function LedgerPage() {
  const { accounts, paydayDay, hideBalances } = useAppCtx();
  const [scope, setScope] = useState<"all" | "account">("all");
  const [accountId, setAccountId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [cursor, setCursor] = useState<string | null>(null);
  const [rows, setRows] = useState<LedgerRow[]>([]);
  const [hasMore, setHasMore] = useState(true);
  const [loading, setLoading] = useState(false);
  const [totalAsset, setTotalAsset] = useState(0);
  const sentinelRef = useRef<HTMLDivElement>(null);

  const [txModal, setTxModal] = useState(false);
  const [editingRow, setEditingRow] = useState<LedgerRow | null>(null);
  const [switchModal, setSwitchModal] = useState(false);
  const [exportModal, setExportModal] = useState(false);

  const { data: categoriesData } = useQuery<{ categories: Category[] }>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
  });
  const categories = categoriesData?.categories ?? [];

  const fetchPage = useCallback(async (reset = false) => {
    if (loading) return;
    setLoading(true);
    try {
      const body: Record<string, unknown> = {
        scope,
        account_id: accountId,
        limit: 50,
        order: "desc",
        q: search || null,
        cursor: reset ? null : cursor,
        include_switch: true,
      };
      const data = await api.post<LedgerResponse>("/ledger", body);
      setRows((prev) => (reset ? data.rows : [...prev, ...data.rows]));
      setHasMore(data.paging.has_more);
      setCursor(data.paging.next_cursor ?? null);
      // total asset from summary is fetched separately; use first row balance as proxy
    } finally {
      setLoading(false);
    }
  }, [scope, accountId, search, cursor, loading]);

  // Reset on filter change
  useEffect(() => {
    setCursor(null);
    setRows([]);
    setHasMore(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope, accountId, search]);

  useEffect(() => {
    if (rows.length === 0 && !loading) fetchPage(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope, accountId, search]);

  // Infinite scroll
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting && hasMore && !loading) fetchPage(); },
      { threshold: 0.1 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [hasMore, loading, fetchPage]);

  function openEdit(row: LedgerRow) {
    if (row.is_transfer) return; // transfers use switch endpoint
    setEditingRow(row);
    setTxModal(true);
  }

  const bal = (n: number) => hideBalances ? "••••" : fmtMoney(n);

  return (
    <div className="flex flex-col h-[calc(100vh-49px)]">
      {/* Toolbar */}
      <div className="px-4 py-2 border-b border-[var(--border)] bg-[var(--surface)] flex flex-wrap gap-2 items-center">
        <select
          className="border border-[var(--border)] rounded px-2 py-1.5 text-sm bg-[var(--surface)] text-[var(--text)]"
          value={scope === "all" ? "all" : accountId ?? "all"}
          onChange={(e) => {
            if (e.target.value === "all") { setScope("all"); setAccountId(null); }
            else { setScope("account"); setAccountId(e.target.value); }
          }}
        >
          <option value="all">All Accounts</option>
          {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
        </select>
        <input
          type="search"
          placeholder="Search…"
          className="border border-[var(--border)] rounded px-2 py-1.5 text-sm flex-1 min-w-32 bg-[var(--surface)] text-[var(--text)]"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <div className="flex gap-1 ml-auto">
          <Button size="sm" variant="secondary" onClick={() => setExportModal(true)}>Export</Button>
          <Button size="sm" variant="secondary" onClick={() => setSwitchModal(true)}>Switch</Button>
          <Button size="sm" variant="primary" onClick={() => { setEditingRow(null); setTxModal(true); }}>+ Add</Button>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-[var(--surface)] border-b border-[var(--border)] text-[var(--muted)] text-xs uppercase">
            <tr>
              <th className="px-3 py-2 text-left">Date</th>
              {scope === "all" && <th className="px-3 py-2 text-left">Account</th>}
              <th className="px-3 py-2 text-left">Description</th>
              <th className="px-3 py-2 text-right">In</th>
              <th className="px-3 py-2 text-right">Out</th>
              <th className="px-3 py-2 text-right">Balance</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {rows.map((row) => (
              <tr
                key={row.transaction_id}
                onClick={() => openEdit(row)}
                className={`hover:bg-[var(--bg)] cursor-pointer transition-colors ${row.is_transfer ? "opacity-60" : ""}`}
              >
                <td className="px-3 py-2 whitespace-nowrap text-[var(--muted)] text-xs">
                  {new Date(row.date).toLocaleDateString("id-ID", { day: "2-digit", month: "short" })}
                  <span className="ml-1 text-[10px]">{new Date(row.date).toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit" })}</span>
                </td>
                {scope === "all" && (
                  <td className="px-3 py-2 text-xs text-[var(--muted)] max-w-[80px] truncate">{row.account_name}</td>
                )}
                <td className="px-3 py-2 max-w-[200px] truncate">
                  {row.is_transfer && <span className="text-[var(--muted)] mr-1 text-xs">⇄</span>}
                  {row.is_cycle_topup && <span className="text-green-600 mr-1 text-xs">★</span>}
                  {row.transaction_name}
                </td>
                <td className="px-3 py-2 text-right text-green-600 font-medium tabular-nums">
                  {row.debit > 0 ? bal(row.debit) : ""}
                </td>
                <td className="px-3 py-2 text-right text-red-500 font-medium tabular-nums">
                  {row.credit > 0 ? bal(row.credit) : ""}
                </td>
                <td className="px-3 py-2 text-right font-medium tabular-nums text-[var(--text)]">
                  {bal(row.balance)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {loading && <div className="text-center py-4 text-sm text-[var(--muted)]">Loading…</div>}
        {!loading && rows.length === 0 && (
          <div className="text-center py-12 text-[var(--muted)] text-sm">No transactions found.</div>
        )}
        <div ref={sentinelRef} className="h-1" />
      </div>

      {/* Modals */}
      <TransactionModal
        open={txModal}
        onClose={() => { setTxModal(false); setEditingRow(null); setRows([]); fetchPage(true); }}
        accounts={accounts}
        categories={categories}
        editing={editingRow}
        defaultAccountId={accountId ?? accounts[0]?.account_id}
      />
      <SwitchModal
        open={switchModal}
        onClose={() => { setSwitchModal(false); setRows([]); fetchPage(true); }}
        accounts={accounts}
      />
      <ExportModal
        open={exportModal}
        onClose={() => setExportModal(false)}
        accounts={accounts}
        paydayDay={paydayDay}
      />
    </div>
  );
}
