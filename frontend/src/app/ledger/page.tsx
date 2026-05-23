"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import type { LedgerResponse, LedgerRow, Account } from "@/types/domain";
import AppLayout from "@/components/layout/AppLayout";

export default function LedgerPage() {
  const [scope, setScope] = useState<"all" | "account">("all");
  const [accountId, setAccountId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [cursor, setCursor] = useState<string | null>(null);
  const [rows, setRows] = useState<LedgerRow[]>([]);
  const [hasMore, setHasMore] = useState(true);
  const [loading, setLoading] = useState(false);
  const sentinelRef = useRef<HTMLDivElement>(null);

  const { data: accountsData } = useQuery<{ accounts: Account[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const fetchPage = useCallback(
    async (reset = false) => {
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
      } finally {
        setLoading(false);
      }
    },
    [scope, accountId, search, cursor, loading],
  );

  // Reset on filter change
  useEffect(() => {
    setCursor(null);
    setRows([]);
    setHasMore(true);
    fetchPage(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope, accountId, search]);

  // Infinite scroll
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && hasMore && !loading) fetchPage();
      },
      { threshold: 0.1 },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [hasMore, loading, fetchPage]);

  return (
    <AppLayout>
      <div className="p-4 space-y-3">
        <div className="flex flex-wrap gap-2 items-center">
          <select
            className="border rounded px-2 py-1.5 text-sm dark:bg-gray-800 dark:border-gray-600"
            value={scope === "all" ? "all" : accountId ?? "all"}
            onChange={(e) => {
              if (e.target.value === "all") {
                setScope("all");
                setAccountId(null);
              } else {
                setScope("account");
                setAccountId(e.target.value);
              }
            }}
          >
            <option value="all">All Accounts</option>
            {accountsData?.accounts.map((a) => (
              <option key={a.account_id} value={a.account_id}>
                {a.account_name}
              </option>
            ))}
          </select>
          <input
            type="search"
            placeholder="Search transactions…"
            className="border rounded px-2 py-1.5 text-sm flex-1 min-w-40 dark:bg-gray-800 dark:border-gray-600"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>

        <div className="overflow-x-auto rounded-xl shadow">
          <table className="w-full text-sm bg-white dark:bg-gray-800">
            <thead className="bg-gray-50 dark:bg-gray-700 text-gray-500 text-xs uppercase">
              <tr>
                <th className="px-3 py-2 text-left">Date</th>
                {scope === "all" && <th className="px-3 py-2 text-left">Account</th>}
                <th className="px-3 py-2 text-left">Description</th>
                <th className="px-3 py-2 text-right">In</th>
                <th className="px-3 py-2 text-right">Out</th>
                <th className="px-3 py-2 text-right">Balance</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
              {rows.map((row) => (
                <tr
                  key={row.transaction_id}
                  className={`hover:bg-gray-50 dark:hover:bg-gray-700/50 ${row.is_transfer ? "opacity-60" : ""}`}
                >
                  <td className="px-3 py-2 whitespace-nowrap text-gray-500 text-xs">
                    {new Date(row.date).toLocaleDateString("id-ID", { day: "2-digit", month: "short" })}
                  </td>
                  {scope === "all" && (
                    <td className="px-3 py-2 text-xs text-gray-500 max-w-24 truncate">
                      {row.account_name}
                    </td>
                  )}
                  <td className="px-3 py-2 max-w-xs truncate">
                    {row.is_transfer && <span className="text-xs text-gray-400 mr-1">⇄</span>}
                    {row.transaction_name}
                  </td>
                  <td className="px-3 py-2 text-right text-green-600 font-medium">
                    {row.debit > 0 ? fmtMoney(row.debit) : ""}
                  </td>
                  <td className="px-3 py-2 text-right text-red-500 font-medium">
                    {row.credit > 0 ? fmtMoney(row.credit) : ""}
                  </td>
                  <td className="px-3 py-2 text-right font-medium">
                    {fmtMoney(row.balance)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {loading && (
            <div className="text-center py-4 text-sm text-gray-400">Loading…</div>
          )}
          <div ref={sentinelRef} className="h-1" />
        </div>
      </div>
    </AppLayout>
  );
}
