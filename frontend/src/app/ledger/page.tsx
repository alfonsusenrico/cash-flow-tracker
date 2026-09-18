"use client";

import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";

interface TransactionItem {
  id: string;
  account_id: string;
  account_name: string;
  category_id: string | null;
  category_name: string | null;
  category_icon: string | null;
  category_color: string | null;
  goal_id: string | null;
  goal_name: string | null;
  obligation_id: string | null;
  obligation_name: string | null;
  type: "expense" | "income";
  amount: number;
  notes: string | null;
  date: string;
  receipt_path: string | null;
  created_at: string;
  is_excluded_from_budget?: boolean;
  partner_id?: string;
  is_consolidated_transfer?: boolean;
  target_account_name?: string;
  target_account_id?: string;
}

function consolidateTransactions(
  items: TransactionItem[],
  enabled: boolean
): TransactionItem[] {
  if (!enabled) {
    return items.map((t) => {
      const isMovement =
        t.category_name === "Internal Movement" ||
        !!t.is_excluded_from_budget ||
        (t.notes?.toLowerCase().includes("pindah saldo") ?? false);
      if (isMovement && t.notes && t.notes.includes("→")) {
        const parts = t.notes.split("→");
        if (parts.length > 1) {
          const target = parts[1].replace(/[()]/g, "").trim();
          return { ...t, target_account_name: target };
        }
      }
      return t;
    });
  }

  const result: TransactionItem[] = [];
  const consumed = new Set<string>();

  for (let i = 0; i < items.length; i++) {
    const t1 = items[i];
    if (consumed.has(t1.id)) continue;

    const isT1Movement =
      t1.category_name === "Internal Movement" ||
      !!t1.is_excluded_from_budget ||
      (t1.notes?.toLowerCase().includes("pindah saldo") ?? false);

    if (!isT1Movement) {
      result.push(t1);
      continue;
    }

    let bestIndex = -1;
    let minDiffMs = Infinity;

    for (let j = i + 1; j < items.length; j++) {
      const t2 = items[j];
      if (consumed.has(t2.id)) continue;

      const isT2Movement =
        t2.category_name === "Internal Movement" ||
        !!t2.is_excluded_from_budget ||
        (t2.notes?.toLowerCase().includes("pindah saldo") ?? false);

      if (!isT2Movement) continue;
      if (t1.type === t2.type) continue;
      if (t1.amount !== t2.amount) continue;
      if (t1.account_id === t2.account_id) continue;

      const diffMs = Math.abs(new Date(t1.date).getTime() - new Date(t2.date).getTime());
      if (diffMs <= 10 * 60 * 1000 && diffMs < minDiffMs) {
        minDiffMs = diffMs;
        bestIndex = j;
        if (
          t1.notes &&
          t2.notes &&
          (t1.notes === t2.notes || t1.notes.includes(t2.notes) || t2.notes.includes(t1.notes))
        ) {
          break;
        }
      }
    }

    if (bestIndex !== -1) {
      const t2 = items[bestIndex];
      consumed.add(t1.id);
      consumed.add(t2.id);

      const outTx = t1.type === "expense" ? t1 : t2;
      const inTx = t1.type === "expense" ? t2 : t1;

      result.push({
        ...outTx,
        partner_id: inTx.id,
        is_consolidated_transfer: true,
        account_name: outTx.account_name,
        target_account_name: inTx.account_name,
        target_account_id: inTx.account_id,
      });
    } else {
      let parsedTarget: string | undefined;
      if (t1.notes && t1.notes.includes("→")) {
        const parts = t1.notes.split("→");
        if (parts.length > 1) {
          parsedTarget = parts[1].replace(/[()]/g, "").trim();
        }
      }
      result.push({
        ...t1,
        target_account_name: parsedTarget,
      });
    }
  }

  return result;
}

export default function LedgerPage() {
  const qc = useQueryClient();
  const { openQuickAdd, bal } = useAppCtx();

  // Filters state
  const [searchQuery, setSearchQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<string>("all");
  const [accountFilter, setAccountFilter] = useState<string>("all");
  const [categoryFilter, setCategoryFilter] = useState<string>("all");
  const [page, setPage] = useState(0);
  const pageSize = 50;

  // Selected for Edit/Detail
  const [editingTx, setEditingTx] = useState<TransactionItem | null>(null);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);

  // Edit form state
  const [editAmount, setEditAmount] = useState("");
  const [editNotes, setEditNotes] = useState("");
  const [editAccountId, setEditAccountId] = useState("");
  const [editTargetAccountId, setEditTargetAccountId] = useState("");
  const [editCategoryId, setEditCategoryId] = useState("");
  const [editGoalId, setEditGoalId] = useState("");
  const [editObligationId, setEditObligationId] = useState("");
  const [editDate, setEditDate] = useState("");
  const [editError, setEditError] = useState("");

  // Fetch Accounts
  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });
  const accounts = accountsData?.accounts ?? [];

  // Fetch Categories
  const { data: categoriesData } = useQuery<{ categories: any[] }>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
  });
  const categories = categoriesData?.categories ?? [];

  // Fetch Goals & Obligations
  const { data: goalsData } = useQuery<{ goals: any[] }>({
    queryKey: ["goals"],
    queryFn: () => api.get("/goals"),
  });
  const goals = goalsData?.goals ?? [];

  const { data: obligationsData } = useQuery<{ obligations: any[] }>({
    queryKey: ["obligations"],
    queryFn: () => api.get("/obligations"),
  });
  const obligations = obligationsData?.obligations ?? [];

  // Build query params
  const queryParams = useMemo(() => {
    const p = new URLSearchParams();
    p.set("limit", pageSize.toString());
    p.set("offset", (page * pageSize).toString());
    if (searchQuery.trim()) p.set("q", searchQuery.trim());
    if (typeFilter !== "all") p.set("type", typeFilter);
    if (accountFilter !== "all") p.set("account_id", accountFilter);
    if (categoryFilter !== "all") p.set("category_id", categoryFilter);
    return p.toString();
  }, [searchQuery, typeFilter, accountFilter, categoryFilter, page]);

  // Fetch Transactions
  const { data: txData, isLoading } = useQuery<{
    ok: boolean;
    total: number;
    transactions: TransactionItem[];
  }>({
    queryKey: ["transactions-ledger", queryParams],
    queryFn: () => api.get(`/transactions?${queryParams}`),
  });

  const transactions = useMemo(() => txData?.transactions ?? [], [txData?.transactions]);
  const totalCount = txData?.total ?? 0;
  const totalPages = Math.ceil(totalCount / pageSize);

  // Consolidate paired internal movements into single visual entries in all-account ledger
  const displayTransactions = useMemo(() => {
    return consolidateTransactions(
      transactions,
      accountFilter === "all" && typeFilter === "all"
    );
  }, [transactions, accountFilter, typeFilter]);

  // Compute page totals (excluding internal movements so totals reflect real cash flow)
  const pageInflow = transactions
    .filter(
      (t) =>
        t.type === "income" &&
        !t.is_excluded_from_budget &&
        t.category_name !== "Internal Movement"
    )
    .reduce((sum, t) => sum + t.amount, 0);
  const pageOutflow = transactions
    .filter(
      (t) =>
        t.type === "expense" &&
        !t.is_excluded_from_budget &&
        t.category_name !== "Internal Movement"
    )
    .reduce((sum, t) => sum + t.amount, 0);

  // Open Edit Modal
  const handleOpenEdit = (tx: TransactionItem) => {
    setEditingTx(tx);
    setEditAmount(formatNumberWithDots(tx.amount));
    setEditNotes(tx.notes || "");
    setEditAccountId(tx.account_id);
    setEditTargetAccountId(tx.target_account_id || "");
    setEditCategoryId(tx.category_id || "");
    setEditGoalId(tx.goal_id || "");
    setEditObligationId(tx.obligation_id || "");
    setEditDate(tx.date ? toDatetimeLocal(tx.date) : "");
    setEditError("");
  };

  // Update mutation (handles both normal transactions and synchronized paired movements)
  const updateMutation = useMutation({
    mutationFn: async () => {
      if (!editingTx) return;
      const amt = parseInt(editAmount.replace(/[^0-9]/g, ""), 10);
      if (!amt || amt <= 0) throw new Error("Nominal harus lebih dari 0");
      if (!editAccountId) throw new Error("Pilih rekening");

      const isoDate = editDate
        ? (localDatetimeToISO(editDate) || new Date(editDate).toISOString())
        : undefined;

      if (editingTx.is_consolidated_transfer && editingTx.partner_id) {
        if (editTargetAccountId && editAccountId === editTargetAccountId) {
          throw new Error("Rekening asal dan tujuan tidak boleh sama");
        }
        await Promise.all([
          api.patch(`/transactions/${editingTx.id}`, {
            amount: amt,
            notes: editNotes.trim() || null,
            account_id: editAccountId,
            date: isoDate,
          }),
          api.patch(`/transactions/${editingTx.partner_id}`, {
            amount: amt,
            notes: editNotes.trim() || null,
            account_id: editTargetAccountId || editingTx.target_account_id,
            date: isoDate,
          }),
        ]);
        return;
      }

      return api.patch(`/transactions/${editingTx.id}`, {
        amount: amt,
        notes: editNotes.trim() || null,
        account_id: editAccountId,
        category_id: editCategoryId || null,
        goal_id: editGoalId || null,
        obligation_id: editObligationId || null,
        date: isoDate,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["obligations"] });
      setEditingTx(null);
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal memperbarui transaksi");
    },
  });

  // Delete mutation (handles single transaction and atomic paired movements)
  const deleteMutation = useMutation({
    mutationFn: async (tx: TransactionItem) => {
      await api.del(`/transactions/${tx.id}`);
      if (tx.partner_id) {
        await api.del(`/transactions/${tx.partner_id}`);
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["obligations"] });
      setEditingTx(null);
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal menghapus transaksi");
    },
  });

  return (
    <div className="space-y-6">
      {/* 1. Header & Summary Ribbon */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-[var(--border)]">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
              Riwayat & Catatan Keuangan
            </span>
            <span className="h-1 w-1 rounded-full bg-[var(--muted)]" />
            <span className="text-xs text-emerald-500 font-semibold">
              {totalCount} Total Transaksi
            </span>
          </div>
          <h1 className="text-2xl font-extrabold tracking-tight text-[var(--text)] mt-1">
            Riwayat Transaksi
          </h1>
        </div>

        <div className="flex items-center gap-2 self-start sm:self-auto">
          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="flex items-center gap-1.5 px-3.5 py-2.5 rounded-2xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-xs font-semibold text-[var(--text)] shadow-xs transition-transform active:scale-95"
            title="Kelola Transaksi Rutin & Otomatis"
          >
            <Icon name="repeat" className="h-3.5 w-3.5 text-blue-500" />
            <span>Rutin</span>
          </button>

          <button
            type="button"
            onClick={() => openQuickAdd()}
            className="flex items-center gap-2 px-4 py-2.5 rounded-2xl bg-income hover:bg-income-hover text-white text-xs font-bold shadow-xs transition-transform active:scale-95"
          >
            <Icon name="plus" className="h-4 w-4" />
            <span>Catat Transaksi</span>
            <kbd className="hidden sm:inline-block rounded bg-black/20 px-1.5 py-0.5 text-[9px] font-mono text-white/90">
              N
            </kbd>
          </button>
        </div>
      </div>

      {/* Pending Scheduled & Recurring Banner */}
      <PendingScheduledBanner onOpenRulesManager={() => setRecurringModalOpen(true)} />

      {/* 2. Ledger Volume Summary Bar */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs">
          <span className="text-xs text-[var(--muted)] font-medium">Uang Masuk (Halaman Ini)</span>
          <div className="text-xl font-bold tabular tracking-tight text-emerald-500 mt-1 select-all">
            +{bal(pageInflow)}
          </div>
        </div>
        <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs">
          <span className="text-xs text-[var(--muted)] font-medium">Uang Keluar (Halaman Ini)</span>
          <div className="text-xl font-bold tabular tracking-tight text-rose-500 mt-1 select-all">
            -{bal(pageOutflow)}
          </div>
        </div>
        <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs">
          <span className="text-xs text-[var(--muted)] font-medium">Selisih Bersih (Halaman Ini)</span>
          <div
            className={cn(
              "text-xl font-bold tabular tracking-tight mt-1 select-all",
              pageInflow - pageOutflow >= 0 ? "text-emerald-500" : "text-rose-500"
            )}
          >
            {pageInflow - pageOutflow >= 0 ? "+" : ""}
            {bal(pageInflow - pageOutflow)}
          </div>
        </div>
      </div>

      {/* 3. Search & Filter Bar */}
      <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs space-y-3">
        <div className="grid grid-cols-1 sm:grid-cols-12 gap-3 items-center">
          {/* Search Input */}
          <div className="sm:col-span-6 relative">
            <Icon
              name="search"
              className="h-4 w-4 absolute left-3.5 top-1/2 -translate-y-1/2 text-[var(--muted)]"
            />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value);
                setPage(0);
              }}
              placeholder="Cari catatan, kategori, toko, target..."
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] pl-10 pr-4 py-2 text-xs text-[var(--text)] placeholder-[var(--muted)] focus:outline-none focus:border-income transition-colors"
            />
            {searchQuery && (
              <button
                type="button"
                onClick={() => setSearchQuery("")}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-[var(--muted)] hover:text-[var(--text)]"
              >
                ✕
              </button>
            )}
          </div>

          {/* Account Filter */}
          <div className="sm:col-span-3">
            <select
              value={accountFilter}
              onChange={(e) => {
                setAccountFilter(e.target.value);
                setPage(0);
              }}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] focus:outline-none focus:border-income"
            >
              <option value="all">Semua Rekening</option>
              <AccountSelectOptions accounts={accounts} allowParentSelection={true} />
            </select>
          </div>

          {/* Category Filter */}
          <div className="sm:col-span-3">
            <select
              value={categoryFilter}
              onChange={(e) => {
                setCategoryFilter(e.target.value);
                setPage(0);
              }}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] focus:outline-none focus:border-income"
            >
              <option value="all">Semua Kategori</option>
              {categories.map((cat) => (
                <option key={cat.id} value={cat.id}>
                  {cat.name} ({cat.kind === "income" ? "Masuk" : "Keluar"})
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Type Filter Chips */}
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <span className="text-xs text-[var(--muted)] font-medium mr-1">Jenis:</span>
          {[
            { key: "all", label: "Semua" },
            { key: "expense", label: "Uang Keluar" },
            { key: "income", label: "Uang Masuk" },
          ].map(({ key, label }) => (
            <button
              key={key}
              type="button"
              onClick={() => {
                setTypeFilter(key);
                setPage(0);
              }}
              className={cn(
                "px-3 py-1 rounded-xl text-xs font-semibold transition-colors",
                typeFilter === key
                  ? "bg-income text-white shadow-2xs"
                  : "bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)]"
              )}
            >
              {label}
            </button>
          ))}

          {(searchQuery || typeFilter !== "all" || accountFilter !== "all" || categoryFilter !== "all") && (
            <button
              type="button"
              onClick={() => {
                setSearchQuery("");
                setTypeFilter("all");
                setAccountFilter("all");
                setCategoryFilter("all");
                setPage(0);
              }}
              className="ml-auto text-xs text-income hover:underline font-medium"
            >
              Hapus filter
            </button>
          )}
        </div>
      </div>

      {/* 4. Ledger Data Table */}
      <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] shadow-xs overflow-hidden">
        {isLoading ? (
          <div className="py-20 text-center text-xs text-[var(--muted)] animate-pulse">
            Memuat riwayat transaksi...
          </div>
        ) : displayTransactions.length === 0 ? (
          <div className="py-20 text-center space-y-2">
            <p className="text-sm font-semibold text-[var(--text)]">Belum ada transaksi ditemukan</p>
            <p className="text-xs text-[var(--muted)]">
              Coba sesuaikan kata kunci pencarian atau filter Anda.
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs text-left">
              <thead>
                <tr className="border-b border-[var(--border)] text-[var(--muted)] uppercase text-[10px] bg-[var(--surface-raised)]/40">
                  <th className="py-3 px-4 font-semibold">Tanggal</th>
                  <th className="py-3 px-4 font-semibold">Jenis</th>
                  <th className="py-3 px-4 font-semibold">Keterangan / Catatan</th>
                  <th className="py-3 px-4 font-semibold">Kategori</th>
                  <th className="py-3 px-4 font-semibold">Rekening</th>
                  <th className="py-3 px-4 font-semibold">Target / Tagihan</th>
                  <th className="py-3 px-4 font-semibold text-right">Nominal</th>
                  <th className="py-3 px-4 font-semibold text-right">Aksi</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {displayTransactions.map((tx) => {
                  const isMovement =
                    tx.category_name === "Internal Movement" ||
                    !!tx.is_excluded_from_budget ||
                    (tx.notes?.toLowerCase().includes("pindah saldo") ?? false);
                  const isIncome = tx.type === "income";

                  return (
                    <tr
                      key={tx.id}
                      onClick={() => handleOpenEdit(tx)}
                      className="hover:bg-[var(--surface-raised)]/60 transition-colors cursor-pointer group"
                    >
                      {/* Date */}
                      <td className="py-3.5 px-4 text-[var(--muted)] whitespace-nowrap">
                        {new Date(tx.date).toLocaleDateString("id-ID", {
                          month: "short",
                          day: "numeric",
                          year: "numeric",
                        })}
                      </td>

                      {/* Type Badge */}
                      <td className="py-3.5 px-4 whitespace-nowrap">
                        {tx.is_consolidated_transfer ? (
                          <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-sky-500/10 text-sky-500 border border-sky-500/20 tracking-wide">
                            <span className="text-xs leading-none">↔️</span>
                            <span>Pindah Saldo</span>
                          </span>
                        ) : (
                          <span
                            className={cn(
                              "px-2 py-0.5 rounded-md text-[10px] font-bold uppercase",
                              isMovement
                                ? "bg-sky-500/10 text-sky-500"
                                : isIncome
                                ? "bg-income/10 text-income"
                                : "bg-expense/10 text-expense"
                            )}
                          >
                            {isMovement
                              ? isIncome
                                ? "Pindah Saldo (Masuk)"
                                : "Pindah Saldo (Keluar)"
                              : isIncome
                              ? "Uang Masuk"
                              : "Uang Keluar"}
                          </span>
                        )}
                      </td>

                      {/* Notes / Description */}
                      <td className="py-3.5 px-4 font-medium text-[var(--text)] group-hover:text-income transition-colors max-w-xs truncate">
                        {tx.notes || (isMovement ? "Pindah Saldo" : tx.category_name || "Umum")}
                      </td>

                      {/* Category */}
                      <td className="py-3.5 px-4 whitespace-nowrap">
                        {isMovement || tx.category_name ? (
                          <span className="inline-flex items-center gap-1.5 text-[var(--text)] font-medium">
                            <span
                              className="h-2 w-2 rounded-full"
                              style={{
                                backgroundColor: isMovement
                                  ? "#0ea5e9"
                                  : tx.category_color || "#3b82f6",
                              }}
                            />
                            <span>{isMovement ? "Pindah Saldo" : tx.category_name}</span>
                          </span>
                        ) : (
                          <span className="text-[var(--muted)]">-</span>
                        )}
                      </td>

                      {/* Account (Source) */}
                      <td className="py-3.5 px-4 whitespace-nowrap font-medium text-[var(--text)]">
                        {tx.account_name}
                      </td>

                      {/* Target Account or Linked Goal/Debt */}
                      <td className="py-3.5 px-4 whitespace-nowrap">
                        {tx.is_consolidated_transfer && tx.target_account_name ? (
                          <span className="inline-flex items-center gap-1 text-[11px] font-semibold text-sky-500">
                            <span className="text-xs">→</span>
                            <span>{tx.target_account_name}</span>
                          </span>
                        ) : tx.target_account_name ? (
                          <span className="inline-flex items-center gap-1 text-[11px] font-medium text-sky-500">
                            <span className="text-xs">{isIncome ? "←" : "→"}</span>
                            <span>{tx.target_account_name}</span>
                          </span>
                        ) : tx.goal_name ? (
                          <span className="inline-flex items-center gap-1 text-[11px] text-emerald-500 font-semibold">
                            🎯 {tx.goal_name}
                          </span>
                        ) : tx.obligation_name ? (
                          <span className="inline-flex items-center gap-1 text-[11px] text-rose-500 font-semibold">
                            💳 {tx.obligation_name}
                          </span>
                        ) : (
                          <span className="text-[var(--muted)]">-</span>
                        )}
                      </td>

                      {/* Amount */}
                      <td
                        className={cn(
                          "py-3.5 px-4 text-right font-bold tabular whitespace-nowrap",
                          tx.is_consolidated_transfer
                            ? "text-[var(--text)]"
                            : isMovement
                            ? "text-sky-500"
                            : isIncome
                            ? "text-income"
                            : "text-expense"
                        )}
                      >
                        {tx.is_consolidated_transfer ? "" : isIncome ? "+" : "-"}
                        {bal(tx.amount)}
                      </td>

                      {/* Action */}
                      <td className="py-3.5 px-4 text-right whitespace-nowrap">
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleOpenEdit(tx);
                          }}
                          className="px-2.5 py-1 rounded-lg border border-[var(--border)] text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--surface)] text-[11px]"
                        >
                          Ubah
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination Bar */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between p-4 border-t border-[var(--border)] bg-[var(--surface)] text-xs text-[var(--muted)]">
            <span>
              Menampilkan {page * pageSize + 1} - {Math.min((page + 1) * pageSize, totalCount)} dari {totalCount}
            </span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                disabled={page === 0}
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                className="px-3 py-1.5 rounded-xl border border-[var(--border)] hover:bg-[var(--surface-raised)] disabled:opacity-40"
              >
                Sebelumnya
              </button>
              <span className="text-[11px] px-2 font-semibold text-[var(--text)]">
                {page + 1} / {totalPages}
              </span>
              <button
                type="button"
                disabled={page >= totalPages - 1}
                onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                className="px-3 py-1.5 rounded-xl border border-[var(--border)] hover:bg-[var(--surface-raised)] disabled:opacity-40"
              >
                Berikutnya
              </button>
            </div>
          </div>
        )}
      </div>

      {/* 5. Transaction Edit Modal */}
      {editingTx && (
        <Modal
          open={Boolean(editingTx)}
          onClose={() => setEditingTx(null)}
          title={editingTx.is_consolidated_transfer ? "Detail & Ubah Pindah Saldo" : "Ubah Transaksi"}
        >
          <div className="space-y-4 pt-2 text-xs">
            {editError && (
              <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {editError}
              </div>
            )}

            {editingTx.is_consolidated_transfer ? (
              <>
                <div className="p-3.5 rounded-2xl bg-sky-500/10 border border-sky-500/20 text-sky-500 space-y-1">
                  <div className="flex items-center gap-1.5 font-semibold text-xs">
                    <span className="text-sm">↔️</span>
                    <span>Pemindahan Saldo Internal</span>
                  </div>
                  <p className="text-[11px] text-[var(--muted)]">
                    Transaksi ini menghubungkan dua rekening (
                    <span className="text-[var(--text)] font-semibold">{editingTx.account_name}</span> →{" "}
                    <span className="text-[var(--text)] font-semibold">{editingTx.target_account_name || "Tujuan"}</span>
                    ). Perubahan nominal atau penghapusan akan diterapkan secara bersamaan ke kedua rekening.
                  </p>
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Nominal Transfer (IDR)</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={editAmount}
                    onChange={(e) => setEditAmount(formatNumberWithDots(e.target.value))}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
                  />
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  <div>
                    <label className="text-xs font-medium text-[var(--muted)] block mb-1">Rekening Asal (Sumber)</label>
                    <select
                      value={editAccountId}
                      onChange={(e) => setEditAccountId(e.target.value)}
                      className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)]"
                    >
                      <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} />
                    </select>
                  </div>

                  <div>
                    <label className="text-xs font-medium text-[var(--muted)] block mb-1">Rekening Tujuan</label>
                    <select
                      value={editTargetAccountId}
                      onChange={(e) => setEditTargetAccountId(e.target.value)}
                      className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)]"
                    >
                      <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} />
                    </select>
                  </div>
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Tanggal & Waktu</label>
                  <input
                    type="datetime-local"
                    value={editDate}
                    onChange={(e) => setEditDate(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)]"
                  />
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Catatan</label>
                  <input
                    type="text"
                    value={editNotes}
                    onChange={(e) => setEditNotes(e.target.value)}
                    placeholder="Tambahkan catatan pemindahan..."
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)]"
                  />
                </div>
              </>
            ) : (
              <>
                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Nominal Uang (IDR)</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={editAmount}
                    onChange={(e) => setEditAmount(formatNumberWithDots(e.target.value))}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
                  />
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Rekening / Dompet</label>
                  <select
                    value={editAccountId}
                    onChange={(e) => setEditAccountId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} />
                  </select>
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Kategori</label>
                  <select
                    value={editCategoryId}
                    onChange={(e) => setEditCategoryId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tanpa Kategori</option>
                    {categories
                      .filter((c) => c.kind === editingTx.type)
                      .map((cat) => (
                        <option key={cat.id} value={cat.id}>
                          {cat.name}
                        </option>
                      ))}
                  </select>
                </div>

                {/* Optional Goal Link */}
                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">
                    Hubungkan ke Target Tabungan (Opsional)
                  </label>
                  <select
                    value={editGoalId}
                    onChange={(e) => {
                      setEditGoalId(e.target.value);
                      if (e.target.value) setEditObligationId("");
                    }}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tidak ada</option>
                    {goals.map((g) => (
                      <option key={g.id} value={g.id}>
                        🎯 {g.name} ({bal(g.current_amount)} / {bal(g.target_amount)})
                      </option>
                    ))}
                  </select>
                </div>

                {/* Optional Obligation Link */}
                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">
                    Hubungkan ke Tagihan / Utang (Opsional)
                  </label>
                  <select
                    value={editObligationId}
                    onChange={(e) => {
                      setEditObligationId(e.target.value);
                      if (e.target.value) setEditGoalId("");
                    }}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tidak ada</option>
                    {obligations.map((o) => (
                      <option key={o.id} value={o.id}>
                        💳 {o.name} ({bal(o.remaining_amount)} tersisa)
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Tanggal & Waktu</label>
                  <input
                    type="datetime-local"
                    value={editDate}
                    onChange={(e) => setEditDate(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  />
                </div>

                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Catatan</label>
                  <input
                    type="text"
                    value={editNotes}
                    onChange={(e) => setEditNotes(e.target.value)}
                    placeholder="Tambahkan catatan..."
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  />
                </div>
              </>
            )}

            <div className="pt-4 flex items-center justify-between border-t border-[var(--border)]">
              <button
                type="button"
                onClick={() => {
                  if (
                    confirm(
                      editingTx.is_consolidated_transfer
                        ? "Hapus pemindahan saldo ini? Saldo kedua rekening akan dikembalikan."
                        : "Hapus transaksi ini? Saldo rekening dan progres target terkait akan dikembalikan."
                    )
                  ) {
                    deleteMutation.mutate(editingTx);
                  }
                }}
                disabled={deleteMutation.isPending}
                className="px-3.5 py-2 rounded-btn bg-rose-500/10 text-rose-500 border border-rose-500/20 font-semibold hover:bg-rose-500/20 disabled:opacity-50 transition-all active:scale-95"
              >
                {deleteMutation.isPending ? "Menghapus..." : "Hapus"}
              </button>

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => setEditingTx(null)}
                  className="btn-secondary"
                >
                  Batal
                </button>
                <button
                  type="button"
                  disabled={updateMutation.isPending}
                  onClick={() => updateMutation.mutate()}
                  className="btn-primary"
                >
                  {updateMutation.isPending ? "Menyimpan..." : "Simpan Perubahan"}
                </button>
              </div>
            </div>
          </div>
        </Modal>
      )}

      {/* Recurring Rules Modal */}
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => setRecurringModalOpen(false)}
      />
    </div>
  );
}
