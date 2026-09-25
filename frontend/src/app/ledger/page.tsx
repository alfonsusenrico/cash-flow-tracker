"use client";

import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, uploadTransactionReceipt } from "@/lib/api";
import { cn, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { PendingScheduledBanner } from "@/components/recurring/PendingScheduledBanner";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { MobileLedgerFeed } from "@/components/ledger/MobileLedgerFeed";
import { InternalMovementModal } from "@/components/ui/InternalMovementModal";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { InfoHelp } from "@/components/ui/InfoHelp";
import { queryKeys } from "@/lib/queryKeys";
import { DebtAllocationEditor, allocationError, totalDebtPayments, type DebtAllocationValue } from "@/components/ui/DebtAllocationEditor";
import { consolidateLedgerMovements } from "@/lib/ledgerMovements";

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
  obligation_allocations?: { obligation_id: string; obligation_name: string; amount: number }[];
  type: "expense" | "income";
  kakeibo_type?: string | null;
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
  movement_id?: string | null;
  movement_role?: "outbound" | "inbound" | null;
  transfer_target_account_id?: string | null;
  transfer_target_account_name?: string | null;
  is_inferred_transfer?: boolean;
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
  const [selectionMode, setSelectionMode] = useState(false);
  const [selectedTransactions, setSelectedTransactions] = useState<TransactionItem[]>([]);
  const [mergePreview, setMergePreview] = useState<TransactionItem[] | null>(null);
  const [mergeError, setMergeError] = useState("");
  const [preparingMerge, setPreparingMerge] = useState(false);

  // Selected for Edit/Detail
  const [editingTx, setEditingTx] = useState<TransactionItem | null>(null);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);
  const [filterDrawerOpen, setFilterDrawerOpen] = useState(false);

  // Edit form state
  const [editAmount, setEditAmount] = useState("");
  const [editType, setEditType] = useState<"expense" | "income">("expense");
  const [editKakeiboType, setEditKakeiboType] = useState<"need" | "want" | "saving">("need");
  const [editHasKakeiboOverride, setEditHasKakeiboOverride] = useState(false);
  const [editReceipt, setEditReceipt] = useState<File | null>(null);
  const [receiptRetryTransactionId, setReceiptRetryTransactionId] = useState<string | null>(null);
  const [editNotes, setEditNotes] = useState("");
  const [editAccountId, setEditAccountId] = useState("");
  const [editCategoryId, setEditCategoryId] = useState("");
  const [editGoalId, setEditGoalId] = useState("");
  const [editObligationId, setEditObligationId] = useState("");
  const [editDebtRows, setEditDebtRows] = useState<DebtAllocationValue[]>([]);
  const [editDate, setEditDate] = useState("");
  const [editError, setEditError] = useState("");

  // Fetch Accounts
  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
  });
  const accounts = accountsData?.accounts ?? [];

  // Fetch Categories
  const { data: categoriesData } = useQuery<{ categories: any[] }>({
    queryKey: queryKeys.categories,
    queryFn: () => api.get("/categories"),
  });
  const categories = categoriesData?.categories ?? [];

  // Fetch Goals & Obligations
  const { data: goalsData } = useQuery<{ goals: any[] }>({
    queryKey: queryKeys.goals,
    queryFn: () => api.get("/goals"),
  });
  const goals = goalsData?.goals ?? [];

  const { data: obligationsData } = useQuery<{ obligations: any[] }>({
    queryKey: queryKeys.obligations,
    queryFn: () => api.get("/obligations"),
  });
  const obligations = obligationsData?.obligations ?? [];

  // Build query params
  const queryParams = useMemo(() => {
    const p = new URLSearchParams();
    p.set("limit", pageSize.toString());
    p.set("offset", (page * pageSize).toString());
    if (!selectionMode && !searchQuery.trim() && accountFilter === "all" && typeFilter === "all" && categoryFilter === "all") {
      p.set("logical_movements", "true");
    }
    if (searchQuery.trim()) p.set("q", searchQuery.trim());
    if (typeFilter !== "all") p.set("type", typeFilter);
    if (accountFilter !== "all") p.set("account_id", accountFilter);
    if (categoryFilter !== "all") p.set("category_id", categoryFilter);
    return p.toString();
  }, [searchQuery, typeFilter, accountFilter, categoryFilter, page, selectionMode]);

  // Fetch Transactions
  const { data: txData, isLoading } = useQuery<{
    ok: boolean;
    total: number;
    transactions: TransactionItem[];
  }>({
    queryKey: queryKeys.transactions.ledger(queryParams),
    queryFn: () => api.get(`/transactions?${queryParams}`),
  });

  const transactions = useMemo(() => txData?.transactions ?? [], [txData?.transactions]);
  const totalCount = txData?.total ?? 0;
  const totalPages = Math.ceil(totalCount / pageSize);

  // Consolidate paired internal movements into single visual entries in all-account ledger
  const displayTransactions = useMemo(() => {
    return consolidateLedgerMovements(
      transactions,
      !selectionMode && accountFilter === "all" && typeFilter === "all" && categoryFilter === "all"
    );
  }, [transactions, accountFilter, typeFilter, categoryFilter, selectionMode]);

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
    if (tx.is_inferred_transfer && tx.partner_id) {
      const partner = transactions.find((item) => item.id === tx.partner_id);
      if (partner) {
        setSelectionMode(true);
        setSelectedTransactions([tx, partner]);
        setMergeError("");
      }
      return;
    }
    setEditingTx(tx);
    setEditAmount(formatNumberWithDots(tx.amount));
    setEditType(tx.type);
    setEditKakeiboType(
      tx.kakeibo_type === "want" || tx.kakeibo_type === "saving"
        ? tx.kakeibo_type
        : "need",
    );
    const originalCategory = categories.find((category) => category.id === tx.category_id);
    setEditHasKakeiboOverride(Boolean(
      tx.kakeibo_type && originalCategory?.kakeibo_type && tx.kakeibo_type !== originalCategory.kakeibo_type,
    ));
    setEditReceipt(null);
    setReceiptRetryTransactionId(null);
    setEditNotes(tx.notes || "");
    setEditAccountId(tx.account_id);
    setEditCategoryId(tx.category_id || "");
    setEditGoalId(tx.goal_id || "");
    setEditObligationId(tx.obligation_id || "");
    setEditDebtRows(tx.type === "expense" ? (tx.obligation_allocations ?? (tx.obligation_id ? [{ obligation_id: tx.obligation_id, obligation_name: tx.obligation_name || "", amount: tx.amount }] : [])).map((item) => ({ obligation_id: item.obligation_id, obligation_name: item.obligation_name, amount: formatNumberWithDots(item.amount) })) : []);
    setEditDate(tx.date ? toDatetimeLocal(tx.date) : "");
    setEditError("");
  };

  const refreshTransactionViews = () => {
    qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
    qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    qc.invalidateQueries({ queryKey: queryKeys.accounts });
    qc.invalidateQueries({ queryKey: queryKeys.goals });
    qc.invalidateQueries({ queryKey: queryKeys.obligations });
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

      const updated = await api.patch(`/transactions/${editingTx.id}`, {
        type: editType,
        amount: amt,
        notes: editNotes.trim() || null,
        account_id: editAccountId,
        category_id: editCategoryId || null,
        goal_id: editGoalId || null,
        obligation_id: editType === "income" ? (editObligationId || null) : editDebtRows.length === 1 ? editDebtRows[0].obligation_id : null,
        ...(editType === "expense" && editDebtRows.length > 1 ? { obligation_allocations: editDebtRows.map((row) => ({ obligation_id: row.obligation_id, amount: Number(row.amount.replace(/\./g, "")) })) } : {}),
        kakeibo_type: editType === "expense" ? editKakeiboType : null,
        date: isoDate,
      });
      if (editReceipt) {
        try {
          await uploadTransactionReceipt(editingTx.id, editReceipt);
        } catch (uploadError) {
          setReceiptRetryTransactionId(editingTx.id);
          refreshTransactionViews();
          throw new Error(
            `Perubahan tersimpan, tetapi bukti gagal diunggah: ${uploadError instanceof Error ? uploadError.message : "unggahan gagal"}`,
          );
        }
      }
      return updated;
    },
    onSuccess: () => {
      refreshTransactionViews();
      setEditingTx(null);
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal memperbarui transaksi");
    },
  });

  const retryReceiptMutation = useMutation({
    mutationFn: async () => {
      if (!receiptRetryTransactionId || !editReceipt) {
        throw new Error("Pilih bukti transaksi untuk diunggah ulang");
      }
      return uploadTransactionReceipt(receiptRetryTransactionId, editReceipt);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      setReceiptRetryTransactionId(null);
      setEditingTx(null);
    },
    onError: (error: Error) => {
      setEditError(`Perubahan tetap tersimpan, tetapi bukti belum terunggah: ${error.message}`);
    },
  });

  // Delete mutation (handles single transaction and atomic paired movements)
  const deleteMutation = useMutation({
    mutationFn: async (tx: TransactionItem) => {
      if (tx.movement_id) {
        await api.del(`/movements/${tx.movement_id}`);
      } else {
        await api.del(`/transactions/${tx.id}`);
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      setEditingTx(null);
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal menghapus transaksi");
    },
  });

  const toggleSelection = (tx: TransactionItem) => {
    if (tx.movement_id) return;
    setMergeError("");
    setSelectedTransactions((selected) => {
      if (selected.some((item) => item.id === tx.id)) {
        return selected.filter((item) => item.id !== tx.id);
      }
      return selected.length < 2 ? [...selected, tx] : selected;
    });
  };

  const prepareMerge = async () => {
    if (selectedTransactions.length !== 2) return;
    setPreparingMerge(true);
    setMergeError("");
    try {
      const details = await Promise.all(selectedTransactions.map(async (tx) => {
        const response = await api.get<{ transaction: TransactionItem }>(`/transactions/${tx.id}`);
        return response.transaction;
      }));
      const expense = details.find((tx) => tx.type === "expense");
      const income = details.find((tx) => tx.type === "income");
      if (!expense || !income || expense.movement_id || income.movement_id ||
          expense.amount !== income.amount || expense.account_id === income.account_id) {
        throw new Error("Pilih satu uang keluar dan satu uang masuk dengan nominal sama dari rekening berbeda.");
      }
      setMergePreview([expense, income]);
    } catch (error) {
      setMergeError(error instanceof Error ? error.message : "Gagal memeriksa transaksi terpilih.");
    } finally {
      setPreparingMerge(false);
    }
  };

  const mergeMutation = useMutation({
    mutationFn: async () => {
      if (!mergePreview) throw new Error("Pilih dua transaksi terlebih dahulu.");
      return api.post("/movements/merge", {
        expense_transaction_id: mergePreview[0].id,
        income_transaction_id: mergePreview[1].id,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.categories });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
      setMergePreview(null);
      setSelectedTransactions([]);
      setSelectionMode(false);
      setPage(0);
      setMergeError("");
    },
    onError: (error: Error) => {
      setMergeError(error.message);
    },
  });

  return (
    <div className="space-y-6">
      {/* 1. Header & Summary Ribbon (Desktop Only, Mobile uses streamlined topbar) */}
      <div className="hidden sm:flex sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-[var(--border)]">
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
      {/* Mobile 1-Row Summary Ribbon (< sm) */}
      <div className="sm:hidden flex items-center justify-between p-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs">
        <div className="flex-1 text-center">
          <div className="text-[10px] uppercase font-bold text-[var(--muted)]">Masuk</div>
          <div className="text-xs font-bold text-emerald-500 tabular mt-0.5">+{bal(pageInflow)}</div>
        </div>
        <div className="h-6 w-px bg-[var(--border)]" />
        <div className="flex-1 text-center">
          <div className="text-[10px] uppercase font-bold text-[var(--muted)]">Keluar</div>
          <div className="text-xs font-bold text-rose-500 tabular mt-0.5">-{bal(pageOutflow)}</div>
        </div>
        <div className="h-6 w-px bg-[var(--border)]" />
        <div className="flex-1 text-center">
          <div className="text-[10px] uppercase font-bold text-[var(--muted)]">Net</div>
          <div
            className={cn(
              "text-xs font-bold tabular mt-0.5",
              pageInflow - pageOutflow >= 0 ? "text-emerald-500" : "text-rose-500"
            )}
          >
            {pageInflow - pageOutflow >= 0 ? "+" : ""}
            {bal(pageInflow - pageOutflow)}
          </div>
        </div>
      </div>

      {/* Desktop 3-Card Summary (>= sm) */}
      <div className="hidden sm:grid sm:grid-cols-3 gap-4">
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

      {/* 3A. Mobile Search & Filter Toolbar (< lg) */}
      <div className="lg:hidden space-y-2.5">
        {/* Search Input Pill */}
        <div className="relative">
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
            placeholder="Cari catatan, kategori, toko…"
            className="w-full rounded-2xl border border-[var(--border)] bg-[var(--surface)] pl-10 pr-9 py-2.5 text-xs text-[var(--text)] placeholder-[var(--muted)] focus:outline-none focus:border-income transition-colors shadow-2xs"
          />
          {searchQuery && (
            <button
              type="button"
              onClick={() => setSearchQuery("")}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-[var(--muted)] hover:text-[var(--text)] p-1"
            >
              ✕
            </button>
          )}
        </div>

        {/* Horizontal Filter Chips & Filter Drawer Button */}
        <div className="flex items-center gap-1.5 overflow-x-auto pb-1 scrollbar-none">
          {[
            { key: "all", label: "Semua" },
            { key: "expense", label: "Keluar" },
            { key: "income", label: "Masuk" },
          ].map(({ key, label }) => (
            <button
              key={key}
              type="button"
              onClick={() => {
                setTypeFilter(key);
                setPage(0);
              }}
              className={cn(
                "px-3 py-1.5 rounded-full text-xs font-semibold whitespace-nowrap transition-[background-color,color,transform] shrink-0 active:scale-95",
                typeFilter === key
                  ? "bg-[#1E201E] text-white shadow-2xs border border-white/10"
                  : "bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] border border-[var(--border)]"
              )}
            >
              {label}
            </button>
          ))}

          {/* Filter Drawer Trigger Button */}
          <button
            type="button"
            onClick={() => setFilterDrawerOpen(true)}
            className={cn(
              "ml-auto flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold whitespace-nowrap border transition-[background-color,border-color,color,transform] shrink-0 active:scale-95",
              accountFilter !== "all" || categoryFilter !== "all"
                ? "bg-emerald-500/15 text-emerald-500 border-emerald-500/30"
                : "bg-[var(--surface)] text-[var(--text)] border-[var(--border)] hover:bg-[var(--surface-raised)]"
            )}
          >
            <Icon name="filter" className="h-3.5 w-3.5" />
            <span>Filter</span>
            {(accountFilter !== "all" || categoryFilter !== "all") && (
              <span className="h-2 w-2 rounded-full bg-emerald-500" />
            )}
          </button>
        </div>
      </div>

      {/* 3B. Desktop Search & Filter Bar (>= lg) */}
      <div className="hidden lg:block p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs space-y-3">
        <div className="grid grid-cols-12 gap-3 items-center">
          {/* Search Input */}
          <div className="col-span-6 relative">
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
              placeholder="Cari catatan, kategori, toko, target…"
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
          <div className="col-span-3">
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
          <div className="col-span-3">
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

      {/* 4. Ledger Data Display */}
      <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-xs">
        <button
          type="button"
          onClick={() => {
            setSelectionMode((enabled) => !enabled);
            setSelectedTransactions([]);
            setMergeError("");
            setPage(0);
          }}
          aria-pressed={selectionMode}
          className="min-h-10 rounded-lg border border-[var(--border)] px-3 font-semibold text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
        >
          {selectionMode ? "Selesai memilih" : "Pilih 2 transaksi"}
        </button>
        {selectionMode && (
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[var(--muted)] tabular-nums">{selectedTransactions.length}/2 dipilih</span>
            <button
              type="button"
              disabled={selectedTransactions.length !== 2 || preparingMerge}
              onClick={prepareMerge}
              className="min-h-10 rounded-lg bg-[#1E201E] px-3 font-semibold text-white disabled:opacity-40 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
            >
              {preparingMerge ? "Memeriksa…" : "Gabungkan sebagai Pindah Saldo"}
            </button>
          </div>
        )}
        {mergeError && !mergePreview && <p role="alert" className="w-full text-rose-500">{mergeError}</p>}
      </div>
      {/* Mobile Feed (< lg) */}
      <div className="lg:hidden space-y-4">
        <MobileLedgerFeed
          transactions={displayTransactions}
          onOpenEdit={selectionMode ? toggleSelection : handleOpenEdit}
          selectionMode={selectionMode}
          selectedIds={selectedTransactions.map((item) => item.id)}
          bal={bal}
          isLoading={isLoading}
        />

        {/* Mobile Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between p-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] text-xs text-[var(--muted)]">
            <button
              type="button"
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              className="px-3 py-1.5 rounded-xl border border-[var(--border)] hover:bg-[var(--surface-raised)] disabled:opacity-40 font-medium"
            >
              Sebelumnya
            </button>
            <span className="text-[11px] px-2 font-bold tabular text-[var(--text)]">
              {page + 1} / {totalPages}
            </span>
            <button
              type="button"
              disabled={page >= totalPages - 1}
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              className="px-3 py-1.5 rounded-xl border border-[var(--border)] hover:bg-[var(--surface-raised)] disabled:opacity-40 font-medium"
            >
              Berikutnya
            </button>
          </div>
        )}
      </div>

      {/* Desktop Data Table (>= lg) */}
      <div className="hidden lg:block rounded-3xl border border-[var(--border)] bg-[var(--surface)] shadow-xs overflow-hidden">
        {isLoading ? (
          <div className="py-20 text-center text-xs text-[var(--muted)] animate-pulse">
            Memuat riwayat transaksi…
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
                  {selectionMode && <th className="py-3 px-3 font-semibold">Pilih</th>}
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
                      onClick={() => selectionMode ? toggleSelection(tx) : handleOpenEdit(tx)}
                      className="hover:bg-[var(--surface-raised)]/60 transition-colors cursor-pointer group"
                    >
                      {selectionMode && (
                        <td className="px-3 py-3.5">
                          <button
                            type="button"
                            aria-label={`Pilih transaksi ${tx.notes || tx.category_name || tx.id}`}
                            aria-pressed={selectedTransactions.some((item) => item.id === tx.id)}
                            disabled={Boolean(tx.movement_id) || (selectedTransactions.length === 2 && !selectedTransactions.some((item) => item.id === tx.id))}
                            onClick={(event) => {
                              event.stopPropagation();
                              toggleSelection(tx);
                            }}
                            className="min-h-9 min-w-9 rounded-lg border border-[var(--border)] font-semibold text-[var(--text)] aria-pressed:bg-sky-500/15 aria-pressed:text-sky-500 disabled:opacity-40 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                          >
                            {selectedTransactions.some((item) => item.id === tx.id) ? "✓" : "+"}
                          </button>
                        </td>
                      )}
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
                        {tx.is_consolidated_transfer || tx.is_inferred_transfer ? (
                          <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-sky-500/10 text-sky-500 border border-sky-500/20 tracking-wide">
                            <span className="text-xs leading-none">↔️</span>
                            <span>{tx.is_inferred_transfer ? "Pindah Saldo (perkiraan)" : "Pindah Saldo"}</span>
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
                        {(tx.is_consolidated_transfer || tx.is_inferred_transfer) && tx.target_account_name ? (
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
                        ) : (tx.obligation_allocations?.length ?? 0) > 1 ? (
                          <span className="block max-w-52 text-[11px] text-rose-500">
                            <span className="block font-semibold">💳 {tx.obligation_allocations?.length} tagihan</span>
                            <span className="block truncate text-[var(--muted)]" title={tx.obligation_allocations?.map((item) => `${item.obligation_name}: ${bal(item.amount)}`).join(" · ")}>
                              {tx.obligation_allocations?.map((item) => `${item.obligation_name} ${bal(item.amount)}`).join(" · ")}
                            </span>
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
                          tx.is_consolidated_transfer || tx.is_inferred_transfer
                            ? "text-[var(--text)]"
                            : isMovement
                            ? "text-sky-500"
                            : isIncome
                            ? "text-income"
                            : "text-expense"
                        )}
                      >
                        {tx.is_consolidated_transfer || tx.is_inferred_transfer ? "" : isIncome ? "+" : "-"}
                        {bal(tx.amount)}
                      </td>

                      {/* Action */}
                      <td className="py-3.5 px-4 text-right whitespace-nowrap">
                        {!selectionMode && <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleOpenEdit(tx);
                          }}
                          className="px-2.5 py-1 rounded-lg border border-[var(--border)] text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--surface)] text-[11px]"
                        >
                          Ubah
                        </button>}
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

      <Modal
        open={Boolean(mergePreview)}
        onClose={() => {
          if (!mergeMutation.isPending) setMergePreview(null);
        }}
        title="Gabungkan sebagai Pindah Saldo"
      >
        {mergePreview && (
          <div className="space-y-4 text-sm">
            <p className="text-[var(--muted)]">
              Kedua transaksi akan menjadi satu pindah saldo. Saldo rekening tidak berubah, tetapi kategori serta ringkasan pemasukan, pengeluaran, dan Kakeibo akan berubah.
            </p>
            <div className="divide-y divide-[var(--border)] rounded-xl border border-[var(--border)]">
              {mergePreview.map((tx) => (
                <div key={tx.id} className="space-y-1 px-3 py-3">
                  <div className="flex items-center justify-between gap-3 font-semibold text-[var(--text)]">
                    <span>{tx.type === "expense" ? "Keluar dari" : "Masuk ke"} {tx.account_name}</span>
                    <span className="tabular-nums">{bal(tx.amount)}</span>
                  </div>
                  <p className="text-xs text-[var(--muted)]">
                    {new Date(tx.date).toLocaleString("id-ID", {
                      day: "2-digit", month: "2-digit", year: "numeric",
                      hour: "2-digit", minute: "2-digit", second: "2-digit",
                    })}
                    {tx.category_name ? ` · ${tx.category_name}` : ""}
                  </p>
                  {tx.notes && <p className="text-xs text-[var(--muted)]">{tx.notes}</p>}
                </div>
              ))}
            </div>
            {mergeError && <p role="alert" className="text-xs text-rose-500">{mergeError}</p>}
            <div className="flex flex-wrap justify-end gap-2 border-t border-[var(--border)] pt-4">
              <button
                type="button"
                disabled={mergeMutation.isPending}
                onClick={() => setMergePreview(null)}
                className="min-h-11 rounded-xl border border-[var(--border)] px-4 font-semibold text-[var(--text)]"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={mergeMutation.isPending}
                onClick={() => mergeMutation.mutate()}
                className="min-h-11 rounded-xl bg-[#1E201E] px-4 font-semibold text-white disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
              >
                {mergeMutation.isPending ? "Menggabungkan…" : "Ya, gabungkan"}
              </button>
            </div>
          </div>
        )}
      </Modal>

      <InternalMovementModal
        open={Boolean(editingTx?.movement_id)}
        onClose={() => setEditingTx(null)}
        editingMovement={editingTx?.movement_id ? {
          id: editingTx.movement_id,
          sourceAccountId: editingTx.movement_role === "inbound"
            ? editingTx.transfer_target_account_id ?? ""
            : editingTx.account_id,
          targetAccountId: editingTx.movement_role === "inbound"
            ? editingTx.account_id
            : editingTx.target_account_id ?? editingTx.transfer_target_account_id ?? "",
          amount: editingTx.amount,
          notes: editingTx.notes,
          date: editingTx.date,
        } : null}
      />

      {/* 5. Ordinary transaction edit modal */}
      {editingTx && !editingTx.movement_id && (
        <Modal
          open={Boolean(editingTx)}
          onClose={() => setEditingTx(null)}
          title="Ubah Transaksi"
        >
          <form
            className="space-y-4 pt-2 text-xs"
            onSubmit={(event) => {
              event.preventDefault();
              if (editType === "expense") {
                if (editDebtRows.some((row) => !row.obligation_id || editDebtRows.filter((item) => item.obligation_id === row.obligation_id).length > 1)) {
                  setEditError("Pilih tagihan yang berbeda untuk setiap baris.");
                  document.getElementById("ledger-edit-debt-0")?.focus();
                  return;
                }
                const amount = Number(editAmount.replace(/\./g, ""));
                const originalAmounts = Object.fromEntries((editingTx.obligation_allocations ?? []).map((item) => [item.obligation_id, item.amount]));
                const splitError = allocationError(editDebtRows, amount, obligations, originalAmounts);
                if (splitError) {
                  setEditError(splitError);
                  (document.getElementById("ledger-edit-debt-0-amount") || document.getElementById("ledger-edit-edit-amounts"))?.focus();
                  return;
                }
              }
              if (!updateMutation.isPending) updateMutation.mutate();
            }}
          >
            {editError && (
              <div role="alert" aria-live="assertive" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {editError}
              </div>
            )}
            {receiptRetryTransactionId === editingTx.id && (
              <button
                type="button"
                disabled={retryReceiptMutation.isPending || !editReceipt}
                onClick={() => retryReceiptMutation.mutate()}
                className="min-h-11 rounded-xl border border-[var(--border-strong)] px-4 text-sm font-semibold text-[var(--text)] disabled:opacity-50"
              >
                {retryReceiptMutation.isPending ? "Mengunggah…" : "Coba unggah bukti lagi"}
              </button>
            )}

            <div className="grid grid-cols-2 gap-2" role="group" aria-label="Jenis transaksi">
                  {(["expense", "income"] as const).map((candidate) => (
                    <button
                      key={candidate}
                      type="button"
                      aria-pressed={editType === candidate}
                      onClick={() => {
                        setEditType(candidate);
                        setEditCategoryId("");
                        setEditHasKakeiboOverride(false);
                        if (candidate === "income") { setEditGoalId(""); setEditDebtRows([]); }
                      }}
                      className={cn(
                        "min-h-11 rounded-xl border px-3 text-sm font-semibold",
                        editType === candidate
                          ? "border-[var(--text)] bg-[var(--text)] text-[var(--surface)]"
                          : "border-[var(--border)] bg-[var(--surface-raised)] text-[var(--muted)]",
                      )}
                    >
                      {candidate === "expense" ? "Pengeluaran" : "Pemasukan"}
                    </button>
                  ))}
            </div>
                <div>
                  <label htmlFor="ledger-edit-amount" className="text-xs font-medium text-[var(--muted)] block mb-1">{editType === "expense" && editDebtRows.some((row) => row.obligation_id) ? "Total bayar (IDR)" : "Nominal Uang (IDR)"}</label>
                  <input
                    id="ledger-edit-amount"
                    name="amount"
                    type="text"
                    inputMode="numeric"
                    required
                    value={editAmount}
                    readOnly={editType === "expense" && editDebtRows.some((row) => row.obligation_id)}
                    onChange={(e) => setEditAmount(formatNumberWithDots(e.target.value))}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
                  />
                </div>

                <div>
                  <label htmlFor="ledger-edit-account" className="text-xs font-medium text-[var(--muted)] block mb-1">{editType === "expense" ? "Bayar dari" : "Masuk ke"}</label>
                  <select
                    id="ledger-edit-account"
                    name="account_id"
                    required
                    value={editAccountId}
                    onChange={(e) => setEditAccountId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} liquidOnly />
                  </select>
                </div>

                <div>
                  <label htmlFor="ledger-edit-category" className="text-xs font-medium text-[var(--muted)] block mb-1">Kategori</label>
                  <select
                    id="ledger-edit-category"
                    name="category_id"
                    value={editCategoryId}
                    onChange={(event) => {
                      const nextCategoryId = event.target.value;
                      setEditCategoryId(nextCategoryId);
                      if (editType === "expense" && !editHasKakeiboOverride) {
                        const category = categories.find((item) => item.id === nextCategoryId);
                        const pillar = category?.kakeibo_type;
                        setEditKakeiboType(pillar === "want" || pillar === "saving" ? pillar : "need");
                      }
                    }}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tanpa Kategori</option>
                    {categories
                      .filter((c) => c.kind === editType && !c.is_archived)
                      .map((cat) => (
                        <option key={cat.id} value={cat.id}>
                          {cat.name}
                        </option>
                      ))}
                  </select>
                </div>

                {editType === "expense" && (
                  <div>
                    <div className="mb-1 flex items-center gap-1">
                      <label htmlFor="ledger-edit-kakeibo" className="text-xs font-medium text-[var(--muted)]">Pilar Kakeibo</label>
                      <InfoHelp label="Pilar Kakeibo">Mengikuti pilar kategori, kecuali Anda memilih pilar lain untuk transaksi ini.</InfoHelp>
                    </div>
                    <select id="ledger-edit-kakeibo" name="kakeibo_type" value={editKakeiboType} onChange={(event) => { setEditKakeiboType(event.target.value as typeof editKakeiboType); setEditHasKakeiboOverride(true); }} className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]">
                      <option value="need">Kebutuhan</option>
                      <option value="want">Keinginan</option>
                      <option value="saving">Tabungan</option>
                    </select>
                  </div>
                )}

                {/* Optional Goal Link */}
                <div>
                  <label htmlFor="ledger-edit-goal" className="text-xs font-medium text-[var(--muted)] block mb-1">
                    Kaitkan ke target (opsional)
                  </label>
                  <select
                    id="ledger-edit-goal"
                    name="goal_id"
                    value={editGoalId}
                    onChange={(e) => {
                      setEditGoalId(e.target.value);
                      if (e.target.value) { setEditObligationId(""); setEditDebtRows([]); }
                    }}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tidak ada</option>
                    {goals.filter((goal) => !goal.is_archived).map((g) => (
                      <option key={g.id} value={g.id}>
                        🎯 {g.name} ({bal(g.current_amount)} / {bal(g.target_amount)})
                      </option>
                    ))}
                  </select>
                  {editGoalId && <p className="mt-1 text-xs text-[var(--muted)]">Kaitan ini tidak menambah progres target. Target terhubung rekening mengikuti saldonya.</p>}
                </div>

                {editType === "expense" ? <DebtAllocationEditor idPrefix="ledger-edit" debts={obligations} rows={editDebtRows} total={Number(editAmount.replace(/\./g, "")) || 0} currency={bal} originalAmounts={Object.fromEntries((editingTx.obligation_allocations ?? []).map((item) => [item.obligation_id, item.amount]))} onChange={(rows) => {
                  setEditDebtRows(rows);
                  if (rows.length) setEditGoalId("");
                  if (rows.some((row) => row.obligation_id)) {
                    setEditAmount(formatNumberWithDots(totalDebtPayments(rows)));
                  }
                  setEditError("");
                }} /> : <div>
                  <label htmlFor="ledger-edit-obligation" className="text-xs font-medium text-[var(--muted)] block mb-1">
                    Kaitkan ke tagihan / utang (opsional)
                  </label>
                  <select
                    id="ledger-edit-obligation"
                    name="obligation_id"
                    value={editObligationId}
                    onChange={(e) => {
                      setEditObligationId(e.target.value);
                      if (e.target.value) setEditGoalId("");
                    }}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tidak ada</option>
                    {obligations.filter((obligation) => !obligation.is_archived && obligation.remaining_amount > 0).map((o) => (
                      <option key={o.id} value={o.id}>
                        💳 {o.name} ({bal(o.remaining_amount)} tersisa)
                      </option>
                    ))}
                  </select>
                  {editObligationId && <p className="mt-1 text-xs text-[var(--muted)]">Pemasukan ini menambah kembali sisa tagihan.</p>}
                </div>}

                <div>
                  <label htmlFor="ledger-edit-date" className="text-xs font-medium text-[var(--muted)] block mb-1">Tanggal & Waktu</label>
                  <input
                    id="ledger-edit-date"
                    name="date"
                    type="datetime-local"
                    step={1}
                    required
                    value={editDate}
                    onChange={(e) => setEditDate(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  />
                </div>

                <div>
                  <label htmlFor="ledger-edit-notes" className="text-xs font-medium text-[var(--muted)] block mb-1">Catatan</label>
                  <input
                    id="ledger-edit-notes"
                    name="notes"
                    type="text"
                    autoComplete="off"
                    value={editNotes}
                    onChange={(e) => setEditNotes(e.target.value)}
                    placeholder="Tambahkan catatan…"
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  />
                </div>

                <div>
                  <label htmlFor="edit-transaction-receipt" className="text-xs font-medium text-[var(--muted)] block mb-1">Ganti / tambahkan bukti</label>
                  <input id="edit-transaction-receipt" type="file" accept="image/jpeg,image/png,image/webp,application/pdf" onChange={(event) => setEditReceipt(event.target.files?.[0] ?? null)} className="block min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)]" />
                  {editingTx.receipt_path && <p className="mt-1 text-[10px] text-[var(--muted)]">Bukti saat ini akan diganti hanya setelah unggahan baru berhasil.</p>}
                </div>
            <div className="pt-4 flex items-center justify-between border-t border-[var(--border)]">
              <ConfirmActionButton
                label={editingTx.is_consolidated_transfer ? "Hapus pemindahan saldo" : "Hapus transaksi"}
                confirmation={editingTx.is_consolidated_transfer
                  ? "Hapus pemindahan saldo ini? Saldo kedua rekening akan dikembalikan."
                  : (editingTx.obligation_allocations?.length ?? 0) > 0
                    ? "Hapus pembayaran ini? Saldo rekening diperbarui dan seluruh pembagian tagihan dikembalikan."
                  : "Hapus transaksi ini? Saldo rekening akan diperbarui."}
                onConfirm={() => deleteMutation.mutate(editingTx)}
                disabled={deleteMutation.isPending}
                className="min-h-11 rounded-xl border border-rose-500/25 bg-rose-500/10 px-3.5 font-semibold text-rose-600 hover:bg-rose-500/20 disabled:opacity-50"
              >
                {deleteMutation.isPending ? "Menghapus…" : "Hapus"}
              </ConfirmActionButton>

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => setEditingTx(null)}
                  className="btn-secondary"
                >
                  Batal
                </button>
                <button
                  type="submit"
                  disabled={updateMutation.isPending}
                  className="btn-primary"
                >
                  {updateMutation.isPending ? "Menyimpan…" : "Simpan Perubahan"}
                </button>
              </div>
            </div>
          </form>
        </Modal>
      )}

      {/* Recurring Rules Modal */}
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => setRecurringModalOpen(false)}
      />

      {/* Mobile Bottom-Sheet Filter Drawer */}
      <Modal
        open={filterDrawerOpen}
        onClose={() => setFilterDrawerOpen(false)}
        title="Filter Transaksi"
      >
        <div className="space-y-4 pt-1 text-xs">
          {/* Account Filter */}
          <div className="space-y-2">
            <p className="text-xs font-semibold text-[var(--muted)]">
              Rekening & Dompet
            </p>
            <div role="group" aria-label="Filter rekening dan dompet" className="flex flex-wrap gap-2">
              <button
                type="button"
                aria-pressed={accountFilter === "all"}
                onClick={() => {
                  setAccountFilter("all");
                  setPage(0);
                }}
                className={cn(
                  "min-h-11 rounded-xl border px-3 text-xs font-medium transition-colors",
                  accountFilter === "all"
                    ? "border-[var(--text)] bg-[var(--text)] font-semibold text-[var(--surface)]"
                    : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--text)]"
                )}
              >
                Semua Rekening
              </button>
              {accounts.map((acc: any) => (
                <button
                  key={acc.id}
                  type="button"
                  aria-pressed={accountFilter === acc.id}
                  onClick={() => {
                    setAccountFilter(acc.id);
                    setPage(0);
                  }}
                  className={cn(
                    "min-h-11 rounded-xl border px-3 text-xs font-medium transition-colors",
                    accountFilter === acc.id
                      ? "border-[var(--text)] bg-[var(--text)] font-semibold text-[var(--surface)]"
                      : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--text)]"
                  )}
                >
                  {acc.name}
                </button>
              ))}
            </div>
          </div>

          {/* Category Filter */}
          <div className="space-y-2">
            <p className="text-xs font-semibold text-[var(--muted)]">
              Kategori
            </p>
            <div role="group" aria-label="Filter kategori" className="flex flex-wrap gap-2">
              <button
                type="button"
                aria-pressed={categoryFilter === "all"}
                onClick={() => {
                  setCategoryFilter("all");
                  setPage(0);
                }}
                className={cn(
                  "min-h-11 rounded-xl border px-3 text-xs font-medium transition-colors",
                  categoryFilter === "all"
                    ? "border-[var(--text)] bg-[var(--text)] font-semibold text-[var(--surface)]"
                    : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--text)]"
                )}
              >
                Semua Kategori
              </button>
              {categories.map((cat: any) => (
                <button
                  key={cat.id}
                  type="button"
                  aria-pressed={categoryFilter === cat.id}
                  onClick={() => {
                    setCategoryFilter(cat.id);
                    setPage(0);
                  }}
                  className={cn(
                    "min-h-11 rounded-xl border px-3 text-xs font-medium transition-colors",
                    categoryFilter === cat.id
                      ? "border-[var(--text)] bg-[var(--text)] font-semibold text-[var(--surface)]"
                      : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--text)]"
                  )}
                >
                  {cat.name}
                </button>
              ))}
            </div>
          </div>

          {/* Drawer Actions */}
          <div className="flex items-center gap-2 pt-3 border-t border-[var(--border)]">
            <button
              type="button"
              onClick={() => {
                setAccountFilter("all");
                setCategoryFilter("all");
                setPage(0);
              }}
              className="flex-1 py-2.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] font-semibold text-center hover:text-[var(--text)]"
            >
              Reset Filter
            </button>
            <button
              type="button"
              onClick={() => setFilterDrawerOpen(false)}
              className="flex-1 py-2.5 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-white font-bold text-center shadow-xs"
            >
              Terapkan
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
