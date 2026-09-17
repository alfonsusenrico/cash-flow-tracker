"use client";

import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { StatusBadge, BadgeVariant } from "@/components/ui/StatusBadge";
import { DetailSheet } from "@/components/ui/DetailSheet";
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
  type: "expense" | "income" | "transfer";
  kakeibo_type?: "need" | "want" | "saving" | null;
  amount: number;
  notes: string | null;
  date: string;
  receipt_path: string | null;
  created_at: string;
  is_excluded_from_budget?: boolean;
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

  // Selected for Edit/Detail (Ref 6 Slide-Out Sheet)
  const [selectedTx, setSelectedTx] = useState<TransactionItem | null>(null);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);

  // Edit form state
  const [editAmount, setEditAmount] = useState("");
  const [editNotes, setEditNotes] = useState("");
  const [editAccountId, setEditAccountId] = useState("");
  const [editCategoryId, setEditCategoryId] = useState("");
  const [editKakeiboType, setEditKakeiboType] = useState<"need" | "want" | "saving">("need");
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

  const transactions = txData?.transactions ?? [];
  const totalCount = txData?.total ?? 0;
  const totalPages = Math.ceil(totalCount / pageSize);

  // Helper to detect internal transfers/movements
  const isMovementTx = (t: TransactionItem) =>
    t.type === "transfer" ||
    t.category_name === "Internal Movement" ||
    t.category_name === "Investasi" ||
    Boolean(t.is_excluded_from_budget);

  // Compute real page cash flow (strictly excluding internal transfers so totals are accurate)
  const pageInflow = useMemo(() => {
    return transactions
      .filter((t) => t.type === "income" && !isMovementTx(t))
      .reduce((sum, t) => sum + t.amount, 0);
  }, [transactions]);

  const pageOutflow = useMemo(() => {
    return transactions
      .filter((t) => t.type === "expense" && !isMovementTx(t))
      .reduce((sum, t) => sum + t.amount, 0);
  }, [transactions]);

  const pageNet = pageInflow - pageOutflow;

  // Open Edit Sheet
  const handleOpenDetail = (tx: TransactionItem) => {
    setSelectedTx(tx);
    setEditAmount(formatNumberWithDots(tx.amount));
    setEditNotes(tx.notes || "");
    setEditAccountId(tx.account_id);
    setEditCategoryId(tx.category_id || "");
    setEditKakeiboType(tx.kakeibo_type || "need");
    setEditGoalId(tx.goal_id || "");
    setEditObligationId(tx.obligation_id || "");
    setEditDate(tx.date ? toDatetimeLocal(tx.date) : "");
    setEditError("");
  };

  // Update mutation
  const updateMutation = useMutation({
    mutationFn: async () => {
      if (!selectedTx) return;
      const amt = parseInt(editAmount.replace(/[^0-9]/g, ""), 10);
      if (!amt || amt <= 0) throw new Error("Nominal harus lebih dari 0");
      if (!editAccountId) throw new Error("Pilih rekening");

      return api.patch(`/transactions/${selectedTx.id}`, {
        amount: amt,
        notes: editNotes.trim() || null,
        account_id: editAccountId,
        category_id: editCategoryId || null,
        kakeibo_type: selectedTx.type !== "income" ? editKakeiboType : null,
        goal_id: editGoalId || null,
        obligation_id: editObligationId || null,
        date: editDate ? (localDatetimeToISO(editDate) || new Date(editDate).toISOString()) : undefined,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["obligations"] });
      setSelectedTx(null);
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal memperbarui transaksi");
    },
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: (txId: string) => api.del(`/transactions/${txId}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["obligations"] });
      setSelectedTx(null);
    },
  });

  return (
    <div className="space-y-5 select-none">
      {/* 1. Header & Quick Actions */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-1">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl sm:text-2xl font-bold tracking-tight text-[var(--text-primary)]">
              Riwayat Transaksi
            </h1>
            <span className="text-xs font-semibold px-2 py-0.5 rounded-full bg-[var(--canvas-subtle)] text-[var(--text-secondary)] border border-[var(--border-structural)]">
              {totalCount} Transaksi
            </span>
          </div>
          <p className="text-xs text-[var(--text-muted)] font-medium mt-0.5">
            Semua catatan arus kas, belanja, dan pemindahan saldo
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="btn-secondary text-xs rounded-xl cursor-pointer"
          >
            <Icon name="repeat" className="h-3.5 w-3.5 text-sky-500" />
            <span>Rutin</span>
          </button>

          <button
            type="button"
            onClick={() => openQuickAdd()}
            className="btn-charcoal px-3.5 py-2 text-xs font-semibold rounded-xl"
          >
            <Icon name="plus" className="h-3.5 w-3.5 stroke-[2.5]" />
            <span>Catat Transaksi</span>
            <kbd className="hidden md:inline rounded bg-white/20 px-1 py-0.2 text-[9px] font-mono text-white/90">
              N
            </kbd>
          </button>
        </div>
      </div>

      {/* Pending Scheduled Banner */}
      <PendingScheduledBanner onOpenRulesManager={() => setRecurringModalOpen(true)} />

      {/* 2. Real Cash Flow Summary Strip (Strictly excludes internal movements) */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3.5">
        <div className="card-crisp p-4">
          <span className="text-xs font-semibold text-[var(--text-muted)] block">
            Uang Masuk (Halaman Ini)
          </span>
          <div className="text-xl font-bold tabular tracking-tight text-[#1E7E34] dark:text-emerald-400 mt-1">
            +{bal(pageInflow)}
          </div>
        </div>

        <div className="card-crisp p-4">
          <span className="text-xs font-semibold text-[var(--text-muted)] block">
            Uang Keluar (Halaman Ini)
          </span>
          <div className="text-xl font-bold tabular tracking-tight text-[#DC2626] dark:text-rose-400 mt-1">
            -{bal(pageOutflow)}
          </div>
        </div>

        <div className="card-crisp p-4">
          <span className="text-xs font-semibold text-[var(--text-muted)] block">
            Selisih Bersih (Halaman Ini)
          </span>
          <div
            className={cn(
              "text-xl font-bold tabular tracking-tight mt-1",
              pageNet >= 0
                ? "text-[#1E7E34] dark:text-emerald-400"
                : "text-[#DC2626] dark:text-rose-400"
            )}
          >
            {pageNet >= 0 ? "+" : ""}
            {bal(pageNet)}
          </div>
        </div>
      </div>

      {/* 3. Filter Chip Bar (Ref 2 & Ref 5) */}
      <div className="card-crisp p-4 space-y-3">
        <div className="grid grid-cols-1 sm:grid-cols-12 gap-3">
          {/* Search Box */}
          <div className="sm:col-span-6 relative">
            <input
              type="text"
              placeholder="Cari catatan, merchant, rekening..."
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value);
                setPage(0);
              }}
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-app)] px-3.5 py-2 text-xs text-[var(--text-primary)] outline-none focus:border-[#1A1D1A]"
            />
            {searchQuery && (
              <button
                type="button"
                onClick={() => setSearchQuery("")}
                className="absolute right-3 top-2.5 text-xs text-[var(--text-muted)] hover:text-[var(--text-primary)]"
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
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-app)] px-3 py-2 text-xs font-medium text-[var(--text-primary)] outline-none"
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
              className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-app)] px-3 py-2 text-xs font-medium text-[var(--text-primary)] outline-none"
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

        {/* Type Segment Pills */}
        <div className="flex flex-wrap items-center gap-2 pt-1 border-t border-[var(--border-divider)]">
          <span className="text-[11px] font-semibold text-[var(--text-muted)] mr-1">
            Jenis:
          </span>
          {[
            { key: "all", label: "Semua" },
            { key: "expense", label: "Uang Keluar" },
            { key: "income", label: "Uang Masuk" },
            { key: "transfer", label: "Pindah Saldo" },
          ].map(({ key, label }) => {
            const isSelected = typeFilter === key;
            return (
              <button
                key={key}
                type="button"
                onClick={() => {
                  setTypeFilter(key);
                  setPage(0);
                }}
                className={cn(
                  "px-3 py-1 rounded-full text-xs font-semibold transition-all cursor-pointer",
                  isSelected
                    ? "bg-[#1E201E] text-white shadow-xs"
                    : "bg-[var(--canvas-subtle)] text-[var(--text-secondary)] hover:bg-[var(--canvas-app)] hover:text-[var(--text-primary)]"
                )}
              >
                {label}
              </button>
            );
          })}

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
              className="ml-auto text-xs text-[#1E7E34] dark:text-[#66CC55] font-semibold hover:underline cursor-pointer"
            >
              Reset Filter
            </button>
          )}
        </div>
      </div>

      {/* 4. High-Density Data Table (Ref 1 & Ref 6) */}
      <div className="card-crisp overflow-hidden">
        {isLoading ? (
          <div className="py-20 text-center text-xs text-[var(--text-muted)] animate-pulse">
            Memuat riwayat transaksi...
          </div>
        ) : transactions.length === 0 ? (
          <div className="py-20 text-center text-xs text-[var(--text-muted)] space-y-2">
            <div>Tidak ada transaksi yang cocok dengan filter.</div>
            <button
              type="button"
              onClick={() => openQuickAdd()}
              className="btn-lime px-3 py-1.5 text-xs rounded-xl"
            >
              + Catat Transaksi Baru
            </button>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs text-left">
              <thead>
                <tr className="border-b border-[var(--border-structural)] text-[var(--text-tertiary)] uppercase text-[10px] bg-[var(--canvas-app)]">
                  <th className="py-3 px-4 font-bold tracking-wider">Tanggal</th>
                  <th className="py-3 px-4 font-bold tracking-wider">Jenis</th>
                  <th className="py-3 px-4 font-bold tracking-wider">Keterangan</th>
                  <th className="py-3 px-4 font-bold tracking-wider">Kategori</th>
                  <th className="py-3 px-4 font-bold tracking-wider">Rekening</th>
                  <th className="py-3 px-4 font-bold tracking-wider text-right">Nominal</th>
                  <th className="py-3 px-4 font-bold tracking-wider text-right">Aksi</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border-divider)]">
                {transactions.map((tx) => {
                  const isMovement = isMovementTx(tx);
                  const isIncome = tx.type === "income" && !isMovement;

                  return (
                    <tr
                      key={tx.id}
                      onClick={() => handleOpenDetail(tx)}
                      className="hover:bg-[var(--canvas-subtle)] transition-colors cursor-pointer group"
                    >
                      {/* Date */}
                      <td className="py-3 px-4 whitespace-nowrap text-[var(--text-muted)] tabular font-medium">
                        {new Date(tx.date).toLocaleDateString("id-ID", {
                          day: "numeric",
                          month: "short",
                          year: "numeric",
                        })}
                      </td>

                      {/* Type Badge */}
                      <td className="py-3 px-4 whitespace-nowrap">
                        <StatusBadge
                          variant={isMovement ? "info" : isIncome ? "success" : "danger"}
                        >
                          {isMovement
                            ? "Pindah Saldo"
                            : isIncome
                            ? "Uang Masuk"
                            : "Uang Keluar"}
                        </StatusBadge>
                      </td>

                      {/* Notes / Description */}
                      <td className="py-3 px-4 font-semibold text-[var(--text-primary)] group-hover:text-emerald-600 dark:group-hover:text-emerald-400 transition-colors max-w-xs truncate">
                        {tx.notes || (isMovement ? "Pindah Saldo" : tx.category_name || "Umum")}
                      </td>

                      {/* Category */}
                      <td className="py-3 px-4 whitespace-nowrap">
                        {tx.category_name ? (
                          <span className="inline-flex items-center gap-1.5 text-[var(--text-secondary)] font-medium">
                            <span
                              className="h-2 w-2 rounded-full shrink-0"
                              style={{
                                backgroundColor: isMovement ? "#0284C7" : tx.category_color || "#0284C7",
                              }}
                            />
                            <span>{isMovement ? "Pindah Saldo" : tx.category_name}</span>
                          </span>
                        ) : (
                          <span className="text-[var(--text-muted)]">-</span>
                        )}
                      </td>

                      {/* Account */}
                      <td className="py-3 px-4 whitespace-nowrap font-medium text-[var(--text-secondary)]">
                        {tx.account_name}
                      </td>

                      {/* Amount */}
                      <td
                        className={cn(
                          "py-3 px-4 text-right font-bold tabular whitespace-nowrap",
                          isMovement
                            ? "text-sky-600 dark:text-sky-400"
                            : isIncome
                            ? "text-[#1E7E34] dark:text-emerald-400"
                            : "text-[#DC2626] dark:text-rose-400"
                        )}
                      >
                        {isIncome ? "+" : !isMovement && tx.type === "expense" ? "-" : ""}
                        {bal(tx.amount)}
                      </td>

                      {/* Action */}
                      <td className="py-3 px-4 text-right whitespace-nowrap">
                        <span className="text-[11px] font-semibold text-[var(--text-muted)] group-hover:text-[var(--text-primary)] transition-colors">
                          Detail &rarr;
                        </span>
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
          <div className="flex items-center justify-between px-4 py-3 border-t border-[var(--border-divider)] bg-[var(--canvas-app)] text-xs">
            <span className="text-[var(--text-muted)]">
              Halaman <span className="font-bold text-[var(--text-primary)]">{page + 1}</span> dari {totalPages}
            </span>
            <div className="flex gap-1.5">
              <button
                type="button"
                disabled={page === 0}
                onClick={() => setPage(page - 1)}
                className="btn-secondary px-3 py-1 rounded-lg disabled:opacity-40"
              >
                Sebelumnya
              </button>
              <button
                type="button"
                disabled={page >= totalPages - 1}
                onClick={() => setPage(page + 1)}
                className="btn-secondary px-3 py-1 rounded-lg disabled:opacity-40"
              >
                Berikutnya
              </button>
            </div>
          </div>
        )}
      </div>

      {/* ===================== SLIDE-OUT DETAIL & EDIT SHEET (Ref 6) ===================== */}
      <DetailSheet
        open={Boolean(selectedTx)}
        onClose={() => setSelectedTx(null)}
        title="Edit & Detail Transaksi"
        subtitle={
          selectedTx
            ? new Date(selectedTx.date).toLocaleString("id-ID", {
                dateStyle: "medium",
                timeStyle: "short",
              })
            : ""
        }
        footer={
          selectedTx && (
            <div className="flex items-center gap-2">
              <button
                type="button"
                disabled={deleteMutation.isPending}
                onClick={() => {
                  if (confirm("Hapus transaksi ini secara permanen?")) {
                    deleteMutation.mutate(selectedTx.id);
                  }
                }}
                className="px-3 py-2 rounded-xl bg-rose-500/10 hover:bg-rose-500/20 text-rose-600 font-semibold text-xs transition-colors cursor-pointer"
              >
                {deleteMutation.isPending ? "Menghapus..." : "Hapus"}
              </button>

              <button
                type="button"
                disabled={updateMutation.isPending}
                onClick={() => updateMutation.mutate()}
                className="btn-charcoal flex-1 py-2 text-xs font-semibold rounded-xl"
              >
                {updateMutation.isPending ? "Menyimpan..." : "Simpan Perubahan"}
              </button>
            </div>
          )
        }
      >
        {selectedTx && (
          <div className="space-y-4 text-xs">
            {editError && (
              <div className="p-2.5 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 text-xs">
                {editError}
              </div>
            )}

            {/* Amount Field */}
            <div>
              <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
                Nominal (IDR)
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={editAmount}
                onChange={(e) => setEditAmount(formatNumberWithDots(e.target.value))}
                className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] px-3.5 py-2 text-xl font-bold tabular text-[var(--text-primary)]"
              />
            </div>

            {/* Notes Field */}
            <div>
              <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
                Keterangan / Catatan
              </label>
              <input
                type="text"
                value={editNotes}
                onChange={(e) => setEditNotes(e.target.value)}
                placeholder="Catatan transaksi..."
                className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] px-3 py-2 text-xs text-[var(--text-primary)]"
              />
            </div>

            {/* Account Selector */}
            <div>
              <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
                Rekening / Dompet
              </label>
              <select
                value={editAccountId}
                onChange={(e) => setEditAccountId(e.target.value)}
                className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3 py-2 text-xs text-[var(--text-primary)]"
              >
                <AccountSelectOptions accounts={accounts} allowParentSelection={true} />
              </select>
            </div>

            {/* Category Selector */}
            <div>
              <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
                Kategori
              </label>
              <select
                value={editCategoryId}
                onChange={(e) => setEditCategoryId(e.target.value)}
                className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3 py-2 text-xs text-[var(--text-primary)]"
              >
                <option value="">Tanpa Kategori (Umum)</option>
                {categories.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name} ({c.kind === "income" ? "Masuk" : "Keluar"})
                  </option>
                ))}
              </select>
            </div>

            {/* Kakeibo Pillar Chips (If expense) */}
            {selectedTx.type === "expense" && (
              <div>
                <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1.5">
                  Pilar Kakeibo
                </label>
                <div className="grid grid-cols-3 gap-2">
                  {[
                    { key: "need", label: "🍞 Pokok" },
                    { key: "want", label: "👑 Keinginan" },
                    { key: "saving", label: "💎 Tabungan" },
                  ].map((p) => (
                    <button
                      key={p.key}
                      type="button"
                      onClick={() => setEditKakeiboType(p.key as any)}
                      className={cn(
                        "py-1.5 px-2 rounded-xl text-xs font-bold border transition-colors cursor-pointer text-center",
                        editKakeiboType === p.key
                          ? "bg-[#1E201E] text-white border-transparent"
                          : "bg-[var(--canvas-subtle)] text-[var(--text-secondary)] border-[var(--border-structural)] hover:bg-[var(--canvas-app)]"
                      )}
                    >
                      {p.label}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Date Field */}
            <div>
              <label className="text-[11px] font-semibold text-[var(--text-muted)] block mb-1">
                Waktu Transaksi
              </label>
              <input
                type="datetime-local"
                value={editDate}
                onChange={(e) => setEditDate(e.target.value)}
                className="w-full rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-subtle)] px-3 py-2 text-xs text-[var(--text-primary)]"
              />
            </div>

            {/* Receipt Preview */}
            {selectedTx.receipt_path && (
              <div className="space-y-1">
                <span className="text-[11px] font-bold text-[var(--text-muted)]">Foto Struk / Lampiran</span>
                <div className="rounded-2xl border border-[var(--border-structural)] overflow-hidden bg-[var(--canvas-subtle)] max-h-60 flex items-center justify-center">
                  <img
                    src={`/api/receipts/${selectedTx.receipt_path}`}
                    alt="Bukti Struk"
                    className="object-contain w-full h-full"
                  />
                </div>
              </div>
            )}
          </div>
        )}
      </DetailSheet>

      {/* Global Recurring Rules Modal */}
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => setRecurringModalOpen(false)}
      />
    </div>
  );
}
