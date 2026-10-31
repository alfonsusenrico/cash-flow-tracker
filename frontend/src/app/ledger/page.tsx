"use client";

import {
  Suspense,
  useState,
  useMemo,
  useEffect,
  useRef,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent,
  type PointerEvent as ReactPointerEvent,
} from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter, useSearchParams } from "next/navigation";
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
import {
  DEFAULT_LEDGER_FILTERS,
  UNCATEGORIZED,
  hasDateRange,
  hasRowFilters,
  ledgerApiParams,
  ledgerFilterQuery,
  parseLedgerFilters,
  type LedgerFilters,
  type LedgerPeriod,
  type LedgerReviewFilter,
  type LedgerTypeFilter,
} from "@/lib/ledgerFilters";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { HabitMark, ReviewStatusMark, feeLinkText, type FeeLinkFields } from "@/components/ledger/LedgerRowMarks";
import { InvestmentTopupModal } from "@/components/ui/InvestmentTopupModal";
import { ExpenseTopupConversionModal } from "@/components/ui/ExpenseTopupConversionModal";
import { canConvertExpenseToTopup } from "@/lib/investmentTopups";
import { KAKEIBO_PILLARS, type KakeiboPillar, kakeiboPillarInfo, pillarOrNeed } from "@/lib/kakeibo";
import {
  MERCHANT_MAX_LENGTH,
  categoryOffsetsSpending,
  classifierLabel,
  habitBasisText,
  habitRecordLabel,
  isHabitRecorded,
  needsOwnerReview,
  offsetCategoryOptions,
  reviewStatusLabel,
  sourceLabel,
  type HabitBasis,
} from "@/lib/provenance";

interface TransactionItem extends FeeLinkFields {
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
  investment_topup_id?: string | null;
  movement_kind?: "investment_topup" | null;
  movement_role?: "outbound" | "inbound" | null;
  transfer_target_account_id?: string | null;
  transfer_target_account_name?: string | null;
  is_inferred_transfer?: boolean;
  source?: string;
  merchant?: string | null;
  review_status?: string;
  classified_by?: string | null;
  habit_basis?: HabitBasis | null;
  offset_category_id?: string | null;
  offset_category_name?: string | null;
  is_reimbursement?: boolean;
}

interface CashFlowMetrics {
  inflow: number;
  outflow: number;
  net: number;
  /** Outflow before reimbursement offsets; outflow is net of them. */
  gross_outflow?: number;
  reimbursement_offset?: number;
}

interface CycleCashFlowMetrics extends CashFlowMetrics {
  start: string;
  end: string;
  label?: string;
}

interface PeriodCashFlowMetrics extends CashFlowMetrics {
  basis: "cycle" | "month" | null;
  label: string | null;
  start: string | null;
  end: string | null;
}

interface TransactionSummary {
  cycle: CycleCashFlowMetrics;
  cumulative: CashFlowMetrics;
  period?: PeriodCashFlowMetrics | null;
}

const REVIEW_FILTER_OPTIONS: { value: LedgerReviewFilter; label: string }[] = [
  { value: "all", label: "Semua status" },
  { value: "needs_review", label: "Perlu ditinjau" },
  { value: "auto", label: "Otomatis, belum ditinjau" },
  { value: "reviewed", label: "Sudah ditinjau" },
];

const PERIOD_OPTIONS: { value: LedgerPeriod; label: string }[] = [
  { value: "all", label: "Semua" },
  { value: "cycle", label: "Siklus gajian" },
  { value: "month", label: "Bulan kalender" },
];

function shortDate(value: string): string {
  return new Date(value).toLocaleDateString("id-ID", { day: "numeric", month: "short" });
}

function rangeLabel(from: string | null, to: string | null): string {
  if (from && to) return `${shortDate(from)} – ${shortDate(to)}`;
  return from ? `sejak ${shortDate(from)}` : `sampai ${shortDate(to ?? "")}`;
}

function periodLabel(period: PeriodCashFlowMetrics | null | undefined): string {
  if (!period?.start || !period.end) return "Memuat…";
  if (period.label) return period.label;
  return `${shortDate(period.start)} – ${shortDate(period.end)}`;
}

export default function LedgerPage() {
  // The page reads its filters from the URL, which needs a suspense boundary for static rendering.
  return (
    <Suspense fallback={null}>
      <LedgerPageContent />
    </Suspense>
  );
}

function LedgerPageContent() {
  const holdTimerRef = useRef<{
    timer: number;
    pointerId: number;
    startX: number;
    startY: number;
  } | null>(null);
  const suppressNextClickRef = useRef(false);
  const holdTriggeredRef = useRef(false);
  const suppressClickResetRef = useRef<number | null>(null);
  const qc = useQueryClient();
  const { openQuickAdd, bal } = useAppCtx();
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchKey = searchParams?.toString() ?? "";

  // Filters live in the URL so a report figure can link to the rows behind it.
  const filters = useMemo(() => parseLedgerFilters(new URLSearchParams(searchKey)), [searchKey]);
  const typeFilter = filters.type;
  const accountFilter = filters.account;
  const categoryFilter = filters.category;
  const rowFiltersActive = hasRowFilters(filters);
  const editTargetId = useMemo(() => new URLSearchParams(searchKey).get("edit"), [searchKey]);
  const returnTo = useMemo(() => new URLSearchParams(searchKey).get("return"), [searchKey]);
  const [searchQuery, setSearchQuery] = useState(filters.q);
  // A report link may carry search text; it seeds the search box, which the owner can then edit.
  useEffect(() => setSearchQuery(filters.q), [filters.q]);
  const rangeScoped = hasDateRange(filters);
  const periodScoped = filters.period !== "all" || rangeScoped;
  const [page, setPage] = useState(0);
  const [linkError, setLinkError] = useState("");
  const updateFilters = (patch: Partial<LedgerFilters>) => {
    const query = ledgerFilterQuery({ ...filters, ...patch });
    router.replace(query ? `/ledger?${query}` : "/ledger", { scroll: false });
    setPage(0);
  };
  const pageSize = 50;
  const [selectedTransactions, setSelectedTransactions] = useState<TransactionItem[]>([]);
  const selectionMode = selectedTransactions.length > 0;
  const [mergeWarningAmounts, setMergeWarningAmounts] = useState<{ expense: number; income: number } | null>(null);
  const [mergeError, setMergeError] = useState("");
  const [preparingMerge, setPreparingMerge] = useState(false);

  useEffect(() => {
    return () => {
      if (holdTimerRef.current) window.clearTimeout(holdTimerRef.current.timer);
      if (suppressClickResetRef.current) window.clearTimeout(suppressClickResetRef.current);
    };
  }, []);

  // Selected for Edit/Detail
  const [editingTx, setEditingTx] = useState<TransactionItem | null>(null);
  const [topupExpense, setTopupExpense] = useState<TransactionItem | null>(null);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);
  const [filterDrawerOpen, setFilterDrawerOpen] = useState(false);
  const [summaryScope, setSummaryScope] = useState<"cycle" | "cumulative">("cycle");

  // Edit form state
  const [editAmount, setEditAmount] = useState("");
  const [editType, setEditType] = useState<"expense" | "income">("expense");
  const [editKakeiboType, setEditKakeiboType] = useState<KakeiboPillar>("need");
  const [editHasKakeiboOverride, setEditHasKakeiboOverride] = useState(false);
  const [editReceipt, setEditReceipt] = useState<File | null>(null);
  const [receiptRetryTransactionId, setReceiptRetryTransactionId] = useState<string | null>(null);
  const [editNotes, setEditNotes] = useState("");
  const [editMerchant, setEditMerchant] = useState("");
  const [editOffsetCategoryId, setEditOffsetCategoryId] = useState("");
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
    const p = ledgerApiParams(filters);
    p.set("limit", pageSize.toString());
    p.set("offset", (page * pageSize).toString());
    if (!searchQuery.trim() && filters.period === "all" && !rowFiltersActive) {
      p.set("logical_movements", "true");
    }
    if (searchQuery.trim()) p.set("q", searchQuery.trim());
    return p.toString();
  }, [searchQuery, filters, rowFiltersActive, page]);

  // Fetch Transactions
  const { data: txData, isLoading } = useQuery<{
    ok: boolean;
    total: number;
    transactions: TransactionItem[];
    summary?: TransactionSummary;
  }>({
    queryKey: queryKeys.transactions.ledger(queryParams),
    queryFn: () => api.get(`/transactions?${queryParams}`),
  });

  const transactions = useMemo(() => txData?.transactions ?? [], [txData?.transactions]);
  const totalCount = txData?.total ?? 0;
  const totalPages = Math.ceil(totalCount / pageSize);

  // Consolidate paired internal movements into single visual entries in all-account ledger
  const displayTransactions = useMemo(() => {
    return consolidateLedgerMovements(transactions, !rowFiltersActive);
  }, [transactions, rowFiltersActive]);

  // Compute page totals (used as resilient fallback if backend summary is omitted)
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

  const summary = txData?.summary;
  const periodSummary = periodScoped ? summary?.period : null;
  const cycleSummary = summary?.cycle;
  const cumulativeSummary = summary?.cumulative;

  const cycleInflow = cycleSummary ? cycleSummary.inflow : pageInflow;
  const cycleOutflow = cycleSummary ? cycleSummary.outflow : pageOutflow;
  const cycleNet = cycleSummary ? cycleSummary.net : (cycleInflow - cycleOutflow);

  const cumulativeInflow = cumulativeSummary ? cumulativeSummary.inflow : pageInflow;
  const cumulativeOutflow = cumulativeSummary ? cumulativeSummary.outflow : pageOutflow;
  const cumulativeNet = cumulativeSummary ? cumulativeSummary.net : (cumulativeInflow - cumulativeOutflow);

  const cycleFigures = { inflow: cycleInflow, outflow: cycleOutflow, net: cycleNet };
  const cumulativeFigures = { inflow: cumulativeInflow, outflow: cumulativeOutflow, net: cumulativeNet };
  // With a period filter the filtered rows are the period, so its figures replace the cycle/cumulative pair.
  const scopeLabel = periodScoped ? "Periode" : summaryScope === "cycle" ? "Siklus" : "Kumulatif";
  const shown = periodScoped
    ? (periodSummary ?? { inflow: 0, outflow: 0, net: 0 })
    : summaryScope === "cycle" ? cycleFigures : cumulativeFigures;
  const other = periodScoped ? null : summaryScope === "cycle" ? cumulativeFigures : cycleFigures;
  const shownOffset = periodScoped
    ? periodSummary?.reimbursement_offset
    : summaryScope === "cycle" ? cycleSummary?.reimbursement_offset : cumulativeSummary?.reimbursement_offset;

  const cycleStart = cycleSummary?.start;
  const cycleEnd = cycleSummary?.end;

  const cycleDateRangeLabel = useMemo(() => {
    if (cycleSummary?.label) return cycleSummary.label;
    if (!cycleStart || !cycleEnd) return null;
    const s = new Date(cycleStart);
    const e = new Date(cycleEnd);
    const startStr = s.toLocaleDateString("id-ID", { day: "numeric", month: "short" });
    const endStr = e.toLocaleDateString("id-ID", { day: "numeric", month: "short" });
    return `${startStr} – ${endStr}`;
  }, [cycleStart, cycleEnd, cycleSummary?.label]);

  // Open Edit Modal
  const handleOpenEdit = (tx: TransactionItem) => {
    setEditingTx(tx);
    setEditAmount(formatNumberWithDots(tx.amount));
    setEditType(tx.type);
    setEditKakeiboType(pillarOrNeed(tx.kakeibo_type));
    const originalCategory = categories.find((category) => category.id === tx.category_id);
    setEditHasKakeiboOverride(Boolean(
      tx.kakeibo_type && originalCategory?.kakeibo_type && tx.kakeibo_type !== originalCategory.kakeibo_type,
    ));
    setEditReceipt(null);
    setReceiptRetryTransactionId(null);
    setEditNotes(tx.notes || "");
    setEditMerchant(tx.merchant || "");
    setEditOffsetCategoryId(tx.offset_category_id || "");
    setEditAccountId(tx.account_id);
    setEditCategoryId(tx.category_id || "");
    setEditGoalId(tx.goal_id || "");
    setEditObligationId(tx.obligation_id || "");
    setEditDebtRows(tx.type === "expense" ? (tx.obligation_allocations ?? (tx.obligation_id ? [{ obligation_id: tx.obligation_id, obligation_name: tx.obligation_name || "", amount: tx.amount }] : [])).map((item) => ({ obligation_id: item.obligation_id, obligation_name: item.obligation_name, amount: formatNumberWithDots(item.amount) })) : []);
    setEditDate(tx.date ? toDatetimeLocal(tx.date) : "");
    setEditError("");
  };

  const editOffsetsSpending = editType === "income" && categoryOffsetsSpending(categories, editCategoryId);
  const offsetCategories = offsetCategoryOptions(categories);

  // `?edit=<id>` opens the edit dialog of one row, for example from the review page.
  const openedEditTarget = useRef<string | null>(null);
  useEffect(() => {
    if (!editTargetId || openedEditTarget.current === editTargetId) return;
    openedEditTarget.current = editTargetId;
    api.get<{ transaction: TransactionItem }>(`/transactions/${editTargetId}`)
      .then((response) => handleOpenEdit(response.transaction))
      .catch(() => setLinkError("Transaksi yang ingin diubah tidak ditemukan atau sudah dihapus."));
    // handleOpenEdit reads the categories of this render; the dialog only needs them once loaded.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [editTargetId]);

  const closeEdit = () => {
    setEditingTx(null);
    if (!editTargetId) return;
    openedEditTarget.current = null;
    if (returnTo === "review") {
      router.push("/review");
      return;
    }
    const query = ledgerFilterQuery(filters);
    router.replace(query ? `/ledger?${query}` : "/ledger", { scroll: false });
  };

  const refreshTransactionViews = () => {
    qc.invalidateQueries({ queryKey: queryKeys.review.all });
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
        merchant: editMerchant.trim() || null,
        ...(editOffsetsSpending ? { offset_category_id: editOffsetCategoryId || null } : {}),
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
      closeEdit();
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal memperbarui transaksi");
    },
  });

  const reviewMutation = useMutation({
    mutationFn: async (tx: TransactionItem) => api.patch(`/transactions/${tx.id}`, { review_status: "reviewed" }),
    onSuccess: (_result, tx) => {
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.review.all });
      setEditingTx((current) => (current && current.id === tx.id ? { ...current, review_status: "reviewed" } : current));
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal menandai transaksi");
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
      closeEdit();
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
      qc.invalidateQueries({ queryKey: queryKeys.review.all });
      closeEdit();
    },
    onError: (err: any) => {
      setEditError(err?.message || "Gagal menghapus transaksi");
    },
  });

  const mergeMutation = useMutation({
    mutationFn: async ({ expenseId, incomeId }: { expenseId: string; incomeId: string }) => {
      return api.post("/movements/merge", {
        expense_transaction_id: expenseId,
        income_transaction_id: incomeId,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.categories });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
      setSelectedTransactions([]);
      setMergeWarningAmounts(null);
      setPage(0);
      setMergeError("");
    },
    onError: (error: Error) => {
      setMergeError(error.message);
    },
  });

  const getSelectionGroup = (tx: TransactionItem, selected: TransactionItem[]) => {
    if (tx.is_inferred_transfer && tx.partner_id) {
      const partner =
        transactions.find((item) => item.id === tx.partner_id) ??
        selected.find((item) => item.id === tx.partner_id);
      return partner ? [tx, partner] : [tx];
    }

    const inferredPair = selected.find(
      (item) =>
        item.is_inferred_transfer &&
        (item.id === tx.id || item.partner_id === tx.id),
    );
    if (!inferredPair?.partner_id) return [tx];

    const partner =
      transactions.find((item) => item.id === inferredPair.partner_id) ??
      selected.find((item) => item.id === inferredPair.partner_id);
    if (partner && partner.id !== inferredPair.id) return [inferredPair, partner];
    if (tx.id !== inferredPair.id) return [inferredPair, tx];
    return [inferredPair];
  };

  const toggleSelection = (tx: TransactionItem) => {
    if (tx.movement_id || mergeMutation.isPending || preparingMerge) return;
    setMergeError("");
    setSelectedTransactions((selected) => {
      const group = getSelectionGroup(tx, selected);
      const groupIds = new Set(group.map((item) => item.id));
      const selectedGroup = group.filter((item) =>
        selected.some((selectedItem) => selectedItem.id === item.id),
      );

      if (selectedGroup.length > 0) {
        return selected.filter((item) => !groupIds.has(item.id));
      }

      if (selected.length + group.length > 2) return selected;
      return [...selected, ...group];
    });
  };

  const selectFromHold = (tx: TransactionItem) => {
    if (tx.movement_id || mergeMutation.isPending) return;
    setMergeError("");
    setSelectedTransactions((selected) => {
      if (selected.length > 0) return selected;
      return getSelectionGroup(tx, selected);
    });
  };

  const clearHoldTimer = () => {
    if (!holdTimerRef.current) return;
    window.clearTimeout(holdTimerRef.current.timer);
    holdTimerRef.current = null;
  };

  const handleHoldPointerDown = (tx: TransactionItem, event: ReactPointerEvent<HTMLElement>) => {
    if (selectionMode || tx.movement_id || event.button !== 0) return;
    clearHoldTimer();
    holdTriggeredRef.current = false;
    const pointerId = event.pointerId;
    const startX = event.clientX;
    const startY = event.clientY;
    const timer = window.setTimeout(() => {
      holdTimerRef.current = null;
      holdTriggeredRef.current = true;
      selectFromHold(tx);
    }, 1000);
    holdTimerRef.current = { timer, pointerId, startX, startY };
  };

  const handleHoldPointerMove = (event: ReactPointerEvent<HTMLElement>) => {
    const pendingHold = holdTimerRef.current;
    if (!pendingHold || pendingHold.pointerId !== event.pointerId) return;
    if (Math.hypot(event.clientX - pendingHold.startX, event.clientY - pendingHold.startY) > 10) {
      clearHoldTimer();
    }
  };

  const handleHoldPointerEnd = () => {
    clearHoldTimer();
    if (!holdTriggeredRef.current) return;
    holdTriggeredRef.current = false;
    suppressNextClickRef.current = true;
    if (suppressClickResetRef.current) window.clearTimeout(suppressClickResetRef.current);
    suppressClickResetRef.current = window.setTimeout(() => {
      suppressNextClickRef.current = false;
      suppressClickResetRef.current = null;
    }, 750);
  };

  const handleHoldContextMenu = (event: ReactMouseEvent<HTMLElement>) => {
    if (holdTriggeredRef.current || suppressNextClickRef.current) event.preventDefault();
  };

  const handleRowActivation = (tx: TransactionItem) => {
    if (suppressNextClickRef.current) {
      suppressNextClickRef.current = false;
      if (suppressClickResetRef.current) window.clearTimeout(suppressClickResetRef.current);
      suppressClickResetRef.current = null;
      return;
    }
    if (selectionMode) {
      toggleSelection(tx);
      return;
    }
    handleOpenEdit(tx);
  };

  const activeFilterLabels = [
    filters.type !== "all" ? (filters.type === "expense" ? "Uang keluar" : "Uang masuk") : null,
    filters.category === UNCATEGORIZED
      ? "Tanpa kategori"
      : filters.category !== "all"
        ? `Kategori: ${categories.find((category) => category.id === filters.category)?.name ?? "…"}`
        : null,
    filters.kakeibo !== "all" ? `Pilar: ${kakeiboPillarInfo(filters.kakeibo)?.label ?? filters.kakeibo}` : null,
    filters.account !== "all" ? `Rekening: ${accounts.find((account) => account.id === filters.account)?.name ?? "…"}` : null,
    filters.review !== "all" ? REVIEW_FILTER_OPTIONS.find((option) => option.value === filters.review)?.label ?? null : null,
    rangeScoped ? `Rentang: ${rangeLabel(filters.from, filters.to)}` : null,
  ].filter((label): label is string => Boolean(label));

  const prepareMerge = async () => {
    if (selectedTransactions.length !== 2 || preparingMerge || mergeMutation.isPending) return;
    setPreparingMerge(true);
    setMergeError("");
    try {
      const details = await Promise.all(selectedTransactions.map(async (tx) => {
        const response = await api.get<{ transaction: TransactionItem }>(`/transactions/${tx.id}`);
        return response.transaction;
      }));
      const expense = details.find((tx) => tx.type === "expense");
      const income = details.find((tx) => tx.type === "income");
      if (!expense || !income) {
        throw new Error("Pilih satu transaksi uang keluar dan satu uang masuk.");
      }
      if (expense.amount !== income.amount) {
        setMergeWarningAmounts({ expense: expense.amount, income: income.amount });
        return;
      }
      if (expense.movement_id || income.movement_id || expense.account_id === income.account_id) {
        throw new Error("Pilih transaksi yang belum tertaut dari dua rekening berbeda.");
      }
      mergeMutation.mutate({ expenseId: expense.id, incomeId: income.id });
    } catch (error) {
      setMergeError(error instanceof Error ? error.message : "Gagal memeriksa transaksi terpilih.");
    } finally {
      setPreparingMerge(false);
    }
  };

  return (
    <div className="space-y-6">
      <span className="sr-only" id="ledger-selection-instructions">
        Klik singkat untuk membuka transaksi. Tahan satu detik atau tekan Spasi untuk mulai memilih. Dalam mode pilih, klik baris untuk memilih atau membatalkan pilihan.
      </span>
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

      {linkError && (
        <p role="alert" className="rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-xs font-medium text-rose-700 dark:text-rose-300">
          {linkError}
        </p>
      )}

      {/* Period: all time, payday cycle or calendar month, kept in the URL */}
      <div className="flex flex-wrap items-center gap-2">
        <div role="group" aria-label="Periode" className="inline-flex items-center rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-0.5 text-xs">
          {PERIOD_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              aria-pressed={filters.period === option.value}
              onClick={() => updateFilters({ period: option.value, offset: 0 })}
              className={cn(
                "min-h-10 rounded-lg px-3 font-medium transition-colors focus-visible:outline-2 focus-visible:outline-[var(--primary)]",
                filters.period === option.value
                  ? "bg-[var(--surface)] font-semibold text-[var(--text)] shadow-xs"
                  : "text-[var(--text-secondary)] hover:text-[var(--text)]",
              )}
            >
              {option.label}
            </button>
          ))}
        </div>
        {filters.period !== "all" && (
          <PeriodStepper
            unit={filters.period === "month" ? "Bulan" : "Siklus"}
            label={periodLabel(summary?.period)}
            offset={filters.offset}
            onChange={(offset) => updateFilters({ offset })}
          />
        )}
      </div>

      {rowFiltersActive && (
        <div role="group" aria-label="Filter aktif" className="flex flex-wrap items-center gap-2 text-xs">
          <span className="font-semibold text-[var(--muted)]">Filter aktif:</span>
          {activeFilterLabels.map((label) => (
            <span key={label} className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-2.5 py-1 font-medium text-[var(--text)]">
              {label}
            </span>
          ))}
          <button
            type="button"
            onClick={() => updateFilters({ ...DEFAULT_LEDGER_FILTERS, period: filters.period, offset: filters.offset })}
            className="min-h-10 px-2 font-semibold text-[var(--text)] underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
          >
            Hapus filter
          </button>
        </div>
      )}

      {/* 2. Ledger Volume Summary Bar */}
      {/* Mobile 1-Row Summary Ribbon (< sm) */}
      <div className="sm:hidden space-y-2">
        <div className="flex items-center justify-between px-0.5">
          <div className="flex items-center gap-1.5">
            <span className="text-[11px] font-bold uppercase tracking-wider text-[var(--muted)]">Arus Kas</span>
            {!periodScoped && cycleDateRangeLabel && summaryScope === "cycle" && (
              <span className="text-[10px] text-[var(--muted)] font-medium">({cycleDateRangeLabel})</span>
            )}
          </div>
          {!periodScoped && (
          <div className="inline-flex items-center p-0.5 rounded-lg border border-[var(--border)] bg-[var(--surface-raised)] text-[10px]">
            <button
              type="button"
              onClick={() => setSummaryScope("cycle")}
              className={cn(
                "px-2 py-0.5 rounded-md font-medium transition-all duration-150",
                summaryScope === "cycle"
                  ? "bg-[var(--surface)] text-[var(--foreground)] shadow-xs font-semibold"
                  : "text-[var(--muted)] hover:text-[var(--foreground)]"
              )}
              aria-pressed={summaryScope === "cycle"}
            >
              Siklus
            </button>
            <button
              type="button"
              onClick={() => setSummaryScope("cumulative")}
              className={cn(
                "px-2 py-0.5 rounded-md font-medium transition-all duration-150",
                summaryScope === "cumulative"
                  ? "bg-[var(--surface)] text-[var(--foreground)] shadow-xs font-semibold"
                  : "text-[var(--muted)] hover:text-[var(--foreground)]"
              )}
              aria-pressed={summaryScope === "cumulative"}
            >
              Kumulatif
            </button>
          </div>
          )}
        </div>

        <div className="flex items-center justify-between p-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs">
          <div className="flex-1 text-center">
            <div className="text-[10px] uppercase font-bold text-[var(--muted)]">Masuk</div>
            <div className="text-xs font-bold text-emerald-500 tabular mt-0.5">
              +{bal(shown.inflow)}
            </div>
          </div>
          <div className="h-6 w-px bg-[var(--border)]" />
          <div className="flex-1 text-center">
            <div className="text-[10px] uppercase font-bold text-[var(--muted)]">Keluar</div>
            <div className="text-xs font-bold text-rose-500 tabular mt-0.5">
              -{bal(shown.outflow)}
            </div>
          </div>
          <div className="h-6 w-px bg-[var(--border)]" />
          <div className="flex-1 text-center">
            <div className="text-[10px] uppercase font-bold text-[var(--muted)]">Net</div>
            <div
              className={cn(
                "text-xs font-bold tabular mt-0.5",
                (shown.net) >= 0 ? "text-emerald-500" : "text-rose-500"
              )}
            >
              {(shown.net) >= 0 ? "+" : ""}
              {bal(shown.net)}
            </div>
          </div>
        </div>
        {shownOffset ? (
          <p className="px-0.5 text-[11px] text-[var(--muted)] tabular">Uang keluar sudah dikurangi ganti uang {bal(shownOffset)}</p>
        ) : null}
      </div>

      {/* Desktop 3-Card Summary (>= sm) */}
      <div className="hidden sm:block space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">Arus Kas</span>
            {!periodScoped && cycleDateRangeLabel && (
              <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-[var(--surface-raised)] border border-[var(--border)] text-[var(--foreground)]">
                <Icon name="calendar" className="h-3.5 w-3.5 text-[var(--muted)]" />
                {cycleSummary?.label ? cycleDateRangeLabel : `Siklus ${cycleDateRangeLabel}`}
              </span>
            )}
          </div>
          {!periodScoped && (
          <div className="inline-flex items-center p-0.5 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] text-xs">
            <button
              type="button"
              onClick={() => setSummaryScope("cycle")}
              className={cn(
                "px-3 py-1 rounded-lg font-medium transition-all duration-150",
                summaryScope === "cycle"
                  ? "bg-[var(--surface)] text-[var(--foreground)] shadow-xs font-semibold"
                  : "text-[var(--muted)] hover:text-[var(--foreground)]"
              )}
              aria-pressed={summaryScope === "cycle"}
            >
              Siklus Berjalan
            </button>
            <button
              type="button"
              onClick={() => setSummaryScope("cumulative")}
              className={cn(
                "px-3 py-1 rounded-lg font-medium transition-all duration-150",
                summaryScope === "cumulative"
                  ? "bg-[var(--surface)] text-[var(--foreground)] shadow-xs font-semibold"
                  : "text-[var(--muted)] hover:text-[var(--foreground)]"
              )}
              aria-pressed={summaryScope === "cumulative"}
            >
              Total Kumulatif
            </button>
          </div>
          )}
        </div>

        <div className="grid grid-cols-3 gap-4">
          {/* Card 1: Inflow */}
          <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs flex flex-col justify-between">
            <div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--muted)] font-medium">
                  {`Uang Masuk (${scopeLabel})`}
                </span>
                <span className="text-[10px] font-semibold uppercase tracking-wider text-emerald-600 dark:text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded-md">
                  Inflow
                </span>
              </div>
              <div className="text-2xl font-bold tabular tracking-tight text-emerald-500 mt-1.5 select-all">
                +{bal(shown.inflow)}
              </div>
            </div>
            {other && (
            <div className="mt-3 pt-2.5 border-t border-[var(--border)] flex items-center justify-between text-xs text-[var(--muted)]">
              <span>{summaryScope === "cycle" ? "Kumulatif:" : "Siklus ini:"}</span>
              <span className="font-semibold tabular text-[var(--foreground)]">
                +{bal(other.inflow)}
              </span>
            </div>
            )}
          </div>

          {/* Card 2: Outflow */}
          <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs flex flex-col justify-between">
            <div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--muted)] font-medium">
                  {`Uang Keluar (${scopeLabel})`}
                </span>
                <span className="text-[10px] font-semibold uppercase tracking-wider text-rose-600 dark:text-rose-400 bg-rose-500/10 px-2 py-0.5 rounded-md">
                  Outflow
                </span>
              </div>
              <div className="text-2xl font-bold tabular tracking-tight text-rose-500 mt-1.5 select-all">
                -{bal(shown.outflow)}
              </div>
              {shownOffset ? (
                <p className="mt-1 text-[11px] text-[var(--muted)] tabular">Sudah dikurangi ganti uang {bal(shownOffset)}</p>
              ) : null}
            </div>
            {other && (
            <div className="mt-3 pt-2.5 border-t border-[var(--border)] flex items-center justify-between text-xs text-[var(--muted)]">
              <span>{summaryScope === "cycle" ? "Kumulatif:" : "Siklus ini:"}</span>
              <span className="font-semibold tabular text-[var(--foreground)]">
                -{bal(other.outflow)}
              </span>
            </div>
            )}
          </div>

          {/* Card 3: Net */}
          <div className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs flex flex-col justify-between">
            <div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--muted)] font-medium">
                  {`Selisih Bersih (${scopeLabel})`}
                </span>
                <span
                  className={cn(
                    "text-[10px] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-md",
                    (shown.net) >= 0
                      ? "text-emerald-600 dark:text-emerald-400 bg-emerald-500/10"
                      : "text-rose-600 dark:text-rose-400 bg-rose-500/10"
                  )}
                >
                  Arus Bersih
                </span>
              </div>
              <div
                className={cn(
                  "text-2xl font-bold tabular tracking-tight mt-1.5 select-all",
                  (shown.net) >= 0 ? "text-emerald-500" : "text-rose-500"
                )}
              >
                {(shown.net) >= 0 ? "+" : ""}
                {bal(shown.net)}
              </div>
            </div>
            {other && (
            <div className="mt-3 pt-2.5 border-t border-[var(--border)] flex items-center justify-between text-xs text-[var(--muted)]">
              <span>{summaryScope === "cycle" ? "Kumulatif:" : "Siklus ini:"}</span>
              <span
                className={cn(
                  "font-semibold tabular",
                  (other.net) >= 0 ? "text-emerald-500" : "text-rose-500"
                )}
              >
                {(other.net) >= 0 ? "+" : ""}
                {bal(other.net)}
              </span>
            </div>
            )}
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
                updateFilters({ type: key as LedgerTypeFilter });
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
              accountFilter !== "all" || categoryFilter !== "all" || filters.kakeibo !== "all" || filters.review !== "all"
                ? "bg-emerald-500/15 text-emerald-500 border-emerald-500/30"
                : "bg-[var(--surface)] text-[var(--text)] border-[var(--border)] hover:bg-[var(--surface-raised)]"
            )}
          >
            <Icon name="filter" className="h-3.5 w-3.5" />
            <span>Filter</span>
            {(accountFilter !== "all" || categoryFilter !== "all" || filters.kakeibo !== "all" || filters.review !== "all") && (
              <span className="h-2 w-2 rounded-full bg-emerald-500" aria-hidden="true" />
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
              aria-label="Filter rekening"
              value={accountFilter}
              onChange={(e) => {
                updateFilters({ account: e.target.value });
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
              aria-label="Filter kategori"
              value={categoryFilter}
              onChange={(e) => {
                updateFilters({ category: e.target.value });
                setPage(0);
              }}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] focus:outline-none focus:border-income"
            >
              <option value="all">Semua Kategori</option>
              <option value={UNCATEGORIZED}>Tanpa kategori</option>
              {categories.map((cat) => (
                <option key={cat.id} value={cat.id}>
                  {cat.name} ({cat.kind === "income" ? "Masuk" : "Keluar"})
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="grid grid-cols-12 gap-3 items-center">
          <div className="col-span-3">
            <label htmlFor="ledger-filter-pillar" className="sr-only">Filter pilar Kakeibo</label>
            <select
              id="ledger-filter-pillar"
              value={filters.kakeibo}
              onChange={(event) => updateFilters({ kakeibo: event.target.value as LedgerFilters["kakeibo"] })}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] focus:outline-none focus:border-income"
            >
              <option value="all">Semua Pilar</option>
              {KAKEIBO_PILLARS.map((pillar) => (
                <option key={pillar.value} value={pillar.value}>{pillar.label}</option>
              ))}
            </select>
          </div>
          <div className="col-span-3">
            <label htmlFor="ledger-filter-review" className="sr-only">Filter status tinjauan</label>
            <select
              id="ledger-filter-review"
              value={filters.review}
              onChange={(event) => updateFilters({ review: event.target.value as LedgerReviewFilter })}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] focus:outline-none focus:border-income"
            >
              {REVIEW_FILTER_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
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
                updateFilters({ type: key as LedgerTypeFilter });
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

          {(searchQuery || rowFiltersActive || filters.period !== "all") && (
            <button
              type="button"
              onClick={() => {
                setSearchQuery("");
                updateFilters(DEFAULT_LEDGER_FILTERS);
              }}
              className="ml-auto text-xs text-income hover:underline font-medium"
            >
              Hapus filter
            </button>
          )}
        </div>
      </div>

      {/* 4. Ledger Data Display */}
      <p aria-live="polite" className="text-[11px] text-[var(--muted)]">
        {selectionMode
          ? `${selectedTransactions.length}/2 dipilih · klik baris untuk mengubah pilihan`
          : "Tahan baris 1 detik untuk mulai memilih"}
      </p>
      {/* Mobile Feed (< lg) */}
      <div className="lg:hidden space-y-4">
        <MobileLedgerFeed
          transactions={displayTransactions}
          onOpenEdit={handleRowActivation}
          onHoldSelect={selectFromHold}
          onHoldPointerDown={(tx, event) => handleHoldPointerDown(tx, event)}
          onHoldPointerMove={handleHoldPointerMove}
          onHoldPointerEnd={handleHoldPointerEnd}
          onHoldContextMenu={handleHoldContextMenu}
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
                  <th className="py-3 px-4 font-semibold">Tanggal</th>
                  <th className="py-3 px-4 font-semibold">Jenis</th>
                  <th className="py-3 px-4 font-semibold">Keterangan / Catatan</th>
                  <th className="py-3 px-4 font-semibold">Kategori</th>
                  <th className="py-3 px-4 font-semibold">Rekening</th>
                  <th className="py-3 px-4 font-semibold">Target / Tagihan</th>
                  <th className="py-3 px-4 font-semibold text-right">Nominal</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)]">
                {displayTransactions.map((tx) => {
                  const isMovement =
                    tx.category_name === "Internal Movement" ||
                    !!tx.is_excluded_from_budget ||
                    (tx.notes?.toLowerCase().includes("pindah saldo") ?? false);
                  const isIncome = tx.type === "income";
                  const selectionGroup = getSelectionGroup(tx, selectedTransactions);
                  const selectedGroupCount = selectionGroup.filter((item) =>
                    selectedTransactions.some((selectedItem) => selectedItem.id === item.id),
                  ).length;
                  const isSelected = selectedGroupCount > 0;
                  const selectionBlocked =
                    selectionMode &&
                    !tx.movement_id &&
                    selectedGroupCount === 0 &&
                    selectedTransactions.length + selectionGroup.length > 2;
                  const selectionDisabled = selectionMode && (Boolean(tx.movement_id) || selectionBlocked);

                  return (
                    <tr
                      key={tx.id}
                      onClick={() => handleRowActivation(tx)}
                      onPointerDown={(event) => handleHoldPointerDown(tx, event)}
                      onPointerMove={handleHoldPointerMove}
                      onPointerUp={handleHoldPointerEnd}
                      onPointerCancel={handleHoldPointerEnd}
                      onContextMenu={handleHoldContextMenu}
                      onKeyDown={(event: ReactKeyboardEvent<HTMLTableRowElement>) => {
                        if (event.key === " ") {
                          event.preventDefault();
                          if (selectionMode) toggleSelection(tx);
                          else selectFromHold(tx);
                        } else if (event.key === "Enter") {
                          event.preventDefault();
                          handleRowActivation(tx);
                        }
                      }}
                      aria-label={`${selectionMode ? (tx.movement_id ? "Tergabung" : isSelected ? "Dipilih" : "Belum dipilih") : "Buka atau ubah"}: ${tx.notes || tx.category_name || tx.id}`}
                      aria-describedby="ledger-selection-instructions"
                      aria-keyshortcuts="Space Enter"
                      aria-disabled={selectionDisabled}
                      tabIndex={selectionDisabled ? -1 : 0}
                      className={cn(
                        "select-none hover:bg-[var(--surface-raised)]/60 transition-colors group focus-visible:outline-2 focus-visible:outline-inset focus-visible:outline-[var(--primary)]",
                        selectionDisabled ? "cursor-default" : "cursor-pointer",
                        selectionMode && isSelected && "bg-[#66CC55]/15 outline outline-1 -outline-offset-1 outline-[#66CC55]/60",
                      )}
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
                        {tx.is_consolidated_transfer || tx.is_inferred_transfer ? (
                          <span className={cn("inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-bold border tracking-wide", tx.investment_topup_id ? "bg-purple-100 text-purple-900 border-purple-200 dark:bg-purple-950 dark:text-purple-100 dark:border-purple-800" : "bg-sky-500/10 text-sky-500 border-sky-500/20")}>
                            <span className="text-xs leading-none">↔️</span>
                            <span>{tx.investment_topup_id ? "Top up Investasi" : "Pindah Saldo"}</span>
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
                            {tx.investment_topup_id ? "Top up Investasi" : isMovement
                              ? isIncome
                                ? "Pindah Saldo (Masuk)"
                                : "Pindah Saldo (Keluar)"
                            : isIncome
                              ? "Uang Masuk"
                              : "Uang Keluar"}
                          </span>
                        )}
                        {selectionMode && tx.movement_id && (
                          <span className="mt-1 block text-[10px] font-semibold text-sky-500">Tergabung</span>
                        )}
                      </td>

                      {/* Notes / Description */}
                      <td className="py-3.5 px-4 font-medium text-[var(--text)] max-w-xs">
                        <span className="flex min-w-0 items-center gap-1.5">
                          <span className="truncate group-hover:text-income transition-colors">
                            {tx.notes || (isMovement ? "Pindah Saldo" : tx.category_name || "Umum")}
                          </span>
                          <ReviewStatusMark status={tx.review_status} />
                          <HabitMark classifiedBy={tx.classified_by} basis={tx.habit_basis} />
                        </span>
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
                            <span>{tx.investment_topup_id ? "Top up Investasi" : isMovement ? "Pindah Saldo" : tx.category_name}</span>
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
                        {tx.fee_movement ? (
                          <span className="text-[11px] font-medium text-[var(--text-secondary)]">{feeLinkText(tx, bal)}</span>
                        ) : (tx.is_consolidated_transfer || tx.is_inferred_transfer) && tx.target_account_name ? (
                          <span className="block">
                            <span className="inline-flex items-center gap-1 text-[11px] font-semibold text-sky-500">
                              <span className="text-xs">→</span>
                              <span>{tx.target_account_name}</span>
                            </span>
                            {tx.movement_fee && (
                              <span className="block text-[10px] text-[var(--text-secondary)] tabular">{feeLinkText(tx, bal)}</span>
                            )}
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

      {selectedTransactions.length === 2 && (
        <div className="pointer-events-none fixed bottom-[calc(5.5rem+env(safe-area-inset-bottom,0px))] left-1/2 z-[60] flex w-[calc(100%-2rem)] max-w-sm -translate-x-1/2 flex-col items-center gap-2 lg:bottom-6">
          {mergeError && (
            <p role="alert" className="pointer-events-auto w-full max-w-sm rounded-xl border border-rose-500/30 bg-[var(--surface)] px-4 py-3 text-xs text-rose-500 shadow-lg">
              {mergeError}
            </p>
          )}
          <button
            type="button"
            disabled={preparingMerge || mergeMutation.isPending}
            onClick={prepareMerge}
            className="pointer-events-auto inline-flex min-h-12 w-full max-w-sm items-center justify-center gap-2 rounded-full bg-[#1E201E] px-5 text-sm font-semibold text-white shadow-xl transition-colors hover:bg-[#303330] disabled:cursor-wait disabled:opacity-70 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
          >
            <Icon name="repeat" className="h-4 w-4 text-[var(--accent-lime,#66CC55)]" />
            {preparingMerge ? "Memeriksa transaksi…" : mergeMutation.isPending ? "Menyimpan…" : "Jadikan Pindah Saldo"}
          </button>
        </div>
      )}

      <Modal
        open={Boolean(mergeWarningAmounts)}
        onClose={() => setMergeWarningAmounts(null)}
        title="Nominal tidak sama"
        centered
        hideCloseButton
      >
        {mergeWarningAmounts && (
          <div className="space-y-4 text-sm">
            <p role="alert" className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-3 text-[var(--text)]">
              Nominal uang keluar ({bal(mergeWarningAmounts.expense)}) berbeda dengan uang masuk ({bal(mergeWarningAmounts.income)}). Pindah saldo hanya dapat dibuat jika nominalnya sama. Tindakan dibatalkan.
            </p>
            <div className="flex justify-end border-t border-[var(--border)] pt-3">
              <button
                type="button"
                data-autofocus
                onClick={() => setMergeWarningAmounts(null)}
                className="min-h-11 rounded-xl bg-[#1E201E] px-5 font-semibold text-white focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
              >
                Ok
              </button>
            </div>
          </div>
        )}
      </Modal>

      <InternalMovementModal
        open={Boolean(editingTx?.movement_id && !editingTx?.investment_topup_id)}
        onClose={closeEdit}
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

      {editingTx?.investment_topup_id && <InvestmentTopupModal
        open
        onClose={closeEdit}
        product={accounts.find((account) => account.id === (editingTx.movement_role === "inbound" ? editingTx.account_id : editingTx.target_account_id ?? editingTx.transfer_target_account_id)) ?? {
          id: editingTx.movement_role === "inbound" ? editingTx.account_id : editingTx.target_account_id ?? editingTx.transfer_target_account_id ?? "",
          name: editingTx.movement_role === "inbound" ? editingTx.account_name : editingTx.target_account_name ?? editingTx.transfer_target_account_name ?? "Produk reksadana",
          type: "investment",
        }}
        allAccounts={accounts}
        editingTopup={{
          id: editingTx.investment_topup_id,
          source_account_id: editingTx.movement_role === "inbound" ? editingTx.transfer_target_account_id ?? "" : editingTx.account_id,
          target_account_id: editingTx.movement_role === "inbound" ? editingTx.account_id : editingTx.target_account_id ?? editingTx.transfer_target_account_id ?? "",
          amount: editingTx.amount, date: editingTx.date, notes: editingTx.notes,
        }}
      />}

      <ExpenseTopupConversionModal
        expense={topupExpense}
        accounts={accounts}
        onClose={() => setTopupExpense(null)}
        onConverted={() => setTopupExpense(null)}
      />

      {/* 5. Ordinary transaction edit modal */}
      {editingTx && !editingTx.movement_id && (
        <Modal
          open={Boolean(editingTx)}
          onClose={closeEdit}
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
            {editingTx.source && (
              <section aria-labelledby="ledger-edit-provenance-title" className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3">
                <h3 id="ledger-edit-provenance-title" className="sr-only">Asal transaksi</h3>
                <dl className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                  <div>
                    <dt className="text-[11px] font-medium text-[var(--muted)]">Sumber</dt>
                    <dd className="text-sm font-semibold text-[var(--text)]">{sourceLabel(editingTx.source)}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-medium text-[var(--muted)]">Status tinjauan</dt>
                    <dd className="text-sm font-semibold text-[var(--text)]">{reviewStatusLabel(editingTx.review_status)}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-medium text-[var(--muted)]">Diklasifikasi oleh</dt>
                    <dd className="text-sm font-semibold text-[var(--text)]">{classifierLabel(editingTx.classified_by) ?? "—"}</dd>
                  </div>
                </dl>
                {isHabitRecorded(editingTx.classified_by, editingTx.habit_basis) && (
                  <p className="mt-2 text-xs text-[var(--text-secondary)]">
                    <span className="font-semibold text-[var(--text)]">{habitRecordLabel(editingTx.category_name)}</span>
                    {" · "}
                    <span className="tabular-nums">{habitBasisText(editingTx.habit_basis)}</span>. Ubah kategorinya bila
                    keliru; pembayaran serupa berikutnya akan ditanyakan lagi.
                  </p>
                )}
                {needsOwnerReview(editingTx.review_status) && (
                  <button
                    type="button"
                    disabled={reviewMutation.isPending}
                    onClick={() => reviewMutation.mutate(editingTx)}
                    className="mt-3 inline-flex min-h-11 items-center gap-1.5 rounded-xl border border-[var(--border-strong)] bg-[var(--surface)] px-4 text-sm font-semibold text-[var(--text)] disabled:opacity-50"
                  >
                    <Icon name="check" className="h-4 w-4" aria-hidden="true" />
                    {reviewMutation.isPending ? "Menandai…" : "Tandai sudah ditinjau"}
                  </button>
                )}
              </section>
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
                        setEditKakeiboType(pillarOrNeed(pillar));
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

                {editOffsetsSpending && (
                  <div>
                    <label htmlFor="ledger-edit-offset-category" className="text-xs font-medium text-[var(--muted)] block mb-1">Mengurangi pengeluaran</label>
                    <select
                      id="ledger-edit-offset-category"
                      name="offset_category_id"
                      value={editOffsetCategoryId}
                      onChange={(event) => setEditOffsetCategoryId(event.target.value)}
                      aria-describedby="ledger-edit-offset-help"
                      className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                    >
                      <option value="">Belum dipilih</option>
                      {offsetCategories.map((cat) => (
                        <option key={cat.id} value={cat.id}>{cat.name}</option>
                      ))}
                    </select>
                    <p id="ledger-edit-offset-help" className="mt-1 text-[11px] text-[var(--muted)]">Uang ganti ini bukan pemasukan. Pilih kategori pengeluaran yang dibayar kembali.</p>
                  </div>
                )}

                {editType === "expense" && (
                  <div>
                    <div className="mb-1 flex items-center gap-1">
                      <label htmlFor="ledger-edit-kakeibo" className="text-xs font-medium text-[var(--muted)]">Pilar Kakeibo</label>
                      <InfoHelp label="Pilar Kakeibo">Mengikuti pilar kategori, kecuali Anda memilih pilar lain untuk transaksi ini.</InfoHelp>
                    </div>
                    <select id="ledger-edit-kakeibo" name="kakeibo_type" value={editKakeiboType} onChange={(event) => { setEditKakeiboType(event.target.value as typeof editKakeiboType); setEditHasKakeiboOverride(true); }} className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]">
                      {KAKEIBO_PILLARS.map((pillar) => (
                        <option key={pillar.value} value={pillar.value}>{pillar.label}</option>
                      ))}
                    </select>
                    <p className="mt-1 text-[11px] text-[var(--muted)]">{kakeiboPillarInfo(editKakeiboType)?.help}</p>
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
                  <label htmlFor="ledger-edit-merchant" className="text-xs font-medium text-[var(--muted)] block mb-1">Merchant / tempat (opsional)</label>
                  <input
                    id="ledger-edit-merchant"
                    name="merchant"
                    type="text"
                    autoComplete="off"
                    maxLength={MERCHANT_MAX_LENGTH}
                    value={editMerchant}
                    onChange={(e) => setEditMerchant(e.target.value)}
                    placeholder="Nama warung, toko, atau aplikasi"
                    className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
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
            {canConvertExpenseToTopup(editingTx, accounts) && (
              <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-purple-200 bg-purple-50 p-3 text-purple-900 dark:border-purple-800 dark:bg-purple-950 dark:text-purple-100">
                <p className="text-xs">Debit ini untuk membeli reksadana?</p>
                <button
                  type="button"
                  onClick={() => { setTopupExpense(editingTx); setEditingTx(null); }}
                  className="min-h-11 rounded-xl bg-[#1E201E] px-3.5 text-sm font-semibold text-white"
                >
                  Jadikan Top up Investasi
                </button>
              </div>
            )}
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
                  onClick={closeEdit}
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
                  updateFilters({ account: "all" });
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
                    updateFilters({ account: acc.id });
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
                  updateFilters({ category: "all" });
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
              <button
                type="button"
                aria-pressed={categoryFilter === UNCATEGORIZED}
                onClick={() => updateFilters({ category: UNCATEGORIZED })}
                className={cn(
                  "min-h-11 rounded-xl border px-3 text-xs font-medium transition-colors",
                  categoryFilter === UNCATEGORIZED
                    ? "border-[var(--text)] bg-[var(--text)] font-semibold text-[var(--surface)]"
                    : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--text)]"
                )}
              >
                Tanpa kategori
              </button>
              {categories.map((cat: any) => (
                <button
                  key={cat.id}
                  type="button"
                  aria-pressed={categoryFilter === cat.id}
                  onClick={() => {
                    updateFilters({ category: cat.id });
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

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div className="space-y-1">
              <label htmlFor="ledger-drawer-pillar" className="block text-xs font-semibold text-[var(--muted)]">Pilar Kakeibo</label>
              <select
                id="ledger-drawer-pillar"
                value={filters.kakeibo}
                onChange={(event) => updateFilters({ kakeibo: event.target.value as LedgerFilters["kakeibo"] })}
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-xs text-[var(--text)]"
              >
                <option value="all">Semua Pilar</option>
                {KAKEIBO_PILLARS.map((pillar) => (
                  <option key={pillar.value} value={pillar.value}>{pillar.label}</option>
                ))}
              </select>
            </div>
            <div className="space-y-1">
              <label htmlFor="ledger-drawer-review" className="block text-xs font-semibold text-[var(--muted)]">Status tinjauan</label>
              <select
                id="ledger-drawer-review"
                value={filters.review}
                onChange={(event) => updateFilters({ review: event.target.value as LedgerReviewFilter })}
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-xs text-[var(--text)]"
              >
                {REVIEW_FILTER_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Drawer Actions */}
          <div className="flex items-center gap-2 pt-3 border-t border-[var(--border)]">
            <button
              type="button"
              onClick={() => updateFilters({ ...DEFAULT_LEDGER_FILTERS, period: filters.period, offset: filters.offset })}
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
