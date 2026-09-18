"use client";

import { useState, useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots, formatDecimalInput, parseDecimal, parseUnits } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { Icon } from "@/components/ui/Icon";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { PayrollAllocationModal } from "@/components/recurring/PayrollAllocationModal";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";
import { InvestmentTradeModal } from "@/components/ui/InvestmentTradeModal";

function getContrastTextColor(hexColor?: string): string {
  if (!hexColor || !hexColor.startsWith("#")) return "#FFFFFF";
  const hex = hexColor.replace("#", "");
  if (hex.length !== 6 && hex.length !== 3) return "#FFFFFF";
  const r = parseInt(hex.length === 3 ? hex[0] + hex[0] : hex.slice(0, 2), 16);
  const g = parseInt(hex.length === 3 ? hex[1] + hex[1] : hex.slice(2, 4), 16);
  const b = parseInt(hex.length === 3 ? hex[2] + hex[2] : hex.slice(4, 6), 16);
  const luminance = 0.299 * r + 0.587 * g + 0.114 * b;
  return luminance > 150 ? "#0F172A" : "#FFFFFF";
}

const ACCOUNT_TAG_COLORS = [
  { name: "Biru", hex: "#2563EB" },
  { name: "Emerald", hex: "#059669" },
  { name: "Ungu", hex: "#7C3AED" },
  { name: "Amber", hex: "#D97706" },
  { name: "Rose", hex: "#E11D48" },
  { name: "Indigo", hex: "#4F46E5" },
  { name: "Teal", hex: "#0D9488" },
  { name: "Slate", hex: "#475569" },
];

interface AccountItem {
  id: string;
  parent_id?: string | null;
  default_funding_account_id?: string | null;
  default_funding_account_name?: string | null;
  name: string;
  type: "cash" | "bank" | "wallet" | "investment";
  initial_balance: number;
  balance: number;
  color?: string;
  display_order?: number;
  instrument_type?: "stock" | "mutual_fund" | "gold" | "crypto" | "deposit" | "other" | null;
  instrument_symbol?: string | null;
  units?: number | null;
  avg_buy_price?: number | null;
  last_price?: number | null;
  last_price_at?: string | null;
  capital_gain?: number | null;
  capital_gain_pct?: number | null;
  cost_basis?: number | null;
  is_archived: boolean;
  is_parent?: boolean;
  default_pocket_id?: string | null;
  children?: AccountItem[];
  created_at: string;
}

interface AccountsResponse {
  ok: boolean;
  accounts: AccountItem[];
  total_balance: number;
}

interface NetWorthResponse {
  ok: boolean;
  net_worth: number;
  total_assets: number;
  total_liabilities: number;
  accounts: any[];
  obligations: any[];
  runway: {
    daily_burn_rate: number;
    runway_days: number;
    runway_months: number;
    status: "healthy" | "moderate" | "critical" | "zero";
    emergency_fund_balance?: number;
    monthly_primary_expense?: number;
    is_flagged?: boolean;
    emergency_goal_names?: string[];
  };
}

export default function AccountsPage() {
  const qc = useQueryClient();
  const { bal } = useAppCtx();

  // Modals state
  const [transferModalOpen, setTransferModalOpen] = useState(false);
  const [accountModalOpen, setAccountModalOpen] = useState(false);
  const [payrollModalOpen, setPayrollModalOpen] = useState(false);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);
  const [editingAccount, setEditingAccount] = useState<AccountItem | null>(null);
  const [reconcileAccount, setReconcileAccount] = useState<AccountItem | null>(null);
  // Investment Trade Modal state
  const [tradeModalOpen, setTradeModalOpen] = useState(false);
  const [tradePocket, setTradePocket] = useState<AccountItem | null>(null);
  const [tradeAction, setTradeAction] = useState<"buy" | "sell">("buy");

  // Transfer form
  const [fromAccountId, setFromAccountId] = useState("");
  const [toAccountId, setToAccountId] = useState("");
  const [transferAmount, setTransferAmount] = useState("");
  const [transferNotes, setTransferNotes] = useState("");
  const [transferError, setTransferError] = useState("");

  // Instrument Options
  const INSTRUMENT_OPTIONS = [
    { value: "stock", label: "Saham" },
    { value: "mutual_fund", label: "Reksadana" },
    { value: "gold", label: "Emas" },
    { value: "crypto", label: "Kripto" },
    { value: "deposit", label: "Deposito" },
    { value: "other", label: "Lainnya" },
  ];

  const getInstrumentConfig = (instType?: string | null) => {
    switch (instType) {
      case "gold":
        return {
          unitsLabel: "Berat Emas (Gram)",
          unitsPlaceholder: "Contoh: 10 atau 0.5",
          unitsHint: "Dalam satuan gram (bisa pecahan, contoh: 0.5, 1, 10.25 gram)",
          priceLabel: "Harga Beli / Gram (IDR)",
          pricePlaceholder: "Contoh: 1.450.000",
          totalLabel: "Total Modal Pembelian Emas (IDR)",
          unitUnit: "gram",
        };
      case "mutual_fund":
        return {
          unitsLabel: "Jumlah Unit Penyertaan (UP)",
          unitsPlaceholder: "Contoh: 2871.45",
          unitsHint: "Jumlah unit tercatat di aplikasi (Bibit / Bareksa / BCA)",
          priceLabel: "NAB / Harga Beli per Unit (IDR)",
          pricePlaceholder: "Contoh: 1.922",
          totalLabel: "Total Modal Pembelian Reksadana (IDR)",
          unitUnit: "UP",
        };
      case "crypto":
        return {
          unitsLabel: "Jumlah Koin / Token",
          unitsPlaceholder: "Contoh: 0.05",
          unitsHint: "Bisa pecahan desimal (contoh: 0.05 BTC, 1.25 ETH)",
          priceLabel: "Harga Beli / Koin (IDR)",
          pricePlaceholder: "Contoh: 1.000.000.000",
          totalLabel: "Total Modal Pembelian Kripto (IDR)",
          unitUnit: "koin",
        };
      case "deposit":
        return {
          unitsLabel: "Nominal Pokok Deposito (IDR)",
          unitsPlaceholder: "Contoh: 10.000.000",
          unitsHint: "Nominal dana yang didepositokan",
          priceLabel: "Bunga / Imbal Hasil p.a (%)",
          pricePlaceholder: "Contoh: 5",
          totalLabel: "Total Pokok Deposito (IDR)",
          unitUnit: "IDR",
        };
      case "stock":
      default:
        return {
          unitsLabel: "Jumlah Lembar Saham",
          unitsPlaceholder: "Contoh: 1000",
          unitsHint: "1 lot = 100 lembar (contoh: 10 lot = 1000 lembar)",
          priceLabel: "Harga Beli / Lembar (IDR)",
          pricePlaceholder: "Contoh: 6.000",
          totalLabel: "Total Modal Pembelian Saham (IDR)",
          unitUnit: "lembar",
        };
    }
  };

  // Investment Account Form States
  const [accIsMultiInstrument, setAccIsMultiInstrument] = useState<boolean>(true);
  const [accInstrumentType, setAccInstrumentType] = useState<string>("stock");
  const [accTicker, setAccTicker] = useState("");
  const [accUnits, setAccUnits] = useState("");
  const [accAvgBuyPrice, setAccAvgBuyPrice] = useState("");
  const [tickerSuggestions, setTickerSuggestions] = useState<Array<{ symbol: string; name: string; type: string; exchange: string }>>([]);
  const [isSearchingTicker, setIsSearchingTicker] = useState(false);

  // Investment Pocket Form States
  const [pocketIsInvestment, setPocketIsInvestment] = useState<boolean>(false);
  const [pocketInstrumentType, setPocketInstrumentType] = useState<string>("mutual_fund");
  const [pocketTicker, setPocketTicker] = useState("");
  const [pocketUnits, setPocketUnits] = useState("");
  const [pocketAvgBuyPrice, setPocketAvgBuyPrice] = useState("");
  const [pocketTickerSuggestions, setPocketTickerSuggestions] = useState<Array<{ symbol: string; name: string; type: string; exchange: string }>>([]);
  const [pocketIsSearchingTicker, setPocketIsSearchingTicker] = useState(false);

  // Valuation Modal States (Mode B / Update Nilai)
  const [valuationAccount, setValuationAccount] = useState<AccountItem | null>(null);
  const [valCurrentBalStr, setValCurrentBalStr] = useState("");
  const [valCostBasisStr, setValCostBasisStr] = useState("");
  const [valNotes, setValNotes] = useState("");
  const [valError, setValError] = useState("");

  const handleOpenValuation = (acc: AccountItem) => {
    setValuationAccount(acc);
    setValCurrentBalStr(formatNumberWithDots(acc.balance));
    const totalCost = acc.cost_basis !== null && acc.cost_basis !== undefined
      ? acc.cost_basis
      : ((acc.units && acc.avg_buy_price)
        ? Math.round(acc.units * acc.avg_buy_price)
        : (acc.initial_balance || acc.avg_buy_price || acc.balance));
    setValCostBasisStr(formatNumberWithDots(totalCost));
    setValNotes("");
    setValError("");
  };

  // Sync Prices Mutation
  const [syncMessage, setSyncMessage] = useState<string | null>(null);
  const syncPricesMutation = useMutation({
    mutationFn: async () => {
      return api.post<{ ok: boolean; symbols_checked: number; accounts_updated: number }>("/accounts/sync-prices");
    },
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setSyncMessage(`Sync selesai: ${data.accounts_updated} akun diperbarui`);
      setTimeout(() => setSyncMessage(null), 4000);
    },
    onError: () => {
      setSyncMessage("Gagal sinkronisasi harga pasar");
      setTimeout(() => setSyncMessage(null), 4000);
    },
  });

  // Valuation Mutation
  const valuationMutation = useMutation({
    mutationFn: async () => {
      if (!valuationAccount) return;
      const cleanCur = valCurrentBalStr.trim().replace(/[^0-9]/g, "");
      const curBal = cleanCur ? parseInt(cleanCur, 10) : 0;
      const cleanCost = valCostBasisStr.trim().replace(/[^0-9]/g, "");
      const costBasis = cleanCost ? parseInt(cleanCost, 10) : undefined;
      return api.post(`/accounts/${valuationAccount.id}/valuation`, {
        current_balance: curBal,
        cost_basis: costBasis,
        notes: valNotes.trim() || null,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setValuationAccount(null);
    },
    onError: (err: any) => {
      setValError(err?.message || "Gagal memperbarui nilai investasi");
    },
  });

  // Ticker lazy search
  const handleSearchTicker = async (query: string, isPocket: boolean = false) => {
    const q = query.trim();
    if (isPocket) setPocketTicker(query);
    else setAccTicker(query);

    if (q.length < 2) {
      if (isPocket) setPocketTickerSuggestions([]);
      else setTickerSuggestions([]);
      return;
    }

    if (isPocket) setPocketIsSearchingTicker(true);
    else setIsSearchingTicker(true);

    try {
      const res = await api.get<{ ok: boolean; results: Array<{ symbol: string; name: string; type: string; exchange: string }> }>(
        `/accounts/instruments/search?q=${encodeURIComponent(q)}&limit=6`
      );
      if (isPocket) setPocketTickerSuggestions(res.results || []);
      else setTickerSuggestions(res.results || []);
    } catch {
      // ignore
    } finally {
      if (isPocket) setPocketIsSearchingTicker(false);
      else setIsSearchingTicker(false);
    }
  };

  // Account form
  const [accName, setAccName] = useState("");
  const [accType, setAccType] = useState<"cash" | "bank" | "wallet" | "investment">("bank");
  const [accInitBal, setAccInitBal] = useState("");
  const [accColor, setAccColor] = useState<string>("#2563EB");
  const [accDefaultFundingId, setAccDefaultFundingId] = useState<string>("");
  const [accDefaultPocketId, setAccDefaultPocketId] = useState<string>("");
  const [accError, setAccError] = useState("");

  // Account drag-and-drop reordering state
  const [draggedAccountId, setDraggedAccountId] = useState<string | null>(null);
  const [dragOverAccountId, setDragOverAccountId] = useState<string | null>(null);
  const [orderedAccountIds, setOrderedAccountIds] = useState<string[] | null>(null);

  // Pocket drag-and-drop reordering state
  const [draggedPocketId, setDraggedPocketId] = useState<string | null>(null);
  const [dragOverPocketId, setDragOverPocketId] = useState<string | null>(null);
  const [orderedPocketsByParent, setOrderedPocketsByParent] = useState<Record<string, string[]>>({});

  const reorderMutation = useMutation({
    mutationFn: async (accountIds: string[]) => {
      return api.post("/accounts/reorder", { account_ids: accountIds });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
    },
  });

  // Pocket form
  const [pocketModalOpen, setPocketModalOpen] = useState(false);
  const [pocketParentAccount, setPocketParentAccount] = useState<AccountItem | null>(null);
  const [pocketName, setPocketName] = useState("");
  const [pocketInitBal, setPocketInitBal] = useState("");
  const [pocketError, setPocketError] = useState("");

  // Collapsible cards state
  const [expandedAccounts, setExpandedAccounts] = useState<Record<string, boolean>>({});
  const toggleExpand = (id: string) => {
    setExpandedAccounts((prev) => ({
      ...prev,
      [id]: prev[id] === undefined ? false : !prev[id],
    }));
  };

  // Reconciliation form
  const [actualBalStr, setActualBalStr] = useState("");
  const [reconcileNotes, setReconcileNotes] = useState("");
  const [reconcileError, setReconcileError] = useState("");

  const reconcileMutation = useMutation({
    mutationFn: async () => {
      if (!reconcileAccount) return;
      const clean = actualBalStr.trim().replace(/[^0-9]/g, "");
      const actual = clean ? parseInt(clean, 10) : 0;
      return api.post(`/accounts/${reconcileAccount.id}/reconcile`, {
        actual_balance: actual,
        notes: reconcileNotes.trim() || null,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      setReconcileAccount(null);
    },
    onError: (err: any) => {
      setReconcileError(err?.message || "Gagal menyesuaikan saldo");
    },
  });

  const { data: accountsData, isLoading: accountsLoading } = useQuery<AccountsResponse>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
  });

  const { data: netWorthData, isLoading: netWorthLoading } = useQuery<NetWorthResponse>({
    queryKey: ["dashboard-net-worth"],
    queryFn: () => api.get("/dashboard/net-worth"),
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const activeAccounts = useMemo(() => accounts.filter((a) => !a.is_archived), [accounts]);
  const totalBalance = accountsData?.total_balance ?? 0;

  const topAccounts = useMemo(() => {
    const list = activeAccounts.filter((a) => !a.parent_id);
    if (!orderedAccountIds) return list;
    return [...list].sort((a, b) => {
      const idxA = orderedAccountIds.indexOf(a.id);
      const idxB = orderedAccountIds.indexOf(b.id);
      if (idxA === -1 && idxB === -1) return 0;
      if (idxA === -1) return 1;
      if (idxB === -1) return -1;
      return idxA - idxB;
    });
  }, [activeAccounts, orderedAccountIds]);

  const netWorth = netWorthData?.net_worth ?? totalBalance;
  const totalAssets = netWorthData?.total_assets ?? totalBalance;
  const totalLiabilities = netWorthData?.total_liabilities ?? 0;
  const runway = netWorthData?.runway;

  const animNetWorth = useAnimatedCounter(netWorth);
  const animTotalAssets = useAnimatedCounter(totalAssets);
  const animTotalLiabilities = useAnimatedCounter(totalLiabilities);

  // Open Transfer modal preselected
  const handleOpenTransfer = (sourceAccId?: string) => {
    const transactable = activeAccounts;
    if (transactable.length < 2) {
      alert("Anda membutuhkan minimal 2 rekening atau kantong untuk melakukan pindah saldo.");
      return;
    }
    const source = sourceAccId || transactable[0].id;
    const dest = transactable.find((a) => a.id !== source)?.id || transactable[1].id;
    setFromAccountId(source);
    setToAccountId(dest);
    setTransferAmount("");
    setTransferNotes("");
    setTransferError("");
    setTransferModalOpen(true);
  };

  // Open Add Account modal
  const handleOpenNewAccount = () => {
    setEditingAccount(null);
    setAccName("");
    setAccType("bank");
    setAccInitBal("");
    setAccColor("#2563EB");
    setAccDefaultFundingId("");
    setAccDefaultPocketId("");
    setAccIsMultiInstrument(true);
    setAccInstrumentType("stock");
    setAccTicker("");
    setAccUnits("");
    setAccAvgBuyPrice("");
    setTickerSuggestions([]);
    setAccError("");
    setAccountModalOpen(true);
  };

  // Open Add Pocket modal
  const handleOpenNewPocket = (parent: AccountItem) => {
    setPocketParentAccount(parent);
    setPocketName("");
    setPocketInitBal("");
    const isParentInvest = parent.type === "investment";
    setPocketIsInvestment(isParentInvest);
    setPocketInstrumentType(isParentInvest ? "mutual_fund" : "stock");
    setPocketTicker("");
    setPocketUnits("");
    setPocketAvgBuyPrice("");
    setPocketTickerSuggestions([]);
    setPocketError("");
    setPocketModalOpen(true);
  };

  // Open Edit Account modal
  const handleOpenEditAccount = (acc: AccountItem) => {
    setEditingAccount(acc);
    setAccName(acc.name);
    setAccType(acc.type);
    setAccColor(acc.color || "#2563EB");
    setAccDefaultFundingId(acc.default_funding_account_id || "");
    setAccDefaultPocketId(acc.default_pocket_id || "");
    setAccInitBal(formatNumberWithDots(acc.initial_balance));
    const hasInst = Boolean(acc.instrument_type || acc.instrument_symbol || (acc.units !== null && acc.units !== undefined));
    setAccIsMultiInstrument(!hasInst);
    setAccInstrumentType(acc.instrument_type || "stock");
    setAccTicker(acc.instrument_symbol || "");
    setAccUnits(acc.units !== null && acc.units !== undefined ? String(acc.units).replace(".", ",") : "");
    setAccAvgBuyPrice(acc.avg_buy_price ? formatDecimalInput(acc.avg_buy_price) : "");
    setTickerSuggestions([]);
    setAccError("");
    setAccountModalOpen(true);
  };

  // Pocket Mutation (Create Pocket under Parent)
  const savePocketMutation = useMutation({
    mutationFn: async () => {
      if (!pocketParentAccount) return;
      const name = pocketName.trim();
      if (!name) throw new Error("Nama kantong wajib diisi");
      const initBal = parseNumberFromDots(pocketInitBal);
      const isInvest = pocketIsInvestment;
      const pUnitsNum = isInvest && pocketUnits.trim() ? parseUnits(pocketUnits.trim()) : undefined;
      const pAvgPriceNum = isInvest && pocketAvgBuyPrice.trim() ? parseDecimal(pocketAvgBuyPrice.trim()) : undefined;
      const pTickerSymbol = isInvest && pocketTicker.trim() ? pocketTicker.trim().toUpperCase() : undefined;

      return api.post("/accounts", {
        name,
        type: isInvest ? "investment" : pocketParentAccount.type,
        parent_id: pocketParentAccount.id,
        initial_balance: initBal,
        instrument_type: isInvest ? pocketInstrumentType : null,
        instrument_symbol: isInvest ? pTickerSymbol : null,
        units: isInvest ? pUnitsNum : null,
        avg_buy_price: isInvest ? pAvgPriceNum : null,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setPocketModalOpen(false);
    },
    onError: (err: any) => {
      setPocketError(err?.message || "Gagal membuat kantong");
    },
  });

  // Transfer Mutation
  const transferMutation = useMutation({
    mutationFn: async () => {
      const amt = parseInt(transferAmount.replace(/[^0-9]/g, ""), 10);
      if (!amt || amt <= 0) throw new Error("Masukkan nominal uang yang valid");
      if (!fromAccountId || !toAccountId) throw new Error("Pilih rekening asal dan tujuan");
      if (fromAccountId === toAccountId) throw new Error("Rekening asal dan tujuan harus berbeda");

      return api.post("/movements", {
        source_account_id: fromAccountId,
        target_account_id: toAccountId,
        amount: amt,
        notes: transferNotes.trim() || null,
        date: new Date().toISOString(),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setTransferModalOpen(false);
    },
    onError: (err: any) => {
      setTransferError(err?.message || "Pindah saldo gagal");
    },
  });

  // Account Mutation (Create / Update)
  const saveAccountMutation = useMutation({
    mutationFn: async () => {
      const name = accName.trim();
      if (!name) throw new Error("Nama rekening wajib diisi");
      const initBal = parseInt(accInitBal.replace(/[^0-9]/g, "") || "0", 10);
      const isEditingPocket = Boolean(editingAccount?.parent_id);

      const isInvestPocket = isEditingPocket && !accIsMultiInstrument;
      const isInvestTop = !isEditingPocket && accType === "investment";
      const hasInstrument = isEditingPocket ? isInvestPocket : (isInvestTop && !accIsMultiInstrument);

      const unitsNum = hasInstrument && accUnits.trim() ? parseUnits(accUnits.trim()) : null;
      const avgPriceNum = hasInstrument && accAvgBuyPrice.trim() ? parseDecimal(accAvgBuyPrice.trim()) : null;
      const tickerSymbol = hasInstrument && accTicker.trim() ? accTicker.trim().toUpperCase() : null;

      const defaultFunding = !isEditingPocket && accType === "investment" && accDefaultFundingId.trim() ? accDefaultFundingId.trim() : null;

      if (editingAccount) {
        return api.patch(`/accounts/${editingAccount.id}`, {
          name,
          type: isEditingPocket ? (isInvestPocket ? "investment" : editingAccount.type) : accType,
          instrument_type: hasInstrument ? accInstrumentType : null,
          instrument_symbol: tickerSymbol,
          units: unitsNum,
          avg_buy_price: avgPriceNum,
          ...(!isEditingPocket ? {
            color: accColor,
            default_funding_account_id: accType === "investment" ? defaultFunding : null,
            default_pocket_id: accDefaultPocketId ? accDefaultPocketId : null,
          } : {}),
        });
      } else {
        return api.post("/accounts", {
          name,
          type: accType,
          initial_balance: initBal,
          instrument_type: hasInstrument ? accInstrumentType : null,
          instrument_symbol: tickerSymbol,
          units: unitsNum,
          avg_buy_price: avgPriceNum,
          color: accColor,
          default_funding_account_id: defaultFunding,
        });
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      setAccountModalOpen(false);
    },
    onError: (err: any) => {
      setAccError(err?.message || "Operation failed");
    },
  });

  // Archive Account Mutation
  const archiveMutation = useMutation({
    mutationFn: async (accId: string) => {
      return api.del(`/accounts/${accId}`);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
    },
  });

  if (accountsLoading || netWorthLoading) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-44 rounded-3xl bg-[var(--border)]/40" />
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="h-40 rounded-3xl bg-[var(--border)]/30" />
          <div className="h-40 rounded-3xl bg-[var(--border)]/30" />
          <div className="h-40 rounded-3xl bg-[var(--border)]/30" />
        </div>
      </div>
    );
  }

  // Calculate liquidity breakdown across all liquid funds and investment holdings:
  // If an account has children, examine each pocket's actual type.
  // If an account has no children, use the account's own balance and type.
  let bankTotal = 0;
  let walletTotal = 0;
  let cashTotal = 0;
  let investTotal = 0;

  for (const acc of topAccounts) {
    if (acc.children && acc.children.length > 0) {
      for (const c of acc.children) {
        if (c.type === "investment" || c.instrument_type) {
          investTotal += c.balance;
        } else if (c.type === "bank") {
          bankTotal += c.balance;
        } else if (c.type === "wallet") {
          walletTotal += c.balance;
        } else if (c.type === "cash") {
          cashTotal += c.balance;
        } else {
          if (acc.type === "investment") investTotal += c.balance;
          else if (acc.type === "bank") bankTotal += c.balance;
          else if (acc.type === "wallet") walletTotal += c.balance;
          else cashTotal += c.balance;
        }
      }
      const childrenSum = acc.children.reduce((s, c) => s + c.balance, 0);
      const remainder = acc.balance - childrenSum;
      if (remainder > 0) {
        if (acc.type === "investment" || acc.instrument_type) investTotal += remainder;
        else if (acc.type === "bank") bankTotal += remainder;
        else if (acc.type === "wallet") walletTotal += remainder;
        else cashTotal += remainder;
      }
    } else {
      if (acc.type === "investment" || acc.instrument_type) {
        investTotal += acc.balance;
      } else if (acc.type === "bank") {
        bankTotal += acc.balance;
      } else if (acc.type === "wallet") {
        walletTotal += acc.balance;
      } else {
        cashTotal += acc.balance;
      }
    }
  }

  const positiveAssets = Math.max(1, totalAssets);
  const bankPct = Math.round((Math.max(0, bankTotal) / positiveAssets) * 100);
  const walletPct = Math.round((Math.max(0, walletTotal) / positiveAssets) * 100);
  const cashPct = Math.round((Math.max(0, cashTotal) / positiveAssets) * 100);
  const investPct = Math.round((Math.max(0, investTotal) / positiveAssets) * 100);

  return (
    <div className="space-y-6">
      {/* 1. Header & Actions */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-[var(--border)]">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
              Rekening & Saldo
            </span>
            <span className="h-1 w-1 rounded-full bg-[var(--muted)]" />
            <span className="text-xs text-[var(--text-secondary)]">
              {topAccounts.length} Rekening Utama ({activeAccounts.length} Total Kantong)
            </span>
          </div>
          <h1 className="text-2xl font-extrabold tracking-tight text-[var(--text)] mt-1">
            Rekening & Total Saldo
          </h1>
        </div>

        <div className="flex items-center gap-2.5 self-start sm:self-auto flex-wrap">
          {syncMessage && (
            <span className="text-[11px] font-medium px-2.5 py-1 rounded-xl bg-blue-500/10 text-blue-400 border border-blue-500/20 animate-fade-in">
              {syncMessage}
            </span>
          )}

          <button
            type="button"
            onClick={() => syncPricesMutation.mutate()}
            disabled={syncPricesMutation.isPending}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-xs font-bold text-[var(--text)] transition-all pressable shadow-2xs disabled:opacity-50"
            title="Sinkronisasi harga pasar saham/investasi terkini"
          >
            <Icon
              name="refresh-cw"
              className={cn("h-3.5 w-3.5 text-blue-500 stroke-[2.5]", syncPricesMutation.isPending && "animate-spin")}
            />
            <span>{syncPricesMutation.isPending ? "Sinkronisasi..." : "Sync Harga"}</span>
          </button>

          <button
            type="button"
            onClick={() => setPayrollModalOpen(true)}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-emerald-500/25 bg-emerald-500/10 hover:bg-emerald-500/20 text-xs font-bold text-emerald-500 transition-all pressable shadow-2xs"
            title="Buka Alokasi Gaji Bulanan"
          >
            <Icon name="allocation" className="h-3.5 w-3.5 text-emerald-500 stroke-[2.5]" />
            <span>Alokasi Gaji</span>
          </button>

          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-raised)] text-xs font-bold text-[var(--text)] transition-all pressable shadow-2xs"
            title="Kelola Transaksi Rutin & Otomatis"
          >
            <Icon name="repeat" className="h-3.5 w-3.5 text-blue-500 stroke-[2.5]" />
            <span>Rutin</span>
          </button>

          <button
            type="button"
            onClick={() => handleOpenTransfer()}
            disabled={activeAccounts.length < 2}
            className="flex items-center gap-1.5 px-4 py-2 rounded-2xl border border-indigo-500/25 bg-indigo-500/10 hover:bg-indigo-500/20 text-xs font-bold text-indigo-500 transition-all pressable shadow-2xs disabled:opacity-40"
          >
            <Icon name="move" className="h-3.5 w-3.5 text-indigo-500 stroke-[2.5]" />
            <span>Pindah Saldo</span>
          </button>

          <button
            type="button"
            onClick={handleOpenNewAccount}
            className="flex items-center gap-1.5 px-4 py-2 rounded-2xl bg-emerald-500 hover:bg-emerald-400 text-white text-xs font-black shadow-xs pressable"
          >
            <span className="font-black text-sm leading-none">+</span>
            <span>Rekening Baru</span>
          </button>
        </div>
      </div>

      {/* 2. Executive Net Worth & Runway Hero Card */}
      <div className="card-squircle relative overflow-hidden bg-gradient-to-br from-white via-slate-50 to-slate-100 text-slate-900 dark:from-[#1E2127] dark:via-[#14161A] dark:to-[#0D0E11] dark:text-white p-6 sm:p-7 shadow-xs dark:shadow-md border border-[var(--border-strong)]">
        <div className="absolute -right-16 -top-16 h-48 w-48 rounded-full bg-emerald-500/10 dark:bg-emerald-500/15 blur-3xl pointer-events-none" />
        <div className="absolute -left-16 -bottom-16 h-48 w-48 rounded-full bg-indigo-500/10 dark:bg-indigo-500/15 blur-3xl pointer-events-none" />

        <div className="relative flex items-center justify-between">
          <span className="text-[11px] font-bold uppercase tracking-wider text-[var(--muted)]">
            Total Kekayaan Bersih (Total saldo kas dikurangi sisa cicilan & utang)
          </span>
          {runway && (
            <span
              className={cn(
                "text-xs px-3 py-1 rounded-full font-bold border inline-flex items-center gap-1.5 shadow-2xs",
                totalAssets <= 0 || runway.status === "zero"
                  ? "bg-zinc-500/10 text-zinc-500 dark:text-zinc-400 border-zinc-500/20"
                  : runway.status === "healthy"
                  ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20"
                  : runway.status === "moderate"
                  ? "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20"
                  : "bg-rose-500/10 text-rose-600 dark:text-rose-400 border-rose-500/20"
              )}
            >
              <span
                className={cn(
                  "h-1.5 w-1.5 rounded-full",
                  runway.status === "healthy"
                    ? "bg-emerald-500"
                    : runway.status === "moderate"
                    ? "bg-amber-500"
                    : "bg-rose-500"
                )}
              />
              Ketahanan Dana:{" "}
              {totalAssets <= 0 || runway.status === "zero"
                ? `0 Hari (${bal(0)})`
                : runway.runway_months >= 99
                ? "> 12x Biaya Hidup"
                : `${runway.runway_months}x Biaya Hidup (${runway.runway_days > 365 ? "> 1 Thn" : `${runway.runway_days} Hari`})`}
            </span>
          )}
        </div>

        <div className="relative mt-3.5">
          <div className="text-3xl sm:text-5xl font-black tracking-tight tabular select-all text-[var(--text)]">
            {bal(animNetWorth)}
          </div>
          <p className="text-xs text-[var(--muted)] mt-1">
            {totalAssets >= totalLiabilities ? "Saldo kas mencukupi seluruh kewajiban" : "Total kewajiban melebihi saldo kas tersedia"}
          </p>
        </div>

        {/* 3-Column Asset vs Liability Ribbon */}
        <div className="relative mt-6 pt-5 border-t border-[var(--border)] grid grid-cols-3 gap-3 sm:gap-6 text-xs">
          <div>
            <div className="text-[10px] text-[var(--muted)] uppercase tracking-wider font-bold">Saldo Kas Tersedia</div>
            <div className="text-base sm:text-lg font-black text-emerald-600 dark:text-emerald-400 mt-0.5 tabular select-all">
              {bal(animTotalAssets)}
            </div>
            <div className="text-[10px] text-[var(--muted)] opacity-75 mt-0.5">{topAccounts.length} akun utama</div>
          </div>

          <div>
            <div className="text-[10px] text-[var(--muted)] uppercase tracking-wider font-bold">Total Cicilan & Utang</div>
            <div className="text-base sm:text-lg font-black text-rose-600 dark:text-rose-400 mt-0.5 tabular select-all">
              {bal(animTotalLiabilities)}
            </div>
            <div className="text-[10px] text-[var(--muted)] opacity-75 mt-0.5">
              {netWorthData?.obligations?.length || 0} cicilan aktif
            </div>
          </div>

          <div>
            <div className="text-[10px] text-[var(--muted)] uppercase tracking-wider font-bold">Biaya Hidup Pokok Bulanan</div>
            <div className="text-base sm:text-lg font-black text-[var(--text)] mt-0.5 tabular select-all">
              {bal(runway?.monthly_primary_expense ?? (runway?.daily_burn_rate ?? 0) * 30)}/bln
            </div>
            <div className="text-[10px] text-[var(--muted)] opacity-75 mt-0.5">
              {runway?.is_flagged ? "Target Dana Darurat Terhubung" : "Pengeluaran Pokok 30 Hari"}
            </div>
          </div>
        </div>
      </div>

      {/* 3. Liquidity Distribution Bar */}
      <div className="card-squircle p-5 sm:p-6 space-y-3.5 shadow-xs">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
          <h2 className="text-base font-bold tracking-tight text-[var(--text)]">
            Alokasi Saldo
          </h2>
          <div className="flex flex-wrap items-center gap-3.5 text-xs text-[var(--muted)] font-medium">
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full bg-blue-500" />
              Bank: <strong className="text-[var(--text)]">{bankPct}%</strong>
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full bg-emerald-500" />
              Dompet: <strong className="text-[var(--text)]">{walletPct}%</strong>
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full bg-amber-500" />
              Tunai: <strong className="text-[var(--text)]">{cashPct}%</strong>
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full bg-indigo-500" />
              Investasi: <strong className="text-[var(--text)]">{investPct}%</strong>
            </span>
          </div>
        </div>

        {/* Multi-segmented distribution bar */}
        <div className="w-full h-2.5 rounded-full bg-[var(--border)]/60 overflow-hidden flex shadow-2xs">
          {bankPct > 0 && (
            <div
              style={{ width: `${bankPct}%` }}
              className="h-full bg-blue-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
            />
          )}
          {walletPct > 0 && (
            <div
              style={{ width: `${walletPct}%` }}
              className="h-full bg-emerald-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
            />
          )}
          {cashPct > 0 && (
            <div
              style={{ width: `${cashPct}%` }}
              className="h-full bg-amber-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
            />
          )}
          {investPct > 0 && (
            <div
              style={{ width: `${investPct}%` }}
              className="h-full bg-indigo-500 transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)]"
            />
          )}
        </div>
      </div>

      {/* 4. Realistic Digital Bank Account & Pocket Cards with Visual Drag-and-Drop */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
        {topAccounts.map((acc) => {
          const hasChildren = acc.children && acc.children.length > 0;
          const isExpanded = expandedAccounts[acc.id] !== false;

          return (
            <div
              key={acc.id}
              draggable
              onDragStart={(e) => {
                setDraggedAccountId(acc.id);
                e.dataTransfer.effectAllowed = "move";
                e.dataTransfer.setData("text/plain", acc.id);
              }}
              onDragOver={(e) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = "move";
                if (dragOverAccountId !== acc.id) {
                  setDragOverAccountId(acc.id);
                }
              }}
              onDragLeave={() => {
                if (dragOverAccountId === acc.id) {
                  setDragOverAccountId(null);
                }
              }}
              onDrop={(e) => {
                e.preventDefault();
                setDragOverAccountId(null);
                if (!draggedAccountId || draggedAccountId === acc.id) {
                  setDraggedAccountId(null);
                  return;
                }
                const currentOrder = topAccounts.map((a) => a.id);
                const fromIndex = currentOrder.indexOf(draggedAccountId);
                const toIndex = currentOrder.indexOf(acc.id);
                if (fromIndex !== -1 && toIndex !== -1) {
                  const newOrder = [...currentOrder];
                  const [moved] = newOrder.splice(fromIndex, 1);
                  newOrder.splice(toIndex, 0, moved);
                  setOrderedAccountIds(newOrder);
                  reorderMutation.mutate(newOrder);
                }
                setDraggedAccountId(null);
              }}
              onDragEnd={() => {
                setDraggedAccountId(null);
                setDragOverAccountId(null);
              }}
              className={cn(
                "card-squircle relative overflow-hidden p-4 sm:p-5 shadow-xs hover:shadow-md hover:-translate-y-0.5 transition-all duration-200 flex flex-col justify-between cursor-grab active:cursor-grabbing group bg-[var(--surface)] text-[var(--text)]",
                draggedAccountId === acc.id && "opacity-40 scale-[0.98]",
                dragOverAccountId === acc.id && "ring-2 ring-emerald-500 ring-offset-2 border-emerald-500"
              )}
            >
              <div>
                {/* Card Top: Prominent Tagged Name & Quick Actions */}
                <div className="flex items-start justify-between gap-3 min-h-[50px]">
                  <div className="space-y-1 min-w-0">
                    {/* Prominent Account Name Tag */}
                    <div>
                      <span
                        style={{
                          backgroundColor: acc.color || "#3b82f6",
                          color: getContrastTextColor(acc.color || "#3b82f6"),
                        }}
                        className="inline-flex items-center font-bold text-sm sm:text-base px-3 py-0.5 rounded-xl shadow-2xs max-w-full truncate tracking-tight"
                        title={acc.name}
                      >
                        {acc.name}
                      </span>
                    </div>

                    {/* Account Type and Badges */}
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span className="text-[10px] font-bold uppercase tracking-wider text-[var(--muted)]">
                        {acc.type === "wallet"
                          ? "DOMPET DIGITAL"
                          : acc.type === "cash"
                          ? "UANG TUNAI"
                          : acc.type === "investment"
                          ? "INVESTASI"
                          : "BANK"}
                      </span>
                      {acc.instrument_type && (
                        <span className="px-2 py-0.5 rounded-full text-[10px] bg-violet-500/15 text-violet-600 dark:text-violet-300 font-semibold uppercase">
                          {INSTRUMENT_OPTIONS.find((o) => o.value === acc.instrument_type)?.label || acc.instrument_type}
                        </span>
                      )}
                      {acc.instrument_symbol && (
                        <span className="px-2 py-0.5 rounded-full text-[10px] bg-[var(--surface-raised)] border border-[var(--border)] text-[var(--text)] font-bold">
                          {acc.instrument_symbol}
                        </span>
                      )}
                      {acc.default_funding_account_name && (
                        <span className="px-2 py-0.5 rounded-full text-[10px] bg-blue-500/15 text-blue-600 dark:text-blue-400 font-semibold flex items-center gap-1">
                          <Icon name="credit-card" className="h-2.5 w-2.5" />
                          <span>RDN: {acc.default_funding_account_name}</span>
                        </span>
                      )}
                      {hasChildren && (
                        <span className="px-2 py-0.5 rounded-full text-[10px] bg-blue-500/15 text-blue-600 dark:text-blue-400 font-semibold">
                          {acc.children!.length} Kantong
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Action Icons */}
                  <div className="flex items-center gap-1 shrink-0">
                    {(acc.type === "investment" || Boolean(acc.instrument_type)) && !hasChildren && (
                      <>
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            setTradePocket(acc);
                            setTradeAction("buy");
                            setTradeModalOpen(true);
                          }}
                          className="px-2 py-1 rounded-lg bg-emerald-500/15 hover:bg-emerald-500/25 text-[11px] font-semibold text-emerald-600 dark:text-emerald-400 transition-colors flex items-center gap-1"
                          title="Catat Pembelian"
                        >
                          <span>+Beli</span>
                        </button>
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            setTradePocket(acc);
                            setTradeAction("sell");
                            setTradeModalOpen(true);
                          }}
                          className="px-2 py-1 rounded-lg bg-rose-500/15 hover:bg-rose-500/25 text-[11px] font-semibold text-rose-600 dark:text-rose-400 transition-colors flex items-center gap-1"
                          title="Catat Penjualan"
                        >
                          <span>−Jual</span>
                        </button>
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleOpenValuation(acc);
                          }}
                          className="px-2 py-1 rounded-lg bg-[var(--surface-raised)] hover:bg-[var(--border)]/50 text-[11px] font-semibold text-[var(--muted)] border border-[var(--border)] transition-colors flex items-center gap-1"
                          title="Update Nilai Pasar (Valuation)"
                        >
                          <Icon name="trending-up" className="h-3 w-3" />
                          <span>Nilai</span>
                        </button>
                      </>
                    )}
                    {acc.type === "investment" && hasChildren && acc.children && acc.children.length > 0 && (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          setTradePocket(acc.children![0]);
                          setTradeAction("buy");
                          setTradeModalOpen(true);
                        }}
                        className="px-2 py-1 rounded-lg bg-emerald-500/15 hover:bg-emerald-500/25 text-[11px] font-semibold text-emerald-600 dark:text-emerald-400 border border-emerald-500/20 transition-colors flex items-center gap-1"
                        title="Catat Pembelian instrumen"
                      >
                        <span>+ Beli</span>
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleOpenNewPocket(acc);
                      }}
                      className="px-2 py-1 rounded-lg bg-[var(--surface-raised)] hover:bg-[var(--border)]/50 text-[11px] font-semibold text-[var(--text)] border border-[var(--border)] transition-colors flex items-center gap-1"
                      title="Tambah kantong baru di rekening ini"
                    >
                      <span>+ Kantong</span>
                    </button>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleOpenEditAccount(acc);
                      }}
                      className="p-1.5 rounded-lg text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--surface-raised)] transition-colors"
                      title="Ubah rekening"
                    >
                      <Icon name="edit" className="h-3.5 w-3.5" />
                    </button>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        if (confirm(`Arsipkan rekening "${acc.name}"${hasChildren ? " beserta seluruh kantong di dalamnya" : ""}?`)) {
                          archiveMutation.mutate(acc.id);
                        }
                      }}
                      className="p-1.5 rounded-lg text-[var(--muted)] hover:text-rose-500 hover:bg-rose-500/10 transition-colors"
                      title="Arsipkan rekening"
                    >
                      <Icon name="trash" className="h-3.5 w-3.5" />
                    </button>
                  </div>
                </div>

                {/* Card Middle: Balance & Status Row (Consistent height for perfect grid alignment) */}
                <div className="mt-3 min-h-[58px] flex flex-col justify-end">
                  <div className="text-2xl sm:text-3xl font-extrabold tabular tracking-tight text-[var(--text)] select-all leading-none">
                    {bal(acc.balance)}
                  </div>

                  {/* Sub-line: Total Kantong and Capital Gain in a single aligned row */}
                  <div className="mt-1.5 flex items-center justify-between gap-2 min-h-[22px]">
                    {hasChildren ? (
                      <span className="text-[11px] text-[var(--muted)] font-medium truncate">
                        Total akumulasi {acc.children!.length} kantong
                      </span>
                    ) : (acc.last_price || acc.avg_buy_price) ? (
                      <div className="text-[10px] text-[var(--muted)] flex items-center gap-2 truncate">
                        {acc.last_price ? <span>Pasar: <strong className="text-[var(--text)] font-semibold">{fmtMoney(acc.last_price)}</strong></span> : null}
                        {acc.avg_buy_price ? <span>Modal: <strong className="text-[var(--text)] font-semibold">{fmtMoney(acc.avg_buy_price)}</strong></span> : null}
                      </div>
                    ) : (
                      <span className="text-[11px] text-[var(--muted)] font-medium">
                        {acc.type === "cash" ? "Kas Tunai Fisik" : "Rekening Standalone"}
                      </span>
                    )}

                    {acc.capital_gain !== null && acc.capital_gain !== undefined && (
                      <span
                        className={cn(
                          "px-2 py-0.5 rounded-md text-[10px] font-bold flex items-center gap-1 border shrink-0",
                          acc.capital_gain >= 0
                            ? "bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/25"
                            : "bg-rose-500/15 text-rose-600 dark:text-rose-400 border-rose-500/25"
                        )}
                      >
                        <span>{acc.capital_gain >= 0 ? "▲" : "▼"}</span>
                        <span>Capital Gain: {acc.capital_gain >= 0 ? "+" : ""}{bal(acc.capital_gain)} ({acc.capital_gain_pct ?? 0}%)</span>
                      </span>
                    )}
                  </div>
                </div>

                {/* Child Pockets Accordion */}
                {hasChildren && (
                  <div className="mt-3 pt-3 border-t border-[var(--border)] space-y-2">
                    <div
                      onClick={(e) => {
                        e.stopPropagation();
                        toggleExpand(acc.id);
                      }}
                      className="flex items-center justify-between cursor-pointer py-0.5 text-[11px] font-semibold text-[var(--muted)] hover:text-[var(--text)] transition-colors select-none"
                    >
                      <span>Daftar Kantong ({acc.children!.length})</span>
                      <span className="text-[10px]">{isExpanded ? "▲ Tutup" : "▼ Lihat"}</span>
                    </div>

                    {isExpanded && (
                      <div className="space-y-2 max-h-[250px] overflow-y-auto pr-1">
                        {(() => {
                          const parentPocketOrder = orderedPocketsByParent[acc.id];
                          const childPockets = parentPocketOrder
                            ? [...(acc.children ?? [])].sort((a, b) => {
                                const idxA = parentPocketOrder.indexOf(a.id);
                                const idxB = parentPocketOrder.indexOf(b.id);
                                if (idxA === -1 && idxB === -1) return 0;
                                if (idxA === -1) return 1;
                                if (idxB === -1) return -1;
                                return idxA - idxB;
                              })
                            : (acc.children ?? []);

                          return childPockets.map((pocket) => {
                            const isInvestPocket = acc.type === "investment" || pocket.type === "investment" || Boolean(pocket.instrument_type);

                            const pocketDragHandlers = {
                              draggable: true,
                              onDragStart: (e: React.DragEvent) => {
                                e.stopPropagation();
                                setDraggedPocketId(pocket.id);
                                e.dataTransfer.effectAllowed = "move";
                                e.dataTransfer.setData("text/plain", pocket.id);
                              },
                              onDragOver: (e: React.DragEvent) => {
                                e.preventDefault();
                                e.stopPropagation();
                                e.dataTransfer.dropEffect = "move";
                                if (dragOverPocketId !== pocket.id) {
                                  setDragOverPocketId(pocket.id);
                                }
                              },
                              onDragLeave: (e: React.DragEvent) => {
                                e.stopPropagation();
                                if (dragOverPocketId === pocket.id) {
                                  setDragOverPocketId(null);
                                }
                              },
                              onDrop: (e: React.DragEvent) => {
                                e.preventDefault();
                                e.stopPropagation();
                                setDragOverPocketId(null);
                                if (!draggedPocketId || draggedPocketId === pocket.id) {
                                  setDraggedPocketId(null);
                                  return;
                                }
                                const currentOrder = childPockets.map((p) => p.id);
                                const fromIndex = currentOrder.indexOf(draggedPocketId);
                                const toIndex = currentOrder.indexOf(pocket.id);
                                if (fromIndex !== -1 && toIndex !== -1) {
                                  const newOrder = [...currentOrder];
                                  const [moved] = newOrder.splice(fromIndex, 1);
                                  newOrder.splice(toIndex, 0, moved);
                                  setOrderedPocketsByParent((prev) => ({
                                    ...prev,
                                    [acc.id]: newOrder,
                                  }));
                                  reorderMutation.mutate(newOrder);
                                }
                                setDraggedPocketId(null);
                              },
                              onDragEnd: (e: React.DragEvent) => {
                                e.stopPropagation();
                                setDraggedPocketId(null);
                                setDragOverPocketId(null);
                              },
                            };

                            if (isInvestPocket) {
                              return (
                                <div
                                  key={pocket.id}
                                  {...pocketDragHandlers}
                                  className={cn(
                                    "p-2.5 rounded-xl bg-[var(--surface-raised)] hover:bg-[var(--surface-raised)]/90 border border-[var(--border)] text-[var(--text)] transition-all space-y-1.5 group/pocket cursor-grab active:cursor-grabbing",
                                    draggedPocketId === pocket.id && "opacity-40 scale-[0.98]",
                                    dragOverPocketId === pocket.id && "ring-2 ring-emerald-500/80 border-emerald-500"
                                  )}
                                >
                                  {/* Top Row: Identification on Left, Balance & Gain on Right */}
                                  <div className="flex items-start justify-between gap-2">
                                    <div className="min-w-0 flex-1">
                                      <div className="flex items-center gap-1.5 flex-wrap">
                                        <div
                                          className="cursor-grab active:cursor-grabbing text-[var(--muted)]/40 hover:text-[var(--muted)] transition-colors shrink-0"
                                          title="Tahan & geser untuk mengubah urutan"
                                        >
                                          <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="currentColor">
                                            <circle cx="9" cy="6" r="1.5" />
                                            <circle cx="15" cy="6" r="1.5" />
                                            <circle cx="9" cy="12" r="1.5" />
                                            <circle cx="15" cy="12" r="1.5" />
                                            <circle cx="9" cy="18" r="1.5" />
                                            <circle cx="15" cy="18" r="1.5" />
                                          </svg>
                                        </div>
                                        <span className="font-bold text-xs truncate text-[var(--text)]">{pocket.name}</span>
                                        {pocket.instrument_symbol && (
                                          <span className="px-1.5 py-0.5 rounded text-[10px] font-extrabold bg-[var(--surface)] border border-[var(--border)] text-[var(--text)]">
                                            {pocket.instrument_symbol}
                                          </span>
                                        )}
                                        {pocket.instrument_type && (
                                          <span className="px-1.5 py-0.5 rounded text-[9px] uppercase bg-violet-500/15 text-violet-600 dark:text-violet-300 font-semibold">
                                            {INSTRUMENT_OPTIONS.find((o) => o.value === pocket.instrument_type)?.label || pocket.instrument_type}
                                          </span>
                                        )}
                                      </div>
                                      {pocket.units ? (
                                        <div className="text-[11px] text-[var(--muted)] mt-0.5 font-medium pl-5">
                                          {pocket.units.toLocaleString("id-ID")} {getInstrumentConfig(pocket.instrument_type).unitUnit}
                                          {pocket.instrument_type === "stock" && (
                                            <span className="ml-1 text-[var(--text)] font-semibold">
                                              ({(pocket.units / 100).toLocaleString("id-ID")} lot)
                                            </span>
                                          )}
                                        </div>
                                      ) : null}
                                    </div>

                                    <div className="text-right shrink-0">
                                      <div className="text-xs sm:text-sm font-extrabold text-[var(--text)] tabular">
                                        {bal(pocket.balance)}
                                      </div>
                                      {pocket.capital_gain !== null && pocket.capital_gain !== undefined && (
                                        <div
                                          className={cn(
                                            "text-[10px] font-bold tabular mt-0.5",
                                            pocket.capital_gain >= 0
                                              ? "text-emerald-600 dark:text-emerald-400"
                                              : "text-rose-600 dark:text-rose-400"
                                          )}
                                        >
                                          {pocket.capital_gain >= 0 ? "▲ +" : "▼ "}{fmtMoney(pocket.capital_gain)} ({pocket.capital_gain_pct ?? 0}%)
                                        </div>
                                      )}
                                    </div>
                                  </div>

                                  {/* Middle Row: Full-width Price Comparison Pill */}
                                  {(pocket.last_price || pocket.avg_buy_price) && (
                                    <div className="px-2 py-1 rounded-lg bg-[var(--surface)] border border-[var(--border)]/70 text-[10px] flex items-center justify-between gap-2 flex-wrap">
                                      {pocket.last_price ? (
                                        <div className="flex items-center gap-1">
                                          <span className="text-[var(--muted)] text-[9px] uppercase font-semibold">Pasar:</span>
                                          <span className="font-bold tabular text-[var(--text)]">
                                            {fmtMoney(pocket.last_price)}
                                          </span>
                                          <span className="text-[9px] text-[var(--muted)]">
                                            /{getInstrumentConfig(pocket.instrument_type).unitUnit}
                                          </span>
                                        </div>
                                      ) : <div />}

                                      {pocket.avg_buy_price ? (
                                        <div className="flex items-center gap-1">
                                          <span className="text-[var(--muted)] text-[9px] uppercase font-semibold">Modal Beli:</span>
                                          <span className="font-bold tabular text-[var(--text)]">
                                            {fmtMoney(pocket.avg_buy_price)}
                                          </span>
                                          <span className="text-[9px] text-[var(--muted)]">
                                            /{getInstrumentConfig(pocket.instrument_type).unitUnit}
                                          </span>
                                        </div>
                                      ) : null}
                                    </div>
                                  )}

                                  {/* Bottom Row: Action Controls Bar (Full width, seamless) */}
                                  <div className="pt-1.5 border-t border-[var(--border)]/60 flex items-center justify-between gap-2">
                                    {/* Left: Trade Actions */}
                                    <div className="flex items-center gap-1">
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          setTradePocket(pocket);
                                          setTradeAction("buy");
                                          setTradeModalOpen(true);
                                        }}
                                        className="px-2 py-0.5 rounded-md text-[11px] font-bold bg-emerald-500/15 hover:bg-emerald-500/25 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20 transition-all pressable flex items-center gap-0.5"
                                        title="Catat Pembelian"
                                      >
                                        <span>+ Beli</span>
                                      </button>
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          setTradePocket(pocket);
                                          setTradeAction("sell");
                                          setTradeModalOpen(true);
                                        }}
                                        className="px-2 py-0.5 rounded-md text-[11px] font-bold bg-rose-500/15 hover:bg-rose-500/25 text-rose-600 dark:text-rose-400 border border-rose-500/20 transition-all pressable flex items-center gap-0.5"
                                        title="Catat Penjualan"
                                      >
                                        <span>− Jual</span>
                                      </button>
                                    </div>

                                    {/* Right: Management Icons */}
                                    <div className="flex items-center gap-0.5">
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          handleOpenValuation(pocket);
                                        }}
                                        className="p-1 rounded-md hover:bg-[var(--surface)] text-[var(--muted)] hover:text-emerald-600 dark:hover:text-emerald-400 transition-colors"
                                        title="Update Nilai Pasar (Valuation)"
                                      >
                                        <Icon name="trending-up" className="h-3 w-3" />
                                      </button>
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          setReconcileAccount(pocket);
                                          setActualBalStr(formatNumberWithDots(pocket.balance));
                                          setReconcileNotes("");
                                          setReconcileError("");
                                        }}
                                        className="p-1 rounded-md hover:bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] transition-colors"
                                        title="Sesuaikan Saldo"
                                      >
                                        <Icon name="check" className="h-3 w-3" />
                                      </button>
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          handleOpenTransfer(pocket.id);
                                        }}
                                        className="p-1 rounded-md hover:bg-[var(--surface)] text-[var(--muted)] hover:text-blue-500 transition-colors"
                                        title="Pindah Saldo"
                                      >
                                        <Icon name="move" className="h-3 w-3" />
                                      </button>
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          handleOpenEditAccount(pocket);
                                        }}
                                        className="p-1 rounded-md hover:bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] transition-colors"
                                        title="Ubah Nama Kantong"
                                      >
                                        <Icon name="edit" className="h-3 w-3" />
                                      </button>
                                      <button
                                        type="button"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          if (confirm(`Arsipkan kantong "${pocket.name}"?`)) {
                                            archiveMutation.mutate(pocket.id);
                                          }
                                        }}
                                        className="p-1 rounded-md hover:bg-rose-500/20 text-[var(--muted)] hover:text-rose-500 transition-colors"
                                        title="Arsipkan Kantong"
                                      >
                                        <Icon name="trash" className="h-3 w-3" />
                                      </button>
                                    </div>
                                  </div>
                                </div>
                              );
                            }

                            // Non-investment (regular) pocket
                            return (
                              <div
                                key={pocket.id}
                                {...pocketDragHandlers}
                                className={cn(
                                  "p-2 rounded-lg bg-[var(--surface-raised)] hover:bg-[var(--surface-raised)]/90 border border-[var(--border)] text-[var(--text)] transition-all space-y-1.5 group/pocket cursor-grab active:cursor-grabbing",
                                  draggedPocketId === pocket.id && "opacity-40 scale-[0.98]",
                                  dragOverPocketId === pocket.id && "ring-2 ring-emerald-500/80 border-emerald-500"
                                )}
                              >
                                <div className="flex items-center justify-between gap-2">
                                  <div className="flex items-center gap-1.5 min-w-0">
                                    <div
                                      className="cursor-grab active:cursor-grabbing text-[var(--muted)]/40 hover:text-[var(--muted)] transition-colors shrink-0"
                                      title="Tahan & geser untuk mengubah urutan"
                                    >
                                      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="currentColor">
                                        <circle cx="9" cy="6" r="1.5" />
                                        <circle cx="15" cy="6" r="1.5" />
                                        <circle cx="9" cy="12" r="1.5" />
                                        <circle cx="15" cy="12" r="1.5" />
                                        <circle cx="9" cy="18" r="1.5" />
                                        <circle cx="15" cy="18" r="1.5" />
                                      </svg>
                                    </div>
                                    <span className="font-semibold text-xs text-[var(--text)] truncate">{pocket.name}</span>
                                  </div>
                                  <span className="text-xs font-bold text-emerald-600 dark:text-emerald-400 tabular shrink-0">
                                    {bal(pocket.balance)}
                                  </span>
                                </div>
                                <div className="pt-1 border-t border-[var(--border)]/60 flex items-center justify-between gap-2 text-xs">
                                  <button
                                    type="button"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      handleOpenTransfer(pocket.id);
                                    }}
                                    className="flex items-center gap-1 font-bold text-[10px] text-emerald-600 dark:text-emerald-400 hover:text-emerald-500"
                                  >
                                    <Icon name="move" className="h-2.5 w-2.5 stroke-[2.5]" />
                                    <span>Pindah Saldo</span>
                                  </button>
                                  <div className="flex items-center gap-0.5">
                                    <button
                                      type="button"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        setReconcileAccount(pocket);
                                        setActualBalStr(formatNumberWithDots(pocket.balance));
                                        setReconcileNotes("");
                                        setReconcileError("");
                                      }}
                                      className="p-0.5 rounded hover:bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] transition-colors"
                                      title="Sesuaikan Saldo"
                                    >
                                      <Icon name="check" className="h-3 w-3 text-emerald-600 dark:text-emerald-400" />
                                    </button>
                                    <button
                                      type="button"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        handleOpenEditAccount(pocket);
                                      }}
                                      className="p-0.5 rounded hover:bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] transition-colors"
                                      title="Ubah Nama Kantong"
                                    >
                                      <Icon name="edit" className="h-3 w-3" />
                                    </button>
                                    <button
                                      type="button"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        if (confirm(`Arsipkan kantong "${pocket.name}"?`)) {
                                          archiveMutation.mutate(pocket.id);
                                        }
                                      }}
                                      className="p-0.5 rounded hover:bg-rose-500/20 text-[var(--muted)] hover:text-rose-500 transition-colors"
                                      title="Arsipkan Kantong"
                                    >
                                      <Icon name="trash" className="h-3 w-3" />
                                    </button>
                                  </div>
                                </div>
                              </div>
                            );
                          });
                        })()}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Card Bottom CTA (for Standalone accounts) */}
              {!hasChildren && (
                <div className="pt-3 border-t border-[var(--border)] flex items-center justify-between text-xs mt-4">
                  <span className="text-[11px] text-[var(--muted)] font-medium">
                    Saldo Awal: {bal(acc.initial_balance)}
                  </span>
                  <div className="flex items-center gap-2.5">
                    {acc.type === "investment" && (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleOpenValuation(acc);
                        }}
                        className="flex items-center gap-1 font-semibold text-xs text-emerald-600 dark:text-emerald-400 hover:text-emerald-500"
                      >
                        <Icon name="trending-up" className="h-3 w-3" />
                        <span>Update Nilai</span>
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        setReconcileAccount(acc);
                        setActualBalStr(formatNumberWithDots(acc.balance));
                        setReconcileNotes("");
                        setReconcileError("");
                      }}
                      className="flex items-center gap-1 font-bold text-xs text-[var(--muted)] hover:text-[var(--text)] transition-all pressable"
                    >
                      <Icon name="check" className="h-3 w-3 text-emerald-500 stroke-[2.5]" />
                      <span>Sesuaikan</span>
                    </button>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleOpenTransfer(acc.id);
                      }}
                      className="flex items-center gap-1 font-bold text-xs text-emerald-600 dark:text-emerald-400 hover:text-emerald-500 transition-all pressable"
                    >
                      <Icon name="move" className="h-3 w-3 stroke-[2.5]" />
                      <span>Pindah Saldo</span>
                    </button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Transfer Modal */}
      <Modal
        open={transferModalOpen}
        onClose={() => setTransferModalOpen(false)}
        title="Pindah Saldo Antar Rekening"
      >
        <div className="space-y-4 pt-2">
          {transferError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-xs text-rose-500">
              {transferError}
            </div>
          )}

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">Dari Rekening / Kantong</label>
            <select
              value={fromAccountId}
              onChange={(e) => setFromAccountId(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            >
              <AccountSelectOptions accounts={activeAccounts} formatBalance={bal} allowParentSelection={true} />
            </select>
          </div>

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">Ke Rekening / Kantong Tujuan</label>
            <select
              value={toAccountId}
              onChange={(e) => setToAccountId(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            >
              <AccountSelectOptions accounts={activeAccounts} formatBalance={bal} allowParentSelection={true} />
            </select>
          </div>

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">Nominal Uang (IDR)</label>
            <input
              type="text"
              inputMode="numeric"
              value={transferAmount}
              onChange={(e) => setTransferAmount(formatNumberWithDots(e.target.value))}
              placeholder="Contoh: 500.000"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
            />
          </div>

          <div>
            <label className="text-xs font-medium text-[var(--muted)] block mb-1">Catatan (opsional)</label>
            <input
              type="text"
              value={transferNotes}
              onChange={(e) => setTransferNotes(e.target.value)}
              placeholder="Contoh: Top-up saldo atau tarik tunai ATM"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setTransferModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="button"
              disabled={transferMutation.isPending}
              onClick={() => transferMutation.mutate()}
              className="flex-1 rounded-xl bg-transfer text-white py-2 text-xs font-semibold hover:bg-blue-600 disabled:opacity-50"
            >
              {transferMutation.isPending ? "Memproses..." : "Selesaikan Transfer"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Add / Edit Account Modal */}
      <Modal
        open={accountModalOpen}
        onClose={() => setAccountModalOpen(false)}
        title={
          editingAccount
            ? editingAccount.parent_id
              ? `Ubah Kantong: ${editingAccount.name}`
              : `Ubah Rekening: ${editingAccount.name}`
            : "Tambah Rekening Baru"
        }
      >
        <div className="space-y-4 pt-2 text-xs">
          {accError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-xs text-rose-500">
              {accError}
            </div>
          )}

          <div>
            <label className="font-medium text-[var(--muted)] block mb-1">
              {editingAccount?.parent_id ? "Nama Kantong" : "Nama Rekening"}
            </label>
            <input
              type="text"
              value={accName}
              onChange={(e) => setAccName(e.target.value)}
              placeholder="Contoh: BCA Utama, Bibit, Binance, GoPay"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
          </div>

          {!editingAccount?.parent_id && (
            <div className="space-y-2">
              <label className="font-medium text-[var(--muted)] block">
                Warna Tag Rekening
              </label>
              <div className="flex items-center gap-2 flex-wrap">
                {ACCOUNT_TAG_COLORS.map((col) => {
                  const isSelected = accColor.toLowerCase() === col.hex.toLowerCase();
                  return (
                    <button
                      key={col.hex}
                      type="button"
                      onClick={() => setAccColor(col.hex)}
                      className={cn(
                        "h-7 w-7 rounded-full transition-transform flex items-center justify-center text-white",
                        isSelected
                          ? "ring-2 ring-offset-2 ring-[var(--text)] scale-110"
                          : "hover:scale-105 opacity-80 hover:opacity-100"
                      )}
                      style={{ backgroundColor: col.hex }}
                      title={col.name}
                    >
                      {isSelected && <Icon name="check" className="h-3.5 w-3.5" />}
                    </button>
                  );
                })}
              </div>

              {accName.trim() && (
                <div className="mt-2 p-2.5 rounded-xl bg-[var(--surface-raised)] border border-[var(--border)] flex items-center justify-between">
                  <span className="text-[11px] text-[var(--muted)] font-medium">Pratinjau Tag Rekening:</span>
                  <span
                    style={{
                      backgroundColor: accColor,
                      color: getContrastTextColor(accColor),
                    }}
                    className="font-bold text-xs sm:text-sm px-3 py-1 rounded-xl shadow-2xs"
                  >
                    {accName.trim()}
                  </span>
                </div>
              )}
            </div>
          )}

          {!editingAccount?.parent_id ? (
            <>
              <div>
                <label className="font-medium text-[var(--muted)] block mb-1">Jenis Rekening</label>
                <select
                  value={accType}
                  onChange={(e) => setAccType(e.target.value as any)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="bank">Rekening Bank (BCA, Mandiri, BRI, dll)</option>
                  <option value="wallet">Dompet Digital / E-Wallet (GoPay, OVO, ShopeePay, dll)</option>
                  <option value="cash">Uang Tunai / Cash (Dompet, Brankas)</option>
                  <option value="investment">Rekening Investasi (Bibit, Stockbit, Binance, dll)</option>
                </select>
              </div>

              {editingAccount && !editingAccount.parent_id && editingAccount.children && editingAccount.children.length > 0 && (
                <div>
                  <label className="font-medium text-[var(--muted)] block mb-1">
                    Kantong Default (Penerima Notifikasi Otomatis)
                  </label>
                  <select
                    value={accDefaultPocketId}
                    onChange={(e) => setAccDefaultPocketId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tanpa Kantong Default (Gunakan Rekening Induk)</option>
                    {editingAccount.children.map((child) => (
                      <option key={child.id} value={child.id}>
                        {child.name} ({bal(child.balance)})
                      </option>
                    ))}
                  </select>
                  <p className="text-[10px] text-[var(--muted)] mt-1">
                    Transaksi mobile tanpa rincian kantong akan otomatis dialokasikan ke kantong default ini.
                  </p>
                </div>
              )}

              {accType === "investment" && (
                <div className="space-y-3">
                  <div>
                    <label className="font-medium text-[var(--muted)] block mb-1.5">Model Akun Investasi</label>
                    <div className="grid grid-cols-2 gap-2">
                      <button
                        type="button"
                        onClick={() => setAccIsMultiInstrument(true)}
                        className={cn(
                          "p-2.5 rounded-xl border text-left transition-all",
                          accIsMultiInstrument
                            ? "bg-violet-600/15 border-violet-500 text-white shadow-xs"
                            : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--muted)] hover:text-[var(--text)]"
                        )}
                      >
                        <div className="flex items-center gap-1.5 font-bold text-xs text-[var(--text)]">
                          <Icon name="layers" className="h-3.5 w-3.5 text-violet-400" />
                          <span>Platform Multi-Kantong</span>
                        </div>
                        <p className="text-[10px] text-[var(--muted)] mt-1 leading-relaxed">
                          Bibit, Bareksa, Stockbit. Memiliki banyak produk instrumen yang dikelola per kantong di dalam.
                        </p>
                      </button>

                      <button
                        type="button"
                        onClick={() => setAccIsMultiInstrument(false)}
                        className={cn(
                          "p-2.5 rounded-xl border text-left transition-all",
                          !accIsMultiInstrument
                            ? "bg-violet-600/15 border-violet-500 text-white shadow-xs"
                            : "bg-[var(--surface-raised)] border-[var(--border)] text-[var(--muted)] hover:text-[var(--text)]"
                        )}
                      >
                        <div className="flex items-center gap-1.5 font-bold text-xs text-[var(--text)]">
                          <Icon name="chart" className="h-3.5 w-3.5 text-violet-400" />
                          <span>Instrumen Tunggal</span>
                        </div>
                        <p className="text-[10px] text-[var(--muted)] mt-1 leading-relaxed">
                          Binance (langsung Kripto), Emas Fisik, atau akun khusus 1 instrumen tanpa kantong.
                        </p>
                      </button>
                    </div>
                  </div>

                  {accIsMultiInstrument && (
                    <div className="p-3 rounded-xl bg-violet-500/10 border border-violet-500/20 text-violet-300 text-[11px] leading-relaxed">
                      💡 Rekening <strong>{accName || "Investasi"}</strong> akan dibuat sebagai akun induk platform. Setelah dibuat, Anda bisa menambahkan berbagai kantong instrumen (seperti RDPU, Saham BBCA, Bareksa Sukuk, dll) di dalamnya.
                    </div>
                  )}

                  <div className="pt-2 border-t border-[var(--border)]">
                    <label className="font-medium text-[var(--muted)] flex items-center justify-between mb-1.5">
                      <span className="flex items-center gap-1.5 font-bold text-xs text-[var(--text)]">
                        <Icon name="credit-card" className="h-3.5 w-3.5 text-blue-400" />
                        <span>Sumber Dana / RDN Default</span>
                      </span>
                      <span className="text-[10px] text-[var(--muted)]">Opsional</span>
                    </label>
                    <select
                      value={accDefaultFundingId}
                      onChange={(e) => setAccDefaultFundingId(e.target.value)}
                      className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                    >
                      <option value="">-- Tanpa RDN / Sumber Default --</option>
                      {accounts
                        .filter((a) => a.id !== editingAccount?.id && (a.type === "bank" || a.type === "wallet" || a.type === "cash"))
                        .map((a) => (
                          <option key={a.id} value={a.id}>
                            {a.name} ({a.type.toUpperCase()})
                          </option>
                        ))}
                    </select>
                    <p className="text-[10px] text-[var(--muted)] mt-1 leading-relaxed">
                      Notifikasi beli/jual dari broker (misal Stockbit) akan otomatis mendebit rekening ini tanpa memotong budget belanja harian.
                    </p>
                  </div>
                </div>
              )}
            </>
          ) : (
            /* Editing a child pocket */
            <div>
              <label className="font-medium text-[var(--muted)] block mb-1.5">Tipe Kantong</label>
              <div className="grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={() => setAccIsMultiInstrument(true)}
                  className={cn(
                    "px-3 py-2 rounded-xl border text-xs font-semibold transition-all flex items-center justify-center gap-1.5",
                    accIsMultiInstrument
                      ? "bg-blue-600 text-white border-blue-500 shadow-xs"
                      : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                  )}
                >
                  <Icon name="credit-card" className="h-3.5 w-3.5" />
                  <span>Kas / Tabungan Biasa</span>
                </button>
                <button
                  type="button"
                  onClick={() => setAccIsMultiInstrument(false)}
                  className={cn(
                    "px-3 py-2 rounded-xl border text-xs font-semibold transition-all flex items-center justify-center gap-1.5",
                    !accIsMultiInstrument
                      ? "bg-violet-600 text-white border-violet-500 shadow-xs"
                      : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                  )}
                >
                  <Icon name="chart" className="h-3.5 w-3.5" />
                  <span>Instrumen Investasi</span>
                </button>
              </div>
            </div>
          )}

          {/* Show Instrument Fields if single instrument OR editing investment pocket */}
          {((!editingAccount?.parent_id && accType === "investment" && !accIsMultiInstrument) ||
            (editingAccount?.parent_id && !accIsMultiInstrument)) && (
            <div className="space-y-3 p-3.5 rounded-2xl bg-violet-500/5 border border-violet-500/20">
              <div>
                <label className="font-medium text-[var(--muted)] block mb-1.5">Tipe Instrumen</label>
                <div className="grid grid-cols-3 gap-1.5">
                  {INSTRUMENT_OPTIONS.map((opt) => (
                    <button
                      key={opt.value}
                      type="button"
                      onClick={() => setAccInstrumentType(opt.value)}
                      className={cn(
                        "px-2 py-1.5 rounded-xl border text-xs font-semibold transition-all",
                        accInstrumentType === opt.value
                          ? "bg-violet-600 text-white border-violet-500 shadow-xs"
                          : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                      )}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="relative">
                <label className="font-medium text-[var(--muted)] flex items-center justify-between mb-1">
                  <span>Simbol Ticker / Kode (Yahoo Finance)</span>
                  <span className="text-[10px] text-[var(--muted)]">Opsional</span>
                </label>
                <div className="relative">
                  <input
                    type="text"
                    value={accTicker}
                    onChange={(e) => handleSearchTicker(e.target.value, false)}
                    placeholder="Contoh: BBCA, GC=F, AAPL, BTC-USD"
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] uppercase pr-8 font-semibold"
                  />
                  {isSearchingTicker && (
                    <div className="absolute right-2.5 top-2.5">
                      <Icon name="refresh-cw" className="h-4 w-4 animate-spin text-violet-400" />
                    </div>
                  )}
                </div>
                <p className="text-[10px] text-[var(--muted)] mt-1">
                  Kosongkan jika produk manual tanpa ticker publik (misal RDPU Bibit / Bareksa).
                </p>

                {tickerSuggestions.length > 0 && (
                  <div className="absolute z-50 left-0 right-0 mt-1 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-2xl overflow-hidden max-h-48 overflow-y-auto">
                    {tickerSuggestions.map((item) => (
                      <div
                        key={item.symbol}
                        onClick={() => {
                          setAccTicker(item.symbol);
                          if (!accName || accName === "BCA Utama") {
                            setAccName(item.name);
                          }
                          if (item.type) {
                            setAccInstrumentType(item.type);
                          }
                          setTickerSuggestions([]);
                        }}
                        className="p-2.5 hover:bg-[var(--surface-raised)] cursor-pointer border-b border-[var(--border)]/50 last:border-0 flex items-center justify-between text-xs"
                      >
                        <div>
                          <span className="font-bold text-violet-400">{item.symbol}</span>
                          <span className="text-[var(--text)] ml-2">{item.name}</span>
                        </div>
                        <span className="text-[10px] uppercase text-[var(--muted)] px-1.5 py-0.5 rounded bg-[var(--surface-raised)] font-medium">
                          {item.exchange || item.type}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {(() => {
                const aCfg = getInstrumentConfig(accInstrumentType);
                return (
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="font-medium text-[var(--muted)] block mb-1">
                        {aCfg.unitsLabel}
                      </label>
                      <input
                        type="text"
                        inputMode="decimal"
                        value={accUnits}
                        onChange={(e) => {
                          setAccUnits(e.target.value);
                          const u = parseUnits(e.target.value);
                          const p = parseDecimal(accAvgBuyPrice);
                          if (u > 0 && p > 0) {
                            setAccInitBal(formatNumberWithDots(Math.round(u * p)));
                          }
                        }}
                        placeholder={aCfg.unitsPlaceholder}
                        className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                      />
                      <p className="text-[10px] text-[var(--muted)] mt-0.5">{aCfg.unitsHint}</p>
                    </div>
                    <div>
                      <label className="font-medium text-[var(--muted)] block mb-1">
                        {aCfg.priceLabel}
                      </label>
                      <input
                        type="text"
                        inputMode="decimal"
                        value={accAvgBuyPrice}
                        onChange={(e) => {
                          const formatted = formatDecimalInput(e.target.value);
                          setAccAvgBuyPrice(formatted);
                          const p = parseDecimal(formatted);
                          const u = parseUnits(accUnits);
                          if (u > 0 && p > 0) {
                            setAccInitBal(formatNumberWithDots(Math.round(u * p)));
                          }
                        }}
                        placeholder={aCfg.pricePlaceholder}
                        className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                      />
                    </div>
                  </div>
                );
              })()}
            </div>
          )}

          {!editingAccount && (
            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                {accType === "investment"
                  ? accIsMultiInstrument
                    ? "Saldo Kas / RDN Awal (IDR - Opsional)"
                    : getInstrumentConfig(accInstrumentType).totalLabel
                  : "Saldo Awal (IDR)"}
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={accInitBal}
                onChange={(e) => setAccInitBal(formatNumberWithDots(e.target.value))}
                placeholder="0"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-bold"
              />
            </div>
          )}

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setAccountModalOpen(false)}
              className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Batal
            </button>
            <button
              type="button"
              disabled={saveAccountMutation.isPending}
              onClick={() => saveAccountMutation.mutate()}
              className="flex-1 rounded-xl bg-income text-white py-2 text-xs font-semibold hover:bg-emerald-600 disabled:opacity-50"
            >
              {saveAccountMutation.isPending
                ? "Menyimpan..."
                : editingAccount
                ? "Simpan Perubahan"
                : "Tambah Rekening"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Pocket Modal */}
      {pocketParentAccount && (
        <Modal
          open={pocketModalOpen}
          onClose={() => setPocketModalOpen(false)}
          title={`Tambah Kantong di ${pocketParentAccount.name}`}
        >
          <div className="space-y-4 pt-2 text-xs">
            {pocketError && (
              <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {pocketError}
              </div>
            )}

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">Nama Kantong</label>
              <input
                type="text"
                value={pocketName}
                onChange={(e) => setPocketName(e.target.value)}
                placeholder={
                  pocketParentAccount.type === "investment"
                    ? "Contoh: Bibit RDPU, Saham BBCA, Bareksa Sukuk"
                    : "Contoh: Tabungan Utama, RDPU BCA, Tabungan Emas"
                }
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              />
            </div>

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1.5">Tipe Kantong</label>
              <div className="grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={() => setPocketIsInvestment(false)}
                  className={cn(
                    "px-3 py-2 rounded-xl border text-xs font-semibold transition-all flex items-center justify-center gap-1.5",
                    !pocketIsInvestment
                      ? "bg-blue-600 text-white border-blue-500 shadow-xs"
                      : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                  )}
                >
                  <Icon name="credit-card" className="h-3.5 w-3.5" />
                  <span>Kas / Tabungan Biasa</span>
                </button>
                <button
                  type="button"
                  onClick={() => setPocketIsInvestment(true)}
                  className={cn(
                    "px-3 py-2 rounded-xl border text-xs font-semibold transition-all flex items-center justify-center gap-1.5",
                    pocketIsInvestment
                      ? "bg-violet-600 text-white border-violet-500 shadow-xs"
                      : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                  )}
                >
                  <Icon name="chart" className="h-3.5 w-3.5" />
                  <span>Instrumen Investasi</span>
                </button>
              </div>
              <p className="text-[10px] text-[var(--muted)] mt-1">
                {pocketIsInvestment
                  ? "Kantong ini untuk instrumen investasi (Saham, Reksadana, Emas, Kripto, Deposito) dengan pemantauan harga pasar / capital gain."
                  : "Kantong ini untuk uang cair atau pos belanja operasional biasa."}
              </p>
            </div>

            {pocketIsInvestment && (
              <div className="space-y-3 p-3.5 rounded-2xl bg-violet-500/5 border border-violet-500/20">
                <div>
                  <label className="font-medium text-[var(--muted)] block mb-1.5">Tipe Instrumen</label>
                  <div className="grid grid-cols-3 gap-1.5">
                    {INSTRUMENT_OPTIONS.map((opt) => (
                      <button
                        key={opt.value}
                        type="button"
                        onClick={() => setPocketInstrumentType(opt.value)}
                        className={cn(
                          "px-2 py-1.5 rounded-xl border text-xs font-semibold transition-all",
                          pocketInstrumentType === opt.value
                            ? "bg-violet-600 text-white border-violet-500 shadow-xs"
                            : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                        )}
                      >
                        {opt.label}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="relative">
                  <label className="font-medium text-[var(--muted)] flex items-center justify-between mb-1">
                    <span>Simbol Ticker / Kode (Yahoo Finance)</span>
                    <span className="text-[10px] text-[var(--muted)]">Opsional</span>
                  </label>
                  <div className="relative">
                    <input
                      type="text"
                      value={pocketTicker}
                      onChange={(e) => handleSearchTicker(e.target.value, true)}
                      placeholder="Contoh: BBCA, GC=F, AAPL, BTC-USD"
                      className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] uppercase pr-8 font-semibold"
                    />
                    {pocketIsSearchingTicker && (
                      <div className="absolute right-2.5 top-2.5">
                        <Icon name="refresh-cw" className="h-4 w-4 animate-spin text-violet-400" />
                      </div>
                    )}
                  </div>
                  <p className="text-[10px] text-[var(--muted)] mt-1">
                    Kosongkan jika produk manual tanpa ticker publik (misal RDPU Bibit / BCA Reksadana). Nilai pasar dapat diupdate berkala via tombol "Update Nilai".
                  </p>

                  {pocketTickerSuggestions.length > 0 && (
                    <div className="absolute z-50 left-0 right-0 mt-1 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-2xl overflow-hidden max-h-48 overflow-y-auto">
                      {pocketTickerSuggestions.map((item) => (
                        <div
                          key={item.symbol}
                          onClick={() => {
                            setPocketTicker(item.symbol);
                            if (!pocketName) {
                              setPocketName(item.name);
                            }
                            if (item.type) {
                              setPocketInstrumentType(item.type);
                            }
                            setPocketTickerSuggestions([]);
                          }}
                          className="p-2.5 hover:bg-[var(--surface-raised)] cursor-pointer border-b border-[var(--border)]/50 last:border-0 flex items-center justify-between text-xs"
                        >
                          <div>
                            <span className="font-bold text-violet-400">{item.symbol}</span>
                            <span className="text-[var(--text)] ml-2">{item.name}</span>
                          </div>
                          <span className="text-[10px] uppercase text-[var(--muted)] px-1.5 py-0.5 rounded bg-[var(--surface-raised)] font-medium">
                            {item.exchange || item.type}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {(() => {
                  const pCfg = getInstrumentConfig(pocketInstrumentType);
                  return (
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="font-medium text-[var(--muted)] block mb-1">
                          {pCfg.unitsLabel}
                        </label>
                        <input
                          type="text"
                          inputMode="decimal"
                          value={pocketUnits}
                          onChange={(e) => {
                            setPocketUnits(e.target.value);
                            const u = parseUnits(e.target.value);
                            const p = parseDecimal(pocketAvgBuyPrice);
                            if (u > 0 && p > 0) {
                              setPocketInitBal(formatNumberWithDots(Math.round(u * p)));
                            }
                          }}
                          placeholder={pCfg.unitsPlaceholder}
                          className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                        />
                        <p className="text-[10px] text-[var(--muted)] mt-0.5">{pCfg.unitsHint}</p>
                      </div>
                      <div>
                        <label className="font-medium text-[var(--muted)] block mb-1">
                          {pCfg.priceLabel}
                        </label>
                        <input
                          type="text"
                          inputMode="decimal"
                          value={pocketAvgBuyPrice}
                          onChange={(e) => {
                            const formatted = formatDecimalInput(e.target.value);
                            setPocketAvgBuyPrice(formatted);
                            const p = parseDecimal(formatted);
                            const u = parseUnits(pocketUnits);
                            if (u > 0 && p > 0) {
                              setPocketInitBal(formatNumberWithDots(Math.round(u * p)));
                            }
                          }}
                          placeholder={pCfg.pricePlaceholder}
                          className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                        />
                      </div>
                    </div>
                  );
                })()}
              </div>
            )}

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                {pocketIsInvestment
                  ? getInstrumentConfig(pocketInstrumentType).totalLabel
                  : "Saldo Awal Kantong (IDR)"}
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={pocketInitBal}
                onChange={(e) => setPocketInitBal(formatNumberWithDots(e.target.value))}
                placeholder="0"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-bold"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setPocketModalOpen(false)}
                className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={savePocketMutation.isPending}
                onClick={() => savePocketMutation.mutate()}
                className="flex-1 rounded-xl bg-income text-white py-2 text-xs font-semibold hover:bg-emerald-600 disabled:opacity-50"
              >
                {savePocketMutation.isPending ? "Menyimpan..." : "Buat Kantong"}
              </button>
            </div>
          </div>
        </Modal>
      )}

      {/* Valuation Modal (Update Nilai Investasi - Mode B / Penyesuaian) */}
      {valuationAccount && (
        <Modal
          open={Boolean(valuationAccount)}
          onClose={() => setValuationAccount(null)}
          title={`Update Nilai: ${valuationAccount.name}`}
        >
          <div className="space-y-4 pt-2 text-xs">
            {valError && (
              <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {valError}
              </div>
            )}

            <p className="text-xs text-[var(--muted)]">
              Perbarui nilai pasar investasi terkini (misal dari aplikasi Bibit, Bareksa, atau harga penutupan). Capital Gain akan dihitung otomatis terhadap modal beli Anda.
            </p>

            <div className="p-3.5 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-[var(--muted)]">Nilai Tercatat Saat Ini:</span>
                <span className="font-bold tabular text-sm text-[var(--text)]">
                  {bal(valuationAccount.balance)}
                </span>
              </div>
              {valuationAccount.units && valuationAccount.avg_buy_price ? (
                <div className="flex items-center justify-between text-[11px] text-[var(--muted)] pt-1.5 border-t border-[var(--border)]/50">
                  <span>Kepemilikan:</span>
                  <span className="tabular text-[var(--text)]">
                    {valuationAccount.units} {getInstrumentConfig(valuationAccount.instrument_type).unitUnit} @ {fmtMoney(valuationAccount.avg_buy_price)}
                  </span>
                </div>
              ) : null}
            </div>

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                Nilai Pasar Terkini (IDR)
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={valCurrentBalStr}
                onChange={(e) => setValCurrentBalStr(formatNumberWithDots(e.target.value))}
                placeholder="Contoh: 10.450.000"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
              />
              <p className="text-[10px] text-[var(--muted)] mt-1">
                Nilai portofolio total saat ini dari aplikasi atau pantauan harga terbaru.
              </p>
            </div>

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                Total Modal Terinvestasi / Harga Beli (IDR)
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={valCostBasisStr}
                onChange={(e) => setValCostBasisStr(formatNumberWithDots(e.target.value))}
                placeholder="Contoh: 10.000.000"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
              />
              <p className="text-[10px] text-[var(--muted)] mt-1">
                Total modal bersih yang dibayarkan saat membeli ({valuationAccount.units ? `${valuationAccount.units} ${getInstrumentConfig(valuationAccount.instrument_type).unitUnit}` : "aset ini"}).
              </p>
            </div>

            {/* Capital Gain preview */}
            {(() => {
              const curVal = parseNumberFromDots(valCurrentBalStr);
              const costVal = parseNumberFromDots(valCostBasisStr);
              const gain = curVal - costVal;
              const gainPct = costVal > 0 ? ((gain / costVal) * 100).toFixed(2) : "0.00";
              return (
                <div className="p-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/50 space-y-1">
                  <div className="flex justify-between items-center tabular">
                    <span className="text-[var(--muted)]">Estimasi Capital Gain:</span>
                    <span
                      className={cn(
                        "font-bold text-xs",
                        gain >= 0 ? "text-emerald-400" : "text-rose-400"
                      )}
                    >
                      {gain >= 0 ? "+" : ""}{fmtMoney(gain)} ({gain >= 0 ? "+" : ""}{gainPct}%)
                    </span>
                  </div>
                </div>
              );
            })()}

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                Catatan (Opsional)
              </label>
              <input
                type="text"
                value={valNotes}
                onChange={(e) => setValNotes(e.target.value)}
                placeholder="Contoh: Update return bulanan RDPU Bibit"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setValuationAccount(null)}
                className="btn-secondary flex-1"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={valuationMutation.isPending}
                onClick={() => valuationMutation.mutate()}
                className="btn-primary flex-1"
              >
                {valuationMutation.isPending ? "Menyimpan..." : "Simpan Nilai Baru"}
              </button>
            </div>
          </div>
        </Modal>
      )}
      {reconcileAccount && (
        <Modal
          open={Boolean(reconcileAccount)}
          onClose={() => setReconcileAccount(null)}
          title={`Sesuaikan Saldo: ${reconcileAccount.name}`}
        >
          <div className="space-y-4 pt-2 text-xs">
            {reconcileError && (
              <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {reconcileError}
              </div>
            )}

            <p className="text-xs text-[var(--muted)]">
              Masukkan nominal saldo yang saat ini tercatat pada aplikasi bank atau dompet fisik Anda. Transaksi penyesuaian akan dicatat secara otomatis jika ada selisih.
            </p>

            <div className="p-3.5 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] flex items-center justify-between">
              <span className="text-[var(--muted)]">Saldo Tercatat Saat Ini:</span>
              <span className="font-bold tabular text-sm text-[var(--text)]">
                {bal(reconcileAccount.balance)}
              </span>
            </div>

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                Saldo Sebenarnya di Bank / Dompet (IDR)
              </label>
              <input
                type="text"
                inputMode="numeric"
                value={actualBalStr}
                onChange={(e) => setActualBalStr(formatNumberWithDots(e.target.value))}
                placeholder="Contoh: 10.000.000"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-base font-bold tabular text-[var(--text)]"
              />
            </div>

            {/* Calculated variance preview */}
            {(() => {
              const actual = parseNumberFromDots(actualBalStr);
              const diff = actual - reconcileAccount.balance;
              return (
                <div className="p-3 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/50 space-y-1">
                  <div className="flex justify-between items-center tabular">
                    <span className="text-[var(--muted)]">Penyesuaian Selisih:</span>
                    <span
                      className={cn(
                        "font-bold text-xs",
                        diff > 0
                          ? "text-emerald-500"
                          : diff < 0
                          ? "text-rose-500"
                          : "text-[var(--muted)]"
                      )}
                    >
                      {diff > 0 ? `+${bal(diff)} (Uang Masuk)` : diff < 0 ? `-${bal(Math.abs(diff))} (Uang Keluar)` : "0 (Sesuai / Cocok)"}
                    </span>
                  </div>
                </div>
              );
            })()}

            <div>
              <label className="font-medium text-[var(--muted)] block mb-1">
                Catatan Penyesuaian (Opsional)
              </label>
              <input
                type="text"
                value={reconcileNotes}
                onChange={(e) => setReconcileNotes(e.target.value)}
                placeholder="Contoh: Biaya admin bulanan bank atau pembulatan"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setReconcileAccount(null)}
                className="btn-secondary flex-1"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={reconcileMutation.isPending}
                onClick={() => reconcileMutation.mutate()}
                className="btn-primary flex-1"
              >
                {reconcileMutation.isPending ? "Menyesuaikan..." : "Konfirmasi & Sesuaikan Saldo"}
              </button>
            </div>
          </div>
        </Modal>
      )}

      {/* Payroll Allocation Modal */}
      <PayrollAllocationModal
        open={payrollModalOpen}
        onClose={() => setPayrollModalOpen(false)}
        onOpenRulesManager={() => {
          setPayrollModalOpen(false);
          setRecurringModalOpen(true);
        }}
      />

      {/* Recurring Rules Management Modal */}
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => setRecurringModalOpen(false)}
      />

      {/* Investment Trade Modal (Beli / Jual) */}
      <InvestmentTradeModal
        open={tradeModalOpen}
        onClose={() => setTradeModalOpen(false)}
        pocket={tradePocket}
        allAccounts={accountsData?.accounts ?? []}
        defaultAction={tradeAction}
      />
    </div>
  );
}
