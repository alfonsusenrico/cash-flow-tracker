"use client";

import { useState, useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots, formatDecimalInput, parseDecimal, parseUnits } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { MonetaryInput } from "@/components/ui/FormField";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { Icon } from "@/components/ui/Icon";
import { InternalMovementModal } from "@/components/ui/InternalMovementModal";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { AccountOptions } from "@/components/ui/AccountOptions";
import { PayrollAllocationModal } from "@/components/recurring/PayrollAllocationModal";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";
import { InvestmentTradeModal } from "@/components/ui/InvestmentTradeModal";
import { listLiquidAccountChoices } from "@/lib/accountOptions";

function getContrastTextColor(hexColor?: string): string {
  if (!hexColor || !hexColor.startsWith("#")) return "#FFFFFF";
  const hex = hexColor.replace("#", "");
  if (hex.length !== 6 && hex.length !== 3) return "#FFFFFF";
  const r = parseInt(hex.length === 3 ? hex[0] + hex[0] : hex.slice(0, 2), 16);
  const g = parseInt(hex.length === 3 ? hex[1] + hex[1] : hex.slice(2, 4), 16);
  const b = parseInt(hex.length === 3 ? hex[2] + hex[2] : hex.slice(4, 6), 16);
  const linear = (channel: number) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  };
  const luminance = 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b);
  return luminance > 0.179 ? "#000000" : "#FFFFFF";
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
          unitsHint: "Masukkan pokok pada nilai pembukaan di bawah",
          priceLabel: "",
          pricePlaceholder: "",
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
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
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
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
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
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: queryKeys.accounts });
      setOrderedAccountIds(null);
      setOrderedPocketsByParent({});
      setOrderError("");
      setOrderStatus("Urutan rekening tersimpan.");
    },
    onError: () => {
      setOrderedAccountIds(null);
      setOrderedPocketsByParent({});
      setOrderError("Urutan gagal disimpan. Coba lagi.");
      setOrderStatus("");
    },
  });
  const [orderError, setOrderError] = useState("");
  const [orderStatus, setOrderStatus] = useState("");
  const [accountActionError, setAccountActionError] = useState("");

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
      if (!clean) throw new Error("Masukkan saldo aktual sebelum menyesuaikan rekening.");
      const actual = parseInt(clean, 10);
      return api.post(`/accounts/${reconcileAccount.id}/reconcile`, {
        actual_balance: actual,
        notes: reconcileNotes.trim() || null,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      setReconcileAccount(null);
    },
    onError: (err: any) => {
      setReconcileError(err?.message || "Gagal menyesuaikan saldo");
      document.getElementById("reconcile-actual-balance")?.focus();
    },
  });

  const { data: accountsData, isLoading: accountsLoading } = useQuery<AccountsResponse>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
  });

  const { data: netWorthData, isLoading: netWorthLoading } = useQuery<NetWorthResponse>({
    queryKey: queryKeys.dashboard.netWorth,
    queryFn: () => api.get("/dashboard/net-worth"),
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const activeAccounts = useMemo(() => accounts.filter((a) => !a.is_archived), [accounts]);
  const liquidAccounts = useMemo(
    () => listLiquidAccountChoices(activeAccounts).filter((account) => !account.is_archived),
    [activeAccounts],
  );
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
  const totalPocketCount = topAccounts.reduce((count, account) => count + (account.children?.length ?? 0), 0);

  const [mobileAccountFilter, setMobileAccountFilter] = useState<
    "all" | "bank_wallet" | "cash" | "investment"
  >("all");

  const filteredMobileAccounts = useMemo(() => {
    if (mobileAccountFilter === "bank_wallet") {
      return topAccounts.filter((a) => a.type === "bank" || a.type === "wallet");
    }
    if (mobileAccountFilter === "cash") {
      return topAccounts.filter((a) => a.type === "cash");
    }
    if (mobileAccountFilter === "investment") {
      return topAccounts.filter(
        (a) => a.type === "investment" || Boolean(a.instrument_type)
      );
    }
    return topAccounts;
  }, [topAccounts, mobileAccountFilter]);

  const netWorth = netWorthData?.net_worth ?? totalBalance;
  const totalAssets = netWorthData?.total_assets ?? totalBalance;
  const totalLiabilities = netWorthData?.total_liabilities ?? 0;
  const runway = netWorthData?.runway;

  const animNetWorth = useAnimatedCounter(netWorth);
  const animTotalAssets = useAnimatedCounter(totalAssets);
  const animTotalLiabilities = useAnimatedCounter(totalLiabilities);

  // Open Transfer modal preselected
  const handleOpenTransfer = (sourceAccId?: string) => {
    const transactable = liquidAccounts;
    if (transactable.length < 2) {
      return;
    }
    const source = transactable.some((account) => account.id === sourceAccId)
      ? sourceAccId!
      : transactable[0].id;
    const dest = transactable.find((a) => a.id !== source)?.id || transactable[1].id;
    setFromAccountId(source);
    setToAccountId(dest);
    setTransferModalOpen(true);
  };
  const canTransferFrom = (accountId: string) =>
    liquidAccounts.length >= 2 && liquidAccounts.some((account) => account.id === accountId);

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
      const tracksUnits = isInvest && pocketInstrumentType !== "deposit";
      const pUnitsNum = tracksUnits && pocketUnits.trim() ? parseUnits(pocketUnits.trim()) : undefined;
      const pAvgPriceNum = tracksUnits && pocketAvgBuyPrice.trim() ? parseDecimal(pocketAvgBuyPrice.trim()) : undefined;
      const pTickerSymbol = tracksUnits && pocketTicker.trim() ? pocketTicker.trim().toUpperCase() : undefined;
      let pocketType = pocketParentAccount.type;
      if (isInvest) {
        pocketType = "investment";
      } else if (pocketType === "investment") {
        pocketType = "bank";
      }

      return api.post("/accounts", {
        name,
        type: pocketType,
        parent_id: pocketParentAccount.id,
        initial_balance: initBal,
        instrument_type: isInvest ? pocketInstrumentType : null,
        instrument_symbol: isInvest ? pTickerSymbol : null,
        units: isInvest ? pUnitsNum : null,
        avg_buy_price: isInvest ? pAvgPriceNum : null,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setPocketModalOpen(false);
    },
    onError: (err: any) => {
      const message = err?.message || "Gagal membuat kantong";
      setPocketError(message);
      if (/nama kantong|pocket name/i.test(message)) {
        document.getElementById("pocket-name")?.focus();
      } else if (/saldo|initial_balance/i.test(message)) {
        document.getElementById("pocket-initial-balance")?.focus();
      }
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

      const tracksUnits = hasInstrument && accInstrumentType !== "deposit";
      const unitsNum = tracksUnits && accUnits.trim() ? parseUnits(accUnits.trim()) : null;
      const avgPriceNum = tracksUnits && accAvgBuyPrice.trim() ? parseDecimal(accAvgBuyPrice.trim()) : null;
      const tickerSymbol = hasInstrument && accInstrumentType !== "deposit" && accTicker.trim()
        ? accTicker.trim().toUpperCase()
        : null;

      const defaultFunding = !isEditingPocket && accType === "investment" && accDefaultFundingId.trim() ? accDefaultFundingId.trim() : null;
      const openingBalance = accType === "investment" && accIsMultiInstrument ? 0 : initBal;
      let savedType = accType;
      if (isEditingPocket && editingAccount) {
        savedType = editingAccount.type;
        if (isInvestPocket) {
          savedType = "investment";
        } else if (savedType === "investment") {
          savedType = "bank";
        }
      }

      if (editingAccount) {
        return api.patch(`/accounts/${editingAccount.id}`, {
          name,
          type: savedType,
          instrument_type: hasInstrument ? accInstrumentType : null,
          instrument_symbol: tickerSymbol,
          ...(!(hasInstrument && accInstrumentType === "deposit" && editingAccount.instrument_type === "deposit") ? {
            units: unitsNum,
            avg_buy_price: avgPriceNum,
          } : {}),
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
          initial_balance: openingBalance,
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
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setAccountModalOpen(false);
    },
    onError: (err: any) => {
      const message = err?.message || "Gagal menyimpan rekening";
      setAccError(message);
      if (/nama rekening|account name/i.test(message)) {
        document.getElementById("account-name")?.focus();
      } else if (/kantong default|default pocket/i.test(message)) {
        document.getElementById("account-default-pocket")?.focus();
      } else if (/saldo|initial_balance/i.test(message)) {
        document.getElementById("account-initial-balance")?.focus();
      }
    },
  });

  // Archive Account Mutation
  const archiveMutation = useMutation({
    mutationFn: async (accId: string) => {
      return api.del(`/accounts/${accId}`);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setAccountActionError("");
    },
    onError: (error: Error) => setAccountActionError(error.message || "Rekening gagal diarsipkan. Coba lagi."),
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

  const accountNameError = /nama rekening|account name/i.test(accError) ? accError : undefined;
  const accountBalanceError = /saldo|initial_balance/i.test(accError) ? accError : undefined;
  const defaultPocketError = /kantong default|default pocket/i.test(accError) ? accError : undefined;
  const pocketNameError = /nama kantong|pocket name/i.test(pocketError) ? pocketError : undefined;
  const pocketBalanceError = /saldo|initial_balance/i.test(pocketError) ? pocketError : undefined;

  const orderedChildren = (account: AccountItem) => {
    const children = account.children ?? [];
    const order = orderedPocketsByParent[account.id];
    if (!order) return children;
    return [...children].sort((left, right) => order.indexOf(left.id) - order.indexOf(right.id));
  };

  const submitOrder = (ids: string[], parentId?: string) => {
    setOrderError("");
    setOrderStatus("");
    if (parentId) {
      setOrderedPocketsByParent((current) => ({ ...current, [parentId]: ids }));
    } else {
      setOrderedAccountIds(ids);
    }
    reorderMutation.mutate(ids);
  };

  const moveId = (ids: string[], id: string, destination: number, parentId?: string) => {
    const source = ids.indexOf(id);
    if (source < 0 || destination < 0 || destination >= ids.length || source === destination) return;
    const next = [...ids];
    next.splice(source, 1);
    next.splice(destination, 0, id);
    submitOrder(next, parentId);
  };

  const openTrade = (account: AccountItem, action: "buy" | "sell") => {
    setTradePocket(account);
    setTradeAction(action);
    setTradeModalOpen(true);
  };

  const openReconciliation = (account: AccountItem) => {
    setReconcileAccount(account);
    setActualBalStr(formatNumberWithDots(account.balance));
    setReconcileNotes("");
    setReconcileError("");
  };

  const actionClass = "inline-flex min-h-11 items-center justify-center rounded-xl px-4 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:cursor-not-allowed disabled:opacity-50";

  const renderAccountGroup = (account: AccountItem, viewport: "desktop" | "mobile") => {
    const children = orderedChildren(account);
    const hasChildren = children.length > 0;
    const expanded = expandedAccounts[account.id] !== false;
    const topIndex = topAccounts.findIndex((item) => item.id === account.id);
    const isPosition = !hasChildren && Boolean(
      account.instrument_type || account.instrument_symbol || account.units !== null && account.units !== undefined ||
      account.avg_buy_price !== null && account.avg_buy_price !== undefined ||
      account.last_price !== null && account.last_price !== undefined
    );
    const kind = account.type === "wallet" ? "Dompet digital" : account.type === "cash" ? "Tunai" : account.type === "investment" ? "Investasi" : "Bank";

    return (
      <section
        key={account.id}
        aria-label={`Rekening ${account.name}`}
        onDragOver={(event) => {
          event.preventDefault();
          if (draggedAccountId && draggedAccountId !== account.id) setDragOverAccountId(account.id);
        }}
        onDragLeave={() => setDragOverAccountId(null)}
        onDrop={(event) => {
          event.preventDefault();
          if (draggedAccountId && draggedAccountId !== account.id) {
            moveId(topAccounts.map((item) => item.id), draggedAccountId, topIndex);
          }
          setDraggedAccountId(null);
          setDragOverAccountId(null);
        }}
        className={cn(
          "flex h-[28rem] min-h-0 flex-col scroll-mt-20 rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xs",
          dragOverAccountId === account.id && "ring-2 ring-[var(--primary)]",
          draggedAccountId === account.id && "opacity-50"
        )}
      >
        <div className="flex shrink-0 flex-col gap-3 p-4 sm:p-5">
          <div className="flex min-w-0 items-start justify-between gap-2">
          <div className="flex min-w-0 items-start gap-2.5">
            <span
              draggable
              aria-hidden="true"
              onDragStart={(event) => {
                setDraggedAccountId(account.id);
                event.dataTransfer.effectAllowed = "move";
                event.dataTransfer.setData("text/plain", account.id);
              }}
              onDragEnd={() => {
                setDraggedAccountId(null);
                setDragOverAccountId(null);
              }}
              className="hidden cursor-grab select-none pt-2 text-lg text-[var(--muted)] lg:inline-flex"
              title="Geser untuk mengubah urutan rekening"
            >
              ⋮⋮
            </span>
            <div className="min-w-0 space-y-2">
              <h2 className="text-base font-bold leading-snug">
                <span
                  style={{ backgroundColor: account.color || "#3b82f6", color: getContrastTextColor(account.color || "#3b82f6") }}
                  className="inline-block max-w-full break-words rounded-lg px-3 py-1.5"
                >
                  {account.name}
                </span>
              </h2>
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-[var(--muted)]">
                <span>{kind}</span>
                {hasChildren && <span className="inline-flex items-center gap-0.5">{children.length} kantong <button type="button" onClick={() => toggleExpand(account.id)} aria-label={`${expanded ? "Tutup" : "Buka"} kantong ${account.name}`} aria-expanded={expanded} aria-controls={expanded ? `pockets-${viewport}-${account.id}` : undefined} title={expanded ? "Tutup kantong" : "Buka kantong"} className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"><Icon name="chevron-down" className={cn("h-4 w-4 transition-transform", !expanded && "-rotate-90")} /></button></span>}
                {account.instrument_symbol && <span>{account.instrument_symbol}</span>}
                {account.instrument_type && <span>{INSTRUMENT_OPTIONS.find((option) => option.value === account.instrument_type)?.label}</span>}
                {account.default_funding_account_name && <span>RDN: {account.default_funding_account_name}</span>}
              </div>
            </div>
          </div>
          <div className="flex max-w-[55%] shrink-0 flex-wrap justify-end gap-1.5">
            {isPosition ? (
              <>
                <button type="button" onClick={() => openTrade(account, "buy")} className={cn(actionClass, "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300")}>Beli</button>
                <button type="button" onClick={() => openTrade(account, "sell")} className={cn(actionClass, "bg-rose-500/15 text-rose-700 dark:text-rose-300")}>Jual</button>
              </>
            ) : (
              <button
                type="button"
                onClick={() => handleOpenNewPocket(account)}
                aria-label={`Tambah kantong ${account.name}`}
                title="Tambah kantong"
                className={cn(actionClass, "min-w-11 bg-[var(--text)] px-2 text-[var(--surface)]")}
              >
                <Icon name="plus" className="h-4 w-4" />
              </button>
            )}
            <AccountOptions name={account.name} iconOnly>
              {canTransferFrom(account.id) && (
                <button type="button" onClick={() => handleOpenTransfer(account.id)}>
                  Pindah Saldo
                </button>
              )}
              {isPosition && <button type="button" onClick={() => handleOpenValuation(account)}>Update Nilai</button>}
              {!hasChildren && <button type="button" onClick={() => openReconciliation(account)}>Sesuaikan Saldo</button>}
              <button type="button" onClick={() => handleOpenEditAccount(account)}>Ubah rekening</button>
              <button type="button" onClick={() => moveId(topAccounts.map((item) => item.id), account.id, topIndex - 1)} disabled={topIndex === 0 || reorderMutation.isPending}>Naikkan urutan</button>
              <button type="button" onClick={() => moveId(topAccounts.map((item) => item.id), account.id, topIndex + 1)} disabled={topIndex === topAccounts.length - 1 || reorderMutation.isPending}>Turunkan urutan</button>
              <ConfirmActionButton label={`Arsipkan rekening ${account.name}`} confirmation={`Arsipkan rekening "${account.name}"${hasChildren ? " beserta seluruh kantong di dalamnya" : ""}?`} onConfirm={() => archiveMutation.mutate(account.id)} className="text-rose-600 dark:text-rose-400">Arsipkan rekening</ConfirmActionButton>
            </AccountOptions>
          </div>
          </div>
          <div className="min-w-0 pl-0 lg:pl-6">
            <div className="break-words text-2xl font-extrabold tabular-nums tracking-tight text-[var(--text)] sm:text-3xl">
              {bal(account.balance)}
            </div>
            {account.capital_gain !== null && account.capital_gain !== undefined && (
              <p className={cn("text-xs font-semibold tabular-nums", account.capital_gain >= 0 ? "text-emerald-600 dark:text-emerald-400" : "text-rose-600 dark:text-rose-400")}>
                Capital Gain: {account.capital_gain >= 0 ? "+" : ""}{bal(account.capital_gain)} ({account.capital_gain_pct ?? 0}%)
              </p>
            )}
            {!hasChildren && isPosition && (account.last_price || account.avg_buy_price) && (
              <p className="text-xs text-[var(--muted)] tabular-nums">
                {account.last_price ? `Pasar ${fmtMoney(account.last_price)}` : ""}
                {account.last_price && account.avg_buy_price ? " · " : ""}
                {account.avg_buy_price ? `Modal beli ${fmtMoney(account.avg_buy_price)}` : ""}
              </p>
            )}
          </div>
        </div>

        {hasChildren && expanded && (
          <div
            id={`pockets-${viewport}-${account.id}`}
            role="region"
            aria-label={`${children.length} kantong ${account.name}`}
            tabIndex={0}
            className="min-h-0 flex-1 overflow-y-auto overscroll-contain border-t border-[var(--border)] focus-visible:outline-2 focus-visible:outline-offset-[-2px] focus-visible:outline-[var(--primary)]"
          >
            {children.map((pocket, index) => {
              const position = pocket.type === "investment" || Boolean(pocket.instrument_type);
              return (
                <div
                  key={pocket.id}
                  role="group"
                  aria-label={`Kantong ${pocket.name}`}
                  onDragOver={(event) => {
                    if (!draggedPocketId) return;
                    event.preventDefault();
                    event.stopPropagation();
                    if (draggedPocketId !== pocket.id) setDragOverPocketId(pocket.id);
                  }}
                  onDragLeave={(event) => {
                    if (!draggedPocketId) return;
                    event.stopPropagation();
                    setDragOverPocketId(null);
                  }}
                  onDrop={(event) => {
                    if (!draggedPocketId) return;
                    event.preventDefault();
                    event.stopPropagation();
                    if (draggedPocketId !== pocket.id) {
                      moveId(children.map((item) => item.id), draggedPocketId, index, account.id);
                    }
                    setDraggedPocketId(null);
                    setDragOverPocketId(null);
                  }}
                  className={cn("grid gap-2 border-b border-[var(--border)] px-4 py-3 last:border-b-0 sm:px-5", dragOverPocketId === pocket.id && "bg-[var(--surface-raised)] ring-2 ring-inset ring-[var(--primary)]", draggedPocketId === pocket.id && "opacity-50")}
                >
                  <div className="flex min-w-0 items-start justify-between gap-2.5">
                  <div className="flex min-w-0 items-start gap-2.5">
                    <span draggable aria-hidden="true" onDragStart={(event) => { event.stopPropagation(); setDraggedPocketId(pocket.id); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", pocket.id); }} onDragEnd={(event) => { event.stopPropagation(); setDraggedPocketId(null); setDragOverPocketId(null); }} className="hidden cursor-grab select-none pt-1 text-base text-[var(--muted)] lg:inline-flex" title="Geser untuk mengubah urutan kantong">⋮⋮</span>
                    <div className="min-w-0 space-y-1">
                      <h3 className="break-words text-sm font-semibold text-[var(--text)]">{pocket.name}</h3>
                      <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-[var(--muted)]">
                        {pocket.instrument_symbol && <span>{pocket.instrument_symbol}</span>}
                        {pocket.instrument_type && <span>{INSTRUMENT_OPTIONS.find((option) => option.value === pocket.instrument_type)?.label}</span>}
                        {pocket.units ? <span className="tabular-nums">{pocket.units.toLocaleString("id-ID")} {getInstrumentConfig(pocket.instrument_type).unitUnit}{pocket.instrument_type === "stock" ? ` (${(pocket.units / 100).toLocaleString("id-ID")} lot)` : ""}</span> : null}
                      </div>
                    </div>
                  </div>
                    <div className="flex shrink-0 items-center gap-1">
                      {position ? <>
                        <button type="button" onClick={() => openTrade(pocket, "buy")} className={cn(actionClass, "bg-emerald-500/15 px-2.5 text-emerald-700 dark:text-emerald-300")}>Beli</button>
                        <button type="button" onClick={() => openTrade(pocket, "sell")} className={cn(actionClass, "bg-rose-500/15 px-2.5 text-rose-700 dark:text-rose-300")}>Jual</button>
                      </> : <button type="button" onClick={() => handleOpenTransfer(pocket.id)} disabled={!canTransferFrom(pocket.id)} aria-label={`Pindah saldo ${pocket.name}`} title="Pindah saldo" className={cn(actionClass, "min-w-11 bg-[var(--surface-raised)] px-2 text-[var(--text)]")}><Icon name="repeat" className="h-4 w-4" /></button>}
                      <AccountOptions name={pocket.name} iconOnly>
                        {position && <button type="button" onClick={() => handleOpenValuation(pocket)}>Update Nilai</button>}
                        <button type="button" onClick={() => openReconciliation(pocket)}>Sesuaikan Saldo</button>
                        <button type="button" onClick={() => handleOpenEditAccount(pocket)}>Ubah kantong</button>
                        <button type="button" onClick={() => moveId(children.map((item) => item.id), pocket.id, index - 1, account.id)} disabled={index === 0 || reorderMutation.isPending}>Naikkan urutan</button>
                        <button type="button" onClick={() => moveId(children.map((item) => item.id), pocket.id, index + 1, account.id)} disabled={index === children.length - 1 || reorderMutation.isPending}>Turunkan urutan</button>
                        <ConfirmActionButton label={`Arsipkan kantong ${pocket.name}`} confirmation={`Arsipkan kantong "${pocket.name}"?`} onConfirm={() => archiveMutation.mutate(pocket.id)} className="text-rose-600 dark:text-rose-400">Arsipkan kantong</ConfirmActionButton>
                      </AccountOptions>
                    </div>
                  </div>
                  <div className="min-w-0 pl-0 lg:pl-7">
                    <div className="break-words text-base font-bold tabular-nums text-[var(--text)]">{bal(pocket.balance)}</div>
                    {pocket.capital_gain !== null && pocket.capital_gain !== undefined && <div className={cn("text-xs font-medium tabular-nums", pocket.capital_gain >= 0 ? "text-emerald-600 dark:text-emerald-400" : "text-rose-600 dark:text-rose-400")}>Capital Gain {pocket.capital_gain >= 0 ? "+" : ""}{bal(pocket.capital_gain)} ({pocket.capital_gain_pct ?? 0}%)</div>}
                    {(pocket.last_price || pocket.avg_buy_price) && <div className="mt-1 flex flex-wrap gap-x-3 text-xs tabular-nums text-[var(--muted)]">{pocket.last_price ? <span>Pasar {fmtMoney(pocket.last_price)}</span> : null}{pocket.avg_buy_price ? <span>Modal beli {fmtMoney(pocket.avg_buy_price)}</span> : null}</div>}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>
    );
  };

  return (
    <div className="space-y-6">
      {/* 1. Header & Actions */}
      <div className="flex flex-col justify-between gap-4 border-b border-[var(--border)] pb-2 lg:flex-row lg:items-center">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
              Rekening & Saldo
            </span>
            <span className="h-1 w-1 rounded-full bg-[var(--muted)]" />
            <span className="text-xs text-[var(--text-secondary)]">
              {topAccounts.length} Rekening Utama ({totalPocketCount} Kantong)
            </span>
          </div>
          <h1 className="text-2xl font-extrabold tracking-tight text-[var(--text)] mt-1">
            Rekening & Total Saldo
          </h1>
        </div>

        {/* Mobile Action Ribbon (< lg) */}
        <div className="lg:hidden flex items-center justify-between gap-2 w-full pt-1">
          <div className="flex items-center gap-1.5 flex-1">
            <button
              type="button"
              onClick={handleOpenNewAccount}
              className="flex min-h-11 flex-1 items-center justify-center gap-1.5 rounded-2xl border border-white/10 bg-[#1E201E] px-3 text-xs font-bold text-white shadow-xs focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
            >
              <span className="text-sm leading-none text-[var(--accent-lime,#66CC55)] font-black">+</span>
              <span>Rekening</span>
            </button>
            <button
              type="button"
              onClick={() => handleOpenTransfer()}
              disabled={liquidAccounts.length < 2}
              className="flex min-h-11 flex-1 items-center justify-center gap-1.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-xs font-bold text-[var(--text)] shadow-xs focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-40"
            >
              <Icon name="repeat" className="h-3.5 w-3.5 text-sky-500 stroke-[2.5]" />
              <span>Pindah</span>
            </button>
          </div>

          <div className="flex items-center gap-1 shrink-0">
            <button
              type="button"
              onClick={() => syncPricesMutation.mutate()}
              disabled={syncPricesMutation.isPending}
              aria-label={syncPricesMutation.isPending ? "Menyinkronkan harga pasar" : "Sinkronkan harga pasar"}
              className="flex min-h-11 min-w-11 items-center justify-center rounded-2xl border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
              title="Sync Harga"
            >
              <Icon name="refresh-cw" className={cn("h-4 w-4", syncPricesMutation.isPending && "animate-spin")} />
            </button>
            <button
              type="button"
              onClick={() => setPayrollModalOpen(true)}
              aria-label="Buka alokasi gaji"
              className="flex min-h-11 min-w-11 items-center justify-center rounded-2xl border border-[var(--border)] bg-[var(--surface)] text-emerald-600 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] dark:text-emerald-400"
              title="Alokasi Gaji"
            >
              <Icon name="allocation" className="h-4 w-4 stroke-[2.5]" />
            </button>
          </div>
        </div>

        {/* Desktop Action Buttons (>= lg) */}
        <div className="hidden lg:flex items-center gap-2.5 self-start sm:self-auto flex-wrap">
          {syncMessage && (
            <span className="text-[11px] font-medium px-2.5 py-1 rounded-xl bg-blue-500/10 text-blue-400 border border-blue-500/20 animate-fade-in">
              {syncMessage}
            </span>
          )}

          <button
            type="button"
            onClick={() => syncPricesMutation.mutate()}
            disabled={syncPricesMutation.isPending}
            className="flex min-h-11 items-center gap-1.5 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3.5 text-xs font-bold text-[var(--text)] shadow-2xs hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50"
            title="Sinkronisasi harga pasar saham/investasi terkini"
          >
            <Icon
              name="refresh-cw"
              className={cn("h-3.5 w-3.5 text-[var(--muted)] stroke-[2.5]", syncPricesMutation.isPending && "animate-spin")}
            />
            <span>{syncPricesMutation.isPending ? "Sinkronisasi…" : "Sync Harga"}</span>
          </button>

          <button
            type="button"
            onClick={() => setPayrollModalOpen(true)}
            className="flex min-h-11 items-center gap-1.5 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3.5 text-xs font-bold text-[var(--text)] shadow-2xs hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
            title="Buka Alokasi Gaji Bulanan"
          >
            <Icon name="allocation" className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400 stroke-[2.5]" />
            <span>Alokasi Gaji</span>
          </button>

          <button
            type="button"
            onClick={() => setRecurringModalOpen(true)}
            className="flex min-h-11 items-center gap-1.5 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3.5 text-xs font-bold text-[var(--text)] shadow-2xs hover:bg-[var(--surface-raised)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
            title="Kelola Transaksi Rutin & Otomatis"
          >
            <Icon name="repeat" className="h-3.5 w-3.5 text-[var(--muted)] stroke-[2.5]" />
            <span>Rutin</span>
          </button>

          <button
            type="button"
            onClick={() => handleOpenTransfer()}
            disabled={liquidAccounts.length < 2}
            className="flex min-h-11 items-center gap-1.5 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 text-xs font-bold text-amber-700 shadow-2xs hover:bg-amber-500/20 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-40 dark:text-amber-400"
          >
            <Icon name="move" className="h-3.5 w-3.5 text-amber-600 dark:text-amber-400 stroke-[2.5]" />
            <span>Pindah Saldo</span>
          </button>

          <button
            type="button"
            onClick={handleOpenNewAccount}
            className="flex min-h-11 items-center gap-1.5 rounded-2xl bg-[#1E201E] px-4 text-xs font-black text-white shadow-xs hover:bg-[#343834] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
          >
            <span className="font-black text-sm leading-none">+</span>
            <span>Rekening Baru</span>
          </button>
        </div>
      </div>

      {/* ===================== DESKTOP COCKPIT (>= lg) ===================== */}
      <div className="hidden lg:block space-y-6">
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
              className="h-full bg-blue-500 transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
            />
          )}
          {walletPct > 0 && (
            <div
              style={{ width: `${walletPct}%` }}
              className="h-full bg-emerald-500 transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
            />
          )}
          {cashPct > 0 && (
            <div
              style={{ width: `${cashPct}%` }}
              className="h-full bg-amber-500 transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
            />
          )}
          {investPct > 0 && (
            <div
              style={{ width: `${investPct}%` }}
              className="h-full bg-indigo-500 transition-[width] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none"
            />
          )}
        </div>
      </div>

      <div className="grid gap-3 xl:grid-cols-2 2xl:grid-cols-3 xl:items-stretch">
        {(orderError || accountActionError) && <p role="alert" className="col-span-full rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-700 dark:text-rose-300">{orderError || accountActionError}</p>}
        <p role="status" className="sr-only">{orderStatus}</p>
        {topAccounts.length === 0 ? (
          <div className="col-span-full rounded-2xl border border-dashed border-[var(--border)] p-6 text-sm text-[var(--muted)]">
            Belum ada rekening. Tambahkan rekening untuk mulai mencatat saldo.
          </div>
        ) : topAccounts.map((account) => renderAccountGroup(account, "desktop"))}
      </div>
      </div>
      {/* ===================== MOBILE-NATIVE ACCOUNTS (< lg) ===================== */}
      <div className="lg:hidden space-y-4">
        {/* 1. Mobile Net Worth & Runway Hero Card */}
        <div className="card-squircle p-4 sm:p-5 border border-[var(--border)] bg-[var(--surface)] space-y-3 shadow-xs">
          <div className="flex items-center justify-between gap-2">
            <span className="text-[10px] font-bold uppercase tracking-wider text-[var(--muted)]">
              Total Kekayaan Bersih
            </span>
            {runway && (
              <span
                className={cn(
                  "text-[10px] px-2.5 py-0.5 rounded-full font-bold border inline-flex items-center gap-1.5 shadow-2xs tabular",
                  totalAssets <= 0 || runway.status === "zero"
                    ? "bg-zinc-500/10 text-zinc-500 border-zinc-500/20"
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
                Ketahanan:{" "}
                {totalAssets <= 0 || runway.status === "zero"
                  ? "0 Hari"
                  : runway.runway_months >= 99
                  ? "> 12x Biaya Hidup"
                  : `${runway.runway_months}x (${runway.runway_days > 365 ? "> 1 Thn" : `${runway.runway_days} Hari`})`}
              </span>
            )}
          </div>

          <div>
            <div className="text-2xl sm:text-3xl font-black tracking-tight tabular text-[var(--text)] select-all">
              {bal(animNetWorth)}
            </div>
            <p className="text-[11px] text-[var(--muted)] mt-0.5">
              {totalAssets >= totalLiabilities
                ? "Saldo kas mencukupi seluruh kewajiban"
                : "Total kewajiban melebihi saldo kas"}
            </p>
          </div>

          {/* Asset vs Liability Strip */}
          <div className="flex items-center justify-between text-[11px] pt-2 border-t border-[var(--border)]/60 text-[var(--muted)] tabular">
            <div>
              <span className="text-[10px] text-[var(--muted)] block">Kas Tersedia</span>
              <span className="font-bold text-emerald-600 dark:text-emerald-400">{bal(animTotalAssets)}</span>
            </div>
            <span className="text-[var(--border)]">•</span>
            <div>
              <span className="text-[10px] text-[var(--muted)] block">Cicilan & Utang</span>
              <span className="font-bold text-rose-600 dark:text-rose-400">{bal(animTotalLiabilities)}</span>
            </div>
            <span className="text-[var(--border)]">•</span>
            <div className="text-right">
              <span className="text-[10px] text-[var(--muted)] block">Biaya Pokok</span>
              <span className="font-bold text-[var(--text)]">
                {bal(runway?.monthly_primary_expense ?? (runway?.daily_burn_rate ?? 0) * 30)}/bln
              </span>
            </div>
          </div>

          {/* Segmented Distribution Line */}
          <div className="space-y-1 pt-1">
            <div className="w-full h-1.5 rounded-full bg-[var(--border)]/60 overflow-hidden flex shadow-2xs">
              {bankPct > 0 && <div style={{ width: `${bankPct}%` }} className="h-full bg-blue-500" />}
              {walletPct > 0 && <div style={{ width: `${walletPct}%` }} className="h-full bg-emerald-500" />}
              {cashPct > 0 && <div style={{ width: `${cashPct}%` }} className="h-full bg-amber-500" />}
              {investPct > 0 && <div style={{ width: `${investPct}%` }} className="h-full bg-indigo-500" />}
            </div>
            <div className="flex items-center justify-between text-[9px] text-[var(--muted)] tabular font-medium">
              <span>Bank {bankPct}%</span>
              <span>Dompet {walletPct}%</span>
              <span>Kas {cashPct}%</span>
              <span>Investasi {investPct}%</span>
            </div>
          </div>
        </div>

        {/* 2. Account Type Segmented Filter Chips */}
        <div className="flex items-center gap-1.5 overflow-x-auto no-scrollbar py-0.5">
          {[
            { id: "all", label: "Semua", count: topAccounts.length },
            {
              id: "bank_wallet",
              label: "Bank & Dompet",
              count: topAccounts.filter((a) => a.type === "bank" || a.type === "wallet").length,
            },
            { id: "cash", label: "Kas Tunai", count: topAccounts.filter((a) => a.type === "cash").length },
            {
              id: "investment",
              label: "Investasi",
              count: topAccounts.filter((a) => a.type === "investment" || Boolean(a.instrument_type)).length,
            },
          ].map((chip) => (
            <button
              key={chip.id}
              type="button"
              onClick={() => setMobileAccountFilter(chip.id as any)}
              className={cn(
                "flex min-h-11 cursor-pointer items-center gap-1.5 whitespace-nowrap rounded-xl px-3 text-xs font-bold transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]",
                mobileAccountFilter === chip.id
                  ? "bg-[#1E201E] text-white shadow-xs border border-black dark:border-white/20"
                  : "bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)] border border-[var(--border)]"
              )}
            >
              <span>{chip.label}</span>
              <span
                className={cn(
                  "text-[10px] px-1.5 py-0.2 rounded-full tabular font-semibold",
                  mobileAccountFilter === chip.id
                    ? "bg-white/20 text-white"
                    : "bg-[var(--border)] text-[var(--muted)]"
                )}
              >
                {chip.count}
              </span>
            </button>
          ))}
        </div>

        {(orderError || accountActionError) && <p role="alert" className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-700 dark:text-rose-300">{orderError || accountActionError}</p>}
        <p role="status" className="sr-only">{orderStatus}</p>
        {filteredMobileAccounts.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-[var(--border)] p-6 text-sm text-[var(--muted)]">
            {topAccounts.length === 0 ? "Belum ada rekening. Tambahkan rekening untuk mulai mencatat saldo." : "Tidak ada rekening di kategori ini."}
          </div>
        ) : (
          <div className="grid gap-3">{filteredMobileAccounts.map((account) => renderAccountGroup(account, "mobile"))}</div>
        )}
      </div>
      <InternalMovementModal
        open={transferModalOpen}
        onClose={() => setTransferModalOpen(false)}
        defaultSourceAccountId={fromAccountId}
        defaultTargetAccountId={toAccountId}
      />

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
        <form
          className="space-y-4 pt-2 text-xs [&_input]:min-h-11 [&_select]:min-h-11"
          onSubmit={(event) => {
            event.preventDefault();
            if (!saveAccountMutation.isPending) saveAccountMutation.mutate();
          }}
        >
          {accError && (
            <div role="alert" className="rounded-xl border border-rose-500/20 bg-rose-500/10 p-3 text-xs text-rose-600">
              {accError}
            </div>
          )}

          <div>
            <label htmlFor="account-name" className="font-medium text-[var(--muted)] block mb-1">
              {editingAccount?.parent_id ? "Nama Kantong" : "Nama Rekening"}
            </label>
            <input
              id="account-name"
              name="name"
              type="text"
              required
              value={accName}
              onChange={(e) => setAccName(e.target.value)}
              aria-invalid={Boolean(accountNameError) || undefined}
              aria-describedby={accountNameError ? "account-name-error" : undefined}
              placeholder="Contoh: BCA Utama, Bibit, Binance, GoPay"
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
            />
            {accountNameError && <p id="account-name-error" className="mt-1 text-xs text-rose-600">{accountNameError}</p>}
          </div>

          {!editingAccount?.parent_id && (
            <div className="space-y-2">
              <p className="font-medium text-[var(--muted)] block">Warna tag rekening</p>
              <div className="flex items-center gap-2 flex-wrap">
                {ACCOUNT_TAG_COLORS.map((col) => {
                  const isSelected = accColor.toLowerCase() === col.hex.toLowerCase();
                  return (
                    <button
                      key={col.hex}
                      type="button"
                      onClick={() => setAccColor(col.hex)}
                      aria-label={`Warna ${col.name}`}
                      aria-pressed={isSelected}
                      className={cn(
                        "min-h-11 min-w-11 rounded-full transition-transform flex items-center justify-center text-white focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--text)]",
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
                <label htmlFor="account-type" className="font-medium text-[var(--muted)] block mb-1">Jenis rekening</label>
                <select
                  id="account-type"
                  name="type"
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
                  <label htmlFor="account-default-pocket" className="font-medium text-[var(--muted)] block mb-1">
                    Kantong Default (Penerima Notifikasi Otomatis)
                  </label>
                  <select
                    id="account-default-pocket"
                    name="default_pocket_id"
                    value={accDefaultPocketId}
                    onChange={(e) => setAccDefaultPocketId(e.target.value)}
                    aria-invalid={Boolean(defaultPocketError) || undefined}
                    aria-describedby={defaultPocketError ? "account-default-pocket-error" : undefined}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tanpa Kantong Default (Gunakan Rekening Induk)</option>
                    {editingAccount.children.filter((child) => !child.is_archived).map((child) => (
                      <option key={child.id} value={child.id}>
                        {child.name} ({bal(child.balance)})
                      </option>
                    ))}
                  </select>
                  {defaultPocketError && <p id="account-default-pocket-error" className="mt-1 text-xs text-rose-600">{defaultPocketError}</p>}
                  <p className="text-[10px] text-[var(--muted)] mt-1">
                    Transaksi mobile tanpa rincian kantong akan otomatis dialokasikan ke kantong default ini.
                  </p>
                </div>
              )}

              {accType === "investment" && (
                <div className="space-y-3">
                  <div>
                    <p className="font-medium text-[var(--muted)] block mb-1.5">Model akun investasi</p>
                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => setAccIsMultiInstrument(true)}
                        aria-pressed={accIsMultiInstrument}
                        className={cn(
                          "min-h-11 p-2.5 rounded-xl border text-left transition-[border-color,background-color,color]",
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
                        aria-pressed={!accIsMultiInstrument}
                        className={cn(
                          "min-h-11 p-2.5 rounded-xl border text-left transition-[border-color,background-color,color]",
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
                    <label htmlFor="account-default-funding" className="font-medium text-[var(--muted)] flex items-center justify-between mb-1.5">
                      <span className="flex items-center gap-1.5 font-bold text-xs text-[var(--text)]">
                        <Icon name="credit-card" className="h-3.5 w-3.5 text-blue-400" />
                        <span>Sumber Dana / RDN Default</span>
                      </span>
                      <span className="text-[10px] text-[var(--muted)]">Opsional</span>
                    </label>
                    <select
                      id="account-default-funding"
                      name="default_funding_account_id"
                      value={accDefaultFundingId}
                      onChange={(e) => setAccDefaultFundingId(e.target.value)}
                      className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                    >
                      <option value="">-- Tanpa RDN / Sumber Default --</option>
                      <AccountSelectOptions
                        accounts={activeAccounts}
                        liquidOnly
                        allowParentSelection
                        excludeAccountId={editingAccount?.id}
                        formatBalance={fmtMoney}
                      />
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
              <p className="font-medium text-[var(--muted)] block mb-1.5">Tipe kantong</p>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <button
                  type="button"
                  onClick={() => setAccIsMultiInstrument(true)}
                  aria-pressed={accIsMultiInstrument}
                  className={cn(
                    "min-h-11 px-3 rounded-xl border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
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
                  aria-pressed={!accIsMultiInstrument}
                  className={cn(
                    "min-h-11 px-3 rounded-xl border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
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
                <p className="font-medium text-[var(--muted)] block mb-1.5">Tipe instrumen</p>
                <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3">
                  {INSTRUMENT_OPTIONS.map((opt) => (
                    <button
                      key={opt.value}
                      type="button"
                      onClick={() => setAccInstrumentType(opt.value)}
                      aria-pressed={accInstrumentType === opt.value}
                      className={cn(
                        "min-h-11 px-2 rounded-xl border text-xs font-semibold transition-colors",
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

              {accInstrumentType !== "deposit" && <div className="relative">
                <label htmlFor="account-ticker" className="font-medium text-[var(--muted)] flex items-center justify-between mb-1">
                  <span>Simbol Ticker / Kode (Yahoo Finance)</span>
                  <span className="text-[10px] text-[var(--muted)]">Opsional</span>
                </label>
                <div className="relative">
                  <input
                    id="account-ticker"
                    name="instrument_symbol"
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
                      <button
                        key={item.symbol}
                        type="button"
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
                        className="flex min-h-11 w-full items-center justify-between border-b border-[var(--border)]/50 p-2.5 text-left text-xs hover:bg-[var(--surface-raised)] last:border-0"
                      >
                        <div>
                          <span className="font-bold text-violet-400">{item.symbol}</span>
                          <span className="text-[var(--text)] ml-2">{item.name}</span>
                        </div>
                        <span className="text-[10px] uppercase text-[var(--muted)] px-1.5 py-0.5 rounded bg-[var(--surface-raised)] font-medium">
                          {item.exchange || item.type}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </div>}

              {accInstrumentType !== "deposit" && (() => {
                const aCfg = getInstrumentConfig(accInstrumentType);
                return (
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <div>
                      <label htmlFor="account-units" className="font-medium text-[var(--muted)] block mb-1">
                        {aCfg.unitsLabel}
                      </label>
                      <input
                        id="account-units"
                        name="units"
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
                      <label htmlFor="account-average-price" className="font-medium text-[var(--muted)] block mb-1">
                        {aCfg.priceLabel}
                      </label>
                      <input
                        id="account-average-price"
                        name="avg_buy_price"
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

          {!editingAccount && !(accType === "investment" && accIsMultiInstrument) && (
            <MonetaryInput
              id="account-initial-balance"
              name="initial_balance"
              label={accType === "investment" ? getInstrumentConfig(accInstrumentType).totalLabel : "Saldo Awal (IDR)"}
              value={accInitBal}
              onChange={setAccInitBal}
              error={accountBalanceError}
              description={accType === "investment" && accInstrumentType === "deposit"
                ? "Pokok deposito saat pembukaan. Imbal hasil tahunan belum dihitung otomatis."
                : accType === "investment"
                ? "Nilai pembukaan posisi; unit dan harga beli dipakai sebagai dasar modal jika keduanya diisi. Beli/Jual berikutnya dicatat lewat transaksi investasi."
                : "Saldo pembukaan rekening, bukan transaksi baru di buku kas."}
            />
          )}

          {!editingAccount && accType === "investment" && accIsMultiInstrument && (
            <p className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3 text-xs leading-relaxed text-[var(--muted)]">
              Platform ini hanya mengelompokkan kantong. Setelah dibuat, tambahkan kantong kas/RDN untuk saldo dana dan kantong instrumen untuk posisi investasi.
            </p>
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
              type="submit"
              disabled={saveAccountMutation.isPending}
              className="min-h-11 flex-1 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
            >
              {saveAccountMutation.isPending
                ? "Menyimpan…"
                : editingAccount
                ? "Simpan Perubahan"
                : "Tambah Rekening"}
            </button>
          </div>
        </form>
      </Modal>

      {/* Pocket Modal */}
      {pocketParentAccount && (
        <Modal
          open={pocketModalOpen}
          onClose={() => setPocketModalOpen(false)}
          title={`Tambah Kantong di ${pocketParentAccount.name}`}
        >
          <form
            className="space-y-4 pt-2 text-xs [&_input]:min-h-11 [&_select]:min-h-11"
            onSubmit={(event) => {
              event.preventDefault();
              if (!savePocketMutation.isPending) savePocketMutation.mutate();
            }}
          >
            {pocketError && (
              <div role="alert" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-600 font-medium">
                {pocketError}
              </div>
            )}

            <div>
              <label htmlFor="pocket-name" className="font-medium text-[var(--muted)] block mb-1">Nama kantong</label>
              <input
                id="pocket-name"
                name="name"
                type="text"
                required
                value={pocketName}
                onChange={(e) => setPocketName(e.target.value)}
                aria-invalid={Boolean(pocketNameError) || undefined}
                aria-describedby={pocketNameError ? "pocket-name-error" : undefined}
                placeholder={
                  pocketParentAccount.type === "investment"
                    ? "Contoh: Bibit RDPU, Saham BBCA, Bareksa Sukuk"
                    : "Contoh: Tabungan Utama, RDPU BCA, Tabungan Emas"
                }
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              />
              {pocketNameError && <p id="pocket-name-error" className="mt-1 text-xs text-rose-600">{pocketNameError}</p>}
            </div>

            <div>
              <p className="font-medium text-[var(--muted)] block mb-1.5">Tipe kantong</p>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <button
                  type="button"
                  onClick={() => setPocketIsInvestment(false)}
                  aria-pressed={!pocketIsInvestment}
                  className={cn(
                    "min-h-11 px-3 rounded-xl border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
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
                  aria-pressed={pocketIsInvestment}
                  className={cn(
                    "min-h-11 px-3 rounded-xl border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
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
                  <p className="font-medium text-[var(--muted)] block mb-1.5">Tipe instrumen</p>
                  <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3">
                    {INSTRUMENT_OPTIONS.map((opt) => (
                      <button
                        key={opt.value}
                        type="button"
                        onClick={() => setPocketInstrumentType(opt.value)}
                        aria-pressed={pocketInstrumentType === opt.value}
                        className={cn(
                          "min-h-11 px-2 rounded-xl border text-xs font-semibold transition-colors",
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

                {pocketInstrumentType !== "deposit" && <div className="relative">
                  <label htmlFor="pocket-ticker" className="font-medium text-[var(--muted)] flex items-center justify-between mb-1">
                    <span>Simbol Ticker / Kode (Yahoo Finance)</span>
                    <span className="text-[10px] text-[var(--muted)]">Opsional</span>
                  </label>
                  <div className="relative">
                    <input
                      id="pocket-ticker"
                      name="instrument_symbol"
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
                        <button
                          key={item.symbol}
                          type="button"
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
                          className="flex min-h-11 w-full items-center justify-between border-b border-[var(--border)]/50 p-2.5 text-left text-xs hover:bg-[var(--surface-raised)] last:border-0"
                        >
                          <div>
                            <span className="font-bold text-violet-400">{item.symbol}</span>
                            <span className="text-[var(--text)] ml-2">{item.name}</span>
                          </div>
                          <span className="text-[10px] uppercase text-[var(--muted)] px-1.5 py-0.5 rounded bg-[var(--surface-raised)] font-medium">
                            {item.exchange || item.type}
                          </span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>}

                {pocketInstrumentType !== "deposit" && (() => {
                  const pCfg = getInstrumentConfig(pocketInstrumentType);
                  return (
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <div>
                        <label htmlFor="pocket-units" className="font-medium text-[var(--muted)] block mb-1">
                          {pCfg.unitsLabel}
                        </label>
                        <input
                          id="pocket-units"
                          name="units"
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
                        <label htmlFor="pocket-average-price" className="font-medium text-[var(--muted)] block mb-1">
                          {pCfg.priceLabel}
                        </label>
                        <input
                          id="pocket-average-price"
                          name="avg_buy_price"
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

            <MonetaryInput
              id="pocket-initial-balance"
              name="initial_balance"
              label={pocketIsInvestment ? getInstrumentConfig(pocketInstrumentType).totalLabel : "Saldo Awal Kantong (IDR)"}
              value={pocketInitBal}
              onChange={setPocketInitBal}
              error={pocketBalanceError}
              description={pocketIsInvestment && pocketInstrumentType === "deposit"
                ? "Pokok deposito saat pembukaan. Imbal hasil tahunan belum dihitung otomatis."
                : pocketIsInvestment
                ? "Nilai pembukaan instrumen; perubahan unit berikutnya dicatat melalui Beli/Jual."
                : "Saldo pembukaan kantong, bukan pemindahan saldo atau transaksi baru."}
            />

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setPocketModalOpen(false)}
                className="flex-1 rounded-xl border border-[var(--border)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              >
                Batal
              </button>
              <button
                type="submit"
                disabled={savePocketMutation.isPending}
                className="min-h-11 flex-1 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
              >
                {savePocketMutation.isPending ? "Menyimpan…" : "Buat Kantong"}
              </button>
            </div>
          </form>
        </Modal>
      )}

      {/* Valuation Modal (Update Nilai Investasi - Mode B / Penyesuaian) */}
      {valuationAccount && (
        <Modal
          open={Boolean(valuationAccount)}
          onClose={() => setValuationAccount(null)}
          title={`Update Nilai: ${valuationAccount.name}`}
        >
          <form
            className="space-y-4 pt-2 text-xs [&_input]:min-h-11 [&_select]:min-h-11"
            onSubmit={(event) => {
              event.preventDefault();
              if (!valuationMutation.isPending) valuationMutation.mutate();
            }}
          >
            {valError && (
              <div role="alert" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-600 font-medium">
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
              <label htmlFor="valuation-current-value" className="font-medium text-[var(--muted)] block mb-1">
                Nilai Pasar Terkini (IDR)
              </label>
              <input
                id="valuation-current-value"
                name="current_balance"
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
              <label htmlFor="valuation-cost-basis" className="font-medium text-[var(--muted)] block mb-1">
                Total Modal Terinvestasi / Harga Beli (IDR)
              </label>
              <input
                id="valuation-cost-basis"
                name="cost_basis"
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
              <label htmlFor="valuation-notes" className="font-medium text-[var(--muted)] block mb-1">
                Catatan (Opsional)
              </label>
              <input
                id="valuation-notes"
                name="notes"
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
                type="submit"
                disabled={valuationMutation.isPending}
                className="btn-primary flex-1"
              >
                {valuationMutation.isPending ? "Menyimpan…" : "Simpan Nilai Baru"}
              </button>
            </div>
          </form>
        </Modal>
      )}
      {reconcileAccount && (
        <Modal
          open={Boolean(reconcileAccount)}
          onClose={() => setReconcileAccount(null)}
          title={`Sesuaikan Saldo: ${reconcileAccount.name}`}
        >
          <form
            className="space-y-4 pt-2 text-xs"
            onSubmit={(event) => {
              event.preventDefault();
              if (!reconcileMutation.isPending) reconcileMutation.mutate();
            }}
          >
            {reconcileError && (
              <div role="alert" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-600 font-medium">
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

            <MonetaryInput
              id="reconcile-actual-balance"
              name="actual_balance"
              label="Saldo Sebenarnya di Bank / Dompet (IDR)"
              value={actualBalStr}
              onChange={(value) => { setActualBalStr(value); setReconcileError(""); }}
              placeholder="Contoh: 10.000.000"
              error={reconcileError}
              className="text-base"
            />

            {/* Calculated variance preview */}
            {actualBalStr.trim() && (() => {
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
              <label htmlFor="reconcile-notes" className="font-medium text-[var(--muted)] block mb-1">
                Catatan Penyesuaian (Opsional)
              </label>
              <input
                id="reconcile-notes"
                name="notes"
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
                type="submit"
                disabled={reconcileMutation.isPending}
                className="btn-primary flex-1"
              >
                {reconcileMutation.isPending ? "Menyesuaikan…" : "Konfirmasi & Sesuaikan Saldo"}
              </button>
            </div>
          </form>
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
