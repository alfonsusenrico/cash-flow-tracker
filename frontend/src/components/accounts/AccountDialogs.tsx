"use client";

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots, formatDecimalInput, parseDecimal, parseUnits } from "@/lib/utils";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { FormField, MonetaryInput } from "@/components/ui/FormField";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { Icon } from "@/components/ui/Icon";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { InvestmentTradeModal } from "@/components/ui/InvestmentTradeModal";
import { InvestmentTopupModal } from "@/components/ui/InvestmentTopupModal";
import { AmountTrackingSwitchModal } from "@/components/ui/AmountTrackingSwitchModal";
import { InvestmentScheduleDraft } from "@/lib/investmentTopups";
import { DEFAULT_ACCOUNT_TAG_COLOR, INVESTMENT_ACCOUNT_TAG_COLOR, defaultAccountTagColor } from "@/lib/accountColours";
import { RoleField, cashRoleOf, fixedRole, type CashRole } from "./RoleField";
import type { AccountItem, AccountsResponse } from "./types";

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
  { name: "Biru", hex: DEFAULT_ACCOUNT_TAG_COLOR },
  { name: "Emerald", hex: "#059669" },
  { name: "Lavender", hex: INVESTMENT_ACCOUNT_TAG_COLOR },
  { name: "Ungu", hex: "#7C3AED" },
  { name: "Amber", hex: "#D97706" },
  { name: "Rose", hex: "#E11D48" },
  { name: "Indigo", hex: "#4F46E5" },
  { name: "Teal", hex: "#0D9488" },
  { name: "Slate", hex: "#475569" },
];

/** The credit-only fields of a save; empty inputs clear them. */
function creditFieldValues(limit: string, paymentDay: string): { credit_limit: number | null; payment_day: number | null } {
  const limitValue = parseNumberFromDots(limit);
  const dayText = paymentDay.trim();
  const day = Number(dayText);
  if (dayText && (!Number.isInteger(day) || day < 1 || day > 31)) {
    throw new Error("Tanggal bayar harus antara 1 dan 31.");
  }
  return { credit_limit: limitValue > 0 ? limitValue : null, payment_day: dayText ? day : null };
}

/**
 * Every Rekening & Kantong form and its write, moved unchanged from the page (design D5): the page and the
 * row drawer only decide which one opens. Request bodies and query invalidations are those of the old page.
 */
export function useAccountDialogs() {
  const qc = useQueryClient();
  const { bal } = useAppCtx();

  // Modals state
  const [accountModalOpen, setAccountModalOpen] = useState(false);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);
  const [editingAccount, setEditingAccount] = useState<AccountItem | null>(null);
  const [reconcileAccount, setReconcileAccount] = useState<AccountItem | null>(null);
  // Investment Trade Modal state
  const [tradeModalOpen, setTradeModalOpen] = useState(false);
  const [topupProduct, setTopupProduct] = useState<AccountItem | null>(null);
  const [amountSwitchProduct, setAmountSwitchProduct] = useState<AccountItem | null>(null);
  const [investmentSchedule, setInvestmentSchedule] = useState<InvestmentScheduleDraft | null>(null);
  const [tradePocket, setTradePocket] = useState<AccountItem | null>(null);
  const [tradeAction, setTradeAction] = useState<"buy" | "sell">("buy");

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

  // Valuation modal state ("Perbarui nilai")
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
    setValCostBasisStr(acc.investment_tracking_mode === "amount" && acc.cost_basis == null ? "" : formatNumberWithDots(totalCost));
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
      setSyncMessage(`Harga diperbarui: ${data.accounts_updated} produk`);
      setTimeout(() => setSyncMessage(null), 4000);
    },
    onError: () => {
      setSyncMessage("Harga belum dapat diperbarui");
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
  const [accType, setAccType] = useState<AccountItem["type"]>("bank");
  const [accInitBal, setAccInitBal] = useState("");
  // Credit-only fields (add-credit-accounts D2); what a credit account owes today is typed, positive, in accInitBal.
  const [accCreditLimit, setAccCreditLimit] = useState("");
  const [accPaymentDay, setAccPaymentDay] = useState("");
  const [accColor, setAccColor] = useState<string>(DEFAULT_ACCOUNT_TAG_COLOR);
  // Until the owner picks a colour, a new account's colour follows the default of its type.
  const [accColorChosen, setAccColorChosen] = useState(false);
  const [accDefaultFundingId, setAccDefaultFundingId] = useState<string>("");
  const [accDefaultPocketId, setAccDefaultPocketId] = useState<string>("");
  const [accParentId, setAccParentId] = useState<string>("");
  // "Kegunaan" of a cash, bank or e-wallet account; credit and products take theirs from the type (D1).
  const [accRole, setAccRole] = useState<CashRole>("harian");
  const [accError, setAccError] = useState("");

  const [accountActionError, setAccountActionError] = useState("");

  // Pocket form
  const [pocketModalOpen, setPocketModalOpen] = useState(false);
  const [pocketParentAccount, setPocketParentAccount] = useState<AccountItem | null>(null);
  const [pocketName, setPocketName] = useState("");
  const [pocketInitBal, setPocketInitBal] = useState("");
  const [pocketRole, setPocketRole] = useState<CashRole>("harian");
  const [pocketError, setPocketError] = useState("");

  // Reconciliation form
  const [actualBalStr, setActualBalStr] = useState("");
  const [reconcileNotes, setReconcileNotes] = useState("");
  const [reconcileError, setReconcileError] = useState("");
  // Set when "Cocokkan saldo" opened the adjustment, so it can record a matched point afterwards (OD145).
  const [afterReconcile, setAfterReconcile] = useState<((actual: number) => void) | null>(null);

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
      afterReconcile?.(parseNumberFromDots(actualBalStr));
      setAfterReconcile(null);
      setReconcileAccount(null);
    },
    onError: (err: any) => {
      setReconcileError(err?.message || "Gagal menyesuaikan saldo");
      document.getElementById("reconcile-actual-balance")?.focus();
    },
  });

  const { data: accountsData } = useQuery<AccountsResponse>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const activeAccounts = useMemo(() => accounts.filter((a) => !a.is_archived), [accounts]);
  const parentCandidates = useMemo(
    () => activeAccounts.filter((a) => !a.parent_id && a.type !== "investment" && a.type !== "credit" && a.id !== editingAccount?.id),
    [activeAccounts, editingAccount?.id],
  );
  // Open Add Account modal
  const handleOpenNewAccount = () => {
    setEditingAccount(null);
    setAccName("");
    setAccType("bank");
    setAccInitBal("");
    setAccCreditLimit("");
    setAccPaymentDay("");
    setAccColor(DEFAULT_ACCOUNT_TAG_COLOR);
    setAccColorChosen(false);
    setAccDefaultFundingId("");
    setAccDefaultPocketId("");
    setAccRole("harian");
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
    if (parent.type === "credit") return;
    setPocketParentAccount(parent);
    setPocketName("");
    setPocketInitBal("");
    setPocketRole("harian");
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
    setAccColor(acc.color || DEFAULT_ACCOUNT_TAG_COLOR);
    setAccDefaultFundingId(acc.default_funding_account_id || "");
    setAccDefaultPocketId(acc.default_pocket_id || "");
    setAccParentId("");
    setAccRole(cashRoleOf(acc.role));
    setAccInitBal(formatNumberWithDots(acc.initial_balance));
    setAccCreditLimit(acc.credit_limit ? formatNumberWithDots(acc.credit_limit) : "");
    setAccPaymentDay(acc.payment_day ? String(acc.payment_day) : "");
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
        role: isInvest ? "investasi" : pocketRole,
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
      } else if (/^kegunaan/i.test(message)) {
        document.getElementById("pocket-role")?.focus();
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
      const isCredit = !isEditingPocket && accType === "credit";
      const creditFields = isCredit ? creditFieldValues(accCreditLimit, accPaymentDay) : null;

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
      let openingBalance = accType === "investment" && accIsMultiInstrument ? 0 : initBal;
      // A credit account starts at minus what it owes today; "|| 0" keeps zero from becoming -0.
      if (isCredit) openingBalance = -initBal || 0;
      let savedType = accType;
      if (isEditingPocket && editingAccount) {
        savedType = editingAccount.type;
        if (isInvestPocket) {
          savedType = "investment";
        } else if (savedType === "investment") {
          savedType = "bank";
        }
      }
      const role = fixedRole(savedType, hasInstrument) ?? accRole;

      if (editingAccount) {
        const moveUnder = !isEditingPocket && !isCredit && accParentId ? { parent_id: accParentId } : {};
        // Limit and payment day belong to credit only; leaving credit clears them so the type can change.
        let creditPatch = {};
        if (creditFields) creditPatch = creditFields;
        else if (editingAccount.type === "credit") creditPatch = { credit_limit: null, payment_day: null };
        return api.patch(`/accounts/${editingAccount.id}`, {
          ...moveUnder,
          ...creditPatch,
          name,
          type: savedType,
          role,
          instrument_type: hasInstrument ? accInstrumentType : null,
          ...(editingAccount.investment_tracking_mode !== "amount" ? { instrument_symbol: tickerSymbol } : {}),
          ...(!(hasInstrument && accInstrumentType === "deposit" && editingAccount.instrument_type === "deposit") && editingAccount.investment_tracking_mode !== "amount" ? {
            units: unitsNum,
            avg_buy_price: avgPriceNum,
          } : {}),
          ...(!isEditingPocket && !accParentId ? {
            color: accColor,
            default_funding_account_id: accType === "investment" ? defaultFunding : null,
            default_pocket_id: accDefaultPocketId ? accDefaultPocketId : null,
          } : {}),
        });
      } else {
        return api.post("/accounts", {
          name,
          type: accType,
          role,
          initial_balance: openingBalance,
          instrument_type: hasInstrument ? accInstrumentType : null,
          instrument_symbol: tickerSymbol,
          units: unitsNum,
          avg_buy_price: avgPriceNum,
          color: accColor,
          default_funding_account_id: defaultFunding,
          ...(creditFields ?? {}),
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
      } else if (/^kegunaan/i.test(message)) {
        document.getElementById("account-role")?.focus();
      } else if (/saldo|initial_balance/i.test(message)) {
        document.getElementById("account-initial-balance")?.focus();
      } else if (/^limit/i.test(message)) {
        document.getElementById("account-credit-limit")?.focus();
      } else if (/tanggal bayar/i.test(message)) {
        document.getElementById("account-payment-day")?.focus();
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

  const accountNameError = /nama rekening|account name/i.test(accError) ? accError : undefined;
  const accountBalanceError = /saldo|initial_balance/i.test(accError) ? accError : undefined;
  const defaultPocketError = /kantong default|default pocket/i.test(accError) ? accError : undefined;
  const creditLimitError = /^limit/i.test(accError) ? accError : undefined;
  const paymentDayError = !creditLimitError && /tanggal bayar/i.test(accError) ? accError : undefined;
  const roleError = /^kegunaan/i.test(accError) ? accError : undefined;
  const pocketNameError = /nama kantong|pocket name/i.test(pocketError) ? pocketError : undefined;
  const pocketRoleError = /^kegunaan/i.test(pocketError) ? pocketError : undefined;
  const pocketBalanceError = /saldo|initial_balance/i.test(pocketError) ? pocketError : undefined;

  const openTrade = (account: AccountItem, action: "buy" | "sell") => {
    setTradePocket(account);
    setTradeAction(action);
    setTradeModalOpen(true);
  };

  const openReconciliation = (account: AccountItem, options?: { actual?: number; onReconciled?: (actual: number) => void }) => {
    setReconcileAccount(account);
    setActualBalStr(formatNumberWithDots(options?.actual ?? account.balance));
    setReconcileNotes("");
    setReconcileError("");
    // A function value in state must be wrapped, or React would call it as an updater.
    setAfterReconcile(() => options?.onReconciled ?? null);
  };

  const dialogs = (
    <>
      {/* Add / Edit Account Modal */}
      <Modal
        open={accountModalOpen}
        onClose={() => setAccountModalOpen(false)}
        title={
          editingAccount
            ? editingAccount.parent_id
              ? `Ubah Kantong: ${editingAccount.name}`
              : `Ubah Rekening: ${editingAccount.name}`
            : "Tambah rekening"
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
            <div role="alert" className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)] p-3 text-xs text-out-fg">
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
              className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
            />
            {accountNameError && <p id="account-name-error" className="mt-1 text-xs text-out-fg">{accountNameError}</p>}
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
                      onClick={() => {
                        setAccColor(col.hex);
                        setAccColorChosen(true);
                      }}
                      aria-label={`Warna ${col.name}`}
                      aria-pressed={isSelected}
                      className={cn(
                        "min-h-11 min-w-11 rounded-full transition-transform flex items-center justify-center text-white",
                        isSelected
                          ? "scale-110"
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
                <div className="mt-2 p-2.5 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] flex items-center justify-between">
                  <span className="text-xs text-[var(--muted)] font-medium">Pratinjau Tag Rekening:</span>
                  <span
                    style={{
                      backgroundColor: accColor,
                      color: getContrastTextColor(accColor),
                    }}
                    className="font-semibold text-xs sm:text-sm px-3 py-1 rounded-md"
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
                  onChange={(e) => {
                    const nextType = e.target.value as typeof accType;
                    setAccType(nextType);
                    if (!editingAccount && !accColorChosen) setAccColor(defaultAccountTagColor(nextType));
                  }}
                  className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="bank">Rekening Bank (BCA, Mandiri, BRI, dll)</option>
                  <option value="wallet">Dompet Digital / E-Wallet (GoPay, OVO, ShopeePay, dll)</option>
                  <option value="cash">Uang Tunai / Cash (Dompet, Brankas)</option>
                  <option value="investment">Rekening Investasi (Bibit, Stockbit, Binance, dll)</option>
                  <option value="credit">Kredit / PayLater (SPayLater, Kredivo, kartu kredit)</option>
                </select>
              </div>

              <RoleField id="account-role" value={accRole} onChange={setAccRole} fixed={fixedRole(accType, false)} error={roleError} />

              {editingAccount && accType !== "investment" && accType !== "credit" && editingAccount.type !== "credit" && !(editingAccount.children && editingAccount.children.length > 0) && parentCandidates.length > 0 && (
                <div>
                  <label htmlFor="account-move-parent" className="font-medium text-[var(--muted)] block mb-1">Pindahkan ke rekening</label>
                  <select
                    id="account-move-parent"
                    name="parent_id"
                    value={accParentId}
                    onChange={(e) => setAccParentId(e.target.value)}
                    aria-describedby="account-move-parent-hint"
                    className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tetap sebagai rekening utama</option>
                    {parentCandidates.map((candidate) => (
                      <option key={candidate.id} value={candidate.id}>{candidate.name}</option>
                    ))}
                  </select>
                  <p id="account-move-parent-hint" className="mt-1 text-xs text-[var(--muted)]">
                    Untuk saldo yang sebenarnya disimpan di bank lain, misalnya GoPay Tabungan di Jago. Riwayat transaksi tetap
                    utuh; beri nama kantong sama seperti di notifikasi bank.
                  </p>
                </div>
              )}

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
                    className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Tanpa Kantong Default (Gunakan Rekening Induk)</option>
                    {editingAccount.children.filter((child) => !child.is_archived).map((child) => (
                      <option key={child.id} value={child.id}>
                        {child.name} ({bal(child.balance)})
                      </option>
                    ))}
                  </select>
                  {defaultPocketError && <p id="account-default-pocket-error" className="mt-1 text-xs text-out-fg">{defaultPocketError}</p>}
                  <p className="text-xs text-[var(--muted)] mt-1">
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
                          "min-h-11 p-2.5 rounded-md border text-left transition-[border-color,background-color,color]",
                          accIsMultiInstrument
                            ? "bg-[var(--window)] border-[var(--line-strong)] text-[var(--text)]"
                            : "bg-[var(--surface-2)] border-[var(--line)] text-[var(--muted)] hover:text-[var(--text)]"
                        )}
                      >
                        <div className="flex items-center gap-1.5 font-semibold text-xs text-[var(--text)]">
                          <Icon name="layers" className="h-3.5 w-3.5 text-inv-fg" />
                          <span>Platform Multi-Kantong</span>
                        </div>
                        <p className="text-xs text-[var(--muted)] mt-1 leading-relaxed">
                          Bibit, Bareksa, Stockbit. Memiliki banyak produk instrumen yang dikelola per kantong di dalam.
                        </p>
                      </button>

                      <button
                        type="button"
                        onClick={() => setAccIsMultiInstrument(false)}
                        aria-pressed={!accIsMultiInstrument}
                        className={cn(
                          "min-h-11 p-2.5 rounded-md border text-left transition-[border-color,background-color,color]",
                          !accIsMultiInstrument
                            ? "bg-[var(--window)] border-[var(--line-strong)] text-[var(--text)]"
                            : "bg-[var(--surface-2)] border-[var(--line)] text-[var(--muted)] hover:text-[var(--text)]"
                        )}
                      >
                        <div className="flex items-center gap-1.5 font-semibold text-xs text-[var(--text)]">
                          <Icon name="chart" className="h-3.5 w-3.5 text-inv-fg" />
                          <span>Instrumen Tunggal</span>
                        </div>
                        <p className="text-xs text-[var(--muted)] mt-1 leading-relaxed">
                          Binance (langsung Kripto), Emas Fisik, atau akun khusus 1 instrumen tanpa kantong.
                        </p>
                      </button>
                    </div>
                  </div>

                  {accIsMultiInstrument && (
                    <div className="p-3 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] text-inv-fg text-xs leading-relaxed">
                      💡 Rekening <strong>{accName || "Investasi"}</strong> akan dibuat sebagai akun induk platform. Setelah dibuat, berbagai kantong instrumen dapat ditambahkan (seperti RDPU, Saham BBCA, Bareksa Sukuk, dll) di dalamnya.
                    </div>
                  )}

                  <div className="pt-2 border-t border-[var(--line)]">
                    <label htmlFor="account-default-funding" className="font-medium text-[var(--muted)] flex items-center justify-between mb-1.5">
                      <span className="flex items-center gap-1.5 font-semibold text-xs text-[var(--text)]">
                        <Icon name="credit-card" className="h-3.5 w-3.5 text-tr-fg" />
                        <span>Sumber Dana / RDN Default</span>
                      </span>
                      <span className="text-xs text-[var(--muted)]">Opsional</span>
                    </label>
                    <select
                      id="account-default-funding"
                      name="default_funding_account_id"
                      value={accDefaultFundingId}
                      onChange={(e) => setAccDefaultFundingId(e.target.value)}
                      className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
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
                    <p className="text-xs text-[var(--muted)] mt-1 leading-relaxed">
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
                    "min-h-11 px-3 rounded-md border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
                    accIsMultiInstrument
                      ? "bg-[var(--window)] text-[var(--text)] border-[var(--line-strong)]"
                      : "bg-[var(--surface-2)] text-[var(--muted)] border-[var(--line)] hover:text-[var(--text)]"
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
                    "min-h-11 px-3 rounded-md border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
                    !accIsMultiInstrument
                      ? "bg-[var(--window)] text-[var(--text)] border-[var(--line-strong)]"
                      : "bg-[var(--surface-2)] text-[var(--muted)] border-[var(--line)] hover:text-[var(--text)]"
                  )}
                >
                  <Icon name="chart" className="h-3.5 w-3.5" />
                  <span>Instrumen Investasi</span>
                </button>
              </div>
              <div className="mt-4">
                <RoleField id="account-role" value={accRole} onChange={setAccRole} fixed={accIsMultiInstrument ? null : "investasi"} error={roleError} />
              </div>
            </div>
          )}

          {/* Show Instrument Fields if single instrument OR editing investment pocket */}
          {((!editingAccount?.parent_id && accType === "investment" && !accIsMultiInstrument) ||
            (editingAccount?.parent_id && !accIsMultiInstrument)) && (
            <div className="space-y-3 p-3.5 rounded-xl bg-[var(--surface-2)] border border-[var(--line)]">
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
                        "min-h-11 px-2 rounded-md border text-xs font-semibold transition-colors",
                        accInstrumentType === opt.value
                          ? "bg-[var(--window)] text-[var(--text)] border-[var(--line-strong)]"
                          : "bg-[var(--surface-2)] text-[var(--muted)] border-[var(--line)] hover:text-[var(--text)]"
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
                  <span className="text-xs text-[var(--muted)]">Opsional</span>
                </label>
                <div className="relative">
                  <input
                    id="account-ticker"
                    name="instrument_symbol"
                    type="text"
                    value={accTicker}
                    onChange={(e) => handleSearchTicker(e.target.value, false)}
                    placeholder="Contoh: BBCA, GC=F, AAPL, BTC-USD"
                    className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] uppercase pr-8 font-semibold"
                  />
                  {isSearchingTicker && (
                    <div className="absolute right-2.5 top-2.5">
                      <Icon name="refresh-cw" className="h-4 w-4 animate-spin text-inv-fg" />
                    </div>
                  )}
                </div>
                <p className="text-xs text-[var(--muted)] mt-1">
                  Kosongkan jika produk manual tanpa ticker publik (misal RDPU Bibit / Bareksa).
                </p>

                {tickerSuggestions.length > 0 && (
                  <div className="absolute z-50 left-0 right-0 mt-1 rounded-xl border border-[var(--line)] bg-[var(--dialog)] shadow-float overflow-hidden max-h-48 overflow-y-auto">
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
                        className="flex min-h-11 w-full items-center justify-between border-b border-[var(--line)]/50 p-2.5 text-left text-xs hover:bg-[var(--surface-2)] last:border-0"
                      >
                        <div>
                          <span className="font-semibold text-inv-fg">{item.symbol}</span>
                          <span className="text-[var(--text)] ml-2">{item.name}</span>
                        </div>
                        <span className="text-xs uppercase text-[var(--muted)] px-1.5 py-0.5 rounded-md bg-[var(--surface-2)] font-medium">
                          {item.exchange || item.type}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </div>}

              {accInstrumentType !== "deposit" && editingAccount?.investment_tracking_mode !== "amount" && (() => {
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
                        className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                      />
                      <p className="text-xs text-[var(--muted)] mt-0.5">{aCfg.unitsHint}</p>
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
                        className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                      />
                    </div>
                  </div>
                );
              })()}
            </div>
          )}

          {!editingAccount?.parent_id && accType === "credit" && (
            <>
              {!editingAccount && (
                <MonetaryInput
                  id="account-initial-balance"
                  name="initial_balance"
                  label="Utang saat ini (IDR)"
                  value={accInitBal}
                  onChange={setAccInitBal}
                  error={accountBalanceError}
                  description="Jumlah yang belum dibayar hari ini. Isi 0 jika belum ada utang."
                />
              )}
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <MonetaryInput
                  id="account-credit-limit"
                  name="credit_limit"
                  label="Limit (opsional)"
                  value={accCreditLimit}
                  onChange={setAccCreditLimit}
                  error={creditLimitError}
                  placeholder="Contoh: 2.000.000"
                  description="Batas dari penyedia kredit. Melewati limit hanya diberi tanda."
                />
                <FormField id="account-payment-day" label="Tanggal bayar tiap bulan (opsional)" error={paymentDayError}>
                  {(field) => (
                    <input
                      {...field}
                      name="payment_day"
                      type="number"
                      inputMode="numeric"
                      min={1}
                      max={31}
                      step={1}
                      value={accPaymentDay}
                      onChange={(event) => setAccPaymentDay(event.target.value)}
                      placeholder="Contoh: 11"
                      className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-semibold tabular-nums text-[var(--text)]"
                    />
                  )}
                </FormField>
              </div>
            </>
          )}

          {!editingAccount && accType !== "credit" && !(accType === "investment" && accIsMultiInstrument) && (
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
            <p className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)] p-3 text-xs leading-relaxed text-[var(--muted)]">
              Platform ini hanya mengelompokkan kantong. Setelah dibuat, tambahkan kantong kas/RDN untuk saldo dana dan kantong instrumen untuk posisi investasi.
            </p>
          )}

          <div className="flex gap-2 pt-2">
            <button
              type="button"
              onClick={() => setAccountModalOpen(false)}
              className="flex-1 rounded-md border border-[var(--line)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-2)]"
            >
              Batal
            </button>
            <button
              type="submit"
              disabled={saveAccountMutation.isPending}
              className="min-h-11 flex-1 rounded-md bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
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
              <div role="alert" className="p-3 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] text-out-fg font-medium">
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
                className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
              />
              {pocketNameError && <p id="pocket-name-error" className="mt-1 text-xs text-out-fg">{pocketNameError}</p>}
            </div>

            <div>
              <p className="font-medium text-[var(--muted)] block mb-1.5">Tipe kantong</p>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <button
                  type="button"
                  onClick={() => setPocketIsInvestment(false)}
                  aria-pressed={!pocketIsInvestment}
                  className={cn(
                    "min-h-11 px-3 rounded-md border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
                    !pocketIsInvestment
                      ? "bg-[var(--window)] text-[var(--text)] border-[var(--line-strong)]"
                      : "bg-[var(--surface-2)] text-[var(--muted)] border-[var(--line)] hover:text-[var(--text)]"
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
                    "min-h-11 px-3 rounded-md border text-xs font-semibold transition-colors flex items-center justify-center gap-1.5",
                    pocketIsInvestment
                      ? "bg-[var(--window)] text-[var(--text)] border-[var(--line-strong)]"
                      : "bg-[var(--surface-2)] text-[var(--muted)] border-[var(--line)] hover:text-[var(--text)]"
                  )}
                >
                  <Icon name="chart" className="h-3.5 w-3.5" />
                  <span>Instrumen Investasi</span>
                </button>
              </div>
              <p className="text-xs text-[var(--muted)] mt-1">
                {pocketIsInvestment
                  ? "Kantong ini untuk instrumen investasi (Saham, Reksadana, Emas, Kripto, Deposito) dengan pemantauan harga pasar / capital gain."
                  : "Kantong ini untuk uang cair atau pos belanja operasional biasa."}
              </p>
            </div>

            <RoleField id="pocket-role" value={pocketRole} onChange={setPocketRole} fixed={pocketIsInvestment ? "investasi" : null} error={pocketRoleError} />

            {pocketIsInvestment && (
              <div className="space-y-3 p-3.5 rounded-xl bg-[var(--surface-2)] border border-[var(--line)]">
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
                          "min-h-11 px-2 rounded-md border text-xs font-semibold transition-colors",
                          pocketInstrumentType === opt.value
                            ? "bg-[var(--window)] text-[var(--text)] border-[var(--line-strong)]"
                            : "bg-[var(--surface-2)] text-[var(--muted)] border-[var(--line)] hover:text-[var(--text)]"
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
                    <span className="text-xs text-[var(--muted)]">Opsional</span>
                  </label>
                  <div className="relative">
                    <input
                      id="pocket-ticker"
                      name="instrument_symbol"
                      type="text"
                      value={pocketTicker}
                      onChange={(e) => handleSearchTicker(e.target.value, true)}
                      placeholder="Contoh: BBCA, GC=F, AAPL, BTC-USD"
                      className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] uppercase pr-8 font-semibold"
                    />
                    {pocketIsSearchingTicker && (
                      <div className="absolute right-2.5 top-2.5">
                        <Icon name="refresh-cw" className="h-4 w-4 animate-spin text-inv-fg" />
                      </div>
                    )}
                  </div>
                  <p className="text-xs text-[var(--muted)] mt-1">
                    Kosongkan jika produk manual tanpa ticker publik (misal RDPU Bibit / BCA Reksadana). Nilai pasar dapat diupdate berkala via tombol "Perbarui nilai".
                  </p>

                  {pocketTickerSuggestions.length > 0 && (
                    <div className="absolute z-50 left-0 right-0 mt-1 rounded-xl border border-[var(--line)] bg-[var(--dialog)] shadow-float overflow-hidden max-h-48 overflow-y-auto">
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
                          className="flex min-h-11 w-full items-center justify-between border-b border-[var(--line)]/50 p-2.5 text-left text-xs hover:bg-[var(--surface-2)] last:border-0"
                        >
                          <div>
                            <span className="font-semibold text-inv-fg">{item.symbol}</span>
                            <span className="text-[var(--text)] ml-2">{item.name}</span>
                          </div>
                          <span className="text-xs uppercase text-[var(--muted)] px-1.5 py-0.5 rounded-md bg-[var(--surface-2)] font-medium">
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
                          className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
                        />
                        <p className="text-xs text-[var(--muted)] mt-0.5">{pCfg.unitsHint}</p>
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
                          className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
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
                className="flex-1 rounded-md border border-[var(--line)] py-2 text-xs font-medium text-[var(--muted)] hover:bg-[var(--surface-2)]"
              >
                Batal
              </button>
              <button
                type="submit"
                disabled={savePocketMutation.isPending}
                className="min-h-11 flex-1 rounded-md bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
              >
                {savePocketMutation.isPending ? "Menyimpan…" : "Buat Kantong"}
              </button>
            </div>
          </form>
        </Modal>
      )}

      {/* Valuation modal ("Perbarui nilai") */}
      {valuationAccount && (
        <Modal
          open={Boolean(valuationAccount)}
          onClose={() => setValuationAccount(null)}
          title={`Perbarui nilai: ${valuationAccount.name}`}
        >
          <form
            className="space-y-4 pt-2 text-xs [&_input]:min-h-11 [&_select]:min-h-11"
            onSubmit={(event) => {
              event.preventDefault();
              if (!valuationMutation.isPending) valuationMutation.mutate();
            }}
          >
            {valError && (
              <div role="alert" className="p-3 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] text-out-fg font-medium">
                {valError}
              </div>
            )}

            <p className="text-xs text-[var(--muted)]">
              Perbarui nilai pasar investasi terkini (misal dari aplikasi Bibit, Bareksa, atau harga penutupan). Untung/rugi dihitung otomatis terhadap modal beli.
            </p>

            <div className="p-3.5 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-[var(--muted)]">Nilai Tercatat Saat Ini:</span>
                <span className="font-semibold tabular text-sm text-[var(--text)]">
                  {bal(valuationAccount.balance)}
                </span>
              </div>
              {valuationAccount.units && valuationAccount.avg_buy_price ? (
                <div className="flex items-center justify-between text-xs text-[var(--muted)] pt-1.5 border-t border-[var(--line)]/50">
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
                className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-base font-semibold tabular text-[var(--text)]"
              />
              <p className="text-xs text-[var(--muted)] mt-1">
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
                className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] tabular font-medium"
              />
              <p className="text-xs text-[var(--muted)] mt-1">
                Total modal bersih yang dibayarkan saat membeli ({valuationAccount.units ? `${valuationAccount.units} ${getInstrumentConfig(valuationAccount.instrument_type).unitUnit}` : "aset ini"}).
              </p>
            </div>

            {/* Capital Gain preview */}
            {(() => {
              if (!valCostBasisStr.trim()) return <p className="text-xs">Gain belum tersedia sampai total modal diisi.</p>;
              const curVal = parseNumberFromDots(valCurrentBalStr);
              const costVal = parseNumberFromDots(valCostBasisStr);
              const gain = curVal - costVal;
              const gainPct = costVal > 0 ? ((gain / costVal) * 100).toFixed(2) : "0.00";
              return (
                <div className="p-3 rounded-xl border border-[var(--line)] bg-[var(--surface-2)]/50 space-y-1">
                  <div className="flex justify-between items-center tabular">
                    <span className="text-[var(--muted)]">Estimasi untung/rugi:</span>
                    <span
                      className={cn(
                        "font-semibold text-xs",
                        gain >= 0 ? "text-in-fg" : "text-out-fg"
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
                className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setValuationAccount(null)}
                className="ws-btn flex-1"
              >
                Batal
              </button>
              <button
                type="submit"
                disabled={valuationMutation.isPending}
                className="ws-btn ws-btn-ink flex-1"
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
          onClose={() => { setReconcileAccount(null); setAfterReconcile(null); }}
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
              <div role="alert" className="p-3 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] text-out-fg font-medium">
                {reconcileError}
              </div>
            )}

            <p className="text-xs text-[var(--muted)]">
              Masukkan nominal saldo yang saat ini tercatat pada aplikasi bank atau dompet fisik. Transaksi penyesuaian akan dicatat secara otomatis jika ada selisih.
            </p>

            <div className="p-3.5 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] flex items-center justify-between">
              <span className="text-[var(--muted)]">Saldo Tercatat Saat Ini:</span>
              <span className="font-semibold tabular text-sm text-[var(--text)]">
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
                <div className="p-3 rounded-xl border border-[var(--line)] bg-[var(--surface-2)]/50 space-y-1">
                  <div className="flex justify-between items-center tabular">
                    <span className="text-[var(--muted)]">Penyesuaian Selisih:</span>
                    <span
                      className={cn(
                        "font-semibold text-xs",
                        diff > 0
                          ? "text-in-fg"
                          : diff < 0
                          ? "text-out-fg"
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
                className="w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]"
              />
            </div>

            <div className="flex gap-2 pt-2">
              <button
                type="button"
                onClick={() => setReconcileAccount(null)}
                className="ws-btn flex-1"
              >
                Batal
              </button>
              <button
                type="submit"
                disabled={reconcileMutation.isPending}
                className="ws-btn ws-btn-ink flex-1"
              >
                {reconcileMutation.isPending ? "Menyesuaikan…" : "Konfirmasi & Sesuaikan Saldo"}
              </button>
            </div>
          </form>
        </Modal>
      )}

      {/* Recurring Rules Management Modal */}
      <RecurringRulesModal
        open={recurringModalOpen}
        onClose={() => { setRecurringModalOpen(false); setInvestmentSchedule(null); }}
        investmentDraft={investmentSchedule}
      />

      <InvestmentTopupModal
        open={Boolean(topupProduct)}
        onClose={() => setTopupProduct(null)}
        product={topupProduct}
        allAccounts={accountsData?.accounts ?? []}
        onSchedule={(draft) => { setInvestmentSchedule(draft); setTopupProduct(null); setRecurringModalOpen(true); }}
      />

      <AmountTrackingSwitchModal product={amountSwitchProduct} onClose={() => setAmountSwitchProduct(null)} />

      {/* Investment Trade Modal (Beli / Jual) */}
      <InvestmentTradeModal
        open={tradeModalOpen}
        onClose={() => setTradeModalOpen(false)}
        pocket={tradePocket}
        allAccounts={accountsData?.accounts ?? []}
        defaultAction={tradeAction}
      />
    </>
  );

  return {
    dialogs,
    INSTRUMENT_OPTIONS,
    getInstrumentConfig,
    openNewAccount: handleOpenNewAccount,
    openEditAccount: handleOpenEditAccount,
    openNewPocket: handleOpenNewPocket,
    openValuation: handleOpenValuation,
    openReconciliation,
    openTrade,
    openTopup: setTopupProduct,
    openAmountSwitch: setAmountSwitchProduct,
    openPlans: () => setRecurringModalOpen(true),
    archiveMutation,
    syncPricesMutation,
    syncMessage,
    accountActionError,
  };
}
