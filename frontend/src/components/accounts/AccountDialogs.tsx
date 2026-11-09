"use client";

import { useMemo, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api, type ApiErrorDetail } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { cn, formatNumberWithDots, parseNumberFromDots, formatDecimalInput, parseDecimal, parseUnits } from "@/lib/utils";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { Icon } from "@/components/ui/Icon";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { InvestmentTradeModal } from "@/components/ui/InvestmentTradeModal";
import { InvestmentTopupModal } from "@/components/ui/InvestmentTopupModal";
import { AmountTrackingSwitchModal } from "@/components/ui/AmountTrackingSwitchModal";
import { GainText } from "@/components/workspace/figures";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { useToast } from "@/components/workspace/overlays";
import { RadioSegments } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { InvestmentScheduleDraft } from "@/lib/investmentTopups";
import { DEFAULT_ACCOUNT_TAG_COLOR, INVESTMENT_ACCOUNT_TAG_COLOR, defaultAccountTagColor } from "@/lib/accountColours";
import { RoleField, cashRoleOf, fixedRole, type CashRole } from "./RoleField";
import type { AccountItem, AccountsResponse } from "./types";

/** The owner's nine tag colours; stored colours stay selectable (apply-markas-rekening D9). */
const ACCOUNT_TAG_COLORS = [
  { key: "blue", hex: DEFAULT_ACCOUNT_TAG_COLOR },
  { key: "emerald", hex: "#059669" },
  { key: "lavender", hex: INVESTMENT_ACCOUNT_TAG_COLOR },
  { key: "purple", hex: "#7C3AED" },
  { key: "amber", hex: "#D97706" },
  { key: "rose", hex: "#E11D48" },
  { key: "indigo", hex: "#4F46E5" },
  { key: "teal", hex: "#0D9488" },
  { key: "slate", hex: "#475569" },
] as const;

const ACCOUNT_TYPES: AccountItem["type"][] = ["bank", "wallet", "cash", "investment", "credit"];
const INSTRUMENT_TYPES = ["stock", "mutual_fund", "gold", "crypto", "deposit", "other"] as const;
type InstrumentType = (typeof INSTRUMENT_TYPES)[number];

/** The instrument whose field labels apply; anything unknown reads as shares, as before. */
function instrumentCopyKey(type?: string | null): "stock" | "mutual_fund" | "gold" | "crypto" | "deposit" {
  if (type === "mutual_fund" || type === "gold" || type === "crypto" || type === "deposit") return type;
  return "stock";
}

/** A field that a save can refuse, so its message sits under that field (SPEC §7). */
type FieldKey = "name" | "role" | "balance" | "defaultPocket" | "limit" | "statementDay" | "paymentDay";
type FieldErrors = Partial<Record<FieldKey, string>>;

const FIELD_OF_CODE: Record<string, FieldKey> = {
  account_name_taken: "name",
  account_role_type_mismatch: "role",
  credit_balance_sign: "balance",
  default_pocket_before_pockets: "defaultPocket",
  default_pocket_blocks_nesting: "defaultPocket",
  default_pocket_invalid: "defaultPocket",
  default_pocket_not_found: "defaultPocket",
  default_pocket_parent_only: "defaultPocket",
};

/** The field a refusal belongs to: by the server's code, else by the words of an older plain message. */
function fieldOfError(error: unknown): FieldKey | null {
  const detail = (error as { detail?: unknown } | null)?.detail;
  const code = typeof detail === "object" && detail ? (detail as ApiErrorDetail).code : undefined;
  if (code && FIELD_OF_CODE[code]) return FIELD_OF_CODE[code];
  const message = error instanceof Error ? error.message : "";
  if (/nama (rekening|kantong)|account name|pocket name/i.test(message)) return "name";
  if (/kantong default|kantong utama|default pocket/i.test(message)) return "defaultPocket";
  if (/^kegunaan/i.test(message)) return "role";
  if (/saldo|initial_balance/i.test(message)) return "balance";
  if (/^limit/i.test(message)) return "limit";
  return null;
}

const FIELD_ID: Record<FieldKey, string> = {
  name: "account-name",
  role: "account-role",
  balance: "account-initial-balance",
  defaultPocket: "account-default-pocket",
  limit: "account-credit-limit",
  statementDay: "account-statement-day",
  paymentDay: "account-payment-day",
};

/** A day of the month typed in a credit field: null when empty, false when outside 1 to 31. */
function dayValue(text: string): number | null | false {
  const trimmed = text.trim();
  if (!trimmed) return null;
  const day = Number(trimmed);
  return Number.isInteger(day) && day >= 1 && day <= 31 ? day : false;
}

function errorText(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

/** A SPEC field: label, control, the error under it and a hint; the control gets its ids and state. */
function Field({
  id,
  label,
  hint,
  error,
  describedBy: extraDescription,
  className,
  children,
}: {
  id: string;
  label: string;
  hint?: ReactNode;
  error?: string;
  /** Another element that describes the control, such as a hint shared by two fields. */
  describedBy?: string;
  className?: string;
  children: (aria: { id: string; "aria-invalid"?: true; "aria-describedby"?: string }) => ReactNode;
}) {
  const errorId = error ? `${id}-error` : undefined;
  const hintId = hint ? `${id}-hint` : undefined;
  const describedBy = [errorId, hintId, extraDescription].filter(Boolean).join(" ") || undefined;
  return (
    <div className={cn("field", className)}>
      <label htmlFor={id}>{label}</label>
      {children({ id, "aria-invalid": error ? true : undefined, "aria-describedby": describedBy })}
      {error && <p id={errorId} className="err">{error}</p>}
      {hint && <p id={hintId} className="hint">{hint}</p>}
    </div>
  );
}

/** The nine colours as one radio group: arrows move and choose, the chosen swatch has a 1 px ink outline. */
function ColourField({ value, onChange }: { value: string; onChange: (hex: string) => void }) {
  const t = useTranslations("accounts.form");
  const chosen = ACCOUNT_TAG_COLORS.findIndex((colour) => colour.hex.toLowerCase() === value.toLowerCase());
  const focusable = chosen < 0 ? 0 : chosen;
  const move = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const next = (index + step + ACCOUNT_TAG_COLORS.length) % ACCOUNT_TAG_COLORS.length;
    onChange(ACCOUNT_TAG_COLORS[next].hex);
    event.currentTarget.parentElement?.querySelectorAll<HTMLElement>("button")[next]?.focus();
  };
  return (
    <div className="field">
      <span className="flabel" id="account-colour-label">{t("colour")}</span>
      <div className="rk-swatches" role="radiogroup" aria-labelledby="account-colour-label">
        {ACCOUNT_TAG_COLORS.map((colour, index) => (
          <button
            key={colour.hex}
            type="button"
            role="radio"
            aria-checked={index === chosen}
            aria-label={t("colourName", { name: t(`colours.${colour.key}`) })}
            tabIndex={index === focusable ? 0 : -1}
            style={{ backgroundColor: colour.hex }}
            onClick={() => onChange(colour.hex)}
            onKeyDown={(event) => move(event, index)}
          />
        ))}
      </div>
    </div>
  );
}

interface TickerResult {
  symbol: string;
  name: string;
  type: string;
  exchange: string;
}

/** The instrument code with its suggestions from the price source. */
function TickerField({
  id,
  value,
  searching,
  suggestions,
  onSearch,
  onPick,
}: {
  id: string;
  value: string;
  searching: boolean;
  suggestions: TickerResult[];
  onSearch: (query: string) => void;
  onPick: (item: TickerResult) => void;
}) {
  const t = useTranslations("accounts.form");
  return (
    <Field id={id} label={t("ticker")} hint={t("tickerHint")}>
      {(aria) => (
        <div className="relative">
          <input
            {...aria}
            name="instrument_symbol"
            type="text"
            autoComplete="off"
            value={value}
            onChange={(event) => onSearch(event.target.value)}
            placeholder={t("tickerPlaceholder")}
            className="field-control uppercase"
          />
          {searching && <Icon name="refresh-cw" className="absolute right-3 top-3 h-4 w-4 animate-spin text-[var(--muted)]" />}
          {suggestions.length > 0 && (
            <ul className="absolute left-0 right-0 z-50 mt-1 max-h-48 overflow-y-auto rounded-md border border-[var(--line)] bg-[var(--dialog)] shadow-float">
              {suggestions.map((item) => (
                <li key={item.symbol}>
                  <button
                    type="button"
                    onClick={() => onPick(item)}
                    className="flex min-h-11 w-full items-center justify-between gap-3 border-b border-[var(--line)] px-3 text-left text-sm last:border-0 hover:bg-[var(--surface-2)]"
                  >
                    <span className="min-w-0 truncate">
                      <b className="font-semibold">{item.symbol}</b> {item.name}
                    </span>
                    <span className="t-meta uppercase">{item.exchange || item.type}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </Field>
  );
}

/** Units and price of one instrument; their product fills the opening value, as before. */
function PositionFields({
  idPrefix,
  instrument,
  units,
  price,
  onUnits,
  onPrice,
}: {
  idPrefix: string;
  instrument: string;
  units: string;
  price: string;
  onUnits: (value: string) => void;
  onPrice: (value: string) => void;
}) {
  const t = useTranslations("accounts.form.instrument");
  const format = useFormat();
  const key = instrumentCopyKey(instrument);
  return (
    <div className="form-grid">
      <Field id={`${idPrefix}-units`} label={t(`${key}.units`)} hint={t(`${key}.unitsHint`)}>
        {(aria) => (
          <input
            {...aria}
            name="units"
            type="text"
            inputMode="decimal"
            value={units}
            onChange={(event) => onUnits(event.target.value)}
            placeholder={t(`${key}.unitsExample`, { value: format.decimal(key === "mutual_fund" ? 2871.45 : key === "gold" ? 0.5 : key === "crypto" ? 0.05 : 1000, 2) })}
            className="field-control tabular-nums"
          />
        )}
      </Field>
      <Field id={`${idPrefix}-average-price`} label={t(`${key}.price`)}>
        {(aria) => (
          <input
            {...aria}
            name="avg_buy_price"
            type="text"
            inputMode="decimal"
            value={price}
            onChange={(event) => onPrice(formatDecimalInput(event.target.value))}
            placeholder={t("example", { value: format.integer(key === "mutual_fund" ? 1922 : key === "gold" ? 1_450_000 : key === "crypto" ? 1_000_000_000 : 6000) })}
            className="field-control tabular-nums"
          />
        )}
      </Field>
    </div>
  );
}

/**
 * Every Rekening & Kantong form and its write (design D5, apply-markas-rekening D9, D10, D15): the page and the
 * detail only decide which one opens. Request bodies and query invalidations are those of the old page; a credit
 * account also sends its bill day (`statement_day`, add-credit-spending-flow H8).
 */
export function useAccountDialogs() {
  const qc = useQueryClient();
  const t = useTranslations("accounts");
  const tc = useTranslations("common");
  const format = useFormat();
  const money = useMoney();
  const { showToast } = useToast();
  // The control that opened a form gets the focus back when it closes (ShellDialog leaves that to its caller).
  const opener = useRef<HTMLElement | null>(null);
  const rememberOpener = () => {
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  };
  const restoreOpener = () => {
    const element = opener.current;
    opener.current = null;
    requestAnimationFrame(() => {
      if (element?.isConnected) element.focus();
    });
  };

  const [accountModalOpen, setAccountModalOpen] = useState(false);
  const [recurringModalOpen, setRecurringModalOpen] = useState(false);
  const [editingAccount, setEditingAccount] = useState<AccountItem | null>(null);
  const [reconcileAccount, setReconcileAccount] = useState<AccountItem | null>(null);
  const [tradeModalOpen, setTradeModalOpen] = useState(false);
  const [topupProduct, setTopupProduct] = useState<AccountItem | null>(null);
  const [amountSwitchProduct, setAmountSwitchProduct] = useState<AccountItem | null>(null);
  const [investmentSchedule, setInvestmentSchedule] = useState<InvestmentScheduleDraft | null>(null);
  const [tradePocket, setTradePocket] = useState<AccountItem | null>(null);
  const [tradeAction, setTradeAction] = useState<"buy" | "sell">("buy");

  const instrumentOptions = INSTRUMENT_TYPES.map((value) => ({ value, label: t(`form.instruments.${value}`) }));

  // Investment account form
  const [accIsMultiInstrument, setAccIsMultiInstrument] = useState<boolean>(true);
  const [accInstrumentType, setAccInstrumentType] = useState<string>("stock");
  const [accTicker, setAccTicker] = useState("");
  const [accUnits, setAccUnits] = useState("");
  const [accAvgBuyPrice, setAccAvgBuyPrice] = useState("");
  const [tickerSuggestions, setTickerSuggestions] = useState<TickerResult[]>([]);
  const [isSearchingTicker, setIsSearchingTicker] = useState(false);

  // Investment pocket form
  const [pocketIsInvestment, setPocketIsInvestment] = useState<boolean>(false);
  const [pocketInstrumentType, setPocketInstrumentType] = useState<string>("mutual_fund");
  const [pocketTicker, setPocketTicker] = useState("");
  const [pocketUnits, setPocketUnits] = useState("");
  const [pocketAvgBuyPrice, setPocketAvgBuyPrice] = useState("");
  const [pocketTickerSuggestions, setPocketTickerSuggestions] = useState<TickerResult[]>([]);
  const [pocketIsSearchingTicker, setPocketIsSearchingTicker] = useState(false);

  // "Perbarui nilai"
  const [valuationAccount, setValuationAccount] = useState<AccountItem | null>(null);
  const [valCurrentBalStr, setValCurrentBalStr] = useState("");
  const [valCostBasisStr, setValCostBasisStr] = useState("");
  const [valNotes, setValNotes] = useState("");
  const [valError, setValError] = useState("");

  const handleOpenValuation = (acc: AccountItem) => {
    rememberOpener();
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
  const closeValuation = () => {
    setValuationAccount(null);
    restoreOpener();
  };

  // "Perbarui harga" reports through a toast (D15).
  const syncPricesMutation = useMutation({
    mutationFn: async () => {
      return api.post<{ ok: boolean; symbols_checked: number; accounts_updated: number }>("/accounts/sync-prices");
    },
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      showToast(t("toast.pricesUpdated", { count: data.accounts_updated, time: format.time(new Date()) }));
    },
    onError: () => showToast(t("toast.pricesFailed")),
  });

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
      closeValuation();
    },
    onError: (err: unknown) => setValError(errorText(err, t("valuation.failed"))),
  });

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
      const res = await api.get<{ ok: boolean; results: TickerResult[] }>(
        `/accounts/instruments/search?q=${encodeURIComponent(q)}&limit=6`
      );
      if (isPocket) setPocketTickerSuggestions(res.results || []);
      else setTickerSuggestions(res.results || []);
    } catch {
      // Suggestions are a convenience: without them the owner types the code.
    } finally {
      if (isPocket) setPocketIsSearchingTicker(false);
      else setIsSearchingTicker(false);
    }
  };

  // Account form
  const [accName, setAccName] = useState("");
  const [accType, setAccType] = useState<AccountItem["type"]>("bank");
  const [accInitBal, setAccInitBal] = useState("");
  // Credit-only fields (add-credit-accounts D2, H8); what a credit account owes today is typed, positive, in accInitBal.
  const [accCreditLimit, setAccCreditLimit] = useState("");
  const [accStatementDay, setAccStatementDay] = useState("");
  const [accPaymentDay, setAccPaymentDay] = useState("");
  const [accColor, setAccColor] = useState<string>(DEFAULT_ACCOUNT_TAG_COLOR);
  // Until the owner picks a colour, a new account's colour follows the default of its type.
  const [accColorChosen, setAccColorChosen] = useState(false);
  const [accDefaultFundingId, setAccDefaultFundingId] = useState<string>("");
  const [accDefaultPocketId, setAccDefaultPocketId] = useState<string>("");
  const [accParentId, setAccParentId] = useState<string>("");
  // "Kegunaan" of a cash, bank or e-wallet account; credit and products take theirs from the type (D1).
  const [accRole, setAccRole] = useState<CashRole>("harian");
  const [accErrors, setAccErrors] = useState<FieldErrors>({});
  const [accFormError, setAccFormError] = useState("");

  // Pocket form
  const [pocketModalOpen, setPocketModalOpen] = useState(false);
  const [pocketParentAccount, setPocketParentAccount] = useState<AccountItem | null>(null);
  const [pocketName, setPocketName] = useState("");
  const [pocketInitBal, setPocketInitBal] = useState("");
  const [pocketRole, setPocketRole] = useState<CashRole>("harian");
  const [pocketErrors, setPocketErrors] = useState<FieldErrors>({});
  const [pocketFormError, setPocketFormError] = useState("");

  // "Sesuaikan saldo"
  const [actualBalStr, setActualBalStr] = useState("");
  const [reconcileNotes, setReconcileNotes] = useState("");
  const [reconcileError, setReconcileError] = useState("");
  // Set when "Cocokkan saldo" opened the adjustment, so it can record a matched point afterwards (OD145).
  const [afterReconcile, setAfterReconcile] = useState<((actual: number) => void) | null>(null);
  const closeReconcile = () => {
    setReconcileAccount(null);
    setAfterReconcile(null);
    restoreOpener();
  };

  const reconcileMutation = useMutation({
    mutationFn: async () => {
      if (!reconcileAccount) return;
      const clean = actualBalStr.trim().replace(/[^0-9]/g, "");
      if (!clean) throw new Error(t("adjust.required"));
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
      closeReconcile();
    },
    onError: (err: unknown) => {
      setReconcileError(errorText(err, t("adjust.failed")));
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

  const handleOpenNewAccount = () => {
    rememberOpener();
    setEditingAccount(null);
    setAccName("");
    setAccType("bank");
    setAccInitBal("");
    setAccCreditLimit("");
    setAccStatementDay("");
    setAccPaymentDay("");
    setAccColor(DEFAULT_ACCOUNT_TAG_COLOR);
    setAccColorChosen(false);
    setAccDefaultFundingId("");
    setAccDefaultPocketId("");
    setAccParentId("");
    setAccRole("harian");
    setAccIsMultiInstrument(true);
    setAccInstrumentType("stock");
    setAccTicker("");
    setAccUnits("");
    setAccAvgBuyPrice("");
    setTickerSuggestions([]);
    setAccErrors({});
    setAccFormError("");
    setAccountModalOpen(true);
  };

  const handleOpenNewPocket = (parent: AccountItem) => {
    if (parent.type === "credit") return;
    rememberOpener();
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
    setPocketErrors({});
    setPocketFormError("");
    setPocketModalOpen(true);
  };

  const handleOpenEditAccount = (acc: AccountItem) => {
    rememberOpener();
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
    setAccStatementDay(acc.statement_day ? String(acc.statement_day) : "");
    setAccPaymentDay(acc.payment_day ? String(acc.payment_day) : "");
    const hasInst = Boolean(acc.instrument_type || acc.instrument_symbol || (acc.units !== null && acc.units !== undefined));
    setAccIsMultiInstrument(!hasInst);
    setAccInstrumentType(acc.instrument_type || "stock");
    setAccTicker(acc.instrument_symbol || "");
    setAccUnits(acc.units !== null && acc.units !== undefined ? String(acc.units).replace(".", ",") : "");
    setAccAvgBuyPrice(acc.avg_buy_price ? formatDecimalInput(acc.avg_buy_price) : "");
    setTickerSuggestions([]);
    setAccErrors({});
    setAccFormError("");
    setAccountModalOpen(true);
  };
  const closeAccountForm = () => {
    setAccountModalOpen(false);
    restoreOpener();
  };
  const closePocketForm = () => {
    setPocketModalOpen(false);
    restoreOpener();
  };

  const savePocketMutation = useMutation({
    mutationFn: async () => {
      if (!pocketParentAccount) return;
      const name = pocketName.trim();
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
      closePocketForm();
    },
    onError: (err: unknown) => {
      const message = errorText(err, t("pocketForm.failed"));
      const field = fieldOfError(err);
      if (field === "name" || field === "role" || field === "balance") {
        setPocketErrors({ [field]: message });
        document.getElementById({ name: "pocket-name", role: "pocket-role", balance: "pocket-initial-balance" }[field])?.focus();
      } else {
        setPocketFormError(message);
      }
    },
  });

  const submitPocket = (event: FormEvent) => {
    event.preventDefault();
    if (savePocketMutation.isPending) return;
    setPocketErrors({});
    setPocketFormError("");
    if (!pocketName.trim()) {
      setPocketErrors({ name: t("pocketForm.nameRequired") });
      document.getElementById("pocket-name")?.focus();
      return;
    }
    savePocketMutation.mutate();
  };

  const isCreditForm = !editingAccount?.parent_id && accType === "credit";

  const saveAccountMutation = useMutation({
    mutationFn: async () => {
      const name = accName.trim();
      const initBal = parseInt(accInitBal.replace(/[^0-9]/g, "") || "0", 10);
      const isEditingPocket = Boolean(editingAccount?.parent_id);
      const isCredit = !isEditingPocket && accType === "credit";
      const limitValue = parseNumberFromDots(accCreditLimit);
      // The days were checked before the save; an empty day clears it.
      const creditFields = isCredit
        ? {
          credit_limit: limitValue > 0 ? limitValue : null,
          payment_day: dayValue(accPaymentDay) || null,
          statement_day: dayValue(accStatementDay) || null,
        }
        : null;

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
        // Limit and both days belong to credit only; leaving credit clears them so the type can change.
        let creditPatch = {};
        if (creditFields) creditPatch = creditFields;
        else if (editingAccount.type === "credit") creditPatch = { credit_limit: null, payment_day: null, statement_day: null };
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
      }
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
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      closeAccountForm();
    },
    onError: (err: unknown) => {
      const message = errorText(err, t("form.failed"));
      const field = fieldOfError(err);
      if (field) {
        setAccErrors({ [field]: message });
        document.getElementById(FIELD_ID[field])?.focus();
      } else {
        setAccFormError(message);
      }
    },
  });

  const submitAccount = (event: FormEvent) => {
    event.preventDefault();
    if (saveAccountMutation.isPending) return;
    setAccErrors({});
    setAccFormError("");
    const errors: FieldErrors = {};
    if (!accName.trim()) errors.name = t("form.nameRequired");
    if (isCreditForm && dayValue(accStatementDay) === false) errors.statementDay = t("form.dayRange");
    if (isCreditForm && dayValue(accPaymentDay) === false) errors.paymentDay = t("form.dayRange");
    const first = (Object.keys(FIELD_ID) as FieldKey[]).find((key) => errors[key]);
    if (first) {
      setAccErrors(errors);
      document.getElementById(FIELD_ID[first])?.focus();
      return;
    }
    saveAccountMutation.mutate();
  };

  const archiveMutation = useMutation({
    mutationFn: async (accId: string) => {
      return api.del(`/accounts/${accId}`);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    },
    onError: (error: unknown) => showToast(errorText(error, t("toast.archiveFailed"))),
  });

  const openTrade = (account: AccountItem, action: "buy" | "sell") => {
    setTradePocket(account);
    setTradeAction(action);
    setTradeModalOpen(true);
  };

  const openReconciliation = (account: AccountItem, options?: { actual?: number; onReconciled?: (actual: number) => void }) => {
    rememberOpener();
    setReconcileAccount(account);
    setActualBalStr(formatNumberWithDots(options?.actual ?? account.balance));
    setReconcileNotes("");
    setReconcileError("");
    // A function value in state must be wrapped, or React would call it as an updater.
    setAfterReconcile(() => options?.onReconciled ?? null);
  };

  const editingPocket = Boolean(editingAccount?.parent_id);
  const showInstrumentFields = (!editingPocket && accType === "investment" && !accIsMultiInstrument) || (editingPocket && !accIsMultiInstrument);
  const accountTitle = editingAccount ? (editingPocket ? t("form.titleEditPocket") : t("form.titleEditAccount")) : t("form.titleNew");
  const typeOptions = ACCOUNT_TYPES.map((value) => ({ value, label: t(`form.types.${value}`) }));
  const pocketKindOptions = [
    { value: "cash" as const, label: t("form.pocketCash") },
    { value: "investment" as const, label: t("form.pocketInvestment") },
  ];
  const fillOpening = (units: string, price: string, setOpening: (value: string) => void) => {
    const u = parseUnits(units);
    const p = parseDecimal(price);
    if (u > 0 && p > 0) setOpening(formatNumberWithDots(Math.round(u * p)));
  };

  const accountFooter = (
    <>
      <button type="button" className="ws-btn" onClick={closeAccountForm}>{t("form.cancel")}</button>
      <button type="submit" form="account-form" className="ws-btn ws-btn-ink" disabled={saveAccountMutation.isPending}>
        {saveAccountMutation.isPending ? t("form.saving") : editingAccount ? t("form.save") : t("form.create")}
      </button>
    </>
  );

  const accountForm = (
    <form id="account-form" className="form-stack" onSubmit={submitAccount} noValidate>
      {accFormError && <p role="alert" className="form-alert">{accFormError}</p>}

      <Field id="account-name" label={t("form.name")} error={accErrors.name}>
        {(aria) => (
          <input
            {...aria}
            name="name"
            type="text"
            required
            value={accName}
            onChange={(event) => setAccName(event.target.value)}
            placeholder={t("form.namePlaceholder")}
            className="field-control"
          />
        )}
      </Field>

      {!editingPocket ? (
        <>
          <div className="field">
            <span className="flabel" id="account-type-label">{t("form.type")}</span>
            <RadioSegments
              labelledBy="account-type-label"
              options={typeOptions}
              value={accType}
              onChange={(nextType) => {
                setAccType(nextType);
                if (!editingAccount && !accColorChosen) setAccColor(defaultAccountTagColor(nextType));
              }}
            />
          </div>

          <div className="form-grid">
            <RoleField id="account-role" value={accRole} onChange={setAccRole} fixed={fixedRole(accType, false)} error={accErrors.role} />
            {editingAccount && accType !== "investment" && accType !== "credit" && editingAccount.type !== "credit" && !(editingAccount.children && editingAccount.children.length > 0) && parentCandidates.length > 0 && (
              <Field id="account-move-parent" label={t("form.moveUnder")} hint={t("form.moveUnderHint")}>
                {(aria) => (
                  <select {...aria} name="parent_id" value={accParentId} onChange={(event) => setAccParentId(event.target.value)} className="field-control">
                    <option value="">{t("form.stayTopLevel")}</option>
                    {parentCandidates.map((candidate) => (
                      <option key={candidate.id} value={candidate.id}>{candidate.name}</option>
                    ))}
                  </select>
                )}
              </Field>
            )}
          </div>

          {editingAccount && editingAccount.children && editingAccount.children.length > 0 && (
            <Field id="account-default-pocket" label={t("form.defaultPocket")} hint={t("form.defaultPocketHint")} error={accErrors.defaultPocket}>
              {(aria) => (
                <select {...aria} name="default_pocket_id" value={accDefaultPocketId} onChange={(event) => setAccDefaultPocketId(event.target.value)} className="field-control">
                  <option value="">{t("form.noDefaultPocket")}</option>
                  {editingAccount.children!.filter((child) => !child.is_archived).map((child) => (
                    <option key={child.id} value={child.id}>
                      {t("form.pocketOption", { name: child.name, amount: money(child.balance) })}
                    </option>
                  ))}
                </select>
              )}
            </Field>
          )}

          {accType === "investment" && (
            <>
              <div className="field">
                <span className="flabel" id="account-model-label">{t("form.model")}</span>
                <RadioSegments
                  labelledBy="account-model-label"
                  options={[
                    { value: "multi" as const, label: t("form.modelMulti") },
                    { value: "single" as const, label: t("form.modelSingle") },
                  ]}
                  value={accIsMultiInstrument ? "multi" : "single"}
                  onChange={(model) => setAccIsMultiInstrument(model === "multi")}
                />
                <p className="hint">{accIsMultiInstrument ? t("form.modelMultiHint") : t("form.modelSingleHint")}</p>
              </div>
              <Field id="account-default-funding" label={t("form.funding")} hint={t("form.fundingHint")}>
                {(aria) => (
                  <select {...aria} name="default_funding_account_id" value={accDefaultFundingId} onChange={(event) => setAccDefaultFundingId(event.target.value)} className="field-control">
                    <option value="">{t("form.noFunding")}</option>
                    <AccountSelectOptions
                      accounts={activeAccounts}
                      liquidOnly
                      allowParentSelection
                      excludeAccountId={editingAccount?.id}
                      formatBalance={money}
                    />
                  </select>
                )}
              </Field>
            </>
          )}
        </>
      ) : (
        <>
          <div className="field">
            <span className="flabel" id="account-pocket-kind-label">{t("form.pocketKind")}</span>
            <RadioSegments
              labelledBy="account-pocket-kind-label"
              options={pocketKindOptions}
              value={accIsMultiInstrument ? "cash" : "investment"}
              onChange={(kind) => setAccIsMultiInstrument(kind === "cash")}
            />
          </div>
          <RoleField id="account-role" value={accRole} onChange={setAccRole} fixed={accIsMultiInstrument ? null : "investasi"} error={accErrors.role} />
        </>
      )}

      {showInstrumentFields && (
        <>
          <div className="field">
            <span className="flabel" id="account-instrument-label">{t("form.instrumentType")}</span>
            <RadioSegments labelledBy="account-instrument-label" options={instrumentOptions} value={accInstrumentType as InstrumentType} onChange={setAccInstrumentType} />
          </div>
          {accInstrumentType !== "deposit" && (
            <TickerField
              id="account-ticker"
              value={accTicker}
              searching={isSearchingTicker}
              suggestions={tickerSuggestions}
              onSearch={(query) => handleSearchTicker(query, false)}
              onPick={(item) => {
                setAccTicker(item.symbol);
                if (!accName) setAccName(item.name);
                if (item.type) setAccInstrumentType(item.type);
                setTickerSuggestions([]);
              }}
            />
          )}
          {accInstrumentType !== "deposit" && editingAccount?.investment_tracking_mode !== "amount" && (
            <PositionFields
              idPrefix="account"
              instrument={accInstrumentType}
              units={accUnits}
              price={accAvgBuyPrice}
              onUnits={(value) => {
                setAccUnits(value);
                fillOpening(value, accAvgBuyPrice, setAccInitBal);
              }}
              onPrice={(value) => {
                setAccAvgBuyPrice(value);
                fillOpening(accUnits, value, setAccInitBal);
              }}
            />
          )}
        </>
      )}

      {isCreditForm && (
        <>
          <div className="form-grid">
            {!editingAccount && (
              <Field id="account-initial-balance" label={t("form.owedNow")} hint={t("form.owedNowHint")} error={accErrors.balance}>
                {(aria) => (
                  <input {...aria} name="initial_balance" type="text" inputMode="numeric" value={accInitBal}
                    onChange={(event) => setAccInitBal(formatNumberWithDots(event.target.value))} placeholder="0" className="field-control tabular-nums" />
                )}
              </Field>
            )}
            <Field id="account-credit-limit" label={t("form.limit")} hint={t("form.limitHint")} error={accErrors.limit}>
              {(aria) => (
                <input {...aria} name="credit_limit" type="text" inputMode="numeric" value={accCreditLimit}
                  onChange={(event) => setAccCreditLimit(formatNumberWithDots(event.target.value))}
                  placeholder={t("form.instrument.example", { value: format.integer(2_000_000) })} className="field-control tabular-nums" />
              )}
            </Field>
          </div>
          <div className="form-grid">
            <Field id="account-statement-day" label={t("form.statementDay")} error={accErrors.statementDay} describedBy="account-days-hint">
              {(aria) => (
                <input {...aria} name="statement_day" type="number" inputMode="numeric" min={1} max={31} step={1} value={accStatementDay}
                  onChange={(event) => setAccStatementDay(event.target.value)} placeholder={t("form.instrument.example", { value: format.integer(25) })} className="field-control tabular-nums" />
              )}
            </Field>
            <Field id="account-payment-day" label={t("form.paymentDay")} error={accErrors.paymentDay} describedBy="account-days-hint">
              {(aria) => (
                <input {...aria} name="payment_day" type="number" inputMode="numeric" min={1} max={31} step={1} value={accPaymentDay}
                  onChange={(event) => setAccPaymentDay(event.target.value)} placeholder={t("form.instrument.example", { value: format.integer(5) })} className="field-control tabular-nums" />
              )}
            </Field>
            <p className="hint full-row" id="account-days-hint">{t("form.daysHint")}</p>
          </div>
        </>
      )}

      {!editingAccount && accType !== "credit" && !(accType === "investment" && accIsMultiInstrument) && (
        <Field
          id="account-initial-balance"
          label={accType === "investment" ? t(`form.instrument.${instrumentCopyKey(accInstrumentType)}.total`) : t("form.opening")}
          hint={accType === "investment" && accInstrumentType === "deposit"
            ? t("form.openingDepositHint")
            : accType === "investment"
              ? t("form.openingPositionHint")
              : t("form.openingHint")}
          error={accErrors.balance}
        >
          {(aria) => (
            <input {...aria} name="initial_balance" type="text" inputMode="numeric" value={accInitBal}
              onChange={(event) => setAccInitBal(formatNumberWithDots(event.target.value))} placeholder="0" className="field-control tabular-nums" />
          )}
        </Field>
      )}

      {!editingAccount && accType === "investment" && accIsMultiInstrument && <p className="hint">{t("form.platformHint")}</p>}

      {!editingPocket && <ColourField value={accColor} onChange={(hex) => { setAccColor(hex); setAccColorChosen(true); }} />}
    </form>
  );

  const pocketForm = pocketParentAccount && (
    <form id="pocket-form" className="form-stack" onSubmit={submitPocket} noValidate>
      {pocketFormError && <p role="alert" className="form-alert">{pocketFormError}</p>}
      <Field id="pocket-name" label={t("pocketForm.name")} error={pocketErrors.name}>
        {(aria) => (
          <input
            {...aria}
            name="name"
            type="text"
            required
            value={pocketName}
            onChange={(event) => setPocketName(event.target.value)}
            placeholder={pocketParentAccount.type === "investment" ? t("pocketForm.placeholderInvestment") : t("pocketForm.placeholder")}
            className="field-control"
          />
        )}
      </Field>
      <div className="field">
        <span className="flabel" id="pocket-kind-label">{t("form.pocketKind")}</span>
        <RadioSegments
          labelledBy="pocket-kind-label"
          options={pocketKindOptions}
          value={pocketIsInvestment ? "investment" : "cash"}
          onChange={(kind) => setPocketIsInvestment(kind === "investment")}
        />
        <p className="hint">{pocketIsInvestment ? t("pocketForm.investmentHint") : t("pocketForm.cashHint")}</p>
      </div>
      <RoleField id="pocket-role" value={pocketRole} onChange={setPocketRole} fixed={pocketIsInvestment ? "investasi" : null} error={pocketErrors.role} />
      {pocketIsInvestment && (
        <>
          <div className="field">
            <span className="flabel" id="pocket-instrument-label">{t("form.instrumentType")}</span>
            <RadioSegments labelledBy="pocket-instrument-label" options={instrumentOptions} value={pocketInstrumentType as InstrumentType} onChange={setPocketInstrumentType} />
          </div>
          {pocketInstrumentType !== "deposit" && (
            <>
              <TickerField
                id="pocket-ticker"
                value={pocketTicker}
                searching={pocketIsSearchingTicker}
                suggestions={pocketTickerSuggestions}
                onSearch={(query) => handleSearchTicker(query, true)}
                onPick={(item) => {
                  setPocketTicker(item.symbol);
                  if (!pocketName) setPocketName(item.name);
                  if (item.type) setPocketInstrumentType(item.type);
                  setPocketTickerSuggestions([]);
                }}
              />
              <PositionFields
                idPrefix="pocket"
                instrument={pocketInstrumentType}
                units={pocketUnits}
                price={pocketAvgBuyPrice}
                onUnits={(value) => {
                  setPocketUnits(value);
                  fillOpening(value, pocketAvgBuyPrice, setPocketInitBal);
                }}
                onPrice={(value) => {
                  setPocketAvgBuyPrice(value);
                  fillOpening(pocketUnits, value, setPocketInitBal);
                }}
              />
            </>
          )}
        </>
      )}
      <Field
        id="pocket-initial-balance"
        label={pocketIsInvestment ? t(`form.instrument.${instrumentCopyKey(pocketInstrumentType)}.total`) : t("pocketForm.opening")}
        hint={pocketIsInvestment && pocketInstrumentType === "deposit"
          ? t("form.openingDepositHint")
          : pocketIsInvestment
            ? t("pocketForm.openingPositionHint")
            : t("pocketForm.openingHint")}
        error={pocketErrors.balance}
      >
        {(aria) => (
          <input {...aria} name="initial_balance" type="text" inputMode="numeric" value={pocketInitBal}
            onChange={(event) => setPocketInitBal(formatNumberWithDots(event.target.value))} placeholder="0" className="field-control tabular-nums" />
        )}
      </Field>
    </form>
  );

  const valuationGain = (() => {
    if (!valCostBasisStr.trim()) return null;
    const cost = parseNumberFromDots(valCostBasisStr);
    return { amount: parseNumberFromDots(valCurrentBalStr) - cost, cost };
  })();

  const reconcileDifference = reconcileAccount && actualBalStr.trim() ? parseNumberFromDots(actualBalStr) - reconcileAccount.balance : null;

  const dialogs = (
    <>
      <ShellDialog
        open={accountModalOpen}
        onClose={closeAccountForm}
        title={accountTitle}
        meta={editingAccount ? editingAccount.name : t("form.subtitleNew")}
        width={560}
        closeLabel={tc("close")}
        footer={accountFooter}
      >
        {accountForm}
      </ShellDialog>

      <ShellDialog
        open={pocketModalOpen && Boolean(pocketParentAccount)}
        onClose={closePocketForm}
        title={t("pocketForm.title", { parent: pocketParentAccount?.name ?? "" })}
        width={560}
        closeLabel={tc("close")}
        footer={(
          <>
            <button type="button" className="ws-btn" onClick={closePocketForm}>{t("form.cancel")}</button>
            <button type="submit" form="pocket-form" className="ws-btn ws-btn-ink" disabled={savePocketMutation.isPending}>
              {savePocketMutation.isPending ? t("form.saving") : t("pocketForm.create")}
            </button>
          </>
        )}
      >
        {pocketForm}
      </ShellDialog>

      <ShellDialog
        open={Boolean(valuationAccount)}
        onClose={closeValuation}
        title={t("valuation.title")}
        meta={valuationAccount?.name}
        width={520}
        closeLabel={tc("close")}
        footer={(
          <>
            <button type="button" className="ws-btn" onClick={closeValuation}>{t("form.cancel")}</button>
            <button type="submit" form="valuation-form" className="ws-btn ws-btn-ink" disabled={valuationMutation.isPending}>
              {valuationMutation.isPending ? t("form.saving") : t("valuation.save")}
            </button>
          </>
        )}
      >
        {valuationAccount && (
          <form
            id="valuation-form"
            className="form-stack"
            noValidate
            onSubmit={(event) => {
              event.preventDefault();
              if (!valuationMutation.isPending) valuationMutation.mutate();
            }}
          >
            {valError && <p role="alert" className="form-alert">{valError}</p>}
            <p className="t-body muted">{t("valuation.intro")}</p>
            <dl className="dl">
              <dt>{t("valuation.recorded")}</dt>
              <dd className="tabular-nums"><MaskedMoney text={money(valuationAccount.balance)} /></dd>
              {valuationAccount.units && valuationAccount.avg_buy_price ? (
                <>
                  <dt>{t("valuation.holding")}</dt>
                  <dd className="tabular-nums">
                    {t("valuation.holdingValue", {
                      units: format.decimal(valuationAccount.units, 4),
                      unit: t(`form.instrument.${instrumentCopyKey(valuationAccount.instrument_type)}.unit`),
                      price: format.money(valuationAccount.avg_buy_price),
                    })}
                  </dd>
                </>
              ) : null}
            </dl>
            <Field id="valuation-current-value" label={t("valuation.value")} hint={t("valuation.valueHint")}>
              {(aria) => (
                <input {...aria} name="current_balance" type="text" inputMode="numeric" value={valCurrentBalStr}
                  onChange={(event) => setValCurrentBalStr(formatNumberWithDots(event.target.value))}
                  placeholder={t("form.instrument.example", { value: format.integer(10_450_000) })} className="field-control tabular-nums" />
              )}
            </Field>
            <Field id="valuation-cost-basis" label={t("valuation.cost")} hint={t("valuation.costHint")}>
              {(aria) => (
                <input {...aria} name="cost_basis" type="text" inputMode="numeric" value={valCostBasisStr}
                  onChange={(event) => setValCostBasisStr(formatNumberWithDots(event.target.value))}
                  placeholder={t("form.instrument.example", { value: format.integer(10_000_000) })} className="field-control tabular-nums" />
              )}
            </Field>
            <div className="box" aria-live="polite">
              <span className="grow t-body">{t("valuation.gainPreview")}</span>
              {valuationGain ? <GainText gain={valuationGain} /> : <span className="t-meta">{t("valuation.gainUnavailable")}</span>}
            </div>
            <Field id="valuation-notes" label={t("valuation.notes")}>
              {(aria) => (
                <input {...aria} name="notes" type="text" value={valNotes} onChange={(event) => setValNotes(event.target.value)}
                  placeholder={t("valuation.notesPlaceholder")} className="field-control" />
              )}
            </Field>
          </form>
        )}
      </ShellDialog>

      <ShellDialog
        open={Boolean(reconcileAccount)}
        onClose={closeReconcile}
        title={t("adjust.title")}
        meta={reconcileAccount?.name}
        width={520}
        closeLabel={tc("close")}
        footer={(
          <>
            <button type="button" className="ws-btn" onClick={closeReconcile}>{t("form.cancel")}</button>
            <button type="submit" form="reconcile-form" className="ws-btn ws-btn-ink" disabled={reconcileMutation.isPending}>
              {reconcileMutation.isPending ? t("adjust.saving") : t("adjust.save")}
            </button>
          </>
        )}
      >
        {reconcileAccount && (
          <form
            id="reconcile-form"
            className="form-stack"
            noValidate
            onSubmit={(event) => {
              event.preventDefault();
              if (!reconcileMutation.isPending) reconcileMutation.mutate();
            }}
          >
            <p className="t-body muted">{t("adjust.intro")}</p>
            <dl className="dl">
              <dt>{t("adjust.recorded")}</dt>
              <dd className="tabular-nums"><MaskedMoney text={money(reconcileAccount.balance)} /></dd>
            </dl>
            <Field id="reconcile-actual-balance" label={t("adjust.actual")} error={reconcileError || undefined}>
              {(aria) => (
                <input {...aria} name="actual_balance" type="text" inputMode="numeric" value={actualBalStr}
                  onChange={(event) => { setActualBalStr(formatNumberWithDots(event.target.value)); setReconcileError(""); }}
                  placeholder={t("form.instrument.example", { value: format.integer(10_000_000) })} className="field-control tabular-nums" />
              )}
            </Field>
            {reconcileDifference !== null && (
              <div className="box" aria-live="polite">
                <span className="grow t-body">{t("adjust.difference")}</span>
                {reconcileDifference === 0 ? (
                  <span className="t-body">{t("adjust.matches")}</span>
                ) : (
                  <span className={cn("t-body tabular-nums", reconcileDifference > 0 ? "c-income" : "c-expense")}>
                    <MaskedMoney text={`${reconcileDifference > 0 ? "+" : "−"}${money(Math.abs(reconcileDifference))}`} />
                    {" · "}
                    {reconcileDifference > 0 ? t("adjust.in") : t("adjust.out")}
                  </span>
                )}
              </div>
            )}
            <Field id="reconcile-notes" label={t("adjust.notes")}>
              {(aria) => (
                <input {...aria} name="notes" type="text" value={reconcileNotes} onChange={(event) => setReconcileNotes(event.target.value)}
                  placeholder={t("adjust.notesPlaceholder")} className="field-control" />
              )}
            </Field>
          </form>
        )}
      </ShellDialog>

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
  };
}
