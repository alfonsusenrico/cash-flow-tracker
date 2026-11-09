"use client";
import { useState, useEffect, useCallback, useMemo } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { api, ApiError } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { useFormat } from "@/lib/format";
import { parseNumberFromDots } from "@/lib/utils";

interface AccountItem {
  id: string;
  parent_id?: string | null;
  default_funding_account_id?: string | null;
  default_funding_account_name?: string | null;
  name: string;
  type: "cash" | "bank" | "wallet" | "investment" | "credit";
  instrument_type?: "stock" | "mutual_fund" | "gold" | "crypto" | "deposit" | "other" | null;
  instrument_symbol?: string | null;
  units?: number | null;
  avg_buy_price?: number | null;
  last_price?: number | null;
  balance: number;
  is_archived?: boolean;
  children?: AccountItem[];
}

interface Props {
  open: boolean;
  onClose: () => void;
  /** The investment pocket (or single-instrument account) being traded */
  pocket: AccountItem | null;
  /** All accounts, used to populate funding account selector */
  allAccounts: AccountItem[];
  /** The trade the caller opened: the product detail offers "Beli" and "Jual" separately (apply-markas-rekening D10). */
  defaultAction?: "buy" | "sell";
}

type Instrument = "gold" | "fund" | "crypto" | "stock";

/** The unit an instrument is traded in; a stock is entered in lots of 100 shares. */
function instrumentOf(instType?: string | null): { kind: Instrument; step: string; isLotBased: boolean; noteUnit: string } {
  switch (instType) {
    case "gold":
      return { kind: "gold", step: "0.001", isLotBased: false, noteUnit: "gram" };
    case "mutual_fund":
      return { kind: "fund", step: "0.0001", isLotBased: false, noteUnit: "UP" };
    case "crypto":
      return { kind: "crypto", step: "0.000001", isLotBased: false, noteUnit: "token" };
    case "stock":
    default:
      return { kind: "stock", step: "1", isLotBased: true, noteUnit: "lot" };
  }
}

const FORM_ID = "investment-trade-form";

/**
 * Beli and Jual (apply-markas-rekening D10): a 520 px shell dialog with the units and price, the funding account,
 * the note, and the "Perkiraan total" box with the position after the trade. The trade request is unchanged.
 */
export function InvestmentTradeModal({ open, onClose, pocket, allAccounts, defaultAction = "buy" }: Props) {
  const qc = useQueryClient();
  const t = useTranslations("accounts.trade");
  const common = useTranslations("common");
  const format = useFormat();

  const action = defaultAction;
  const [fundingAccountId, setFundingAccountId] = useState("");
  const [inputUnits, setInputUnits] = useState(""); // lots for stock, raw units otherwise
  const [inputPrice, setInputPrice] = useState(""); // price per lembar (stock) or per unit
  const [notes, setNotes] = useState("");
  const [error, setError] = useState("");
  const [errorField, setErrorField] = useState<"units" | "funding" | null>(null);

  const cfg = instrumentOf(pocket?.instrument_type);

  // Derived: actual units to trade (lots × 100 for stocks)
  const parsedInputUnits = parseFloat(inputUnits.replace(/[^0-9.]/g, "")) || 0;
  const parsedPrice = parseNumberFromDots(inputPrice) || 0;
  const tradeUnits = cfg.isLotBased ? parsedInputUnits * 100 : parsedInputUnits;
  const tradeAmount = Math.round(tradeUnits * parsedPrice);

  // Current position
  const currentUnits = pocket?.units ?? 0;
  const currentAvg = pocket?.avg_buy_price ?? 0;
  const exceedsPosition = action === "sell" && tradeUnits > currentUnits;

  // Projected position preview
  const projectedUnits =
    action === "buy" ? currentUnits + tradeUnits : Math.max(0, currentUnits - tradeUnits);
  const projectedAvg =
    action === "buy" && tradeUnits > 0 && parsedPrice > 0
      ? Math.round((currentUnits * currentAvg + tradeUnits * parsedPrice) / (currentUnits + tradeUnits))
      : currentAvg; // avg doesn't change on sell
  // Candidate funding accounts: all non-investment, non-pocket accounts
  const fundingCandidates = useMemo(
    () => listLiquidAccountChoices(allAccounts)
      .filter((account) => !account.is_archived && account.id !== pocket?.id),
    [allAccounts, pocket?.id],
  );
  const selectedFundingAccount = fundingCandidates.find((account) => account.id === fundingAccountId);
  const projectedFundingBalance = selectedFundingAccount
    ? selectedFundingAccount.balance + (action === "buy" ? -tradeAmount : tradeAmount)
    : null;
  const insufficientDisplayedBalance = action === "buy" && projectedFundingBalance !== null && projectedFundingBalance < 0;

  // Reset on open
  useEffect(() => {
    if (!open) return;
    setInputUnits("");
    setInputPrice("");
    setNotes("");
    setError("");
    setErrorField(null);

    // Pre-fill funding account from pocket's parent's default_funding_account_id
    const parentId = pocket?.parent_id;
    const parent = parentId ? allAccounts.find((a) => a.id === parentId) : null;
    const preferredFunding = parent?.default_funding_account_id ?? "";
    if (fundingCandidates.some((account) => account.id === preferredFunding)) {
      setFundingAccountId(preferredFunding);
    } else {
      setFundingAccountId(fundingCandidates[0]?.id ?? "");
    }
  }, [open, defaultAction, pocket, allAccounts, fundingCandidates]);

  const handleClose = useCallback(() => {
    setError("");
    setErrorField(null);
    onClose();
  }, [onClose]);

  const tradeMutation = useMutation({
    mutationFn: async () => {
      if (!pocket) throw new Error(t("errors.noProduct"));
      if (tradeUnits <= 0) throw new Error(t("errors.units"));
      if (action === "sell" && tradeUnits > currentUnits) throw new Error(t("tooMany"));
      if (parsedPrice <= 0) throw new Error(t("errors.price"));
      if (!fundingAccountId) throw new Error(t("errors.funding"));
      if (tradeAmount <= 0) throw new Error(t("errors.amount"));

      // Buy: money flows FROM funding account TO investment pocket
      // Sell: money flows FROM investment pocket TO funding account
      const accountId = action === "buy" ? fundingAccountId : pocket.id;
      const targetAccountId = action === "buy" ? pocket.id : fundingAccountId;
      // The generated note is stored data, written as it always was ("BBCA 2 lot @ Rp 9.100").
      const generatedNote = [
        pocket.instrument_symbol ?? pocket.name,
        String(cfg.isLotBased ? parsedInputUnits : tradeUnits),
        cfg.noteUnit,
        "@",
        "Rp",
        parsedPrice.toLocaleString("id-ID"),
      ].join(" ");

      return api.post("/transactions", {
        type: action === "buy" ? "expense" : "income",
        account_id: accountId,
        target_account_id: targetAccountId,
        amount: tradeAmount,
        investment_action: action,
        units: tradeUnits,
        price_per_unit: parsedPrice,
        notes: notes.trim() || generatedNote,
        date: new Date().toISOString(),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      handleClose();
    },
    onError: (err: Error) => {
      const code = err instanceof ApiError && typeof err.detail !== "string" ? err.detail.code : null;
      const field = code === "insufficient_units" ? "units" : code === "insufficient_funds" ? "funding" : null;
      setErrorField(field);
      setError(err.message || t("errors.failed"));
      if (field) document.getElementById(`investment-trade-${field}`)?.focus();
    },
  });

  if (!pocket) return null;

  const amount = (value: number) => <MaskedMoney text={format.money(value)} />;
  const unitsText = (units: number) =>
    cfg.isLotBased
      ? t("sharesAndLots", { shares: format.decimal(units, 4), lots: format.decimal(units / 100, 4) })
      : t(`unitsOf.${cfg.kind}`, { units: format.decimal(units, 6) });
  const busy = tradeMutation.isPending;
  const unitsInvalid = exceedsPosition || errorField === "units";

  const footer = (
    <>
      <button type="button" className="ws-btn" onClick={handleClose}>{common("cancel")}</button>
      <button
        type="submit"
        form={FORM_ID}
        className="ws-btn ws-btn-ink"
        disabled={busy || exceedsPosition || insufficientDisplayedBalance}
      >
        {busy ? t("processing") : action === "buy" ? t("buy") : t("sell")}
      </button>
    </>
  );

  return (
    <UntranslatedRegion active={false}>
      <ShellDialog
        open={open}
        onClose={handleClose}
        title={action === "buy" ? t("buy") : t("sell")}
        meta={pocket.instrument_symbol ? `${pocket.name} · ${pocket.instrument_symbol}` : pocket.name}
        width={520}
        closeLabel={common("close")}
        footer={footer}
      >
        <form
          id={FORM_ID}
          className="form-stack"
          noValidate
          onSubmit={(event) => {
            event.preventDefault();
            tradeMutation.mutate();
          }}
        >
          {currentUnits > 0 && (
            <dl className="dl" aria-label={t("position")}>
              <dt>{t("owned")}</dt>
              <dd className="tabular-nums">{unitsText(currentUnits)}</dd>
              <dt>{t("avgPrice")}</dt>
              <dd className="tabular-nums">{amount(currentAvg)}</dd>
              {pocket.last_price ? (
                <>
                  <dt>{t("marketPrice")}</dt>
                  <dd className="tabular-nums">{amount(pocket.last_price)}</dd>
                </>
              ) : null}
            </dl>
          )}

          <div className="form-grid">
            <div className="field">
              <label htmlFor="investment-trade-units">{t(`unitLabel.${cfg.kind}`)}</label>
              <input
                id="investment-trade-units"
                name="units"
                type="number"
                min="0"
                max={action === "sell" ? cfg.isLotBased ? currentUnits / 100 : currentUnits : undefined}
                step={cfg.step}
                value={inputUnits}
                onChange={(e) => { setInputUnits(e.target.value); setErrorField(null); setError(""); }}
                aria-invalid={unitsInvalid || undefined}
                aria-describedby={unitsInvalid ? "investment-trade-units-error" : cfg.isLotBased && parsedInputUnits > 0 ? "investment-trade-units-hint" : undefined}
                className="field-control tabular-nums"
              />
              {unitsInvalid && (
                <p id="investment-trade-units-error" className="err" role="alert">
                  {exceedsPosition ? t("tooMany") : error}
                </p>
              )}
              {cfg.isLotBased && parsedInputUnits > 0 && !unitsInvalid && (
                <p id="investment-trade-units-hint" className="hint">
                  {t("sharesHint", { shares: format.decimal(parsedInputUnits * 100, 4) })}
                </p>
              )}
            </div>
            <div className="field">
              <label htmlFor="investment-trade-price">{t(`priceLabel.${cfg.kind}`)}</label>
              <input
                id="investment-trade-price"
                name="price_per_unit"
                type="text"
                inputMode="numeric"
                value={inputPrice}
                onChange={(e) => {
                  const raw = e.target.value.replace(/[^0-9]/g, "");
                  setInputPrice(raw ? parseInt(raw, 10).toLocaleString("id-ID") : "");
                }}
                className="field-control tabular-nums"
              />
              {(pocket.last_price || pocket.avg_buy_price) && (
                <p className="hint">
                  {t("quickFill")}{" "}
                  {pocket.last_price ? (
                    <button
                      type="button"
                      className="ws-linkbtn"
                      onClick={() => setInputPrice(Math.round(pocket.last_price!).toLocaleString("id-ID"))}
                    >
                      {t("market", { price: format.money(pocket.last_price) })}
                    </button>
                  ) : null}
                  {pocket.last_price && pocket.avg_buy_price ? " · " : null}
                  {pocket.avg_buy_price ? (
                    <button
                      type="button"
                      className="ws-linkbtn"
                      onClick={() => setInputPrice(Math.round(pocket.avg_buy_price!).toLocaleString("id-ID"))}
                    >
                      {t("cost", { price: format.money(pocket.avg_buy_price) })}
                    </button>
                  ) : null}
                </p>
              )}
            </div>
          </div>

          <div className="field">
            <label htmlFor="investment-trade-funding">{action === "buy" ? t("from") : t("into")}</label>
            <select
              id="investment-trade-funding"
              name="funding_account_id"
              value={fundingAccountId}
              onChange={(e) => { setFundingAccountId(e.target.value); setErrorField(null); setError(""); }}
              aria-invalid={errorField === "funding" || undefined}
              aria-describedby={errorField === "funding" ? "investment-trade-funding-error" : undefined}
              className="field-control"
            >
              <option value="">{t("chooseAccount")}</option>
              {fundingCandidates.map((account) => (
                <option key={account.id} value={account.id}>{account.name}</option>
              ))}
            </select>
            {errorField === "funding" && <p id="investment-trade-funding-error" role="alert" className="err">{error}</p>}
          </div>

          {tradeUnits > 0 && parsedPrice > 0 && (
            <section className="box" aria-labelledby="investment-trade-estimate">
              <div className="grow">
                <h3 id="investment-trade-estimate" className="t-label">{t("estimate")}</h3>
                <p className="t-fig">{amount(tradeAmount)}</p>
                <p className="t-body">
                  {action === "buy"
                    ? t.rich("buyEffect", { amount: () => amount(tradeAmount), account: selectedFundingAccount?.name ?? t("sourceAccount") })
                    : t.rich("sellEffect", { units: format.decimal(tradeUnits, 6), amount: () => amount(tradeAmount), account: selectedFundingAccount?.name ?? t("targetAccount") })}
                </p>
                <dl className="dl">
                  <dt>{t("unitsAfter")}</dt>
                  <dd className="tabular-nums">{unitsText(projectedUnits)}</dd>
                  {selectedFundingAccount && projectedFundingBalance !== null && (
                    <>
                      <dt>{t("balanceAfter")}</dt>
                      <dd className={insufficientDisplayedBalance ? "tabular-nums c-expense" : "tabular-nums"}>{amount(projectedFundingBalance)}</dd>
                    </>
                  )}
                  {action === "buy" && (
                    <>
                      <dt>{t("newAvg")}</dt>
                      <dd className="tabular-nums">{amount(projectedAvg)}</dd>
                    </>
                  )}
                </dl>
                {action === "sell" && tradeUnits === currentUnits && <p className="t-meta">{t("sellAll")}</p>}
                {insufficientDisplayedBalance && <p role="alert" className="err">{t("insufficient")}</p>}
              </div>
            </section>
          )}

          <div className="field">
            <label htmlFor="investment-trade-notes">{t("note")}</label>
            <input
              id="investment-trade-notes"
              name="notes"
              type="text"
              placeholder={t("notePlaceholder")}
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              className="field-control"
            />
          </div>

          {error && !errorField && <p role="alert" className="form-alert">{error}</p>}
        </form>
      </ShellDialog>
    </UntranslatedRegion>
  );
}
