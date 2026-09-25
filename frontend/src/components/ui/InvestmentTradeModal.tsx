"use client";
import { useState, useEffect, useCallback, useMemo } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { Modal } from "@/components/ui/Modal";
import { fmtMoney, parseNumberFromDots } from "@/lib/utils";

interface AccountItem {
  id: string;
  parent_id?: string | null;
  default_funding_account_id?: string | null;
  default_funding_account_name?: string | null;
  name: string;
  type: "cash" | "bank" | "wallet" | "investment";
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
  /** Pre-selected action: "buy" or "sell" */
  defaultAction?: "buy" | "sell";
}

// Instrument-specific field config
function getInstrumentConfig(instType?: string | null) {
  switch (instType) {
    case "gold":
      return {
        unitLabel: "Berat (Gram)",
        unitPlaceholder: "Contoh: 0.5 atau 10",
        unitStep: "0.001",
        unitUnit: "gram",
        priceLabel: "Harga / Gram (IDR)",
        isLotBased: false,
      };
    case "mutual_fund":
      return {
        unitLabel: "Unit Penyertaan (UP)",
        unitPlaceholder: "Contoh: 2871.45",
        unitStep: "0.0001",
        unitUnit: "UP",
        priceLabel: "NAB / Harga per Unit (IDR)",
        isLotBased: false,
      };
    case "crypto":
      return {
        unitLabel: "Jumlah Token / Koin",
        unitPlaceholder: "Contoh: 0.05",
        unitStep: "0.000001",
        unitUnit: "token",
        priceLabel: "Harga / Koin (IDR)",
        isLotBased: false,
      };
    case "stock":
    default:
      return {
        unitLabel: "Jumlah Lot",
        unitPlaceholder: "Contoh: 5",
        unitStep: "1",
        unitUnit: "lot",
        priceLabel: "Harga / Lembar (IDR)",
        isLotBased: true,
      };
  }
}

function cn(...classes: (string | boolean | undefined | null)[]) {
  return classes.filter(Boolean).join(" ");
}

export function InvestmentTradeModal({ open, onClose, pocket, allAccounts, defaultAction = "buy" }: Props) {
  const qc = useQueryClient();

  const [action, setAction] = useState<"buy" | "sell">(defaultAction);
  const [fundingAccountId, setFundingAccountId] = useState("");
  const [inputUnits, setInputUnits] = useState(""); // lots for stock, raw units otherwise
  const [inputPrice, setInputPrice] = useState(""); // price per lembar (stock) or per unit
  const [notes, setNotes] = useState("");
  const [error, setError] = useState("");
  const [errorField, setErrorField] = useState<"units" | "funding" | null>(null);

  const cfg = getInstrumentConfig(pocket?.instrument_type);

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
      : action === "sell"
      ? currentAvg // avg doesn't change on sell
      : currentAvg;
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
    setAction(defaultAction);
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

  // Recalculate when fundingCandidates changes (avoid stale closure)
  const handleClose = useCallback(() => {
    setError("");
    setErrorField(null);
    onClose();
  }, [onClose]);

  const tradeMutation = useMutation({
    mutationFn: async () => {
      if (!pocket) throw new Error("Tidak ada instrumen yang dipilih");
      if (tradeUnits <= 0) throw new Error("Masukkan jumlah unit yang valid");
      if (action === "sell" && tradeUnits > currentUnits) {
        throw new Error("Jumlah jual melebihi unit yang dimiliki");
      }
      if (parsedPrice <= 0) throw new Error("Masukkan harga per unit yang valid");
      if (!fundingAccountId) throw new Error("Pilih rekening sumber / tujuan dana");
      if (tradeAmount <= 0) throw new Error("Nominal transaksi tidak valid");

      // Buy: money flows FROM funding account TO investment pocket
      // Sell: money flows FROM investment pocket TO funding account
      const accountId = action === "buy" ? fundingAccountId : pocket.id;
      const targetAccountId = action === "buy" ? pocket.id : fundingAccountId;

      return api.post("/transactions", {
        type: action === "buy" ? "expense" : "income",
        account_id: accountId,
        target_account_id: targetAccountId,
        amount: tradeAmount,
        investment_action: action,
        units: tradeUnits,
        price_per_unit: parsedPrice,
        notes:
          notes.trim() ||
          (cfg.isLotBased
            ? `${pocket.instrument_symbol ?? pocket.name} ${parsedInputUnits} lot @ Rp ${parsedPrice.toLocaleString("id-ID")}`
            : `${pocket.instrument_symbol ?? pocket.name} ${tradeUnits} ${cfg.unitUnit} @ Rp ${parsedPrice.toLocaleString("id-ID")}`),
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
      setError(err.message || "Transaksi gagal");
      if (field) document.getElementById(`investment-trade-${field}`)?.focus();
    },
  });

  if (!pocket) return null;

  const fundingLabel = action === "buy" ? "Dana dari Rekening" : "Dana masuk ke Rekening";
  const submitLabel = action === "buy" ? "Catat Pembelian" : "Catat Penjualan";

  return (
    <Modal
      open={open}
      onClose={handleClose}
      title={`Transaksi Investasi — ${pocket.instrument_symbol ?? pocket.name}`}
    >
      <form
        className="space-y-4"
        onSubmit={(event) => {
          event.preventDefault();
          tradeMutation.mutate();
        }}
      >
        <div className="flex rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-1" role="group" aria-label="Aksi investasi">
          {(["buy", "sell"] as const).map((act) => (
            <button
              key={act}
              type="button"
              onClick={() => setAction(act)}
              aria-pressed={action === act}
              className={cn(
                  "min-h-11 flex-1 rounded-lg px-3 text-sm font-semibold transition-colors motion-reduce:transition-none",
                  action === act
                  ? "bg-[var(--text)] text-[var(--surface)] shadow-sm"
                  : "text-[var(--muted)] hover:text-[var(--text)]"
              )}
            >
              {act === "buy" ? "Beli" : "Jual"}
            </button>
          ))}
        </div>

        {/* Instrument & Symbol badge */}
        <div className="flex items-center gap-2 px-1">
          <span className="text-[11px] text-[var(--muted)] uppercase font-medium">Instrumen:</span>
          <span className="px-2 py-0.5 rounded-full text-xs font-bold bg-violet-500/15 text-violet-600 dark:text-violet-300">
            {pocket.instrument_type ?? "investasi"}
          </span>
          {pocket.instrument_symbol && (
            <span className="px-2 py-0.5 rounded-full text-xs font-bold bg-[var(--surface-raised)] border border-[var(--border)] text-[var(--text)]">
              {pocket.instrument_symbol}
            </span>
          )}
        </div>

        {/* Current position */}
        {currentUnits > 0 && (
          <div className="px-3 py-2.5 rounded-lg bg-[var(--surface-raised)] border border-[var(--border)] text-xs space-y-0.5">
            <div className="font-semibold text-[var(--muted)] uppercase tracking-wider mb-1">Posisi Saat Ini</div>
            <div className="flex justify-between">
              <span className="text-[var(--muted)]">Unit dimiliki</span>
              <span className="font-bold tabular">
                {cfg.isLotBased
                  ? `${currentUnits} lembar (${currentUnits / 100} lot)`
                  : `${currentUnits} ${cfg.unitUnit}`}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-[var(--muted)]">Avg. harga beli (Modal)</span>
              <span className="font-bold tabular">{fmtMoney(currentAvg)}</span>
            </div>
            {pocket.last_price && (
              <div className="flex justify-between">
                <span className="text-[var(--muted)]">Harga pasar terkini</span>
                <span className="font-bold tabular">{fmtMoney(pocket.last_price)}</span>
              </div>
            )}
          </div>
        )}

        {/* Unit input */}
        <div>
          <label htmlFor="investment-trade-units" className="block text-xs font-medium text-[var(--muted)] mb-1">{cfg.unitLabel}</label>
          <input
            id="investment-trade-units"
            name="units"
            type="number"
            min="0"
            max={action === "sell" ? cfg.isLotBased ? currentUnits / 100 : currentUnits : undefined}
            step={cfg.unitStep}
            placeholder={cfg.unitPlaceholder}
            value={inputUnits}
            onChange={(e) => { setInputUnits(e.target.value); setErrorField(null); setError(""); }}
            aria-invalid={exceedsPosition || errorField === "units" || undefined}
            aria-describedby={exceedsPosition || errorField === "units" ? "investment-trade-units-error" : undefined}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          />
          {(exceedsPosition || errorField === "units") && (
            <p id="investment-trade-units-error" className="mt-1 text-[11px] font-medium text-rose-600" role="alert">
              {exceedsPosition ? "Jumlah jual melebihi unit yang dimiliki." : error}
            </p>
          )}
          {cfg.isLotBased && parsedInputUnits > 0 && (
            <p className="text-[10px] text-[var(--muted)] mt-1">
              = {(parsedInputUnits * 100).toLocaleString("id-ID")} lembar
            </p>
          )}
        </div>

        {/* Price input */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <label htmlFor="investment-trade-price" className="block text-xs font-medium text-[var(--muted)]">{cfg.priceLabel}</label>
            {(pocket.last_price || pocket.avg_buy_price) && (
              <div className="flex items-center gap-1.5 text-[10px]">
                <span className="text-[var(--muted)]">Isi cepat:</span>
                {pocket.last_price && (
                  <button
                    type="button"
                    onClick={() => setInputPrice(Math.round(pocket.last_price!).toLocaleString("id-ID"))}
                    className="px-1.5 py-0.5 rounded bg-[var(--surface-raised)] hover:bg-[var(--border)] border border-[var(--border)] text-emerald-600 dark:text-emerald-400 font-semibold transition-colors motion-reduce:transition-none"
                    title="Gunakan harga pasar terkini"
                  >
                    Pasar ({fmtMoney(pocket.last_price)})
                  </button>
                )}
                {pocket.avg_buy_price && (
                  <button
                    type="button"
                    onClick={() => setInputPrice(Math.round(pocket.avg_buy_price!).toLocaleString("id-ID"))}
                    className="px-1.5 py-0.5 rounded bg-[var(--surface-raised)] hover:bg-[var(--border)] border border-[var(--border)] text-[var(--text)] font-semibold transition-colors motion-reduce:transition-none"
                    title="Gunakan harga modal rata-rata"
                  >
                    Modal ({fmtMoney(pocket.avg_buy_price)})
                  </button>
                )}
              </div>
            )}
          </div>
          <input
            id="investment-trade-price"
            name="price_per_unit"
            type="text"
            inputMode="numeric"
            placeholder={
              pocket.last_price
                ? `Harga pasar: ${pocket.last_price.toLocaleString("id-ID")}`
                : "Contoh: 3.340"
            }
            value={inputPrice}
            onChange={(e) => {
              // Format with dots as user types
              const raw = e.target.value.replace(/[^0-9]/g, "");
              setInputPrice(raw ? parseInt(raw, 10).toLocaleString("id-ID") : "");
            }}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          />
        </div>

        <div>
          <label htmlFor="investment-trade-funding" className="mb-1 block text-sm font-medium text-[var(--text)]">{fundingLabel}</label>
          <select
            id="investment-trade-funding"
            name="funding_account_id"
            value={fundingAccountId}
            onChange={(e) => { setFundingAccountId(e.target.value); setErrorField(null); setError(""); }}
            aria-invalid={errorField === "funding" || undefined}
            aria-describedby={errorField === "funding" ? "investment-trade-funding-error" : undefined}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          >
            <option value="">Pilih rekening</option>
            {fundingCandidates.map((account) => (
              <option key={account.id} value={account.id}>{account.name}</option>
            ))}
          </select>
          {errorField === "funding" && <p id="investment-trade-funding-error" role="alert" className="mt-1 text-xs text-rose-600">{error}</p>}
        </div>

        {/* Live total preview */}
        {tradeUnits > 0 && parsedPrice > 0 && (
          <div
            className="space-y-1 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3 text-xs text-[var(--text)]"
          >
            <div className="mb-1 text-sm font-semibold">
              {action === "buy" ? "Dampak pembelian" : "Dampak penjualan"}
            </div>
            <div>
              {action === "buy"
                ? `${fmtMoney(tradeAmount)} keluar dari ${selectedFundingAccount?.name ?? "rekening sumber"} dan masuk ke posisi investasi.`
                : `${tradeUnits} unit keluar dari posisi investasi dan ${fmtMoney(tradeAmount)} masuk ke ${selectedFundingAccount?.name ?? "rekening tujuan"}.`}
            </div>
            <div className="flex justify-between">
              <span>Nominal transaksi</span>
              <span className="font-bold tabular">{fmtMoney(tradeAmount)}</span>
            </div>
            <div className="flex justify-between">
              <span>Unit setelah transaksi</span>
              <span className="font-bold tabular">
                {cfg.isLotBased
                  ? `${projectedUnits.toLocaleString("id-ID")} lembar (${(projectedUnits / 100).toLocaleString("id-ID")} lot)`
                  : `${projectedUnits} ${cfg.unitUnit}`}
              </span>
            </div>
            {action === "sell" && tradeUnits === currentUnits && (
              <p className="font-medium text-[var(--muted)]">Seluruh unit dijual; posisi menjadi kosong.</p>
            )}
            {selectedFundingAccount && projectedFundingBalance !== null && (
              <div className="flex justify-between">
                <span>Saldo rekening setelah transaksi</span>
                <span className={cn("font-bold tabular", insufficientDisplayedBalance && "text-rose-600")}>{fmtMoney(projectedFundingBalance)}</span>
              </div>
            )}
            {insufficientDisplayedBalance && <p role="alert" className="font-medium text-rose-600">Saldo rekening yang ditampilkan tidak cukup untuk pembelian ini.</p>}
            {action === "buy" && (
              <div className="flex justify-between">
                <span>Avg. beli baru</span>
                <span className="font-bold tabular">{fmtMoney(projectedAvg)}</span>
              </div>
            )}
          </div>
        )}

        {/* Notes */}
        <div>
          <label htmlFor="investment-trade-notes" className="block text-xs font-medium text-[var(--muted)] mb-1">Catatan (opsional)</label>
          <input
            id="investment-trade-notes"
            name="notes"
            type="text"
            placeholder="Kosongkan untuk auto-generate"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          />
        </div>

        {error && !errorField && (
          <p role="alert" aria-live="assertive" className="text-xs text-rose-500 bg-rose-500/10 border border-rose-500/25 rounded-lg px-3 py-2">
            {error}
          </p>
        )}

        {/* Actions */}
        <div className="flex gap-2 pt-1">
          <button
            type="submit"
            disabled={tradeMutation.isPending || exceedsPosition || insufficientDisplayedBalance}
            className={cn(
              "min-h-11 flex-1 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] transition-colors hover:opacity-90 disabled:opacity-50",
            )}
          >
            {tradeMutation.isPending ? "Memproses…" : submitLabel}
          </button>
          <button
            type="button"
            onClick={handleClose}
            className="px-4 py-2.5 rounded-lg text-sm font-semibold bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--border)]/50 transition-colors"
          >
            Batal
          </button>
        </div>
      </form>
    </Modal>
  );
}
