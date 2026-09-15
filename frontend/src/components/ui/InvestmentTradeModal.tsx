"use client";
import { useState, useEffect, useCallback } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
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

  const cfg = getInstrumentConfig(pocket?.instrument_type);

  // Derived: actual units to trade (lots × 100 for stocks)
  const parsedInputUnits = parseFloat(inputUnits.replace(/[^0-9.]/g, "")) || 0;
  const parsedPrice = parseNumberFromDots(inputPrice) || 0;
  const tradeUnits = cfg.isLotBased ? parsedInputUnits * 100 : parsedInputUnits;
  const tradeAmount = Math.round(tradeUnits * parsedPrice);

  // Current position
  const currentUnits = pocket?.units ?? 0;
  const currentAvg = pocket?.avg_buy_price ?? 0;

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
  const fundingCandidates = allAccounts.filter(
    (a) => a.type !== "investment" && !a.parent_id && !a.is_archived
  );

  // Reset on open
  useEffect(() => {
    if (!open) return;
    setAction(defaultAction);
    setInputUnits("");
    setInputPrice("");
    setNotes("");
    setError("");

    // Pre-fill funding account from pocket's parent's default_funding_account_id
    const parentId = pocket?.parent_id;
    const parent = parentId ? allAccounts.find((a) => a.id === parentId) : null;
    const preferredFunding = parent?.default_funding_account_id ?? "";
    if (preferredFunding) {
      setFundingAccountId(preferredFunding);
    } else if (fundingCandidates.length > 0) {
      setFundingAccountId(fundingCandidates[0].id);
    }
  }, [open, defaultAction, pocket, allAccounts, fundingCandidates]);

  // Recalculate when fundingCandidates changes (avoid stale closure)
  const handleClose = useCallback(() => {
    setError("");
    onClose();
  }, [onClose]);

  const tradeMutation = useMutation({
    mutationFn: async () => {
      if (!pocket) throw new Error("Tidak ada instrumen yang dipilih");
      if (tradeUnits <= 0) throw new Error("Masukkan jumlah unit yang valid");
      if (parsedPrice <= 0) throw new Error("Masukkan harga per unit yang valid");
      if (!fundingAccountId) throw new Error("Pilih rekening sumber / tujuan dana");
      if (tradeAmount <= 0) throw new Error("Nominal transaksi tidak valid");

      // Buy: money flows FROM funding account TO investment pocket
      // Sell: money flows FROM investment pocket TO funding account
      const accountId = action === "buy" ? fundingAccountId : pocket.id;
      const targetAccountId = action === "buy" ? pocket.id : fundingAccountId;

      return api.post("/transactions", {
        type: "transfer",
        account_id: accountId,
        transfer_target_account_id: targetAccountId,
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
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      qc.invalidateQueries({ queryKey: ["ledger"] });
      handleClose();
    },
    onError: (err: any) => {
      setError(err?.message || "Transaksi gagal");
    },
  });

  if (!pocket) return null;

  const fundingLabel = action === "buy" ? "Dana dari Rekening" : "Dana masuk ke Rekening";
  const submitLabel = action === "buy" ? "Catat Pembelian" : "Catat Penjualan";
  const submitColor =
    action === "buy"
      ? "bg-emerald-600 hover:bg-emerald-700 text-white"
      : "bg-rose-600 hover:bg-rose-700 text-white";

  return (
    <Modal
      open={open}
      onClose={handleClose}
      title={`Transaksi Investasi — ${pocket.instrument_symbol ?? pocket.name}`}
    >
      <div className="space-y-4">
        {/* Buy / Sell Toggle */}
        <div className="flex rounded-lg overflow-hidden border border-[var(--border)]">
          {(["buy", "sell"] as const).map((act) => (
            <button
              key={act}
              type="button"
              onClick={() => setAction(act)}
              className={cn(
                "flex-1 py-2 text-sm font-semibold transition-colors",
                action === act
                  ? act === "buy"
                    ? "bg-emerald-600 text-white"
                    : "bg-rose-600 text-white"
                  : "bg-[var(--surface)] text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              )}
            >
              {act === "buy" ? "🟢 Beli" : "🔴 Jual"}
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
          <label className="block text-xs font-medium text-[var(--muted)] mb-1">{cfg.unitLabel}</label>
          <input
            type="number"
            min="0"
            step={cfg.unitStep}
            placeholder={cfg.unitPlaceholder}
            value={inputUnits}
            onChange={(e) => setInputUnits(e.target.value)}
            className="w-full px-3 py-2 rounded-lg bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/50"
          />
          {cfg.isLotBased && parsedInputUnits > 0 && (
            <p className="text-[10px] text-[var(--muted)] mt-1">
              = {(parsedInputUnits * 100).toLocaleString("id-ID")} lembar
            </p>
          )}
        </div>

        {/* Price input */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <label className="block text-xs font-medium text-[var(--muted)]">{cfg.priceLabel}</label>
            {(pocket.last_price || pocket.avg_buy_price) && (
              <div className="flex items-center gap-1.5 text-[10px]">
                <span className="text-[var(--muted)]">Isi cepat:</span>
                {pocket.last_price && (
                  <button
                    type="button"
                    onClick={() => setInputPrice(Math.round(pocket.last_price!).toLocaleString("id-ID"))}
                    className="px-1.5 py-0.5 rounded bg-[var(--surface-raised)] hover:bg-[var(--border)] border border-[var(--border)] text-emerald-600 dark:text-emerald-400 font-semibold transition-colors"
                    title="Gunakan harga pasar terkini"
                  >
                    Pasar ({fmtMoney(pocket.last_price)})
                  </button>
                )}
                {pocket.avg_buy_price && (
                  <button
                    type="button"
                    onClick={() => setInputPrice(Math.round(pocket.avg_buy_price!).toLocaleString("id-ID"))}
                    className="px-1.5 py-0.5 rounded bg-[var(--surface-raised)] hover:bg-[var(--border)] border border-[var(--border)] text-[var(--text)] font-semibold transition-colors"
                    title="Gunakan harga modal rata-rata"
                  >
                    Modal ({fmtMoney(pocket.avg_buy_price)})
                  </button>
                )}
              </div>
            )}
          </div>
          <input
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
            className="w-full px-3 py-2 rounded-lg bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/50"
          />
        </div>

        {/* Live total preview */}
        {tradeUnits > 0 && parsedPrice > 0 && (
          <div
            className={cn(
              "px-3 py-2.5 rounded-lg border text-xs space-y-1",
              action === "buy"
                ? "bg-emerald-500/10 border-emerald-500/25 text-emerald-700 dark:text-emerald-300"
                : "bg-rose-500/10 border-rose-500/25 text-rose-700 dark:text-rose-300"
            )}
          >
            <div className="font-semibold uppercase tracking-wider mb-1">
              {action === "buy" ? "Preview Pembelian" : "Preview Penjualan"}
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
            {action === "buy" && (
              <div className="flex justify-between">
                <span>Avg. beli baru</span>
                <span className="font-bold tabular">{fmtMoney(projectedAvg)}</span>
              </div>
            )}
          </div>
        )}

        {/* Funding account selector */}
        <div>
          <label className="block text-xs font-medium text-[var(--muted)] mb-1">{fundingLabel}</label>
          <select
            value={fundingAccountId}
            onChange={(e) => setFundingAccountId(e.target.value)}
            className="w-full px-3 py-2 rounded-lg bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/50"
          >
            <option value="">— Pilih Rekening —</option>
            {fundingCandidates.map((a) => (
              <option key={a.id} value={a.id}>
                {a.name}
              </option>
            ))}
          </select>
        </div>

        {/* Notes */}
        <div>
          <label className="block text-xs font-medium text-[var(--muted)] mb-1">Catatan (opsional)</label>
          <input
            type="text"
            placeholder="Kosongkan untuk auto-generate"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            className="w-full px-3 py-2 rounded-lg bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/50"
          />
        </div>

        {error && (
          <p className="text-xs text-rose-500 bg-rose-500/10 border border-rose-500/25 rounded-lg px-3 py-2">
            {error}
          </p>
        )}

        {/* Actions */}
        <div className="flex gap-2 pt-1">
          <button
            type="button"
            disabled={tradeMutation.isPending}
            onClick={() => tradeMutation.mutate()}
            className={cn(
              "flex-1 py-2.5 rounded-lg text-sm font-semibold transition-colors disabled:opacity-50",
              submitColor
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
      </div>
    </Modal>
  );
}
