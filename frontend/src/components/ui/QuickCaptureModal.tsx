"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";

interface QuickCaptureModalProps {
  open: boolean;
  onClose: () => void;
  defaultAccountId?: string;
  defaultType?: "expense" | "income";
}

export function QuickCaptureModal({
  open,
  onClose,
  defaultAccountId,
  defaultType = "expense",
}: QuickCaptureModalProps) {
  const qc = useQueryClient();
  const inputRef = useRef<HTMLInputElement>(null);

  const [type, setType] = useState<"expense" | "income">(defaultType);
  const [amountStr, setAmountStr] = useState("");
  const [selectedAccount, setSelectedAccount] = useState(defaultAccountId ?? "");
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null);
  const [kakeiboType, setKakeiboType] = useState<"need" | "want" | "saving">("need");
  const [selectedGoalId, setSelectedGoalId] = useState<string>("");
  const [selectedObligationId, setSelectedObligationId] = useState<string>("");
  const [notes, setNotes] = useState("");
  const [txDate, setTxDate] = useState(() => toDatetimeLocal(new Date()));
  const [err, setErr] = useState("");
  const [isSuccess, setIsSuccess] = useState(false);

  // Fetch accounts
  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  // Fetch categories
  const { data: categoriesData } = useQuery<{ categories: any[] }>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
    enabled: open,
  });

  // Fetch goals
  const { data: goalsData } = useQuery<{ goals: any[] }>({
    queryKey: ["goals"],
    queryFn: () => api.get("/goals"),
    enabled: open,
  });

  // Fetch obligations
  const { data: obligationsData } = useQuery<{ obligations: any[] }>({
    queryKey: ["obligations"],
    queryFn: () => api.get("/obligations"),
    enabled: open,
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const categories = useMemo(() => categoriesData?.categories ?? [], [categoriesData?.categories]);
  const goals = useMemo(() => goalsData?.goals ?? [], [goalsData?.goals]);
  const obligations = useMemo(() => obligationsData?.obligations ?? [], [obligationsData?.obligations]);

  // Filter categories by type
  const availableCategories = useMemo(() => {
    return categories.filter((c: any) => c.kind === type && !c.is_archived);
  }, [categories, type]);

  // Set default account when accounts load
  useEffect(() => {
    if (accounts.length > 0 && !selectedAccount) {
      setSelectedAccount(accounts[0].id);
    }
  }, [accounts, selectedAccount]);

  // Sync default type when opened
  useEffect(() => {
    if (open) {
      setType(defaultType);
      setAmountStr("");
      setNotes("");
      setTxDate(toDatetimeLocal(new Date()));
      setErr("");
      setIsSuccess(false);
      setSelectedCategory(null);
      setKakeiboType("need");
      setSelectedGoalId("");
      setSelectedObligationId("");
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  }, [open, defaultType]);

  // Parse numeric amount or math expression (CSP-safe, no eval / Function)
  const parseAmount = (val: string): number => {
    try {
      const sanitized = val.replace(/\./g, "").trim();
      if (!sanitized) return 0;

      // Pure integer check
      if (/^[0-9]+$/.test(sanitized)) {
        const n = parseInt(sanitized, 10);
        return isNaN(n) ? 0 : n;
      }

      // Safe arithmetic tokenizer and evaluator (CSP compliant)
      const tokens = sanitized.match(/([0-9]+|[+\-*/])/g);
      if (!tokens || tokens.length === 0) return 0;

      // Pass 1: Multiplicative operations (*, /)
      const intermediate: (number | string)[] = [];
      let i = 0;
      while (i < tokens.length) {
        const token = tokens[i];
        if (token === "*" || token === "/") {
          const prev = intermediate.pop();
          const next = tokens[i + 1];
          if (typeof prev !== "number" || !next || isNaN(Number(next))) return 0;
          const nNext = Number(next);
          const res = token === "*" ? prev * nNext : (nNext !== 0 ? Math.floor(prev / nNext) : 0);
          intermediate.push(res);
          i += 2;
        } else if (!isNaN(Number(token))) {
          intermediate.push(Number(token));
          i++;
        } else {
          intermediate.push(token);
          i++;
        }
      }

      // Pass 2: Additive operations (+, -)
      let total = typeof intermediate[0] === "number" ? intermediate[0] : 0;
      let op = "+";
      for (let j = 1; j < intermediate.length; j++) {
        const item = intermediate[j];
        if (item === "+" || item === "-") {
          op = item;
        } else if (typeof item === "number") {
          if (op === "+") total += item;
          else if (op === "-") total -= item;
        }
      }

      return typeof total === "number" && !isNaN(total) && total > 0 ? Math.round(total) : 0;
    } catch {
      return 0;
    }
  };

  const parsedAmount = parseAmount(amountStr);

  const handleCategoryChange = (catId: string | null) => {
    setSelectedCategory(catId);
  };

  const mutation = useMutation({
    mutationFn: async () => {
      if (parsedAmount <= 0) throw new Error("Masukkan nominal yang valid");
      if (!selectedAccount) throw new Error("Pilih rekening / dompet");

      const isoDate = localDatetimeToISO(txDate) || new Date().toISOString();

      return api.post("/transactions", {
        type,
        amount: parsedAmount,
        account_id: selectedAccount,
        category_id: selectedCategory || null,
        kakeibo_type: type !== "income" ? kakeiboType : null,
        goal_id: selectedGoalId || null,
        obligation_id: selectedObligationId || null,
        notes: notes.trim() || null,
        date: isoDate,
      });
    },
    onSuccess: () => {
      setIsSuccess(true);
      qc.invalidateQueries({ queryKey: ["pulse"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["insights"] });
      qc.invalidateQueries({ queryKey: ["transactions"] });
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["obligations"] });
      setTimeout(() => {
        setIsSuccess(false);
        onClose();
      }, 450);
    },
    onError: (e: any) => setErr(e.message || "Gagal menyimpan transaksi"),
  });

  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    mutation.mutate();
  };

  return (
    <Modal open={open} onClose={onClose} title="Catat Transaksi Cepat">
      <form onSubmit={handleSubmit} className="space-y-3.5 pt-1">
        {/* Type Selector Pills with Smooth Sliding Indicator */}
        <div className="relative flex rounded-2xl border border-[var(--border)] p-1 bg-[var(--surface-raised)] overflow-hidden">
          <div
            className="absolute top-1 bottom-1 rounded-xl transition-all duration-200 ease-[cubic-bezier(0.16,1,0.3,1)] shadow-xs"
            style={{
              width: "calc((100% - 8px) / 2)",
              transform: type === "expense" ? "translateX(0%)" : "translateX(100%)",
              backgroundColor: type === "expense" ? "var(--color-expense)" : "var(--color-income)",
            }}
          />
          <button
            type="button"
            onClick={() => { setType("expense"); setSelectedCategory(null); }}
            className={cn(
              "relative z-10 flex-1 py-2 text-xs font-bold rounded-xl transition-colors duration-150 text-center pressable",
              type === "expense"
                ? "text-white"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            💸 Keluar
          </button>
          <button
            type="button"
            onClick={() => { setType("income"); setSelectedCategory(null); }}
            className={cn(
              "relative z-10 flex-1 py-2 text-xs font-bold rounded-xl transition-colors duration-150 text-center pressable",
              type === "income"
                ? "text-white"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            💰 Masuk
          </button>
        </div>

        {/* Big Numeric Input with Live Math Indicator */}
        <div className="relative rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-4 focus-within:ring-2 focus-within:ring-[var(--primary)]/30 focus-within:border-[var(--primary)] transition-all card-squircle">
          <div className="flex items-baseline justify-between">
            <span className="text-[11px] font-bold text-[var(--muted)] uppercase tracking-wider">
              Nominal (IDR)
            </span>
            {parsedAmount > 0 && (
              <span className="text-xs font-bold text-emerald-500 tabular select-all">
                = {fmtMoney(parsedAmount)}
              </span>
            )}
          </div>
          <input
            ref={inputRef}
            type="text"
            inputMode="numeric"
            placeholder="0"
            value={amountStr}
            onChange={(e) => {
              const val = e.target.value;
              if (!/[+\-*/]/.test(val)) {
                setAmountStr(formatNumberWithDots(val));
              } else {
                setAmountStr(val);
              }
            }}
            className="mt-1 w-full bg-transparent text-3xl sm:text-4xl font-black tracking-tight text-[var(--text)] outline-none tabular placeholder:text-[var(--muted)]/30"
          />
        </div>

        {/* Kakeibo 3-Way Chips (For Expense) */}
        {type === "expense" && (
          <div className="space-y-1.5">
            <div className="flex items-center justify-between text-[11px] font-medium text-[var(--muted)]">
              <span className="text-[10px] uppercase tracking-wider">Pilar Kakeibo</span>
              <span className="text-[10px]">
                {kakeiboType === "need"
                  ? "Kebutuhan Pokok (≤50%)"
                  : kakeiboType === "want"
                  ? "Keinginan & Gaya Hidup (≤30%)"
                  : "Tabungan & Investasi (≥20%)"}
              </span>
            </div>
            <div className="grid grid-cols-3 gap-2">
              <button
                type="button"
                onClick={() => setKakeiboType("need")}
                className={cn(
                  "py-2 px-2.5 rounded-2xl text-xs font-bold border text-center transition-all pressable",
                  kakeiboType === "need"
                    ? "kakeibo-chip-need shadow-sm ring-1 ring-sky-400/40"
                    : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                )}
              >
                🍞 Need
              </button>
              <button
                type="button"
                onClick={() => setKakeiboType("want")}
                className={cn(
                  "py-2 px-2.5 rounded-2xl text-xs font-bold border text-center transition-all pressable",
                  kakeiboType === "want"
                    ? "kakeibo-chip-want shadow-sm ring-1 ring-rose-400/40"
                    : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                )}
              >
                👑 Want
              </button>
              <button
                type="button"
                onClick={() => setKakeiboType("saving")}
                className={cn(
                  "py-2 px-2.5 rounded-2xl text-xs font-bold border text-center transition-all pressable",
                  kakeiboType === "saving"
                    ? "kakeibo-chip-saving shadow-sm ring-1 ring-emerald-400/40"
                    : "bg-[var(--surface-raised)] text-[var(--muted)] border-[var(--border)] hover:text-[var(--text)]"
                )}
              >
                💎 Saving
              </button>
            </div>
          </div>
        )}

        {/* Account and Category Selectors */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
          <div>
            <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Rekening / Dompet
            </label>
            <select
              value={selectedAccount}
              onChange={(e) => setSelectedAccount(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30 focus:border-[var(--primary)] transition-all"
            >
              <AccountSelectOptions accounts={accounts} formatBalance={fmtMoney} allowParentSelection={true} />
            </select>
          </div>

          <div>
            <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Kategori
            </label>
            <select
              value={selectedCategory || ""}
              onChange={(e) => handleCategoryChange(e.target.value || null)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30 focus:border-[var(--primary)] transition-all"
            >
              <option value="">Pilih Kategori...</option>
              {availableCategories.map((c: any) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Datetime Picker (Local Timezone Aware) */}
        <div>
          <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Waktu Transaksi
          </label>
          <input
            type="datetime-local"
            value={txDate}
            onChange={(e) => setTxDate(e.target.value)}
            className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30 focus:border-[var(--primary)] transition-all"
          />
        </div>

        {/* Optional Link to Goal or Debt */}
        {(goals.length > 0 || obligations.length > 0) && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
            <div>
              <label className="block text-[10px] font-medium text-[var(--muted)] mb-1">
                Target Tabungan (Opsional)
              </label>
              <select
                value={selectedGoalId}
                onChange={(e) => {
                  setSelectedGoalId(e.target.value);
                  if (e.target.value) {
                    setSelectedObligationId("");
                    setKakeiboType("saving");
                    const g = goals.find((item: any) => item.id === e.target.value);
                    if (g && !notes) setNotes(`Tabungan: ${g.name}`);
                  }
                }}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-1.5 text-xs text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30"
              >
                <option value="">Tidak ada</option>
                {goals.map((g: any) => (
                  <option key={g.id} value={g.id}>
                    🎯 {g.name}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-[10px] font-medium text-[var(--muted)] mb-1">
                Tagihan / Utang (Opsional)
              </label>
              <select
                value={selectedObligationId}
                onChange={(e) => {
                  setSelectedObligationId(e.target.value);
                  if (e.target.value) {
                    setSelectedGoalId("");
                    const o = obligations.find((item: any) => item.id === e.target.value);
                    if (o && !notes) setNotes(`Pembayaran: ${o.name}`);
                  }
                }}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-1.5 text-xs text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30"
              >
                <option value="">Tidak ada</option>
                {obligations.map((o: any) => (
                  <option key={o.id} value={o.id}>
                    💳 {o.name}
                  </option>
                ))}
              </select>
            </div>
          </div>
        )}

        {/* Optional Notes */}
        <div>
          <input
            type="text"
            placeholder="Catatan transaksi (opsional, misal: Makan siang, Kopi kenangan)"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] outline-none placeholder:text-[var(--muted)] focus:ring-2 focus:ring-[var(--primary)]/30 transition-all"
          />
        </div>

        {err && (
          <div className="p-2.5 rounded-2xl bg-rose-500/10 border border-rose-500/20 text-rose-500 text-xs font-medium">
            {err}
          </div>
        )}

        {/* Modal Action Buttons */}
        <div className="flex items-center justify-end gap-2 pt-2 border-t border-[var(--border)]">
          <button
            type="button"
            onClick={onClose}
            className="btn-secondary pressable rounded-xl"
          >
            Batal
          </button>
          <button
            type="submit"
            disabled={parsedAmount <= 0 || mutation.isPending || isSuccess}
            className={cn(
              "inline-flex items-center justify-center gap-2 px-5 py-2.5 text-xs font-black rounded-2xl shadow-sm transition-all duration-200 pressable text-white",
              isSuccess
                ? "bg-emerald-500 scale-105 shadow-md shadow-emerald-500/30"
                : type === "expense"
                ? "bg-rose-500 hover:bg-rose-600 active:bg-rose-700 shadow-rose-500/20"
                : "bg-emerald-500 hover:bg-emerald-600 active:bg-emerald-700 shadow-emerald-500/20",
              "disabled:opacity-40 disabled:cursor-not-allowed disabled:active:scale-100"
            )}
          >
            {isSuccess ? (
              <span className="flex items-center gap-1.5 animate-in fade-in zoom-in-95 duration-150">
                <svg className="w-4 h-4 stroke-[3]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
                ✓ Tersimpan!
              </span>
            ) : mutation.isPending ? (
              "Menyimpan..."
            ) : (
              "Simpan Transaksi"
            )}
          </button>
        </div>
      </form>
    </Modal>
  );
}
