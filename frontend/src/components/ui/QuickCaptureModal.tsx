"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, uploadTransactionReceipt } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { FormSection, PendingSubmitButton } from "@/components/ui/FormField";
import { InfoHelp } from "@/components/ui/InfoHelp";
import { DebtAllocationEditor, allocationError, totalDebtPayments, type DebtAllocationValue } from "@/components/ui/DebtAllocationEditor";
import { queryKeys } from "@/lib/queryKeys";
import { z } from "zod";

const quickCaptureSchema = z
  .object({
    type: z.enum(["expense", "income"]),
    amount: z.number().int().positive("Masukkan nominal yang valid"),
    accountId: z.string().min(1, "Pilih rekening / dompet"),
    categoryId: z.string().nullable(),
    goalId: z.string().nullable(),
    obligationId: z.string().nullable(),
    transactionDate: z.string().min(1, "Pilih waktu transaksi"),
    notes: z.string().max(500, "Catatan maksimal 500 karakter"),
  })
  .superRefine((values, context) => {
    if (values.goalId && values.obligationId) {
      context.addIssue({
        code: "custom",
        message: "Pilih target tabungan atau tagihan, bukan keduanya",
        path: ["goalId"],
      });
    }
  });

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
  const accountRef = useRef<HTMLSelectElement>(null);
  const categoryRef = useRef<HTMLSelectElement>(null);

  const [type, setType] = useState<"expense" | "income">(defaultType);
  const [amountStr, setAmountStr] = useState("");
  const [selectedAccount, setSelectedAccount] = useState(defaultAccountId ?? "");
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null);
  const [kakeiboType, setKakeiboType] = useState<"need" | "want" | "saving">("need");
  const [hasKakeiboOverride, setHasKakeiboOverride] = useState(false);
  const [selectedGoalId, setSelectedGoalId] = useState<string>("");
  const [selectedObligationId, setSelectedObligationId] = useState<string>("");
  const [debtRows, setDebtRows] = useState<DebtAllocationValue[]>([]);
  const [notes, setNotes] = useState("");
  const [receipt, setReceipt] = useState<File | null>(null);
  const [savedTransactionId, setSavedTransactionId] = useState<string | null>(null);
  const [txDate, setTxDate] = useState(() => toDatetimeLocal(new Date()));
  const [err, setErr] = useState("");
  const [isSuccess, setIsSuccess] = useState(false);

  // Fetch accounts
  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  // Fetch categories
  const { data: categoriesData } = useQuery<{ categories: any[] }>({
    queryKey: queryKeys.categories,
    queryFn: () => api.get("/categories"),
    enabled: open,
  });

  // Fetch goals
  const { data: goalsData } = useQuery<{ goals: any[] }>({
    queryKey: queryKeys.goals,
    queryFn: () => api.get("/goals"),
    enabled: open,
  });

  // Fetch obligations
  const { data: obligationsData } = useQuery<{ obligations: any[] }>({
    queryKey: queryKeys.obligations,
    queryFn: () => api.get("/obligations"),
    enabled: open,
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);
  const categories = useMemo(() => categoriesData?.categories ?? [], [categoriesData?.categories]);
  const goals = useMemo(() => goalsData?.goals ?? [], [goalsData?.goals]);
  const obligations = useMemo(() => obligationsData?.obligations ?? [], [obligationsData?.obligations]);
  const availableGoals = useMemo(
    () => goals.filter((goal: any) => !goal.is_archived),
    [goals],
  );
  const availableObligations = useMemo(
    () => obligations.filter((obligation: any) => !obligation.is_archived && obligation.remaining_amount > 0),
    [obligations],
  );

  // Filter categories by type
  const availableCategories = useMemo(() => {
    return categories.filter((c: any) => c.kind === type && !c.is_archived);
  }, [categories, type]);

  // Set default account when accounts load
  useEffect(() => {
    if (liquidAccounts.length > 0 && !liquidAccounts.some((account) => account.id === selectedAccount)) {
      const preferred = liquidAccounts.find((account) => account.id === defaultAccountId);
      setSelectedAccount(String(preferred?.id ?? liquidAccounts[0].id));
    }
  }, [defaultAccountId, liquidAccounts, selectedAccount]);

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
      setHasKakeiboOverride(false);
      setSelectedGoalId("");
      setSelectedObligationId("");
      setDebtRows([]);
      setReceipt(null);
      setSavedTransactionId(null);
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
  const hasSelectedDebt = type === "expense" && debtRows.some((row) => row.obligation_id);

  const addFastAmount = (increment: number) => {
    const next = (parsedAmount || 0) + increment;
    setAmountStr(formatNumberWithDots(String(next)));
  };

  const handleCategoryChange = (catId: string | null) => {
    setSelectedCategory(catId);
    const category = categories.find((item: any) => item.id === catId);
    if (type === "expense" && !hasKakeiboOverride) {
      setKakeiboType(category?.kakeibo_type ?? "need");
    }
  };

  const handleTypeChange = (nextType: "expense" | "income") => {
    setType(nextType);
    setSelectedCategory(null);
    setHasKakeiboOverride(false);
    if (nextType === "income") {
      setSelectedGoalId("");
      setSelectedObligationId("");
      setDebtRows([]);
    } else {
      setKakeiboType("need");
    }
  };

  const mutation = useMutation({
    mutationFn: async () => {
      if (savedTransactionId && receipt) {
        await uploadTransactionReceipt(savedTransactionId, receipt);
        return { ok: true, transaction_id: savedTransactionId };
      }
      if (parsedAmount <= 0) throw new Error("Masukkan nominal yang valid");
      if (!selectedAccount) throw new Error("Pilih rekening / dompet");

      const isoDate = localDatetimeToISO(txDate) || new Date().toISOString();

      const created = await api.post<{ ok: boolean; transaction_id: string }>("/transactions", {
        type,
        amount: parsedAmount,
        account_id: selectedAccount,
        category_id: selectedCategory || null,
        kakeibo_type: type !== "income" ? kakeiboType : null,
        goal_id: selectedGoalId || null,
        obligation_id: type === "expense" && debtRows.length === 1 ? debtRows[0].obligation_id : null,
        ...(type === "expense" && debtRows.length > 1 ? { obligation_allocations: debtRows.map((row) => ({ obligation_id: row.obligation_id, amount: Number(row.amount.replace(/\./g, "")) })) } : {}),
        notes: notes.trim() || null,
        date: isoDate,
      });
      if (receipt) {
        try {
          await uploadTransactionReceipt(created.transaction_id, receipt);
        } catch (uploadError) {
          return {
            ...created,
            receiptWarning:
              uploadError instanceof Error
                ? uploadError.message
                : "Bukti gagal diunggah",
          };
        }
      }
      return created;
    },
    onSuccess: (result) => {
      setIsSuccess(true);
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.obligations });
      if ("receiptWarning" in result) {
        setIsSuccess(false);
        setSavedTransactionId(result.transaction_id);
        setErr(`Transaksi tersimpan, tetapi bukti belum terunggah: ${result.receiptWarning}`);
        return;
      }
      setTimeout(() => {
        setIsSuccess(false);
        onClose();
      }, 450);
    },
    onError: (e: any) => {
      setErr(e.message || "Gagal menyimpan transaksi");
      if (e.message?.includes("nominal")) {
        inputRef.current?.focus();
      } else {
        accountRef.current?.focus();
      }
    },
  });

  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    if (mutation.isPending || isSuccess) return;
    const validation = quickCaptureSchema.safeParse({
      type,
      amount: parsedAmount,
      accountId: selectedAccount,
      categoryId: selectedCategory,
      goalId: selectedGoalId || null,
      obligationId: debtRows[0]?.obligation_id || selectedObligationId || null,
      transactionDate: txDate,
      notes: notes.trim(),
    });
    if (!validation.success) {
      const issue = validation.error.issues[0];
      setErr(issue.message);
      if (issue.path[0] === "amount") inputRef.current?.focus();
      else if (issue.path[0] === "categoryId") categoryRef.current?.focus();
      else if (issue.path[0] === "goalId") document.getElementById("quick-capture-goal")?.focus();
      else accountRef.current?.focus();
      return;
    }
    if (debtRows.some((row) => !row.obligation_id || debtRows.filter((item) => item.obligation_id === row.obligation_id).length > 1)) {
      setErr("Pilih tagihan yang berbeda untuk setiap baris.");
      document.getElementById("quick-capture-debt-0")?.focus();
      return;
    }
    const splitError = allocationError(debtRows, parsedAmount, availableObligations);
    if (splitError) {
      setErr(splitError);
      (document.getElementById("quick-capture-debt-0-amount") || document.getElementById("quick-capture-edit-amounts"))?.focus();
      return;
    }
    mutation.mutate();
  };

  return (
    <Modal open={open} onClose={onClose} title="Catat transaksi">
      <form onSubmit={handleSubmit} className="space-y-4 pt-1">
        <div className="flex rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-1" role="group" aria-label="Jenis transaksi">
          <button
            type="button"
            onClick={() => handleTypeChange("expense")}
            aria-pressed={type === "expense"}
            className={cn(
              "min-h-11 flex-1 rounded-lg px-3 text-sm font-semibold transition-colors motion-reduce:transition-none",
              type === "expense"
                ? "bg-[var(--text)] text-[var(--surface)] shadow-sm"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            Pengeluaran
          </button>
          <button
            type="button"
            onClick={() => handleTypeChange("income")}
            aria-pressed={type === "income"}
            className={cn(
              "min-h-11 flex-1 rounded-lg px-3 text-sm font-semibold transition-colors motion-reduce:transition-none",
              type === "income"
                ? "bg-[var(--text)] text-[var(--surface)] shadow-sm"
                : "text-[var(--muted)] hover:text-[var(--text)]"
            )}
          >
            Pemasukan
          </button>
        </div>

        <div className="rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] p-4 focus-within:border-[var(--text)] focus-within:ring-2 focus-within:ring-[var(--primary)]/30">
          <div className="flex items-baseline justify-between">
            <label htmlFor="quick-capture-amount" className="text-xs font-semibold text-[var(--muted)]">{hasSelectedDebt ? "Total bayar (IDR)" : "Nominal (IDR)"}</label>
            {parsedAmount > 0 && (
              <span className="text-xs font-semibold text-[var(--muted)] tabular select-all">
                = {fmtMoney(parsedAmount)}
              </span>
            )}
          </div>
          <input
            ref={inputRef}
            data-autofocus
            id="quick-capture-amount"
            name="amount"
            type="text"
            inputMode="numeric"
            placeholder="0"
            value={amountStr}
            readOnly={Boolean(hasSelectedDebt)}
            onChange={(e) => {
              const val = e.target.value;
              if (!/[+\-*/]/.test(val)) {
                setAmountStr(formatNumberWithDots(val));
              } else {
                setAmountStr(val);
              }
            }}
            className="mt-1 w-full bg-transparent text-3xl font-bold tracking-tight text-[var(--text)] outline-none tabular placeholder:text-[var(--muted)]/50 sm:text-4xl"
          />

          {/* Quick Amount Chips */}
          {!hasSelectedDebt && <div className="flex flex-wrap items-center gap-1.5 pt-2">
            {[10_000, 20_000, 50_000, 100_000, 500_000].map((inc) => (
              <button
                key={inc}
                type="button"
                onClick={() => addFastAmount(inc)}
                className="min-h-9 shrink-0 rounded-lg border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-xs font-semibold tabular text-[var(--muted)] hover:border-[var(--border-strong)] hover:text-[var(--text)]"
              >
                +{inc >= 1_000_000 ? `${inc / 1_000_000}M` : `${inc / 1_000}k`}
              </button>
            ))}
            {parsedAmount > 0 && (
              <button
                type="button"
                onClick={() => setAmountStr("")}
                className="min-h-9 shrink-0 rounded-lg border border-[var(--border)] px-3 text-xs font-medium text-[var(--muted)] hover:text-[var(--text)]"
              >
                Reset
              </button>
            )}
          </div>}
        </div>

        <FormSection title="Rincian transaksi">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div>
            <label htmlFor="quick-capture-account" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              {type === "expense" ? "Bayar dari" : "Masuk ke"}
            </label>
            <select
              ref={accountRef}
              id="quick-capture-account"
              name="account_id"
              value={selectedAccount}
              onChange={(e) => setSelectedAccount(e.target.value)}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30 focus-visible:border-[var(--primary)]"
            >
              <AccountSelectOptions accounts={accounts} formatBalance={fmtMoney} allowParentSelection={true} liquidOnly />
            </select>
          </div>

          <div>
            <label htmlFor="quick-capture-category" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Kategori
            </label>
            <select
              ref={categoryRef}
              id="quick-capture-category"
              name="category_id"
              value={selectedCategory || ""}
              onChange={(e) => handleCategoryChange(e.target.value || null)}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30 focus-visible:border-[var(--primary)]"
            >
              <option value="">Pilih Kategori…</option>
              {availableCategories.map((c: any) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
          </div>
        </div>
        {type === "expense" && (
          <fieldset className="space-y-2">
            <legend className="text-xs font-medium text-[var(--muted)]">Pilar Kakeibo <InfoHelp label="Pilar Kakeibo">Mengikuti pilar kategori, kecuali Anda memilih pilar lain untuk transaksi ini.</InfoHelp></legend>
            <div className="grid grid-cols-3 gap-2">
              {([
                ["need", "Kebutuhan"],
                ["want", "Keinginan"],
                ["saving", "Tabungan"],
              ] as const).map(([value, label]) => (
                <button
                  key={value}
                  type="button"
                  onClick={() => { setKakeiboType(value); setHasKakeiboOverride(true); }}
                  aria-pressed={kakeiboType === value}
                  className={cn(
                    "min-h-11 rounded-xl border px-2 text-xs font-semibold transition-colors motion-reduce:transition-none",
                    kakeiboType === value
                      ? "border-[var(--text)] bg-[var(--text)] text-[var(--surface)]"
                      : "border-[var(--border)] bg-[var(--surface-raised)] text-[var(--muted)] hover:text-[var(--text)]",
                  )}
                >
                  {label}
                </button>
              ))}
            </div>
          </fieldset>
        )}
        </FormSection>

        <FormSection title="Detail tambahan">
        <div>
          <label htmlFor="quick-capture-date" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Waktu Transaksi
          </label>
          <input
            id="quick-capture-date"
            name="date"
            type="datetime-local"
            step={1}
            value={txDate}
            onChange={(e) => setTxDate(e.target.value)}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30 focus-visible:border-[var(--primary)]"
          />
        </div>

        {/* Optional Link to Goal or Debt */}
        {type === "expense" && (availableGoals.length > 0 || availableObligations.length > 0) && (
          <div className="space-y-2">
          <div className="flex items-center gap-1 text-xs font-medium text-[var(--muted)]">
            <span>Kaitan transaksi</span>
            <InfoHelp label="Kaitan transaksi">Kaitan target tidak menambah progres secara terpisah. Pembayaran tagihan mengurangi sisanya.</InfoHelp>
          </div>
          <div className="space-y-3">
            <div>
              <label htmlFor="quick-capture-goal" className="block text-[10px] font-medium text-[var(--muted)] mb-1">
                Kaitkan ke target (opsional)
              </label>
              <select
                id="quick-capture-goal"
                name="goal_id"
                value={selectedGoalId}
                onChange={(e) => {
                  setSelectedGoalId(e.target.value);
                  if (e.target.value) {
                    setSelectedObligationId("");
                    setDebtRows([]);
                  }
                }}
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
              >
                <option value="">Tidak ada</option>
                {availableGoals.map((g: any) => (
                  <option key={g.id} value={g.id}>
                    🎯 {g.name}
                  </option>
                ))}
              </select>
            </div>

            <DebtAllocationEditor idPrefix="quick-capture" debts={availableObligations} rows={debtRows} total={parsedAmount} currency={fmtMoney} onChange={(rows) => {
              setDebtRows(rows);
              setSelectedObligationId(rows[0]?.obligation_id || "");
              if (rows.length) setSelectedGoalId("");
              if (rows.some((row) => row.obligation_id)) {
                setAmountStr(formatNumberWithDots(totalDebtPayments(rows)));
              }
              setErr("");
            }} />
          </div>
          </div>
        )}

        {/* Optional Notes */}
        <div>
          <label htmlFor="quick-capture-notes" className="mb-1 block text-xs font-medium text-[var(--muted)]">Catatan (opsional)</label>
          <input
            id="quick-capture-notes"
            name="notes"
            type="text"
            maxLength={500}
            autoComplete="off"
            placeholder="Catatan transaksi (opsional, misal: Makan siang, kopi)"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none placeholder:text-[var(--muted)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          />
        </div>

        <div>
          <label htmlFor="quick-capture-receipt" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Bukti transaksi (opsional)
          </label>
          <input
            id="quick-capture-receipt"
            type="file"
            accept="image/jpeg,image/png,image/webp,application/pdf"
            onChange={(event) => setReceipt(event.target.files?.[0] ?? null)}
            className="block min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs text-[var(--text)] file:mr-3 file:rounded-lg file:border-0 file:bg-[var(--text)] file:px-3 file:py-1.5 file:text-xs file:font-semibold file:text-[var(--surface)]"
          />
          <p className="mt-1 text-[10px] text-[var(--muted)]">JPG, PNG, WebP, atau PDF. Transaksi tetap tersimpan jika unggahan gagal.</p>
        </div>
        </FormSection>

        {err && (
          <div role="alert" className="p-2.5 rounded-2xl bg-rose-500/10 border border-rose-500/20 text-rose-500 text-xs font-medium">
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
          <PendingSubmitButton
            pending={mutation.isPending}
            pendingLabel="Menyimpan…"
            disabled={parsedAmount <= 0 || isSuccess}
            className={cn(
              "inline-flex min-h-11 items-center justify-center gap-2 rounded-xl px-5 text-sm font-semibold transition-colors motion-reduce:transition-none",
              isSuccess
                ? "bg-[var(--primary)] text-[var(--text)]"
                : "bg-[var(--text)] text-[var(--surface)] hover:opacity-90",
              "disabled:cursor-not-allowed disabled:opacity-40"
            )}
          >
            {isSuccess ? (
              <span className="flex items-center gap-1.5 animate-in fade-in zoom-in-95 duration-150">
                <svg className="w-4 h-4 stroke-[3]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
                ✓ Tersimpan!
              </span>
            ) : (
              savedTransactionId ? "Coba Unggah Lagi" : "Simpan Transaksi"
            )}
          </PendingSubmitButton>
        </div>
      </form>
    </Modal>
  );
}
