"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, createMovement } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";

interface InternalMovementModalProps {
  open: boolean;
  onClose: () => void;
  defaultSourceAccountId?: string;
  defaultTargetAccountId?: string;
}

export function InternalMovementModal({
  open,
  onClose,
  defaultSourceAccountId,
  defaultTargetAccountId,
}: InternalMovementModalProps) {
  const qc = useQueryClient();
  const inputRef = useRef<HTMLInputElement>(null);

  const [sourceAccountId, setSourceAccountId] = useState(defaultSourceAccountId ?? "");
  const [targetAccountId, setTargetAccountId] = useState(defaultTargetAccountId ?? "");
  const [amountStr, setAmountStr] = useState("");
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

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);

  // Set default accounts when loaded
  useEffect(() => {
    if (accounts.length > 0) {
      if (!sourceAccountId) {
        setSourceAccountId(accounts[0].id);
      }
      if (!targetAccountId) {
        const allSelectable = accounts.flatMap((a) =>
          a.children && a.children.length > 0 ? [a, ...a.children] : [a]
        );
        const other = allSelectable.find((a) => a.id !== (sourceAccountId || accounts[0].id));
        if (other) setTargetAccountId(other.id);
      }
    }
  }, [accounts, sourceAccountId, targetAccountId]);

  useEffect(() => {
    if (open) {
      setAmountStr("");
      setNotes("");
      setTxDate(toDatetimeLocal(new Date()));
      setErr("");
      setIsSuccess(false);
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  }, [open]);

  const parsedAmount = useMemo(() => {
    const raw = amountStr.replace(/[^0-9]/g, "");
    return raw ? parseInt(raw, 10) : 0;
  }, [amountStr]);

  const mutation = useMutation({
    mutationFn: async () => {
      if (parsedAmount <= 0) throw new Error("Masukkan nominal yang valid");
      if (!sourceAccountId || !targetAccountId) throw new Error("Pilih rekening sumber dan tujuan");
      if (sourceAccountId === targetAccountId) throw new Error("Rekening sumber dan tujuan tidak boleh sama");

      const isoDate = localDatetimeToISO(txDate) || new Date().toISOString();

      return createMovement({
        source_account_id: sourceAccountId,
        target_account_id: targetAccountId,
        amount: parsedAmount,
        notes: notes.trim() || null,
        date: isoDate,
      });
    },
    onSuccess: () => {
      setIsSuccess(true);
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["transactions"] });
      qc.invalidateQueries({ queryKey: ["transactions-ledger"] });
      qc.invalidateQueries({ queryKey: ["pulse"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      setTimeout(() => {
        setIsSuccess(false);
        onClose();
      }, 450);
    },
    onError: (e: any) => setErr(e.message || "Gagal memproses pemindahan saldo"),
  });

  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    mutation.mutate();
  };

  return (
    <Modal open={open} onClose={onClose} title="Pindah Saldo Antar Rekening">
      <form onSubmit={handleSubmit} className="space-y-4 pt-1">
        {/* Source & Target Account Selection */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Dari Rekening (Sumber)
            </label>
            <select
              value={sourceAccountId}
              onChange={(e) => setSourceAccountId(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30 focus:border-[var(--primary)] transition-all"
            >
              <AccountSelectOptions accounts={accounts} formatBalance={fmtMoney} allowParentSelection={true} />
            </select>
          </div>

          <div>
            <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Ke Rekening (Tujuan)
            </label>
            <select
              value={targetAccountId}
              onChange={(e) => setTargetAccountId(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30 focus:border-[var(--primary)] transition-all"
            >
              <AccountSelectOptions
                accounts={accounts}
                formatBalance={fmtMoney}
                allowParentSelection={true}
                excludeAccountId={sourceAccountId}
              />
            </select>
          </div>
        </div>

        {/* Amount Input */}
        <div className="relative card-crisp p-4 focus-within:ring-2 focus-within:ring-[var(--accent-lime)]/30 focus-within:border-[var(--border-strong)] transition-all">
          <div className="flex items-baseline justify-between">
            <span className="text-[11px] font-bold text-[var(--muted)] uppercase tracking-wider">
              Nominal Transfer (IDR)
            </span>
            {parsedAmount > 0 && (
              <span className="text-xs font-bold text-blue-500 tabular select-all">
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
            onChange={(e) => setAmountStr(formatNumberWithDots(e.target.value))}
            className="mt-1 w-full bg-transparent text-3xl sm:text-4xl font-black tracking-tight text-[var(--text)] outline-none tabular placeholder:text-[var(--muted)]/30"
          />
        </div>

        {/* Datetime Picker (Local Timezone Aware) */}
        <div>
          <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Waktu Pemindahan
          </label>
          <input
            type="datetime-local"
            value={txDate}
            onChange={(e) => setTxDate(e.target.value)}
            className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-[var(--primary)]/30 focus:border-[var(--primary)] transition-all"
          />
        </div>

        {/* Optional Notes */}
        <div>
          <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Catatan (Opsional)
          </label>
          <input
            type="text"
            placeholder="Misal: Alokasi jajan, Pindah ke tabungan darurat"
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

        {/* Action Buttons */}
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
              "inline-flex items-center justify-center gap-2 px-5 py-2.5 text-xs font-black rounded-2xl shadow-sm transition-all duration-200 pressable text-white bg-blue-500 hover:bg-blue-600 active:bg-blue-700 shadow-blue-500/20",
              isSuccess && "bg-emerald-500 scale-105 shadow-md shadow-emerald-500/30",
              "disabled:opacity-40 disabled:cursor-not-allowed disabled:active:scale-100"
            )}
          >
            {isSuccess ? (
              <span className="flex items-center gap-1.5 animate-in fade-in zoom-in-95 duration-150">
                <svg className="w-4 h-4 stroke-[3]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
                ✓ Berhasil Dipindahkan!
              </span>
            ) : mutation.isPending ? (
              "Memproses..."
            ) : (
              "Pindahkan Saldo"
            )}
          </button>
        </div>
      </form>
    </Modal>
  );
}
