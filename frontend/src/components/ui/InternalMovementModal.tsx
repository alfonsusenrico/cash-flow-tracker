"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, createMovement, deleteMovement, updateMovement } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { FormSection } from "@/components/ui/FormField";
import { queryKeys } from "@/lib/queryKeys";
import { listLiquidAccountChoices } from "@/lib/accountOptions";

interface InternalMovementModalProps {
  open: boolean;
  onClose: () => void;
  defaultSourceAccountId?: string;
  defaultTargetAccountId?: string;
  editingMovement?: {
    id: string;
    sourceAccountId: string;
    targetAccountId: string;
    amount: number;
    notes?: string | null;
    date: string;
  } | null;
}

export function InternalMovementModal({
  open,
  onClose,
  defaultSourceAccountId,
  defaultTargetAccountId,
  editingMovement,
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
  const [confirmDelete, setConfirmDelete] = useState(false);

  // Fetch accounts
  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);

  // Set default accounts when loaded
  useEffect(() => {
    if (liquidAccounts.length === 0) return;
    const getAccountId = (account: any) => String(account.id ?? account.account_id ?? "");
    const firstAccountId = getAccountId(liquidAccounts[0]);
    setSourceAccountId((current) =>
      liquidAccounts.some((account) => getAccountId(account) === current)
        ? current
        : firstAccountId,
    );
    setTargetAccountId((current) => {
      const sourceId = liquidAccounts.some((account) => getAccountId(account) === sourceAccountId)
        ? sourceAccountId
        : firstAccountId;
      if (current !== sourceId && liquidAccounts.some((account) => getAccountId(account) === current)) {
        return current;
      }
      const target = liquidAccounts.find((account) => getAccountId(account) !== sourceId);
      return target ? getAccountId(target) : "";
    });
  }, [liquidAccounts, sourceAccountId]);

  useEffect(() => {
    if (open) {
      const requestedSource = editingMovement?.sourceAccountId ?? defaultSourceAccountId ?? "";
      const requestedTarget = editingMovement?.targetAccountId ?? defaultTargetAccountId ?? "";
      const getAccountId = (account: any) => String(account.id ?? account.account_id ?? "");
      const sourceId = liquidAccounts.some((account) => getAccountId(account) === requestedSource)
        ? requestedSource
        : getAccountId(liquidAccounts[0] ?? {});
      const targetId = liquidAccounts.some((account) => getAccountId(account) === requestedTarget && requestedTarget !== sourceId)
        ? requestedTarget
        : getAccountId(liquidAccounts.find((account) => getAccountId(account) !== sourceId) ?? {});
      setSourceAccountId(sourceId);
      setTargetAccountId(targetId);
      setAmountStr(editingMovement ? formatNumberWithDots(editingMovement.amount) : "");
      setNotes(editingMovement?.notes ?? "");
      setTxDate(editingMovement ? toDatetimeLocal(editingMovement.date) : toDatetimeLocal(new Date()));
      setErr("");
      setIsSuccess(false);
      setConfirmDelete(false);
    }
  }, [defaultSourceAccountId, defaultTargetAccountId, editingMovement, liquidAccounts, open]);

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

      const payload = {
        source_account_id: sourceAccountId,
        target_account_id: targetAccountId,
        amount: parsedAmount,
        notes: notes.trim() || null,
        date: isoDate,
      };
      return editingMovement
        ? updateMovement(editingMovement.id, payload)
        : createMovement(payload);
    },
    onSuccess: () => {
      setIsSuccess(true);
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      setTimeout(() => {
        setIsSuccess(false);
        onClose();
      }, 450);
    },
    onError: (e: any) => setErr(e.message || "Gagal memproses pemindahan saldo"),
  });

  const deleteMutation = useMutation({
    mutationFn: () => {
      if (!editingMovement) throw new Error("Pemindahan saldo tidak ditemukan");
      return deleteMovement(editingMovement.id);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (error: any) => setErr(error?.message || "Gagal menghapus pemindahan saldo"),
  });

  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    mutation.mutate();
  };

  return (
    <Modal open={open} onClose={onClose} title={editingMovement ? "Ubah pindah saldo" : "Pindah saldo"}>
      <form onSubmit={handleSubmit} className="space-y-4 pt-1">
        <FormSection title="Arah pemindahan" description="Saldo berpindah antar-rekening likuid. Ini bukan pemasukan atau pengeluaran; posisi investasi berubah melalui Beli/Jual." className="border-t-0 pt-0">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div>
            <label htmlFor="movement-source-account" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Dari rekening
            </label>
            <select
              id="movement-source-account"
              name="source_account_id"
              value={sourceAccountId}
              onChange={(e) => setSourceAccountId(e.target.value)}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
            >
              <AccountSelectOptions accounts={accounts} formatBalance={fmtMoney} allowParentSelection={true} liquidOnly />
            </select>
          </div>

          <div>
            <label htmlFor="movement-target-account" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Ke rekening
            </label>
            <select
              id="movement-target-account"
              name="target_account_id"
              value={targetAccountId}
              onChange={(e) => setTargetAccountId(e.target.value)}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
            >
              <AccountSelectOptions
                accounts={accounts}
                formatBalance={fmtMoney}
                allowParentSelection={true}
                excludeAccountId={sourceAccountId}
                liquidOnly
              />
            </select>
          </div>
        </div>
        </FormSection>

        <FormSection title="Nominal dan waktu">
        <div className="rounded-2xl border border-[var(--border-strong)] bg-[var(--surface)] p-4 focus-within:border-[var(--text)] focus-within:ring-2 focus-within:ring-[var(--primary)]/30">
          <div className="flex items-baseline justify-between">
            <label htmlFor="movement-amount" className="text-xs font-semibold text-[var(--muted)]">Nominal (IDR)</label>
            {parsedAmount > 0 && (
              <span className="text-xs font-semibold text-[var(--muted)] tabular select-all">
                = {fmtMoney(parsedAmount)}
              </span>
            )}
          </div>
          <input
            ref={inputRef}
            data-autofocus
            id="movement-amount"
            name="amount"
            type="text"
            inputMode="numeric"
            autoComplete="off"
            placeholder="0"
            value={amountStr}
            onChange={(e) => setAmountStr(formatNumberWithDots(e.target.value))}
            className="mt-1 w-full bg-transparent text-3xl font-bold tracking-tight text-[var(--text)] outline-none tabular placeholder:text-[var(--muted)]/50 sm:text-4xl"
          />
        </div>

        <div>
          <label htmlFor="movement-date" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Waktu Pemindahan
          </label>
          <input
            id="movement-date"
            name="date"
            type="datetime-local"
            value={txDate}
            onChange={(e) => setTxDate(e.target.value)}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          />
        </div>

        <div>
          <label htmlFor="movement-notes" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
            Catatan (Opsional)
          </label>
          <input
            id="movement-notes"
            name="notes"
            type="text"
            maxLength={500}
            autoComplete="off"
            placeholder="Misal: Alokasi jajan, Pindah ke tabungan darurat"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)] outline-none placeholder:text-[var(--muted)] focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
          />
        </div>
        </FormSection>

        {parsedAmount > 0 && sourceAccountId && targetAccountId && (
          <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] p-3 text-xs text-[var(--text)]" role="status">
            {fmtMoney(parsedAmount)} akan dipindahkan; total uang Anda tidak berubah.
          </div>
        )}

        {err && (
          <div role="alert" aria-live="assertive" className="p-2.5 rounded-2xl bg-rose-500/10 border border-rose-500/20 text-rose-500 text-xs font-medium">
            {err}
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex items-center justify-end gap-2 pt-2 border-t border-[var(--border)]">
          {editingMovement ? (
            confirmDelete ? (
              <div className="mr-auto flex items-center gap-2" role="alert">
                <span className="text-[11px] font-medium text-rose-600">Hapus kedua catatan?</span>
                <button type="button" onClick={() => deleteMutation.mutate()} disabled={deleteMutation.isPending} className="min-h-11 rounded-xl bg-rose-600 px-3 text-xs font-bold text-white disabled:opacity-50">
                  {deleteMutation.isPending ? "Menghapus…" : "Ya, hapus"}
                </button>
                <button type="button" onClick={() => setConfirmDelete(false)} className="min-h-11 rounded-xl px-3 text-xs font-semibold text-[var(--muted)]">Tidak</button>
              </div>
            ) : (
              <button type="button" onClick={() => setConfirmDelete(true)} className="mr-auto min-h-11 rounded-xl border border-rose-500/25 px-3 text-xs font-semibold text-rose-600">Hapus</button>
            )
          ) : null}
          <button type="button" onClick={onClose} className="btn-secondary pressable rounded-xl">Batal</button>
          <button
            type="submit"
            disabled={parsedAmount <= 0 || mutation.isPending || isSuccess}
            className={cn(
              "inline-flex min-h-11 items-center justify-center gap-2 rounded-xl bg-[var(--text)] px-5 text-sm font-semibold text-[var(--surface)] transition-colors hover:opacity-90 motion-reduce:transition-none",
              isSuccess && "bg-[var(--primary)] text-[var(--text)]",
              "disabled:cursor-not-allowed disabled:opacity-40"
            )}
          >
            {isSuccess ? (
              <span className="flex items-center gap-1.5 animate-in fade-in zoom-in-95 duration-150">
                <svg className="w-4 h-4 stroke-[3]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
                ✓ {editingMovement ? "Perubahan Tersimpan" : "Berhasil Dipindahkan!"}
              </span>
            ) : mutation.isPending ? (
              "Memproses…"
            ) : (
              editingMovement ? "Simpan Perubahan" : "Pindahkan Saldo"
            )}
          </button>
        </div>
      </form>
    </Modal>
  );
}
