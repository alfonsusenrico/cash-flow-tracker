"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, deleteMovement, splitMovement, updateMovement } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { FormSection } from "@/components/ui/FormField";
import { queryKeys } from "@/lib/queryKeys";
import { isCreditAccountChoice, listCreditAccountChoices, listLiquidAccountChoices } from "@/lib/accountOptions";
import { movementFormSchema } from "@/lib/movementFormSchema";

interface InternalMovementModalProps {
  open: boolean;
  onClose: () => void;
  editingMovement: {
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
  editingMovement,
}: InternalMovementModalProps) {
  const qc = useQueryClient();
  const inputRef = useRef<HTMLInputElement>(null);

  const [sourceAccountId, setSourceAccountId] = useState(editingMovement?.sourceAccountId ?? "");
  const [targetAccountId, setTargetAccountId] = useState(editingMovement?.targetAccountId ?? "");
  const [amountStr, setAmountStr] = useState("");
  const [notes, setNotes] = useState("");
  const [txDate, setTxDate] = useState(() => toDatetimeLocal(new Date()));
  const [err, setErr] = useState("");
  const [isSuccess, setIsSuccess] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [confirmSplit, setConfirmSplit] = useState(false);

  // Fetch accounts
  const { data: accountsData } = useQuery<{ accounts: any[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);
  // A credit account may be either side of a movement, with cash on the other side (add-credit-accounts D6).
  const movementAccounts = useMemo(
    () => [...liquidAccounts, ...listCreditAccountChoices(accounts)],
    [accounts, liquidAccounts],
  );
  const isCreditId = (accountId: string) => movementAccounts.some(
    (account) => String(account.id ?? account.account_id ?? "") === accountId && isCreditAccountChoice(account),
  );
  const sourceIsCredit = isCreditId(sourceAccountId);

  // Set default accounts when loaded
  useEffect(() => {
    if (liquidAccounts.length === 0) return;
    const getAccountId = (account: any) => String(account.id ?? account.account_id ?? "");
    const firstAccountId = getAccountId(liquidAccounts[0]);
    setSourceAccountId((current) =>
      movementAccounts.some((account) => getAccountId(account) === current)
        ? current
        : firstAccountId,
    );
    setTargetAccountId((current) => {
      const source = movementAccounts.find((account) => getAccountId(account) === sourceAccountId);
      const sourceId = source ? sourceAccountId : firstAccountId;
      const targets = source && isCreditAccountChoice(source) ? liquidAccounts : movementAccounts;
      if (current !== sourceId && targets.some((account) => getAccountId(account) === current)) {
        return current;
      }
      const target = liquidAccounts.find((account) => getAccountId(account) !== sourceId);
      return target ? getAccountId(target) : "";
    });
  }, [liquidAccounts, movementAccounts, sourceAccountId]);

  useEffect(() => {
    if (open) {
      const requestedSource = editingMovement?.sourceAccountId ?? "";
      const requestedTarget = editingMovement?.targetAccountId ?? "";
      const getAccountId = (account: any) => String(account.id ?? account.account_id ?? "");
      const sourceId = movementAccounts.some((account) => getAccountId(account) === requestedSource)
        ? requestedSource
        : getAccountId(liquidAccounts[0] ?? {});
      const targetId = movementAccounts.some((account) => getAccountId(account) === requestedTarget && requestedTarget !== sourceId)
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
      setConfirmSplit(false);
    }
  }, [editingMovement, liquidAccounts, movementAccounts, open]);

  const parsedAmount = useMemo(() => {
    const raw = amountStr.replace(/[^0-9]/g, "");
    return raw ? parseInt(raw, 10) : 0;
  }, [amountStr]);

  const mutation = useMutation({
    mutationFn: async () => {
      if (!editingMovement) throw new Error("Pemindahan saldo tidak ditemukan");
      const validation = movementFormSchema.safeParse({
        sourceAccountId,
        targetAccountId,
        amount: parsedAmount,
        date: txDate,
        notes: notes.trim(),
      });
      if (!validation.success) throw new Error(validation.error.issues[0].message);
      if (isCreditId(validation.data.sourceAccountId) && isCreditId(validation.data.targetAccountId)) {
        throw new Error("Salah satu sisi harus rekening bank, e-wallet, atau tunai.");
      }

      const payload = {
        source_account_id: validation.data.sourceAccountId,
        target_account_id: validation.data.targetAccountId,
        amount: validation.data.amount,
        notes: validation.data.notes || null,
        date: localDatetimeToISO(validation.data.date) || new Date().toISOString(),
      };
      return updateMovement(editingMovement.id, payload);
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

  const splitMutation = useMutation({
    mutationFn: () => {
      if (!editingMovement) throw new Error("Pemindahan saldo tidak ditemukan");
      return splitMovement(editingMovement.id);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (error: any) => setErr(error?.message || "Gagal memisahkan pemindahan saldo"),
  });

  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    mutation.mutate();
  };

  return (
    <Modal open={open && Boolean(editingMovement)} onClose={onClose} title="Ubah pindah saldo">
      <form onSubmit={handleSubmit} className="space-y-4 pt-1">
        <FormSection title="Arah pemindahan" description="Saldo berpindah antar-rekening, termasuk membayar kredit. Ini bukan pemasukan atau pengeluaran; posisi investasi berubah melalui Beli/Jual." className="border-t-0 pt-0">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div>
            <label htmlFor="movement-source-account" className="block text-xs font-medium text-[var(--muted)] mb-1">
              Dari rekening
            </label>
            <select
              id="movement-source-account"
              name="source_account_id"
              value={sourceAccountId}
              onChange={(e) => setSourceAccountId(e.target.value)}
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
            >
              <AccountSelectOptions accounts={accounts} formatBalance={fmtMoney} allowParentSelection={true} liquidOnly includeCredit />
            </select>
          </div>

          <div>
            <label htmlFor="movement-target-account" className="block text-xs font-medium text-[var(--muted)] mb-1">
              Ke rekening
            </label>
            <select
              id="movement-target-account"
              name="target_account_id"
              value={targetAccountId}
              onChange={(e) => setTargetAccountId(e.target.value)}
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
            >
              <AccountSelectOptions
                accounts={accounts}
                formatBalance={fmtMoney}
                allowParentSelection={true}
                excludeAccountId={sourceAccountId}
                liquidOnly
                includeCredit={!sourceIsCredit}
              />
            </select>
          </div>
        </div>
        </FormSection>

        <FormSection title="Nominal dan waktu">
        <div className="rounded-xl border border-[var(--line-strong)] bg-[var(--surface)] p-4">
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
            className="mt-1 w-full bg-transparent text-3xl font-semibold tracking-tight text-[var(--text)] outline-none tabular placeholder:text-[var(--muted)]/50"
          />
        </div>

        <div>
          <label htmlFor="movement-date" className="block text-xs font-medium text-[var(--muted)] mb-1">
            Waktu Pemindahan
          </label>
          <input
            id="movement-date"
            name="date"
            type="datetime-local"
            step={1}
            value={txDate}
            onChange={(e) => setTxDate(e.target.value)}
            className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
          />
        </div>

        <div>
          <label htmlFor="movement-notes" className="block text-xs font-medium text-[var(--muted)] mb-1">
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
            className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)] outline-none placeholder:text-[var(--muted)]"
          />
        </div>
        </FormSection>

        {parsedAmount > 0 && sourceAccountId && targetAccountId && (
          <div className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)] p-3 text-xs text-[var(--text)]" role="status">
            {fmtMoney(parsedAmount)} akan dipindahkan; total uang tidak berubah.
          </div>
        )}

        {err && (
          <div role="alert" aria-live="assertive" className="p-2.5 rounded-xl bg-[var(--surface-2)] border border-[var(--line)] text-out-fg text-xs font-medium">
            {err}
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex items-center justify-end gap-2 pt-2 border-t border-[var(--line)]">
          {editingMovement ? (
            confirmDelete ? (
              <div className="mr-auto flex items-center gap-2" role="alert">
                <span className="text-xs font-medium text-out-fg">Hapus kedua catatan?</span>
                <button type="button" onClick={() => deleteMutation.mutate()} disabled={deleteMutation.isPending} className="min-h-11 rounded-md bg-[var(--danger-solid)] px-3 text-xs font-semibold text-[var(--danger-contrast)] hover:bg-[var(--danger-solid-hover)] disabled:opacity-50">
                  {deleteMutation.isPending ? "Menghapus…" : "Ya, hapus"}
                </button>
                <button type="button" onClick={() => setConfirmDelete(false)} className="min-h-11 rounded-md px-3 text-xs font-semibold text-[var(--muted)]">Tidak</button>
              </div>
            ) : confirmSplit ? (
              <div className="mr-auto flex items-center gap-2" role="alert">
                <span className="text-xs font-medium text-[var(--text)]">Jadikan dua transaksi terpisah?</span>
                <button type="button" onClick={() => splitMutation.mutate()} disabled={splitMutation.isPending} className="min-h-11 rounded-md bg-[var(--text)] px-3 text-xs font-semibold text-[var(--surface)] disabled:opacity-50">
                  {splitMutation.isPending ? "Memisahkan…" : "Ya, pisahkan"}
                </button>
                <button type="button" onClick={() => setConfirmSplit(false)} className="min-h-11 rounded-md px-3 text-xs font-semibold text-[var(--muted)]">Tidak</button>
              </div>
            ) : (
              <div className="mr-auto flex items-center gap-2">
                <button type="button" onClick={() => setConfirmDelete(true)} className="min-h-11 rounded-md border border-[var(--line)] px-3 text-xs font-semibold text-out-fg">Hapus</button>
                <button
                  type="button"
                  onClick={() => setConfirmSplit(true)}
                  title="Pisahkan jika dua catatan ini ternyata bukan satu pemindahan saldo"
                  className="min-h-11 rounded-md border border-[var(--line)] px-3 text-xs font-semibold text-[var(--text)]"
                >
                  Pisahkan
                </button>
              </div>
            )
          ) : null}
          <button type="button" onClick={onClose} className="ws-btn rounded-md">Batal</button>
          <button
            type="submit"
            disabled={parsedAmount <= 0 || mutation.isPending || isSuccess}
            className={cn(
              "ws-btn ws-btn-accent",
              "disabled:cursor-not-allowed disabled:opacity-40"
            )}
          >
            {isSuccess ? (
              <span className="flex items-center gap-1.5 animate-in fade-in zoom-in-95 duration-150">
                <svg className="w-4 h-4 stroke-[3]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
                ✓ Perubahan Tersimpan
              </span>
            ) : mutation.isPending ? (
              "Memproses…"
            ) : (
              "Simpan Perubahan"
            )}
          </button>
        </div>
      </form>
    </Modal>
  );
}
