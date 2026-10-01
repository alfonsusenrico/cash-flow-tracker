"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { InvestmentAccount, InvestmentScheduleDraft, InvestmentTopup } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, localDatetimeToISO, parseNumberFromDots, toDatetimeLocal } from "@/lib/utils";
import { Modal } from "./Modal";
import { ConfirmActionButton } from "./ConfirmActionButton";

interface Props {
  open: boolean;
  onClose: () => void;
  product: InvestmentAccount | null;
  allAccounts: InvestmentAccount[];
  editingTopup?: InvestmentTopup | null;
  onSchedule?: (draft: InvestmentScheduleDraft) => void;
}

export function InvestmentTopupModal({ open, onClose, product, allAccounts, editingTopup, onSchedule }: Props) {
  const qc = useQueryClient();
  const [sourceId, setSourceId] = useState("");
  const [amount, setAmount] = useState("");
  const [date, setDate] = useState("");
  const [notes, setNotes] = useState("");
  const [error, setError] = useState("");
  const retryKey = useRef("");
  const initialized = useRef<string | null>(null);
  const funding = useMemo(() => [...new Map(listLiquidAccountChoices(allAccounts)
    .filter((account) => !account.is_archived)
    .map((account) => [account.id, account])).values()], [allAccounts]);

  useEffect(() => {
    if (!open) {
      initialized.current = null;
      return;
    }
    if (!product || initialized.current === (editingTopup?.id ?? product.id)) return;
    initialized.current = editingTopup?.id ?? product.id;
    retryKey.current = crypto.randomUUID();
    setAmount(editingTopup ? formatNumberWithDots(editingTopup.amount) : "");
    setDate(toDatetimeLocal(editingTopup?.date ?? new Date().toISOString()));
    setNotes(editingTopup?.notes ?? "");
    setError("");
    const parent = allAccounts.find((account) => account.id === product.parent_id);
    const preferred = [product.default_funding_account_id, parent?.default_funding_account_id]
      .find((id) => funding.some((account) => account.id === id));
    setSourceId(editingTopup?.source_account_id ?? preferred ?? "");
  }, [open, product, allAccounts, funding, editingTopup]);

  const refresh = () => {
    [queryKeys.accounts, queryKeys.transactions.all, queryKeys.dashboard.all, queryKeys.pulse,
      queryKeys.insights, queryKeys.recurring.all].forEach((key) => qc.invalidateQueries({ queryKey: key }));
  };
  const mutation = useMutation({
    mutationFn: async (remove: boolean) => {
      if (remove && editingTopup) return api.del(`/investment-topups/${editingTopup.id}`);
      const parsedAmount = parseNumberFromDots(amount);
      const isoDate = localDatetimeToISO(date);
      if (!Number.isSafeInteger(parsedAmount) || parsedAmount <= 0) throw new Error("Masukkan nominal rupiah lebih dari 0.");
      if (!isoDate) throw new Error("Pilih tanggal dan waktu debit yang valid.");
      if (!product || !sourceId) throw new Error("Pilih rekening sumber dana.");
      const correction = { amount: parsedAmount, date: isoDate, notes: notes.trim() };
      return editingTopup
        ? api.patch(`/investment-topups/${editingTopup.id}`, correction)
        : api.post(`/investment-topups`, {
          ...correction, source_account_id: sourceId, target_account_id: product.id, idempotency_key: retryKey.current,
        });
    },
    onSuccess: () => { refresh(); onClose(); },
    onError: (failure: Error) => setError(failure.message || "Top up gagal dicatat. Periksa saldo dan coba lagi."),
  });
  if (!product) return null;
  const inputClass = "min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]";
  const busy = mutation.isPending;

  return (
    <Modal open={open} onClose={() => { if (!busy) onClose(); }} title={editingTopup ? "Ubah Top up Investasi" : "Top up Investasi"}>
      <form className="space-y-4" onSubmit={(event) => { event.preventDefault(); setError(""); mutation.mutate(false); }}>
        <div className="rounded-xl border border-purple-200 bg-purple-50 p-3 text-purple-900 dark:border-purple-800 dark:bg-purple-950 dark:text-purple-100">
          <p className="text-xs">Produk reksadana</p>
          <p className="break-words font-semibold">{product.name}</p>
          <p className="mt-1 text-xs">Catat jumlah yang dibayarkan. Nilai investasi menjadi estimasi hingga Anda memperbarui nilai dari Bibit.</p>
        </div>
        {error && <p role="alert" className="text-sm text-rose-700 dark:text-rose-300">{error}</p>}
        <div>
          {editingTopup
            ? <p className="mb-1 text-xs font-medium">Dari rekening</p>
            : <label htmlFor="topup-source" className="mb-1 block text-xs font-medium">Dari rekening</label>}
          {editingTopup ? <p className="text-sm">{allAccounts.find((account) => account.id === sourceId)?.name ?? "Rekening sumber"}</p> : (
            <select id="topup-source" className={inputClass} value={sourceId} onChange={(event) => setSourceId(event.target.value)} required disabled={busy}>
              <option value="">Pilih rekening sumber…</option>
              {funding.map((account) => <option key={account.id} value={account.id}>{account.name}</option>)}
            </select>
          )}
        </div>
        <div>
          <label htmlFor="topup-amount" className="mb-1 block text-xs font-medium">Nominal top up (IDR)</label>
          <input id="topup-amount" data-autofocus inputMode="numeric" className={`${inputClass} tabular-nums`} value={amount} onChange={(event) => setAmount(formatNumberWithDots(event.target.value))} placeholder="112.590" required disabled={busy} />
        </div>
        <div>
          <label htmlFor="topup-date" className="mb-1 block text-xs font-medium">Tanggal dan waktu debit</label>
          <input id="topup-date" type="datetime-local" step="1" className={`${inputClass} min-w-0 tabular-nums`} value={date} onChange={(event) => setDate(event.target.value)} required disabled={busy} />
        </div>
        <div>
          <label htmlFor="topup-notes" className="mb-1 block text-xs font-medium">Catatan (opsional)</label>
          <textarea id="topup-notes" maxLength={500} className={inputClass} value={notes} onChange={(event) => setNotes(event.target.value)} disabled={busy} />
        </div>
        {onSchedule && !editingTopup && (
          <button type="button" className="min-h-11 text-sm font-semibold underline underline-offset-4 disabled:opacity-50" disabled={busy || !sourceId || parseNumberFromDots(amount) <= 0} onClick={() => {
            onSchedule({ sourceAccountId: sourceId, targetAccountId: product.id, amount: parseNumberFromDots(amount), productName: product.name });
          }}>Jadwalkan bulanan</button>
        )}
        {!editingTopup && <p className="text-xs text-[var(--text-secondary)]">Catat setelah debit berhasil. Pastikan debit yang sama belum tercatat di buku kas.</p>}
        {editingTopup && <p className="text-xs text-[var(--text-secondary)]">Koreksi nominal menyesuaikan modal dan estimasi nilai. Periksa kembali nilai aktual di Bibit setelah mengubahnya.</p>}
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-[var(--border)] pt-3">
          {editingTopup && <ConfirmActionButton label="Hapus top up" confirmation="Hapus top up? Modal dan estimasi nilai investasi ikut berkurang. Top up dari pengeluaran tercatat kembali menjadi pengeluaran biasa; top up lainnya mengembalikan saldo sumber." onConfirm={() => mutation.mutate(true)} disabled={busy} className="min-h-11 rounded-xl border border-rose-300 px-3 text-sm text-rose-700 dark:text-rose-300">Hapus</ConfirmActionButton>}
          <button type="button" onClick={onClose} disabled={busy} className="min-h-11 rounded-xl border border-[var(--border)] px-3 text-sm">Batal</button>
          <button type="submit" disabled={busy} className="min-h-11 rounded-xl bg-[#1E201E] px-4 text-sm font-semibold text-white disabled:opacity-50">{busy ? "Menyimpan…" : editingTopup ? "Simpan perubahan" : "Catat top up"}</button>
        </div>
      </form>
    </Modal>
  );
}
