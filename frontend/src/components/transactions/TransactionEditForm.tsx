"use client";

import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { DebtAllocationEditor, allocationError, totalDebtPayments, type DebtAllocationValue } from "@/components/ui/DebtAllocationEditor";
import { InfoHelp } from "@/components/ui/InfoHelp";
import { api, uploadTransactionReceipt } from "@/lib/api";
import { canConvertExpenseToTopup } from "@/lib/investmentTopups";
import { KAKEIBO_PILLARS, kakeiboPillarInfo, pillarOrUnclassified, type KakeiboPillar } from "@/lib/kakeibo";
import { MERCHANT_MAX_LENGTH, categoryOffsetsSpending, offsetCategoryOptions } from "@/lib/provenance";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots, localDatetimeToISO, toDatetimeLocal } from "@/lib/utils";
import type { LedgerRowDetail } from "./types";

type EditKind = "expense" | "income";

const FIELD = "min-h-11 w-full rounded-xl border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]";
const LABEL = "mb-1 block text-xs font-medium text-[var(--muted)]";

function initialDebtRows(tx: LedgerRowDetail): DebtAllocationValue[] {
  if (tx.type !== "expense") return [];
  const rows = tx.obligation_allocations
    ?? (tx.obligation_id ? [{ obligation_id: tx.obligation_id, obligation_name: tx.obligation_name || "", amount: tx.amount }] : []);
  return rows.map((item) => ({ obligation_id: item.obligation_id, obligation_name: item.obligation_name, amount: formatNumberWithDots(item.amount) }));
}

/**
 * Inline edit of an ordinary row in the transaction drawer (redesign-transactions-workspace D7, D8). It
 * keeps the fields, validation and `PATCH` of the edit modal it replaces. The type control also offers
 * "Pindah saldo" (pick the pair in the list, OD23) and "Top up Investasi" (the existing conversion).
 */
export function TransactionEditForm({
  tx,
  onSaved,
  onCancel,
  onPickPair,
  onConvertToTopup,
}: {
  tx: LedgerRowDetail;
  onSaved: () => void;
  onCancel: () => void;
  onPickPair: () => void;
  onConvertToTopup: () => void;
}) {
  const { bal } = useAppCtx();
  const accounts = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") }).data?.accounts ?? [];
  const categories = useQuery<{ categories: any[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") }).data?.categories ?? [];
  const goals = useQuery<{ goals: any[] }>({ queryKey: queryKeys.goals, queryFn: () => api.get("/goals") }).data?.goals ?? [];
  const obligations = useQuery<{ obligations: any[] }>({ queryKey: queryKeys.obligations, queryFn: () => api.get("/obligations") }).data?.obligations ?? [];

  const originalCategory = categories.find((category) => category.id === tx.category_id);
  const [kind, setKind] = useState<EditKind>(tx.type);
  const [amount, setAmount] = useState(formatNumberWithDots(tx.amount));
  // "" is an unclassified row or category; saving keeps it unclassified instead of storing "need".
  const [pillar, setPillar] = useState<KakeiboPillar | "">(pillarOrUnclassified(tx.kakeibo_type));
  const [pillarOverride, setPillarOverride] = useState(Boolean(
    tx.kakeibo_type && originalCategory?.kakeibo_type && tx.kakeibo_type !== originalCategory.kakeibo_type,
  ));
  const [receipt, setReceipt] = useState<File | null>(null);
  const [receiptRetry, setReceiptRetry] = useState(false);
  const [notes, setNotes] = useState(tx.notes || "");
  const [merchant, setMerchant] = useState(tx.merchant || "");
  const [offsetCategoryId, setOffsetCategoryId] = useState(tx.offset_category_id || "");
  const [accountId, setAccountId] = useState(tx.account_id);
  const [categoryId, setCategoryId] = useState(tx.category_id || "");
  const [goalId, setGoalId] = useState(tx.goal_id || "");
  const [obligationId, setObligationId] = useState(tx.obligation_id || "");
  const [debtRows, setDebtRows] = useState<DebtAllocationValue[]>(initialDebtRows(tx));
  const [date, setDate] = useState(tx.date ? toDatetimeLocal(tx.date) : "");
  const [error, setError] = useState("");

  const offsetsSpending = kind === "income" && categoryOffsetsSpending(categories, categoryId);
  const offsetCategories = offsetCategoryOptions(categories);
  const originalAmounts = Object.fromEntries((tx.obligation_allocations ?? []).map((item) => [item.obligation_id, item.amount]));
  const debtLinked = kind === "expense" && debtRows.some((row) => row.obligation_id);
  const convertible = canConvertExpenseToTopup(tx, accounts);

  const save = useMutation({
    mutationFn: async () => {
      const value = parseInt(amount.replace(/[^0-9]/g, ""), 10);
      if (!value || value <= 0) throw new Error("Nominal harus lebih dari 0.");
      if (!accountId) throw new Error("Pilih rekening.");
      const isoDate = date ? (localDatetimeToISO(date) || new Date(date).toISOString()) : undefined;
      await api.patch(`/transactions/${tx.id}`, {
        type: kind,
        amount: value,
        notes: notes.trim() || null,
        account_id: accountId,
        category_id: categoryId || null,
        goal_id: goalId || null,
        obligation_id: kind === "income" ? (obligationId || null) : debtRows.length === 1 ? debtRows[0].obligation_id : null,
        ...(kind === "expense" && debtRows.length > 1
          ? { obligation_allocations: debtRows.map((row) => ({ obligation_id: row.obligation_id, amount: Number(row.amount.replace(/\./g, "")) })) }
          : {}),
        kakeibo_type: kind === "expense" && pillar ? pillar : null,
        merchant: merchant.trim() || null,
        ...(offsetsSpending ? { offset_category_id: offsetCategoryId || null } : {}),
        date: isoDate,
      });
      if (receipt) {
        try {
          await uploadTransactionReceipt(tx.id, receipt);
        } catch (uploadError) {
          setReceiptRetry(true);
          throw new Error(`Perubahan tersimpan, tetapi bukti gagal diunggah: ${uploadError instanceof Error ? uploadError.message : "unggahan gagal"}`);
        }
      }
    },
    onSuccess: onSaved,
    onError: (failure: Error) => setError(failure.message || "Gagal memperbarui transaksi."),
  });

  const retryReceipt = useMutation({
    mutationFn: async () => {
      if (!receipt) throw new Error("Pilih bukti transaksi untuk diunggah ulang.");
      return uploadTransactionReceipt(tx.id, receipt);
    },
    onSuccess: onSaved,
    onError: (failure: Error) => setError(`Perubahan tetap tersimpan, tetapi bukti belum terunggah: ${failure.message}`),
  });

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    if (kind === "expense") {
      if (debtRows.some((row) => !row.obligation_id || debtRows.filter((item) => item.obligation_id === row.obligation_id).length > 1)) {
        setError("Pilih tagihan yang berbeda untuk setiap baris.");
        document.getElementById("tx-edit-debt-0")?.focus();
        return;
      }
      const splitError = allocationError(debtRows, Number(amount.replace(/\./g, "")), obligations, originalAmounts);
      if (splitError) {
        setError(splitError);
        (document.getElementById("tx-edit-debt-0-amount") || document.getElementById("tx-edit-amount"))?.focus();
        return;
      }
    }
    if (!save.isPending) save.mutate();
  };

  const switchKind = (next: EditKind) => {
    setKind(next);
    setCategoryId("");
    setPillarOverride(false);
    if (next === "income") {
      setGoalId("");
      setDebtRows([]);
    }
  };

  return (
    <form className="ws-tx-edit space-y-4 text-xs" aria-labelledby="tx-edit-title" onSubmit={submit}>
      <h3 id="tx-edit-title" className="text-[15px] font-medium text-[var(--text)]">Ubah transaksi</h3>
      {error && (
        <div role="alert" className="ws-notice ws-tx-error">
          {error}
        </div>
      )}
      <fieldset>
        <legend className={LABEL}>Jenis</legend>
        <div className="ws-seg ws-tx-kind">
          <button type="button" aria-pressed={kind === "expense"} onClick={() => switchKind("expense")}>Pengeluaran</button>
          <button type="button" aria-pressed={kind === "income"} onClick={() => switchKind("income")}>Masuk</button>
          <button type="button" aria-pressed={false} onClick={onPickPair}>Pindah saldo</button>
          {convertible && (
            <button type="button" aria-pressed={false} onClick={onConvertToTopup}>Top up Investasi</button>
          )}
        </div>
        <p className="ws-caption mt-1">
          Pindah saldo menggabungkan transaksi ini dengan pasangannya dari rekening lain.
          {convertible ? " Top up Investasi memindahkan debit ini ke produk reksadana." : ""}
        </p>
      </fieldset>

      <div>
        <label htmlFor="tx-edit-amount" className={LABEL}>{debtLinked ? "Total bayar (IDR)" : "Nominal (IDR)"}</label>
        <input id="tx-edit-amount" type="text" inputMode="numeric" required value={amount} readOnly={debtLinked}
          onChange={(event) => setAmount(formatNumberWithDots(event.target.value))} className={`${FIELD} text-base font-semibold tabular-nums`} />
      </div>
      <div>
        <label htmlFor="tx-edit-account" className={LABEL}>{kind === "expense" ? "Bayar dari" : "Masuk ke"}</label>
        <select id="tx-edit-account" required value={accountId} onChange={(event) => setAccountId(event.target.value)} className={FIELD}>
          <AccountSelectOptions accounts={accounts} formatBalance={bal} allowParentSelection={true} liquidOnly />
        </select>
      </div>
      <div>
        <label htmlFor="tx-edit-category" className={LABEL}>Kategori</label>
        <select id="tx-edit-category" value={categoryId} className={FIELD} onChange={(event) => {
          setCategoryId(event.target.value);
          if (kind === "expense" && !pillarOverride) {
            setPillar(pillarOrUnclassified(categories.find((item) => item.id === event.target.value)?.kakeibo_type));
          }
        }}>
          <option value="">Tanpa kategori</option>
          {categories.filter((category) => category.kind === kind && !category.is_archived).map((category) => (
            <option key={category.id} value={category.id}>{category.name}</option>
          ))}
        </select>
      </div>
      {offsetsSpending && (
        <div>
          <label htmlFor="tx-edit-offset" className={LABEL}>Mengurangi pengeluaran</label>
          <select id="tx-edit-offset" value={offsetCategoryId} onChange={(event) => setOffsetCategoryId(event.target.value)} aria-describedby="tx-edit-offset-help" className={FIELD}>
            <option value="">Belum dipilih</option>
            {offsetCategories.map((category) => <option key={category.id} value={category.id}>{category.name}</option>)}
          </select>
          <p id="tx-edit-offset-help" className="ws-caption mt-1">Uang pengganti ini bukan pemasukan. Pilih kategori pengeluaran yang dibayar kembali.</p>
        </div>
      )}
      {kind === "expense" && (
        <div>
          <div className="mb-1 flex items-center gap-1">
            <label htmlFor="tx-edit-pillar" className="text-xs font-medium text-[var(--muted)]">Pilar Kakeibo</label>
            <InfoHelp label="Pilar Kakeibo">Mengikuti pilar kategori, kecuali pilar lain dipilih untuk transaksi ini.</InfoHelp>
          </div>
          <select id="tx-edit-pillar" value={pillar} className={FIELD} onChange={(event) => { setPillar(event.target.value as KakeiboPillar | ""); setPillarOverride(true); }}>
            {pillar === "" && <option value="">Belum diklasifikasi</option>}
            {KAKEIBO_PILLARS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
          </select>
          <p className="ws-caption mt-1">{pillar ? kakeiboPillarInfo(pillar)?.help : "Pilih pilar agar transaksi ini masuk ke ringkasan Kakeibo."}</p>
        </div>
      )}
      <div>
        <label htmlFor="tx-edit-goal" className={LABEL}>Kaitkan ke target (opsional)</label>
        <select id="tx-edit-goal" value={goalId} className={FIELD} onChange={(event) => {
          setGoalId(event.target.value);
          if (event.target.value) { setObligationId(""); setDebtRows([]); }
        }}>
          <option value="">Tidak ada</option>
          {goals.filter((goal) => !goal.is_archived).map((goal) => (
            <option key={goal.id} value={goal.id}>{goal.name} ({bal(goal.current_amount)} / {bal(goal.target_amount)})</option>
          ))}
        </select>
        {goalId && <p className="ws-caption mt-1">Kaitan ini tidak menambah progres target. Target yang terhubung ke rekening mengikuti saldonya.</p>}
      </div>
      {kind === "expense" ? (
        <DebtAllocationEditor idPrefix="tx-edit" debts={obligations} rows={debtRows} total={Number(amount.replace(/\./g, "")) || 0} currency={bal}
          originalAmounts={originalAmounts} onChange={(rows) => {
            setDebtRows(rows);
            if (rows.length) setGoalId("");
            if (rows.some((row) => row.obligation_id)) setAmount(formatNumberWithDots(totalDebtPayments(rows)));
            setError("");
          }} />
      ) : (
        <div>
          <label htmlFor="tx-edit-obligation" className={LABEL}>Kaitkan ke tagihan atau utang (opsional)</label>
          <select id="tx-edit-obligation" value={obligationId} className={FIELD} onChange={(event) => {
            setObligationId(event.target.value);
            if (event.target.value) setGoalId("");
          }}>
            <option value="">Tidak ada</option>
            {obligations.filter((item) => !item.is_archived && item.remaining_amount > 0).map((item) => (
              <option key={item.id} value={item.id}>{item.name} ({bal(item.remaining_amount)} tersisa)</option>
            ))}
          </select>
          {obligationId && <p className="ws-caption mt-1">Pemasukan ini menambah kembali sisa tagihan.</p>}
        </div>
      )}
      <div>
        <label htmlFor="tx-edit-date" className={LABEL}>Tanggal dan waktu</label>
        <input id="tx-edit-date" type="datetime-local" step={1} required value={date} onChange={(event) => setDate(event.target.value)} className={FIELD} />
      </div>
      <div>
        <label htmlFor="tx-edit-merchant" className={LABEL}>Merchant atau tempat (opsional)</label>
        <input id="tx-edit-merchant" type="text" autoComplete="off" maxLength={MERCHANT_MAX_LENGTH} value={merchant}
          onChange={(event) => setMerchant(event.target.value)} placeholder="Nama warung, toko, atau aplikasi" className={FIELD} />
      </div>
      <div>
        <label htmlFor="tx-edit-notes" className={LABEL}>Catatan</label>
        <input id="tx-edit-notes" type="text" autoComplete="off" value={notes} onChange={(event) => setNotes(event.target.value)} placeholder="Tambahkan catatan" className={FIELD} />
      </div>
      <div>
        <label htmlFor="tx-edit-receipt" className={LABEL}>Ganti atau tambahkan bukti</label>
        <input id="tx-edit-receipt" type="file" accept="image/jpeg,image/png,image/webp,application/pdf" onChange={(event) => setReceipt(event.target.files?.[0] ?? null)} className={`${FIELD} text-xs`} />
        {tx.receipt_path && <p className="ws-caption mt-1">Bukti saat ini diganti hanya setelah unggahan baru berhasil.</p>}
      </div>
      {receiptRetry && (
        <button type="button" className="ws-btn" disabled={retryReceipt.isPending || !receipt} onClick={() => retryReceipt.mutate()}>
          {retryReceipt.isPending ? "Mengunggah…" : "Coba unggah bukti lagi"}
        </button>
      )}
      <div className="ws-tx-actions">
        <button type="submit" className="ws-btn ws-btn-ink" disabled={save.isPending}>{save.isPending ? "Menyimpan…" : "Simpan"}</button>
        <button type="button" className="ws-btn" onClick={onCancel}>Batal</button>
      </div>
    </form>
  );
}
