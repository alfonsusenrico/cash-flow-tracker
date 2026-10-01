"use client";

import { useEffect, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { InvestmentAccount } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import { fmtIDR } from "@/lib/utils";
import { Modal } from "./Modal";

interface Props {
  product: InvestmentAccount | null;
  onClose: () => void;
}

export function AmountTrackingSwitchModal({ product, onClose }: Props) {
  const qc = useQueryClient();
  const [error, setError] = useState("");
  useEffect(() => setError(""), [product?.id]);
  const mutation = useMutation({
    mutationFn: (productId: string) => api.post(`/accounts/${productId}/amount-tracking`),
    onSuccess: () => {
      [queryKeys.accounts, queryKeys.dashboard.all, queryKeys.pulse, queryKeys.insights]
        .forEach((key) => qc.invalidateQueries({ queryKey: key }));
      onClose();
    },
    onError: (failure: Error) => setError(failure.message || "Pelacakan produk gagal diubah. Coba lagi."),
  });
  if (!product) return null;
  const busy = mutation.isPending;

  return (
    <Modal open onClose={() => { if (!busy) onClose(); }} title="Ubah ke pelacakan nominal">
      <div className="space-y-4">
        <div className="rounded-xl border border-purple-200 bg-purple-50 p-3 text-purple-900 dark:border-purple-800 dark:bg-purple-950 dark:text-purple-100">
          <p className="text-xs">Produk reksadana</p>
          <p className="break-words font-semibold">{product.name}</p>
        </div>
        <dl className="grid grid-cols-2 gap-3 text-sm">
          <div>
            <dt className="text-xs text-[var(--text-secondary)]">Nilai sekarang</dt>
            <dd className="font-semibold tabular-nums">{fmtIDR(product.balance ?? 0)}</dd>
          </div>
          <div>
            <dt className="text-xs text-[var(--text-secondary)]">Modal</dt>
            <dd className="font-semibold tabular-nums">{product.cost_basis == null ? "Belum diketahui" : fmtIDR(product.cost_basis)}</dd>
          </div>
        </dl>
        <p className="text-sm">
          Nilai dan modal di atas tetap. Jumlah unit dan harga rata-rata per unit tidak lagi dicatat, lalu produk memakai Top up dan Update Nilai dalam rupiah, seperti tampilan Bibit.
        </p>
        <p className="text-sm font-semibold">Perubahan ini tidak dapat dibatalkan.</p>
        {error && <p role="alert" className="text-sm text-rose-700 dark:text-rose-300">{error}</p>}
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-[var(--border)] pt-3">
          <button type="button" onClick={onClose} disabled={busy} className="min-h-11 rounded-xl border border-[var(--border)] px-3 text-sm">Batal</button>
          <button type="button" data-autofocus onClick={() => { setError(""); mutation.mutate(product.id); }} disabled={busy} className="min-h-11 rounded-xl bg-[#1E201E] px-4 text-sm font-semibold text-white disabled:opacity-50">
            {busy ? "Menyimpan…" : "Ubah ke nominal"}
          </button>
        </div>
      </div>
    </Modal>
  );
}
