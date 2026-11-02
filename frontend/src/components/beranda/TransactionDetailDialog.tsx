"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { Modal } from "@/components/ui/Modal";
import { api } from "@/lib/api";
import { kakeiboLabel } from "@/lib/kakeibo";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import type { ActivityRow } from "./types";

/** The Beranda row detail with delete, kept from before the redesign until phase 3 brings the drawer. */
export function TransactionDetailDialog({ selectedTx, onClose }: { selectedTx: ActivityRow | null; onClose: () => void }) {
  const { bal } = useAppCtx();
  const qc = useQueryClient();
  const setSelectedTx = (value: null) => value === null && onClose();
  const deleteTxMutation = useMutation({
    mutationFn: (txId: string) => api.del(`/transactions/${txId}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.review.all });
      onClose();
    },
  });

  return (
    <>
      {/* Transaction Detail & Delete Modal */}
      {selectedTx && (
        <Modal
          open={Boolean(selectedTx)}
          onClose={() => setSelectedTx(null)}
          title="Detail Transaksi"
        >
          <div className="space-y-4 pt-2 text-xs">
            <div className="p-4 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] flex items-center justify-between">
              <div>
                <div className="font-semibold text-sm text-[var(--text)]">
                  {selectedTx.notes ||
                    (selectedTx.investment_topup_id
                      ? "Top up Investasi"
                      : (selectedTx.type as string) === "transfer" ||
                        selectedTx.category_name === "Internal Movement" ||
                        selectedTx.is_excluded_from_budget
                      ? "Pindah Saldo"
                      : selectedTx.category_name || "Umum")}
                </div>
                <div className="text-[var(--muted)] mt-0.5">
                  {new Date(selectedTx.date).toLocaleString("id-ID")}
                </div>
              </div>
              <div
                className={cn(
                  "text-base font-bold tabular",
                  selectedTx.investment_topup_id
                    ? "text-investment"
                    : (selectedTx.type as string) === "transfer" ||
                      selectedTx.category_name === "Internal Movement" ||
                      selectedTx.is_excluded_from_budget
                    ? "text-sky-500"
                    : selectedTx.type === "income"
                    ? "text-emerald-500"
                    : "text-rose-500"
                )}
              >
                {selectedTx.type === "income"
                  ? "+"
                  : selectedTx.type === "expense"
                  ? "-"
                  : ""}
                {bal(selectedTx.amount)}
              </div>
            </div>

            <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
              <span className="text-[var(--muted)]">Rekening</span>
              <span className="font-semibold text-[var(--text)]">
                {selectedTx.account_name}
              </span>
            </div>

            {(selectedTx.transfer_target_account_name ?? selectedTx.target_account_name) && (
              <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
                <span className="text-[var(--muted)]">Rekening Tujuan</span>
                <span className="font-semibold text-[var(--text)]">
                  {(selectedTx.transfer_target_account_name ?? selectedTx.target_account_name)}
                </span>
              </div>
            )}

            {selectedTx.kakeibo_type && (
              <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
                <span className="text-[var(--muted)]">Pilar Kakeibo</span>
                <span className="font-semibold uppercase text-[var(--text)]">
                  {kakeiboLabel(selectedTx.kakeibo_type)}
                </span>
              </div>
            )}

            <div className="flex justify-between items-center py-2 border-b border-[var(--border)]">
              <span className="text-[var(--muted)]">Jenis Transaksi</span>
              <span className="font-semibold uppercase text-[var(--text)]">
                {selectedTx.investment_topup_id
                  ? "Top up Investasi"
                  : (selectedTx.type as string) === "transfer" ||
                    selectedTx.category_name === "Internal Movement" ||
                    selectedTx.is_excluded_from_budget
                  ? selectedTx.type === "income"
                    ? "Pindah Saldo (Masuk)"
                    : "Pindah Saldo (Keluar)"
                  : selectedTx.type === "income"
                  ? "Uang Masuk"
                  : "Uang Keluar"}
              </span>
            </div>

            <div className="pt-4 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setSelectedTx(null)}
                className="px-4 py-2 rounded-xl border border-[var(--border)] text-[var(--muted)] cursor-pointer"
              >
                Tutup
              </button>
              <ConfirmActionButton
                label="Hapus transaksi"
                confirmation="Hapus transaksi ini? Saldo akun akan diperbarui."
                onConfirm={() => deleteTxMutation.mutate(selectedTx.id)}
                disabled={deleteTxMutation.isPending}
                className="px-4 py-2 rounded-xl bg-rose-500/10 text-rose-500 border border-rose-500/20 font-semibold hover:bg-rose-500/20 cursor-pointer"
              >
                {deleteTxMutation.isPending ? "Menghapus..." : "Hapus Transaksi"}
              </ConfirmActionButton>
            </div>
          </div>
        </Modal>
      )}

    </>
  );
}
