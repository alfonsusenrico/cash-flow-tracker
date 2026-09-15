"use client";

import { useState, useEffect, useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { RecurringRule, PayrollItemPayload } from "@/types/recurring";

interface Props {
  open: boolean;
  onClose: () => void;
  onOpenRulesManager?: () => void;
}

interface EditableItem {
  id: string; // rule id or temp id
  enabled: boolean;
  rule_id?: string;
  source_account_id: string;
  target_account_id: string;
  target_account_name: string;
  name: string;
  amountStr: string;
  notes?: string;
}

export function PayrollAllocationModal({ open, onClose, onOpenRulesManager }: Props) {
  const qc = useQueryClient();

  const [items, setItems] = useState<EditableItem[]>([]);
  const [sourceAccountId, setSourceAccountId] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [successCount, setSuccessCount] = useState<number | null>(null);

  // Fetch accounts
  const { data: accountsData } = useQuery<any>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  // Fetch payroll recurring rules
  const { data: rulesData } = useQuery<{ ok: boolean; rules: RecurringRule[] }>({
    queryKey: ["recurring-rules", "payroll"],
    queryFn: () => api.get("/recurring?is_payroll=true"),
    enabled: open,
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const rules = useMemo(() => rulesData?.rules ?? [], [rulesData?.rules]);

  // Populate items when rules load
  useEffect(() => {
    if (rules.length > 0) {
      // Find common source account
      setSourceAccountId(rules[0].source_account_id);
      setItems(
        rules.map((r) => ({
          id: r.id,
          enabled: r.is_active,
          rule_id: r.id,
          source_account_id: r.source_account_id,
          target_account_id: r.target_account_id || "",
          target_account_name: r.target_account_name || "Rekening Tujuan",
          name: r.name,
          amountStr: formatNumberWithDots(r.amount),
          notes: r.notes || "Alokasi Gaji Bulanan",
        }))
      );
    } else if (accounts.length > 0 && !sourceAccountId) {
      setSourceAccountId(accounts[0].id);
    }
  }, [rules, accounts, sourceAccountId]);

  // Source account details
  const sourceAccount = accounts.find((a: any) => a.id === sourceAccountId);

  // Calculate totals
  const totalAllocated = items
    .filter((i) => i.enabled)
    .reduce((sum, i) => sum + parseNumberFromDots(i.amountStr), 0);

  // Batch execute mutation
  const batchMutation = useMutation({
    mutationFn: async () => {
      setError("");
      const enabledItems = items.filter((i) => i.enabled);
      if (enabledItems.length === 0) throw new Error("Pilih minimal satu alokasi untuk dijalankan");

      const payloadItems: PayrollItemPayload[] = enabledItems.map((item) => {
        const amt = parseNumberFromDots(item.amountStr);
        if (amt <= 0) throw new Error(`Nominal untuk ${item.name} harus lebih dari 0`);
        return {
          rule_id: item.rule_id,
          source_account_id: sourceAccountId,
          target_account_id: item.target_account_id,
          amount: amt,
          notes: item.notes || "Alokasi Gaji Bulanan",
        };
      });

      return api.post("/recurring/payroll/execute", { items: payloadItems });
    },
    onSuccess: (res: any) => {
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-net-worth"] });
      qc.invalidateQueries({ queryKey: ["transactions"] });
      qc.invalidateQueries({ queryKey: ["recurring-rules"] });
      qc.invalidateQueries({ queryKey: ["recurring-pending"] });
      setSuccessCount(res?.allocated_items_count || items.filter((i) => i.enabled).length);
    },
    onError: (err: any) => {
      setError(err?.message || "Gagal menjalankan alokasi gaji");
    },
  });

  const handleAmountChange = (id: string, val: string) => {
    setItems((prev) =>
      prev.map((item) =>
        item.id === id ? { ...item, amountStr: formatNumberWithDots(val) } : item
      )
    );
  };

  const handleToggleItem = (id: string) => {
    setItems((prev) =>
      prev.map((item) => (item.id === id ? { ...item, enabled: !item.enabled } : item))
    );
  };

  const handleResetModal = () => {
    setSuccessCount(null);
    setError("");
    onClose();
  };

  return (
    <Modal open={open} onClose={handleResetModal} title="Alokasi Gaji ke Rekening & Kantong">
      {successCount !== null ? (
        <div className="py-8 px-4 text-center space-y-4">
          <div className="h-16 w-16 mx-auto rounded-full bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 flex items-center justify-center">
            <Icon name="check" className="h-8 w-8" />
          </div>
          <div className="space-y-1">
            <h3 className="text-base font-bold text-[var(--text)]">Alokasi Gaji Berhasil!</h3>
            <p className="text-xs text-[var(--muted)]">
              Sebanyak <strong>{successCount} transfer alokasi</strong> telah selesai dibukukan ke rekening dan kantong Anda. Total nominal: <strong className="font-bold tabular text-[var(--text)]">{fmtMoney(totalAllocated)}</strong>.
            </p>
          </div>
          <button
            type="button"
            onClick={handleResetModal}
            className="px-6 py-2.5 rounded-xl bg-income hover:bg-income-hover text-white text-xs font-bold shadow-xs transition-all active:scale-95"
          >
            Selesai
          </button>
        </div>
      ) : (
        <div className="space-y-4 pt-1 text-xs">
          {error && (
            <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
              {error}
            </div>
          )}

          {/* Salary Source Account Banner */}
          <div className="p-3.5 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-semibold text-[var(--muted)] uppercase">
                Rekening Sumber Gaji
              </span>
              {sourceAccount && (
                <span className="font-bold tabular text-xs text-emerald-600 dark:text-emerald-400">
                  Saldo: {fmtMoney(sourceAccount.balance)}
                </span>
              )}
            </div>
            <select
              value={sourceAccountId}
              onChange={(e) => setSourceAccountId(e.target.value)}
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm font-bold text-[var(--text)]"
            >
              <AccountSelectOptions accounts={accounts} allowParentSelection={true} />
            </select>
          </div>

          {/* Destination Allocations List */}
          <div className="space-y-2">
            <div className="flex items-center justify-between px-1">
              <span className="font-bold text-xs text-[var(--text)]">
                Daftar Alokasi Rekening & Kantong ({items.filter((i) => i.enabled).length}/{items.length})
              </span>
              {onOpenRulesManager && (
                <button
                  type="button"
                  onClick={() => {
                    onClose();
                    onOpenRulesManager();
                  }}
                  className="text-[11px] text-blue-500 hover:underline font-semibold"
                >
                  Kelola Aturan Alokasi
                </button>
              )}
            </div>

            {items.length === 0 ? (
              <div className="p-6 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/40 text-center space-y-2">
                <p className="text-xs text-[var(--muted)]">
                  Belum ada aturan alokasi gaji yang dikonfigurasi.
                </p>
                {onOpenRulesManager && (
                  <button
                    type="button"
                    onClick={() => {
                      onClose();
                      onOpenRulesManager();
                    }}
                    className="px-3.5 py-1.5 rounded-xl bg-blue-600 text-white font-bold text-xs"
                  >
                    + Buat Paket Alokasi Gaji
                  </button>
                )}
              </div>
            ) : (
              <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
                {items.map((item) => (
                  <div
                    key={item.id}
                    className={cn(
                      "p-3 rounded-xl border transition-all flex items-center justify-between gap-3",
                      item.enabled
                        ? "bg-[var(--surface-raised)] border-black/[0.08] dark:border-white/[0.08]"
                        : "bg-[var(--surface)]/40 border-[var(--border)] opacity-50"
                    )}
                  >
                    <div className="flex items-center gap-2.5 min-w-0">
                      <input
                        type="checkbox"
                        checked={item.enabled}
                        onChange={() => handleToggleItem(item.id)}
                        className="rounded border-[var(--border)] text-income focus:ring-income h-4 w-4 shrink-0"
                      />
                      <div className="min-w-0">
                        <div className="font-bold text-xs text-[var(--text)] truncate">{item.name}</div>
                        <div className="text-[11px] text-[var(--muted)] truncate font-medium">
                          Tujuan: {item.target_account_name}
                        </div>
                      </div>
                    </div>

                    <div className="w-36 shrink-0">
                      <input
                        type="text"
                        inputMode="numeric"
                        disabled={!item.enabled}
                        value={item.amountStr}
                        onChange={(e) => handleAmountChange(item.id, e.target.value)}
                        placeholder="0"
                        className="w-full text-right rounded-lg border border-[var(--border)] bg-[var(--surface)] px-2 py-1 text-xs font-bold tabular text-[var(--text)] disabled:opacity-50"
                      />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Allocation Summary Bar */}
          <div className="p-3.5 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] flex items-center justify-between">
            <span className="font-medium text-xs text-[var(--muted)]">Total Alokasi Bulan Ini</span>
            <span className="text-base font-bold tabular text-[var(--text)]">
              {fmtMoney(totalAllocated)}
            </span>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center justify-end gap-2 pt-2 border-t border-[var(--border)]">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2.5 rounded-xl border border-[var(--border)] text-[var(--muted)] hover:bg-[var(--surface-raised)]"
            >
              Tutup
            </button>
            <button
              type="button"
              disabled={batchMutation.isPending || items.filter((i) => i.enabled).length === 0}
              onClick={() => batchMutation.mutate()}
              className="px-5 py-2.5 rounded-xl bg-income hover:bg-income-hover text-white font-bold shadow-xs disabled:opacity-50 flex items-center gap-1.5 transition-all active:scale-95"
            >
              <Icon name="check" className="h-4 w-4" />
              <span>
                {batchMutation.isPending
                  ? "Menjalankan..."
                  : `Jalankan Alokasi Sekarang (${items.filter((i) => i.enabled).length})`}
              </span>
            </button>
          </div>
        </div>
      )}
    </Modal>
  );
}
