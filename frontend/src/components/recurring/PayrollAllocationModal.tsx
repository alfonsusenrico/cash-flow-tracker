"use client";

import { useState, useEffect, useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { RecurringRule, PayrollItemPayload } from "@/types/recurring";
import { listLiquidAccountChoices } from "@/lib/accountOptions";

interface Props {
  open: boolean;
  onClose: () => void;
  onOpenRulesManager?: () => void;
}

interface EditableItem {
  id: string; // rule id or temp id
  enabled: boolean;
  eligible: boolean;
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
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  // Fetch payroll recurring rules
  const { data: rulesData } = useQuery<{ ok: boolean; rules: RecurringRule[] }>({
    queryKey: queryKeys.recurring.payroll,
    queryFn: () => api.get("/recurring?is_payroll=true"),
    enabled: open,
  });

  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);
  const rules = useMemo(() => rulesData?.rules ?? [], [rulesData?.rules]);

  // Populate items when rules load
  useEffect(() => {
    if (rules.length > 0) {
      // Find common source account
      const defaultSourceAccountId = rules.find((rule) =>
        liquidAccounts.some((account: any) => (account.id ?? account.account_id) === rule.source_account_id)
      )?.source_account_id ?? String(liquidAccounts[0]?.id ?? liquidAccounts[0]?.account_id ?? "");
      setSourceAccountId(defaultSourceAccountId);
      setItems(
        rules.map((r) => ({
          id: r.id,
          eligible: liquidAccounts.some((account: any) => (account.id ?? account.account_id) === r.source_account_id) &&
            liquidAccounts.some((account: any) => (account.id ?? account.account_id) === r.target_account_id),
          enabled: r.is_active &&
            liquidAccounts.some((account: any) => (account.id ?? account.account_id) === r.source_account_id) &&
            liquidAccounts.some((account: any) => (account.id ?? account.account_id) === r.target_account_id),
          rule_id: r.id,
          source_account_id: r.source_account_id,
          target_account_id: r.target_account_id || "",
          target_account_name: r.target_account_name || "Rekening Tujuan",
          name: r.name,
          amountStr: formatNumberWithDots(r.amount),
          notes: r.notes || "Alokasi Gaji Bulanan",
        }))
      );
    } else if (rules.length === 0) {
      setItems([]);
    }
  }, [rules, liquidAccounts]);

  useEffect(() => {
    if (sourceAccountId) return;

    const defaultRuleSource = rules.find((rule) =>
      liquidAccounts.some((account: any) => (account.id ?? account.account_id) === rule.source_account_id)
    )?.source_account_id;
    const defaultSource = defaultRuleSource ?? liquidAccounts[0]?.id ?? liquidAccounts[0]?.account_id;
    if (defaultSource) setSourceAccountId(String(defaultSource));
  }, [sourceAccountId, rules, liquidAccounts]);

  // Source account details
  const sourceAccount = liquidAccounts.find((account) => (account.id ?? account.account_id) === sourceAccountId);

  // Calculate totals
  const totalAllocated = items
    .filter((i) => i.enabled)
    .reduce((sum, i) => sum + parseNumberFromDots(i.amountStr), 0);
  const availableBalance = Math.max(0, Number(sourceAccount?.balance ?? 0));
  const remainingBalance = Math.max(0, availableBalance - totalAllocated);
  const shortfall = Math.max(0, totalAllocated - availableBalance);
  const hasSourceDestinationConflict = items.some(
    (item) => item.enabled && item.target_account_id === sourceAccountId,
  );

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
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
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
      prev.map((item) => (item.id === id && item.eligible ? { ...item, enabled: !item.enabled } : item))
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
            className="min-h-11 px-6 py-2.5 rounded-xl bg-income hover:bg-income-hover text-white text-xs font-bold shadow-xs transition-[background-color,transform] active:scale-95 motion-reduce:transition-none"
          >
            Selesai
          </button>
        </div>
      ) : (
        <form
          className="space-y-4 pt-1 text-xs"
          onSubmit={(event) => {
            event.preventDefault();
            if (batchMutation.isPending || hasSourceDestinationConflict) return;
            batchMutation.mutate();
          }}
        >
          {error && (
            <div role="alert" aria-live="assertive" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
              {error}
            </div>
          )}
          {hasSourceDestinationConflict && (
            <p role="alert" className="rounded-xl border border-amber-500/25 bg-amber-500/10 p-3 text-amber-800 dark:text-amber-200">
              Rekening sumber gaji tidak boleh sama dengan tujuan alokasi yang dipilih.
            </p>
          )}

          {/* Salary Source Account Banner */}
          <div className="p-3.5 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] space-y-2">
            <div className="flex items-center justify-between">
              <label htmlFor="payroll-source-account" className="text-[11px] font-semibold text-[var(--muted)] uppercase">
                Rekening Sumber Gaji
              </label>
              {sourceAccount && typeof sourceAccount.balance === "number" && (
                <span className="font-bold tabular text-xs text-emerald-600 dark:text-emerald-400">
                  Saldo: {fmtMoney(sourceAccount.balance)}
                </span>
              )}
            </div>
            <select
              id="payroll-source-account"
              name="source_account_id"
              value={sourceAccountId}
              onChange={(e) => setSourceAccountId(e.target.value)}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm font-semibold text-[var(--text)]"
            >
              <AccountSelectOptions accounts={accounts} allowParentSelection={true} liquidOnly />
            </select>
          </div>
          <p role="note" className="text-xs text-[var(--muted)]">
            Posisi investasi hanya berubah melalui Beli/Jual. Alokasi gaji memakai rekening likuid.
          </p>

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
              <div className="space-y-2">
                {items.map((item) => (
                  <div
                    key={item.id}
                    className={cn(
                      "flex flex-col gap-3 rounded-xl border p-3 transition-[color,border-color,background-color] motion-reduce:transition-none sm:flex-row sm:items-center sm:justify-between",
                      item.enabled
                        ? "bg-[var(--surface-raised)] border-black/[0.08] dark:border-white/[0.08]"
                        : "bg-[var(--surface)] border-[var(--border)]"
                    )}
                  >
                    <label className="flex items-center gap-2.5 min-w-0 cursor-pointer">
                      <input
                        type="checkbox"
                        aria-label={`Sertakan ${item.name} dalam alokasi`}
                        checked={item.enabled}
                        disabled={!item.eligible}
                        onChange={() => handleToggleItem(item.id)}
                        className="rounded border-[var(--border)] text-income focus:ring-income h-4 w-4 shrink-0"
                      />
                      <span className="min-w-0">
                        <div className="font-bold text-xs text-[var(--text)] truncate">{item.name}</div>
                        <div className="text-[11px] text-[var(--muted)] truncate font-medium">
                          Tujuan: {item.target_account_name}
                        </div>
                        {!item.eligible && (
                          <div className="text-[11px] text-amber-700 dark:text-amber-300">
                            Posisi investasi hanya berubah melalui Beli/Jual.
                          </div>
                        )}
                      </span>
                    </label>

                    <div className="w-full shrink-0 sm:w-36">
                      <input
                        aria-label={`Nominal alokasi ${item.name}`}
                        name={`allocation_amount_${item.id}`}
                        type="text"
                        inputMode="numeric"
                        disabled={!item.enabled}
                        value={item.amountStr}
                        onChange={(e) => handleAmountChange(item.id, e.target.value)}
                        placeholder="0"
                        className="min-h-11 w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 text-right text-sm font-semibold tabular text-[var(--text)] disabled:opacity-50"
                      />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Allocation Summary Bar */}
          <div className="p-3.5 rounded-2xl bg-[var(--surface-raised)] border border-[var(--border)] space-y-1.5">
            <div className="flex items-center justify-between">
              <span className="font-medium text-xs text-[var(--muted)]">Total Alokasi Bulan Ini</span>
              <span className="text-base font-bold tabular text-[var(--text)]">{fmtMoney(totalAllocated)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="font-medium text-xs text-[var(--muted)]">Sisa saldo setelah alokasi</span>
              <span className="font-bold tabular text-[var(--text)]">{fmtMoney(remainingBalance)}</span>
            </div>
            {shortfall > 0 && (
              <p role="alert" className="text-[11px] font-semibold text-rose-600">
                Alokasi melebihi saldo sumber sebesar {fmtMoney(shortfall)}. Kurangi nominal atau pilih item yang lebih sedikit.
              </p>
            )}
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
              type="submit"
              disabled={batchMutation.isPending || items.filter((i) => i.enabled).length === 0 || shortfall > 0 || hasSourceDestinationConflict}
              className="min-h-11 px-5 py-2.5 rounded-xl bg-[var(--text)] text-[var(--surface)] font-semibold disabled:opacity-50 flex items-center gap-1.5 hover:opacity-90 transition-opacity motion-reduce:transition-none"
            >
              <Icon name="check" className="h-4 w-4" />
              <span>
                {batchMutation.isPending
                  ? "Menjalankan…"
                  : `Jalankan Alokasi Sekarang (${items.filter((i) => i.enabled).length})`}
              </span>
            </button>
          </div>
        </form>
      )}
    </Modal>
  );
}
