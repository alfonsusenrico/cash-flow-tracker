"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { RecurringRule } from "@/types/recurring";

interface Props {
  open: boolean;
  onClose: () => void;
}

export function RecurringRulesModal({ open, onClose }: Props) {
  const qc = useQueryClient();

  const [activeTab, setActiveTab] = useState<"list" | "create">("list");
  const [filterType, setFilterType] = useState<"all" | "payroll" | "debt" | "routine">("all");

  // Form states
  const [name, setName] = useState("");
  const [type, setType] = useState<"expense" | "transfer" | "income">("transfer");
  const [amountStr, setAmountStr] = useState("");
  const [sourceAccountId, setSourceAccountId] = useState("");
  const [targetAccountId, setTargetAccountId] = useState("");
  const [categoryId, setCategoryId] = useState("");
  const [obligationId, setObligationId] = useState("");
  const [scheduleType, setScheduleType] = useState<"monthly_day" | "payday" | "weekly">("payday");
  const [scheduleDay, setScheduleDay] = useState<number>(25);
  const [isPayrollAllocation, setIsPayrollAllocation] = useState(true);
  const [autoPost, setAutoPost] = useState(false);
  const [notes, setNotes] = useState("");
  const [formError, setFormError] = useState("");

  // Data queries
  const { data: rulesData } = useQuery<{ ok: boolean; rules: RecurringRule[] }>({
    queryKey: ["recurring-rules"],
    queryFn: () => api.get("/recurring"),
    enabled: open,
  });

  const { data: accountsData } = useQuery<any>({
    queryKey: ["accounts"],
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  const { data: categoriesData } = useQuery<any>({
    queryKey: ["categories"],
    queryFn: () => api.get("/categories"),
    enabled: open,
  });

  const { data: obligationsData } = useQuery<any>({
    queryKey: ["obligations"],
    queryFn: () => api.get("/obligations"),
    enabled: open,
  });

  const rules = rulesData?.rules ?? [];
  const accounts = accountsData?.accounts ?? [];
  const categories = categoriesData?.categories ?? [];
  const obligations = (obligationsData?.obligations ?? []).filter((o: any) => !o.is_archived);

  // Set default source account
  if (!sourceAccountId && accounts.length > 0) {
    setSourceAccountId(accounts[0].id);
  }
  if (!targetAccountId && accounts.length > 1) {
    setTargetAccountId(accounts[1].id);
  }

  // Filter rules
  const filteredRules = rules.filter((r) => {
    if (filterType === "payroll") return r.is_payroll_allocation;
    if (filterType === "debt") return Boolean(r.obligation_id);
    if (filterType === "routine") return !r.is_payroll_allocation && !r.obligation_id;
    return true;
  });

  // Create mutation
  const createMutation = useMutation({
    mutationFn: async () => {
      setFormError("");
      const parsedAmount = parseNumberFromDots(amountStr);
      if (!name.trim()) throw new Error("Nama transaksi terjadwal wajib diisi");
      if (parsedAmount <= 0) throw new Error("Nominal harus lebih dari 0");
      if (!sourceAccountId) throw new Error("Rekening sumber wajib dipilih");
      if (type === "transfer" && !targetAccountId) throw new Error("Rekening tujuan transfer wajib dipilih");
      if (type === "transfer" && targetAccountId === sourceAccountId) throw new Error("Rekening sumber dan tujuan tidak boleh sama");

      return api.post("/recurring", {
        name: name.trim(),
        type,
        amount: parsedAmount,
        source_account_id: sourceAccountId,
        target_account_id: type === "transfer" ? targetAccountId : null,
        category_id: type !== "transfer" && categoryId ? categoryId : null,
        obligation_id: obligationId || null,
        schedule_type: scheduleType,
        schedule_day: scheduleType === "monthly_day" ? Number(scheduleDay) : null,
        is_payroll_allocation: isPayrollAllocation,
        auto_post: autoPost,
        notes: notes.trim() || null,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["recurring-rules"] });
      qc.invalidateQueries({ queryKey: ["recurring-pending"] });
      setActiveTab("list");
      setName("");
      setAmountStr("");
      setNotes("");
      setFormError("");
    },
    onError: (err: any) => {
      setFormError(err?.message || "Gagal membuat transaksi otomatis");
    },
  });

  // Toggle active mutation
  const toggleMutation = useMutation({
    mutationFn: async (id: string) => api.post(`/recurring/${id}/toggle`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["recurring-rules"] });
      qc.invalidateQueries({ queryKey: ["recurring-pending"] });
    },
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: async (id: string) => api.del(`/recurring/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["recurring-rules"] });
      qc.invalidateQueries({ queryKey: ["recurring-pending"] });
    },
  });

  return (
    <Modal open={open} onClose={onClose} title="Transaksi Terjadwal & Otomatis">
      <div className="space-y-4 pt-1 text-xs">
        {/* Navigation Tabs */}
        <div className="flex items-center justify-between border-b border-[var(--border)] pb-2">
          <div className="flex items-center gap-1">
            <button
              type="button"
              onClick={() => setActiveTab("list")}
              className={cn(
                "px-3 py-1.5 rounded-xl font-semibold transition-colors",
                activeTab === "list"
                  ? "bg-[var(--surface-raised)] text-[var(--text)] border border-[var(--border)]"
                  : "text-[var(--muted)] hover:text-[var(--text)]"
              )}
            >
              Daftar Aturan ({rules.length})
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("create")}
              className={cn(
                "px-3 py-1.5 rounded-xl font-semibold transition-colors",
                activeTab === "create"
                  ? "bg-[var(--surface-raised)] text-[var(--text)] border border-[var(--border)]"
                  : "text-[var(--muted)] hover:text-[var(--text)]"
              )}
            >
              + Tambah Aturan
            </button>
          </div>

          {activeTab === "list" && (
            <div className="flex items-center gap-1 text-[11px] font-medium">
              {(["all", "payroll", "debt", "routine"] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setFilterType(t)}
                  className={cn(
                    "px-2 py-0.5 rounded-lg transition-colors",
                    filterType === t
                      ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 font-bold"
                      : "text-[var(--muted)] hover:text-[var(--text)]"
                  )}
                >
                  {t === "all" ? "Semua" : t === "payroll" ? "Gajian" : t === "debt" ? "Cicilan" : "Rutin"}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Tab 1: Rules List */}
        {activeTab === "list" && (
          <div className="space-y-2.5 max-h-[60vh] overflow-y-auto pr-1">
            {filteredRules.length === 0 ? (
              <div className="py-10 text-center text-[var(--muted)]">
                Belum ada transaksi terjadwal. Klik &quot;+ Tambah Aturan&quot; untuk memulai.
              </div>
            ) : (
              filteredRules.map((rule) => {
                const isTransfer = rule.type === "transfer";
                const isDebt = Boolean(rule.obligation_id);

                return (
                  <div
                    key={rule.id}
                    className={cn(
                      "p-3.5 rounded-2xl border transition-all flex flex-col sm:flex-row sm:items-center justify-between gap-3",
                      rule.is_active
                        ? "bg-[var(--surface-raised)] border-black/[0.08] dark:border-white/[0.08]"
                        : "bg-[var(--surface)]/50 border-[var(--border)] opacity-60"
                    )}
                  >
                    <div className="space-y-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-bold text-sm text-[var(--text)]">{rule.name}</span>
                        {rule.is_payroll_allocation && (
                          <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-blue-500/15 text-blue-600 dark:text-blue-400">
                            Alokasi Gaji
                          </span>
                        )}
                        {isDebt && (
                          <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-rose-500/15 text-rose-600 dark:text-rose-400">
                            Cicilan: {rule.obligation_name}
                          </span>
                        )}
                        <span
                          className={cn(
                            "px-1.5 py-0.5 rounded text-[9px] font-bold",
                            rule.auto_post
                              ? "bg-emerald-500/15 text-emerald-600 dark:text-emerald-400"
                              : "bg-amber-500/15 text-amber-600 dark:text-amber-400"
                          )}
                        >
                          {rule.auto_post ? "Auto-Post" : "Konfirmasi 1-Tap"}
                        </span>
                      </div>

                      <div className="text-[11px] text-[var(--muted)] flex items-center gap-1.5 font-medium">
                        {isTransfer ? (
                          <span>
                            {rule.source_account_name} → {rule.target_account_name}
                          </span>
                        ) : (
                          <span>
                            {rule.source_account_name} • {rule.category_name || "Umum"}
                          </span>
                        )}
                        <span>•</span>
                        <span>
                          {rule.schedule_type === "payday"
                            ? "Setiap Tanggal Gajian"
                            : rule.schedule_type === "monthly_day"
                            ? `Tiap tgl ${rule.schedule_day}`
                            : "Mingguan"}
                        </span>
                      </div>
                    </div>

                    <div className="flex items-center justify-between sm:justify-end gap-3 pt-2 sm:pt-0 border-t sm:border-t-0 border-[var(--border)]">
                      <div className="text-right">
                        <div className="font-bold tabular text-sm text-[var(--text)]">
                          {fmtMoney(rule.amount)}
                        </div>
                        <div className="text-[10px] text-[var(--muted)] font-medium tabular">
                          Jatuh tempo: {rule.next_due_date}
                        </div>
                      </div>

                      <div className="flex items-center gap-1">
                        <button
                          type="button"
                          onClick={() => toggleMutation.mutate(rule.id)}
                          className={cn(
                            "p-1.5 rounded-lg transition-colors",
                            rule.is_active
                              ? "text-emerald-600 dark:text-emerald-400 hover:bg-emerald-500/10"
                              : "text-[var(--muted)] hover:bg-[var(--surface-raised)]"
                          )}
                          title={rule.is_active ? "Nonaktifkan aturan" : "Aktifkan aturan"}
                        >
                          <Icon name={rule.is_active ? "check" : "close"} className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            if (confirm(`Hapus aturan terjadwal "${rule.name}"?`)) {
                              deleteMutation.mutate(rule.id);
                            }
                          }}
                          className="p-1.5 rounded-lg hover:bg-rose-500/10 text-[var(--muted)] hover:text-rose-500 transition-colors"
                          title="Hapus aturan"
                        >
                          <Icon name="trash" className="h-4 w-4" />
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        )}

        {/* Tab 2: Create Rule Form */}
        {activeTab === "create" && (
          <div className="space-y-3.5 max-h-[60vh] overflow-y-auto pr-1">
            {formError && (
              <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {formError}
              </div>
            )}

            <div>
              <label className="text-xs font-medium text-[var(--muted)] block mb-1">
                Nama Aturan / Transaksi
              </label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Contoh: Alokasi Makan, Cicilan KPR, Bayar Wifi"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-semibold text-[var(--text)]"
              />
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs font-medium text-[var(--muted)] block mb-1">Jenis Transaksi</label>
                <select
                  value={type}
                  onChange={(e) => setType(e.target.value as any)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="transfer">Pindah Saldo / Alokasi Kantong</option>
                  <option value="expense">Pengeluaran / Pembayaran Tagihan</option>
                  <option value="income">Pemasukan Rutin</option>
                </select>
              </div>

              <div>
                <label className="text-xs font-medium text-[var(--muted)] block mb-1">Nominal (IDR)</label>
                <input
                  type="text"
                  inputMode="numeric"
                  value={amountStr}
                  onChange={(e) => setAmountStr(formatNumberWithDots(e.target.value))}
                  placeholder="Contoh: 1.500.000"
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-bold tabular text-[var(--text)]"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs font-medium text-[var(--muted)] block mb-1">
                  {type === "transfer" ? "Dari Rekening Sumber" : "Dari Rekening / Dompet"}
                </label>
                <select
                  value={sourceAccountId}
                  onChange={(e) => setSourceAccountId(e.target.value)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <AccountSelectOptions accounts={accounts} allowParentSelection={true} />
                </select>
              </div>

              {type === "transfer" ? (
                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Ke Rekening / Kantong Tujuan</label>
                  <select
                    value={targetAccountId}
                    onChange={(e) => setTargetAccountId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <AccountSelectOptions accounts={accounts} allowParentSelection={true} excludeAccountId={sourceAccountId} />
                  </select>
                </div>
              ) : (
                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Kategori Pengeluaran</label>
                  <select
                    value={categoryId}
                    onChange={(e) => setCategoryId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Pilih Kategori...</option>
                    {categories.map((c: any) => (
                      <option key={c.id} value={c.id}>
                        {c.name}
                      </option>
                    ))}
                  </select>
                </div>
              )}
            </div>

            {/* Optional Debt Obligation Link */}
            {type === "expense" && obligations.length > 0 && (
              <div className="p-3 rounded-xl border border-rose-500/20 bg-rose-500/5 space-y-1">
                <label className="text-xs font-medium text-[var(--text)] block mb-1">
                  Hubungkan dengan Cicilan / Hutang (Otomatis potong sisa hutang)
                </label>
                <select
                  value={obligationId}
                  onChange={(e) => setObligationId(e.target.value)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="">Tidak terhubung ke hutang</option>
                  {obligations.map((o: any) => (
                    <option key={o.id} value={o.id}>
                      {o.name} (Sisa: {fmtMoney(o.remaining_amount)})
                    </option>
                  ))}
                </select>
              </div>
            )}

            {/* Schedule Presets */}
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs font-medium text-[var(--muted)] block mb-1">Jadwal Pengulangan</label>
                <select
                  value={scheduleType}
                  onChange={(e) => setScheduleType(e.target.value as any)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="payday">Tepat Setiap Tanggal Gajian</option>
                  <option value="monthly_day">Tanggal Tertentu Bulanan</option>
                  <option value="weekly">Mingguan</option>
                </select>
              </div>

              {scheduleType === "monthly_day" && (
                <div>
                  <label className="text-xs font-medium text-[var(--muted)] block mb-1">Tanggal Setiap Bulan (1–31)</label>
                  <input
                    type="number"
                    min={1}
                    max={31}
                    value={scheduleDay}
                    onChange={(e) => setScheduleDay(Number(e.target.value))}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm tabular text-[var(--text)] font-semibold"
                  />
                </div>
              )}
            </div>

            {/* Checkboxes: Payroll Bundle & Auto-Post */}
            <div className="space-y-2 pt-1">
              <label className="flex items-center gap-2.5 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={isPayrollAllocation}
                  onChange={(e) => setIsPayrollAllocation(e.target.checked)}
                  className="rounded border-[var(--border)] text-blue-600 focus:ring-blue-500 h-4 w-4"
                />
                <span className="font-semibold text-xs text-[var(--text)]">
                  Gabungkan dalam Paket Alokasi Gaji Bulanan (1-Tap Payroll)
                </span>
              </label>

              <label className="flex items-center gap-2.5 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={autoPost}
                  onChange={(e) => setAutoPost(e.target.checked)}
                  className="rounded border-[var(--border)] text-emerald-600 focus:ring-emerald-500 h-4 w-4"
                />
                <span className="font-semibold text-xs text-[var(--text)]">
                  Otomatis Catat ke Ledger (Auto-Post tanpa perlu konfirmasi manual)
                </span>
              </label>
            </div>

            <div className="flex items-center justify-end gap-2 pt-3 border-t border-[var(--border)]">
              <button
                type="button"
                onClick={() => setActiveTab("list")}
                className="px-4 py-2 rounded-xl border border-[var(--border)] text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              >
                Batal
              </button>
              <button
                type="button"
                disabled={createMutation.isPending}
                onClick={() => createMutation.mutate()}
                className="px-4 py-2 rounded-xl bg-income hover:bg-income-hover text-white font-semibold shadow-xs disabled:opacity-50"
              >
                {createMutation.isPending ? "Menyimpan..." : "Simpan Aturan"}
              </button>
            </div>
          </div>
        )}
      </div>
    </Modal>
  );
}
