"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { cn, fmtMoney, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { Modal } from "@/components/ui/Modal";
import { FormSection } from "@/components/ui/FormField";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import { RecurringRule } from "@/types/recurring";
import { queryKeys } from "@/lib/queryKeys";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { parseRecurringRuleForm } from "@/lib/recurringForm";

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
  const [isPayrollAllocation, setIsPayrollAllocation] = useState(false);
  const [autoPost, setAutoPost] = useState(false);
  const [isActive, setIsActive] = useState(true);
  const [editingRuleId, setEditingRuleId] = useState<string | null>(null);
  const [notes, setNotes] = useState("");
  const [formError, setFormError] = useState("");

  const handleTypeChange = (nextType: "expense" | "transfer" | "income") => {
    setType(nextType);
    setCategoryId("");
    setObligationId("");
    if (nextType !== "transfer") {
      setTargetAccountId("");
      setIsPayrollAllocation(false);
    }
  };

  const handleScheduleTypeChange = (nextSchedule: "monthly_day" | "payday" | "weekly") => {
    setScheduleType(nextSchedule);
    if (nextSchedule === "weekly" && (scheduleDay < 1 || scheduleDay > 7)) {
      setScheduleDay(1);
    }
  };

  const startNewRule = () => {
    setEditingRuleId(null);
    setName("");
    setType("transfer");
    setAmountStr("");
    setSourceAccountId("");
    setTargetAccountId("");
    setCategoryId("");
    setObligationId("");
    setScheduleType("payday");
    setScheduleDay(25);
    setIsPayrollAllocation(false);
    setAutoPost(false);
    setIsActive(true);
    setNotes("");
    setFormError("");
    setActiveTab("create");
  };

  // Data queries
  const { data: rulesData } = useQuery<{ ok: boolean; rules: RecurringRule[] }>({
    queryKey: queryKeys.recurring.all,
    queryFn: () => api.get("/recurring"),
    enabled: open,
  });

  const { data: accountsData } = useQuery<any>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
    enabled: open,
  });

  const { data: categoriesData } = useQuery<any>({
    queryKey: queryKeys.categories,
    queryFn: () => api.get("/categories"),
    enabled: open,
  });

  const { data: obligationsData } = useQuery<any>({
    queryKey: queryKeys.obligations,
    queryFn: () => api.get("/obligations"),
    enabled: open,
  });

  const rules = rulesData?.rules ?? [];
  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData?.accounts]);
  const liquidAccounts = useMemo(() => listLiquidAccountChoices(accounts), [accounts]);
  const categories = categoriesData?.categories ?? [];
  const obligations = (obligationsData?.obligations ?? []).filter((o: any) => !o.is_archived);

  useEffect(() => {
    if (!open || liquidAccounts.length === 0) return;
    const sourceChoices = liquidAccounts;
    const getAccountId = (account: any) => String(account.id ?? account.account_id ?? "");
    const firstSourceId = getAccountId(sourceChoices[0]);
    const effectiveSourceId = sourceChoices.some((account: any) => getAccountId(account) === sourceAccountId)
      ? sourceAccountId
      : firstSourceId;
    setSourceAccountId((current) =>
      sourceChoices.some((account: any) => getAccountId(account) === current)
        ? current
        : firstSourceId,
    );
    setTargetAccountId((current) => {
      if (liquidAccounts.some((account) => getAccountId(account) === current && current !== effectiveSourceId)) {
        return current;
      }
      const target = liquidAccounts.find((account) => getAccountId(account) !== effectiveSourceId);
      return target ? getAccountId(target) : "";
    });
  }, [liquidAccounts, open, sourceAccountId]);

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
      const validation = parseRecurringRuleForm({
        name,
        type,
        amount: parsedAmount,
        sourceAccountId,
        targetAccountId: type === "transfer" ? targetAccountId || null : null,
        categoryId: type === "transfer" ? null : categoryId || null,
        obligationId: obligationId || null,
        scheduleType,
        scheduleDay: scheduleType === "payday" ? null : Number(scheduleDay),
        isPayrollAllocation,
        autoPost,
        isActive,
        notes: notes.trim(),
      });
      if (!validation.success) {
        throw new Error(validation.error.issues[0]?.message ?? "Periksa kembali data transaksi terjadwal");
      }
      const rule = validation.data;
      const payload = {
        name: rule.name,
        type: rule.type,
        amount: rule.amount,
        source_account_id: rule.sourceAccountId,
        target_account_id: rule.targetAccountId,
        category_id: rule.categoryId,
        obligation_id: rule.obligationId,
        schedule_type: rule.scheduleType,
        schedule_day: rule.scheduleDay,
        is_payroll_allocation: rule.isPayrollAllocation,
        auto_post: rule.autoPost,
        is_active: rule.isActive,
        notes: rule.notes || null,
      };
      return editingRuleId
        ? api.patch(`/recurring/${editingRuleId}`, payload)
        : api.post("/recurring", payload);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      setActiveTab("list");
      setName("");
      setAmountStr("");
      setNotes("");
      setFormError("");
      setEditingRuleId(null);
      setIsPayrollAllocation(false);
      setAutoPost(false);
      setIsActive(true);
    },
    onError: (err: any) => {
      setFormError(err?.message || "Gagal membuat transaksi otomatis");
    },
  });

  // Toggle active mutation
  const toggleMutation = useMutation({
    mutationFn: async (id: string) => api.post(`/recurring/${id}/toggle`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
    },
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: async (id: string) => api.del(`/recurring/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
    },
  });

  return (
    <Modal open={open} onClose={onClose} title="Transaksi Terjadwal & Otomatis">
      <div className="space-y-4 pt-1 text-xs">
        {/* Navigation Tabs */}
        <div className="space-y-3 border-b border-[var(--border)] pb-3">
          <div className="grid grid-cols-2 gap-2">
            <button
              type="button"
              onClick={() => setActiveTab("list")}
              className={cn(
                "min-h-11 rounded-xl px-3 text-xs font-semibold transition-colors",
                activeTab === "list"
                  ? "bg-[var(--surface-raised)] text-[var(--text)] border border-[var(--border)]"
                  : "text-[var(--muted)] hover:text-[var(--text)]"
              )}
            >
              Daftar Aturan ({rules.length})
            </button>
            <button
              type="button"
              onClick={startNewRule}
              className={cn(
                "min-h-11 rounded-xl px-3 text-xs font-semibold transition-colors",
                activeTab === "create"
                  ? "bg-[var(--surface-raised)] text-[var(--text)] border border-[var(--border)]"
                  : "text-[var(--muted)] hover:text-[var(--text)]"
              )}
            >
              + Tambah Aturan
            </button>
          </div>

          {activeTab === "list" && (
            <div role="group" aria-label="Filter aturan terjadwal" className="grid grid-cols-4 gap-1 text-xs font-medium">
              {(["all", "payroll", "debt", "routine"] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  aria-pressed={filterType === t}
                  onClick={() => setFilterType(t)}
                  className={cn(
                    "min-h-11 rounded-lg px-1.5 transition-colors",
                    filterType === t
                      ? "bg-[var(--text)] font-semibold text-[var(--surface)]"
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
          <div className="space-y-2.5">
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
                      "flex flex-col gap-3 rounded-2xl border p-3.5 transition-[color,border-color,background-color,opacity] motion-reduce:transition-none",
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

                      <div className="flex flex-wrap gap-x-2 gap-y-1 text-[11px] font-medium text-[var(--muted)]">
                        {isTransfer ? (
                          <span>
                            {rule.source_account_name} → {rule.target_account_name}
                          </span>
                        ) : (
                          <span>
                            {rule.source_account_name} • {rule.category_name || "Umum"}
                          </span>
                        )}
                        <span>
                          {rule.schedule_type === "payday"
                            ? "Setiap Tanggal Gajian"
                            : rule.schedule_type === "monthly_day"
                            ? `Tiap tgl ${rule.schedule_day}`
                            : "Mingguan"}
                        </span>
                      </div>
                    </div>

                    <div className="flex items-center justify-between gap-2 border-t border-[var(--border)] pt-2">
                      <div className="min-w-0">
                        <div className="whitespace-nowrap font-bold tabular text-sm text-[var(--text)]">
                          {fmtMoney(rule.amount)}
                        </div>
                        <div className="text-[10px] text-[var(--muted)] font-medium tabular">
                          Jatuh tempo: {rule.next_due_date}
                        </div>
                      </div>

                      <div className="flex items-center gap-1">
                        <button
                          type="button"
                          onClick={() => {
                            setEditingRuleId(rule.id);
                            setName(rule.name);
                            setType(rule.type);
                            setAmountStr(formatNumberWithDots(rule.amount));
                            setSourceAccountId(rule.source_account_id);
                            setTargetAccountId(rule.target_account_id ?? "");
                            setCategoryId(rule.category_id ?? "");
                            setObligationId(rule.obligation_id ?? "");
                            setScheduleType(rule.schedule_type);
                            setScheduleDay(rule.schedule_day ?? (rule.schedule_type === "weekly" ? 1 : 25));
                            setIsPayrollAllocation(rule.is_payroll_allocation);
                            setAutoPost(rule.auto_post);
                            setIsActive(rule.is_active);
                            setNotes(rule.notes ?? "");
                            setFormError("");
                            setActiveTab("create");
                          }}
                          className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg text-[var(--muted)] hover:bg-[var(--surface)] hover:text-[var(--text)]"
                          aria-label={`Ubah aturan ${rule.name}`}
                        >
                          <Icon name="edit" className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          onClick={() => toggleMutation.mutate(rule.id)}
                          className={cn(
                            "inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg transition-colors",
                            rule.is_active
                              ? "text-emerald-600 dark:text-emerald-400 hover:bg-emerald-500/10"
                              : "text-[var(--muted)] hover:bg-[var(--surface-raised)]"
                          )}
                          aria-label={rule.is_active ? `Nonaktifkan ${rule.name}` : `Aktifkan ${rule.name}`}
                        >
                          <Icon name={rule.is_active ? "check" : "close"} className="h-4 w-4" />
                        </button>
                        <ConfirmActionButton
                          label={`Hapus aturan ${rule.name}`}
                          confirmation={`Hapus aturan terjadwal "${rule.name}"?`}
                          onConfirm={() => deleteMutation.mutate(rule.id)}
                          className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-lg hover:bg-rose-500/10 text-[var(--muted)] hover:text-rose-500 transition-colors"
                        >
                          <Icon name="trash" className="h-4 w-4" />
                        </ConfirmActionButton>
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
          <form
            className="space-y-4 pt-1"
            onSubmit={(event) => {
              event.preventDefault();
              createMutation.mutate();
            }}
          >
            {formError && (
              <div role="alert" aria-live="assertive" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
                {formError}
              </div>
            )}

            <FormSection title="Transaksi yang dijadwalkan" description={type === "transfer" ? "Posisi investasi berubah melalui Beli/Jual, bukan transfer terjadwal." : undefined} className="border-t-0 pt-0">
            <div>
              <label htmlFor="recurring-name" className="text-xs font-medium text-[var(--muted)] block mb-1">
                Nama Aturan / Transaksi
              </label>
              <input
                id="recurring-name"
                name="name"
                type="text"
                maxLength={100}
                autoComplete="off"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Contoh: Alokasi Makan, Cicilan KPR, Bayar Wifi"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-semibold text-[var(--text)]"
              />
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div>
                <label htmlFor="recurring-type" className="text-xs font-medium text-[var(--muted)] block mb-1">Jenis Transaksi</label>
                <select
                  id="recurring-type"
                  name="type"
                  value={type}
                  onChange={(e) => handleTypeChange(e.target.value as "expense" | "transfer" | "income")}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="transfer">Pindah Saldo / Alokasi Kantong</option>
                  <option value="expense">Pengeluaran / Pembayaran Tagihan</option>
                  <option value="income">Pemasukan Rutin</option>
                </select>
              </div>

              <div>
                <label htmlFor="recurring-amount" className="text-xs font-medium text-[var(--muted)] block mb-1">Nominal (IDR)</label>
                <input
                  id="recurring-amount"
                  name="amount"
                  type="text"
                  inputMode="numeric"
                  value={amountStr}
                  onChange={(e) => setAmountStr(formatNumberWithDots(e.target.value))}
                  placeholder="Contoh: 1.500.000"
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-bold tabular text-[var(--text)]"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div>
                <label htmlFor="recurring-source" className="text-xs font-medium text-[var(--muted)] block mb-1">
                  {type === "income" ? "Masuk ke rekening" : "Dari rekening"}
                </label>
                <select
                  id="recurring-source"
                  name="source_account_id"
                  value={sourceAccountId}
                  onChange={(e) => setSourceAccountId(e.target.value)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="">Pilih rekening…</option>
                  <AccountSelectOptions accounts={accounts} allowParentSelection={true} liquidOnly />
                </select>
              </div>

              {type === "transfer" ? (
                <div>
                  <label htmlFor="recurring-target" className="text-xs font-medium text-[var(--muted)] block mb-1">Ke Rekening / Kantong Tujuan</label>
                  <select
                    id="recurring-target"
                    name="target_account_id"
                    value={targetAccountId}
                    onChange={(e) => setTargetAccountId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Pilih rekening tujuan…</option>
                    <AccountSelectOptions accounts={accounts} allowParentSelection={true} excludeAccountId={sourceAccountId} liquidOnly />
                  </select>
                </div>
              ) : (
                <div>
                  <label htmlFor="recurring-category" className="text-xs font-medium text-[var(--muted)] block mb-1">{type === "expense" ? "Kategori Pengeluaran" : "Kategori Pemasukan"}</label>
                  <select
                    id="recurring-category"
                    name="category_id"
                    value={categoryId}
                    onChange={(e) => setCategoryId(e.target.value)}
                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                  >
                    <option value="">Pilih Kategori…</option>
                    {categories.filter((c: any) => c.kind === type && !c.is_archived).map((c: any) => (
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
                <label htmlFor="recurring-obligation" className="text-xs font-medium text-[var(--text)] block mb-1">
                  Bayar tagihan / utang (opsional)
                </label>
                <select
                  id="recurring-obligation"
                  name="obligation_id"
                  value={obligationId}
                  onChange={(e) => setObligationId(e.target.value)}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="">Tidak terkait tagihan</option>
                  {obligations.map((o: any) => (
                    <option key={o.id} value={o.id}>
                      {o.name} (Sisa: {fmtMoney(o.remaining_amount)})
                    </option>
                  ))}
                </select>
                <p className="mt-1 text-xs text-[var(--muted)]">Saat pembayaran dicatat, sisa tagihan akan berkurang.</p>
              </div>
            )}

            <div>
              <label htmlFor="recurring-notes" className="text-xs font-medium text-[var(--muted)] block mb-1">Catatan (Opsional)</label>
              <input
                id="recurring-notes"
                name="notes"
                type="text"
                value={notes}
                onChange={(event) => setNotes(event.target.value)}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
              />
            </div>
            </FormSection>

            <FormSection title="Jadwal dan pencatatan">
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div>
                <label htmlFor="recurring-schedule" className="text-xs font-medium text-[var(--muted)] block mb-1">Jadwal Pengulangan</label>
                <select
                  id="recurring-schedule"
                  name="schedule_type"
                  value={scheduleType}
                  onChange={(e) => handleScheduleTypeChange(e.target.value as "monthly_day" | "payday" | "weekly")}
                  className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                >
                  <option value="payday">Tepat Setiap Tanggal Gajian</option>
                  <option value="monthly_day">Tanggal Tertentu Bulanan</option>
                  <option value="weekly">Mingguan</option>
                </select>
              </div>

              {scheduleType !== "payday" && (
                <div>
                  {scheduleType === "weekly" ? (
                    <>
                      <label htmlFor="recurring-schedule-day" className="text-xs font-medium text-[var(--muted)] block mb-1">
                        Hari pengulangan
                      </label>
                      <select
                        id="recurring-schedule-day"
                        name="schedule_day"
                        value={scheduleDay}
                        onChange={(event) => setScheduleDay(Number(event.target.value))}
                        className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm text-[var(--text)]"
                      >
                        <option value={1}>Senin</option>
                        <option value={2}>Selasa</option>
                        <option value={3}>Rabu</option>
                        <option value={4}>Kamis</option>
                        <option value={5}>Jumat</option>
                        <option value={6}>Sabtu</option>
                        <option value={7}>Minggu</option>
                      </select>
                    </>
                  ) : (
                    <>
                      <label htmlFor="recurring-schedule-day" className="text-xs font-medium text-[var(--muted)] block mb-1">
                        Tanggal Setiap Bulan (1–31)
                      </label>
                      <input
                        id="recurring-schedule-day"
                        name="schedule_day"
                        type="number"
                        min={1}
                        max={31}
                        value={scheduleDay}
                        onChange={(event) => setScheduleDay(Number(event.target.value))}
                        className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm tabular text-[var(--text)] font-semibold"
                      />
                    </>
                  )}
                </div>
              )}
            </div>

            <div className="space-y-2 pt-1">
              {type === "transfer" && (
                <>
                  <label className="flex items-center gap-2.5 cursor-pointer select-none">
                    <input
                      name="is_payroll_allocation"
                      type="checkbox"
                      checked={isPayrollAllocation}
                      onChange={(e) => setIsPayrollAllocation(e.target.checked)}
                      className="rounded border-[var(--border)] text-blue-600 focus:ring-blue-500 h-4 w-4"
                    />
                    <span className="font-semibold text-xs text-[var(--text)]">
                      Masukkan ke alokasi gaji
                    </span>
                  </label>
                  <p className="pl-7 text-[10px] text-[var(--muted)]">
                    Tidak otomatis termasuk alokasi gaji; aktifkan hanya untuk pemindahan setelah gajian.
                  </p>
                </>
              )}

              {editingRuleId && (
                <label className="flex items-center gap-2.5 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    name="is_active"
                    checked={isActive}
                    onChange={(e) => setIsActive(e.target.checked)}
                    className="rounded border-[var(--border)] text-emerald-600 focus:ring-emerald-500 h-4 w-4"
                  />
                  <span className="font-semibold text-xs text-[var(--text)]">Aturan aktif</span>
                </label>
              )}

              <label className="flex items-center gap-2.5 cursor-pointer select-none">
                <input
                  type="checkbox"
                  name="auto_post"
                  checked={autoPost}
                  onChange={(e) => setAutoPost(e.target.checked)}
                  className="rounded border-[var(--border)] text-emerald-600 focus:ring-emerald-500 h-4 w-4"
                />
                <span className="font-semibold text-xs text-[var(--text)]">
                  Catat otomatis tanpa konfirmasi
                </span>
              </label>
              <p className="pl-7 text-xs text-[var(--muted)]">Jika tidak dipilih, transaksi menunggu konfirmasi Anda saat jatuh tempo.</p>
            </div>
            </FormSection>

            <div className="flex items-center justify-end gap-2 pt-3 border-t border-[var(--border)]">
              <button
                type="button"
                onClick={() => {
                  setEditingRuleId(null);
                  setActiveTab("list");
                }}
                className="px-4 py-2 rounded-xl border border-[var(--border)] text-[var(--muted)] hover:bg-[var(--surface-raised)]"
              >
                Batal
              </button>
              <button
                type="submit"
                disabled={createMutation.isPending}
                className="min-h-11 rounded-xl bg-[var(--text)] px-4 text-sm font-semibold text-[var(--surface)] hover:opacity-90 disabled:opacity-50"
              >
                {createMutation.isPending ? "Menyimpan…" : editingRuleId ? "Simpan Perubahan" : "Simpan Aturan"}
              </button>
            </div>
          </form>
        )}
      </div>
    </Modal>
  );
}
