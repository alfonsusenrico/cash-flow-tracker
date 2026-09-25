"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, getApiKeyInfo, rotateApiKey, type ApiKeyMetadata } from "@/lib/api";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";
import { Icon } from "@/components/ui/Icon";
import { formatNumberWithDots } from "@/lib/utils";
import { queryKeys } from "@/lib/queryKeys";

interface Props {
  open: boolean;
  onClose: () => void;
}

export function SettingsModal({ open, onClose }: Props) {
  const qc = useQueryClient();
  const { user } = useAppCtx();

  const [name, setName] = useState("");
  const [day, setDay] = useState("25");
  const [currency, setCurrency] = useState("IDR");
  const [multiplier, setMultiplier] = useState("6");
  const [monthlyBudget, setMonthlyBudget] = useState("");
  const [err, setErr] = useState("");
  const [msg, setMsg] = useState("");

  // Fetch current user settings
  const { data: userData } = useQuery<{ ok: boolean; user: any }>({
    queryKey: queryKeys.auth.me,
    queryFn: () => api.get("/auth/me"),
    enabled: open,
  });

  // Fetch currency rates
  const { data: ratesData } = useQuery<{ ok: boolean; usdidr: number }>({
    queryKey: queryKeys.auth.currencyRates,
    queryFn: () => api.get("/auth/currency/rates"),
    enabled: open,
  });

  const [showRotateConfirm, setShowRotateConfirm] = useState(false);
  const [newPlainKey, setNewPlainKey] = useState<string | null>(null);
  const [copiedKey, setCopiedKey] = useState(false);

  // Fetch API key metadata
  const { data: apiKeyData } = useQuery<{ ok: boolean; api_key: ApiKeyMetadata | null }>({
    queryKey: queryKeys.auth.apiKey,
    queryFn: getApiKeyInfo,
    enabled: open,
  });

  const persistedUser = userData?.user || user;
  const persistedName = persistedUser?.name;
  const persistedUsername = persistedUser?.username;
  const persistedPaydayDay = persistedUser?.payday_day;
  const persistedCurrency = persistedUser?.currency;
  const persistedMultiplier = persistedUser?.emergency_fund_multiplier;
  const persistedBudget = persistedUser?.monthly_spending_budget;

  const rotateKeyMut = useMutation({
    mutationFn: rotateApiKey,
    onSuccess: (data) => {
      setNewPlainKey(data.api_key);
      setShowRotateConfirm(false);
      qc.invalidateQueries({ queryKey: queryKeys.auth.apiKey });
    },
    onError: (e: Error) => setErr(e.message || "Gagal memperbarui kunci API"),
  });

  const handleCopyKey = () => {
    if (!newPlainKey) return;
    navigator.clipboard.writeText(newPlainKey);
    setCopiedKey(true);
    setTimeout(() => setCopiedKey(false), 2000);
  };

  useEffect(() => {
    if (!open) return;
    if (persistedUsername) {
      setName(persistedName || persistedUsername);
      if (persistedPaydayDay) setDay(String(persistedPaydayDay));
      if (persistedCurrency) setCurrency(persistedCurrency.toUpperCase());
      if (persistedMultiplier) setMultiplier(String(persistedMultiplier));
      if (persistedBudget != null) setMonthlyBudget(formatNumberWithDots(persistedBudget));
      else setMonthlyBudget("");
    }
    setShowRotateConfirm(false);
    setNewPlainKey(null);
    setCopiedKey(false);
    setErr("");
    setMsg("");
  }, [open, persistedName, persistedUsername, persistedPaydayDay, persistedCurrency, persistedMultiplier, persistedBudget]);

  const settingsMut = useMutation({
    mutationFn: () => {
      const cleanBudget = monthlyBudget.trim().replace(/[^0-9]/g, "");
      const budgetVal = cleanBudget ? parseInt(cleanBudget, 10) : null;
      const paydayDay = parseInt(day, 10);
      const emergencyMultiplier = parseInt(multiplier, 10);
      if (!name.trim()) throw new Error("Nama tampilan wajib diisi");
      if (paydayDay < 1 || paydayDay > 31) throw new Error("Tanggal gajian harus antara 1 dan 31");
      if (emergencyMultiplier < 1 || emergencyMultiplier > 36) throw new Error("Target dana darurat harus antara 1 dan 36 bulan");
      if (currency !== "IDR" && currency !== "USD") throw new Error("Mata uang yang didukung hanya IDR dan USD");
      return api.patch("/auth/settings", {
        name: name.trim(),
        payday_day: paydayDay,
        currency,
        emergency_fund_multiplier: emergencyMultiplier,
        monthly_spending_budget: budgetVal,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.auth.all });
      qc.invalidateQueries({ queryKey: queryKeys.pulse });
      qc.invalidateQueries({ queryKey: queryKeys.insights });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all });
      qc.invalidateQueries({ queryKey: queryKeys.recurring.all });
      setMsg("Pengaturan berhasil disimpan.");
      setTimeout(() => {
        setMsg("");
        onClose();
      }, 1200);
    },
    onError: (e: Error) => setErr(e.message),
  });

  const displayUser = userData?.user || user;

  return (
    <Modal open={open} onClose={onClose} title="Pengaturan Akun & Keuangan">
      <form
        className="space-y-5 pt-1 text-xs"
        onSubmit={(event) => {
          event.preventDefault();
          if (!settingsMut.isPending) settingsMut.mutate();
        }}
      >
        {err && (
          <div role="alert" aria-live="assertive" className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
            {err}
          </div>
        )}
        {msg && (
          <div role="status" aria-live="polite" className="p-3 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-600 dark:text-emerald-400 font-medium">
            {msg}
          </div>
        )}

        {/* User Profile Overview */}
        <section className="p-3.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/70 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2.5">
              <div className="h-9 w-9 rounded-xl bg-income/10 border border-income/20 flex items-center justify-center text-income font-bold text-sm">
                <Icon name="user" className="h-4 w-4" />
              </div>
              <div>
                <div className="font-bold text-sm text-[var(--text)]">
                  {displayUser?.name || displayUser?.username || "Pengguna"}
                </div>
                <div className="text-[11px] text-[var(--muted)] font-medium">
                  @{displayUser?.username}
                </div>
              </div>
            </div>
            <span className="text-[10px] font-semibold px-2 py-0.5 rounded bg-[var(--surface)] border border-[var(--border)] text-[var(--muted)]">
              Aktif
            </span>
          </div>

          <div>
            <label htmlFor="settings-display-name" className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Nama Tampilan (Display Name)
            </label>
            <Input
              id="settings-display-name"
              name="name"
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Contoh: Enrico"
              className="w-full text-xs"
            />
          </div>
        </section>

        {/* Siklus Keuangan & Mata Uang */}
        <section className="space-y-3">
          <h3 className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
            Siklus Keuangan & Mata Uang
          </h3>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div>
              <label htmlFor="settings-payday-day" className="block text-xs font-medium text-[var(--text)] mb-1">
                Tanggal Gajian (1–31)
              </label>
              <Input
                id="settings-payday-day"
                name="payday_day"
                type="number"
                min={1}
                max={31}
                value={day}
                onChange={(e) => setDay(e.target.value)}
                className="w-full text-xs tabular font-bold"
              />
              <p className="text-[10px] text-[var(--muted)] mt-1">
                Siklus dihitung ulang tiap tgl {day}.
              </p>
            </div>

            <div>
              <label htmlFor="settings-currency" className="block text-xs font-medium text-[var(--text)] mb-1">
                Mata Uang Utama
              </label>
              <select
                id="settings-currency"
                name="currency"
                value={currency}
                onChange={(e) => setCurrency(e.target.value)}
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-semibold text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
              >
                <option value="IDR">IDR (Indonesian Rupiah)</option>
                <option value="USD">USD (United States Dollar)</option>
              </select>
              <p className="text-[10px] text-[var(--muted)] mt-1 tabular font-medium">
                {ratesData?.usdidr
                  ? `Kurs: 1 USD ≈ Rp ${ratesData.usdidr.toLocaleString("id-ID")}`
                  : "Kurs belum tersedia"}
              </p>
            </div>
          </div>
        </section>

        {/* Target Ketahanan Dana & Batas Belanja Bulanan */}
        <section className="space-y-3 pt-3 border-t border-[var(--border)]">
          <h3 className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
            Perencanaan & Batas Anggaran
          </h3>

          <div className="space-y-3">
            <div>
              <div className="flex items-center justify-between mb-1">
                <label htmlFor="settings-emergency-multiplier" className="text-xs font-medium text-[var(--text)]">
                  Target Ketahanan Dana (Bulan)
                </label>
                <span className="text-[11px] font-bold text-income tabular">
                  {multiplier}x Pengeluaran Pokok
                </span>
              </div>
              <Input
                id="settings-emergency-multiplier"
                name="emergency_fund_multiplier"
                type="number"
                min={1}
                max={36}
                value={multiplier}
                onChange={(e) => setMultiplier(e.target.value)}
                className="w-full text-xs tabular font-bold"
              />
              <p className="text-[10px] text-[var(--muted)] mt-1">
                Berapa bulan pengeluaran pokok yang ingin dicadangkan sebagai dana darurat (standar umum 6x).
              </p>
            </div>

            <div>
              <label htmlFor="settings-monthly-budget" className="block text-xs font-medium text-[var(--text)] mb-1">
                Batas Belanja Operasional Bulanan (IDR)
              </label>
              <Input
                id="settings-monthly-budget"
                name="monthly_spending_budget"
                type="text"
                inputMode="numeric"
                value={monthlyBudget}
                onChange={(e) => setMonthlyBudget(formatNumberWithDots(e.target.value))}
                placeholder="Contoh: 10.000.000 (Kosongkan jika otomatis)"
                className="w-full text-xs font-bold tabular"
              />
              <p className="text-[10px] text-[var(--muted)] mt-1">
                Batas aman belanja operasional sebulan. Digunakan untuk menghitung sisa batas belanja harian. Jika kosong, diambil dari total anggaran kategori.
              </p>
            </div>
          </div>
        </section>

        <div className="flex items-center justify-end gap-2 border-t border-[var(--border)] pt-3">
          <Button type="button" variant="secondary" size="sm" onClick={onClose}>Batal</Button>
          <Button type="submit" variant="primary" size="sm" disabled={settingsMut.isPending}>
            {settingsMut.isPending ? "Menyimpan…" : "Simpan pengaturan"}
          </Button>
        </div>
      </form>

        <section className="space-y-3 pt-3 border-t border-[var(--border)]">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">
              Kunci API (Aplikasi Mobile & Bot)
            </h3>
            <span className="text-[10px] text-[var(--muted)] font-mono">
              Bearer Token
            </span>
          </div>

          {newPlainKey ? (
            <div className="p-3.5 rounded-2xl bg-amber-500/10 border border-amber-500/30 space-y-2.5">
              <div className="flex items-center gap-2 text-amber-600 dark:text-amber-400 font-bold text-xs">
                <span>⚠️ Salin Kunci API Baru Anda Sekarang</span>
              </div>
              <p className="text-[11px] text-[var(--muted)]">
                Kunci ini hanya ditampilkan satu kali ini saja. Simpan atau masukkan ke aplikasi mobile tracker Anda.
              </p>
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  aria-label="Kunci API baru"
                  readOnly
                  value={newPlainKey}
                  className="flex-1 font-mono text-xs p-2 rounded-xl bg-[var(--surface)] border border-[var(--border)] text-[var(--text)] select-all"
                />
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={handleCopyKey}
                  className="shrink-0"
                >
                  {copiedKey ? "✓ Tersalin!" : "Salin"}
                </Button>
              </div>
              <div className="flex justify-end pt-1">
                <button
                  type="button"
                  onClick={() => setNewPlainKey(null)}
                  className="text-[11px] text-[var(--muted)] hover:text-[var(--text)] underline cursor-pointer"
                >
                  Tutup & selesai
                </button>
              </div>
            </div>
          ) : (
            <div className="p-3.5 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/70 space-y-3">
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-[11px] text-[var(--muted)] font-medium">Awalan Kunci Aktif</div>
                  <div className="font-mono font-bold text-sm text-[var(--text)] mt-0.5">
                    {apiKeyData?.api_key?.key_prefix ? `${apiKeyData.api_key.key_prefix}••••••••••••` : "Belum ada kunci"}
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-[11px] text-[var(--muted)] font-medium">Terakhir Digunakan</div>
                  <div className="text-xs text-[var(--text)] font-medium mt-0.5">
                    {apiKeyData?.api_key?.last_used_at
                      ? new Date(apiKeyData.api_key.last_used_at).toLocaleDateString("id-ID", {
                          day: "numeric",
                          month: "short",
                          year: "numeric",
                          hour: "2-digit",
                          minute: "2-digit",
                        })
                      : "Belum pernah"}
                  </div>
                </div>
              </div>

              {showRotateConfirm ? (
                <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/30 space-y-2">
                  <p className="text-xs font-semibold text-rose-500">
                    Kunci lama akan langsung dicabut dan tidak dapat digunakan lagi. Lanjutkan?
                  </p>
                  <div className="flex items-center justify-end gap-2">
                    <Button
                      type="button"
                      variant="secondary"
                      size="sm"
                      onClick={() => setShowRotateConfirm(false)}
                    >
                      Batal
                    </Button>
                    <Button
                      type="button"
                      variant="primary"
                      size="sm"
                      onClick={() => rotateKeyMut.mutate()}
                      disabled={rotateKeyMut.isPending}
                      className="bg-rose-600 hover:bg-rose-700 text-white"
                    >
                      {rotateKeyMut.isPending ? "Membuat…" : "Ya, Buat Kunci Baru"}
                    </Button>
                  </div>
                </div>
              ) : (
                <div className="flex justify-end pt-1">
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    onClick={() => setShowRotateConfirm(true)}
                  >
                    Buat Kunci Baru (Reset)
                  </Button>
                </div>
              )}
            </div>
          )}
        </section>

    </Modal>
  );
}
