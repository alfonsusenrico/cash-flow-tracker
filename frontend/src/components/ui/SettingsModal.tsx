"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";
import { Icon } from "@/components/ui/Icon";
import { formatNumberWithDots } from "@/lib/utils";

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
    queryKey: ["auth-me"],
    queryFn: () => api.get("/auth/me"),
    enabled: open,
  });

  // Fetch currency rates
  const { data: ratesData } = useQuery<{ ok: boolean; usdidr: number }>({
    queryKey: ["currency-rates"],
    queryFn: () => api.get("/auth/currency/rates"),
    enabled: open,
  });

  useEffect(() => {
    if (!open) return;
    const u = userData?.user || user;
    if (u) {
      setName(u.name || u.username || "");
      if (u.payday_day) setDay(String(u.payday_day));
      if (u.currency) setCurrency(u.currency.toUpperCase());
      if (u.emergency_fund_multiplier) setMultiplier(String(u.emergency_fund_multiplier));
      if (u.monthly_spending_budget) setMonthlyBudget(formatNumberWithDots(u.monthly_spending_budget));
      else setMonthlyBudget("");
    }
    setErr("");
    setMsg("");
  }, [open, userData, user]);

  const settingsMut = useMutation({
    mutationFn: () => {
      const cleanBudget = monthlyBudget.trim().replace(/[^0-9]/g, "");
      const budgetVal = cleanBudget ? parseInt(cleanBudget, 10) : null;
      return api.patch("/auth/settings", {
        name: name.trim() || undefined,
        payday_day: parseInt(day, 10),
        currency: currency.trim().toUpperCase(),
        emergency_fund_multiplier: parseInt(multiplier, 10) || 6,
        monthly_spending_budget: budgetVal,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["auth-me"] });
      qc.invalidateQueries({ queryKey: ["currency-rates"] });
      qc.invalidateQueries({ queryKey: ["pulse"] });
      qc.invalidateQueries({ queryKey: ["insights"] });
      qc.invalidateQueries({ queryKey: ["dashboard-overview"] });
      qc.invalidateQueries({ queryKey: ["dashboard-analytics"] });
      qc.invalidateQueries({ queryKey: ["goals"] });
      qc.invalidateQueries({ queryKey: ["accounts"] });
      qc.invalidateQueries({ queryKey: ["ledger"] });
      qc.invalidateQueries({ queryKey: ["recurring-rules"] });
      qc.invalidateQueries({ queryKey: ["pending-scheduled-transactions"] });
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
      <div className="space-y-5 pt-1 text-xs">
        {err && (
          <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-500 font-medium">
            {err}
          </div>
        )}
        {msg && (
          <div className="p-3 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-600 dark:text-emerald-400 font-medium">
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
            <label className="block text-[11px] font-medium text-[var(--muted)] mb-1">
              Nama Tampilan (Display Name)
            </label>
            <Input
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
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-[var(--text)] mb-1">
                Tanggal Gajian (1–31)
              </label>
              <Input
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
              <label className="block text-xs font-medium text-[var(--text)] mb-1">
                Mata Uang Utama
              </label>
              <select
                value={currency}
                onChange={(e) => setCurrency(e.target.value)}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-semibold text-[var(--text)] focus:outline-hidden"
              >
                <option value="IDR">IDR (Indonesian Rupiah)</option>
                <option value="USD">USD (United States Dollar)</option>
              </select>
              <p className="text-[10px] text-[var(--muted)] mt-1 tabular font-medium">
                {ratesData?.usdidr
                  ? `Kurs: 1 USD ≈ Rp ${ratesData.usdidr.toLocaleString("id-ID")}`
                  : "Kurs: 1 USD ≈ Rp 16.500"}
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
                <label className="text-xs font-medium text-[var(--text)]">
                  Target Ketahanan Dana (Bulan)
                </label>
                <span className="text-[11px] font-bold text-income tabular">
                  {multiplier}x Pengeluaran Pokok
                </span>
              </div>
              <Input
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
              <label className="block text-xs font-medium text-[var(--text)] mb-1">
                Batas Belanja Operasional Bulanan (IDR)
              </label>
              <Input
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

        {/* Action Footer */}
        <div className="flex items-center justify-end gap-2 pt-3 border-t border-[var(--border)]">
          <Button
            variant="secondary"
            size="sm"
            onClick={onClose}
          >
            Batal
          </Button>
          <Button
            variant="primary"
            size="sm"
            onClick={() => settingsMut.mutate()}
            disabled={settingsMut.isPending}
          >
            {settingsMut.isPending ? "Menyimpan..." : "Simpan Pengaturan"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
