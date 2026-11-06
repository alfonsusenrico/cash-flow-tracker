"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Button } from "@/components/ui/Button";
import { Modal } from "@/components/ui/Modal";
import { api } from "@/lib/api";
import { type MonthlyReport, categoryLedgerHref } from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import { isNonReportableCategory } from "@/lib/systemCategories";
import { cn } from "@/lib/utils";
import { LINK_CLASS, ReportPanel } from "./ReportParts";

interface CategoryRow {
  id: string;
  name: string;
  kind: string;
  monthly_budget: number | null;
  is_archived: boolean;
  is_excluded_from_budget: boolean;
  system_key: string | null;
}

const digits = (value: string) => value.replace(/[^0-9]/g, "");
const grouped = (value: number | null) => (value && value > 0 ? value.toLocaleString("id-ID") : "");

export function BudgetEditorDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const { data, isLoading, isError } = useQuery<{ categories: CategoryRow[] }>({
    queryKey: [...queryKeys.categories, "expense"],
    queryFn: () => api.get("/categories?kind=expense"),
    enabled: open,
  });
  // Reports leave these categories out of spending, so a budget on them would never be compared.
  const categories = useMemo(
    () => (data?.categories ?? []).filter(
      (row) => row.kind === "expense" && !row.is_archived && !row.is_excluded_from_budget && !isNonReportableCategory(row.system_key),
    ),
    [data],
  );
  const [values, setValues] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  useEffect(() => {
    if (open) setValues(Object.fromEntries(categories.map((row) => [row.id, grouped(row.monthly_budget)])));
  }, [open, categories]);

  const save = useMutation({
    mutationFn: async () => {
      const changed = categories.filter((row) => (Number(digits(values[row.id] ?? "")) || null) !== (row.monthly_budget || null));
      for (const row of changed) {
        const amount = Number(digits(values[row.id] ?? "")) || null;
        await api.patch(`/categories/${row.id}`, { monthly_budget: amount });
      }
      return changed.length;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.categories });
      queryClient.invalidateQueries({ queryKey: queryKeys.reports.all });
      queryClient.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (failure: Error) => setError(failure.message || "Anggaran belum tersimpan. Coba lagi."),
  });

  return (
    <Modal open={open} onClose={onClose} title="Atur anggaran">
      <form
        className="space-y-4"
        onSubmit={(event) => {
          event.preventDefault();
          setError("");
          save.mutate();
        }}
      >
        <p className="text-sm text-[var(--text-2)]">
          Batas pengeluaran per kategori setiap bulan. Kosongkan kategori yang tidak perlu dibatasi.
        </p>
        {isLoading && <p className="text-sm text-[var(--muted)]">Memuat kategori…</p>}
        {isError && <p role="alert" className="text-sm text-[var(--out-fg)]">Kategori belum bisa dimuat.</p>}
        <ul className="space-y-2">
          {categories.map((row) => (
            <li key={row.id} className="flex items-center justify-between gap-3">
              <label htmlFor={`budget-${row.id}`} className="min-w-0 truncate text-sm text-[var(--text)]">{row.name}</label>
              <div className="relative w-40 shrink-0">
                <span aria-hidden="true" className="absolute left-3 top-1/2 -translate-y-1/2 text-sm text-[var(--muted)]">Rp</span>
                <input
                  id={`budget-${row.id}`}
                  inputMode="numeric"
                  autoComplete="off"
                  value={values[row.id] ?? ""}
                  onChange={(event) => setValues((current) => ({ ...current, [row.id]: grouped(Number(digits(event.target.value)) || null) }))}
                  placeholder="Tanpa batas"
                  className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface)] pl-9 pr-3 text-right text-sm tabular-nums text-[var(--text)]"
                />
              </div>
            </li>
          ))}
        </ul>
        {error && <p role="alert" className="text-sm text-[var(--out-fg)]">{error}</p>}
        <div className="flex justify-end gap-2">
          <Button type="button" className="min-h-11" onClick={onClose}>Batal</Button>
          <Button type="submit" variant="primary" className="min-h-11" disabled={save.isPending || isLoading}>
            {save.isPending ? "Menyimpan…" : "Simpan anggaran"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}

export function BudgetPanel({ report, onEdit }: { report: MonthlyReport; onEdit: () => void }) {
  const { bal } = useAppCtx();
  const { budget, period } = report;
  if (!budget) {
    return (
      <ReportPanel id="report-budget" title="Anggaran">
        <p className="text-sm text-[var(--text-2)]">Belum ada anggaran. Anggaran tidak wajib; laporan tetap lengkap tanpanya.</p>
        <Button type="button" className="min-h-11" onClick={onEdit}>Atur anggaran</Button>
      </ReportPanel>
    );
  }
  return (
    <ReportPanel id="report-budget" title="Anggaran dan realisasi">
      {budget.overall && (
        <p className="text-sm text-[var(--text)]">
          Total: keluar <span className="font-semibold tabular-nums">{bal(budget.overall.spent)}</span> dari anggaran{" "}
          <span className="tabular-nums">{bal(budget.overall.budget)}</span>,{" "}
          {budget.overall.remaining >= 0 ? `sisa ${bal(budget.overall.remaining)}` : `lebih ${bal(-budget.overall.remaining)}`}.
        </p>
      )}
      {budget.categories.length > 0 && (
        <ul className="divide-y divide-[var(--line)]">
          {budget.categories.map((row) => (
            <li key={row.category_id} className="space-y-1 py-2">
              <div className="flex min-w-0 items-center justify-between gap-3 text-sm">
                <span className="min-w-0 truncate text-[var(--text)]">{row.name}</span>
                <Link
                  href={categoryLedgerHref(period, { category_id: row.category_id })}
                  aria-label={`${bal(row.spent)} dari ${bal(row.budget)}, lihat transaksi ${row.name}`}
                  className={cn("inline-flex min-h-11 shrink-0 items-center tabular-nums", LINK_CLASS)}
                >
                  <span className="font-semibold">{bal(row.spent)}</span>
                  <span className="text-[var(--muted)]">&nbsp;/ {bal(row.budget)}</span>
                </Link>
              </div>
              <p className="text-xs text-[var(--text-2)]">
                {row.remaining >= 0 ? `Sisa ${bal(row.remaining)}` : `Lebih ${bal(-row.remaining)}`}
              </p>
            </li>
          ))}
        </ul>
      )}
      {budget.period_basis === "cycle" && (
        <p className="text-xs text-[var(--text-2)]">Anggaran bulanan dibandingkan apa adanya dengan satu siklus gaji.</p>
      )}
      <Button type="button" className="min-h-11" onClick={onEdit}>Ubah anggaran</Button>
    </ReportPanel>
  );
}
