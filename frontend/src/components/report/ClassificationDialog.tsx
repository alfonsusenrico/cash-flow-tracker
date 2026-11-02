"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Button } from "@/components/ui/Button";
import { Modal } from "@/components/ui/Modal";
import { api } from "@/lib/api";
import { KAKEIBO_PILLARS, type KakeiboPillar } from "@/lib/kakeibo";
import { shortDate } from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";

interface Gaps {
  categories: { id: string; name: string; unclassified_count: number; unclassified_amount: number }[];
  transactions: {
    id: string;
    date: string;
    amount: number;
    merchant: string | null;
    notes: string | null;
    category_name: string | null;
    account_name: string;
  }[];
  transaction_count: number;
  transaction_limit: number;
}

const SELECT_CLASS =
  "min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]";

function PillarSelect({ id, label, value, onChange }: { id: string; label: string; value: KakeiboPillar | ""; onChange: (value: KakeiboPillar | "") => void }) {
  return (
    <>
      <label htmlFor={id} className="sr-only">{label}</label>
      <select id={id} value={value} onChange={(event) => onChange(event.target.value as KakeiboPillar | "")} className={SELECT_CLASS}>
        <option value="">Pilih pilar</option>
        {KAKEIBO_PILLARS.map((pillar) => (
          <option key={pillar.value} value={pillar.value}>{pillar.label}</option>
        ))}
      </select>
    </>
  );
}

/** "Lengkapi klasifikasi": set a pillar per category, or for one or many unclassified rows at once. */
export function ClassificationDialog({ open, onClose, scope }: { open: boolean; onClose: () => void; scope: { start: string; end: string } | null }) {
  const { bal } = useAppCtx();
  const queryClient = useQueryClient();
  const params = scope ? `?from_date=${encodeURIComponent(scope.start)}&to_date=${encodeURIComponent(scope.end)}` : "";
  const { data, isLoading, isError } = useQuery<Gaps>({
    queryKey: queryKeys.classificationGaps(scope?.start ?? null, scope?.end ?? null),
    queryFn: () => api.get(`/classification/gaps${params}`),
    enabled: open,
  });
  const [categoryPillars, setCategoryPillars] = useState<Record<string, KakeiboPillar | "">>({});
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [rowPillar, setRowPillar] = useState<KakeiboPillar | "">("");
  const [status, setStatus] = useState("");
  useEffect(() => {
    if (!open) return;
    setCategoryPillars({});
    setSelected(new Set());
    setRowPillar("");
    setStatus("");
  }, [open]);

  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["classification"] }),
      queryClient.invalidateQueries({ queryKey: queryKeys.reports.all }),
      queryClient.invalidateQueries({ queryKey: queryKeys.dashboard.all }),
      queryClient.invalidateQueries({ queryKey: queryKeys.transactions.all }),
      queryClient.invalidateQueries({ queryKey: queryKeys.categories }),
    ]);
  };
  const classifyCategory = useMutation({
    mutationFn: ({ id, pillar }: { id: string; name: string; pillar: KakeiboPillar }) =>
      api.post<{ corrections: number }>(`/classification/categories/${id}`, { kakeibo_type: pillar }),
    onSuccess: async (result, variables) => {
      setStatus(`${variables.name} disimpan. ${result.corrections} transaksi ikut terklasifikasi.`);
      await refresh();
    },
    onError: (error: Error) => setStatus(error.message || "Belum tersimpan. Coba lagi."),
  });
  const classifyRows = useMutation({
    mutationFn: ({ ids, pillar }: { ids: string[]; pillar: KakeiboPillar }) =>
      api.post<{ transaction_count: number }>("/classification/transactions", { transaction_ids: ids, kakeibo_type: pillar }),
    onSuccess: async (result) => {
      setStatus(`${result.transaction_count} transaksi diklasifikasi.`);
      setSelected(new Set());
      await refresh();
    },
    onError: (error: Error) => setStatus(error.message || "Belum tersimpan. Coba lagi."),
  });

  const rows = data?.transactions ?? [];
  const toggle = (id: string) =>
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const allSelected = rows.length > 0 && rows.every((row) => selected.has(row.id));

  return (
    <Modal open={open} onClose={onClose} title="Lengkapi klasifikasi" wide>
      <div className="space-y-5">
        <p className="text-sm text-[var(--text-secondary)]">
          Pengeluaran tanpa pilar tampil sebagai Belum diklasifikasi. Pilihan Anda juga dipakai untuk belajar kebiasaan Anda.
        </p>
        <p role="status" className="min-h-5 text-sm font-semibold text-[var(--text)]">{status}</p>
        {isLoading && <p className="text-sm text-[var(--muted)]">Memuat…</p>}
        {isError && <p role="alert" className="text-sm text-[var(--danger-text)]">Daftar belum bisa dimuat.</p>}
        {data && data.categories.length === 0 && data.transaction_count === 0 && (
          <p className="text-sm text-[var(--text)]">Semua pengeluaran sudah punya pilar.</p>
        )}

        {data && data.categories.length > 0 && (
          <section aria-labelledby="classify-categories" className="space-y-2">
            <h3 id="classify-categories" className="text-sm font-bold text-[var(--text)]">Kategori tanpa pilar</h3>
            <p className="text-xs text-[var(--text-secondary)]">Berlaku untuk transaksinya yang belum punya pilar sendiri.</p>
            <ul className="divide-y divide-[var(--border)]">
              {data.categories.map((category) => {
                const value = categoryPillars[category.id] ?? "";
                return (
                  <li key={category.id} className="flex flex-wrap items-center justify-between gap-2 py-2">
                    <span className="min-w-0 text-sm text-[var(--text)]">
                      <span className="block font-semibold">{category.name}</span>
                      <span className="block text-xs text-[var(--muted)] tabular-nums">
                        {category.unclassified_count} transaksi · {bal(category.unclassified_amount)}
                      </span>
                    </span>
                    <span className="flex items-center gap-2">
                      <PillarSelect
                        id={`classify-category-${category.id}`}
                        label={`Pilar untuk ${category.name}`}
                        value={value}
                        onChange={(next) => setCategoryPillars((current) => ({ ...current, [category.id]: next }))}
                      />
                      <Button
                        type="button"
                        className="min-h-11"
                        disabled={!value || classifyCategory.isPending}
                        aria-label={`Simpan pilar ${category.name}`}
                        onClick={() => value && classifyCategory.mutate({ id: category.id, name: category.name, pillar: value })}
                      >
                        Simpan
                      </Button>
                    </span>
                  </li>
                );
              })}
            </ul>
          </section>
        )}

        {rows.length > 0 && (
          <section aria-labelledby="classify-rows" className="space-y-2">
            <h3 id="classify-rows" className="text-sm font-bold text-[var(--text)]">
              Transaksi belum diklasifikasi <span className="tabular-nums text-[var(--muted)]">({data?.transaction_count})</span>
            </h3>
            {data && data.transaction_count > rows.length && (
              <p className="text-xs text-[var(--text-secondary)]">Menampilkan {rows.length} terbaru.</p>
            )}
            <label className="flex min-h-11 items-center gap-2 text-sm text-[var(--text)]">
              <input
                type="checkbox"
                className="h-5 w-5"
                checked={allSelected}
                onChange={() => setSelected(allSelected ? new Set() : new Set(rows.map((row) => row.id)))}
              />
              Pilih semua
            </label>
            <ul className="max-h-72 divide-y divide-[var(--border)] overflow-y-auto rounded-xl border border-[var(--border)] px-3">
              {rows.map((row) => (
                <li key={row.id}>
                  <label className="flex min-h-11 items-center gap-3 py-1.5 text-sm text-[var(--text)]">
                    <input type="checkbox" className="h-5 w-5 shrink-0" checked={selected.has(row.id)} onChange={() => toggle(row.id)} />
                    <span className="min-w-0 flex-1">
                      <span className="block truncate">{row.merchant || row.notes || "Tanpa keterangan"}</span>
                      <span className="block truncate text-xs text-[var(--muted)]">
                        {shortDate(row.date.slice(0, 10))} · {row.category_name ?? "Tanpa kategori"} · {row.account_name}
                      </span>
                    </span>
                    <span className="shrink-0 font-bold tabular-nums">{bal(row.amount)}</span>
                  </label>
                </li>
              ))}
            </ul>
            <div className="flex flex-wrap items-center gap-2">
              <PillarSelect id="classify-rows-pillar" label="Pilar untuk transaksi yang dipilih" value={rowPillar} onChange={setRowPillar} />
              <Button
                type="button"
                variant="primary"
                className="min-h-11"
                disabled={!rowPillar || selected.size === 0 || classifyRows.isPending}
                onClick={() => rowPillar && classifyRows.mutate({ ids: [...selected], pillar: rowPillar })}
              >
                Terapkan ke {selected.size} transaksi
              </Button>
            </div>
          </section>
        )}
        <div className="flex justify-end">
          <Button type="button" className="min-h-11" onClick={onClose}>Selesai</Button>
        </div>
      </div>
    </Modal>
  );
}
