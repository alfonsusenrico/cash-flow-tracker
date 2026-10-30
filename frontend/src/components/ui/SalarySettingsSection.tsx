"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";

type Income = {
  id: string;
  amount: number;
  date: string;
  merchant: string | null;
  notes: string | null;
  movement_id?: string | null;
  is_reimbursement?: boolean;
};
type Candidate = Income & { confirmed: boolean; needs_review: boolean; confirmation_method: string | null };
type Receipt = Income & { confirmation_method: string };
type SalaryAction = { kind: "source" | "confirm" | "revoke" | "reject"; id: string } | { kind: "detect" };

const PAGE_SIZE = 50;
const buttonClass = "min-h-11 px-2 underline underline-offset-2 disabled:opacity-40";

function describe(row: Income) {
  const date = new Date(row.date).toLocaleDateString("id-ID", { dateStyle: "medium" });
  const amount = new Intl.NumberFormat("id-ID", { style: "currency", currency: "IDR", maximumFractionDigits: 0 }).format(row.amount);
  return `${date} · ${amount} · ${row.merchant || row.notes || "Tanpa keterangan"}`;
}

function request(action: SalaryAction) {
  if (action.kind === "detect") return api.post("/salary/detect");
  if (action.kind === "source") return api.put("/salary/source", { transaction_id: action.id });
  if (action.kind === "confirm") return api.post("/salary/receipts", { transaction_id: action.id });
  if (action.kind === "reject") return api.post("/salary/reject", { transaction_id: action.id });
  return api.del(`/salary/receipts/${action.id}`);
}

/** Salary cycles start only from confirmed receipts; the owner picks the source and reviews unclear income. */
export function SalarySettingsSection({ open }: { open: boolean }) {
  const qc = useQueryClient();
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState("");
  const [error, setError] = useState("");

  const source = useQuery({
    queryKey: ["salary", "source"],
    queryFn: () => api.get<{ source: { field: string; value: string } | null }>("/salary/source"),
    enabled: open,
  });
  const candidates = useQuery({
    queryKey: ["salary", "candidates"],
    queryFn: () => api.get<{ candidates: Candidate[] }>("/salary/candidates"),
    enabled: open,
  });
  const invalidated = useQuery({
    queryKey: ["salary", "invalidated"],
    queryFn: () => api.get<{ detections: Income[] }>("/salary/invalidated-detections"),
    enabled: open,
  });
  const receipts = useQuery({
    queryKey: ["salary", "receipts"],
    queryFn: () => api.get<{ receipts: Receipt[] }>("/salary/receipts"),
    enabled: open,
  });
  const incomes = useQuery({
    queryKey: ["salary", "income", offset],
    queryFn: () => api.get<{ transactions: Income[]; total: number }>(`/transactions?type=income&limit=${PAGE_SIZE}&offset=${offset}`),
    enabled: open,
  });

  const action = useMutation({
    mutationFn: request,
    onSuccess: () => {
      setError("");
      for (const key of ["salary", "dashboard", "transactions", "pulse", "insights"]) {
        void qc.invalidateQueries({ queryKey: [key] });
      }
    },
    onError: (err: Error) => setError(err.message || "Gagal menyimpan pilihan gaji."),
  });

  const queries = [source, candidates, invalidated, receipts, incomes];
  const failed = queries.some((query) => query.isError);
  const pending = queries.some((query) => query.isPending);
  const eligible = incomes.data?.transactions.filter((row) => !row.movement_id && !row.is_reimbursement) ?? [];
  const confirmedIds = new Set(receipts.data?.receipts.map((row) => row.id) ?? []);
  const reviewRows = new Map<string, Income>();
  for (const row of candidates.data?.candidates ?? []) {
    if (row.needs_review) reviewRows.set(row.id, row);
  }
  for (const row of invalidated.data?.detections ?? []) reviewRows.set(row.id, row);
  const configured = source.data?.source;
  const changePage = (next: number) => {
    setOffset(next);
    setSelected("");
  };

  return (
    <section aria-labelledby="salary-settings-title" className="space-y-3 border-t border-[var(--border)] pt-4 text-xs">
      <h3 id="salary-settings-title" className="font-bold uppercase tracking-wider text-[var(--muted)]">Siklus Gaji</h3>
      <p className="text-[var(--text-secondary)]">
        Siklus dimulai dari gaji yang benar-benar diterima. Pilih satu pemasukan dari pemberi gaji sebagai sumber. Pemasukan
        dari sumber itu yang menyebut gaji atau salary dikenali otomatis; bonus, refund, dan pemasukan lain perlu ditinjau.
        Akhir siklus berjalan baru diketahui saat gaji berikutnya diterima.
      </p>
      {pending && <p role="status">Memuat data gaji…</p>}
      {failed && <p role="alert">Data gaji tidak dapat dimuat. Tutup dan buka pengaturan untuk mencoba lagi.</p>}
      {error && <p role="alert" className="text-rose-600 dark:text-rose-300">{error}</p>}

      {!pending && !failed && (
        <>
          <p>
            {configured
              ? `Sumber: ${configured.value} (${configured.field === "merchant" ? "pengirim" : "keterangan"})`
              : "Sumber gaji belum dipilih. Laporan memakai bulan kalender sampai ada gaji terkonfirmasi."}
          </p>

          {eligible.length === 0 ? (
            <p className="text-[var(--muted)]">Belum ada pemasukan tercatat yang bisa dipilih.</p>
          ) : (
            <>
              <label htmlFor="salary-income" className="block font-medium">Pilih pemasukan yang sudah tercatat</label>
              <select
                id="salary-income"
                value={selected}
                onChange={(event) => setSelected(event.target.value)}
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-sm text-[var(--text)]"
              >
                <option value="">Pilih pemasukan</option>
                {eligible.map((row) => <option key={row.id} value={row.id}>{describe(row)}</option>)}
              </select>
            </>
          )}
          <div className="flex flex-wrap gap-2">
            <button type="button" disabled={offset === 0} onClick={() => changePage(Math.max(0, offset - PAGE_SIZE))} className={buttonClass}>
              Lebih baru
            </button>
            <button
              type="button"
              disabled={offset + PAGE_SIZE >= (incomes.data?.total ?? 0)}
              onClick={() => changePage(offset + PAGE_SIZE)}
              className={buttonClass}
            >
              Lebih lama
            </button>
            <button type="button" disabled={!selected || action.isPending} onClick={() => action.mutate({ kind: "source", id: selected })} className={buttonClass}>
              Gunakan sebagai sumber
            </button>
            <button
              type="button"
              disabled={!selected || action.isPending || confirmedIds.has(selected)}
              onClick={() => action.mutate({ kind: "confirm", id: selected })}
              className={buttonClass}
            >
              Tandai sebagai gaji
            </button>
          </div>
          {configured && (
            <button type="button" disabled={action.isPending} onClick={() => action.mutate({ kind: "detect" })} className={buttonClass}>
              Periksa pemasukan dari sumber ini
            </button>
          )}

          <h4 className="font-semibold">Perlu ditinjau</h4>
          {reviewRows.size === 0 ? (
            <p className="text-[var(--muted)]">Tidak ada pemasukan yang perlu ditinjau.</p>
          ) : (
            <ul className="space-y-2">
              {Array.from(reviewRows.values()).map((row) => (
                <li key={row.id} className="border-b border-[var(--border)] pb-2">
                  <span>{describe(row)}</span>
                  <button
                    type="button"
                    disabled={action.isPending}
                    onClick={() => action.mutate({ kind: "confirm", id: row.id })}
                    aria-label={`Konfirmasi gaji: ${describe(row)}`}
                    className={`ml-2 ${buttonClass}`}
                  >
                    Konfirmasi gaji
                  </button>
                  <button
                    type="button"
                    disabled={action.isPending}
                    onClick={() => action.mutate({ kind: "reject", id: row.id })}
                    aria-label={`Bukan gaji: ${describe(row)}`}
                    className={`ml-2 ${buttonClass}`}
                  >
                    Bukan gaji
                  </button>
                </li>
              ))}
            </ul>
          )}

          <h4 className="font-semibold">Gaji terkonfirmasi</h4>
          {!receipts.data?.receipts.length ? (
            <p className="text-[var(--muted)]">Belum ada gaji terkonfirmasi.</p>
          ) : (
            <ul className="space-y-2">
              {receipts.data.receipts.map((row) => (
                <li key={row.id} className="border-b border-[var(--border)] pb-2">
                  <span>
                    {describe(row)} · {row.confirmation_method === "detected" ? "otomatis" : "dikonfirmasi"}
                  </span>
                  <button
                    type="button"
                    disabled={action.isPending}
                    onClick={() => action.mutate({ kind: "revoke", id: row.id })}
                    aria-label={`Batalkan konfirmasi gaji: ${describe(row)}`}
                    className={`ml-2 ${buttonClass}`}
                  >
                    Batalkan konfirmasi
                  </button>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </section>
  );
}
