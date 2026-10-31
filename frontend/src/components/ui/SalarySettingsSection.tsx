"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/Button";
import { Icon } from "@/components/ui/Icon";
import {
  SettingsAlert, SettingsCard, SettingsField, SettingsList, SettingsSubsection, settingsControlClass,
} from "@/components/settings/SettingsCard";

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
const quietButton = "min-h-11 text-[var(--text-secondary)]";

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

function IncomeRow({ row, detail, children }: { row: Income; detail?: string; children: React.ReactNode }) {
  return (
    <li className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-2.5">
      <p className="min-w-0 flex-1 break-words tabular-nums text-[var(--text)]">
        {describe(row)}
        {detail && <span className="block text-xs text-[var(--muted)]">{detail}</span>}
      </p>
      <div className="flex flex-wrap justify-end gap-2">{children}</div>
    </li>
  );
}

/** Salary cycles start only from confirmed receipts; the owner picks the source and reviews unclear income. */
export function SalarySettingsSection() {
  const qc = useQueryClient();
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState("");
  const [error, setError] = useState("");

  const source = useQuery({
    queryKey: ["salary", "source"],
    queryFn: () => api.get<{ source: { field: string; value: string } | null }>("/salary/source"),
  });
  const candidates = useQuery({
    queryKey: ["salary", "candidates"],
    queryFn: () => api.get<{ candidates: Candidate[] }>("/salary/candidates"),
  });
  const invalidated = useQuery({
    queryKey: ["salary", "invalidated"],
    queryFn: () => api.get<{ detections: Income[] }>("/salary/invalidated-detections"),
  });
  const receipts = useQuery({
    queryKey: ["salary", "receipts"],
    queryFn: () => api.get<{ receipts: Receipt[] }>("/salary/receipts"),
  });
  const incomes = useQuery({
    queryKey: ["salary", "income", offset],
    queryFn: () => api.get<{ transactions: Income[]; total: number }>(`/transactions?type=income&limit=${PAGE_SIZE}&offset=${offset}`),
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
  const busy = action.isPending;
  const changePage = (next: number) => {
    setOffset(next);
    setSelected("");
  };

  return (
    <SettingsCard section="gaji">
      <p className="text-xs text-[var(--muted)]">
        Pemasukan dari sumber yang menyebut gaji atau salary dikenali otomatis; bonus, refund dan pemasukan lain ditinjau dulu.
        Akhir siklus berjalan baru diketahui saat gaji berikutnya diterima.
      </p>
      {pending && <p role="status" className="text-[var(--muted)]">Memuat data gaji…</p>}
      {failed && <SettingsAlert>Data gaji tidak dapat dimuat. Muat ulang halaman untuk mencoba lagi.</SettingsAlert>}
      {error && <SettingsAlert>{error}</SettingsAlert>}

      {!pending && !failed && (
        <>
          <SettingsSubsection id="salary-source-title" title="Sumber gaji saat ini">
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-[var(--radius-md)] border border-[var(--border)] bg-[var(--surface-raised)]/40 px-3 py-2.5">
              {configured ? (
                <p className="min-w-0 flex-1 break-words text-[var(--text)]">
                  <span className="font-semibold">{configured.value}</span>
                  <span className="block text-xs text-[var(--muted)]">
                    Dikenali dari {configured.field === "merchant" ? "nama pengirim" : "keterangan"}
                  </span>
                </p>
              ) : (
                <p className="min-w-0 flex-1 text-[var(--text-secondary)]">
                  Sumber gaji belum dipilih. Laporan memakai bulan kalender sampai ada gaji terkonfirmasi.
                </p>
              )}
              {configured && (
                <Button type="button" size="sm" className="min-h-11" disabled={busy} onClick={() => action.mutate({ kind: "detect" })}>
                  Periksa pemasukan dari sumber ini
                </Button>
              )}
            </div>

            <details open={!configured} className="group rounded-[var(--radius-md)] border border-[var(--border)]">
              <summary className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-2 rounded-[var(--radius-md)] px-3 font-semibold text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)] [&::-webkit-details-marker]:hidden">
                {configured ? "Ganti sumber" : "Pilih sumber"}
                <Icon name="chevron-down" className="h-4 w-4 text-[var(--muted)] transition-transform group-open:rotate-180" />
              </summary>
              <div className="space-y-3 border-t border-[var(--border)] p-3">
                {eligible.length === 0 ? (
                  <p className="text-[var(--muted)]">Belum ada pemasukan tercatat yang bisa dipilih.</p>
                ) : (
                  <SettingsField id="salary-income" label="Pilih pemasukan yang sudah tercatat" help="Pilih satu pemasukan dari pemberi gaji.">
                    {(field) => (
                      <select {...field} value={selected} onChange={(event) => setSelected(event.target.value)} className={`${settingsControlClass} tabular-nums`}>
                        <option value="">Pilih pemasukan</option>
                        {eligible.map((row) => <option key={row.id} value={row.id}>{describe(row)}</option>)}
                      </select>
                    )}
                  </SettingsField>
                )}
                <div role="group" aria-label="Halaman pemasukan" className="flex flex-wrap gap-2">
                  <Button type="button" size="sm" className="min-h-11" disabled={offset === 0} onClick={() => changePage(Math.max(0, offset - PAGE_SIZE))}>
                    Lebih baru
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    className="min-h-11"
                    disabled={offset + PAGE_SIZE >= (incomes.data?.total ?? 0)}
                    onClick={() => changePage(offset + PAGE_SIZE)}
                  >
                    Lebih lama
                  </Button>
                </div>
                <div className="flex flex-wrap justify-end gap-2 border-t border-[var(--border)] pt-3">
                  <Button
                    type="button"
                    size="sm"
                    className="min-h-11"
                    disabled={!selected || busy || confirmedIds.has(selected)}
                    onClick={() => action.mutate({ kind: "confirm", id: selected })}
                  >
                    Tandai sebagai gaji
                  </Button>
                  <Button
                    type="button"
                    variant="primary"
                    size="sm"
                    className="min-h-11"
                    disabled={!selected || busy}
                    onClick={() => action.mutate({ kind: "source", id: selected })}
                  >
                    Jadikan sumber gaji
                  </Button>
                </div>
              </div>
            </details>
          </SettingsSubsection>

          <SettingsSubsection id="salary-review-title" title="Perlu ditinjau" description="Pemasukan dari sumber gaji yang belum jelas gaji atau bukan.">
            {reviewRows.size === 0 ? (
              <p className="text-[var(--muted)]">Tidak ada pemasukan yang perlu ditinjau.</p>
            ) : (
              <SettingsList label="Pemasukan yang perlu ditinjau">
                {Array.from(reviewRows.values()).map((row) => (
                  <IncomeRow key={row.id} row={row}>
                    <Button
                      type="button"
                      size="sm"
                      className="min-h-11"
                      disabled={busy}
                      onClick={() => action.mutate({ kind: "confirm", id: row.id })}
                      aria-label={`Konfirmasi gaji: ${describe(row)}`}
                    >
                      Ini gaji
                    </Button>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className={quietButton}
                      disabled={busy}
                      onClick={() => action.mutate({ kind: "reject", id: row.id })}
                      aria-label={`Bukan gaji: ${describe(row)}`}
                    >
                      Bukan gaji
                    </Button>
                  </IncomeRow>
                ))}
              </SettingsList>
            )}
          </SettingsSubsection>

          <SettingsSubsection id="salary-receipts-title" title="Gaji terkonfirmasi">
            {!receipts.data?.receipts.length ? (
              <p className="text-[var(--muted)]">Belum ada gaji terkonfirmasi.</p>
            ) : (
              <SettingsList label="Gaji terkonfirmasi">
                {receipts.data.receipts.map((row) => (
                  <IncomeRow key={row.id} row={row} detail={row.confirmation_method === "detected" ? "Dikenali otomatis" : "Dikonfirmasi Anda"}>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className={quietButton}
                      disabled={busy}
                      onClick={() => action.mutate({ kind: "revoke", id: row.id })}
                      aria-label={`Batalkan konfirmasi gaji: ${describe(row)}`}
                    >
                      Batalkan
                    </Button>
                  </IncomeRow>
                ))}
              </SettingsList>
            )}
          </SettingsSubsection>
        </>
      )}
    </SettingsCard>
  );
}
