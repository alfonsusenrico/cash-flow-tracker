"use client";

import { useEffect, useRef, useState } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { rowKind, rowStatus, rowTitle } from "@/components/beranda/activity";
import { Icon } from "@/components/ui/Icon";
import { Drawer, useToast } from "@/components/workspace/overlays";
import { ErrorState, Pill, Skeleton, type Tone } from "@/components/workspace/primitives";
import { ApiError, api } from "@/lib/api";
import { classifierLabel, habitBasisText, isHabitRecorded, sourceLabel } from "@/lib/provenance";
import { queryKeys } from "@/lib/queryKeys";
import type { UndoFields } from "@/lib/reviewUndo";
import type { LedgerRowDetail } from "./types";
import { useRefreshTransactionViews } from "./useTransactionUndo";

// The editors load when first used, so Beranda, which also opens this drawer, stays light.
const TransactionEditForm = dynamic(() => import("./TransactionEditForm").then((module) => module.TransactionEditForm), { ssr: false });
const ExpenseTopupConversionModal = dynamic(() => import("@/components/ui/ExpenseTopupConversionModal").then((module) => module.ExpenseTopupConversionModal), { ssr: false });
const InternalMovementModal = dynamic(() => import("@/components/ui/InternalMovementModal").then((module) => module.InternalMovementModal), { ssr: false });
const InvestmentTopupModal = dynamic(() => import("@/components/ui/InvestmentTopupModal").then((module) => module.InvestmentTopupModal), { ssr: false });

type RecordUndo = (text: string, label: string, response: UndoFields | null | undefined, transactionId: string) => void;

const WHEN = new Intl.DateTimeFormat("id-ID", { weekday: "long", day: "numeric", month: "long", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });

const KIND_PILL: { [key: string]: { tone: Tone; text: string } } = {
  expense: { tone: "out", text: "Keluar" },
  income: { tone: "in", text: "Masuk" },
  movement: { tone: "tr", text: "Pindah saldo" },
  topup: { tone: "inv", text: "Top up Investasi" },
};

/** Rows the owner can mark as checked here: automatic ones; waiting rows are answered in Tinjau (D9). */
function checkable(row: LedgerRowDetail): boolean {
  return !row.waiting && row.review_status === "auto";
}

function counterpartName(row: LedgerRowDetail): string | null {
  return row.transfer_target_account_name ?? row.target_account_name ?? null;
}

function deleteEffects(row: LedgerRowDetail, bal: (amount: number) => string): string[] {
  const kind = rowKind(row);
  if (kind === "topup") {
    return [`Nilai produk berkurang ${bal(row.amount)}.`, "Debit yang pernah diubah menjadi top up ini kembali menjadi pengeluaran."];
  }
  if (kind === "movement" && row.movement_id) {
    return [`Saldo ${row.account_name}${counterpartName(row) ? ` dan ${counterpartName(row)}` : ""} kembali seperti sebelum pindah saldo.`];
  }
  const effects = [row.type === "expense"
    ? `Saldo ${row.account_name} bertambah kembali ${bal(row.amount)}.`
    : `Saldo ${row.account_name} berkurang ${bal(row.amount)}.`];
  for (const item of row.obligation_allocations ?? []) {
    effects.push(`Pembayaran ${item.obligation_name} ${bal(item.amount)} dibatalkan, sisa tagihannya bertambah lagi.`);
  }
  if (!row.obligation_allocations?.length && row.obligation_name) {
    effects.push(row.type === "expense"
      ? `Pembayaran ${row.obligation_name} dibatalkan, sisa tagihannya bertambah lagi.`
      : `Sisa ${row.obligation_name} berkurang lagi.`);
  }
  return effects;
}

/** The path each row kind is deleted through (D10): a top-up only through its own lifecycle. */
function deletePath(row: LedgerRowDetail): string {
  if (row.investment_topup_id) return `/investment-topups/${row.investment_topup_id}`;
  if (row.movement_id) return `/movements/${row.movement_id}`;
  return `/transactions/${row.id}`;
}

/**
 * The shared transaction drawer of Transaksi, Beranda and Tinjau (redesign-transactions-workspace D6–D10):
 * detail and provenance, the owner's source text, edit with the type switch, "Tandai sudah dicek" with
 * undo, and delete after a confirmation. A side column, an overlay or a bottom sheet by width.
 */
export function TransactionDrawer({
  transactionId,
  onClose,
  record,
  onPickPair,
  onDeleted,
}: {
  transactionId: string | null;
  onClose: () => void;
  record: RecordUndo;
  /** Starts the two-row merge with this row selected (OD23); absent where the page has no list to pick from. */
  onPickPair?: (row: LedgerRowDetail) => void;
  onDeleted?: (row: LedgerRowDetail) => void;
}) {
  const { bal } = useAppCtx();
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();
  const [mode, setMode] = useState<"view" | "edit" | "delete">("view");
  const [topupOpen, setTopupOpen] = useState(false);
  const [modalEdit, setModalEdit] = useState(false);
  const [actionError, setActionError] = useState("");
  const cancelDeleteRef = useRef<HTMLButtonElement>(null);
  const accounts = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts"), enabled: Boolean(transactionId) }).data?.accounts ?? [];

  const detail = useQuery<{ transaction: LedgerRowDetail }>({
    queryKey: queryKeys.transactions.detail(transactionId ?? ""),
    queryFn: () => api.get(`/transactions/${transactionId}`),
    enabled: Boolean(transactionId),
    retry: (count, error) => !(error instanceof ApiError && error.status === 404) && count < 2,
  });
  const row = detail.data?.transaction ?? null;
  const missing = detail.error instanceof ApiError && detail.error.status === 404;

  useEffect(() => {
    setMode("view");
    setActionError("");
  }, [transactionId]);

  useEffect(() => {
    if (mode === "delete") cancelDeleteRef.current?.focus();
  }, [mode]);

  // A row deleted elsewhere: the lists behind the drawer still show it, so they reload.
  useEffect(() => {
    if (missing) refresh();
  }, [missing, refresh]);

  const markChecked = useMutation({
    mutationFn: (target: LedgerRowDetail) => api.post<UndoFields>("/review/bulk", { action: "accept", transaction_ids: [target.id] }),
    onSuccess: (response, target) => {
      refresh();
      record("Ditandai sudah dicek.", rowTitle(target), response, target.id);
    },
    onError: (error: Error) => setActionError(error.message || "Gagal menandai transaksi."),
  });

  const remove = useMutation({
    mutationFn: (target: LedgerRowDetail) => api.del(deletePath(target)),
    onSuccess: (_result, target) => {
      refresh();
      showToast("Transaksi dihapus.");
      onDeleted?.(target);
      onClose();
    },
    onError: (error: Error) => setActionError(error.message || "Gagal menghapus transaksi."),
  });

  const open = Boolean(transactionId);
  const kind = row ? rowKind(row) : null;
  const status = row ? rowStatus(row) : null;
  const title = row ? rowTitle(row) : missing ? "Transaksi tidak ditemukan" : "Detail transaksi";
  const subtitle = row ? `${WHEN.format(new Date(row.date))} · ${row.account_name}` : undefined;
  const editInline = row && !row.movement_id && !row.investment_topup_id;

  return (
    <>
      <Drawer open={open} onClose={onClose} title={title} subtitle={subtitle}>
        {detail.isLoading && (
          <div className="flex flex-col gap-3" aria-busy="true">
            <Skeleton className="h-16" />
            <Skeleton className="h-28" />
          </div>
        )}
        {missing && (
          <div role="alert" className="ws-notice">Transaksi ini sudah tidak ada. Daftarnya sudah dimuat ulang.</div>
        )}
        {detail.isError && !missing && (
          <ErrorState message="Detail transaksi belum bisa dimuat." onRetry={() => detail.refetch()} />
        )}
        {row && kind && status && (
          <>
            <div className="ws-tx-amount">
              <span className="ws-caption">Jumlah</span>
              <span className={`ws-tx-amount-v ws-tx-${kind}`}>
                {kind === "income" ? "+" : kind === "expense" ? "−" : ""}{bal(row.amount)}
              </span>
              <Pill tone={KIND_PILL[kind].tone}>{KIND_PILL[kind].text}</Pill>
            </div>

            {row.waiting && (
              <div className="ws-banner ws-banner-warn" role="note">
                <div>
                  <b>Menunggu jawaban di Tinjau.</b> Transaksi ini sudah tercatat; jawabannya melengkapi kategori atau pasangannya.
                  <div className="mt-2">
                    <Link className="ws-btn ws-btn-sm" href={`/review?item=${row.id}`}>Jawab di Tinjau</Link>
                  </div>
                </div>
              </div>
            )}

            <dl className="ws-tx-facts">
              <dt>Dicatat</dt>
              <dd>{sourceLabel(row.source) ?? "—"}</dd>
              <dt>Diklasifikasikan</dt>
              <dd>{classifierLabel(row.classified_by) ?? "—"}</dd>
              {(kind === "movement" || kind === "topup") && (
                <>
                  <dt>Dari</dt>
                  <dd>{row.movement_role === "inbound" ? counterpartName(row) ?? "—" : row.account_name}</dd>
                  <dt>Ke</dt>
                  <dd>{row.movement_role === "inbound" ? row.account_name : counterpartName(row) ?? "Pasangan belum ditemukan"}</dd>
                </>
              )}
              {kind === "expense" && (
                <>
                  <dt>Kakeibo</dt>
                  <dd>
                    <span className="ws-kk" data-bucket={status.bucket} style={status.color ? ({ "--kc": status.color } as React.CSSProperties) : undefined}>{status.text}</span>
                  </dd>
                </>
              )}
              {kind !== "movement" && kind !== "topup" && (
                <>
                  <dt>Kategori</dt>
                  <dd>{row.category_name ?? "Tanpa kategori"}</dd>
                </>
              )}
              {isHabitRecorded(row.classified_by, row.habit_basis) && (
                <>
                  <dt>Kebiasaan</dt>
                  <dd>{habitBasisText(row.habit_basis)}</dd>
                </>
              )}
              {row.merchant && (
                <>
                  <dt>Merchant</dt>
                  <dd>{row.merchant}</dd>
                </>
              )}
              {row.notes && (
                <>
                  <dt>Catatan</dt>
                  <dd>{row.notes}</dd>
                </>
              )}
              {row.goal_name && (
                <>
                  <dt>Target</dt>
                  <dd>{row.goal_name}</dd>
                </>
              )}
              {(row.obligation_allocations?.length ? row.obligation_allocations : row.obligation_name ? [{ obligation_name: row.obligation_name, amount: row.amount, obligation_id: row.obligation_id ?? "" }] : []).map((item) => (
                <div key={item.obligation_id} className="contents">
                  <dt>Tagihan</dt>
                  <dd>{item.obligation_name} · {bal(item.amount)}</dd>
                </div>
              ))}
              <dt>Dicek</dt>
              <dd>{row.review_status === "auto" || row.waiting ? "Belum dicek" : "Sudah dicek"}</dd>
            </dl>

            {row.origin && (
              <figure className="ws-tx-quote">
                <figcaption>{row.origin.kind === "capture" ? "Teks yang diketik" : `Notifikasi asli${row.origin.app ? ` · ${row.origin.app}` : ""}`}</figcaption>
                <blockquote>{row.origin.text}</blockquote>
              </figure>
            )}

            {actionError && <div role="alert" className="ws-notice ws-tx-error">{actionError}</div>}

            {mode === "edit" && editInline && (
              <TransactionEditForm
                tx={row}
                onSaved={() => {
                  refresh();
                  setMode("view");
                  showToast("Perubahan disimpan.");
                }}
                onCancel={() => setMode("view")}
                onPickPair={() => onPickPair?.(row)}
                onConvertToTopup={() => setTopupOpen(true)}
              />
            )}

            {mode === "delete" && (
              <section className="ws-tx-confirm" role="group" aria-labelledby="tx-delete-title">
                <h3 id="tx-delete-title">Hapus transaksi {bal(row.amount)}?</h3>
                <ul>
                  {deleteEffects(row, bal).map((effect) => <li key={effect}>{effect}</li>)}
                </ul>
                <p className="ws-caption">Penghapusan tidak dapat dibatalkan.</p>
                <div className="ws-tx-actions">
                  <button type="button" className="ws-btn ws-btn-danger" disabled={remove.isPending} onClick={() => remove.mutate(row)}>
                    {remove.isPending ? "Menghapus…" : "Hapus"}
                  </button>
                  <button ref={cancelDeleteRef} type="button" className="ws-btn" onClick={() => setMode("view")}>Batal</button>
                </div>
              </section>
            )}

            {mode === "view" && (
              <div className="ws-tx-actions">
                {!row.waiting && (
                  <button type="button" className="ws-btn" onClick={() => (editInline ? setMode("edit") : setModalEdit(true))}>
                    <Icon name="edit" className="h-4 w-4" />
                    Ubah
                  </button>
                )}
                {checkable(row) && (
                  <button type="button" className="ws-btn" disabled={markChecked.isPending} onClick={() => markChecked.mutate(row)}>
                    <Icon name="check" className="h-4 w-4" />
                    {markChecked.isPending ? "Menandai…" : "Tandai sudah dicek"}
                  </button>
                )}
                <button type="button" className="ws-btn ws-tx-danger" onClick={() => setMode("delete")}>
                  <Icon name="trash" className="h-4 w-4" />
                  Hapus
                </button>
              </div>
            )}
          </>
        )}
      </Drawer>

      {row && topupOpen && (
        <ExpenseTopupConversionModal
          expense={topupOpen ? row : null}
          accounts={accounts}
          onClose={() => setTopupOpen(false)}
          onConverted={(response) => {
            setTopupOpen(false);
            setMode("view");
            refresh();
            record("Dicatat sebagai top up investasi.", rowTitle(row), response, row.id);
          }}
        />
      )}
      {row && modalEdit && row.movement_id && !row.investment_topup_id && (
        <InternalMovementModal
          open
          onClose={() => { setModalEdit(false); refresh(); }}
          editingMovement={{
            id: row.movement_id,
            sourceAccountId: row.movement_role === "inbound" ? row.transfer_target_account_id ?? "" : row.account_id,
            targetAccountId: row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id ?? "",
            amount: row.amount,
            notes: row.notes,
            date: row.date,
          }}
        />
      )}
      {row && modalEdit && row.investment_topup_id && (
        <InvestmentTopupModal
          open
          onClose={() => { setModalEdit(false); refresh(); }}
          product={accounts.flatMap((account) => [account, ...(account.children ?? [])]).find((account) => account.id === (row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id)) ?? {
            id: row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id ?? "",
            name: row.movement_role === "inbound" ? row.account_name : row.transfer_target_account_name ?? "Produk reksadana",
            type: "investment",
          }}
          allAccounts={accounts}
          editingTopup={{
            id: row.investment_topup_id,
            source_account_id: row.movement_role === "inbound" ? row.transfer_target_account_id ?? "" : row.account_id,
            target_account_id: row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id ?? "",
            amount: row.amount,
            date: row.date,
            notes: row.notes,
          }}
        />
      )}
    </>
  );
}
