"use client";

import Link from "next/link";
import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, parseNumberFromDots } from "@/lib/utils";
import { Modal } from "@/components/ui/Modal";
import { MonetaryInput } from "@/components/ui/FormField";
import styles from "./money.module.css";
import type { AccountItem } from "./types";

type CauseKind = "duplicate" | "unpaired" | "held" | "skipped" | "unrecorded" | "pending";

export interface BalanceCause {
  key: string;
  kind: CauseKind;
  effect: number | null;
  amount: number | null;
  direction: "income" | "expense" | null;
  at: string;
  label: string | null;
  account: string | null;
  app: string | null;
  link: { kind: "transaction"; id: string } | { kind: "review"; folder: string | null };
  twin: { transaction_id: string; account: string | null; app: string | null; at: string } | null;
}

export interface BalanceCheckResult {
  account: { id: string; name: string; is_parent: boolean };
  actual_balance: number;
  recorded_balance: number;
  difference: number;
  since: { at: string; basis: "matched" | "adjusted" | "window" };
  candidates: BalanceCause[];
  unattributed: BalanceCause[];
  explanation: string[] | null;
  last_report_at: string | null;
  capture_gap: boolean;
  can_adjust: boolean;
}

const KIND_LABEL: Record<CauseKind, string> = {
  duplicate: "Kemungkinan tercatat dua kali",
  unpaired: "Pindah saldo tanpa pasangan",
  held: "Laporan kedua menunggu di Tinjau",
  skipped: "Laporan dilewati otomatis",
  unrecorded: "Laporan belum dicatat",
  pending: "Laporan masih diproses",
};

const WHEN = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });

/** Liquid accounts and pockets, and parents whose pockets are all liquid (manual-balance-check D2). */
export function canCheckBalance(account: AccountItem): boolean {
  // Credit is left out too: its balance is what is owed, not money in a bank app (add-credit-accounts D8).
  const liquid = (item: AccountItem) => (
    item.type !== "investment"
    && item.type !== "credit"
    && !item.instrument_type
    && item.investment_tracking_mode !== "amount"
  );
  return liquid(account) && (account.children ?? []).every(liquid);
}

function basisText(since: BalanceCheckResult["since"]): string {
  const day = DAY.format(new Date(since.at));
  if (since.basis === "matched") return `Sejak saldo cocok ${day}`;
  if (since.basis === "adjusted") return `Sejak penyesuaian saldo ${day}`;
  return "30 hari terakhir";
}

function causeDetail(cause: BalanceCause): string {
  const parts = [cause.label, cause.account, cause.app, WHEN.format(new Date(cause.at))].filter(Boolean);
  if (cause.kind === "duplicate" && cause.twin) {
    const twin = [cause.twin.account, cause.twin.app].filter(Boolean).join(", ");
    parts.push(`juga tercatat di ${twin} ${WHEN.format(new Date(cause.twin.at))}`);
  }
  return parts.join(" · ");
}

function signedAmount(cause: BalanceCause, money: (value: number) => string): string {
  if (cause.amount == null) return "Nominal belum diketahui";
  if (cause.direction === "income") return `+${money(cause.amount)}`;
  if (cause.direction === "expense") return `−${money(cause.amount)}`;
  return money(cause.amount);
}

function causeHref(cause: BalanceCause): string {
  if (cause.link.kind === "transaction") return `/ledger?edit=${cause.link.id}`;
  return cause.link.folder ? `/review?folder=${cause.link.folder}` : "/review";
}

function CauseList({ causes, money, highlighted }: { causes: BalanceCause[]; money: (value: number) => string; highlighted: Set<string> }) {
  return (
    <ul className={styles.checkCauses}>
      {causes.map((cause) => (
        <li key={cause.key} data-explains={highlighted.has(cause.key) || undefined}>
          <span className={styles.checkCauseText}>
            <b>{KIND_LABEL[cause.kind]}</b>
            <small>{causeDetail(cause)}</small>
          </span>
          <span className={styles.checkCauseEnd}>
            <b className="tabular-nums">{signedAmount(cause, money)}</b>
            <Link href={causeHref(cause)} className={styles.checkLink}>
              {cause.link.kind === "transaction" ? "Buka transaksi" : "Buka di Tinjau"}
            </Link>
          </span>
        </li>
      ))}
    </ul>
  );
}

export function BalanceCheckDialog({
  account,
  money,
  onClose,
  onAdjust,
}: {
  account: AccountItem;
  money: (value: number) => string;
  onClose: () => void;
  /** Opens "Sesuaikan saldo" with the typed balance; `onReconciled` runs after it succeeds. */
  onAdjust: (actual: number, onReconciled: (actual: number) => void) => void;
}) {
  const qc = useQueryClient();
  const [actualText, setActualText] = useState("");
  const [inputError, setInputError] = useState("");
  const [result, setResult] = useState<BalanceCheckResult | null>(null);
  const [matchedAt, setMatchedAt] = useState<string | null>(null);

  const checkMutation = useMutation({
    mutationFn: (actual: number) => api.post<BalanceCheckResult>(`/accounts/${account.id}/balance-check`, { actual_balance: actual }),
    onSuccess: (data) => { setResult(data); setMatchedAt(null); },
    onError: (error: Error) => setInputError(error.message || "Saldo belum bisa diperiksa."),
  });
  const matchMutation = useMutation({
    mutationFn: (actual: number) => api.post<{ matched_at: string }>(`/accounts/${account.id}/balance-matches`, { actual_balance: actual }),
    onSuccess: (data) => {
      setMatchedAt(data.matched_at);
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
    },
  });

  const runCheck = () => {
    const clean = actualText.trim().replace(/[^0-9]/g, "");
    if (!clean) {
      setInputError("Masukkan saldo yang tampil di aplikasi bank.");
      document.getElementById("balance-check-actual")?.focus();
      return;
    }
    setInputError("");
    checkMutation.mutate(parseNumberFromDots(actualText));
  };

  const afterAdjust = (actual: number) => {
    matchMutation.mutate(actual, { onSuccess: () => checkMutation.mutate(actual) });
  };

  const highlighted = new Set(result?.explanation ?? []);
  const explaining = result ? result.candidates.filter((cause) => highlighted.has(cause.key)) : [];
  const difference = result?.difference ?? 0;

  return (
    <Modal open onClose={onClose} title={`Cocokkan saldo: ${account.name}`} subtitle="Bandingkan dengan saldo di aplikasi bank" wide>
      <form
        className={styles.checkForm}
        onSubmit={(event) => {
          event.preventDefault();
          if (!checkMutation.isPending) runCheck();
        }}
      >
        <MonetaryInput
          id="balance-check-actual"
          name="actual_balance"
          label="Saldo di aplikasi bank (IDR)"
          value={actualText}
          onChange={(value) => { setActualText(value); setInputError(""); }}
          placeholder="Contoh: 1.250.000"
          error={inputError}
        />
        <button type="submit" className="ws-btn ws-btn-accent" disabled={checkMutation.isPending}>
          {checkMutation.isPending ? "Memeriksa…" : "Periksa"}
        </button>
      </form>

      <div aria-live="polite">
        {result && (
          <section className={styles.checkResult} aria-label="Hasil pemeriksaan">
            <dl className={styles.detailFacts}>
              <div><dt>Tercatat</dt><dd>{money(result.recorded_balance)}</dd></div>
              <div><dt>Di aplikasi bank</dt><dd>{money(result.actual_balance)}</dd></div>
              <div>
                <dt>Selisih</dt>
                <dd className={cn(difference > 0 && styles.checkUp, difference < 0 && styles.checkDown)}>
                  {difference === 0 ? "Cocok" : `${difference > 0 ? "+" : "−"}${money(Math.abs(difference))}`}
                </dd>
              </div>
            </dl>
            <p className="ws-caption">{basisText(result.since)}{result.account.is_parent ? " · termasuk semua kantong" : ""}</p>
            {result.capture_gap && result.last_report_at && (
              <p className={styles.checkNote}>
                Notifikasi terakhir dari HP diterima {WHEN.format(new Date(result.last_report_at))}. Transaksi sesudahnya mungkin belum tercatat.
              </p>
            )}

            {difference === 0 ? (
              matchedAt ? (
                <p className={styles.checkNote} role="status">Saldo cocok disimpan. Pemeriksaan berikutnya dimulai dari sini.</p>
              ) : (
                <div className={styles.detailActions}>
                  <button type="button" className="ws-btn ws-btn-accent" disabled={matchMutation.isPending} onClick={() => matchMutation.mutate(result.actual_balance)}>
                    {matchMutation.isPending ? "Menyimpan…" : "Saldo cocok"}
                  </button>
                </div>
              )
            ) : (
              <>
                {explaining.length > 0 && (
                  <div className={styles.checkExplain}>
                    <b>Selisih ini sama dengan {explaining.length === 1 ? "penyebab berikut" : `${explaining.length} penyebab berikut`}:</b>
                    <CauseList causes={explaining} money={money} highlighted={highlighted} />
                  </div>
                )}
                {result.candidates.length > explaining.length && (
                  <section aria-labelledby="balance-check-causes">
                    <h3 id="balance-check-causes" className={styles.detailHeading}>
                      {explaining.length ? "Kemungkinan penyebab lain" : "Kemungkinan penyebab"}
                    </h3>
                    <CauseList causes={result.candidates.filter((cause) => !highlighted.has(cause.key))} money={money} highlighted={highlighted} />
                  </section>
                )}
                {result.candidates.length === 0 && (
                  <p className={styles.checkNote}>Tidak ada penyebab yang ditemukan. Periksa transaksi di aplikasi bank yang belum tercatat.</p>
                )}
                {result.unattributed.length > 0 && (
                  <section aria-labelledby="balance-check-unattributed">
                    <h3 id="balance-check-unattributed" className={styles.detailHeading}>Laporan yang rekeningnya belum diketahui</h3>
                    <CauseList causes={result.unattributed} money={money} highlighted={highlighted} />
                  </section>
                )}
                {matchMutation.isError && <p role="alert" className={styles.checkNote}>Saldo cocok belum bisa disimpan. Periksa lagi.</p>}
                <div className={styles.detailActions}>
                  {result.can_adjust && (
                    <button type="button" className="ws-btn ws-btn-accent" onClick={() => onAdjust(result.actual_balance, afterAdjust)}>
                      Sesuaikan saldo
                    </button>
                  )}
                  <button type="button" className="ws-btn" disabled={checkMutation.isPending} onClick={() => checkMutation.mutate(result.actual_balance)}>
                    Periksa lagi
                  </button>
                </div>
              </>
            )}
          </section>
        )}
      </div>
    </Modal>
  );
}
