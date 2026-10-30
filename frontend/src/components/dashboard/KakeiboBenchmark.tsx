"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import {
  KAKEIBO_PILLARS,
  type KakeiboBreakdown,
  type KakeiboPillar,
  kakeiboGroups,
  pillarPct,
  pillarSpent,
} from "@/lib/kakeibo";

interface KakeiboBenchmarkProps {
  kakeibo?: KakeiboBreakdown;
  pillarHref?: (pillar: KakeiboPillar) => string | null;
}

function formatPct(value: number) {
  return `${value.toLocaleString("id-ID", { maximumFractionDigits: 1 })}%`;
}

/** Five classifications shown individually; the 50/30/20 grouping is an optional, explained reference. */
export function KakeiboBenchmark({ kakeibo, pillarHref }: KakeiboBenchmarkProps) {
  const { bal } = useAppCtx();
  const allocated = kakeibo?.total_allocated ?? KAKEIBO_PILLARS.reduce((sum, pillar) => sum + pillarSpent(kakeibo, pillar.value), 0);
  const empty = !kakeibo || allocated <= 0;

  return (
    <section aria-labelledby="kakeibo-title" className="cockpit-card space-y-5 border border-[var(--border)] p-5 shadow-xs sm:p-6">
      <div>
        <h2 id="kakeibo-title" className="text-base font-bold tracking-tight text-[var(--text)]">Klasifikasi Kakeibo</h2>
        <p className="mt-1 text-xs text-[var(--muted)]">
          Jumlah tercatat pada periode yang dipilih, setelah penggantian uang. Persentase dihitung dari pemasukan atau total
          alokasi, mana yang lebih besar.
        </p>
      </div>

      {!kakeibo ? (
        <p className="text-xs text-[var(--muted)]">Data belum tersedia.</p>
      ) : empty ? (
        <p className="text-xs text-[var(--muted)]">Belum ada pengeluaran atau tabungan tercatat pada periode ini.</p>
      ) : (
        <ul className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-5">
          {KAKEIBO_PILLARS.map((pillar) => {
            const amount = bal(pillarSpent(kakeibo, pillar.value));
            const share = formatPct(pillarPct(kakeibo, pillar.value));
            // Saving also contains transfers, so a ledger filter on the pillar would show only part of it.
            const href = pillar.value === "saving" ? null : pillarHref?.(pillar.value);
            return (
              <li key={pillar.value} className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/40 p-3">
                <h3 className="flex items-center gap-1.5 text-xs font-semibold text-[var(--muted)]">
                  <span aria-hidden="true" className="h-2 w-2 rounded-full" style={{ backgroundColor: pillar.color }} />
                  {pillar.label}
                </h3>
                <div className="mt-2 text-base font-bold tabular-nums text-[var(--text)]">
                  {href ? (
                    <Link
                      href={href}
                      aria-label={`${pillar.label} ${amount}, lihat transaksinya`}
                      className="underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
                    >
                      {amount}
                    </Link>
                  ) : (
                    amount
                  )}
                </div>
                <div className="text-[11px] tabular-nums text-[var(--muted)]">{share}</div>
              </li>
            );
          })}
        </ul>
      )}

      {kakeibo && !empty && kakeibo.saving_expenses !== undefined && kakeibo.net_saving_transfers !== undefined && (
        <p className="text-xs text-[var(--text-secondary)]">
          Tabungan = pengeluaran berklasifikasi tabungan{" "}
          <span className="tabular-nums">{bal(kakeibo.saving_expenses)}</span> + perpindahan bersih ke rekening tabungan
          atau investasi <span className="tabular-nums">{bal(kakeibo.net_saving_transfers)}</span>, minimal nol. Pindah
          saldo biasa dan jual-beli saham tidak dihitung sebagai tabungan baru.
        </p>
      )}
      <p className="text-xs text-[var(--text-secondary)]">
        Selisih uang masuk dan keluar adalah surplus arus kas, bukan bukti uang sudah disisihkan.
      </p>

      <details className="text-xs text-[var(--text-secondary)]">
        <summary className="flex min-h-11 cursor-pointer items-center font-semibold">Acuan 50/30/20 (opsional)</summary>
        <div className="space-y-2">
          <p>
            Hanya pembanding, bukan penilaian kesehatan keuangan. Kebutuhan dan Tak Terduga dikelompokkan sebagai pokok,
            Keinginan dan Budaya sebagai gaya hidup, Tabungan tetap sendiri.
          </p>
          {kakeibo && !empty && (
            <ul className="space-y-1 tabular-nums">
              {kakeiboGroups(kakeibo).map((group) => (
                <li key={group.key}>
                  {group.label}: {formatPct(group.pct)} (acuan {group.targetPct}%)
                </li>
              ))}
            </ul>
          )}
        </div>
      </details>
    </section>
  );
}
