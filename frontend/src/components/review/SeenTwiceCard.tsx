"use client";

import Link from "next/link";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Choice } from "@/components/review/triage/Choice";
import { ItemHead, longWhen } from "@/components/review/triage/ItemHead";
import type { SeenTwiceItem } from "@/lib/review";
import { transaksiHref } from "@/lib/triage";

interface SeenTwiceCardProps {
  item: SeenTwiceItem;
  headingId: string;
  pending: boolean;
  onAcknowledge: (item: SeenTwiceItem) => Promise<boolean>;
  onRecordSeparately: (item: SeenTwiceItem) => Promise<boolean>;
}

function signed(item: SeenTwiceItem, bal: (amount: number) => string): string {
  if (item.amount === null) return "Nominal tidak terbaca";
  return `${item.type === "income" ? "+" : item.type === "expense" ? "−" : ""}${bal(item.amount)}`;
}

function linkLine(item: SeenTwiceItem): string | null {
  const { link } = item;
  if (!link) return null;
  return `${link.account_label ?? "Rekening"} juga dilaporkan ${link.institution_label}`;
}

/**
 * A second app's report of money that is already recorded once (recognise-one-arrival-seen-twice D3):
 * confirm it, which links the account to that app, or record it as its own transaction. A report an
 * owner's account link skipped is already settled and only offers "Catat terpisah"
 * (reconcile-linked-account-reports D5, D6).
 */
export function SeenTwiceCard({ item, headingId, pending, onAcknowledge, onRecordSeparately }: SeenTwiceCardProps) {
  const { bal } = useAppCtx();
  const skipped = item.kind === "skipped";
  const kept = item.kept;
  const recordSub = item.account_name ? `Transaksi lain; catat di ${item.account_name}` : "Transaksi lain; catat sendiri";
  const keptPlace = kept?.movement
    ? `${kept.movement.source ?? "?"} → ${kept.movement.target ?? "?"}`
    : kept?.account_name ?? null;
  const link = linkLine(item);
  const sameAccount = !skipped && kept?.account_name ? `${kept.account_name} juga terlihat di ${item.app}` : null;
  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={{ tone: "tr", icon: "layers", text: skipped ? "Dilewati otomatis" : "Terlihat dua kali" }}
        title={`${item.app} melaporkan uang yang sudah tercatat`}
        account={item.account_name}
        date={item.date}
        source="notification"
        amount={{ text: signed(item, bal), className: item.type === "income" ? "ws-amt-in" : "" }}
        reasons={[]}
      />
      <dl className="ws-tri-note grid gap-1.5 tabular sm:grid-cols-[auto_1fr] sm:gap-x-3" aria-label="Sudah tercatat">
        <dt className="text-[var(--muted)]">Sudah tercatat sekali</dt>
        <dd className="min-w-0 font-semibold">{keptPlace ?? "Transaksi yang tercatat sudah dihapus"}</dd>
        {kept && (
          <>
            <dt className="text-[var(--muted)]">Waktu tercatat</dt>
            <dd className="min-w-0">{longWhen(kept.date) ?? "Tanpa waktu"}</dd>
            <dt className="text-[var(--muted)]">Nominal tercatat</dt>
            <dd className="min-w-0">{bal(kept.amount)}</dd>
          </>
        )}
        <dt className="text-[var(--muted)]">Laporan ini</dt>
        <dd className="min-w-0">{skipped ? "Dilewati, tidak dicatat" : "Tidak dicatat lagi"}</dd>
        {sameAccount && (
          <>
            <dt className="text-[var(--muted)]">Rekening yang sama</dt>
            <dd className="min-w-0">{sameAccount}</dd>
          </>
        )}
        {link && (
          <>
            <dt className="text-[var(--muted)]">Rekening terhubung</dt>
            <dd className="min-w-0">{link}</dd>
          </>
        )}
      </dl>
      <div role="group" aria-label="Keputusan" className="ws-choices">
        {skipped ? (
          <Choice n={1} disabled={pending} label="Catat terpisah" sub={recordSub}
            onClick={() => void onRecordSeparately(item)} />
        ) : (
          <>
            <Choice n={1} primary disabled={pending} label="Benar, satu transaksi" sub="Rekening ditautkan; laporan serupa dilewati otomatis"
              onClick={() => void onAcknowledge(item)} />
            <Choice n={2} disabled={pending} label="Catat terpisah" sub={recordSub}
              onClick={() => void onRecordSeparately(item)} />
          </>
        )}
      </div>
      {kept && (
        <div className="mt-4">
          <Link href={transaksiHref(kept.transaction_id, skipped ? "lewat" : "dua")} className="ws-btn ws-btn-sm">
            Buka yang sudah tercatat
          </Link>
        </div>
      )}
    </>
  );
}
