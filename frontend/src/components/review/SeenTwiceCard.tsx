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

/**
 * A second app's report of money that is already recorded once, or a report the AI held as one
 * (recognise-one-arrival-seen-twice D3, D5): confirm it, or record it as its own transaction.
 */
export function SeenTwiceCard({ item, headingId, pending, onAcknowledge, onRecordSeparately }: SeenTwiceCardProps) {
  const { bal } = useAppCtx();
  const duplicate = item.kind === "duplicate";
  const kept = item.kept;
  const recordSub = item.account_name ? `Transaksi lain; catat di ${item.account_name}` : "Transaksi lain; catat sendiri";
  const keptPlace = kept?.movement
    ? `${kept.movement.source ?? "?"} → ${kept.movement.target ?? "?"}`
    : kept?.account_name ?? null;
  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={{ tone: duplicate ? "tr" : "warn", icon: "layers", text: duplicate ? "Terlihat dua kali" : "Mungkin terlihat dua kali" }}
        title={duplicate ? `${item.app} melaporkan uang yang sudah tercatat` : `${item.app} mungkin melaporkan uang yang sudah tercatat`}
        account={item.account_name}
        date={item.date}
        source="notification"
        amount={{ text: signed(item, bal), className: item.type === "income" ? "ws-amt-in" : "" }}
        reasons={duplicate ? [] : ["possible_duplicate_observation"]}
      />
      <dl className="ws-tri-note grid gap-1.5 tabular sm:grid-cols-[auto_1fr] sm:gap-x-3" aria-label="Sudah tercatat">
        <dt className="text-[var(--muted)]">{duplicate ? "Sudah tercatat sekali" : "Mungkin sama dengan"}</dt>
        <dd className="min-w-0 font-semibold">{keptPlace ?? "Belum ada transaksi yang cocok"}</dd>
        {kept && (
          <>
            <dt className="text-[var(--muted)]">Waktu tercatat</dt>
            <dd className="min-w-0">{longWhen(kept.date) ?? "Tanpa waktu"}</dd>
            <dt className="text-[var(--muted)]">Nominal tercatat</dt>
            <dd className="min-w-0">{bal(kept.amount)}</dd>
          </>
        )}
        <dt className="text-[var(--muted)]">Laporan ini</dt>
        <dd className="min-w-0">{duplicate ? "Tidak dicatat lagi" : "Belum dicatat"}</dd>
      </dl>
      {/* A second report is confirmed first; a held one may be the only report, so recording it comes first. */}
      <div role="group" aria-label="Keputusan" className="ws-choices">
        {duplicate ? (
          <>
            <Choice n={1} primary disabled={pending} label="Benar, satu transaksi" sub="Laporan ini tidak dicatat"
              onClick={() => void onAcknowledge(item)} />
            <Choice n={2} disabled={pending} label="Catat terpisah" sub={recordSub}
              onClick={() => void onRecordSeparately(item)} />
          </>
        ) : (
          <>
            <Choice n={1} primary disabled={pending} label="Catat transaksi ini"
              sub={item.account_name ? `Catat di ${item.account_name}` : "Catat sebagai transaksi"}
              onClick={() => void onRecordSeparately(item)} />
            <Choice n={2} disabled={pending}
              label={kept ? "Benar, satu transaksi" : "Abaikan laporan ini"}
              sub={kept ? "Laporan ini tidak dicatat, yang sudah tercatat tetap" : "Tidak dicatat"}
              onClick={() => void onAcknowledge(item)} />
          </>
        )}
      </div>
      {kept && (
        <div className="mt-4">
          <Link href={transaksiHref(kept.transaction_id, "dua")} className="ws-btn ws-btn-sm">
            Buka yang sudah tercatat
          </Link>
        </div>
      )}
    </>
  );
}
