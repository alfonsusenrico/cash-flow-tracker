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
      <dl className="ws-facts tabular" aria-label="Sudah tercatat">
        <dt>{duplicate ? "Sudah tercatat sekali" : "Mungkin sama dengan"}</dt>
        <dd>{keptPlace ?? "Belum ada transaksi yang cocok"}</dd>
        {kept && (
          <>
            <dt>Waktu tercatat</dt>
            <dd>{longWhen(kept.date) ?? "Tanpa waktu"}</dd>
            <dt>Nominal tercatat</dt>
            <dd>{bal(kept.amount)}</dd>
          </>
        )}
        <dt>Laporan ini</dt>
        <dd>{duplicate ? "Tidak dicatat lagi" : "Belum dicatat"}</dd>
      </dl>
      <div role="group" aria-label="Keputusan" className="ws-choices">
        <Choice
          n={1}
          primary
          disabled={pending}
          label="Benar, satu transaksi"
          sub={duplicate ? "Laporan ini tidak dicatat" : "Laporan ini tidak dicatat, yang sudah tercatat tetap"}
          onClick={() => void onAcknowledge(item)}
        />
        <Choice
          n={2}
          disabled={pending}
          label="Catat terpisah"
          sub={item.account_name ? `Transaksi lain; catat di ${item.account_name}` : "Transaksi lain; catat sendiri"}
          onClick={() => void onRecordSeparately(item)}
        />
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
