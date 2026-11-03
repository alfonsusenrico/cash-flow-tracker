"use client";

import { Icon, type IconName } from "@/components/ui/Icon";
import { Pill, type Tone } from "@/components/workspace/primitives";
import { reviewReasonLabel } from "@/lib/review";

const WHEN = new Intl.DateTimeFormat("id-ID", {
  weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta",
});

export function longWhen(iso: string | null | undefined): string | null {
  return iso ? WHEN.format(new Date(iso)) : null;
}

const PROVENANCE: Record<string, { icon: IconName; text: string }> = {
  notification: { icon: "smartphone", text: "Ditangkap dari HP" },
  capture: { icon: "edit", text: "Diketik di Catat" },
  manual: { icon: "edit", text: "Dicatat manual" },
};

/** Kind, title (the item's focus target), account, local time, provenance, amount and reasons (D4). */
export function ItemHead({
  headingId,
  kind,
  title,
  account,
  date,
  source,
  amount,
  reasons = [],
}: {
  headingId: string;
  kind: { tone: Tone; icon: IconName; text: string };
  title: string;
  account?: string | null;
  date?: string | null;
  source?: string | null;
  amount: { text: string; className?: string };
  reasons?: string[];
}) {
  const provenance = source ? PROVENANCE[source] : undefined;
  const when = longWhen(date);
  return (
    <>
      <Pill tone={kind.tone} icon={kind.icon}>
        {kind.text}
      </Pill>
      <h2 id={headingId} className="ws-tri-title" tabIndex={-1} data-item-heading="">
        {title}
      </h2>
      <p className="ws-tri-meta">
        {account && <span>{account}</span>}
        {when && <span className="tabular">{when}</span>}
        {provenance && (
          <span>
            <Icon name={provenance.icon} />
            {provenance.text}
          </span>
        )}
      </p>
      <p className={`ws-tri-amount tabular ${amount.className ?? ""}`}>{amount.text}</p>
      {reasons.length > 0 && (
        <ul className="ws-tri-reasons" aria-label="Alasan">
          {reasons.map((reason) => (
            <li key={reason}>
              <Pill tone="warn">{reviewReasonLabel(reason)}</Pill>
            </li>
          ))}
        </ul>
      )}
    </>
  );
}
