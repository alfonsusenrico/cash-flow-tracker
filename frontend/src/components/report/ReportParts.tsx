import Link from "next/link";
import { cn } from "@/lib/utils";

export const LINK_CLASS =
  "rounded underline underline-offset-2 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--ink)]";

/** A side panel of the report (comparison, data quality, budget). */
export function ReportPanel({ id, title, children, className }: { id: string; title: string; children: React.ReactNode; className?: string }) {
  return (
    <section aria-labelledby={`${id}-title`} className={cn("ws-card space-y-3", className)}>
      <h2 id={`${id}-title`} className="text-base font-bold tracking-tight text-[var(--text)]">{title}</h2>
      {children}
    </section>
  );
}

/** A labelled amount row; the amount links to its ledger rows when they add up to it. */
export function AmountRow({
  label,
  amount,
  href,
  linkLabel,
  detail,
}: {
  label: React.ReactNode;
  amount: string;
  href?: string | null;
  linkLabel?: string;
  detail?: React.ReactNode;
}) {
  return (
    <li className="flex min-w-0 items-center justify-between gap-3 py-1">
      <span className="min-w-0 text-sm text-[var(--text)]">
        <span className="block truncate">{label}</span>
        {detail && <span className="block text-xs text-[var(--muted)]">{detail}</span>}
      </span>
      {href ? (
        <Link
          href={href}
          aria-label={linkLabel ? `${amount}, ${linkLabel}` : undefined}
          className={cn("inline-flex min-h-11 shrink-0 items-center text-sm font-bold tabular-nums text-[var(--text)]", LINK_CLASS)}
        >
          {amount}
        </Link>
      ) : (
        <span className="shrink-0 text-sm font-bold tabular-nums text-[var(--text)]">{amount}</span>
      )}
    </li>
  );
}

export function SubHeading({ children }: { children: React.ReactNode }) {
  return <h3 className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">{children}</h3>;
}
