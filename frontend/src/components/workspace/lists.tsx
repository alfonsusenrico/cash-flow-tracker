"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { Icon, type IconName } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

/**
 * Section headers, the context panel, list rows and day headings of the Markas+ pages (option-6 SPEC §7,
 * apply-markas-beranda D6 to D8). Rows are whole-row links or buttons; the panel is the page's context column.
 */
export function SectionHeader({ id, label, aside }: { id?: string; label: string; aside?: ReactNode }) {
  return (
    <div className="sec-h">
      <h2 className="t-label" id={id}>
        {label}
      </h2>
      {aside}
    </div>
  );
}

export function Panel({ label, children }: { label: string; children: ReactNode }) {
  return (
    <aside className="panel" aria-label={label}>
      {children}
    </aside>
  );
}

/** A panel block; with `href` the whole block is one link, its heading followed by a chevron. */
export function PanelBlock({ heading, href, children }: { heading?: string; href?: string; children: ReactNode }) {
  if (href) {
    return (
      <Link href={href} className="panel-block">
        <div className="sec-h is-flush">
          <h2 className="t-label">{heading}</h2>
          <Icon name="chevron-right" className="chev h-4 w-4" />
        </div>
        {children}
      </Link>
    );
  }
  return <div className="panel-block">{children}</div>;
}

export interface ListRowProps {
  /** A link row; otherwise the row is a button. */
  href?: string;
  onClick?: () => void;
  time?: string;
  icon?: IconName;
  title: ReactNode;
  /** The meta line beside the title on a desktop and under it on the phone. */
  meta?: ReactNode;
  /** A different meta line for the phone's second line, such as "18.23 · Kebutuhan". */
  phoneMeta?: ReactNode;
  /** The category column of the full variant (Transaksi). */
  category?: ReactNode;
  /** The status column: a pillar dot and name, or a word such as "Tidak dihitung". */
  status?: ReactNode;
  amount?: ReactNode;
  chevron?: boolean;
  selected?: boolean;
}

/**
 * One list row (SPEC §7): time · icon · title with its meta · category · status · amount · chevron. Below 768 px
 * the row has two lines, the title and the meta, and only the amount stays beside them.
 */
export function ListRow({ href, onClick, time, icon, title, meta, phoneMeta, category, status, amount, chevron, selected }: ListRowProps) {
  const content = (
    <>
      {time !== undefined && <span className="r-time">{time}</span>}
      {icon && (
        <span className="r-ico">
          <Icon name={icon} className="h-4 w-4" />
        </span>
      )}
      <span className="r-main">
        <span className="r-title">{title}</span>
        {meta && <span className={cn("r-meta", phoneMeta !== undefined && "dt-only")}>{meta}</span>}
        {phoneMeta !== undefined && <span className="r-meta ph-only">{phoneMeta}</span>}
      </span>
      {category !== undefined && <span className="r-cat">{category}</span>}
      {status !== undefined && <span className="r-pillar">{status}</span>}
      {amount !== undefined && <span className="r-amt">{amount}</span>}
      {chevron && (
        <span className="chev">
          <Icon name="chevron-right" className="h-4 w-4" />
        </span>
      )}
    </>
  );
  const className = cn("row", "ws-select-row");
  const selection = selected ? { "data-selected": "" } : {};
  if (href) {
    return (
      <Link href={href} className={className} {...selection}>
        {content}
      </Link>
    );
  }
  return (
    <button type="button" className={className} onClick={onClick} {...selection}>
      {content}
    </button>
  );
}

/** A sticky day heading under the top bar, such as "KAMIS, 5 NOVEMBER · HARI INI", with optional totals. */
export function DayHeading({ label, totals }: { label: string; totals?: ReactNode }) {
  return (
    <div className="day-h">
      <h3 className="t-label">{label}</h3>
      {totals && <span className="tot">{totals}</span>}
    </div>
  );
}
