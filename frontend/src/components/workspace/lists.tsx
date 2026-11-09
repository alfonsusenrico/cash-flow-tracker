"use client";

import Link from "next/link";
import type { DOMAttributes, ReactNode } from "react";
import { Icon, type IconName } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

/**
 * Section headers, the context panel, list rows and day headings of the Markas+ pages (option-6 SPEC §7,
 * apply-markas-beranda D6 to D8). Rows are whole-row links or buttons; the panel is the page's context column.
 */
export function SectionHeader({ id, label, aside, level = 2 }: { id?: string; label: string; aside?: ReactNode; level?: 2 | 3 }) {
  // A section inside a level-2 part, such as one of Laporan's questions, takes level 3 (apply-markas-laporan D4).
  const Heading = level === 3 ? "h3" : "h2";
  return (
    <div className="sec-h">
      <Heading className="t-label" id={id}>
        {label}
      </Heading>
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
  /** A link row; with `onClick` a button; with neither a plain row that is not a target (SPEC `is-plain`). */
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
  /** The status column: a pillar dot and name, or a word such as a transfer's saving direction ("Ditabung"). */
  status?: ReactNode;
  amount?: ReactNode;
  chevron?: boolean;
  selected?: boolean;
  /** The item shown beside the list (Tinjau): `aria-current="true"` with the selection bar. */
  current?: boolean;
  /** Title over meta at every width, padding 12 and 64 px high (apply-markas-tinjau D6). */
  layout?: "stacked";
  /** `data-row`, which focus returns to when the item's sheet or dialog closes. */
  rowKey?: string;
  /** Controls at the end of a plain row, such as Tinjau's "Terima" and "Tolak" for one suggestion. */
  actions?: ReactNode;
  /**
   * Selection for a merge (apply-markas-transaksi D9): the row becomes one `label` with a 16 px checkbox before the
   * time, so the whole row toggles it; a disabled row is dimmed and its checkbox disabled.
   */
  selectable?: { checked: boolean; disabled: boolean; onToggle: () => void; label: string };
  /** Pointer handlers of the row element, such as the phone's long-press that starts a merge. */
  pressHandlers?: Pick<DOMAttributes<HTMLElement>, "onPointerDown" | "onPointerMove" | "onPointerUp" | "onPointerCancel" | "onContextMenu">;
}

/**
 * One list row (SPEC §7): time · icon · title with its meta · category · status · amount · chevron. Below 768 px
 * the row has two lines, the title and the meta, and only the amount stays beside them.
 */
export function ListRow({
  href, onClick, time, icon, title, meta, phoneMeta, category, status, amount, chevron, selected, current, layout, rowKey, actions,
  selectable, pressHandlers,
}: ListRowProps) {
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
  if (selectable) {
    return (
      <label
        className={cn("row", "ws-select-row", "is-selectable", layout === "stacked" && "is-stacked")}
        data-row={rowKey}
        data-selected={selectable.checked ? "" : undefined}
        aria-disabled={selectable.disabled || undefined}
        {...pressHandlers}
      >
        <input
          type="checkbox"
          className="r-check"
          checked={selectable.checked}
          disabled={selectable.disabled}
          onChange={selectable.onToggle}
          aria-label={selectable.label}
        />
        {content}
      </label>
    );
  }
  if (!href && !onClick) {
    return (
      <div className={cn("row", "is-plain", layout === "stacked" && "is-stacked")}>
        {content}
        {actions && <span className="r-actions">{actions}</span>}
      </div>
    );
  }
  const className = cn("row", "ws-select-row", layout === "stacked" && "is-stacked");
  const selection = {
    ...(selected ? { "data-selected": "" } : {}),
    ...(current ? { "aria-current": "true" as const } : {}),
    ...(rowKey ? { "data-row": rowKey } : {}),
  };
  if (href) {
    return (
      <Link href={href} className={className} {...selection}>
        {content}
      </Link>
    );
  }
  return (
    <button type="button" className={className} onClick={onClick} {...selection} {...pressHandlers}>
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
