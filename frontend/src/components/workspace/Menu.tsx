"use client";

import Link from "next/link";
import {
  type CSSProperties,
  type KeyboardEvent as ReactKeyboardEvent,
  type ReactNode,
  type RefObject,
  useCallback,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { Icon, type IconName } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";
import { PHONE_QUERY, useEscapeLayer, useFocusTrap, useMediaQuery, useOutsidePress } from "./layers";

// A popover keeps this distance from its button and from the viewport edges (prototype positionPopover).
const ANCHOR_GAP = 8;
const VIEWPORT_MARGIN = 16;

/**
 * Right-aligned under its anchor, moved left when it would leave the viewport and flipped above the anchor
 * when there is no room below (the account menu in the sidebar foot).
 */
export function popoverPosition(anchor: DOMRect, panel: { width: number; height: number }, viewport: { width: number; height: number }) {
  let left = anchor.right - panel.width;
  if (left < VIEWPORT_MARGIN) left = Math.min(anchor.left, viewport.width - panel.width - VIEWPORT_MARGIN);
  let top = anchor.bottom + ANCHOR_GAP;
  if (top + panel.height > viewport.height - VIEWPORT_MARGIN) {
    top = Math.max(VIEWPORT_MARGIN, anchor.top - panel.height - ANCHOR_GAP);
  }
  return { left: Math.max(0, left), top };
}

/**
 * A floating layer anchored to a button (D6): `--dialog`, 1 px line, radius 12, layer shadow. Below 768 px it is
 * a bottom sheet. Esc and a press outside close it; the owner decides where focus returns.
 */
export function Popover({
  open,
  anchorRef,
  onClose,
  width,
  role,
  label,
  className,
  panelRef: externalPanelRef,
  onKeyDown,
  trapFocus = false,
  id,
  children,
}: {
  id?: string;
  open: boolean;
  anchorRef: RefObject<HTMLElement | null>;
  /** `returnFocus` is true for Esc, false for a press outside (focus then stays where the press put it). */
  onClose: (returnFocus: boolean) => void;
  width: number;
  role: "menu" | "dialog";
  label: string;
  className?: string;
  panelRef?: RefObject<HTMLDivElement>;
  onKeyDown?: (event: ReactKeyboardEvent<HTMLDivElement>) => void;
  trapFocus?: boolean;
  children: ReactNode;
}) {
  const ownPanelRef = useRef<HTMLDivElement>(null);
  const panelRef = externalPanelRef ?? ownPanelRef;
  const sheet = useMediaQuery(PHONE_QUERY);
  const closeWithFocus = useCallback(() => onClose(true), [onClose]);
  const closeWithoutFocus = useCallback(() => onClose(false), [onClose]);

  useEscapeLayer(open, closeWithFocus);
  useOutsidePress([anchorRef, panelRef], open, closeWithoutFocus);
  useFocusTrap(panelRef, open && trapFocus);

  // Placed on the element itself before the first paint, so the panel is visible (and focusable) at once.
  useLayoutEffect(() => {
    if (!open || sheet) return;
    const place = () => {
      const anchor = anchorRef.current;
      const panel = panelRef.current;
      if (!anchor || !panel) return;
      const viewport = { width: window.innerWidth, height: window.innerHeight };
      const { left, top } = popoverPosition(anchor.getBoundingClientRect(), { width: panel.offsetWidth, height: panel.offsetHeight }, viewport);
      panel.style.left = `${left}px`;
      panel.style.top = `${top}px`;
    };
    place();
    window.addEventListener("resize", place);
    window.addEventListener("scroll", place, true);
    return () => {
      window.removeEventListener("resize", place);
      window.removeEventListener("scroll", place, true);
    };
  }, [open, sheet, anchorRef, panelRef]);

  if (!open) return null;
  const style = { "--pw": `${width}px` } as CSSProperties;
  return createPortal(
    <div
      ref={panelRef}
      id={id}
      role={role}
      aria-label={label}
      aria-modal={role === "dialog" && sheet ? true : undefined}
      tabIndex={-1}
      className={cn("popover", sheet && "is-sheet", className)}
      style={style}
      onKeyDown={onKeyDown}
    >
      {children}
    </div>,
    document.body,
  );
}

export interface MenuEntry {
  key: string;
  label: string;
  icon?: IconName;
  /** For a theme-style choice: rendered as `menuitemradio` with `aria-checked`. */
  checked?: boolean;
  href?: string;
  onSelect?: () => void;
}

const ITEM_SELECTOR = '[role="menuitem"], [role="menuitemradio"]';

/** Moves focus between menu items with ↑/↓, Home and End; Tab leaves and closes the menu (D6). */
function menuKeys(event: ReactKeyboardEvent<HTMLDivElement>, close: () => void, returnFocus: () => void) {
  const items = Array.from(event.currentTarget.querySelectorAll<HTMLElement>(ITEM_SELECTOR));
  if (!items.length) return;
  const index = items.indexOf(document.activeElement as HTMLElement);
  let next: number | null = null;
  if (event.key === "ArrowDown") next = index < 0 ? 0 : (index + 1) % items.length;
  else if (event.key === "ArrowUp") next = index < 0 ? items.length - 1 : (index - 1 + items.length) % items.length;
  else if (event.key === "Home") next = 0;
  else if (event.key === "End") next = items.length - 1;
  else if (event.key === "Tab") {
    // Focus goes back to the button first, so the browser's Tab moves on from there.
    returnFocus();
    close();
    return;
  }
  if (next === null) return;
  event.preventDefault();
  items[next].focus();
}

/** A `role="menu"` popover with `menuitem` or `menuitemradio` entries, opened from `anchorRef`. */
export function MenuPanel({
  open,
  anchorRef,
  onClose,
  label,
  heading,
  header,
  entries,
  width,
  id,
}: {
  open: boolean;
  anchorRef: RefObject<HTMLElement | null>;
  onClose: () => void;
  label: string;
  /** A small uppercase heading over the entries, such as TEMA. */
  heading?: string;
  /** Non-interactive content above the entries, such as the account's name. */
  header?: ReactNode;
  entries: MenuEntry[];
  width: number;
  id?: string;
}) {
  const panelRef = useRef<HTMLDivElement>(null);
  const focusAnchor = useCallback(() => anchorRef.current?.focus(), [anchorRef]);
  const close = useCallback(
    (returnFocus: boolean) => {
      onClose();
      if (returnFocus) focusAnchor();
    },
    [onClose, focusAnchor],
  );

  useLayoutEffect(() => {
    if (!open) return;
    const panel = panelRef.current;
    if (!panel) return;
    const target = panel.querySelector<HTMLElement>('[aria-checked="true"]') ?? panel.querySelector<HTMLElement>(ITEM_SELECTOR);
    target?.focus();
  }, [open]);

  const select = (entry: MenuEntry) => {
    entry.onSelect?.();
    close(true);
  };

  return (
    <Popover
      open={open}
      anchorRef={anchorRef}
      onClose={close}
      width={width}
      role="menu"
      label={label}
      panelRef={panelRef}
      id={id}
      onKeyDown={(event) => menuKeys(event, () => onClose(), focusAnchor)}
    >
      {header}
      {heading && (
        <span className="t-label popover-h" aria-hidden="true">
          {heading}
        </span>
      )}
      {entries.map((entry) => {
        const role = entry.checked === undefined ? "menuitem" : "menuitemradio";
        const content = (
          <>
            {entry.icon && <Icon name={entry.icon} className="h-4 w-4" />}
            <span className="grow">{entry.label}</span>
          </>
        );
        if (entry.href) {
          return (
            <Link key={entry.key} href={entry.href} role={role} tabIndex={-1} className="menu-item" onClick={() => select(entry)}>
              {content}
            </Link>
          );
        }
        return (
          <button
            key={entry.key}
            type="button"
            role={role}
            tabIndex={-1}
            aria-checked={entry.checked === undefined ? undefined : entry.checked}
            className="menu-item"
            onClick={() => select(entry)}
          >
            {content}
          </button>
        );
      })}
    </Popover>
  );
}

/** A menu button and its menu: `aria-haspopup="menu"`, `aria-expanded`, focus back on the button when it closes. */
export function MenuButton({
  label,
  className,
  children,
  menuLabel,
  heading,
  header,
  entries,
  width,
  lang,
}: {
  label: string;
  className: string;
  /** The button's own language when its name differs from the page around it (add-language-system D10). */
  lang?: string;
  children: ReactNode;
  menuLabel: string;
  heading?: string;
  header?: ReactNode;
  entries: MenuEntry[];
  width: number;
}) {
  const [open, setOpen] = useState(false);
  const button = useRef<HTMLButtonElement>(null);
  const menuId = useId();
  return (
    <>
      <button
        ref={button}
        type="button"
        className={className}
        aria-label={label}
        lang={lang}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? menuId : undefined}
        onClick={() => setOpen((value) => !value)}
        onKeyDown={(event) => {
          if (event.key === "ArrowDown" && !open) {
            event.preventDefault();
            setOpen(true);
          }
        }}
      >
        {children}
      </button>
      <MenuPanel
        open={open}
        anchorRef={button}
        onClose={() => setOpen(false)}
        label={menuLabel}
        heading={heading}
        header={header}
        entries={entries}
        width={width}
        id={menuId}
      />
    </>
  );
}
