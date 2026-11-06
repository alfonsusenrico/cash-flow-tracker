"use client";

import { type RefObject, useEffect, useLayoutEffect, useRef, useState } from "react";

/**
 * Esc closes the top-most open layer (popover, menu, explanation, sheet or drawer). Each open layer
 * registers here; the shortcut provider asks the stack to close the last one.
 */
const escapeStack: { close: () => void }[] = [];

export function useEscapeLayer(open: boolean, onClose: () => void) {
  const closeRef = useRef(onClose);
  useLayoutEffect(() => {
    closeRef.current = onClose;
  }, [onClose]);
  useEffect(() => {
    if (!open) return;
    const entry = { close: () => closeRef.current() };
    escapeStack.push(entry);
    return () => {
      const index = escapeStack.indexOf(entry);
      if (index >= 0) escapeStack.splice(index, 1);
    };
  }, [open]);
}

/** Closes the top-most layer; false when nothing was open. */
export function closeTopLayer(): boolean {
  const top = escapeStack[escapeStack.length - 1];
  if (!top) return false;
  top.close();
  return true;
}

/** Calls onOutside for a pointer press outside every given element while open. */
export function useOutsidePress(refs: RefObject<HTMLElement | null>[], open: boolean, onOutside: () => void) {
  const callback = useRef(onOutside);
  useLayoutEffect(() => {
    callback.current = onOutside;
  }, [onOutside]);
  useEffect(() => {
    if (!open) return;
    const handler = (event: PointerEvent | MouseEvent) => {
      const target = event.target as Node | null;
      if (target && refs.some((ref) => ref.current?.contains(target))) return;
      callback.current();
    };
    document.addEventListener("pointerdown", handler);
    return () => document.removeEventListener("pointerdown", handler);
  }, [open, refs]);
}

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function focusableIn(container: HTMLElement): HTMLElement[] {
  return Array.from(container.querySelectorAll<HTMLElement>(FOCUSABLE)).filter((element) => !element.hasAttribute("inert"));
}

/** Keeps Tab and Shift+Tab inside the container while active (modal sheets, dialogs, drawers). */
export function useFocusTrap(ref: RefObject<HTMLElement | null>, active: boolean) {
  useEffect(() => {
    if (!active) return;
    const handler = (event: KeyboardEvent) => {
      const container = ref.current;
      if (event.key !== "Tab" || !container) return;
      // A dialog opened from inside this layer is portalled after it; only the topmost owns Tab.
      const modals = document.querySelectorAll('[role="dialog"][aria-modal="true"]');
      const top = modals[modals.length - 1];
      if (top && top !== container && !container.contains(top)) return;
      const items = focusableIn(container);
      if (!items.length) {
        event.preventDefault();
        container.focus();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const current = document.activeElement;
      if (event.shiftKey && (current === first || !container.contains(current))) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && (current === last || !container.contains(current))) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [ref, active]);
}

/**
 * Stops the page scrolling behind a dialog or sheet; returns the release. The root element is locked, not
 * the body: with `overflow-x: clip` on the root, a hidden body becomes its own clipping box, so the page
 * still scrolls and the sticky sidebar loses its place.
 */
export function lockPageScroll(): () => void {
  const root = document.documentElement;
  const previous = root.style.overflow;
  root.style.overflow = "hidden";
  return () => {
    root.style.overflow = previous;
  };
}

/** Locks page scrolling behind a sheet or dialog while active. */
export function useScrollLock(active: boolean) {
  useEffect(() => {
    if (!active) return;
    return lockPageScroll();
  }, [active]);
}

/** Below 768 px the phone shell and the phone page layouts apply (apply-markas-shell D2). */
export const PHONE_QUERY = "(max-width: 767px)";

export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(false);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const list = window.matchMedia(query);
    setMatches(list.matches);
    if (typeof list.addEventListener !== "function") return;
    const listener = (event: MediaQueryListEvent) => setMatches(event.matches);
    list.addEventListener("change", listener);
    return () => list.removeEventListener("change", listener);
  }, [query]);
  return matches;
}

/**
 * Closes the native <dialog> around an element at once. A modal dialog makes the rest of the page inert, so a
 * layer that opens from inside it (Lainnya › Notifikasi) can only take focus once the dialog is closed.
 */
export function closeEnclosingDialog(element: Element) {
  syncDialog(element.closest("dialog"), false);
}

/** Opens a native <dialog> modally where supported; jsdom lacks showModal, so it falls back to `open`. */
export function syncDialog(dialog: HTMLDialogElement | null, open: boolean) {
  if (!dialog) return;
  const isOpen = dialog.hasAttribute("open");
  if (open && !isOpen) {
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
  } else if (!open && isOpen) {
    if (typeof dialog.close === "function") dialog.close();
    else dialog.removeAttribute("open");
  }
}
