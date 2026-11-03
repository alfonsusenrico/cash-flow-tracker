"use client";

import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { Icon } from "@/components/ui/Icon";
import { PHONE_QUERY, RAIL_QUERY, useEscapeLayer, useFocusTrap, useMediaQuery, useScrollLock } from "./layers";

/* ---------------------------------------------------------------------------------------------
 * Drawer: a side panel in the window's third column above 1180px, an overlay from 761 to 1180px
 * and a bottom sheet at 760px and below. Overlay and sheet are modal (focus trapped, Esc closes);
 * focus returns to whatever opened the drawer.
 * ------------------------------------------------------------------------------------------- */

interface DrawerSlotValue {
  slot: HTMLElement | null;
  setDrawerOpen: (open: boolean) => void;
}

export const DrawerSlotContext = createContext<DrawerSlotValue>({ slot: null, setDrawerOpen: () => {} });

export type DrawerMode = "side" | "overlay" | "sheet";

export function useDrawerMode(): DrawerMode {
  const phone = useMediaQuery(PHONE_QUERY);
  const rail = useMediaQuery(RAIL_QUERY);
  if (phone) return "sheet";
  if (rail) return "overlay";
  return "side";
}

export function Drawer({
  open,
  onClose,
  title,
  subtitle,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  subtitle?: string;
  children: ReactNode;
}) {
  const { slot, setDrawerOpen } = useContext(DrawerSlotContext);
  const mode = useDrawerMode();
  const ref = useRef<HTMLDivElement>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const titleId = useId();
  const modal = mode !== "side" || !slot;

  useLayoutEffect(() => {
    if (!open) return;
    returnFocus.current = document.activeElement as HTMLElement | null;
    return () => {
      // A row deleted from the drawer is gone; focus then goes to the main region, not to the page body.
      const target = returnFocus.current;
      if (target?.isConnected) target.focus();
      else document.getElementById("main")?.focus();
    };
  }, [open]);

  // The width is known only after the first render, so a drawer mounted open can move from the slot to
  // the body; the portal then remounts and focus has to be placed again.
  useLayoutEffect(() => {
    if (open) headingRef.current?.focus();
  }, [open, modal]);

  useEffect(() => {
    if (mode !== "side") return;
    setDrawerOpen(open);
    return () => setDrawerOpen(false);
  }, [open, mode, setDrawerOpen]);

  useEscapeLayer(open, onClose);
  useFocusTrap(ref, open && modal);
  useScrollLock(open && mode === "sheet");

  if (!open) return null;
  const body = (
    <>
      {modal && <div className="ws-sheet-scrim" aria-hidden="true" onClick={onClose} />}
      <div
        ref={ref}
        role="dialog"
        aria-modal={modal ? "true" : "false"}
        aria-labelledby={titleId}
        className="ws-drawer"
        data-mode={modal ? (mode === "side" ? "overlay" : mode) : "side"}
      >
        {mode === "sheet" && <div className="ws-grab" aria-hidden="true" />}
        <div className="ws-drawer-h">
          <h2 id={titleId} ref={headingRef} tabIndex={-1}>
            {title}
            {subtitle && <span className="block text-[13px] font-normal text-[var(--muted)]">{subtitle}</span>}
          </h2>
          <button type="button" className="ws-close" aria-label="Tutup detail" onClick={onClose}>
            <Icon name="x" className="h-4 w-4" />
          </button>
        </div>
        {children}
      </div>
    </>
  );
  return createPortal(body, modal ? document.body : slot!);
}

/* ---------------------------------------------------------------------------------------------
 * Toast: one polite status message with an optional "Urungkan". It stays 6.5 s and pauses while the
 * pointer or focus is on it, so the undo can always be reached.
 * ------------------------------------------------------------------------------------------- */

export const TOAST_MS = 6500;

interface ToastMessage {
  id: number;
  text: string;
  onUndo?: () => void;
}

interface ToastContextValue {
  showToast: (text: string, options?: { onUndo?: () => void }) => void;
}

const ToastContext = createContext<ToastContextValue>({ showToast: () => {} });

export function useToast() {
  return useContext(ToastContext);
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toast, setToast] = useState<ToastMessage | null>(null);
  const counter = useRef(0);

  const showToast = useCallback((text: string, options?: { onUndo?: () => void }) => {
    counter.current += 1;
    setToast({ id: counter.current, text, onUndo: options?.onUndo });
  }, []);

  const value = useMemo(() => ({ showToast }), [showToast]);
  return (
    <ToastContext.Provider value={value}>
      {children}
      <div role="status" aria-live="polite" className="contents">
        {toast && <Toast key={toast.id} toast={toast} onDone={() => setToast(null)} />}
      </div>
    </ToastContext.Provider>
  );
}

function Toast({ toast, onDone }: { toast: ToastMessage; onDone: () => void }) {
  const [paused, setPaused] = useState(false);
  const remaining = useRef(TOAST_MS);
  const startedAt = useRef(0);

  useEffect(() => {
    if (paused) return;
    startedAt.current = Date.now();
    const timer = window.setTimeout(onDone, remaining.current);
    return () => {
      window.clearTimeout(timer);
      remaining.current -= Date.now() - startedAt.current;
    };
  }, [paused, onDone]);

  return (
    <div
      className="ws-toast"
      onMouseEnter={() => setPaused(true)}
      onMouseLeave={() => setPaused(false)}
      onFocus={() => setPaused(true)}
      onBlur={() => setPaused(false)}
    >
      <span>{toast.text}</span>
      {toast.onUndo && (
        <button
          type="button"
          onClick={() => {
            toast.onUndo?.();
            onDone();
          }}
        >
          Urungkan
        </button>
      )}
    </div>
  );
}
