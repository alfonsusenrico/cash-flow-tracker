"use client";

import { createContext, type ReactNode, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useTranslations } from "next-intl";

/* ---------------------------------------------------------------------------------------------
 * Toast: one polite status message with an optional "Urungkan". It stays 6.5 s and pauses while the
 * pointer or focus is on it, so the undo can always be reached.
 * ------------------------------------------------------------------------------------------- */

export const TOAST_MS = 6500;

interface ToastMessage {
  id: number;
  text: string;
  onUndo?: () => void;
  /** The button's label when it reverses something other than the last save; "Urungkan" otherwise. */
  actionLabel?: string;
}

interface ToastContextValue {
  showToast: (text: string, options?: { onUndo?: () => void; actionLabel?: string }) => void;
}

const ToastContext = createContext<ToastContextValue>({ showToast: () => {} });

export function useToast() {
  return useContext(ToastContext);
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toast, setToast] = useState<ToastMessage | null>(null);
  const counter = useRef(0);

  const showToast = useCallback((text: string, options?: { onUndo?: () => void; actionLabel?: string }) => {
    counter.current += 1;
    setToast({ id: counter.current, text, onUndo: options?.onUndo, actionLabel: options?.actionLabel });
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
  const t = useTranslations("common");

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
          {toast.actionLabel ?? t("undo")}
        </button>
      )}
    </div>
  );
}
