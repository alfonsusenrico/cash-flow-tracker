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
import { useLocale, useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { UntranslatedNotice, useInterfaceLang, useUntranslated } from "./untranslated";
import { PHONE_QUERY, useEscapeLayer, useFocusTrap, useMediaQuery, useScrollLock } from "./layers";

/* ---------------------------------------------------------------------------------------------
 * Drawer: a detail layer that opens as a centered dialog from 768px and as a bottom sheet below 768px.
 * Both are modal (focus trapped, Esc and the scrim close, page scroll locked); focus returns to
 * whatever opened it. A dialog opened from inside it stays on top and owns the keyboard.
 * ------------------------------------------------------------------------------------------- */

export type DrawerMode = "dialog" | "sheet";

export function useDrawerMode(): DrawerMode {
  return useMediaQuery(PHONE_QUERY) ? "sheet" : "dialog";
}

export function Drawer({
  open,
  onClose,
  title,
  subtitle,
  untranslated: untranslatedContent,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  subtitle?: string;
  /** Content a later phase translates: Indonesian with `lang="id"` and the notice line in an English interface. */
  untranslated?: boolean;
  children: ReactNode;
}) {
  const t = useTranslations("common");
  const locale = useLocale();
  const untranslated = useUntranslated(untranslatedContent);
  const mode = useDrawerMode();
  const ref = useRef<HTMLDivElement>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const titleId = useId();

  useLayoutEffect(() => {
    if (!open) return;
    returnFocus.current = document.activeElement as HTMLElement | null;
    return () => {
      // A row deleted from the detail is gone; focus then goes to the main region, not to the page body.
      const target = returnFocus.current;
      if (target?.isConnected) target.focus();
      else document.getElementById("main")?.focus();
    };
  }, [open]);

  // The phone query is known only after the first render, so the layer can switch from dialog to sheet
  // while open; focus is placed again on the heading.
  useLayoutEffect(() => {
    if (open) headingRef.current?.focus();
  }, [open, mode]);

  useEscapeLayer(open, onClose);
  useFocusTrap(ref, open);
  useScrollLock(open);

  if (!open) return null;
  return createPortal(
    <>
      <div className="ws-sheet-scrim" aria-hidden="true" onClick={onClose} />
      <div ref={ref} role="dialog" aria-modal="true" aria-labelledby={titleId} className="ws-drawer" data-mode={mode}>
        {mode === "sheet" && <div className="ws-grab" aria-hidden="true" />}
        <div className="ws-drawer-h">
          {/* The title may be the user's own text (a row's name), so only the fixed subtitle is marked. */}
          <h2 id={titleId} ref={headingRef} tabIndex={-1}>
            {title}
            {subtitle && (
              <span className="block text-sm font-normal text-[var(--muted)]" lang={untranslated ? "id" : undefined}>
                {subtitle}
              </span>
            )}
            {untranslated && <UntranslatedNotice className="block text-sm font-normal text-[var(--muted)]" />}
          </h2>
          <button type="button" className="ws-close" aria-label={t("closeDetails")} lang={untranslated ? locale : undefined} onClick={onClose}>
            <Icon name="x" className="h-4 w-4" />
          </button>
        </div>
        {untranslated ? <div lang="id" className="contents">{children}</div> : children}
      </div>
    </>,
    document.body,
  );
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
  const lang = useInterfaceLang();

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
          {toast.actionLabel ?? <span lang={lang}>{t("undo")}</span>}
        </button>
      )}
    </div>
  );
}
