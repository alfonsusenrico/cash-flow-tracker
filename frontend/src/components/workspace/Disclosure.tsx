"use client";

import { type ReactNode, useCallback, useId, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import { useEscapeLayer, useOutsidePress } from "./layers";

/** A button with a popover panel; Esc or a press outside closes it and Esc returns focus to the button. */
export function Disclosure({
  label,
  buttonClassName,
  buttonContent,
  panelLabel,
  panelClassName,
  anchorClassName,
  children,
}: {
  label: string;
  buttonClassName: string;
  buttonContent: ReactNode;
  panelLabel: string;
  /** Extra classes of the panel, e.g. opening upwards from the sidebar foot. */
  panelClassName?: string;
  anchorClassName?: string;
  children: (close: () => void) => ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const anchor = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const panelId = useId();
  const close = useCallback(() => setOpen(false), []);
  const closeAndReturn = useCallback(() => {
    setOpen(false);
    button.current?.focus();
  }, []);
  useEscapeLayer(open, closeAndReturn);
  useOutsidePress([anchor], open, close);
  return (
    <div className={cn("ws-pop-anchor", anchorClassName)} ref={anchor}>
      <button
        ref={button}
        type="button"
        className={buttonClassName}
        aria-label={label}
        aria-expanded={open}
        aria-controls={open ? panelId : undefined}
        onClick={() => setOpen((value) => !value)}
      >
        {buttonContent}
      </button>
      {open && (
        <div id={panelId} className={cn("ws-menu", panelClassName)} role="region" aria-label={panelLabel}>
          {children(close)}
        </div>
      )}
    </div>
  );
}

