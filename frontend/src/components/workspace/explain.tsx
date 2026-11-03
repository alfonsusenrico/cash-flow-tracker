"use client";

import Link from "next/link";
import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { Icon } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";
import { PHONE_QUERY, useEscapeLayer, useFocusTrap, useOutsidePress, useScrollLock } from "./layers";

/**
 * "Rincian hitungan": any figure marked explainable opens the lines that make it up. The content comes from
 * the server's lines; this module only lays them out (explainable-figures spec).
 */
export interface ExplanationLine {
  label: string;
  note?: string | null;
  /** Already formatted, with its sign. */
  amount: string;
  /** Rows behind this line, such as the ledger filtered by period and pillar. */
  href?: string | null;
}

export interface ExplanationData {
  title: string;
  rule: string;
  lines: ExplanationLine[];
  total?: { label: string; amount: string } | null;
  notes?: string[];
  link?: { href: string; label: string } | null;
  /** A figure that cannot be computed: why, and what would make it available. No total is shown. */
  unavailable?: { reason: string; fix: string } | null;
}

interface OpenState {
  id: string;
  trigger: HTMLElement;
  data: ExplanationData;
}

interface ExplainContextValue {
  openId: string | null;
  open: (id: string, trigger: HTMLElement, data: ExplanationData) => void;
  close: () => void;
}

const ExplainContext = createContext<ExplainContextValue | null>(null);

export function ExplainProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<OpenState | null>(null);
  const stateRef = useRef<OpenState | null>(null);
  useLayoutEffect(() => {
    stateRef.current = state;
  }, [state]);

  const close = useCallback(() => {
    const trigger = stateRef.current?.trigger;
    if (trigger?.isConnected) trigger.focus();
    setState(null);
  }, []);

  const open = useCallback((id: string, trigger: HTMLElement, data: ExplanationData) => {
    setState({ id, trigger, data });
  }, []);

  const value = useMemo(() => ({ openId: state?.id ?? null, open, close }), [state?.id, open, close]);

  return (
    <ExplainContext.Provider value={value}>
      {children}
      {state && <ExplanationPanel key={state.id} state={state} onClose={close} />}
    </ExplainContext.Provider>
  );
}

function useExplain(): ExplainContextValue {
  const context = useContext(ExplainContext);
  if (!context) throw new Error("ExplainableFigure needs an ExplainProvider");
  return context;
}

/**
 * The figure as a button: visible text stays the figure, a dotted underline marks it, and the accessible
 * name reads "<label> <amount>: lihat rincian".
 */
export function ExplainableFigure({
  label,
  amountText,
  explanation,
  children,
  className,
}: {
  label: string;
  /** The figure as text for the accessible name, such as "Rp 214.000" or "belum tersedia". */
  amountText: string;
  explanation: ExplanationData;
  children?: ReactNode;
  className?: string;
}) {
  const id = useId();
  const { openId, open, close } = useExplain();
  const expanded = openId === id;
  return (
    <button
      type="button"
      className={cn("ws-xn", className)}
      aria-haspopup="dialog"
      aria-expanded={expanded}
      aria-label={`${label} ${amountText}: lihat rincian`}
      data-explain-trigger={id}
      onClick={(event) => (expanded ? close() : open(id, event.currentTarget, explanation))}
    >
      {children ?? amountText}
    </button>
  );
}

function ExplanationPanel({ state, onClose }: { state: OpenState; onClose: () => void }) {
  const panelRef = useRef<HTMLDivElement>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const triggerRef = useRef<HTMLElement | null>(state.trigger);
  const titleId = useId();
  const [sheet] = useState(() => typeof window.matchMedia === "function" && window.matchMedia(PHONE_QUERY).matches);
  const [position, setPosition] = useState<{ left: number; top: number; maxHeight?: number } | null>(null);
  const { data } = state;

  useEscapeLayer(true, onClose);
  useOutsidePress([panelRef, triggerRef], true, onClose);
  useScrollLock(sheet);
  useFocusTrap(panelRef, sheet);

  useLayoutEffect(() => {
    headingRef.current?.focus();
    if (sheet || !panelRef.current) return;
    const margin = 16;
    const rect = state.trigger.getBoundingClientRect();
    const width = panelRef.current.offsetWidth;
    const height = panelRef.current.offsetHeight;
    const viewportWidth = document.documentElement.clientWidth || window.innerWidth;
    let left = rect.left + window.scrollX;
    left = Math.min(left, window.scrollX + viewportWidth - width - margin);
    left = Math.max(window.scrollX + margin, left);
    // Below the figure when it fits, else above; a tall explanation scrolls inside the larger side.
    // It never covers the figure itself.
    const below = window.innerHeight - rect.bottom - 8 - margin;
    const above = rect.top - 8 - margin;
    if (height <= below || (height > above && below >= above)) {
      setPosition({ left, top: rect.bottom + window.scrollY + 8, maxHeight: height <= below ? undefined : below });
    } else {
      const shown = Math.min(height, above);
      setPosition({ left, top: rect.top + window.scrollY - shown - 8, maxHeight: height <= above ? undefined : above });
    }
  }, [sheet, state.trigger]);

  const panel = (
    <>
      {sheet && <div className="ws-sheet-scrim" aria-hidden="true" />}
      <div
        ref={panelRef}
        role="dialog"
        aria-modal={sheet ? "true" : "false"}
        aria-labelledby={titleId}
        className="ws-explain"
        data-mode={sheet ? "sheet" : "popover"}
        style={
          sheet
            ? undefined
            : {
                left: position?.left ?? 0,
                top: position?.top ?? 0,
                maxHeight: position?.maxHeight,
                overflowY: position?.maxHeight ? "auto" : undefined,
                // Measured before it is shown; opacity (not visibility) keeps the heading focusable meanwhile.
                opacity: position ? 1 : 0,
              }
        }
      >
        <div className="ws-x-head">
          <span className="ws-x-kick">Rincian hitungan</span>
          <h2 id={titleId} ref={headingRef} tabIndex={-1}>
            {data.title}
          </h2>
          <button type="button" className="ws-close" aria-label="Tutup penjelasan" onClick={onClose}>
            <Icon name="x" className="h-4 w-4" />
          </button>
        </div>
        <ExplanationBody data={data} />
      </div>
    </>
  );
  return createPortal(panel, document.body);
}

export function ExplanationBody({ data }: { data: ExplanationData }) {
  return (
    <div className="ws-x-body">
      <p>{data.rule}</p>
      {data.unavailable ? (
        <>
          <p>
            <strong className="font-medium text-[var(--text)]">Kenapa belum ada: </strong>
            {data.unavailable.reason}
          </p>
          <p>{data.unavailable.fix}</p>
          {data.lines.length > 0 && <LinesTable lines={data.lines} />}
        </>
      ) : (
        <LinesTable lines={data.lines} total={data.total ?? null} />
      )}
      {(data.notes ?? []).map((note) => (
        <p key={note}>{note}</p>
      ))}
      {data.link && (
        <Link href={data.link.href} className="ws-x-link">
          {data.link.label}
          <Icon name="arrow-right" className="h-3.5 w-3.5" />
        </Link>
      )}
    </div>
  );
}

function LinesTable({ lines, total }: { lines: ExplanationLine[]; total?: { label: string; amount: string } | null }) {
  if (!lines.length && !total) return null;
  return (
    <table className="ws-xtable">
      <tbody>
        {lines.map((line, index) => (
          <tr key={`${line.label}-${index}`}>
            <td>
              {line.href ? (
                <Link href={line.href} className="underline decoration-[var(--line-strong)] underline-offset-[3px]">
                  {line.label}
                </Link>
              ) : (
                line.label
              )}
              {line.note && <small>{line.note}</small>}
            </td>
            <td className="r">{line.amount}</td>
          </tr>
        ))}
        {total && (
          <tr className="is-total">
            <td>{total.label}</td>
            <td className="r">{total.amount}</td>
          </tr>
        )}
      </tbody>
    </table>
  );
}
