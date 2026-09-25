"use client";

import { useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { Icon } from "./Icon";

interface InfoHelpProps {
  label: string;
  children: React.ReactNode;
  id?: string;
}

export function InfoHelp({ label, children, id: providedId }: InfoHelpProps) {
  const generatedId = useId();
  const id = providedId ?? generatedId;
  const containerRef = useRef<HTMLSpanElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const noteRef = useRef<HTMLSpanElement>(null);
  const [pinned, setPinned] = useState(false);
  const [hovered, setHovered] = useState(false);
  const [above, setAbove] = useState(false);
  const [leftOffset, setLeftOffset] = useState(0);
  const open = pinned || hovered;

  useLayoutEffect(() => {
    if (!open || !buttonRef.current || !noteRef.current) return;
    const buttonRect = buttonRef.current.getBoundingClientRect();
    const noteWidth = noteRef.current.offsetWidth;
    const noteHeight = noteRef.current.offsetHeight;
    setAbove(window.innerHeight - buttonRect.bottom < noteHeight + 96);
    setLeftOffset(Math.min(0, window.innerWidth - buttonRect.left - noteWidth - 16));
  }, [open]);

  useEffect(() => {
    if (!pinned) return;

    const closeOutside = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setPinned(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setPinned(false);
      setHovered(false);
      buttonRef.current?.focus();
    };

    document.addEventListener("pointerdown", closeOutside);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOutside);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [pinned]);

  return (
    <span
      ref={containerRef}
      className="relative inline-flex align-middle"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <button
        ref={buttonRef}
        type="button"
        aria-label={`Info ${label}`}
        aria-expanded={open}
        aria-controls={id}
        onClick={() => setPinned((current) => !current)}
        className="inline-flex min-h-6 min-w-6 items-center justify-center rounded-md text-[var(--muted)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
      >
        <Icon name="info" aria-hidden="true" className="h-3.5 w-3.5" />
      </button>
      <span
        ref={noteRef}
        id={id}
        hidden={!open}
        role="note"
        style={{ left: leftOffset }}
        className={`absolute z-50 w-64 max-w-[min(16rem,calc(100vw-2rem))] rounded-xl border border-[var(--border)] bg-[var(--surface)] p-3 text-left text-xs font-normal leading-relaxed text-[var(--text)] shadow-lg ${above ? "bottom-full mb-2" : "top-full mt-2"}`}
      >
        {children}
      </span>
    </span>
  );
}
