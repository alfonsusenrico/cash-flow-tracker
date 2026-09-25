"use client";

import { useEffect, useId, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Icon } from "@/components/ui/Icon";

interface AccountOptionsProps {
  name: string;
  children: React.ReactNode;
  iconOnly?: boolean;
}

interface MenuPosition {
  left: number;
  top: number;
  maxHeight: number;
}

export function AccountOptions({ name, children, iconOnly = false }: AccountOptionsProps) {
  const menuId = useId();
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState<MenuPosition | null>(null);

  const close = (restoreFocus = false) => {
    setPosition(null);
    if (restoreFocus) triggerRef.current?.focus();
  };

  const toggle = () => {
    if (position) {
      close();
      return;
    }
    const rect = triggerRef.current?.getBoundingClientRect();
    if (!rect) return;
    const menuWidth = Math.min(240, window.innerWidth - 16);
    const below = window.innerHeight - rect.bottom - 16;
    const above = rect.top - 16;
    const useAbove = below < 280 && above > below;
    const maxHeight = Math.max(44, Math.min(400, useAbove ? above : below));
    setPosition({
      left: Math.max(8, Math.min(rect.right - menuWidth, window.innerWidth - menuWidth - 8)),
      top: useAbove ? Math.max(8, rect.top - maxHeight - 8) : rect.bottom + 8,
      maxHeight,
    });
  };

  useEffect(() => {
    if (!position) return;

    menuRef.current?.querySelector<HTMLButtonElement>("button:not(:disabled)")?.focus();
    const closeOutside = (event: PointerEvent) => {
      const target = event.target as Node;
      if (!menuRef.current?.contains(target) && !triggerRef.current?.contains(target)) close();
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      const activeElement = document.activeElement;
      if (!menuRef.current?.contains(activeElement) && activeElement !== triggerRef.current) return;
      event.preventDefault();
      close(true);
    };
    const closeOnScroll = (event: Event) => {
      if (!menuRef.current?.contains(event.target as Node)) close();
    };
    const closeOnResize = () => close();

    document.addEventListener("pointerdown", closeOutside);
    document.addEventListener("keydown", closeOnEscape);
    window.addEventListener("scroll", closeOnScroll, true);
    window.addEventListener("resize", closeOnResize);
    return () => {
      document.removeEventListener("pointerdown", closeOutside);
      document.removeEventListener("keydown", closeOnEscape);
      window.removeEventListener("scroll", closeOnScroll, true);
      window.removeEventListener("resize", closeOnResize);
    };
  }, [position]);

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        aria-label={`Opsi ${name}`}
        aria-expanded={Boolean(position)}
        aria-controls={position ? menuId : undefined}
        onClick={toggle}
        title={`Opsi ${name}`}
        className={`flex min-h-11 cursor-pointer items-center justify-center rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] text-sm font-semibold text-[var(--text)] hover:border-[var(--border-strong)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] ${iconOnly ? "min-w-11" : "px-4"}`}
      >
        {iconOnly ? <Icon name="more-horizontal" className="h-4 w-4" /> : "Opsi"}
      </button>
      {position && createPortal(
        <div
          ref={menuRef}
          id={menuId}
          role="group"
          aria-label={`Opsi ${name}`}
          style={{ left: position.left, top: position.top, maxHeight: position.maxHeight }}
          className="fixed z-[100] flex w-60 max-w-[calc(100vw-1rem)] flex-col gap-1 overflow-y-auto overscroll-contain rounded-xl border border-[var(--border-strong)] bg-[var(--surface)] p-2 text-[var(--text)] shadow-lg [&_button]:min-h-11 [&_button]:w-full [&_button]:rounded-lg [&_button]:px-3 [&_button]:text-left [&_button]:text-sm [&_button]:font-medium [&_button:hover]:bg-[var(--surface-raised)] [&_button:focus-visible]:outline-2 [&_button:focus-visible]:outline-offset-2 [&_button:focus-visible]:outline-[var(--primary)]"
        >
          {children}
        </div>,
        document.body,
      )}
    </>
  );
}
