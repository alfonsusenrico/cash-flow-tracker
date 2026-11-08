"use client";

import { forwardRef, useEffect, useRef } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { LEDGER_SEARCH_ID } from "@/components/workspace/TopBar";

export interface ActiveChip {
  key: string;
  label: string;
  remove: () => void;
}

/**
 * The search field, "Filter" with its count and the active filters as removable chips (apply-markas-transaksi D7).
 * Removing a chip moves focus to the next chip, or to the search field when none is left.
 */
export const LedgerToolbar = forwardRef<HTMLButtonElement, {
  search: string;
  onSearch: (value: string) => void;
  filterCount: number;
  onFilter: () => void;
  chips: ActiveChip[];
}>(function LedgerToolbar({ search, onSearch, filterCount, onFilter, chips }, filterRef) {
  const t = useTranslations("transactions");
  const chipsRef = useRef<HTMLDivElement>(null);
  const pendingFocus = useRef<number | null>(null);

  useEffect(() => {
    const index = pendingFocus.current;
    if (index === null) return;
    pendingFocus.current = null;
    const buttons = chipsRef.current?.querySelectorAll<HTMLButtonElement>("button") ?? [];
    const next = buttons[Math.min(index, buttons.length - 1)];
    (next ?? document.getElementById(LEDGER_SEARCH_ID))?.focus();
  }, [chips]);

  return (
    <>
      <div className="toolbar">
        <form className="search" role="search" onSubmit={(event) => event.preventDefault()}>
          <span className="ico" aria-hidden="true">
            <Icon name="search" className="h-4 w-4" />
          </span>
          <input
            id={LEDGER_SEARCH_ID}
            type="search"
            value={search}
            onChange={(event) => onSearch(event.target.value)}
            placeholder={t("toolbar.search")}
            aria-label={t("toolbar.search")}
            autoComplete="off"
          />
        </form>
        <button
          ref={filterRef}
          type="button"
          className="ws-btn"
          onClick={onFilter}
          aria-label={filterCount ? t("toolbar.filterCountName", { count: filterCount }) : undefined}
        >
          <Icon name="sliders" className="h-4 w-4" />
          {filterCount ? t("toolbar.filterCount", { n: String(filterCount) }) : t("toolbar.filter")}
        </button>
      </div>
      {chips.length > 0 && (
        <div ref={chipsRef} className="chips" role="group" aria-label={t("chips.label")}>
          {chips.map((chip, index) => (
            <span key={chip.key} className="chip">
              <span className="lbl">{chip.label}</span>
              <button
                type="button"
                aria-label={t("chips.remove", { label: chip.label })}
                onClick={() => {
                  pendingFocus.current = index;
                  chip.remove();
                }}
              >
                <Icon name="x" className="h-3 w-3" />
              </button>
            </span>
          ))}
        </div>
      )}
    </>
  );
});
