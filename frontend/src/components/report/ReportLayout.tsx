"use client";

import { Fragment, useRef, type ReactNode } from "react";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { cn } from "@/lib/utils";
import styles from "./report.module.css";

/**
 * Two columns on a wide workspace; below 900 px one column in reading order. Rendering the order (not
 * CSS `order`) keeps the focus order equal to what is seen. Empty slots (`null`) are skipped.
 */
export function ReportLayout({ wide, narrow }: { wide: [ReactNode[], ReactNode[]]; narrow: ReactNode[] }) {
  const layoutRef = useRef<HTMLDivElement>(null);
  const oneColumn = useWidthAtMost(layoutRef, 900);
  const column = (cards: ReactNode[]) => (
    <div className={styles.column}>{cards.filter(Boolean).map((card, index) => <Fragment key={index}>{card}</Fragment>)}</div>
  );
  return (
    <div ref={layoutRef} className={cn(styles.layout, oneColumn && styles.single)}>
      {oneColumn ? column(narrow) : <>{column(wide[0])}{column(wide[1])}</>}
    </div>
  );
}
