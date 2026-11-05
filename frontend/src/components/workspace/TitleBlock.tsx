"use client";

import { usePathname } from "next/navigation";
import { useWorkspace } from "./context";
import { pageMeta } from "./navigation";

/**
 * The page's title block (D4): Manrope 24/32 title, the one-line subtitle 4 px below, and the actions slot that
 * `PageActions` fills, on the title's bottom edge. The title comes from the route or from `usePageHeader`.
 */
export function TitleBlock() {
  const pathname = usePathname();
  const { header, setActionsSlot } = useWorkspace();
  const meta = header ?? pageMeta(pathname);
  return (
    <header className="title-block">
      <div className="tb-text">
        <h1 className="t-title">{meta.title}</h1>
        {meta.subtitle && <p className="sub t-body">{meta.subtitle}</p>}
      </div>
      <div className="tb-actions" ref={setActionsSlot} />
    </header>
  );
}
