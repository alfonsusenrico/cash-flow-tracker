import type { ReactNode } from "react";
import { Icon } from "./Icon";

/**
 * Summary row for a native <details>. A flex summary drops the browser's marker, so the
 * chevron carries the "this opens" cue and flips when the parent details is open.
 */
export function DisclosureSummary({ children }: { children: ReactNode }) {
  return (
    <summary className="inline-flex min-h-11 cursor-pointer list-none items-center gap-1.5 rounded-[var(--radius-md)] text-sm font-semibold text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--focus)] [&::-webkit-details-marker]:hidden [[open]>&>svg]:rotate-180">
      {children}
      <Icon name="chevron-down" className="h-4 w-4 shrink-0 text-[var(--text-2)] transition-transform motion-reduce:transition-none" />
    </summary>
  );
}
