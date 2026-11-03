/**
 * URL writes for client-side filters (redesign-transactions-workspace D13).
 *
 * While a page's initial URL carries a query, Next.js 16.2 commits no navigation to the same path, so
 * `router.replace` leaves the URL and the filters unchanged. The browser history API is supported by
 * Next.js and kept in sync with `useSearchParams` and `usePathname`, and filter changes need no server
 * round trip, because each page reads its filters on the client.
 */

/**
 * Replaces the current entry. The state passed must not carry Next's own keys (`__NA`): Next.js treats a
 * call with them as its own and then skips the `useSearchParams` sync. It adds its keys itself.
 */
export function replaceSearch(href: string, state: Record<string, unknown> | null = null): void {
  window.history.replaceState(state, "", href);
}

/** Adds an entry, so the browser's back returns to the previous filters or closes a phone sheet. */
export function pushSearch(href: string, state: Record<string, unknown> | null = null): void {
  window.history.pushState(state, "", href);
}

/** True when an href points at the path already shown, whatever its query. */
export function samePath(href: string, pathname: string): boolean {
  return href.split("?")[0] === pathname;
}

/**
 * A click handler for a navigation link: to the same path with another query it writes the URL itself
 * (D13), to any other path it leaves the `<Link>` navigation alone. Modified clicks keep their meaning.
 */
export function sameRouteClick(href: string, pathname: string, after?: () => void) {
  return (event: { preventDefault: () => void; metaKey: boolean; ctrlKey: boolean; shiftKey: boolean; altKey: boolean; button: number }) => {
    if (!samePath(href, pathname) || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) {
      after?.();
      return;
    }
    event.preventDefault();
    if (`${window.location.pathname}${window.location.search}` !== href) pushSearch(href);
    after?.();
  };
}
