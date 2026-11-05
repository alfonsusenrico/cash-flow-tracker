"use client";

import Link from "next/link";
import { type RefObject, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Icon } from "@/components/ui/Icon";
import { useWorkspace, type WorkspaceNotification } from "./context";
import { Popover } from "./Menu";

export const NOTIFICATIONS_SEEN_KEY = "notificationsSeen";
// Enough for every item the bell can list several times over; older ids no longer appear.
const SEEN_LIMIT = 50;

function readSeen(): string[] | null {
  try {
    const raw = window.localStorage.getItem(NOTIFICATIONS_SEEN_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((value): value is string => typeof value === "string") : [];
  } catch {
    return null;
  }
}

/**
 * Per-browser read state (D6): opening the bell marks the listed ids as seen. When storage is unavailable every
 * item stays unread for the visit and nothing fails.
 */
export function useNotificationSeen(notifications: WorkspaceNotification[]) {
  const [seen, setSeen] = useState<string[] | null>([]);

  useEffect(() => {
    setSeen(readSeen());
  }, []);

  const unreadIds = useMemo(() => {
    const known = new Set(seen ?? []);
    return new Set(notifications.filter((item) => !known.has(item.id)).map((item) => item.id));
  }, [notifications, seen]);

  const markSeen = useCallback(() => {
    if (seen === null) return;
    const merged = [...notifications.map((item) => item.id), ...seen.filter((id) => !notifications.some((item) => item.id === id))];
    const next = merged.slice(0, SEEN_LIMIT);
    try {
      window.localStorage.setItem(NOTIFICATIONS_SEEN_KEY, JSON.stringify(next));
      setSeen(next);
    } catch {
      setSeen(null);
    }
  }, [notifications, seen]);

  return { unreadIds, markSeen };
}

export function bellLabel(unread: number): string {
  return unread ? `Notifikasi, ${unread} baru` : "Notifikasi, tidak ada yang baru";
}

/** The notifications popover (400 px; a bottom sheet on the phone) with an unread dot per new item (D6, D7). */
export function NotificationsPanel({
  open,
  anchorRef,
  onClose,
}: {
  open: boolean;
  anchorRef: RefObject<HTMLElement | null>;
  onClose: () => void;
}) {
  const { notifications, unreadIds, markNotificationsSeen, openDuePlans } = useWorkspace();
  const panelRef = useRef<HTMLDivElement>(null);
  // The dots show what was new when the bell opened; opening marks everything as seen.
  const [shownUnread, setShownUnread] = useState<ReadonlySet<string>>(new Set());

  useLayoutEffect(() => {
    if (!open) return;
    setShownUnread(new Set(unreadIds));
    markNotificationsSeen();
    // The first item, else the panel itself; the close button is shown only on the phone sheet.
    const panel = panelRef.current;
    if (!panel) return;
    const firstItem = panel.querySelector<HTMLElement>(".notif");
    const close = panel.querySelector<HTMLElement>(".popover-close");
    if (firstItem) firstItem.focus();
    else if (close && close.offsetParent !== null) close.focus();
    else panel.focus();
    // Only the opening counts; later updates of the list must not mark again.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const close = useCallback(
    (returnFocus: boolean) => {
      onClose();
      if (returnFocus) anchorRef.current?.focus();
    },
    [onClose, anchorRef],
  );

  return (
    <Popover open={open} anchorRef={anchorRef} onClose={close} width={400} role="dialog" label="Notifikasi" panelRef={panelRef} trapFocus>
      <div className="popover-head">
        <span className="t-label popover-h">Notifikasi</span>
        <button type="button" className="ws-icon-btn popover-close" aria-label="Tutup notifikasi" onClick={() => close(true)}>
          <Icon name="x" className="h-4 w-4" />
        </button>
      </div>
      {notifications.length === 0 && <p className="t-body muted popover-empty">Belum ada yang baru.</p>}
      {notifications.map((item) => {
        const unread = shownUnread.has(item.id);
        const body = (
          <>
            <span className={unread ? "unread" : "read"} aria-hidden="true" />
            <span className="n-main">
              <span className="t-body n-title">
                {item.title}
                {unread && <span className="sr-only"> (baru)</span>}
              </span>
              <span className="t-meta">{item.detail}</span>
            </span>
          </>
        );
        if (item.opensDuePlans || !item.href) {
          return (
            <button
              key={item.id}
              type="button"
              className="notif"
              aria-haspopup="dialog"
              onClick={() => {
                close(true);
                openDuePlans();
              }}
            >
              {body}
            </button>
          );
        }
        return (
          <Link key={item.id} href={item.href} className="notif" onClick={() => close(false)}>
            {body}
          </Link>
        );
      })}
    </Popover>
  );
}

/** The bell with its muted count of new items; it opens the notifications popover. */
export function NotificationBell({ iconSize, id }: { iconSize: 16 | 20; id?: string }) {
  const { unreadIds } = useWorkspace();
  const [open, setOpen] = useState(false);
  const button = useRef<HTMLButtonElement>(null);
  const unread = unreadIds.size;
  return (
    <>
      <button
        ref={button}
        id={id}
        type="button"
        className="ws-icon-btn bell"
        aria-label={bellLabel(unread)}
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <Icon name="bell" className={iconSize === 20 ? "h-5 w-5" : "h-4 w-4"} />
        {unread > 0 && (
          <span className="count" aria-hidden="true">
            {unread}
          </span>
        )}
      </button>
      <NotificationsPanel open={open} anchorRef={button} onClose={() => setOpen(false)} />
    </>
  );
}
