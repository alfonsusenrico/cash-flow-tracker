"use client";

import { useQuery } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import type { AutoLinkedOccurrence, DueExpectation } from "@/types/recurring";

const DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "UTC" });

/** "3 Nov" for a plan's calendar date (`YYYY-MM-DD`), like the rest of the app. */
export function planDay(date: string): string {
  return DAY.format(new Date(`${date}T00:00:00Z`));
}

/** The one line of the notice; the full matching detail stays in `PendingScheduledBanner`. */
export function duePlansText(rules: DueExpectation[], autoLinks: AutoLinkedOccurrence[], money: (amount: number) => string): string | null {
  const parts: string[] = [];
  if (rules.length === 1) {
    const [rule] = rules;
    parts.push("1 rencana jatuh tempo", `${rule.name} ${money(rule.amount)}`, planDay(rule.next_due_date));
  } else if (rules.length > 1) {
    const total = rules.reduce((sum, rule) => sum + rule.amount, 0);
    const first = [...rules].map((rule) => rule.next_due_date).sort()[0];
    parts.push(`${rules.length} rencana jatuh tempo`, `total ${money(total)}`, `paling awal ${planDay(first)}`);
  }
  if (autoLinks.length) parts.push(`${autoLinks.length} ditautkan otomatis`);
  return parts.length ? parts.join(" · ") : null;
}

/**
 * The compact form of due plans on Transaksi: one line and one action that opens the existing detail
 * (link, skip, record manually) in the shared drawer. It reads the same query as the detail.
 */
export function DuePlansNotice({ onOpen }: { onOpen: () => void }) {
  const { bal } = useAppCtx();
  const { data } = useQuery<{ ok: boolean; rules: DueExpectation[]; recent_auto_links?: AutoLinkedOccurrence[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    refetchInterval: 30000,
  });
  const text = duePlansText(data?.rules ?? [], data?.recent_auto_links ?? [], bal);
  if (!text) return null;
  return (
    <div className="ws-banner ws-banner-warn ws-plan-notice" role="region" aria-label="Rencana jatuh tempo">
      <Icon name="clock" className="h-4 w-4 shrink-0" />
      <span className="ws-plan-notice-t tabular-nums">{text}</span>
      <button type="button" className="ws-btn ws-btn-sm" onClick={onOpen} aria-haspopup="dialog">
        Lihat rencana
      </button>
    </div>
  );
}
