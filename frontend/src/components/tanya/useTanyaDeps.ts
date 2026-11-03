"use client";

import { useMemo } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { CycleSummary } from "@/components/beranda/types";
import { api } from "@/lib/api";
import { parseCapture, submitCapture } from "@/lib/capture";
import { queryKeys } from "@/lib/queryKeys";
import type { ReportReadiness } from "@/lib/reportReady";
import type { ReviewQueue } from "@/lib/review";
import { FOLDER_LABELS, NEEDED_FOLDERS, buildTriage, itemTitle, triageSearch, type TriageItem } from "@/lib/triage";
import type { TurnDeps } from "@/lib/tanya/resolve";
import type { CardData, WaitingCardItem } from "@/lib/tanya/types";

/** How many Tinjau items the waiting card lists; the count is always the badge's full `attention`. */
export const WAITING_ITEMS = 3;

function itemAmount(item: TriageItem): number {
  switch (item.kind) {
    case "question":
      return item.entry.transaction.amount;
    case "group":
    case "suggestion":
      return item.group.total;
    case "awaiting":
    case "ambiguous":
      return item.item.amount;
  }
}

export function waitingItems(queue: ReviewQueue): WaitingCardItem[] {
  const triage = buildTriage(queue);
  return NEEDED_FOLDERS.flatMap((folder) => triage.folders[folder]).slice(0, WAITING_ITEMS).map((item) => ({
    id: item.id,
    title: itemTitle(item),
    amount: itemAmount(item),
    folder: item.folder,
    folderLabel: FOLDER_LABELS[item.folder],
    href: `/review${triageSearch(item.folder, item)}`,
  }));
}

/**
 * The resolver's API calls, through the shared query cache: a card reuses fresh data the shell or Beranda
 * already holds, and "Perbarui" (`fresh`) always fetches. Every call is same-origin `/api/`.
 */
export function useTanyaDeps(): TurnDeps {
  const queryClient = useQueryClient();
  return useMemo<TurnDeps>(() => {
    const get = <T,>(queryKey: readonly unknown[], path: string, fresh?: boolean) =>
      queryClient.fetchQuery<T>({ queryKey, queryFn: () => api.get<T>(path), ...(fresh ? { staleTime: 0 } : {}) });
    const loadCard = async (kind: CardData["kind"], fresh?: boolean): Promise<CardData> => {
      switch (kind) {
        case "cycle":
        case "kakeibo":
        case "bills":
          return { kind, summary: await get<CycleSummary>(queryKeys.dashboard.cycleSummary, "/dashboard/cycle-summary", fresh) };
        case "waiting": {
          const [summary, queue] = await Promise.all([
            get<{ attention: number }>(queryKeys.review.summary, "/review/summary", fresh),
            get<ReviewQueue>(queryKeys.review.queue, "/review/queue", fresh),
          ]);
          return { kind, waiting: { attention: summary.attention, items: waitingItems(queue) } };
        }
        case "report":
          return { kind, report: await get<ReportReadiness>(queryKeys.reports.latest, "/reports/monthly/latest", fresh) };
      }
    };
    return {
      parse: (text) => parseCapture(text),
      submit: (text, clientCaptureId) => submitCapture(text, clientCaptureId),
      loadCard,
      now: () => new Date(),
    };
  }, [queryClient]);
}
