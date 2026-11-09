"use client";

import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { useReturnFocus } from "@/components/workspace/layers";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import type { AutoLinkedOccurrence, DueExpectation } from "@/types/recurring";
import { PendingScheduledBanner } from "./PendingScheduledBanner";

/**
 * "Rencana jatuh tempo" (apply-markas-catat D20), opened from the bell and from Beranda's Perlu tindakan: phase 9's
 * due-plan actions in a 560 px dialog, a bottom sheet below 768 px. Link, skip and record send today's requests.
 */
export function DuePlansDialog({ open, onClose, onOpenRulesManager }: { open: boolean; onClose: () => void; onOpenRulesManager: () => void }) {
  const t = useTranslations("shell.duePlans");
  useReturnFocus(open);
  const pending = useQuery<{ ok: boolean; rules: DueExpectation[]; recent_auto_links?: AutoLinkedOccurrence[] }>({
    queryKey: queryKeys.recurring.pending,
    queryFn: () => api.get("/recurring/pending"),
    enabled: open,
  });
  const empty = pending.isSuccess && !(pending.data.rules ?? []).length && !(pending.data.recent_auto_links ?? []).length;
  return (
    <ShellDialog open={open} onClose={onClose} title={t("title")} meta={t("subtitle")} width={560} closeLabel={t("close")}>
      <PendingScheduledBanner onOpenRulesManager={onOpenRulesManager} />
      {empty && <p className="t-meta">{t("noneToday")}</p>}
    </ShellDialog>
  );
}
