"use client";

import { useMemo } from "react";
import { useTranslations } from "next-intl";
import { useFormat, type Format } from "@/lib/format";
import type { Translate, Words } from "@/lib/i18n/types";
import type { ReviewGroup } from "@/lib/review";
import type { FolderKey, TriageItem } from "@/lib/triage";

/**
 * Tinjau's words for its data (apply-markas-tinjau D18): `lib/triage` keeps the folders and items, and these turn an
 * item into its title, date, kind and folder name from the `review` catalogue. User data is shown as stored.
 */
export function useTriageWords(): Words {
  const t = useTranslations();
  const format = useFormat();
  return useMemo(() => ({ t, format }), [t, format]);
}

export function folderName(t: Translate, folder: FolderKey): string {
  return folder === "semua" ? t("review.folders.all") : t(`review.folders.${folder}`);
}

/** The kind label above the item's question; "Saran dari riwayat" where the rail says "Saran" (D5). */
export function kindName(t: Translate, item: TriageItem): string {
  return t(`review.kind.${item.folder}`);
}

/** A group's own label, or the fixed fallback the server named by key (add-language-system D7). */
export function groupLabel(t: Translate, group: ReviewGroup): string {
  if (group.label_source === "system" && group.label_key === "undescribed") return t("terms.noDescription");
  if (group.label_source === "system" && group.label_key === "uncategorized") return t("terms.noCategory");
  return group.label;
}

/** Short title of an item for the list, the toast and announcements. */
export function itemTitle(t: Translate, item: TriageItem): string {
  switch (item.kind) {
    case "question": {
      const { merchant, notes, type } = item.entry.transaction;
      return merchant || notes || (type === "income" ? t("review.list.title.moneyIn") : t("review.list.title.noStore"));
    }
    case "group":
      return groupLabel(t, item.group);
    case "suggestion":
      return item.group.label;
    case "awaiting":
    case "ambiguous":
      return item.item.type === "expense"
        ? t("review.list.title.outOf", { account: item.item.account_name })
        : t("review.list.title.into", { account: item.item.account_name });
    case "seen_twice":
      return item.item.kind === "skipped"
        ? t("review.list.title.skipped", { app: item.item.app })
        : t("review.list.title.secondReport", { app: item.item.app });
  }
}

/** The instant an item happened: the question's transaction, a group's latest row, or the report's time. */
export function itemDate(item: TriageItem): string | null {
  switch (item.kind) {
    case "question":
      return item.entry.transaction.date;
    case "group":
    case "suggestion":
      return item.group.last_date;
    default:
      return item.item.date;
  }
}

/** "4 Nov 21.40" or "Nov 4 9:40 PM" in the app's time zone. */
export function dateTime(format: Format, iso: string | null): string | null {
  return iso ? `${format.shortDateAt(iso)} ${format.time(iso)}` : null;
}
