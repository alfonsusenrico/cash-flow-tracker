"use client";

import { useTranslations } from "next-intl";
import type { NavBadge } from "./navigation";

/** Words for a count badge, so the number is never colour or position alone. */
export function useBadgeWords() {
  const t = useTranslations("nav");
  return (kind: NavBadge, count: number) => (kind === "review" ? t("needsReviewCount", { count }) : t("reportReady"));
}
