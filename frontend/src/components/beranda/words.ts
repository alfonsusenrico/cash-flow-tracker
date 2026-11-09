"use client";

import { useTranslations } from "next-intl";
import { useMemo } from "react";
import { useFormat } from "@/lib/format";
import type { Words } from "@/lib/i18n/types";

export { bucketName } from "@/components/transactions/rows";

/** The catalogue and the region's formats for Beranda's text-building functions (add-language-system D5, D6). */
export function useWords(): Words {
  const t = useTranslations();
  const format = useFormat();
  return useMemo(() => ({ t, format }), [t, format]);
}
