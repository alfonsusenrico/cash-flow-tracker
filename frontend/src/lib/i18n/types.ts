import type { useTranslations } from "next-intl";
import type { Format } from "@/lib/format";

/** The translator over every namespace, for plain functions that build a page's text (add-language-system D5). */
export type Translate = ReturnType<typeof useTranslations<never>>;

/** What a text-building function needs: the messages and the region's formats. */
export interface Words {
  t: Translate;
  format: Format;
}
