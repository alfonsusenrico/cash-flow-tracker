import { cookies, headers } from "next/headers";
import { getRequestConfig } from "next-intl/server";
import { LANGUAGE_COOKIE, resolveLanguage, type Language } from "./language";
import { catalogueFor } from "./catalogue";

/**
 * The request's language and catalogue (add-language-system D1, D2). Without i18n routing the language comes from
 * the `cft_language` cookie, then `Accept-Language`, then English; `AppLayout` makes the cookie follow the account.
 */
export default getRequestConfig(async () => {
  const [cookieStore, headerList] = await Promise.all([cookies(), headers()]);
  const locale: Language = resolveLanguage(cookieStore.get(LANGUAGE_COOKIE)?.value, headerList.get("accept-language"));
  return { locale, messages: catalogueFor(locale), timeZone: "Asia/Jakarta" };
});
