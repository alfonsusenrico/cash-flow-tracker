import { isActivePath } from "./navigation";

/**
 * Title and subtitle of the pages no phase has translated yet (add-language-system D10). They stay Indonesian in
 * every interface language, inside a `lang="id"` title block; each phase moves its page into the catalogue and
 * removes its entry here. Every product page writes its own title since apply-markas-pengaturan; only the local
 * component sheet is listed.
 */
interface PageTitle {
  title: string;
  subtitle: string;
}

const PAGE_TITLES: [string, PageTitle][] = [
  ["/komponen", { title: "Komponen", subtitle: "Lembar komponen Markas+, hanya di build lokal" }],
];

/** The untranslated title block of a route, or null for Beranda and any other route without one. */
export function untranslatedPageTitle(pathname: string): PageTitle | null {
  return PAGE_TITLES.find(([href]) => isActivePath(pathname, href))?.[1] ?? null;
}
