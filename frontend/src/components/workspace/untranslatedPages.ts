import { isActivePath } from "./navigation";

/**
 * Title and subtitle of the pages no phase has translated yet (add-language-system D10). They stay Indonesian in
 * every interface language, inside a `lang="id"` title block; each phase moves its page into the catalogue and
 * removes its entry here. Beranda, Tinjau, Transaksi and Rekening & Kantong write their own titles and are not listed.
 */
interface PageTitle {
  title: string;
  subtitle: string;
}

const PAGE_TITLES: [string, PageTitle][] = [
  ["/goals", { title: "Target & Tagihan", subtitle: "Dana darurat, tagihan, utang dan target tabungan" }],
  ["/insights", { title: "Analisis", subtitle: "Ke mana uang digunakan, dibanding periode lalu" }],
  ["/laporan", { title: "Laporan", subtitle: "Laporan bulanan Kakeibo" }],
  ["/settings", { title: "Pengaturan", subtitle: "" }],
  ["/komponen", { title: "Komponen", subtitle: "Lembar komponen Markas+, hanya di build lokal" }],
];

/** The untranslated title block of a route, or null for Beranda and any other route without one. */
export function untranslatedPageTitle(pathname: string): PageTitle | null {
  return PAGE_TITLES.find(([href]) => isActivePath(pathname, href))?.[1] ?? null;
}
