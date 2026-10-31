export type SettingsSectionSlug = "profil" | "gaji" | "anggaran" | "perangkat" | "pemetaan" | "pengirim";

export interface SettingsSectionEntry {
  slug: SettingsSectionSlug;
  href: `/settings/${SettingsSectionSlug}`;
  title: string;
  purpose: string;
}

export const SETTINGS_HOME = "/settings";

export const SETTINGS_SECTIONS: SettingsSectionEntry[] = [
  { slug: "profil", href: "/settings/profil", title: "Profil", purpose: "Nama tampilan dan nama Anda di notifikasi bank." },
  { slug: "gaji", href: "/settings/gaji", title: "Gaji & siklus", purpose: "Sumber gaji yang memulai setiap siklus laporan." },
  {
    slug: "anggaran",
    href: "/settings/anggaran",
    title: "Mata uang & anggaran",
    purpose: "Mata uang tampilan, target dana darurat dan batas belanja bulanan.",
  },
  { slug: "perangkat", href: "/settings/perangkat", title: "Perangkat & akses", purpose: "Ponsel yang mengirim notifikasi bank dan kunci API cadangan." },
  { slug: "pemetaan", href: "/settings/pemetaan", title: "Pemetaan notifikasi", purpose: "Nama rekening di notifikasi yang sudah dipetakan ke rekening Anda." },
  { slug: "pengirim", href: "/settings/pengirim", title: "Nama pengirim", purpose: "Nama pengirim transfer yang sudah Anda konfirmasi." },
];

export function settingsSection(slug: SettingsSectionSlug): SettingsSectionEntry {
  const entry = SETTINGS_SECTIONS.find((section) => section.slug === slug);
  if (!entry) throw new Error(`Unknown settings section: ${slug}`);
  return entry;
}

/** The section a settings path shows; `/settings` itself shows Profil on wide screens. */
export function sectionForPath(pathname: string): SettingsSectionSlug | null {
  return SETTINGS_SECTIONS.find((section) => pathname === section.href || pathname.startsWith(`${section.href}/`))?.slug ?? null;
}
