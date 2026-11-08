export type SettingsSectionSlug =
  | "perangkat" | "kebiasaan" | "tampilan" | "profil" | "gaji" | "anggaran" | "pemetaan" | "pengirim" | "sesi-uji";

export interface SettingsSectionEntry {
  slug: SettingsSectionSlug;
  href: `/settings/${SettingsSectionSlug}`;
  title: string;
  purpose: string;
  /** Sections under one heading in the section navigation; ungrouped ones come first. */
  group?: "Akun";
}

export const SETTINGS_HOME = "/settings";

// Order and grouping approved as OD53 (redesign-money-and-settings D6).
export const SETTINGS_SECTIONS: SettingsSectionEntry[] = [
  { slug: "perangkat", href: "/settings/perangkat", title: "HP & perangkat", purpose: "Status HP, ponsel yang mengirim notifikasi bank dan kunci API cadangan." },
  { slug: "kebiasaan", href: "/settings/kebiasaan", title: "Kebiasaan", purpose: "Kebiasaan dan top up yang sudah dipelajari dari jawaban di Tinjau." },
  { slug: "tampilan", href: "/settings/tampilan", title: "Tampilan", purpose: "Bahasa, tema, pintasan keyboard, sembunyikan saldo dan bantuan." },
  { slug: "profil", href: "/settings/profil", title: "Profil", purpose: "Nama tampilan dan nama di notifikasi bank.", group: "Akun" },
  { slug: "gaji", href: "/settings/gaji", title: "Gaji & siklus", purpose: "Sumber gaji yang memulai setiap siklus laporan.", group: "Akun" },
  {
    slug: "anggaran",
    href: "/settings/anggaran",
    title: "Mata uang & anggaran",
    purpose: "Mata uang tampilan, target dana darurat dan batas belanja bulanan.",
    group: "Akun",
  },
  { slug: "pemetaan", href: "/settings/pemetaan", title: "Pemetaan notifikasi", purpose: "Nama rekening di notifikasi yang dipetakan ke rekening.", group: "Akun" },
  { slug: "pengirim", href: "/settings/pengirim", title: "Nama pengirim", purpose: "Nama pengirim transfer yang sudah dikonfirmasi.", group: "Akun" },
  {
    slug: "sesi-uji",
    href: "/settings/sesi-uji",
    title: "Sesi uji",
    purpose: "Notifikasi selama sesi disimpan untuk pengujian tanpa dicatat ke transaksi.",
    group: "Akun",
  },
];

export function settingsSection(slug: SettingsSectionSlug): SettingsSectionEntry {
  const entry = SETTINGS_SECTIONS.find((section) => section.slug === slug);
  if (!entry) throw new Error(`Unknown settings section: ${slug}`);
  return entry;
}

/** The section a settings path shows; `/settings` itself shows HP & perangkat on wide screens. */
export function sectionForPath(pathname: string): SettingsSectionSlug | null {
  return SETTINGS_SECTIONS.find((section) => pathname === section.href || pathname.startsWith(`${section.href}/`))?.slug ?? null;
}

/** The sections in navigation order with their group headings. */
export function groupedSections(): { group: string | null; sections: SettingsSectionEntry[] }[] {
  const groups: { group: string | null; sections: SettingsSectionEntry[] }[] = [];
  for (const section of SETTINGS_SECTIONS) {
    const key = section.group ?? null;
    const last = groups[groups.length - 1];
    if (last && last.group === key) last.sections.push(section);
    else groups.push({ group: key, sections: [section] });
  }
  return groups;
}
