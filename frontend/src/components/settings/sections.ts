export type SettingsSectionSlug =
  | "perangkat" | "kebiasaan" | "tampilan" | "profil" | "gaji" | "anggaran" | "pemetaan" | "pengirim" | "sesi-uji";

/** The rail's groups (apply-markas-pengaturan D2): PERANGKAT, TAMPILAN and AKUN. */
export type SettingsGroup = "devices" | "appearance" | "account";

export interface SettingsSectionEntry {
  slug: SettingsSectionSlug;
  href: `/settings/${SettingsSectionSlug}`;
  group: SettingsGroup;
}

export const SETTINGS_HOME = "/settings";

// The section addresses and their order are unchanged since OD53 (redesign-money-and-settings D6); titles and
// descriptions are `settings.sections.<slug>` in the catalogues.
export const SETTINGS_SECTIONS: SettingsSectionEntry[] = [
  { slug: "perangkat", href: "/settings/perangkat", group: "devices" },
  { slug: "kebiasaan", href: "/settings/kebiasaan", group: "devices" },
  { slug: "tampilan", href: "/settings/tampilan", group: "appearance" },
  { slug: "profil", href: "/settings/profil", group: "account" },
  { slug: "gaji", href: "/settings/gaji", group: "account" },
  { slug: "anggaran", href: "/settings/anggaran", group: "account" },
  { slug: "pemetaan", href: "/settings/pemetaan", group: "account" },
  { slug: "pengirim", href: "/settings/pengirim", group: "account" },
  { slug: "sesi-uji", href: "/settings/sesi-uji", group: "account" },
];

/** The section a settings path shows; `/settings` itself shows HP & perangkat. */
export function sectionForPath(pathname: string): SettingsSectionSlug | null {
  return SETTINGS_SECTIONS.find((section) => pathname === section.href || pathname.startsWith(`${section.href}/`))?.slug ?? null;
}

/** The sections in rail order, grouped. */
export function groupedSections(): { group: SettingsGroup; sections: SettingsSectionEntry[] }[] {
  const groups: { group: SettingsGroup; sections: SettingsSectionEntry[] }[] = [];
  for (const section of SETTINGS_SECTIONS) {
    const last = groups[groups.length - 1];
    if (last && last.group === section.group) last.sections.push(section);
    else groups.push({ group: section.group, sections: [section] });
  }
  return groups;
}

/** The heading id of a section, which focus moves to after a rail choice (D20). */
export function sectionHeadingId(slug: SettingsSectionSlug): string {
  return `settings-${slug}-title`;
}
