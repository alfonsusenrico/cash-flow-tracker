import Link from "next/link";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { groupedSections, type SettingsSectionSlug } from "@/components/settings/sections";
import styles from "@/components/accounts/money.module.css";

/** Wide-screen section list beside the selected section, with the "Akun" group under its own heading. */
export function SettingsSectionNav({ current, className }: { current: SettingsSectionSlug; className?: string }) {
  return (
    <nav aria-label="Bagian pengaturan" className={cn(styles.settingsNav, className)}>
      {groupedSections().map(({ group, sections }) => (
        <div key={group ?? "main"} className={styles.settingsGroup}>
          {group && <h2 id={`settings-group-${group}`} className={styles.settingsGroupTitle}>{group}</h2>}
          <ul aria-labelledby={group ? `settings-group-${group}` : undefined}>
            {sections.map((section) => (
              <li key={section.slug}>
                <Link href={section.href} aria-current={section.slug === current ? "page" : undefined} className={styles.settingsLink}>
                  {section.title}
                </Link>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </nav>
  );
}

/** Narrow-screen index: each section opens on its own screen. */
export function SettingsSectionList({ className }: { className?: string }) {
  return (
    <nav aria-label="Daftar bagian pengaturan" className={cn(styles.settingsList, className)}>
      {groupedSections().map(({ group, sections }) => (
        <div key={group ?? "main"}>
          {group && <h2 id={`settings-list-${group}`} className={styles.settingsGroupTitle}>{group}</h2>}
          <ul className="ws-card" aria-labelledby={group ? `settings-list-${group}` : undefined}>
            {sections.map((section) => (
              <li key={section.slug}>
                <Link href={section.href} className={styles.settingsListLink}>
                  <span>
                    <b>{section.title}</b>
                    <span>{section.purpose}</span>
                  </span>
                  <Icon name="chevron-right" className="h-4 w-4 shrink-0" />
                </Link>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </nav>
  );
}
