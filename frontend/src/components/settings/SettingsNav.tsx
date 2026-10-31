import Link from "next/link";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { SETTINGS_SECTIONS, type SettingsSectionSlug } from "@/components/settings/sections";

const linkFocus = "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)]";

/** Wide-screen section list beside the selected section. */
export function SettingsSectionNav({ current, className }: { current: SettingsSectionSlug; className?: string }) {
  return (
    <nav aria-label="Bagian pengaturan" className={className}>
      <ul className="space-y-1">
        {SETTINGS_SECTIONS.map((section) => {
          const active = section.slug === current;
          return (
            <li key={section.slug}>
              <Link
                href={section.href}
                aria-current={active ? "page" : undefined}
                className={cn(
                  "flex min-h-11 items-center rounded-[var(--radius-md)] px-3 text-sm font-medium transition-colors",
                  linkFocus,
                  active
                    ? "bg-[var(--surface)] font-semibold text-[var(--text)] shadow-[var(--shadow-sm)] ring-1 ring-[var(--border)]"
                    : "text-[var(--text-secondary)] hover:bg-[var(--surface)] hover:text-[var(--text)]",
                )}
              >
                {section.title}
              </Link>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}

/** Narrow-screen index: each section opens on its own screen. */
export function SettingsSectionList({ className }: { className?: string }) {
  return (
    <nav aria-label="Daftar bagian pengaturan" className={className}>
      <ul className="card-squircle divide-y divide-[var(--border)] overflow-hidden">
        {SETTINGS_SECTIONS.map((section) => (
          <li key={section.slug}>
            <Link
              href={section.href}
              className={cn("flex min-h-14 items-center gap-3 px-4 py-3 hover:bg-[var(--surface-raised)]/50", linkFocus, "focus-visible:-outline-offset-2")}
            >
              <span className="min-w-0 flex-1">
                <span className="block text-sm font-semibold text-[var(--text)]">{section.title}</span>
                <span className="block text-xs text-[var(--muted)]">{section.purpose}</span>
              </span>
              <Icon name="chevron-right" className="h-4 w-4 shrink-0 text-[var(--muted)]" />
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  );
}
