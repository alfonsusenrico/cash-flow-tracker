"use client";

import { useEffect, useRef } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { SettingsSectionList, SettingsSectionNav } from "@/components/settings/SettingsNav";
import { SETTINGS_HOME, sectionForPath } from "@/components/settings/sections";

function isHidden(element: Element | null) {
  return !element || element === document.body || !(element as HTMLElement).offsetParent;
}

export default function SettingsLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const section = sectionForPath(pathname);
  const atIndex = section === null;
  const title = useRef<HTMLHeadingElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const previousPath = useRef(pathname);

  // A phone link hides itself when its screen opens; move focus to what replaced it instead of the page body.
  useEffect(() => {
    if (previousPath.current === pathname) return;
    previousPath.current = pathname;
    if (!isHidden(document.activeElement)) return;
    const heading = content.current?.querySelector<HTMLElement>("[data-settings-heading]");
    (atIndex || isHidden(heading ?? null) ? title.current : heading)?.focus();
  }, [pathname, atIndex]);

  return (
    <div className="mx-auto w-full max-w-[1000px] space-y-5">
      <header className="space-y-1 border-b border-[var(--border)] pb-4">
        <h1
          ref={title}
          tabIndex={-1}
          className="text-2xl font-bold tracking-[-0.02em] text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)]"
        >
          Pengaturan
        </h1>
        <p className="text-sm text-[var(--muted)]">Setiap bagian disimpan sendiri-sendiri.</p>
      </header>

      <div className="lg:flex lg:items-start lg:gap-8">
        <SettingsSectionNav
          current={section ?? "profil"}
          className="hidden w-[220px] shrink-0 lg:sticky lg:top-24 lg:block"
        />
        {atIndex && <SettingsSectionList className="lg:hidden" />}

        <div ref={content} className={cn("min-w-0 flex-1 lg:max-w-[720px]", atIndex && "hidden lg:block")}>
          {!atIndex && (
            <Link
              href={SETTINGS_HOME}
              className="mb-3 inline-flex min-h-11 items-center gap-1.5 rounded-[var(--radius-md)] pr-3 text-sm font-semibold text-[var(--text-secondary)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)] lg:hidden"
            >
              <Icon name="chevron-left" className="h-4 w-4" />
              Semua pengaturan
            </Link>
          )}
          {children}
        </div>
      </div>
    </div>
  );
}
