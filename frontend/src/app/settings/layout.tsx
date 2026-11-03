"use client";

import { useEffect, useRef } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { SettingsSectionList, SettingsSectionNav } from "@/components/settings/SettingsNav";
import { SETTINGS_HOME, sectionForPath } from "@/components/settings/sections";
import styles from "@/components/accounts/money.module.css";

function isHidden(element: Element | null) {
  return !element || element === document.body || !(element as HTMLElement).offsetParent;
}

/** The page title is the top bar's "Pengaturan"; this layout adds the grouped sections (D6). */
export default function SettingsLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const section = sectionForPath(pathname);
  const atIndex = section === null;
  const content = useRef<HTMLDivElement>(null);
  const list = useRef<HTMLDivElement>(null);
  const previousPath = useRef(pathname);

  // A phone link hides itself when its screen opens; move focus to what replaced it instead of the page body.
  useEffect(() => {
    if (previousPath.current === pathname) return;
    previousPath.current = pathname;
    if (!isHidden(document.activeElement)) return;
    // A section screen focuses its heading; the index focuses its first link.
    const heading = content.current?.querySelector<HTMLElement>("[data-settings-heading]");
    if (!atIndex && heading) heading.focus();
    else list.current?.querySelector<HTMLElement>("a")?.focus();
  }, [pathname, atIndex]);

  return (
    <div className={styles.settingsPage}>
      <SettingsSectionNav current={section ?? "perangkat"} className={styles.settingsSide} />
      {atIndex && (
        <div ref={list} className={styles.settingsIndex}>
          <SettingsSectionList />
        </div>
      )}
      <div ref={content} className={cn(styles.settingsContent, atIndex && styles.settingsContentAtIndex)}>
        {!atIndex && (
          <Link href={SETTINGS_HOME} className={cn("ws-btn", styles.settingsBack)}>
            <Icon name="chevron-left" className="h-4 w-4" />
            Semua pengaturan
          </Link>
        )}
        {children}
      </div>
    </div>
  );
}
