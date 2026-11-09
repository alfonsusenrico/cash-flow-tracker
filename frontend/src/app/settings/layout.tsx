"use client";

import { useEffect, useRef } from "react";
import { usePathname } from "next/navigation";
import { SettingsRail } from "@/components/settings/SettingsNav";
import { sectionForPath } from "@/components/settings/sections";
import { PHONE_QUERY } from "@/components/workspace/layers";

/**
 * Pengaturan's frame (apply-markas-pengaturan D1 to D3, D20): the shell's title block is "Pengaturan"; this layout
 * adds the `.set` grid, the rail beside the section from 768 px and above it on a phone. `/settings` shows
 * HP & perangkat. After a rail choice focus moves to the new section's title, which a phone scrolls into view below
 * its 56 px header.
 */
export default function SettingsLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const section = sectionForPath(pathname) ?? "perangkat";
  const content = useRef<HTMLDivElement>(null);
  const previousPath = useRef(pathname);

  useEffect(() => {
    if (previousPath.current === pathname) return;
    previousPath.current = pathname;
    const heading = content.current?.querySelector<HTMLElement>("[data-settings-heading]");
    if (!heading) return;
    heading.focus({ preventScroll: true });
    // On a phone the rail stands above the section, so the title is brought up under the header.
    if (window.matchMedia?.(PHONE_QUERY).matches) heading.scrollIntoView?.({ block: "start" });
  }, [pathname]);

  return (
    <div className="set">
      <SettingsRail current={section} />
      <div ref={content} className="set-form">
        {children}
      </div>
    </div>
  );
}
