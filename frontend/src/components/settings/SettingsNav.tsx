"use client";

import { Fragment } from "react";
import { useTranslations } from "next-intl";
import { Rail, RailItem, RailLabel, RailRule } from "@/components/workspace/rail";
import { useWorkspace } from "@/components/workspace/context";
import { groupedSections, type SettingsGroup, type SettingsSectionSlug } from "@/components/settings/sections";

const GROUP_KEY: Record<SettingsGroup, "groupDevices" | "groupAppearance" | "groupAccount"> = {
  devices: "groupDevices",
  appearance: "groupAppearance",
  account: "groupAccount",
};

/**
 * The Pengaturan rail (apply-markas-pengaturan D2, D5): a navigation of three labelled lists of section links, the
 * current one with `aria-current="page"` and the selection bar, then a rule and "Keluar", which signs out exactly as
 * the account menu does. The same vertical rail serves every width; below 768 px it sits above the section.
 */
export function SettingsRail({ current }: { current: SettingsSectionSlug }) {
  const t = useTranslations("settings");
  const shell = useTranslations("shell");
  const { logout } = useWorkspace();
  return (
    <Rail label={t("nav.label")} showLabel={false} className="set-rail">
      {groupedSections().map(({ group, sections }) => {
        const labelId = `settings-group-${group}`;
        return (
          <Fragment key={group}>
            <RailLabel id={labelId}>{t(`nav.${GROUP_KEY[group]}`)}</RailLabel>
            <ul className="rail-group" aria-labelledby={labelId}>
              {sections.map((section) => (
                <li key={section.slug}>
                  <RailItem href={section.href} current={section.slug === current} label={t(`sections.${section.slug}.title`)} />
                </li>
              ))}
            </ul>
          </Fragment>
        );
      })}
      <RailRule />
      <RailItem icon="logout" label={shell("signOut")} onSelect={logout} />
    </Rail>
  );
}
