import { CompanionDownloadSection } from "@/components/settings/CompanionDownload";
import { DevicePairingSection } from "@/components/settings/DevicePairingSection";
import { ManualApiKey } from "@/components/settings/ManualApiKey";
import { PhoneStatus } from "@/components/settings/PhoneStatus";
import { SettingsSection } from "@/components/settings/SettingsSection";

/** HP & perangkat (apply-markas-pengaturan D6): Status HP, the download, the paired phones, then the manual key. */
export function DevicesSettings() {
  return (
    <SettingsSection section="perangkat">
      <PhoneStatus />
      <CompanionDownloadSection />
      <DevicePairingSection />
      <ManualApiKey />
    </SettingsSection>
  );
}
