import { CompanionDownloadSection } from "@/components/ui/CompanionDownload";
import { DevicePairingSection } from "@/components/ui/DevicePairingSection";
import { ManualApiKey } from "@/components/settings/ManualApiKey";
import { SettingsCard } from "@/components/settings/SettingsCard";
import { CompanionStatus } from "@/components/workspace/CompanionStatus";

export function DevicesSettings() {
  return (
    <SettingsCard section="perangkat">
      {/* Moved here from the sidebar foot (refine-workspace-layout D3). */}
      <CompanionStatus />
      <CompanionDownloadSection />
      <DevicePairingSection />
      <div className="border-t border-[var(--line)] pt-5">
        <ManualApiKey />
      </div>
    </SettingsCard>
  );
}
