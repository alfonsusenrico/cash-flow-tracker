import { DevicePairingSection } from "@/components/ui/DevicePairingSection";
import { ManualApiKey } from "@/components/settings/ManualApiKey";
import { SettingsCard } from "@/components/settings/SettingsCard";

export function DevicesSettings() {
  return (
    <SettingsCard section="perangkat">
      <DevicePairingSection />
      <div className="border-t border-[var(--border)] pt-5">
        <ManualApiKey />
      </div>
    </SettingsCard>
  );
}
