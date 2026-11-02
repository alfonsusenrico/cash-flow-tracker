import type { DeviceHealth, PairedDevice } from "@/lib/api";

/** Synthetic health report; `ready` follows the server rule so fixtures cannot contradict it. */
export function healthReport(
  now: number,
  overrides: Partial<Omit<DeviceHealth, "self_test">> & { self_test?: Partial<DeviceHealth["self_test"]> } = {},
): DeviceHealth {
  const { self_test: selfTest, ...rest } = overrides;
  const report: DeviceHealth = {
    reported_at: new Date(now - 60_000).toISOString(),
    ready: false,
    app_version: "2.4.0",
    listener_connected: true,
    notification_access: true,
    post_notifications: true,
    battery_unrestricted: true,
    self_test: { status: "passed", failed_step: null, at: new Date(now - 60_000).toISOString(), ...selfTest },
    last_capture_at: null,
    queue_pending: 0,
    ...rest,
  };
  report.ready = report.notification_access && report.listener_connected && report.self_test.status === "passed";
  return report;
}

export function pairedPhone(health: DeviceHealth | null, overrides: Partial<PairedDevice> = {}): PairedDevice {
  return {
    id: "key-phone", name: "TECNO CN7c", created_via: "pairing", key_prefix: "cfk_wxyz",
    created_at: "2026-11-02T02:58:00Z", last_used_at: null, health, ...overrides,
  };
}
