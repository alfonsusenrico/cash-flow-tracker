"use client";

import { useQuery } from "@tanstack/react-query";
import { getDevices, type PairedDevice } from "@/lib/api";
import { deviceProblems, isStale } from "@/lib/deviceHealth";
import { queryKeys } from "@/lib/queryKeys";

export interface CompanionState {
  tone: "ok" | "warn" | "none";
  title: string;
  detail: string;
}

function clock(iso: string, now: Date): string {
  const at = new Date(iso);
  const time = at.toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
  const day = (date: Date) => date.toLocaleDateString("id-ID", { timeZone: "Asia/Jakarta" });
  if (day(at) === day(now)) return time;
  const yesterday = new Date(now.getTime() - 86_400_000);
  if (day(at) === day(yesterday)) return `kemarin ${time}`;
  return at.toLocaleDateString("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });
}

/** Ready with the last contact, needs attention with the reason, or no phone paired. */
export function companionState(devices: PairedDevice[], now = new Date()): CompanionState {
  const paired = devices.filter((device) => device.created_via === "pairing");
  if (!paired.length) return { tone: "none", title: "Belum ada HP terhubung", detail: "Sambungkan HP dengan kode di bawah ini" };
  const reporting = paired
    .filter((device) => device.health)
    .sort((a, b) => Date.parse(b.health!.reported_at) - Date.parse(a.health!.reported_at));
  const device = reporting[0];
  if (!device?.health) return { tone: "warn", title: "HP perlu dicek", detail: "HP terhubung tapi belum kirim kabar" };
  const health = device.health;
  const lastContact = health.last_capture_at ?? device.last_used_at ?? health.reported_at;
  if (health.ready && !isStale(health, now.getTime())) {
    return { tone: "ok", title: "HP terhubung", detail: `Terakhir menangkap ${clock(lastContact, now)}` };
  }
  const problem = deviceProblems(health, now.getTime())[0] ?? "HP belum siap menangkap notifikasi.";
  return { tone: "warn", title: "HP perlu dicek", detail: problem };
}

/** The phone's capture status at the top of Pengaturan · HP & perangkat (refine-workspace-layout D3). */
export function CompanionStatus() {
  const { data, isError } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices, refetchInterval: 60_000 });
  if (isError || !data) return null;
  const state = companionState(data.devices ?? []);
  return (
    <p className="ws-companion-status" data-tone={state.tone}>
      <span className="ws-dot" data-tone={state.tone} aria-hidden="true" />
      <span>
        <strong>{state.title}</strong>
        {state.detail}
      </span>
    </p>
  );
}
