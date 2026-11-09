"use client";

import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { getDevices, type PairedDevice } from "@/lib/api";
import { deviceProblems, isStale, problemText, type HealthWords } from "@/lib/deviceHealth";
import { useFormat, type Format } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";

export interface CompanionState {
  tone: "ok" | "warn" | "none";
  title: string;
  detail: string;
}

export interface CompanionText {
  t: ReturnType<typeof useTranslations<"shell.companion">>;
  /** The device problems' words, which Pengaturan translates since apply-markas-pengaturan (D19). */
  health: HealthWords;
  format: Format;
}

function clock(iso: string, now: Date, { t, format }: CompanionText): string {
  const at = new Date(iso);
  const time = format.time(at);
  const day = (date: Date) => date.toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
  if (day(at) === day(now)) return time;
  const yesterday = new Date(now.getTime() - 86_400_000);
  if (day(at) === day(yesterday)) return t("yesterdayAt", { time });
  return format.shortDateAt(at);
}

/** Ready with the last contact, needs attention with the reason, or no phone paired. */
export function companionState(devices: PairedDevice[], now: Date, text: CompanionText): CompanionState {
  const { t } = text;
  const paired = devices.filter((device) => device.created_via === "pairing");
  if (!paired.length) return { tone: "none", title: t("noPhone"), detail: t("connectHint") };
  const reporting = paired
    .filter((device) => device.health)
    .sort((a, b) => Date.parse(b.health!.reported_at) - Date.parse(a.health!.reported_at));
  const device = reporting[0];
  if (!device?.health) return { tone: "warn", title: t("needsCheck"), detail: t("notReporting") };
  const health = device.health;
  const lastContact = health.last_capture_at ?? device.last_used_at ?? health.reported_at;
  if (health.ready && !isStale(health, now.getTime())) {
    return { tone: "ok", title: t("connected"), detail: t("lastCapture", { when: clock(lastContact, now, text) }) };
  }
  const problem = deviceProblems(health, now.getTime())[0];
  if (!problem) return { tone: "warn", title: t("needsCheck"), detail: t("notReady") };
  return { tone: "warn", title: t("needsCheck"), detail: problemText(problem, health, text.health) };
}

/** The phone's capture status at the top of Pengaturan › HP & perangkat's "Status HP" box (refine-workspace-layout D3). */
export function CompanionStatus() {
  const t = useTranslations("shell.companion");
  const health = useTranslations("settings.health");
  const format = useFormat();
  const { data, isError } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices, refetchInterval: 60_000 });
  if (isError || !data) return null;
  const state = companionState(data.devices ?? [], new Date(), { t, health, format });
  // One line of text at the top of the "Status HP" box (apply-markas-pengaturan D7); the dot repeats the words.
  return (
    <p className="ws-companion-status" data-tone={state.tone}>
      <span className="ws-dot" data-tone={state.tone} aria-hidden="true" />
      <span>
        <strong>{state.title}</strong> · {state.detail}
      </span>
    </p>
  );
}
