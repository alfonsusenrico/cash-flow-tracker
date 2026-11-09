import { describe, expect, it } from "vitest";
import type { DeviceHealth, PairedDevice } from "@/lib/api";
import { healthReport as fixtureReport, pairedPhone } from "@/test/deviceHealthFixtures";
import { createTranslator } from "next-intl";
import { CATALOGUES } from "@/lib/i18n/testing";
import {
  checklistAnnouncement as announce,
  deviceProblems,
  pairingChecklist,
  problemText,
  type ChecklistStepState,
  type HealthWords,
  type PairingChecklist,
} from "@/lib/deviceHealth";

const words = createTranslator({ locale: "id", messages: CATALOGUES.id, namespace: "settings.health" }) as unknown as HealthWords;
const READY_MESSAGE = "HP siap. Transaksi dari notifikasi bank kini tercatat otomatis.";
const FIX = "Aplikasi di HP menunjukkan cara memperbaikinya.";

function checklistAnnouncement(checklist: PairingChecklist) {
  return announce(checklist, words, READY_MESSAGE, FIX);
}

const NOW = Date.parse("2026-11-02T03:00:00Z");

function healthReport(overrides: Parameters<typeof fixtureReport>[1] = {}) {
  return fixtureReport(NOW, overrides);
}

function phone(health: DeviceHealth | null): PairedDevice {
  return pairedPhone(health);
}

function states(device: PairedDevice | undefined): ChecklistStepState[] {
  return pairingChecklist(device).steps.map((step) => step.state);
}

describe("pairing checklist", () => {
  it("waits with every step open until a phone pairs", () => {
    const checklist = pairingChecklist(undefined);

    expect(checklist.stage).toBe("waiting");
    expect(states(undefined)).toEqual(["todo", "todo", "todo", "todo"]);
    expect(checklistAnnouncement(checklist)).toBe("");
  });

  it("marks the next step in progress once paired, before the first report", () => {
    const checklist = pairingChecklist(phone(null));

    expect(checklist.stage).toBe("setting-up");
    expect(states(phone(null))).toEqual(["ok", "wait", "todo", "todo"]);
    expect(checklistAnnouncement(checklist)).toBe("HP terhubung. Lanjutkan pengaturan di HP.");
  });

  it("treats permissions reported off before the self-test as still being set up", () => {
    const device = phone(healthReport({
      notification_access: false, listener_connected: false, self_test: { status: "not_run", at: null },
    }));

    expect(states(device)).toEqual(["ok", "wait", "todo", "todo"]);
    expect(pairingChecklist(device).stage).toBe("setting-up");
  });

  it("advances step by step and announces the last finished one", () => {
    const access = phone(healthReport({ listener_connected: false, self_test: { status: "not_run", at: null } }));
    const background = phone(healthReport({ self_test: { status: "not_run", at: null } }));

    expect(states(access)).toEqual(["ok", "ok", "wait", "todo"]);
    expect(checklistAnnouncement(pairingChecklist(access))).toBe("Izin notifikasi selesai.");
    expect(states(background)).toEqual(["ok", "ok", "ok", "wait"]);
    expect(checklistAnnouncement(pairingChecklist(background))).toBe("Jalan di latar belakang selesai.");
  });

  it.each([
    ["post", "HP tidak dapat menampilkan notifikasi tes."],
    ["capture", "Notifikasi tes tidak tertangkap oleh aplikasi."],
    ["server", "HP belum dapat mengirim ke server."],
  ] as const)("names the failed self-test step %s", (step, text) => {
    const checklist = pairingChecklist(phone(healthReport({ self_test: { status: "failed", failed_step: step } })));

    expect(checklist.stage).toBe("failed");
    expect(checklist.failed?.key).toBe("selfTest");
    expect(checklist.selfTestStep).toBe(step);
    expect(checklistAnnouncement(checklist)).toBe(`Tes tangkap notifikasi gagal. ${text} ${FIX}`);
  });

  it("fails a permission turned off after the self-test ran", () => {
    const access = pairingChecklist(phone(healthReport({ notification_access: false })));
    const background = pairingChecklist(phone(healthReport({ listener_connected: false })));

    expect(access.steps.map((step) => step.state)).toEqual(["ok", "fail", "ok", "ok"]);
    expect(access.failed?.key).toBe("notification");
    expect(checklistAnnouncement(access)).toBe(`Izin notifikasi belum berjalan. ${FIX}`);
    expect(background.failed?.key).toBe("background");
  });

  it("is ready on the server's rule, with the battery hint kept as advice only", () => {
    const checklist = pairingChecklist(phone(healthReport({ battery_unrestricted: false })));

    expect(checklist.stage).toBe("ready");
    expect(checklist.batteryRestricted).toBe(true);
    expect(checklist.steps.every((step) => step.state === "ok")).toBe(true);
    expect(checklistAnnouncement(checklist)).toBe(READY_MESSAGE);
  });
});

describe("device problems", () => {
  it("lists nothing for a ready phone that reported recently", () => {
    expect(deviceProblems(healthReport(), NOW)).toEqual([]);
  });

  it("lists every problem, most actionable first", () => {
    const health = healthReport({
      notification_access: false,
      listener_connected: false,
      self_test: { status: "failed", failed_step: "server" },
      reported_at: new Date(NOW - 25 * 3600_000).toISOString(),
    });

    expect(deviceProblems(health, NOW)).toEqual(["notificationOff", "listenerOff", "selfTestFailed", "silent"]);
    expect(deviceProblems(health, NOW).map((problem) => problemText(problem, health, words))).toEqual([
      "Izin notifikasi mati.",
      "Aplikasi tidak berjalan di latar belakang.",
      "Tes tangkap notifikasi gagal. HP belum dapat mengirim ke server.",
      "Lebih dari 24 jam tidak ada kabar dari HP ini.",
    ]);
  });

  it("names a self-test that never ran and keeps a 23-hour-old report fresh", () => {
    const health = healthReport({
      self_test: { status: "not_run", at: null },
      reported_at: new Date(NOW - 23 * 3600_000).toISOString(),
    });

    expect(deviceProblems(health, NOW).map((problem) => problemText(problem, health, words))).toEqual(["Tes tangkap notifikasi belum dijalankan."]);
  });
});
