import type { DeviceHealth, PairedDevice, SelfTestFailedStep } from "@/lib/api";

export type ChecklistStepKey = "connected" | "notification" | "background" | "selfTest";
/** done, failed, being set up on the phone right now, not reached yet */
export type ChecklistStepState = "ok" | "fail" | "wait" | "todo";

export interface ChecklistStep {
  key: ChecklistStepKey;
  label: string;
  state: ChecklistStepState;
}

export interface PairingChecklist {
  stage: "waiting" | "setting-up" | "failed" | "ready";
  steps: ChecklistStep[];
  /** The first failed step, named in the header. */
  failed: ChecklistStep | null;
  /** Explanation of a failed self-test, from the step the phone says failed. */
  selfTestProblem: string | null;
  /** Advisory only: a battery-restricted phone can still be ready. */
  batteryRestricted: boolean;
}

export const STEP_LABELS: Record<ChecklistStepKey, string> = {
  connected: "Terhubung",
  notification: "Izin notifikasi",
  background: "Jalan di latar belakang",
  selfTest: "Tes tangkap notifikasi",
};

export const STEP_STATE_TEXT: Record<ChecklistStepState, string> = {
  ok: "Selesai",
  fail: "Belum berjalan",
  wait: "Sedang diatur di HP",
  todo: "Belum",
};

export const READY_MESSAGE = "HP-mu siap! Mulai sekarang transaksi tercatat sendiri.";
export const FIX_ON_PHONE = "Tenang, aplikasi di HP-mu bakal nunjukin cara benerinnya.";
export const BATTERY_HINT =
  "Biar nggak dimatiin diam-diam, izinkan aplikasinya jalan tanpa batasan baterai. Di HP TECNO/Infinix (HiOS) atau Xiaomi (MIUI), biasanya perlu izin tambahan biar aplikasinya nggak dibekukan.";

export const SELF_TEST_STEP_TEXT: Record<SelfTestFailedStep, string> = {
  post: "HP tidak dapat menampilkan notifikasi tes.",
  capture: "Notifikasi tes tidak tertangkap oleh aplikasi.",
  server: "HP belum bisa ngirim ke server.",
};

/** A report older than this means the phone has gone quiet (frozen, uninstalled or offline). */
export const STALE_REPORT_MS = 24 * 60 * 60 * 1000;

type Known = boolean | null;

function selfTestProblem(health: DeviceHealth | null) {
  const step = health?.self_test.status === "failed" ? health.self_test.failed_step : null;
  return step ? SELF_TEST_STEP_TEXT[step] : null;
}

/**
 * Derives the pairing checklist from the newest device list, so a missed poll or a reopened dialog
 * shows the same thing. Before the self-test has run, a permission reported off means the owner is
 * still granting it on the phone, so it shows as in progress rather than failed.
 */
export function pairingChecklist(device: PairedDevice | undefined): PairingChecklist {
  if (!device) {
    return {
      stage: "waiting",
      steps: (Object.keys(STEP_LABELS) as ChecklistStepKey[]).map((key) => ({ key, label: STEP_LABELS[key], state: "todo" })),
      failed: null,
      selfTestProblem: null,
      batteryRestricted: false,
    };
  }

  const health = device.health;
  const testStatus = health?.self_test.status ?? null;
  const testRan = testStatus === "passed" || testStatus === "failed";
  const known = (value: boolean | undefined): Known => {
    if (value === true) return true;
    return value === false && testRan ? false : null;
  };
  const results: [ChecklistStepKey, Known][] = [
    ["connected", true],
    ["notification", known(health?.notification_access)],
    ["background", known(health?.listener_connected)],
    ["selfTest", testStatus === "passed" ? true : testStatus === "failed" ? false : null],
  ];
  const firstOpen = results.find(([, result]) => result === null)?.[0];
  const steps = results.map(([key, result]): ChecklistStep => {
    let state: ChecklistStepState = "todo";
    if (result === true) state = "ok";
    else if (result === false) state = "fail";
    else if (key === firstOpen) state = "wait";
    return { key, label: STEP_LABELS[key], state };
  });
  const failed = steps.find((step) => step.state === "fail") ?? null;
  let stage: PairingChecklist["stage"] = "setting-up";
  if (health?.ready) stage = "ready";
  else if (failed) stage = "failed";

  return {
    stage,
    steps,
    failed,
    selfTestProblem: selfTestProblem(health),
    batteryRestricted: health?.battery_unrestricted === false,
  };
}

/** One sentence for the polite live region; it changes only when the checklist changes. */
export function checklistAnnouncement(checklist: PairingChecklist): string {
  if (checklist.stage === "waiting") return "";
  if (checklist.stage === "ready") return READY_MESSAGE;
  if (checklist.failed?.key === "selfTest") {
    return `Tes tangkap notifikasi gagal. ${checklist.selfTestProblem ?? ""} ${FIX_ON_PHONE}`.replace(/\s+/g, " ").trim();
  }
  if (checklist.failed) return `${checklist.failed.label} belum jalan. ${FIX_ON_PHONE}`;
  const lastDone = [...checklist.steps].reverse().find((step) => step.state === "ok");
  if (!lastDone || lastDone.key === "connected") return "HP terhubung. Lanjutkan pengaturan di HP.";
  return `${lastDone.label} selesai.`;
}

export function isStale(health: DeviceHealth, now: number) {
  return now - Date.parse(health.reported_at) > STALE_REPORT_MS;
}

/** Problems Settings lists for one phone, most actionable first. */
export function deviceProblems(health: DeviceHealth, now: number): string[] {
  const problems: string[] = [];
  if (!health.notification_access) problems.push("Izin notifikasi mati.");
  if (!health.listener_connected) problems.push("Aplikasi tidak berjalan di latar belakang.");
  if (health.self_test.status === "failed") {
    problems.push(`Tes tangkap notifikasi gagal. ${selfTestProblem(health) ?? ""}`.trim());
  } else if (health.self_test.status === "not_run") {
    problems.push("Tes tangkap notifikasi belum dijalankan.");
  }
  if (isStale(health, now)) problems.push("Lebih dari 24 jam tidak ada kabar dari HP ini.");
  return problems;
}
