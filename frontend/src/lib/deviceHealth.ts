import type { useTranslations } from "next-intl";
import type { DeviceHealth, PairedDevice, SelfTestFailedStep } from "@/lib/api";

export type ChecklistStepKey = "connected" | "notification" | "background" | "selfTest";
/** done, failed, being set up on the phone right now, not reached yet */
export type ChecklistStepState = "ok" | "fail" | "wait" | "todo";

export interface ChecklistStep {
  key: ChecklistStepKey;
  state: ChecklistStepState;
}

export interface PairingChecklist {
  stage: "waiting" | "setting-up" | "failed" | "ready";
  steps: ChecklistStep[];
  /** The first failed step, named in the header. */
  failed: ChecklistStep | null;
  /** The step the phone says failed in its self-test, explained under the step. */
  selfTestStep: SelfTestFailedStep | null;
  /** Advisory only: a battery-restricted phone can still be ready. */
  batteryRestricted: boolean;
}

/** The words of the checklist and the problems: `settings.health` (apply-markas-pengaturan D19). */
export type HealthWords = ReturnType<typeof useTranslations<"settings.health">>;

export const STEP_KEYS: ChecklistStepKey[] = ["connected", "notification", "background", "selfTest"];

/** A report older than this means the phone has gone quiet (frozen, uninstalled or offline). */
export const STALE_REPORT_MS = 24 * 60 * 60 * 1000;

type Known = boolean | null;

function failedSelfTestStep(health: DeviceHealth | null): SelfTestFailedStep | null {
  return health?.self_test.status === "failed" ? health.self_test.failed_step ?? null : null;
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
      steps: STEP_KEYS.map((key) => ({ key, state: "todo" })),
      failed: null,
      selfTestStep: null,
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
    return { key, state };
  });
  const failed = steps.find((step) => step.state === "fail") ?? null;
  let stage: PairingChecklist["stage"] = "setting-up";
  if (health?.ready) stage = "ready";
  else if (failed) stage = "failed";

  return {
    stage,
    steps,
    failed,
    selfTestStep: failedSelfTestStep(health),
    batteryRestricted: health?.battery_unrestricted === false,
  };
}

/** One sentence for the polite live region; it changes only when the checklist changes. */
export function checklistAnnouncement(checklist: PairingChecklist, t: HealthWords, ready: string, fix: string): string {
  if (checklist.stage === "waiting") return "";
  if (checklist.stage === "ready") return ready;
  if (checklist.failed?.key === "selfTest") {
    const problem = checklist.selfTestStep ? t(`testStep.${checklist.selfTestStep}`) : "";
    return t("announceSelfTestFailed", { problem, fix }).replace(/\s+/g, " ").trim();
  }
  if (checklist.failed) return t("announceStepFailed", { step: t(`steps.${checklist.failed.key}`), fix });
  const lastDone = [...checklist.steps].reverse().find((step) => step.state === "ok");
  if (!lastDone || lastDone.key === "connected") return t("announceConnected");
  return t("announceStepDone", { step: t(`steps.${lastDone.key}`) });
}

export function isStale(health: DeviceHealth, now: number) {
  return now - Date.parse(health.reported_at) > STALE_REPORT_MS;
}

/** A phone's problem, as a code its readers word (apply-markas-pengaturan D7, D19). */
export type DeviceProblem = "notificationOff" | "listenerOff" | "selfTestFailed" | "selfTestNotRun" | "silent";

/** The problems of one phone, most actionable first. */
export function deviceProblems(health: DeviceHealth, now: number): DeviceProblem[] {
  const problems: DeviceProblem[] = [];
  if (!health.notification_access) problems.push("notificationOff");
  if (!health.listener_connected) problems.push("listenerOff");
  if (health.self_test.status === "failed") problems.push("selfTestFailed");
  else if (health.self_test.status === "not_run") problems.push("selfTestNotRun");
  if (isStale(health, now)) problems.push("silent");
  return problems;
}

/** A problem in words; a failed self-test adds the step the phone reported. */
export function problemText(problem: DeviceProblem, health: DeviceHealth, t: HealthWords): string {
  if (problem !== "selfTestFailed") return t(problem);
  const step = failedSelfTestStep(health);
  return step ? `${t("selfTestFailed")} ${t(`testStep.${step}`)}` : t("selfTestFailed");
}
