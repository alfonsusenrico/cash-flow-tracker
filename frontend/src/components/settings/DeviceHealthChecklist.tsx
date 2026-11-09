"use client";

import { useTranslations } from "next-intl";
import type { PairedDevice } from "@/lib/api";
import type { ChecklistStepState, PairingChecklist } from "@/lib/deviceHealth";
import { Icon } from "@/components/ui/Icon";

/** Each state's icon and ink; the status word beside it carries the meaning, never the colour alone (D9). */
const STATE_MARK: Record<ChecklistStepState, { icon: string; className: string }> = {
  ok: { icon: "check", className: "c-income" },
  fail: { icon: "alert", className: "c-warning" },
  wait: { icon: "smartphone", className: "c-warning" },
  todo: { icon: "minus-circle", className: "text-[var(--muted)]" },
};

/**
 * The pairing dialog's live checklist (apply-markas-pengaturan D9): four 32 px lines with an icon and a status word,
 * on the dialog surface without tinted fills. Announcements are made by the dialog's live region.
 */
export function DeviceHealthChecklist({ checklist, device }: { checklist: PairingChecklist; device: PairedDevice | undefined }) {
  const t = useTranslations("settings.health");
  const pairing = useTranslations("settings.pairing");
  const devices = useTranslations("settings.devices");
  let headline = t("waiting");
  let detail: string | null = t("waitingDetail");
  if (checklist.stage === "setting-up") {
    headline = t("settingUp");
    detail = device ? t("settingUpDetail", { name: device.name }) : null;
  } else if (checklist.stage === "failed" && checklist.failed) {
    headline = t("failedHeadline", { step: t(`steps.${checklist.failed.key}`) });
    detail = null;
  } else if (checklist.stage === "ready") {
    headline = pairing("ready");
    detail = device?.health ? t("readyDetail", { name: device.name, version: device.health.app_version }) : null;
  }

  return (
    <section aria-labelledby="device-health-title" className="set-box">
      <h3 id="device-health-title" className="t-label">
        {devices("status")}
      </h3>
      <p className="t-strong set-note">{headline}</p>
      {detail && <p className="t-meta set-note">{detail}</p>}
      <ul className="set-checks" aria-label={t("stepsLabel")}>
        {checklist.steps.map((step) => {
          const mark = STATE_MARK[step.state];
          const showBattery = step.key === "background" && checklist.batteryRestricted;
          const showTestProblem = step.key === "selfTest" && step.state === "fail" && checklist.selfTestStep;
          return (
            <li key={step.key} data-state={step.state}>
              <Icon name={mark.icon} className={`h-4 w-4 ${mark.className}`} />
              <span className="t-body">{t(`steps.${step.key}`)}</span>
              <span className="t-meta">{t(`stepState.${step.state}`)}</span>
              {showTestProblem && checklist.selfTestStep && <span className="t-meta set-check-note">{t(`testStep.${checklist.selfTestStep}`)}</span>}
              {showBattery && (
                <span className="t-meta set-check-note">
                  <span className="t-strong">{t("batteryTip")}</span> {t("batteryHint")}
                </span>
              )}
            </li>
          );
        })}
      </ul>
      {checklist.stage === "failed" && <p className="t-body set-note">{pairing("fixOnPhone")}</p>}
    </section>
  );
}
