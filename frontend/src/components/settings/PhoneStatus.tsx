"use client";

import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { getDevices, type DeviceHealth, type PairedDevice } from "@/lib/api";
import { deviceProblems, isStale, problemText, type DeviceProblem } from "@/lib/deviceHealth";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { Icon } from "@/components/ui/Icon";
import { SectionHeader } from "@/components/workspace/lists";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { CompanionStatus } from "@/components/workspace/CompanionStatus";
import { pairedDevices } from "@/components/settings/DevicePairingSection";

/** The four checks of a phone and the problem that fails each (apply-markas-pengaturan D7). */
const CHECKS: { key: "notificationAccess" | "background" | "selfTest" | "contact"; problems: DeviceProblem[] }[] = [
  { key: "notificationAccess", problems: ["notificationOff"] },
  { key: "background", problems: ["listenerOff"] },
  { key: "selfTest", problems: ["selfTestFailed", "selfTestNotRun"] },
  { key: "contact", problems: ["silent"] },
];

function PhoneBlock({ device, checkedAt }: { device: PairedDevice; checkedAt: number }) {
  const t = useTranslations("settings.devices");
  const health = useTranslations("settings.health");
  const words = useTranslations("settings.common");
  const format = useFormat();
  const report: DeviceHealth | null = device.health;
  const ready = report ? report.ready && !isStale(report, checkedAt) : false;
  const problems = report ? deviceProblems(report, checkedAt) : [];
  return (
    <div className="set-phone">
      <div className="set-phone-h">
        <Icon name="smartphone" className="h-5 w-5" />
        <div className="grow">
          <p className="t-strong">
            {device.name} · <span className={ready ? "c-income" : "c-warning"}>{ready ? t("ready") : t("needsCheck")}</span>
          </p>
          <p className="t-meta">
            {report
              ? t("lastReport", {
                  time: words("dateTime", { date: format.yearDateAt(report.reported_at), time: format.time(report.reported_at) }),
                  version: report.app_version,
                })
              : t("neverReported")}
          </p>
        </div>
      </div>
      {report && (
        <ul className="set-checks" aria-label={t("checks", { name: device.name })}>
          {CHECKS.map((check) => {
            const problem = problems.find((found) => check.problems.includes(found));
            return (
              <li key={check.key}>
                <Icon name={problem ? "alert" : "check"} className={`h-4 w-4 ${problem ? "c-warning" : "c-income"}`} />
                <span className="t-body">{health(check.key)}</span>
                <span className="t-meta">{problem ? problemText(problem, report, health) : health("ok")}</span>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

/**
 * "Status HP" (apply-markas-pengaturan D7): the companion line, then one block per paired phone with its readiness,
 * last report, app version and four checks in words. Without a phone only the companion line shows.
 */
export function PhoneStatus() {
  const t = useTranslations("settings.devices");
  // Staleness is judged against the moment the list was fetched, so a render never reads the clock.
  const { data, dataUpdatedAt, isError, isPending, refetch } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices });
  const devices = pairedDevices(data?.devices);
  return (
    <section aria-labelledby="phone-status-title">
      <SectionHeader id="phone-status-title" label={t("status")} />
      {/* A failed device list says so instead of an empty box (D21). */}
      {isError ? (
        <ErrorState message={t("loadFailed")} onRetry={() => void refetch()} />
      ) : isPending ? (
        <div aria-busy="true">
          <Skeleton style={{ height: 112 }} />
          <p role="status" className="sr-only">
            {t("loading")}
          </p>
        </div>
      ) : (
        <div className="set-box">
          <div className="set-companion">
            <CompanionStatus />
          </div>
          {devices.map((device) => (
            <PhoneBlock key={device.id} device={device} checkedAt={dataUpdatedAt} />
          ))}
        </div>
      )}
    </section>
  );
}
