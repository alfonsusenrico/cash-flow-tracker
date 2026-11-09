"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { createPairingCode, getDevices, type PairedDevice } from "@/lib/api";
import { checklistAnnouncement, pairingChecklist } from "@/lib/deviceHealth";
import { queryKeys } from "@/lib/queryKeys";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { CompanionDownloadDisclosure } from "@/components/settings/CompanionDownload";
import { DeviceHealthChecklist } from "@/components/settings/DeviceHealthChecklist";
import { QrCode } from "@/components/settings/QrCode";

/**
 * The pairing code's side in css px. The owner kept it at 200 px or more (2026-11-08), above the prototype's 118 px,
 * because a real phone must scan it reliably; the white box adds an 8 px quiet border.
 */
export const PAIRING_QR_SIZE = 200;
export const DEVICE_POLL_MS = 3000;
/** The phone may take a while to walk the owner through permissions and its self-test. */
export const DEVICE_POLL_WINDOW_MS = 10 * 60 * 1000;
/** The countdown is announced once when this much time is left (D9). */
const LAST_MINUTE_S = 60;

interface IssuedCode {
  code: string;
  payload: string;
  /** Local clock deadline, so a skewed browser clock cannot shorten or extend the countdown. */
  deadline: number;
  /** Local clock end of polling for this code's phone. */
  pollUntil: number;
  /** Server time the code was issued; compared only with other server timestamps. */
  issuedAt: number;
}

function formatCountdown(seconds: number) {
  const minutes = Math.floor(seconds / 60);
  return `${String(minutes).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`;
}

function groupCode(code: string) {
  return `${code.slice(0, 5)} ${code.slice(5)}`;
}

function pairedSince(devices: PairedDevice[] | undefined, issuedAt: number) {
  return devices?.find((device) => device.created_via === "pairing" && Date.parse(device.created_at) >= issuedAt);
}

/**
 * "Hubungkan ponsel" (apply-markas-pengaturan D9): a centered 520 px `ShellDialog`, a sheet below 768 px. Opening it
 * issues one code; the device list is polled every 3 s for at most 10 minutes and stops on ready, at the end of the
 * window or on close. A new code cancels the previous one on the server.
 */
export function DevicePairingDialog({ onClose }: { onClose: () => void }) {
  const t = useTranslations("settings.pairing");
  const health = useTranslations("settings.health");
  const common = useTranslations("common");
  const [issued, setIssued] = useState<IssuedCode | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [qrFailed, setQrFailed] = useState(false);
  const [pending, setPending] = useState(false);
  const [requestError, setRequestError] = useState("");
  const latestRequest = useRef(0);

  const requestCode = useCallback(async () => {
    // Only the newest request may show its code: an older response would be a superseded, dead code.
    const request = ++latestRequest.current;
    setPending(true);
    setRequestError("");
    try {
      const data = await createPairingCode();
      if (request !== latestRequest.current) return;
      const issuedLocally = Date.now();
      setIssued({
        code: data.code,
        payload: data.qr_payload,
        deadline: issuedLocally + data.expires_in_seconds * 1000,
        pollUntil: issuedLocally + DEVICE_POLL_WINDOW_MS,
        issuedAt: Date.parse(data.expires_at) - data.expires_in_seconds * 1000,
      });
      setNow(issuedLocally);
      setQrFailed(false);
    } catch (error) {
      if (request !== latestRequest.current) return;
      setIssued(null);
      setRequestError((error instanceof Error && error.message) || t("failed"));
    } finally {
      if (request === latestRequest.current) setPending(false);
    }
  }, [t]);

  useEffect(() => {
    // The ref survives React's development double-mount, so opening the dialog issues one code.
    if (latestRequest.current === 0) void requestCode();
  }, [requestCode]);

  const { data: deviceData } = useQuery({
    queryKey: queryKeys.devices,
    queryFn: getDevices,
    refetchInterval: (query) => {
      if (!issued || Date.now() >= issued.pollUntil) return false;
      return pairedSince(query.state.data?.devices, issued.issuedAt)?.health?.ready ? false : DEVICE_POLL_MS;
    },
  });
  const paired = issued ? pairedSince(deviceData?.devices, issued.issuedAt) : undefined;
  const ready = paired?.health?.ready === true;
  const remaining = issued ? Math.max(0, Math.ceil((issued.deadline - now) / 1000)) : 0;
  const expired = issued !== null && remaining === 0;
  const pollingOver = issued !== null && now >= issued.pollUntil;
  const active = issued !== null && !expired && !paired ? issued : null;
  const checklist = pairingChecklist(paired);

  useEffect(() => {
    // The clock drives the countdown and the end of the polling window; it stops once neither can change.
    if (!issued || ready || pollingOver) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [issued, ready, pollingOver]);

  const gaveUp = paired !== undefined && !ready && pollingOver;
  let announcement = "";
  if (pending) announcement = t("creating");
  else if (paired) announcement = checklistAnnouncement(checklist, health, t("ready"), t("fixOnPhone"));
  else if (expired) announcement = t("expiredAnnounce");
  else if (active && remaining <= LAST_MINUTE_S) announcement = t("oneMinute");

  let emptyText = t("creating");
  if (requestError) emptyText = t("failedShort");
  else if (expired) emptyText = t("expired");

  const footer = paired ? (
    <button type="button" className="ws-btn" onClick={onClose}>
      {t("done")}
    </button>
  ) : (
    <>
      <button type="button" className="ws-btn" onClick={onClose}>
        {t("later")}
      </button>
      <button type="button" className="ws-btn" disabled={pending} onClick={() => void requestCode()}>
        {t("newCode")}
      </button>
    </>
  );

  return (
    <ShellDialog open onClose={onClose} title={t("title")} meta={t("meta")} width={520} closeLabel={common("close")} footer={footer} initialFocus=".pair-body">
      {/* The body takes focus on opening, so the steps are read before the download disclosure. */}
      <div className="pair-body" tabIndex={-1}>
        {!paired && (
          <>
            <div className="set-qr-row">
              {active ? (
                <div className="qr is-pairing">
                  <QrCode
                    value={active.payload}
                    size={PAIRING_QR_SIZE}
                    label={t("qr", { code: groupCode(active.code) })}
                    onError={() => setQrFailed(true)}
                  />
                </div>
              ) : (
                <div className="qr is-pairing is-empty">{emptyText}</div>
              )}
              {active && (
                <ol className="pair-steps grow" aria-label={t("steps")}>
                  <li className="t-body">{t("step1")}</li>
                  <li className="t-body">{t("step2")}</li>
                  <li className="t-body tabular-nums">{t("step3", { time: formatCountdown(remaining) })}</li>
                </ol>
              )}
            </div>
            {qrFailed && <p role="alert" className="set-err">{t("qrFailed")}</p>}
            {requestError && <p role="alert" className="set-err">{requestError}</p>}
            {active && (
              <div className="set-box">
                <p className="t-meta">{t("typeCode")}</p>
                <p className="t-fig pair-code">{groupCode(active.code)}</p>
              </div>
            )}
            <p className="t-meta">{t("rule")}</p>
            <CompanionDownloadDisclosure />
          </>
        )}

        {paired && <DeviceHealthChecklist checklist={checklist} device={paired} />}
        {gaveUp && <p className="t-body">{t("stopped")}</p>}

        <p role="status" aria-live="polite" className="sr-only">
          {announcement}
        </p>
      </div>
    </ShellDialog>
  );
}
