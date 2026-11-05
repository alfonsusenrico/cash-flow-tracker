"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import QRCode from "qrcode";
import { useQuery } from "@tanstack/react-query";
import { createPairingCode, getDevices, type PairedDevice } from "@/lib/api";
import { checklistAnnouncement, pairingChecklist } from "@/lib/deviceHealth";
import { queryKeys } from "@/lib/queryKeys";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { CompanionDownloadDisclosure } from "@/components/ui/CompanionDownload";
import { DeviceHealthChecklist } from "@/components/ui/DeviceHealthChecklist";

const QR_SIZE = 240;
export const DEVICE_POLL_MS = 3000;
/** The phone may take a while to walk the owner through permissions and its self-test. */
export const DEVICE_POLL_WINDOW_MS = 10 * 60 * 1000;

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

export function DevicePairingDialog({ onClose }: { onClose: () => void }) {
  const [issued, setIssued] = useState<IssuedCode | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [qrFailed, setQrFailed] = useState(false);
  const canvasRef = useRef<HTMLCanvasElement>(null);
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
      setRequestError((error instanceof Error && error.message) || "Kodenya gagal dibuat. Coba lagi, ya.");
    } finally {
      if (request === latestRequest.current) setPending(false);
    }
  }, []);

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

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!active || !canvas) return;
    let cancelled = false;
    // Scanners need dark modules on a light quiet zone, so the code stays black on white in both themes.
    QRCode.toCanvas(canvas, active.payload, {
      width: QR_SIZE,
      margin: 2,
      errorCorrectionLevel: "M",
      color: { dark: "#000000", light: "#ffffff" },
    }).catch(() => {
      if (!cancelled) setQrFailed(true);
    });
    return () => {
      cancelled = true;
    };
  }, [active]);

  const gaveUp = paired !== undefined && !ready && pollingOver;
  let announcement = "";
  if (pending) announcement = "Membuat kode…";
  else if (paired) announcement = checklistAnnouncement(checklist);
  else if (expired) announcement = "Kode sudah kedaluwarsa. Buat kode baru untuk menyambungkan HP.";

  return (
    <Modal open onClose={onClose} title="Hubungkan ponsel" centered>
      <div className="space-y-4 text-xs">
        {!paired && (
          <>
            <p className="text-[var(--muted)]">
              Buka aplikasi pendamping di HP, pilih pindai kode QR, terus arahin kamera ke kode ini.
            </p>

            <div className="flex flex-col items-center gap-3">
              {active ? (
                <>
                  <div className="rounded-xl border border-[var(--line)] bg-white p-2">
                    <canvas
                      ref={canvasRef}
                      width={QR_SIZE}
                      height={QR_SIZE}
                      role="img"
                      aria-label={`Kode QR pemasangan ponsel, kode ${groupCode(active.code)}`}
                      className="block h-60 w-60"
                    />
                  </div>
                  {qrFailed && (
                    <p role="alert" className="text-center text-[var(--out-fg)]">
                      Kode QR gagal ditampilkan. Ketik kode di bawah ini di HP.
                    </p>
                  )}
                  <p className="text-center text-[var(--muted)]">
                    Kode{" "}
                    <span className="tabular-nums text-base font-semibold tracking-widest text-[var(--text)]">
                      {groupCode(active.code)}
                    </span>
                  </p>
                  <p role="timer" className="font-semibold text-[var(--text)]">
                    Berlaku <span className="tabular-nums">{formatCountdown(remaining)}</span>
                  </p>
                </>
              ) : (
                <div className="flex h-64 w-64 max-w-full items-center justify-center rounded-xl border border-dashed border-[var(--line-strong)] p-4 text-center font-semibold text-[var(--text)]">
                  {requestError ? "Kode gagal dibuat." : expired ? "Kode sudah kedaluwarsa." : "Membuat kode…"}
                </div>
              )}
              {requestError && (
                <p role="alert" className="text-center text-[var(--out-fg)]">{requestError}</p>
              )}
            </div>
          </>
        )}

        {(paired || active) && <DeviceHealthChecklist checklist={checklist} device={paired} />}

        {gaveUp && (
          <p className="text-[var(--text)]">
            Aku berhenti ngecek dulu. Lanjutin di aplikasi HP-mu, ya. Statusnya tetap kelihatan di daftar perangkat.
          </p>
        )}

        <p role="status" aria-live="polite" className="sr-only">
          {announcement}
        </p>

        {!paired && (
          <>
            <p className="text-xs text-[var(--muted)]">
              Kode hanya dapat dipakai sekali dan berlaku 5 menit. Membuat kode baru membatalkan kode sebelumnya.
            </p>
            <CompanionDownloadDisclosure />
          </>
        )}

        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-[var(--line)] pt-3">
          {ready ? (
            <Button type="button" variant="primary" size="sm" className="min-h-11" onClick={onClose}>
              Selesai
            </Button>
          ) : paired ? (
            <Button type="button" variant="secondary" size="sm" className="min-h-11" onClick={onClose}>
              Nanti saja
            </Button>
          ) : (
            <>
              <Button type="button" variant="secondary" size="sm" className="min-h-11" onClick={onClose}>
                Tutup
              </Button>
              <Button
                type="button"
                variant="primary"
                size="sm"
                className="min-h-11"
                disabled={pending}
                onClick={() => void requestCode()}
              >
                Buat kode baru
              </Button>
            </>
          )}
        </div>
      </div>
    </Modal>
  );
}
