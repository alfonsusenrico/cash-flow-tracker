"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import QRCode from "qrcode";
import { useQuery } from "@tanstack/react-query";
import { createPairingCode, getDevices, type PairedDevice } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";

const QR_SIZE = 240;
const DEVICE_POLL_MS = 3000;

interface IssuedCode {
  code: string;
  payload: string;
  /** Local clock deadline, so a skewed browser clock cannot shorten or extend the countdown. */
  deadline: number;
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
  const [remaining, setRemaining] = useState(0);
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
      setIssued({
        code: data.code,
        payload: data.qr_payload,
        deadline: Date.now() + data.expires_in_seconds * 1000,
        issuedAt: Date.parse(data.expires_at) - data.expires_in_seconds * 1000,
      });
      setRemaining(data.expires_in_seconds);
      setQrFailed(false);
    } catch (error) {
      if (request !== latestRequest.current) return;
      setIssued(null);
      setRequestError((error instanceof Error && error.message) || "Kode gagal dibuat. Coba lagi.");
    } finally {
      if (request === latestRequest.current) setPending(false);
    }
  }, []);

  useEffect(() => {
    // The ref survives React's development double-mount, so opening the dialog issues one code.
    if (latestRequest.current === 0) void requestCode();
  }, [requestCode]);

  const expired = issued !== null && remaining === 0;

  useEffect(() => {
    if (!issued) return;
    const tick = () => setRemaining(Math.max(0, Math.ceil((issued.deadline - Date.now()) / 1000)));
    tick();
    const timer = window.setInterval(tick, 1000);
    return () => window.clearInterval(timer);
  }, [issued]);

  const { data: deviceData } = useQuery({
    queryKey: queryKeys.devices,
    queryFn: getDevices,
    refetchInterval: (query) => {
      if (!issued || expired) return false;
      return pairedSince(query.state.data?.devices, issued.issuedAt) ? false : DEVICE_POLL_MS;
    },
  });
  const paired = issued ? pairedSince(deviceData?.devices, issued.issuedAt) : undefined;
  const active = issued !== null && !expired && !paired ? issued : null;

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

  let status = "";
  if (pending) status = "Membuat kode pemasangan…";
  else if (paired) status = `${paired.name} terhubung.`;
  else if (expired) status = "Kode kedaluwarsa. Buat kode baru untuk memasangkan ponsel.";

  return (
    <Modal open onClose={onClose} title="Hubungkan ponsel" centered>
      <div className="space-y-4 text-xs">
        <p className="text-[var(--muted)]">
          Buka aplikasi pendamping di ponsel, pilih pindai kode QR, lalu arahkan kamera ke kode ini.
        </p>

        <div className="flex flex-col items-center gap-3">
          {active ? (
            <>
              <div className="rounded-[var(--radius-md)] border border-[var(--border)] bg-white p-2">
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
                <p role="alert" className="text-center text-[var(--danger-text)]">
                  Kode QR gagal ditampilkan. Ketik kode di bawah pada ponsel.
                </p>
              )}
              <p className="text-center text-[var(--muted)]">
                Kode{" "}
                <span className="font-mono text-base font-bold tracking-widest text-[var(--text)]">
                  {groupCode(active.code)}
                </span>
              </p>
              <p role="timer" className="font-semibold text-[var(--text)]">
                Berlaku <span className="tabular-nums">{formatCountdown(remaining)}</span>
              </p>
            </>
          ) : (
            <div className="flex h-64 w-64 max-w-full items-center justify-center rounded-[var(--radius-md)] border border-dashed border-[var(--border-strong)] p-4 text-center font-semibold text-[var(--text)]">
              {requestError
                ? "Kode gagal dibuat."
                : paired
                  ? "Ponsel sudah terhubung."
                  : expired
                    ? "Kode kedaluwarsa."
                    : "Membuat kode…"}
            </div>
          )}
          <p role="status" className="min-h-4 text-center font-semibold text-[var(--text)]">{status}</p>
          {requestError && (
            <p role="alert" className="text-center text-[var(--danger-text)]">{requestError}</p>
          )}
        </div>

        <p className="text-[11px] text-[var(--muted)]">
          Kode hanya bisa dipakai sekali dan berlaku 5 menit. Membuat kode baru membatalkan kode sebelumnya.
        </p>

        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-[var(--border)] pt-3">
          {paired ? (
            <Button type="button" variant="primary" size="sm" className="min-h-11" onClick={onClose}>
              Selesai
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
