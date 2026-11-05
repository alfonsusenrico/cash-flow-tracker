"use client";

import { useEffect, useRef, useState } from "react";
import QRCode from "qrcode";
import { Icon } from "@/components/ui/Icon";
import { SettingsSubsection } from "@/components/settings/SettingsCard";

/** Always the newest signed release: the Android release workflow keeps it marked latest. */
export const COMPANION_DOWNLOAD_URL =
  "https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk";
export const COMPANION_RELEASE_URL = "https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest";
/** minSdk 26 in mobile/app/build.gradle.kts. */
export const COMPANION_MIN_ANDROID = "Android 8.0";

const QR_SIZE = 160;
const linkClass =
  "font-semibold text-[var(--text)] underline underline-offset-2";

function DownloadDetails() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [qrFailed, setQrFailed] = useState(false);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    let cancelled = false;
    // Scanners need dark modules on a light quiet zone, so the code stays black on white in both themes.
    QRCode.toCanvas(canvas, COMPANION_DOWNLOAD_URL, {
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
  }, []);

  return (
    <div className="flex flex-wrap items-start gap-4 text-xs">
      <div className="shrink-0 rounded-xl border border-[var(--line)] bg-white p-2">
        <canvas
          ref={canvasRef}
          width={QR_SIZE}
          height={QR_SIZE}
          role="img"
          aria-label="Kode QR tautan unduhan aplikasi pendamping"
          className="block h-40 w-40"
        />
      </div>
      <div className="min-w-0 flex-1 basis-56 space-y-2 text-[var(--text-2)]">
        <p>Pindai dengan kamera ponsel untuk mengunduh, atau buka tautan ini di ponsel.</p>
        {qrFailed && (
          <p role="alert" className="text-[var(--out-fg)]">
            Kode QR gagal ditampilkan. Pakai tautan unduhan.
          </p>
        )}
        <p>
          <a href={COMPANION_DOWNLOAD_URL} className={`${linkClass} inline-flex min-h-11 items-center gap-1 break-all`}>
            <Icon name="download" className="h-4 w-4 shrink-0" />
            Unduh cashflow-companion.apk
          </a>
        </p>
        <p>Butuh {COMPANION_MIN_ANDROID} atau lebih baru.</p>
        <p>
          Sebelum memasang, cocokkan SHA-256 berkas dengan yang tertulis di{" "}
          <a href={COMPANION_RELEASE_URL} className={linkClass}>
            halaman rilis
          </a>
          .
        </p>
      </div>
    </div>
  );
}

/** Settings → Perangkat & akses: where to get the app before pairing it. */
export function CompanionDownloadSection() {
  return (
    <SettingsSubsection
      id="companion-download-title"
      title="Unduh aplikasi pendamping"
      description="Pasang aplikasi Android ini dulu, lalu hubungkan ponsel dengan kode QR pemasangan."
    >
      <DownloadDetails />
    </SettingsSubsection>
  );
}

/** Collapsed in the pairing dialog, so its QR cannot be mistaken for the pairing code. */
export function CompanionDownloadDisclosure() {
  return (
    <details className="group rounded-xl border border-[var(--line)]">
      <summary
        id="companion-download-disclosure"
        className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-2 rounded-md px-3 font-semibold text-[var(--text)] [&::-webkit-details-marker]:hidden"
      >
        Belum punya aplikasinya? Unduh aplikasi pendamping
        <Icon name="chevron-down" className="h-4 w-4 shrink-0 text-[var(--muted)] transition-transform group-open:rotate-180" />
      </summary>
      <section aria-labelledby="companion-download-disclosure" className="border-t border-[var(--line)] p-3">
        <DownloadDetails />
      </section>
    </details>
  );
}
