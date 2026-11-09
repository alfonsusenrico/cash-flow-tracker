"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { SectionHeader } from "@/components/workspace/lists";
import { QrCode } from "@/components/settings/QrCode";

/** Always the newest signed release: the Android release workflow keeps it marked latest. */
export const COMPANION_DOWNLOAD_URL =
  "https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk";
export const COMPANION_RELEASE_URL = "https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest";
/** minSdk 26 in mobile/app/build.gradle.kts. */
export const COMPANION_MIN_ANDROID = "Android 8.0";

/** The prototype's 136 px box less its 8 px padding and 1 px line on each side. */
const DOWNLOAD_QR_SIZE = 118;

function DownloadDetails() {
  const t = useTranslations("settings.devices");
  const [qrFailed, setQrFailed] = useState(false);
  return (
    <div className="set-qr-row">
      <div className="qr">
        <QrCode value={COMPANION_DOWNLOAD_URL} size={DOWNLOAD_QR_SIZE} label={t("downloadQr")} onError={() => setQrFailed(true)} />
      </div>
      <div className="grow">
        <a href={COMPANION_DOWNLOAD_URL} className="ws-btn">
          <Icon name="download" className="h-4 w-4" />
          {t("downloadButton")}
        </a>
        <p className="t-meta set-note">
          {t.rich("downloadNote", {
            android: COMPANION_MIN_ANDROID,
            link: (chunks) => (
              <a href={COMPANION_RELEASE_URL} className="ws-linkbtn underline">
                {chunks}
              </a>
            ),
          })}
        </p>
        {qrFailed && (
          <p role="alert" className="set-err set-note">
            {t("qrFailed")}
          </p>
        )}
      </div>
    </div>
  );
}

/** Pengaturan › HP & perangkat: where to get the app before pairing it (D6). */
export function CompanionDownloadSection() {
  const t = useTranslations("settings.devices");
  return (
    <section aria-labelledby="companion-download-title">
      <SectionHeader id="companion-download-title" label={t("download")} />
      <DownloadDetails />
    </section>
  );
}

/** Collapsed in the pairing dialog, so its QR cannot be mistaken for the pairing code (D9). */
export function CompanionDownloadDisclosure() {
  const t = useTranslations("settings.pairing");
  return (
    <details>
      <summary id="companion-download-disclosure">
        {t("noApp")}
        <Icon name="chevron-down" className="h-4 w-4" />
      </summary>
      <section aria-labelledby="companion-download-disclosure">
        <DownloadDetails />
      </section>
    </details>
  );
}
