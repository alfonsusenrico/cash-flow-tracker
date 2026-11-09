"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { getDevices, revokeDevice, type PairedDevice } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { DevicePairingDialog } from "@/components/settings/DevicePairingDialog";
import { SettingsAlert } from "@/components/settings/SettingsSection";

/** The phones paired by QR; manually created keys live in "Kunci API manual (cadangan)". */
export function pairedDevices(devices: PairedDevice[] | undefined): PairedDevice[] {
  return devices?.filter((device) => device.created_via === "pairing") ?? [];
}

/**
 * "Perangkat terhubung" (apply-markas-pengaturan D6): one plain row per paired phone with its pairing date, last
 * activity and app version, and a quiet "Cabut" behind a centered 480 px confirmation. "Hubungkan ponsel" opens the
 * pairing dialog; both dialogs return focus to their opener.
 */
export function DevicePairingSection() {
  const t = useTranslations("settings.devices");
  const common = useTranslations("common");
  const words = useTranslations("settings.common");
  const format = useFormat();
  const queryClient = useQueryClient();
  const { data, isError, isPending, refetch } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices });
  const [pairing, setPairing] = useState(false);
  const [confirming, setConfirming] = useState<PairedDevice | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const connectButton = useRef<HTMLButtonElement>(null);
  const revokeOpener = useRef<HTMLButtonElement | null>(null);

  const dateTime = (iso: string) => words("dateTime", { date: format.yearDateAt(iso), time: format.time(iso) });

  const closeConfirm = () => {
    setConfirming(null);
    revokeOpener.current?.focus();
  };

  const revoke = useMutation({
    mutationFn: (device: PairedDevice) => revokeDevice(device.id),
    onSuccess: (_, device) => {
      setError("");
      setNotice(t("revoked", { name: device.name }));
      setConfirming(null);
      queryClient.invalidateQueries({ queryKey: queryKeys.devices });
      // The row and its "Cabut" are gone, so focus goes to the part's own action.
      requestAnimationFrame(() => connectButton.current?.focus());
    },
    onError: (e: Error) => {
      setError(e.message || t("revokeFailed"));
      closeConfirm();
      queryClient.invalidateQueries({ queryKey: queryKeys.devices });
    },
  });

  const devices = pairedDevices(data?.devices);

  return (
    <section aria-labelledby="paired-devices-title">
      <SectionHeader
        id="paired-devices-title"
        label={t("paired")}
        aside={
          <button
            ref={connectButton}
            type="button"
            className="ws-btn"
            onClick={() => {
              setNotice("");
              setPairing(true);
            }}
          >
            {t("connect")}
          </button>
        }
      />
      {notice && <p role="status" className="t-body">{notice}</p>}
      {error && <SettingsAlert>{error}</SettingsAlert>}
      {isError ? (
        <ErrorState message={t("loadFailed")} onRetry={() => void refetch()} />
      ) : isPending ? (
        <div aria-busy="true" aria-label={t("loading")}>
          <Skeleton style={{ height: 56 }} />
        </div>
      ) : devices.length === 0 ? (
        <p className="t-body text-[var(--muted)]">{t("empty")}</p>
      ) : (
        <ul className="list set-list" aria-label={t("pairedList")}>
          {devices.map((device) => {
            const base = t("rowMeta", {
              date: format.yearDateAt(device.created_at),
              active: device.last_used_at ? dateTime(device.last_used_at) : t("neverActive"),
            });
            const meta = device.health ? t("rowVersion", { meta: base, version: device.health.app_version }) : base;
            return (
              <li key={device.id}>
                <ListRow
                  title={device.name}
                  meta={meta}
                  layout="stacked"
                  actions={
                    <button
                      type="button"
                      className="ws-btn ws-btn-quiet c-expense"
                      aria-label={t("revokeName", { name: device.name })}
                      disabled={revoke.isPending}
                      onClick={(event) => {
                        revokeOpener.current = event.currentTarget;
                        setError("");
                        setConfirming(device);
                      }}
                    >
                      {t("revoke")}
                    </button>
                  }
                />
              </li>
            );
          })}
        </ul>
      )}

      {pairing && (
        <DevicePairingDialog
          onClose={() => {
            setPairing(false);
            queryClient.invalidateQueries({ queryKey: queryKeys.devices });
            connectButton.current?.focus();
          }}
        />
      )}

      {confirming && (
        <ShellDialog
          open
          onClose={closeConfirm}
          title={t("revokeTitle", { name: confirming.name })}
          width={480}
          closeLabel={common("close")}
          footer={
            <>
              <button type="button" className="ws-btn" onClick={closeConfirm}>
                {common("cancel")}
              </button>
              <button
                type="button"
                className="ws-btn ws-btn-danger"
                disabled={revoke.isPending}
                onClick={() => revoke.mutate(confirming)}
              >
                {revoke.isPending ? t("revoking") : t("revokeConfirm")}
              </button>
            </>
          }
        >
          <p className="t-body">{t("revokeBody")}</p>
        </ShellDialog>
      )}
    </section>
  );
}
