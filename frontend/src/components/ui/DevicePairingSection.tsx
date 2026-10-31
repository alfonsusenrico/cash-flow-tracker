"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getDevices, revokeDevice, type PairedDevice } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import { Modal } from "@/components/ui/Modal";
import { DevicePairingDialog } from "@/components/ui/DevicePairingDialog";
import {
  SettingsAlert, SettingsList, SettingsSubsection, confirmDangerClass, quietDangerClass,
} from "@/components/settings/SettingsCard";

function formatDate(value: string, withTime: boolean) {
  return new Date(value).toLocaleDateString("id-ID", {
    day: "numeric",
    month: "short",
    year: "numeric",
    ...(withTime ? { hour: "2-digit", minute: "2-digit" } : {}),
  });
}

export function DevicePairingSection() {
  const queryClient = useQueryClient();
  const { data, isError } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices });
  const [pairing, setPairing] = useState(false);
  const [confirming, setConfirming] = useState<PairedDevice | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const heading = useRef<HTMLHeadingElement | null>(null);

  const revoke = useMutation({
    mutationFn: (device: PairedDevice) => revokeDevice(device.id),
    onSuccess: (_, device) => {
      setError("");
      setNotice(`${device.name} dicabut. Ponsel itu harus dipasangkan ulang.`);
      setConfirming(null);
      queryClient.invalidateQueries({ queryKey: queryKeys.devices });
      requestAnimationFrame(() => heading.current?.focus());
    },
    onError: (e: Error) => {
      setConfirming(null);
      setError(e.message || "Perangkat gagal dicabut. Coba lagi.");
      queryClient.invalidateQueries({ queryKey: queryKeys.devices });
    },
  });

  const devices = data?.devices.filter((device) => device.created_via === "pairing") ?? [];

  return (
    <SettingsSubsection
      id="paired-devices-title"
      headingRef={heading}
      title="Perangkat terhubung"
      description="Pindai kode QR dari aplikasi pendamping. Setiap ponsel punya kunci sendiri dan bisa dicabut kapan saja."
      action={
        <Button
          type="button"
          variant="primary"
          size="sm"
          className="min-h-11"
          onClick={() => {
            setNotice("");
            setPairing(true);
          }}
        >
          Hubungkan ponsel
        </Button>
      }
    >
      {notice && <p role="status" className="font-semibold text-[var(--text)]">{notice}</p>}
      {error && <SettingsAlert>{error}</SettingsAlert>}
      {isError ? (
        <SettingsAlert>Daftar perangkat gagal dimuat.</SettingsAlert>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat perangkat…</p>
      ) : devices.length === 0 ? (
        <p className="text-[var(--muted)]">Belum ada ponsel yang dipasangkan.</p>
      ) : (
        <SettingsList label="Ponsel yang dipasangkan">
          {devices.map((device) => (
            <li key={device.id} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-2.5">
              <div className="min-w-0 flex-1 break-words">
                <p className="font-semibold text-[var(--text)]">{device.name}</p>
                <p className="text-xs text-[var(--muted)] tabular-nums">
                  Dipasangkan {formatDate(device.created_at, false)} · Terakhir aktif{" "}
                  {device.last_used_at ? formatDate(device.last_used_at, true) : "belum pernah"}
                </p>
              </div>
              <button
                type="button"
                className={quietDangerClass}
                aria-label={`Cabut ${device.name}`}
                disabled={revoke.isPending}
                onClick={() => {
                  setError("");
                  setConfirming(device);
                }}
              >
                Cabut
              </button>
            </li>
          ))}
        </SettingsList>
      )}

      {pairing && (
        <DevicePairingDialog
          onClose={() => {
            setPairing(false);
            queryClient.invalidateQueries({ queryKey: queryKeys.devices });
          }}
        />
      )}

      <Modal
        open={confirming !== null}
        onClose={() => setConfirming(null)}
        title={confirming ? `Cabut ${confirming.name}?` : "Cabut perangkat?"}
        centered
      >
        <div className="space-y-4 text-xs">
          <p className="text-[var(--text)]">
            Ponsel ini langsung terputus dan tidak bisa mengirim transaksi lagi sampai dipasangkan ulang. Perangkat lain
            tetap terhubung.
          </p>
          <div className="flex flex-wrap justify-end gap-2">
            <Button type="button" variant="secondary" size="sm" className="min-h-11" onClick={() => setConfirming(null)}>
              Batal
            </Button>
            <Button
              type="button"
              variant="danger"
              size="sm"
              className={`min-h-11 ${confirmDangerClass}`}
              disabled={revoke.isPending}
              onClick={() => confirming && revoke.mutate(confirming)}
            >
              {revoke.isPending ? "Mencabut…" : "Cabut perangkat"}
            </Button>
          </div>
        </div>
      </Modal>
    </SettingsSubsection>
  );
}
