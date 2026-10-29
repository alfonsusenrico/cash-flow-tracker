"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getDevices, revokeDevice, type PairedDevice } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import { Modal } from "@/components/ui/Modal";
import { DevicePairingDialog } from "@/components/ui/DevicePairingDialog";

function formatDate(value: string, withTime: boolean) {
  return new Date(value).toLocaleDateString("id-ID", {
    day: "numeric",
    month: "short",
    year: "numeric",
    ...(withTime ? { hour: "2-digit", minute: "2-digit" } : {}),
  });
}

export function DevicePairingSection({ enabled }: { enabled: boolean }) {
  const queryClient = useQueryClient();
  const { data, isError } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices, enabled });
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
    <section aria-labelledby="paired-devices-title" className="space-y-3 border-t border-[var(--border)] pt-3 text-xs">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3
          ref={heading}
          tabIndex={-1}
          id="paired-devices-title"
          className="text-xs font-bold uppercase tracking-wider text-[var(--muted)] focus-visible:outline focus-visible:outline-2"
        >
          Perangkat terhubung
        </h3>
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
      </div>
      <p className="text-[var(--muted)]">
        Pindai kode QR dari aplikasi pendamping. Setiap ponsel mendapat kuncinya sendiri dan bisa dicabut kapan saja.
      </p>
      {notice && <p role="status" className="font-semibold text-[var(--text)]">{notice}</p>}
      {error && <p role="alert" className="text-[var(--danger)]">{error}</p>}
      {isError ? (
        <p role="alert" className="text-[var(--danger)]">Daftar perangkat gagal dimuat.</p>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat perangkat…</p>
      ) : devices.length === 0 ? (
        <p className="text-[var(--muted)]">Belum ada ponsel yang dipasangkan.</p>
      ) : (
        <ul className="divide-y divide-[var(--border)] rounded-xl border border-[var(--border)]">
          {devices.map((device) => (
            <li key={device.id} className="flex flex-wrap items-center justify-between gap-2 px-3 py-2">
              <div className="min-w-0 flex-1 break-words">
                <p className="font-semibold text-[var(--text)]">{device.name}</p>
                <p className="text-[var(--muted)] tabular-nums">
                  Dipasangkan {formatDate(device.created_at, false)} · Terakhir aktif{" "}
                  {device.last_used_at ? formatDate(device.last_used_at, true) : "belum pernah"}
                </p>
              </div>
              <Button
                type="button"
                variant="secondary"
                size="sm"
                className="min-h-11"
                aria-label={`Cabut ${device.name}`}
                disabled={revoke.isPending}
                onClick={() => {
                  setError("");
                  setConfirming(device);
                }}
              >
                Cabut
              </Button>
            </li>
          ))}
        </ul>
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
              className="min-h-11"
              disabled={revoke.isPending}
              onClick={() => confirming && revoke.mutate(confirming)}
            >
              {revoke.isPending ? "Mencabut…" : "Cabut perangkat"}
            </Button>
          </div>
        </div>
      </Modal>
    </section>
  );
}
