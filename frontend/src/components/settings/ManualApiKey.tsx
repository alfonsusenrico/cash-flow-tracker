"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getApiKeyInfo, rotateApiKey, type ApiKeyMetadata } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import { Icon } from "@/components/ui/Icon";
import { SettingsAlert, confirmDangerClass } from "@/components/settings/SettingsCard";

function formatUsed(value: string) {
  return new Date(value).toLocaleDateString("id-ID", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
}

/** Fallback Bearer key for phones that cannot scan the pairing QR; rotation never touches paired devices. */
export function ManualApiKey() {
  const qc = useQueryClient();
  const { data, isError } = useQuery<{ ok: boolean; api_key: ApiKeyMetadata | null }>({
    queryKey: queryKeys.auth.apiKey,
    queryFn: getApiKeyInfo,
  });
  const [confirming, setConfirming] = useState(false);
  const [plainKey, setPlainKey] = useState<string | null>(null);
  const [copy, setCopy] = useState<"idle" | "copied" | "failed">("idle");
  const [error, setError] = useState("");

  const rotate = useMutation({
    mutationFn: rotateApiKey,
    onSuccess: (result) => {
      setError("");
      setPlainKey(result.api_key);
      setConfirming(false);
      void qc.invalidateQueries({ queryKey: queryKeys.auth.apiKey });
    },
    onError: (e: Error) => setError(e.message || "Gagal memperbarui kunci API"),
  });

  const copyKey = async () => {
    if (!plainKey) return;
    try {
      await navigator.clipboard.writeText(plainKey);
      setCopy("copied");
    } catch {
      setCopy("failed");
    }
  };

  const key = data?.api_key;

  return (
    <details className="group rounded-[var(--radius-md)] border border-[var(--border)]">
      <summary
        id="manual-api-key-title"
        className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-2 rounded-[var(--radius-md)] px-3 font-semibold text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)] [&::-webkit-details-marker]:hidden"
      >
        Kunci API manual (cadangan)
        <Icon name="chevron-down" className="h-4 w-4 text-[var(--muted)] transition-transform group-open:rotate-180" />
      </summary>
      <section aria-labelledby="manual-api-key-title" className="space-y-3 border-t border-[var(--border)] p-3">
        <p className="text-xs text-[var(--muted)]">
          Pakai hanya jika ponsel tidak bisa memindai kode QR. Membuat kunci baru hanya mengganti kunci manual; ponsel
          yang dipasangkan lewat QR tetap terhubung.
        </p>
        {isError && <SettingsAlert>Info kunci API gagal dimuat.</SettingsAlert>}
        {error && <SettingsAlert>{error}</SettingsAlert>}

        {plainKey ? (
          <div className="space-y-2.5 rounded-[var(--radius-md)] border border-amber-500/40 bg-amber-500/10 p-3">
            <p className="font-semibold text-[var(--text)]">Salin kunci API baru sekarang</p>
            <p className="text-xs text-[var(--text-secondary)]">Kunci ini hanya ditampilkan sekali. Masukkan ke aplikasi pendamping di ponsel.</p>
            <div className="flex flex-wrap items-center gap-2">
              <input
                type="text"
                aria-label="Kunci API baru"
                readOnly
                value={plainKey}
                className="min-h-11 min-w-0 flex-1 select-all rounded-[var(--radius-md)] border border-[var(--border)] bg-[var(--surface)] px-3 font-mono text-xs text-[var(--text)]"
              />
              <Button type="button" size="sm" className="min-h-11" onClick={() => void copyKey()}>
                {copy === "copied" ? "Tersalin" : "Salin"}
              </Button>
            </div>
            {copy === "failed" && <p role="alert" className="text-xs text-[var(--text)]">Gagal menyalin. Pilih teks kunci lalu salin manual.</p>}
            <div className="flex justify-end">
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="min-h-11"
                onClick={() => {
                  setPlainKey(null);
                  setCopy("idle");
                }}
              >
                Tutup & selesai
              </Button>
            </div>
          </div>
        ) : (
          <>
            <dl className="grid gap-3 sm:grid-cols-2">
              <div>
                <dt className="text-xs text-[var(--muted)]">Awalan kunci aktif</dt>
                <dd className="font-mono font-semibold text-[var(--text)]">{key?.key_prefix ? `${key.key_prefix}••••••••••••` : "Belum ada kunci"}</dd>
              </div>
              <div>
                <dt className="text-xs text-[var(--muted)]">Terakhir digunakan</dt>
                <dd className="tabular-nums text-[var(--text)]">{key?.last_used_at ? formatUsed(key.last_used_at) : "Belum pernah"}</dd>
              </div>
            </dl>
            {confirming ? (
              <div className="space-y-2 rounded-[var(--radius-md)] border border-rose-500/30 bg-rose-500/10 p-3">
                <p className="text-xs font-semibold text-rose-700 dark:text-rose-300">
                  Kunci manual lama langsung dicabut. Ponsel yang dipasangkan lewat QR tetap terhubung. Lanjutkan?
                </p>
                <div className="flex flex-wrap justify-end gap-2">
                  <Button type="button" size="sm" className="min-h-11" onClick={() => setConfirming(false)}>
                    Batal
                  </Button>
                  <Button
                    type="button"
                    variant="danger"
                    size="sm"
                    className={`min-h-11 ${confirmDangerClass}`}
                    disabled={rotate.isPending}
                    onClick={() => rotate.mutate()}
                  >
                    {rotate.isPending ? "Membuat…" : "Ya, Buat Kunci Baru"}
                  </Button>
                </div>
              </div>
            ) : (
              <div className="flex justify-end">
                <Button type="button" size="sm" className="min-h-11" onClick={() => setConfirming(true)}>
                  Buat Kunci Baru (Reset)
                </Button>
              </div>
            )}
          </>
        )}
      </section>
    </details>
  );
}
