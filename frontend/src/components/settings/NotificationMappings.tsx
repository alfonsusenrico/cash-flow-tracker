"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { deleteNotificationAlias, getNotificationAliases } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import { SettingsAlert, SettingsCard, SettingsList } from "@/components/settings/SettingsCard";

/** Account names learned from notifications; removing one affects only future notifications. */
export function NotificationMappings() {
  const qc = useQueryClient();
  const { data, isError } = useQuery({ queryKey: queryKeys.notificationAliases, queryFn: getNotificationAliases });
  const [error, setError] = useState("");

  const remove = useMutation({
    mutationFn: deleteNotificationAlias,
    onSuccess: () => {
      setError("");
      document.getElementById("settings-pemetaan-title")?.focus();
      void qc.invalidateQueries({ queryKey: queryKeys.notificationAliases });
    },
    onError: (e: Error) => setError(e.message || "Gagal menghapus pemetaan"),
  });

  return (
    <SettingsCard section="pemetaan">
      <p className="text-xs text-[var(--muted)]">Hapus pemetaan yang salah; transaksi yang sudah tercatat tidak berubah.</p>
      {error && <SettingsAlert>{error}</SettingsAlert>}
      {isError ? (
        <SettingsAlert>Daftar pemetaan gagal dimuat.</SettingsAlert>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat…</p>
      ) : data.aliases.length === 0 ? (
        <p className="text-[var(--muted)]">Belum ada pemetaan.</p>
      ) : (
        <SettingsList label="Pemetaan nama notifikasi">
          {data.aliases.map((alias) => (
            <li key={alias.id} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-2.5">
              <div className="min-w-0 flex-1 break-words">
                <p className="font-semibold text-[var(--text)]">
                  {alias.name} <span aria-hidden="true">→</span><span className="sr-only">dipetakan ke</span> {alias.account}
                </p>
                <p className="text-xs text-[var(--muted)]">
                  {alias.institution.toUpperCase()} · {alias.source === "owner" ? "Dikonfirmasi Anda" : "Dipetakan otomatis"}
                </p>
              </div>
              <ConfirmRemove
                subject={`pemetaan ${alias.name}`}
                triggerLabel={`Hapus pemetaan ${alias.name}`}
                disabled={remove.isPending}
                pending={remove.isPending && remove.variables === alias.id}
                onConfirm={() => remove.mutate(alias.id)}
              />
            </li>
          ))}
        </SettingsList>
      )}
    </SettingsCard>
  );
}
