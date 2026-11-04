"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import { api, type AccountLink } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import styles from "./money.module.css";

/**
 * "Juga dilaporkan" in the Rekening detail (reconcile-linked-account-reports D6): the other apps that report
 * this account, learned from "Benar, satu transaksi" in Tinjau. Links are never added here, only stopped.
 */
export function AccountLinks({ accountId, accountName }: { accountId: string; accountName: string }) {
  const qc = useQueryClient();
  const [message, setMessage] = useState<string | null>(null);
  const { data, isError } = useQuery({
    queryKey: queryKeys.accountLinks(accountId),
    queryFn: () => api.get<{ ok: boolean; links: AccountLink[] }>(`/ingest/account-links?account_id=${encodeURIComponent(accountId)}`),
  });
  const stop = useMutation({
    mutationFn: (id: string) => api.del<{ ok: boolean }>(`/ingest/account-links/${id}`),
    onSuccess: async (_, id) => {
      const stopped = data?.links.find((link) => link.id === id);
      await qc.invalidateQueries({ queryKey: ["account-links"] });
      setMessage(`Tautan dilepas. Laporan ${stopped?.institution_label ?? "aplikasi itu"} untuk ${accountName} akan ditanyakan lagi di Tinjau.`);
    },
    onError: (error: Error) => setMessage(error.message || "Tautan gagal dilepas."),
  });
  if (isError) return <p className={styles.detailHeading} role="alert">Daftar aplikasi yang juga melaporkan rekening ini gagal dimuat.</p>;
  const links = data?.links ?? [];
  if (!links.length && !message) return null;
  return (
    <section aria-labelledby={`detail-links-${accountId}`}>
      <h3 id={`detail-links-${accountId}`} className={styles.detailHeading}>Juga dilaporkan</h3>
      {links.length > 0 && (
        <ul className={styles.detailRows}>
          {links.map((link) => (
            <li key={link.id}>
              <span>{link.institution_label}</span>
              <ConfirmRemove
                subject={`tautan ${link.institution_label}`}
                triggerLabel={`Lepaskan tautan ${link.institution_label}`}
                pending={stop.isPending}
                onConfirm={() => stop.mutate(link.id)}
                verb="Lepaskan"
                confirmLabel="Ya, lepaskan"
                pendingLabel="Melepas…"
              />
            </li>
          ))}
        </ul>
      )}
      {message && <p role="status" className="mt-1 text-xs text-[var(--text-2)]">{message}</p>}
    </section>
  );
}
