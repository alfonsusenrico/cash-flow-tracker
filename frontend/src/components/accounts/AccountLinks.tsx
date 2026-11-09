"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { SectionHeader } from "@/components/workspace/lists";
import { api, type AccountLink } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";

/**
 * "Juga dilaporkan" in the Rekening detail (reconcile-linked-account-reports D6): the other apps that report
 * this account, learned from "Benar, satu transaksi" in Tinjau. Links are never added here, only stopped, and
 * "Lepaskan" asks once more in its row.
 */
export function AccountLinks({ accountId, accountName }: { accountId: string; accountName: string }) {
  const t = useTranslations("accounts.links");
  const qc = useQueryClient();
  const headingId = useId();
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
      setMessage(stopped ? t("stopped", { app: stopped.institution_label, account: accountName }) : t("stoppedAny", { account: accountName }));
    },
    onError: (error: Error) => setMessage(error.message || t("stopFailed")),
  });
  if (isError) return <p className="t-meta" role="alert">{t("loadFailed")}</p>;
  const links = data?.links ?? [];
  if (!links.length && !message) return null;
  return (
    <section aria-labelledby={headingId}>
      <SectionHeader id={headingId} label={t("title")} />
      {links.length > 0 && (
        <ul className="list">
          {links.map((link) => (
            <li key={link.id} className="row is-plain">
              <span className="r-main">
                <span className="r-title">{link.institution_label}</span>
              </span>
              <ConfirmStop label={link.institution_label} pending={stop.isPending} onConfirm={() => stop.mutate(link.id)} />
            </li>
          ))}
        </ul>
      )}
      {message && <p role="status" className="t-meta">{message}</p>}
    </section>
  );
}

function ConfirmStop({ label, pending, onConfirm }: { label: string; pending: boolean; onConfirm: () => void }) {
  const t = useTranslations("accounts.links");
  const [confirming, setConfirming] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const cancel = useRef<HTMLButtonElement>(null);
  const returnFocus = useRef(false);
  useEffect(() => {
    if (confirming) cancel.current?.focus();
    else if (returnFocus.current) {
      returnFocus.current = false;
      trigger.current?.focus();
    }
  }, [confirming]);
  const stopConfirming = () => {
    returnFocus.current = true;
    setConfirming(false);
  };
  if (!confirming) {
    return (
      <button ref={trigger} type="button" className="ws-btn ws-btn-quiet ws-btn-sm" aria-label={t("removeNamed", { app: label })} onClick={() => setConfirming(true)}>
        {t("remove")}
      </button>
    );
  }
  return (
    <span
      role="group"
      aria-label={t("question", { app: label })}
      className="r-actions"
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          stopConfirming();
        }
      }}
    >
      <span className="t-meta">{t("question", { app: label })}</span>
      <button ref={cancel} type="button" className="ws-btn ws-btn-sm" onClick={stopConfirming}>{t("cancel")}</button>
      <button type="button" className="ws-btn ws-btn-sm" disabled={pending} onClick={() => { stopConfirming(); onConfirm(); }}>
        {pending ? t("removing") : t("confirm")}
      </button>
    </span>
  );
}
