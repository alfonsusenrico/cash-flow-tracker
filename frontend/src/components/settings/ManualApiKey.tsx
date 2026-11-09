"use client";

import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { getApiKeyInfo, rotateApiKey, type ApiKeyMetadata } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { SettingsAlert } from "@/components/settings/SettingsSection";

/**
 * "Kunci API manual (cadangan)" (apply-markas-pengaturan D8): an open section with the key's prefix, creation and
 * last use, the inline rotation confirmation and the once-only box. Rotation replaces only manual keys, so paired
 * phones stay connected. The new key lives in this component's state until "Tutup" and is never logged.
 */
export function ManualApiKey() {
  const t = useTranslations("settings.apiKey");
  const common = useTranslations("common");
  const format = useFormat();
  const qc = useQueryClient();
  const { data, isError } = useQuery<{ ok: boolean; api_key: ApiKeyMetadata | null }>({
    queryKey: queryKeys.auth.apiKey,
    queryFn: getApiKeyInfo,
  });
  const [confirming, setConfirming] = useState(false);
  const [plainKey, setPlainKey] = useState<string | null>(null);
  const [copy, setCopy] = useState<"idle" | "copied" | "failed">("idle");
  const [error, setError] = useState("");
  const newButton = useRef<HTMLButtonElement>(null);
  const cancelButton = useRef<HTMLButtonElement>(null);
  const keyField = useRef<HTMLInputElement>(null);
  const returnFocus = useRef(false);

  useEffect(() => {
    if (confirming) cancelButton.current?.focus();
    else if (plainKey) keyField.current?.focus();
    else if (returnFocus.current) newButton.current?.focus();
    returnFocus.current = false;
  }, [confirming, plainKey]);

  const rotate = useMutation({
    mutationFn: rotateApiKey,
    onSuccess: (result) => {
      setError("");
      setPlainKey(result.api_key);
      setConfirming(false);
      void qc.invalidateQueries({ queryKey: queryKeys.auth.apiKey });
    },
    onError: (e: Error) => setError(e.message || t("rotateFailed")),
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
  const dateTime = (iso: string) => `${format.yearDateAt(iso)} ${format.time(iso)}`;

  return (
    <section aria-labelledby="manual-api-key-title">
      <SectionHeader id="manual-api-key-title" label={t("title")} />
      {isError && <SettingsAlert>{t("loadFailed")}</SettingsAlert>}
      {error && <SettingsAlert>{error}</SettingsAlert>}
      <ul className="list set-list">
        <li>
          <ListRow
            layout="stacked"
            title={key?.key_prefix ? t("row", { prefix: `${key.key_prefix}••••••••••••` }) : t("none")}
            meta={
              key
                ? t("meta", {
                    created: format.yearDateAt(key.created_at),
                    used: key.last_used_at ? dateTime(key.last_used_at) : t("never"),
                  })
                : undefined
            }
          />
        </li>
      </ul>
      <p className="t-meta set-note">{t("note")}</p>

      {plainKey ? (
        <div className="set-box set-note">
          <p className="t-meta">{t("once")}</p>
          <div className="set-actions set-note">
            <input
              ref={keyField}
              type="text"
              aria-label={t("keyLabel")}
              readOnly
              value={plainKey}
              className="field-control border tabular-nums min-w-0 flex-1 select-all"
            />
            <button type="button" className="ws-btn" onClick={() => void copyKey()}>
              {copy === "copied" ? t("copied") : t("copy")}
            </button>
            <button
              type="button"
              className="ws-btn ws-btn-quiet"
              onClick={() => {
                returnFocus.current = true;
                setPlainKey(null);
                setCopy("idle");
              }}
            >
              {t("close")}
            </button>
          </div>
          {copy === "failed" && <p role="alert" className="set-err set-note">{t("copyFailed")}</p>}
        </div>
      ) : confirming ? (
        <div className="set-box set-note" role="group" aria-label={t("new")}>
          <p className="t-body">{t("confirmText")}</p>
          <div className="set-actions set-note">
            <button
              ref={cancelButton}
              type="button"
              className="ws-btn"
              onClick={() => {
                returnFocus.current = true;
                setConfirming(false);
              }}
            >
              {common("cancel")}
            </button>
            <button type="button" className="ws-btn ws-btn-danger" disabled={rotate.isPending} onClick={() => rotate.mutate()}>
              {rotate.isPending ? t("creating") : t("confirm")}
            </button>
          </div>
        </div>
      ) : (
        <div className="set-actions set-note">
          <button ref={newButton} type="button" className="ws-btn" onClick={() => setConfirming(true)}>
            {t("new")}
          </button>
        </div>
      )}
    </section>
  );
}
