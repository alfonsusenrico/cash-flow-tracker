"use client";

import { useEffect, useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";

interface ConfirmRemoveProps {
  /** What is removed, as the owner reads it in the row (e.g. "pemetaan “AN**RA**”"). */
  subject: string;
  /** Accessible name of the first button. */
  triggerLabel: string;
  disabled?: boolean;
  pending?: boolean;
  onConfirm: () => void;
  /** "Hapus data" for a test session's stored notifications; "Hapus" otherwise. */
  kind?: "remove" | "removeData";
  /** The first button is a 36 px trash icon button (Pemetaan, Nama pengirim; apply-markas-pengaturan D14). */
  icon?: boolean;
}

/** "Hapus" that asks once more in the same row; cancelling or Escape changes nothing and restores focus. */
export function ConfirmRemove({ subject, triggerLabel, disabled = false, pending = false, onConfirm, kind = "remove", icon = false }: ConfirmRemoveProps) {
  const t = useTranslations("settings.remove");
  const session = useTranslations("settings.testSession");
  const common = useTranslations("common");
  const [confirming, setConfirming] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const cancel = useRef<HTMLButtonElement>(null);
  const returnFocus = useRef(false);

  useEffect(() => {
    if (confirming) {
      cancel.current?.focus();
    } else if (returnFocus.current) {
      returnFocus.current = false;
      trigger.current?.focus();
    }
  }, [confirming]);

  const stop = () => {
    returnFocus.current = true;
    setConfirming(false);
  };

  const question = kind === "removeData" ? t("askData", { subject }) : t("ask", { subject });

  if (!confirming) {
    return (
      <button
        ref={trigger}
        type="button"
        aria-label={triggerLabel}
        disabled={disabled}
        onClick={() => setConfirming(true)}
        className={icon ? "ws-icon-btn" : "ws-btn ws-btn-quiet c-expense"}
      >
        {icon ? <Icon name="trash" className="h-4 w-4" /> : kind === "removeData" ? session("removeData") : t("verb")}
      </button>
    );
  }

  return (
    <div
      role="group"
      aria-label={question}
      className="set-confirm"
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          stop();
        }
      }}
    >
      <span className="t-strong">{question}</span>
      <button ref={cancel} type="button" className="ws-btn" onClick={stop}>
        {common("cancel")}
      </button>
      <button
        type="button"
        className="ws-btn ws-btn-danger"
        disabled={pending}
        onClick={() => {
          stop();
          onConfirm();
        }}
      >
        {pending ? t("removing") : kind === "removeData" ? session("removeDataYes") : t("yes")}
      </button>
    </div>
  );
}
