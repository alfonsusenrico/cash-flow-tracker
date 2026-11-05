"use client";

import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/Button";
import { confirmDangerClass, quietDangerClass } from "@/components/settings/SettingsCard";

interface ConfirmRemoveProps {
  /** What is removed, as the owner reads it in the row (e.g. "AN**RA** → Andra"). */
  subject: string;
  /** Accessible name of the first "Hapus" button. */
  triggerLabel: string;
  disabled?: boolean;
  pending?: boolean;
  onConfirm: () => void;
  /** The verb on the first button and in the question, for removals that are not deletions. */
  verb?: string;
  confirmLabel?: string;
  pendingLabel?: string;
}

/** "Hapus" that asks once more in the same row; cancelling or Escape changes nothing and restores focus. */
export function ConfirmRemove({
  subject, triggerLabel, disabled = false, pending = false, onConfirm,
  verb = "Hapus", confirmLabel = "Ya, hapus", pendingLabel = "Menghapus…",
}: ConfirmRemoveProps) {
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

  if (!confirming) {
    return (
      <button
        ref={trigger}
        type="button"
        aria-label={triggerLabel}
        disabled={disabled}
        onClick={() => setConfirming(true)}
        className={quietDangerClass}
      >
        {verb}
      </button>
    );
  }

  return (
    <div
      role="group"
      aria-label={`${verb} ${subject}?`}
      className="flex flex-wrap items-center justify-end gap-2"
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          stop();
        }
      }}
    >
      <span className="text-xs font-semibold text-[var(--text)]">{verb} {subject}?</span>
      <Button ref={cancel} type="button" variant="secondary" size="sm" className="min-h-11" onClick={stop}>
        Batal
      </Button>
      <button
        type="button"
        disabled={pending}
        onClick={() => {
          stop();
          onConfirm();
        }}
        className={`inline-flex min-h-11 items-center justify-center rounded-md px-3 text-xs font-semibold disabled:opacity-50 ${confirmDangerClass}`}
      >
        {pending ? pendingLabel : confirmLabel}
      </button>
    </div>
  );
}
