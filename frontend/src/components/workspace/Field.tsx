"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/** A SPEC field: label, control, the error under it and a hint; the control gets its ids and state. */
export function Field({
  id,
  label,
  hint,
  error,
  describedBy: extraDescription,
  className,
  children,
}: {
  id: string;
  label: string;
  hint?: ReactNode;
  error?: string;
  /** Another element that describes the control, such as a hint shared by two fields. */
  describedBy?: string;
  className?: string;
  children: (aria: { id: string; "aria-invalid"?: true; "aria-describedby"?: string }) => ReactNode;
}) {
  const errorId = error ? `${id}-error` : undefined;
  const hintId = hint ? `${id}-hint` : undefined;
  const describedBy = [errorId, hintId, extraDescription].filter(Boolean).join(" ") || undefined;
  return (
    <div className={cn("field", className)}>
      <label htmlFor={id}>{label}</label>
      {children({ id, "aria-invalid": error ? true : undefined, "aria-describedby": describedBy })}
      {error && <p id={errorId} className="err">{error}</p>}
      {hint && <p id={hintId} className="hint">{hint}</p>}
    </div>
  );
}
