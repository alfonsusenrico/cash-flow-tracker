import { useId } from "react";
import { cn, formatNumberWithDots } from "@/lib/utils";

interface FormFieldProps {
  id?: string;
  label: string;
  children: (props: {
    id: string;
    "aria-describedby"?: string;
    "aria-invalid"?: true;
  }) => React.ReactNode;
  description?: string;
  error?: string;
  required?: boolean;
  className?: string;
}

export function FormField({
  id: providedId,
  label,
  children,
  description,
  error,
  required,
  className,
}: FormFieldProps) {
  const generatedId = useId();
  const id = providedId ?? generatedId;
  const descriptionId = description ? `${id}-description` : undefined;
  const errorId = error ? `${id}-error` : undefined;
  const describedBy = [descriptionId, errorId].filter(Boolean).join(" ") || undefined;

  return (
    <div className={cn("space-y-2", className)}>
      <label htmlFor={id} className="block text-xs font-medium text-[var(--muted)]">
        {label}
        {required ? <span aria-hidden="true"> *</span> : null}
      </label>
      {children({
        id,
        "aria-describedby": describedBy,
        "aria-invalid": error ? true : undefined,
      })}
      {error ? (
        <p id={errorId} className="text-xs font-medium text-out-fg" role="alert">
          {error}
        </p>
      ) : null}
      {/* Guidance is one visible hint line under the control, never an information control (apply-markas-catat D8). */}
      {description ? <p id={descriptionId} className="text-xs text-[var(--muted)]">{description}</p> : null}
    </div>
  );
}

interface MonetaryInputProps {
  id?: string;
  name: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  description?: string;
  error?: string;
  placeholder?: string;
  required?: boolean;
  className?: string;
}

export function MonetaryInput({
  id,
  name,
  label,
  value,
  onChange,
  description,
  error,
  placeholder = "0",
  required = false,
  className,
}: MonetaryInputProps) {
  return (
    <FormField id={id} label={label} description={description} error={error} required={required}>
      {(field) => (
        <input
          {...field}
          name={name}
          type="text"
          inputMode="numeric"
          required={required}
          value={value}
          onChange={(event) => onChange(formatNumberWithDots(event.target.value))}
          placeholder={placeholder}
          className={cn(
            // Money input (SPEC §7): 56 px, Inter 20/600 tabular, -0.01em.
            "ws-money-input h-14 w-full rounded-md border border-[var(--line-strong)] bg-[var(--dialog)] px-3 text-xl leading-5 font-semibold tabular-nums tracking-[-0.01em] text-[var(--text)]",
            className,
          )}
        />
      )}
    </FormField>
  );
}
