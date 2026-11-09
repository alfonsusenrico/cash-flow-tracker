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

export function FormErrorSummary({ message }: { message?: string }) {
  if (!message) return null;
  return (
    <div
      role="alert"
      aria-live="assertive"
      tabIndex={-1}
      className="rounded-xl border border-[var(--line)] bg-[var(--surface-2)] p-3 text-xs font-medium text-out-fg"
    >
      {message}
    </div>
  );
}

interface ChoiceOption {
  value: string;
  label: string;
  disabled?: boolean;
}

interface ChoiceGroupProps {
  label: string;
  name: string;
  value: string;
  options: ChoiceOption[];
  onChange: (value: string) => void;
}

export function ChoiceGroup({ label, name, value, options, onChange }: ChoiceGroupProps) {
  return (
    <fieldset className="space-y-1.5">
      <legend className="text-xs font-medium text-[var(--muted)]">{label}</legend>
      <div className="flex flex-wrap gap-2">
        {options.map((option) => (
          <label
            key={option.value}
            className={cn(
              "inline-flex min-h-11 cursor-pointer items-center gap-2 rounded-md border px-3 text-xs font-semibold",
              value === option.value
                ? "border-[var(--line-strong)] bg-[var(--surface-2)] text-[var(--text)]"
                : "border-[var(--line)] text-[var(--muted)]",
              option.disabled && "cursor-not-allowed opacity-50",
            )}
          >
            <input
              type="radio"
              name={name}
              value={option.value}
              checked={value === option.value}
              disabled={option.disabled}
              onChange={() => onChange(option.value)}
              className="accent-[var(--text)]"
            />
            {option.label}
          </label>
        ))}
      </div>
    </fieldset>
  );
}
