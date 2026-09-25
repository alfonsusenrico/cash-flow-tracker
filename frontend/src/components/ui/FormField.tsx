import { useId } from "react";
import { cn, formatNumberWithDots } from "@/lib/utils";
import { InfoHelp } from "./InfoHelp";

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
    <div className={cn("space-y-1", className)}>
      <div className="flex items-center gap-1">
        <label htmlFor={id} className="block text-[11px] font-medium text-[var(--muted)]">
          {label}
          {required ? <span aria-hidden="true"> *</span> : null}
        </label>
        {description ? <InfoHelp id={descriptionId} label={label}>{description}</InfoHelp> : null}
      </div>
      {children({
        id,
        "aria-describedby": describedBy,
        "aria-invalid": error ? true : undefined,
      })}
      {error ? (
        <p id={errorId} className="text-[11px] font-medium text-rose-600" role="alert">
          {error}
        </p>
      ) : null}
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
            "min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-bold tabular-nums text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]",
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
      className="rounded-xl border border-rose-500/25 bg-rose-500/10 p-3 text-xs font-medium text-rose-600"
    >
      {message}
    </div>
  );
}

interface FormSectionProps {
  title: string;
  description?: string;
  children: React.ReactNode;
  className?: string;
}

export function FormSection({ title, description, children, className }: FormSectionProps) {
  const titleId = useId();

  return (
    <section aria-labelledby={titleId} className={cn("space-y-3 border-t border-[var(--border)] pt-4", className)}>
      <div className="flex items-center gap-1">
        <h3 id={titleId} className="text-sm font-semibold text-[var(--text)]">{title}</h3>
        {description ? <InfoHelp label={title}>{description}</InfoHelp> : null}
      </div>
      {children}
    </section>
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
      <legend className="text-[11px] font-medium text-[var(--muted)]">{label}</legend>
      <div className="flex flex-wrap gap-2">
        {options.map((option) => (
          <label
            key={option.value}
            className={cn(
              "inline-flex min-h-11 cursor-pointer items-center gap-2 rounded-xl border px-3 text-xs font-semibold",
              value === option.value
                ? "border-[var(--text)] bg-[var(--surface-raised)] text-[var(--text)]"
                : "border-[var(--border)] text-[var(--muted)]",
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

interface PendingSubmitButtonProps {
  pending: boolean;
  pendingLabel: string;
  children: React.ReactNode;
  disabled?: boolean;
  className?: string;
}

export function PendingSubmitButton({
  pending,
  pendingLabel,
  children,
  disabled = false,
  className,
}: PendingSubmitButtonProps) {
  return (
    <button type="submit" disabled={pending || disabled} className={className}>
      {pending ? pendingLabel : children}
    </button>
  );
}
