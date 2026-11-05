import { cn } from "@/lib/utils";

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

// Field geometry from SPEC §7: 40 px, 12 px sides, radius 6, a 1 px strong-line edge on the dialog surface.
// Focus, hover and error edges come from the global field rules in globals.css.
export function Input({ label, error, className, id, ...props }: InputProps) {
  const inputId = id ?? label?.toLowerCase().replace(/\s+/g, "-");
  const errorId = error && inputId ? `${inputId}-error` : undefined;
  return (
    <div className="flex flex-col gap-2">
      {label && <label htmlFor={inputId} className="text-xs font-medium text-[var(--muted)]">{label}</label>}
      <input
        id={inputId}
        aria-invalid={error ? true : undefined}
        aria-describedby={errorId}
        {...props}
        className={cn(
          "h-10 w-full rounded-md border border-[var(--line-strong)] bg-[var(--dialog)] px-3 text-sm text-[var(--text)]",
          className
        )}
      />
      {error && <p id={errorId} className="text-xs text-[var(--out-fg)]">{error}</p>}
    </div>
  );
}

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string;
}

export function Select({ label, className, id, children, ...props }: SelectProps) {
  const inputId = id ?? label?.toLowerCase().replace(/\s+/g, "-");
  return (
    <div className="flex flex-col gap-2">
      {label && <label htmlFor={inputId} className="text-xs font-medium text-[var(--muted)]">{label}</label>}
      <select
        id={inputId}
        {...props}
        className={cn(
          "ws-select h-10 w-full rounded-md border border-[var(--line-strong)] bg-[var(--dialog)] pl-3 pr-9 text-sm text-[var(--text)]",
          className
        )}
      >
        {children}
      </select>
    </div>
  );
}
