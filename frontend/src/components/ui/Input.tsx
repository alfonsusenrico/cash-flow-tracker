import { cn } from "@/lib/utils";

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

export function Input({ label, error, className, id, ...props }: InputProps) {
  const inputId = id ?? label?.toLowerCase().replace(/\s+/g, "-");
  return (
    <div className="flex flex-col gap-1">
      {label && <label htmlFor={inputId} className="text-xs font-medium text-[var(--muted)]">{label}</label>}
      <input
        id={inputId}
        {...props}
        className={cn(
          "w-full border border-[var(--border)] rounded-[var(--radius-md)] px-3 py-2 text-sm bg-[var(--surface)] text-[var(--text)] placeholder:text-[var(--muted)] focus:outline-none focus:ring-2 focus:ring-[var(--color-focus)]",
          error && "border-[var(--danger)]",
          className
        )}
      />
      {error && <p className="text-xs text-[var(--danger)]">{error}</p>}
    </div>
  );
}

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string;
}

export function Select({ label, className, id, children, ...props }: SelectProps) {
  const inputId = id ?? label?.toLowerCase().replace(/\s+/g, "-");
  return (
    <div className="flex flex-col gap-1">
      {label && <label htmlFor={inputId} className="text-xs font-medium text-[var(--muted)]">{label}</label>}
      <select
        id={inputId}
        {...props}
        className={cn(
          "w-full border border-[var(--border)] rounded-[var(--radius-md)] px-3 py-2 text-sm bg-[var(--surface)] text-[var(--text)] focus:outline-none focus:ring-2 focus:ring-[var(--color-focus)]",
          className
        )}
      >
        {children}
      </select>
    </div>
  );
}
