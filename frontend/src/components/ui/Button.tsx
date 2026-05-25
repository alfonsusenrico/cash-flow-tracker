import { cn } from "@/lib/utils";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "danger" | "ghost";
  size?: "sm" | "md";
}

export function Button({ variant = "secondary", size = "md", className, children, ...props }: ButtonProps) {
  return (
    <button
      {...props}
      className={cn(
        "inline-flex items-center justify-center font-medium rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed",
        size === "sm" ? "px-3 py-1.5 text-xs" : "px-4 py-2 text-sm",
        variant === "primary" && "bg-[var(--primary)] hover:bg-[var(--primary-hover)] text-white",
        variant === "secondary" && "bg-[var(--surface)] border border-[var(--border)] hover:bg-[var(--bg)] text-[var(--text)]",
        variant === "danger" && "bg-[var(--danger)] hover:bg-[var(--danger-hover)] text-white",
        variant === "ghost" && "text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--bg)]",
        className
      )}
    >
      {children}
    </button>
  );
}
