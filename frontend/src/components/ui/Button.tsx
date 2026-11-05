import { forwardRef } from "react";
import { cn } from "@/lib/utils";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "danger" | "ghost";
  size?: "sm" | "md";
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { variant = "secondary", size = "md", className, children, ...props },
  ref,
) {
  return (
    <button
      ref={ref}
      {...props}
      className={cn(
        // Button geometry from SPEC §7: 36 px (44 at phone width), 16 px sides, 8 px gap, radius 6, 14/500.
        "inline-flex items-center justify-center gap-2 rounded-md border text-sm font-medium whitespace-nowrap transition-colors disabled:cursor-not-allowed disabled:opacity-45",
        size === "sm" ? "min-h-7 px-2" : "min-h-11 px-4 sm:min-h-9",
        variant === "primary" && "border-transparent bg-[var(--accent)] text-[var(--accent-ink)] hover:bg-[var(--accent-hover)] active:translate-y-px active:bg-[var(--accent-pressed)]",
        variant === "secondary" && "border-[var(--line-strong)] bg-transparent text-[var(--text)] hover:bg-[var(--surface)] active:bg-[var(--surface-2)]",
        variant === "danger" && "border-transparent bg-[var(--danger-solid)] text-[var(--danger-contrast)] hover:brightness-[.92]",
        variant === "ghost" && "border-transparent px-3 text-[var(--muted)] hover:bg-[var(--surface)] hover:text-[var(--text)] active:bg-[var(--surface-2)]",
        className
      )}
    >
      {children}
    </button>
  );
});
