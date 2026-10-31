import { cn } from "@/lib/utils";
import { settingsSection, type SettingsSectionSlug } from "@/components/settings/sections";

/** Width shared by the short text and number fields of a settings section. */
export const SETTINGS_FIELD_WIDTH = "w-full max-w-md";

export const settingsControlClass =
  "min-h-11 w-full rounded-[var(--radius-md)] border border-[var(--border-strong)] bg-[var(--surface)] px-3 py-2 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)] disabled:opacity-50";

/**
 * Confirmed destructive action. `rose-700` maps to the expense hover token, which is light coral in dark
 * mode, so dark mode uses Tailwind's rose 800 to keep white text above 4.5:1.
 */
export const confirmDangerClass = "bg-rose-700 text-white hover:bg-rose-800 dark:bg-rose-800 dark:hover:bg-rose-900";

/** Quiet destructive text button; rose 700/300 keep 4.5:1 on both surfaces where the coral token does not. */
export const quietDangerClass =
  "inline-flex min-h-11 min-w-11 items-center justify-center rounded-[var(--radius-md)] px-3 text-xs font-semibold text-rose-700 hover:bg-rose-500/10 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)] disabled:opacity-50 dark:text-rose-300";

interface SettingsCardProps {
  section: SettingsSectionSlug;
  children: React.ReactNode;
  footer?: React.ReactNode;
}

/** One settings section: title, one-line purpose, body and, when it saves something, its own actions. */
export function SettingsCard({ section, children, footer }: SettingsCardProps) {
  const { title, purpose } = settingsSection(section);
  const headingId = `settings-${section}-title`;
  return (
    <section aria-labelledby={headingId} className="card-squircle text-sm">
      <div className="space-y-5 p-4 sm:p-6">
        <header className="space-y-1">
          <h2
            id={headingId}
            tabIndex={-1}
            data-settings-heading
            className="text-base font-bold tracking-tight text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)]"
          >
            {title}
          </h2>
          <p className="text-[var(--text-secondary)]">{purpose}</p>
        </header>
        {children}
      </div>
      {footer && (
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-[var(--border)] px-4 py-4 sm:px-6">
          {footer}
        </div>
      )}
    </section>
  );
}

interface SettingsSubsectionProps {
  id: string;
  headingRef?: React.Ref<HTMLHeadingElement>;
  title: string;
  description?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}

/** A titled group inside a section card, separated from the previous group by a rule. */
export function SettingsSubsection({ id, headingRef, title, description, action, children, className }: SettingsSubsectionProps) {
  return (
    <section aria-labelledby={id} className={cn("space-y-3 border-t border-[var(--border)] pt-5 first:border-t-0 first:pt-0", className)}>
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-2">
        <div className="min-w-0 flex-1 space-y-0.5">
          <h3 ref={headingRef} id={id} tabIndex={-1} className="text-sm font-semibold text-[var(--text)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-focus)]">
            {title}
          </h3>
          {description && <p className="text-xs text-[var(--muted)]">{description}</p>}
        </div>
        {action && <div className="shrink-0">{action}</div>}
      </div>
      {children}
    </section>
  );
}

interface SettingsFieldProps {
  id: string;
  label: string;
  help?: React.ReactNode;
  error?: string;
  aside?: React.ReactNode;
  className?: string;
  children: (props: { id: string; "aria-describedby"?: string; "aria-invalid"?: true }) => React.ReactNode;
}

/** Label above the control, help and error below it, both announced with the control. */
export function SettingsField({ id, label, help, error, aside, className, children }: SettingsFieldProps) {
  const helpId = help ? `${id}-help` : undefined;
  const errorId = error ? `${id}-error` : undefined;
  const describedBy = [helpId, errorId].filter(Boolean).join(" ") || undefined;
  return (
    <div className={cn("space-y-1.5", className)}>
      <div className="flex flex-wrap items-baseline justify-between gap-x-3">
        <label htmlFor={id} className="text-sm font-semibold text-[var(--text)]">{label}</label>
        {aside}
      </div>
      {children({ id, "aria-describedby": describedBy, "aria-invalid": error ? true : undefined })}
      {help && <p id={helpId} className="text-xs text-[var(--muted)]">{help}</p>}
      {error && <p id={errorId} className="text-xs font-medium text-rose-700 dark:text-rose-300">{error}</p>}
    </div>
  );
}

/** Save feedback at the left of a card's actions; a fresh save wins until the owner edits again. */
export function SettingsSaveStatus({ dirty, saved }: { dirty: boolean; saved: boolean }) {
  return (
    <p role="status" className="mr-auto text-xs text-[var(--muted)]">
      {saved ? "Tersimpan." : dirty ? "Ada perubahan yang belum disimpan." : ""}
    </p>
  );
}

export function SettingsAlert({ children }: { children: React.ReactNode }) {
  return (
    <p role="alert" className="rounded-[var(--radius-md)] border border-rose-500/25 bg-rose-500/10 px-3 py-2 text-xs font-medium text-rose-700 dark:text-rose-300">
      {children}
    </p>
  );
}

/** Bordered list of rows used for devices, mappings and sender names. */
export function SettingsList({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <ul aria-label={label} className="divide-y divide-[var(--border)] rounded-[var(--radius-md)] border border-[var(--border)]">
      {children}
    </ul>
  );
}
