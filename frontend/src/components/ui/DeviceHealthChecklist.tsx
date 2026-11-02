"use client";

import type { PairedDevice } from "@/lib/api";
import {
  BATTERY_HINT,
  FIX_ON_PHONE,
  READY_MESSAGE,
  STEP_STATE_TEXT,
  type ChecklistStepState,
  type PairingChecklist,
} from "@/lib/deviceHealth";
import { Icon } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

// Badge text sits on the soft fills in the family's darker ink (or --text for amber, which has no
// text ink), so each state reads at 4.5:1 or better without relying on hue alone.
const STATE_STYLE: Record<ChecklistStepState, { icon: string; mark: string; badge: string }> = {
  ok: {
    icon: "check",
    mark: "bg-[var(--color-income-soft)] text-[var(--income-text)]",
    badge: "bg-[var(--color-income-soft)] text-[var(--income-text)]",
  },
  fail: {
    icon: "x",
    mark: "bg-[var(--color-expense-soft)] text-[var(--danger-text)]",
    badge: "bg-[var(--color-expense-soft)] text-[var(--danger-text)]",
  },
  wait: {
    icon: "smartphone",
    mark: "bg-[var(--color-warning-soft)] text-[var(--text)]",
    badge: "bg-[var(--color-warning-soft)] text-[var(--text)]",
  },
  todo: {
    icon: "minus-circle",
    mark: "bg-[var(--surface)] text-[var(--muted)]",
    badge: "bg-[var(--surface)] text-[var(--muted)]",
  },
};

function Pulse() {
  return (
    <span
      aria-hidden="true"
      className="h-2.5 w-2.5 shrink-0 rounded-full bg-[var(--color-warning)] motion-safe:animate-pulse"
    />
  );
}

interface DeviceHealthChecklistProps {
  checklist: PairingChecklist;
  device: PairedDevice | undefined;
}

/** The pairing dialog's live "Status HP-mu" card; announcements are made by the dialog's live region. */
export function DeviceHealthChecklist({ checklist, device }: DeviceHealthChecklistProps) {
  let headline = "Menunggu HP…";
  let detail: string | null = "Halaman ini berubah sendiri begitu HP-mu nyambung. Nggak perlu di-refresh.";
  if (checklist.stage === "setting-up") {
    headline = "Terhubung! Lanjutin pengaturannya di HP, ya.";
    detail = device ? `${device.name} lagi nuntun kamu langkah demi langkah.` : null;
  } else if (checklist.stage === "failed" && checklist.failed) {
    headline = `Ada yang belum beres: ${checklist.failed.label}`;
    detail = null;
  } else if (checklist.stage === "ready") {
    headline = READY_MESSAGE;
    detail = device?.health
      ? `${device.name} · aplikasi pendamping v${device.health.app_version} · tes notifikasi ketangkep dan nggak dicatat jadi transaksi.`
      : null;
  }
  const busy = checklist.stage === "waiting" || checklist.stage === "setting-up";

  return (
    <section
      aria-labelledby="device-health-title"
      className="space-y-2.5 rounded-[var(--radius-lg)] border border-[var(--border-strong)] bg-[var(--surface)] p-3.5"
    >
      <h3 id="device-health-title" className="text-[11px] font-bold uppercase tracking-wider text-[var(--text-secondary)]">
        Status HP-mu
      </h3>
      <p className="flex items-center gap-2.5 text-sm font-bold text-[var(--text)]">
        {busy && <Pulse />}
        <span>{headline}</span>
      </p>
      {detail && <p className="text-xs text-[var(--text-secondary)]">{detail}</p>}

      <ul className="grid gap-1.5" aria-label="Langkah pengaturan HP">
        {checklist.steps.map((step) => {
          const style = STATE_STYLE[step.state];
          const showBattery = step.key === "background" && checklist.batteryRestricted;
          const showTestProblem = step.key === "selfTest" && step.state === "fail" && checklist.selfTestProblem;
          return (
            <li
              key={step.key}
              data-state={step.state}
              className="rounded-[var(--radius-sm)] bg-[var(--bg)] px-2.5 py-2"
            >
              <div className="grid grid-cols-[28px_minmax(0,1fr)_auto] items-center gap-2.5">
                <span aria-hidden="true" className={cn("grid h-7 w-7 place-items-center rounded-full", style.mark)}>
                  <Icon name={style.icon} className="h-4 w-4" strokeWidth={2.5} />
                </span>
                <span className="text-sm font-semibold text-[var(--text)]">{step.label}</span>
                <span className={cn("rounded-full px-2 py-0.5 text-[11px] font-bold", style.badge)}>
                  {STEP_STATE_TEXT[step.state]}
                </span>
              </div>
              {showTestProblem && (
                <p className="mt-1.5 pl-[38px] text-xs text-[var(--text)]">{checklist.selfTestProblem}</p>
              )}
              {showBattery && (
                <p className="mt-1.5 rounded-[var(--radius-xs)] bg-[var(--color-warning-soft)] px-2.5 py-2 text-xs text-[var(--text)]">
                  <span className="font-bold">Saran: </span>
                  {BATTERY_HINT}
                </p>
              )}
            </li>
          );
        })}
      </ul>

      {checklist.stage === "failed" && (
        <p className="rounded-[var(--radius-sm)] bg-[var(--color-expense-soft)] px-3 py-2.5 text-xs text-[var(--text)]">
          {FIX_ON_PHONE}
        </p>
      )}
    </section>
  );
}
