"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import {
  KAKEIBO_PILLARS,
  type KakeiboBreakdown,
  isKakeiboHealthy,
  kakeiboGroups,
  kakeiboLabel,
  kakeiboStatusLabel,
  pillarPct,
  pillarSpent,
} from "@/lib/kakeibo";
import { cn } from "@/lib/utils";

const GROUP_ICONS = { essentials: "🍞", lifestyle: "👑", saving: "💰" } as const;

export function KakeiboBenchmark({ kakeibo }: { kakeibo?: KakeiboBreakdown }) {
  const { bal } = useAppCtx();
  const healthy = isKakeiboHealthy(kakeibo?.kakeibo_status);
  const groups = kakeiboGroups(kakeibo);
  const barSummary = KAKEIBO_PILLARS.map((pillar) => `${pillar.label} ${pillarPct(kakeibo, pillar.value)}%`).join(", ");

  return (
    <div className="cockpit-card p-5 sm:p-6 space-y-5 border border-[var(--border)] shadow-xs">
      <div>
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-base" aria-hidden="true">🇯🇵</span>
          <h2 className="text-base font-bold tracking-tight text-[var(--text)]">Metode Kakeibo (50 / 30 / 20)</h2>
          <span
            className={cn(
              "text-[10px] font-bold px-2 py-0.5 rounded-full border uppercase",
              healthy
                ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20"
                : "bg-amber-500/10 text-amber-700 dark:text-amber-400 border-amber-500/20"
            )}
          >
            {kakeiboStatusLabel(kakeibo?.kakeibo_status)}
          </span>
        </div>
        <p className="text-xs text-[var(--muted)] mt-0.5">
          50% kebutuhan pokok (Kebutuhan + Tak Terduga), 30% gaya hidup (Keinginan + Budaya), 20% tabungan
        </p>
      </div>

      <div className="space-y-1.5">
        <div
          role="img"
          aria-label={`Alokasi pilar: ${barSummary}`}
          className="h-3.5 w-full rounded-full bg-[var(--surface-raised)] border border-[var(--border)] flex overflow-hidden"
        >
          {KAKEIBO_PILLARS.map((pillar) => (
            <div
              key={pillar.value}
              style={{ width: `${Math.min(pillarPct(kakeibo, pillar.value), 100)}%`, backgroundColor: pillar.color }}
              className="h-full transition-[width] motion-reduce:transition-none"
            />
          ))}
        </div>
        <ul className="flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-[var(--muted)] tabular font-medium">
          {KAKEIBO_PILLARS.map((pillar) => (
            <li key={pillar.value} className="flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: pillar.color }} aria-hidden="true" />
              {pillar.label} ({pillarPct(kakeibo, pillar.value)}%)
            </li>
          ))}
        </ul>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {groups.map((group) => (
          <div key={group.key} className="p-4 rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)]/40 space-y-3">
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <span className="text-base" aria-hidden="true">{GROUP_ICONS[group.key]}</span>
                <div>
                  <h3 className="text-xs font-bold text-[var(--text)]">{group.label}</h3>
                  <div className="text-[10px] text-[var(--muted)]">
                    Target: {group.targetIsMinimum ? "≥" : "≤"} {group.targetPct}%
                  </div>
                </div>
              </div>
              <span
                className={cn(
                  "text-[10px] font-bold px-2 py-0.5 rounded-full border shrink-0",
                  group.withinTarget
                    ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20"
                    : "bg-rose-500/10 text-rose-600 dark:text-rose-400 border-rose-500/20"
                )}
              >
                {group.withinTarget ? "Sesuai target" : group.targetIsMinimum ? "Di bawah target" : "Melebihi target"}
              </span>
            </div>

            <div>
              <div className="text-xl font-bold text-[var(--text)] tabular">{bal(group.spent)}</div>
              <div className="text-[11px] text-[var(--muted)] mt-0.5 tabular">{group.pct}% dari total uang masuk</div>
            </div>

            {group.pillars.length > 1 && (
              <ul className="space-y-0.5 text-[11px] text-[var(--muted)] tabular">
                {group.pillars.map((pillar) => (
                  <li key={pillar} className="flex justify-between gap-2">
                    <span>{kakeiboLabel(pillar)}</span>
                    <span>{bal(pillarSpent(kakeibo, pillar))}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
