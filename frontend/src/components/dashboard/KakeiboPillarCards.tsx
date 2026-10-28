"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";
import { useAnimatedCounter } from "@/hooks/useAnimatedCounter";
import { type KakeiboBreakdown, type KakeiboGroup, kakeiboGroups, kakeiboLabel } from "@/lib/kakeibo";

interface KakeiboPillarCardsProps {
  kakeibo?: KakeiboBreakdown;
}

const GROUP_STYLE = {
  essentials: {
    icon: "🍞",
    card: "bg-sky-500/[0.04] border-sky-500/25 hover:border-sky-500/40",
    accent: "text-sky-600 dark:text-sky-400",
    bar: "bg-sky-500",
    track: "bg-sky-500/15",
  },
  lifestyle: {
    icon: "👑",
    card: "bg-purple-500/[0.04] border-purple-500/25 hover:border-purple-500/40",
    accent: "text-purple-600 dark:text-purple-400",
    bar: "bg-purple-500",
    track: "bg-purple-500/15",
  },
  saving: {
    icon: "💎",
    card: "bg-emerald-500/[0.04] border-emerald-500/25 hover:border-emerald-500/40",
    accent: "text-emerald-600 dark:text-emerald-400",
    bar: "bg-emerald-500",
    track: "bg-emerald-500/15",
  },
} as const;

const OFF_TARGET_STYLE = {
  card: "bg-rose-500/[0.05] border-rose-500/30 hover:border-rose-500/50",
  accent: "text-rose-600 dark:text-rose-400",
  bar: "bg-rose-500",
};

function GroupCard({ group }: { group: KakeiboGroup }) {
  const { bal } = useAppCtx();
  const animated = useAnimatedCounter(group.spent);
  const style = GROUP_STYLE[group.key];
  const offTarget = !group.withinTarget;
  const pct = Math.round(group.pct);
  const targetText = `${group.targetIsMinimum ? "Min" : "Maks"}: ${group.targetPct}%`;

  return (
    <div
      className={cn(
        "card-squircle p-4 sm:p-5 transition-all flex flex-col justify-between space-y-3 shadow-xs",
        offTarget && !group.targetIsMinimum ? OFF_TARGET_STYLE.card : style.card,
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="text-xl" aria-hidden="true">{style.icon}</span>
          <div>
            <h3 className="text-xs font-bold text-[var(--text)]">{group.label}</h3>
            <div className="text-[10px] text-[var(--muted)]">{targetText}</div>
          </div>
        </div>
        <span className={cn("text-[10px] font-bold tabular", offTarget && !group.targetIsMinimum ? OFF_TARGET_STYLE.accent : style.accent)}>
          {group.withinTarget ? "Sesuai target" : group.targetIsMinimum ? "Kejar target" : "Melebihi target"}
        </span>
      </div>

      <div>
        <div className="text-lg sm:text-xl font-black text-[var(--text)] tabular select-all">{bal(animated)}</div>
        <div className="text-[10px] text-[var(--muted)] mt-0.5">{group.pillars.map(kakeiboLabel).join(" + ")}</div>
      </div>

      <div className="space-y-1">
        <div className={cn("h-2 w-full rounded-full overflow-hidden", style.track)}>
          <div
            className={cn(
              "h-full rounded-full transition-all duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] motion-reduce:transition-none",
              offTarget && !group.targetIsMinimum ? OFF_TARGET_STYLE.bar : style.bar,
            )}
            style={{ width: `${Math.min(pct, 100)}%` }}
          />
        </div>
        <div className="flex justify-between text-[9px] text-[var(--muted)] tabular">
          <span>0%</span>
          <span className="font-semibold">{pct}% / {group.targetPct}%</span>
        </div>
      </div>
    </div>
  );
}

export function KakeiboPillarCards({ kakeibo }: KakeiboPillarCardsProps) {
  const { bal } = useAppCtx();

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between px-1">
        <div className="flex items-center gap-2">
          <span className="text-base" aria-hidden="true">🎯</span>
          <h2 className="text-xs sm:text-sm font-bold tracking-tight text-[var(--text)] uppercase">
            Aturan Alokasi Finansial (50 / 30 / 20)
          </h2>
        </div>
        <span className="text-[11px] text-[var(--muted)] tabular">
          {kakeibo?.total_allocated ? `Total: ${bal(kakeibo.total_allocated)}` : "Siklus Ini"}
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3.5">
        {kakeiboGroups(kakeibo).map((group) => (
          <GroupCard key={group.key} group={group} />
        ))}
      </div>
    </div>
  );
}
