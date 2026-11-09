import { coverageText, essentialBasisText, type EmergencyRunway } from "@/components/dashboard/EmergencyCoverage";
import type { ExplanationLine } from "@/components/workspace/explain";
import type { RincianData, RincianLine } from "@/components/workspace/ExplanationDialog";
import type { Format } from "@/lib/format";
import type { Translate } from "@/lib/i18n/types";
import { ledgerHref } from "@/lib/ledgerFilters";
import type { GoalItem } from "./types";

/**
 * The Rincian of Ketahanan Dana (apply-markas-target D4, `emergency-cost-estimation` "Essential cost basis
 * disclosure"): the reserve and the goals it comes from, the essential monthly cost with the periods behind it
 * linked to their ledger rows, and the coverage. Every figure is the server's `runway`; nothing is added up here.
 */
export function safetyNetRincian(
  runway: EmergencyRunway,
  goals: GoalItem[],
  { t, format, money }: { t: Translate; format: Format; money: (amount: number) => string },
): RincianData {
  const cost = runway.essential_cost;
  const allCash = runway.balance_basis === "all_liquid";
  const names = new Set(runway.emergency_goal_names ?? []);
  const reserveParts: ExplanationLine[] = allCash
    ? []
    : goals.filter((goal) => goal.is_emergency && names.has(goal.name)).map((goal) => ({ label: goal.name, amount: money(goal.current_amount) }));

  const periodParts: ExplanationLine[] = (cost?.periods_used ?? []).flatMap((period) => {
    const unclassified = period.unclassified_amount ?? 0;
    const label = `${period.label} ${t("breakdown.periodDays", { count: period.days })}`;
    const need: ExplanationLine = {
      label,
      amount: money(period.amount - unclassified),
      href: ledgerHref({ from: period.start, to: period.end, kakeibo: "need", type: "expense" }),
    };
    if (unclassified <= 0) return [need];
    return [
      need,
      {
        label: `${t("breakdown.plusNoPillarYet")} · ${period.label}`,
        amount: money(unclassified),
        href: ledgerHref({ from: period.start, to: period.end, kakeibo: "unclassified", type: "expense" }),
      },
    ];
  });
  const costNotes = [essentialBasisText(cost, t)];
  if (cost?.limited_history) costNotes.push(t("breakdown.limitedHistory", { count: cost.period_count }));
  if (cost && cost.debt_minimum > 0) costNotes.push(t("breakdown.includesMinimumInstallments", { amount: money(cost.debt_minimum) }));
  if (periodParts.length) costNotes.push(t("breakdown.periodsRule"));

  const reserve: RincianLine = {
    key: "reserve",
    label: t("breakdown.reserveBalance"),
    amount: money(runway.emergency_fund_balance),
    parts: reserveParts.length ? reserveParts : undefined,
  };
  const essential: RincianLine =
    runway.monthly_primary_expense == null
      ? { key: "essential", label: t("breakdown.monthlyEssentialCosts"), amount: t("breakdown.unknown"), plain: true }
      : {
          key: "essential",
          label: t("breakdown.monthlyEssentialCosts"),
          amount: money(runway.monthly_primary_expense),
          parts: periodParts.length ? periodParts : undefined,
          partNotes: periodParts.length ? costNotes : undefined,
        };

  return {
    title: t("terms.safetyNet"),
    rule: t("breakdown.safetyNetRule"),
    lines: [reserve, essential],
    total: { label: t("terms.safetyNet"), amount: coverageText(runway, t, format), plain: true },
    // Without periods the basis sentence has no parts to sit under, so it joins the notes.
    notes: [...(allCash ? [t("breakdown.noDedicatedFund")] : []), ...(periodParts.length ? [] : costNotes)],
  };
}
