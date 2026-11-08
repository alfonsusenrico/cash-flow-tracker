"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useTranslations } from "next-intl";
import type { IconName } from "@/components/ui/Icon";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { useMoney } from "@/components/workspace/money";
import type { Words } from "@/lib/i18n/types";
import type { ReviewQueue } from "@/lib/review";
import type { AutoLinkedOccurrence, RecurringRule } from "@/types/recurring";
import { KindAmount, jakartaDay, type AmountKind } from "@/components/transactions/rows";

/** At most this many rows under "Perlu tindakan"; the rest is reached through "Buka Tinjau" and the bell (D6). */
export const TODO_LIMIT = 3;

function when(iso: string | null, today: string, { t, format }: Words): string {
  if (!iso) return "";
  const day = jakartaDay(iso);
  const time = format.time(iso);
  if (day === today) return t("home.todayAt", { time });
  return t("home.dateAt", { date: format.shortDate(day), time });
}

/** A review group's label: the stored text, or the fixed fallback from its key (add-language-system D7). */
function groupLabel(group: ReviewQueue["groups"][number], { t }: Words): string {
  if (group.label_source !== "system") return group.label;
  return group.label_key === "uncategorized" ? t("terms.noCategory") : t("terms.noDescription");
}

export interface TodoItem {
  key: string;
  icon: IconName;
  title: string;
  meta: ReactNode;
  /** The amount and its kind: spending is red with "−", income green with "+", a plan's amount plain (D29). */
  amount?: { value: number; kind: AmountKind };
  href?: string;
  onSelect?: () => void;
}

/**
 * Everything that needs the owner, ranked as D6 orders it: open questions, plans past due, plans due today,
 * awaiting transfers, ambiguous pairs, groups, the automatic-link check, then plans due within two days. Plans
 * arrive sorted by due date.
 */
export function todoItems({
  queue,
  plans,
  autoLinks,
  today,
  words,
  onOpenPlans,
}: {
  queue: ReviewQueue | undefined;
  plans: RecurringRule[];
  autoLinks: AutoLinkedOccurrence[];
  today: string;
  words: Words;
  onOpenPlans: () => void;
}): TodoItem[] {
  const { t, format } = words;
  const signedAmount = (type: "expense" | "income", amount: number): TodoItem["amount"] => ({ value: amount, kind: type });
  const questions: TodoItem[] = (queue?.questions ?? []).map(({ transaction: tx }) => ({
    key: `q-${tx.id}`,
    icon: "help",
    title: tx.merchant || tx.notes || t("home.unnamedTransaction"),
    meta: t("home.notRecognizedWhen", { when: when(tx.date, today, words) }),
    amount: signedAmount(tx.type, tx.amount),
    href: `/review?item=${tx.id}#review-questions`,
  }));
  const duePlan = (plan: RecurringRule): TodoItem => ({
    key: `p-${plan.id}-${plan.next_due_date}`,
    icon: "calendar",
    title: plan.name,
    meta: (
      <>
        <span className="c-warning">{plan.next_due_date < today ? t("home.planOverdue") : t("home.planDueToday")}</span> · {t("home.planMark")}
      </>
    ),
    amount: { value: plan.amount, kind: "plain" },
    onSelect: onOpenPlans,
  });
  const overdue = plans.filter((plan) => plan.next_due_date < today).map(duePlan);
  const dueToday = plans.filter((plan) => plan.next_due_date === today).map(duePlan);
  const awaiting = queue?.awaiting ?? [];
  const awaitingItems: TodoItem[] = awaiting.length
    ? [{
        key: "awaiting",
        icon: "swap",
        title: t("home.pairTransfers", { count: awaiting.length }),
        meta: t("home.transferNotComplete", { accounts: [...new Set(awaiting.map((item) => item.account_name))].join(", ") }),
        href: `/review?item=${awaiting[0].id}#review-awaiting`,
      }]
    : [];
  const ambiguous = queue?.ambiguous ?? [];
  const ambiguousItems: TodoItem[] = ambiguous.length
    ? [{
        key: "ambiguous",
        icon: "swap",
        title: t("home.uncertainPairs", { count: ambiguous.length }),
        meta: t("home.choosePair"),
        href: `/review?item=${ambiguous[0].id}#review-ambiguous`,
      }]
    : [];
  const groups: TodoItem[] = (queue?.groups ?? [])
    .filter((group) => group.needs_review_count > 0)
    .map((group) => {
      const first = group.transactions[0];
      return {
        key: `g-${group.key}`,
        icon: "help",
        title: groupLabel(group, words),
        meta: first?.date
          ? t("home.needsAnswerCountWhen", { count: group.needs_review_count, when: when(first.date, today, words) })
          : t("home.needsAnswerCount", { count: group.needs_review_count }),
        amount: signedAmount(group.type, group.total),
        href: `/review?item=${first?.id ?? group.key}#review-groups`,
      };
    });
  const autoLinkItems: TodoItem[] = autoLinks.length
    ? [{
        key: "auto-links",
        icon: "calendar",
        title: t("home.plansAutoLinked", { count: autoLinks.length }),
        meta: t("home.checkAutoLinks"),
        onSelect: onOpenPlans,
      }]
    : [];
  const tomorrow = jakartaDay(new Date(Date.parse(`${today}T12:00:00+07:00`) + 86_400_000));
  const upcoming: TodoItem[] = plans
    .filter((plan) => plan.next_due_date > today)
    .map((plan) => ({
      key: `p-${plan.id}-${plan.next_due_date}`,
      icon: "calendar",
      title: plan.name,
      meta: t("home.planWhen", { when: plan.next_due_date === tomorrow ? t("home.tomorrow") : format.shortDate(plan.next_due_date) }),
      amount: { value: plan.amount, kind: "plain" },
      href: "/goals",
    }));
  return [...questions, ...overdue, ...dueToday, ...awaitingItems, ...ambiguousItems, ...groups, ...autoLinkItems, ...upcoming];
}

/** The rows Beranda shows: the first three of the ranking. */
export function visibleTodoItems(items: TodoItem[]): TodoItem[] {
  return items.slice(0, TODO_LIMIT);
}

/** "Perlu tindakan" (D6): at most three whole-row items, "Buka Tinjau · N", or one calm line. */
export function TodoSection({ attention, items, error }: { attention: number; items: TodoItem[]; error: boolean }) {
  const t = useTranslations("home");
  const money = useMoney();
  const shown = visibleTodoItems(items);
  return (
    <section aria-labelledby="beranda-todo">
      <SectionHeader
        id="beranda-todo"
        label={t("needsAction")}
        aside={
          <Link href="/review" className="ws-linkbtn">
            {attention > 0 ? t("openReviewCount", { n: String(attention) }) : t("openReview")}
          </Link>
        }
      />
      {error && <p className="t-meta">{t("reviewLoadError")}</p>}
      {shown.length > 0 ? (
        <ul className="list">
          {shown.map((item) => (
            <li key={item.key}>
              <ListRow
                href={item.href}
                onClick={item.onSelect}
                icon={item.icon}
                title={item.title}
                meta={item.meta}
                amount={item.amount && <KindAmount kind={item.amount.kind} amount={item.amount.value} money={money} />}
                chevron
              />
            </li>
          ))}
        </ul>
      ) : (
        !error && <p className="t-body muted">{t("nothingToAnswer")}</p>
      )}
    </section>
  );
}
