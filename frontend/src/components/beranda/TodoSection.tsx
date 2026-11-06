"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import type { IconName } from "@/components/ui/Icon";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import type { ReviewQueue } from "@/lib/review";
import type { AutoLinkedOccurrence, RecurringRule } from "@/types/recurring";
import { jakartaDay } from "./activity";
import { shortDate } from "./explanations";

const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });

/** At most this many rows under "Perlu tindakan"; the rest is reached through "Buka Tinjau" and the bell (D6). */
export const TODO_LIMIT = 3;

function when(iso: string | null, today: string): string {
  if (!iso) return "";
  const day = jakartaDay(iso);
  const time = TIME.format(new Date(iso));
  if (day === today) return `hari ini ${time}`;
  return `${shortDate(day)} ${time}`;
}

export interface TodoItem {
  key: string;
  icon: IconName;
  title: string;
  meta: ReactNode;
  /** The amount with its sign, already formatted; income amounts are green. */
  amount?: { text: string; income?: boolean };
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
  bal,
  onOpenPlans,
}: {
  queue: ReviewQueue | undefined;
  plans: RecurringRule[];
  autoLinks: AutoLinkedOccurrence[];
  today: string;
  bal: (amount: number) => string;
  onOpenPlans: () => void;
}): TodoItem[] {
  const signedAmount = (type: "expense" | "income", amount: number) =>
    type === "income" ? { text: `+${bal(amount)}`, income: true } : { text: `−${bal(amount)}` };
  const questions: TodoItem[] = (queue?.questions ?? []).map(({ transaction: tx }) => ({
    key: `q-${tx.id}`,
    icon: "help",
    title: tx.merchant || tx.notes || "Transaksi tanpa nama",
    meta: `Belum dikenali · ${when(tx.date, today)}`,
    amount: signedAmount(tx.type, tx.amount),
    href: `/review?item=${tx.id}#review-questions`,
  }));
  const duePlan = (plan: RecurringRule): TodoItem => ({
    key: `p-${plan.id}-${plan.next_due_date}`,
    icon: "calendar",
    title: plan.name,
    meta: (
      <>
        <span className="c-warning">{plan.next_due_date < today ? "Lewat jatuh tempo" : "Jatuh tempo hari ini"}</span> · Rencana
      </>
    ),
    amount: { text: bal(plan.amount) },
    onSelect: onOpenPlans,
  });
  const overdue = plans.filter((plan) => plan.next_due_date < today).map(duePlan);
  const dueToday = plans.filter((plan) => plan.next_due_date === today).map(duePlan);
  const awaiting = queue?.awaiting ?? [];
  const awaitingItems: TodoItem[] = awaiting.length
    ? [{
        key: "awaiting",
        icon: "swap",
        title: `Pasangkan ${awaiting.length} pindah saldo`,
        meta: `Pindah saldo belum lengkap · ${[...new Set(awaiting.map((item) => item.account_name))].join(", ")}`,
        href: `/review?item=${awaiting[0].id}#review-awaiting`,
      }]
    : [];
  const ambiguous = queue?.ambiguous ?? [];
  const ambiguousItems: TodoItem[] = ambiguous.length
    ? [{
        key: "ambiguous",
        icon: "swap",
        title: `${ambiguous.length} pasangan pindah saldo belum pasti`,
        meta: "Pilih pasangan yang benar",
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
        title: group.label,
        meta: `Perlu jawaban · ${group.needs_review_count} transaksi${first?.date ? ` · ${when(first.date, today)}` : ""}`,
        amount: signedAmount(group.type, group.total),
        href: `/review?item=${first?.id ?? group.key}#review-groups`,
      };
    });
  const autoLinkItems: TodoItem[] = autoLinks.length
    ? [{
        key: "auto-links",
        icon: "calendar",
        title: `${autoLinks.length} rencana ditautkan otomatis`,
        meta: "Cek dan batalkan kalau keliru",
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
      meta: `${plan.next_due_date === tomorrow ? "Besok" : shortDate(plan.next_due_date)} · Rencana`,
      amount: { text: bal(plan.amount) },
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
  const shown = visibleTodoItems(items);
  return (
    <section aria-labelledby="beranda-todo">
      <SectionHeader
        id="beranda-todo"
        label="Perlu tindakan"
        aside={
          <Link href="/review" className="ws-linkbtn">
            {attention > 0 ? `Buka Tinjau · ${attention}` : "Buka Tinjau"}
          </Link>
        }
      />
      {error && <p className="t-meta">Daftar Tinjau belum bisa dimuat.</p>}
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
                amount={item.amount && <MaskedMoney text={item.amount.text} className={item.amount.income ? "t-amt c-income" : "t-amt"} />}
                chevron
              />
            </li>
          ))}
        </ul>
      ) : (
        !error && <p className="t-body muted">Tidak ada yang perlu dijawab.</p>
      )}
    </section>
  );
}
