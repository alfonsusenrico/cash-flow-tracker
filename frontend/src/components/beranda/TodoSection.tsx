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
/** Review items shown before plans take the remaining places. */
const REVIEW_FIRST = 2;

function when(iso: string | null, today: string): string {
  if (!iso) return "";
  const day = jakartaDay(iso);
  const time = TIME.format(new Date(iso));
  if (day === today) return `hari ini ${time}`;
  return `${shortDate(day)} ${time}`;
}

export interface TodoItem {
  key: string;
  kind: "review" | "plan";
  icon: IconName;
  title: string;
  meta: ReactNode;
  /** The amount with its sign, already formatted; income amounts are green. */
  amount?: { text: string; income?: boolean };
  href?: string;
  onSelect?: () => void;
}

/** Everything that needs the owner, in the D6 order: review items (questions, transfers, groups), then plans. */
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
  const items: TodoItem[] = [];
  const signedAmount = (type: "expense" | "income", amount: number) =>
    type === "income" ? { text: `+${bal(amount)}`, income: true } : { text: `−${bal(amount)}` };
  for (const question of queue?.questions ?? []) {
    const tx = question.transaction;
    items.push({
      key: `q-${tx.id}`,
      kind: "review",
      icon: "help",
      title: tx.merchant || tx.notes || "Transaksi tanpa nama",
      meta: `Belum dikenali · ${when(tx.date, today)}`,
      amount: signedAmount(tx.type, tx.amount),
      href: `/review?item=${tx.id}#review-questions`,
    });
  }
  const awaiting = queue?.awaiting ?? [];
  if (awaiting.length) {
    const accounts = [...new Set(awaiting.map((item) => item.account_name))].join(", ");
    items.push({
      key: "awaiting",
      kind: "review",
      icon: "swap",
      title: `Pasangkan ${awaiting.length} pindah saldo`,
      meta: `Pindah saldo belum lengkap · ${accounts}`,
      href: `/review?item=${awaiting[0].id}#review-awaiting`,
    });
  }
  const ambiguous = queue?.ambiguous ?? [];
  if (ambiguous.length) {
    items.push({
      key: "ambiguous",
      kind: "review",
      icon: "swap",
      title: `${ambiguous.length} pasangan pindah saldo belum pasti`,
      meta: "Pilih pasangan yang benar",
      href: `/review?item=${ambiguous[0].id}#review-ambiguous`,
    });
  }
  for (const group of (queue?.groups ?? []).filter((item) => item.needs_review_count > 0)) {
    const first = group.transactions[0];
    items.push({
      key: `g-${group.key}`,
      kind: "review",
      icon: "help",
      title: group.label,
      meta: `Perlu jawaban · ${group.needs_review_count} transaksi${first?.date ? ` · ${when(first.date, today)}` : ""}`,
      amount: signedAmount(group.type, group.total),
      href: `/review?item=${first?.id ?? group.key}#review-groups`,
    });
  }
  if (autoLinks.length) {
    items.push({
      key: "auto-links",
      kind: "plan",
      icon: "calendar",
      title: `${autoLinks.length} rencana ditautkan otomatis`,
      meta: "Cek dan batalkan kalau keliru",
      onSelect: onOpenPlans,
    });
  }
  const tomorrow = jakartaDay(new Date(Date.parse(`${today}T12:00:00+07:00`) + 86_400_000));
  for (const plan of plans) {
    const due = plan.next_due_date;
    const amount = { text: bal(plan.amount) };
    if (due <= today) {
      items.push({
        key: `p-${plan.id}-${due}`,
        kind: "plan",
        icon: "calendar",
        title: plan.name,
        meta: (
          <>
            <span className="c-warning">{due < today ? "Lewat jatuh tempo" : "Jatuh tempo hari ini"}</span> · Rencana
          </>
        ),
        amount,
        onSelect: onOpenPlans,
      });
    } else {
      items.push({
        key: `p-${plan.id}-${due}`,
        kind: "plan",
        icon: "calendar",
        title: plan.name,
        meta: `${due === tomorrow ? "Besok" : shortDate(due)} · Rencana`,
        amount,
        href: "/goals",
      });
    }
  }
  return items;
}

/** The rows Beranda shows: up to two review items, then plans up to three; review items fill free places. */
export function visibleTodoItems(items: TodoItem[]): TodoItem[] {
  const review = items.filter((item) => item.kind === "review");
  const plans = items.filter((item) => item.kind === "plan");
  const first = review.slice(0, REVIEW_FIRST);
  const planned = plans.slice(0, TODO_LIMIT - first.length);
  const filler = review.slice(REVIEW_FIRST, REVIEW_FIRST + TODO_LIMIT - first.length - planned.length);
  return [...first, ...filler, ...planned];
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
