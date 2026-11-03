"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { Card, CardHeader, Pill } from "@/components/workspace/primitives";
import type { ReviewQueue } from "@/lib/review";
import type { AutoLinkedOccurrence, RecurringRule } from "@/types/recurring";
import { jakartaDay } from "./activity";
import { shortDate } from "./explanations";

const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });

function when(iso: string | null, today: string): string {
  if (!iso) return "";
  const day = jakartaDay(iso);
  const time = TIME.format(new Date(iso));
  if (day === today) return `hari ini ${time}`;
  return `${shortDate(day)} ${time}`;
}

export interface TodoItem {
  key: string;
  title: string;
  detail: string;
  action: ReactNode;
}

const ACTION = "ws-act";

/** Review items first (questions, transfers, groups), then plans due today or within two days. */
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
  const reviewLink = (id: string, section: string, label: string, tone: string, name: string) => (
    <Link href={`/review?item=${id}#${section}`} className={`${ACTION} ${tone}`} aria-label={`${label}: ${name}`}>
      {label}
    </Link>
  );
  for (const question of queue?.questions ?? []) {
    const tx = question.transaction;
    const name = tx.merchant || tx.notes || "transaksi tanpa nama";
    items.push({
      key: `q-${tx.id}`,
      title: `Belum dikenali: ${name} ${bal(tx.amount)}`,
      detail: `${tx.account_name} · ${when(tx.date, today)}`,
      action: reviewLink(tx.id, "review-questions", "Jawab", "ws-tone-out", name),
    });
  }
  const awaiting = queue?.awaiting ?? [];
  if (awaiting.length) {
    const accounts = [...new Set(awaiting.map((item) => item.account_name))].join(", ");
    items.push({
      key: "awaiting",
      title: `Pasangkan ${awaiting.length} pindah saldo`,
      detail: `${accounts} · tujuan belum ditemukan`,
      action: reviewLink(awaiting[0].id, "review-awaiting", "Pasangkan", "ws-tone-tr", `${awaiting.length} pindah saldo`),
    });
  }
  const ambiguous = queue?.ambiguous ?? [];
  if (ambiguous.length) {
    items.push({
      key: "ambiguous",
      title: `${ambiguous.length} pasangan pindah saldo belum pasti`,
      detail: "Pilih pasangan yang benar",
      action: reviewLink(ambiguous[0].id, "review-ambiguous", "Pilih", "ws-tone-warn", "pasangan pindah saldo"),
    });
  }
  for (const group of (queue?.groups ?? []).filter((item) => item.needs_review_count > 0)) {
    const first = group.transactions[0];
    items.push({
      key: `g-${group.key}`,
      title: `${group.label}: perlu jawaban`,
      detail: `${group.needs_review_count} transaksi · ${bal(group.total)}${first?.date ? ` · ${when(first.date, today)}` : ""}`,
      action: reviewLink(first?.id ?? group.key, "review-groups", "Pilih", "ws-tone-warn", group.label),
    });
  }
  if (autoLinks.length) {
    items.push({
      key: "auto-links",
      title: `${autoLinks.length} rencana ditautkan otomatis`,
      detail: "Cek dan batalkan kalau keliru",
      action: (
        <button type="button" className={`${ACTION} ws-tone-neutral`} onClick={onOpenPlans}>
          Cek
        </button>
      ),
    });
  }
  const tomorrow = jakartaDay(new Date(Date.parse(`${today}T12:00:00+07:00`) + 86_400_000));
  for (const plan of plans) {
    const due = plan.next_due_date;
    const label = due < today ? "lewat jatuh tempo" : due === today ? "hari ini" : due === tomorrow ? "besok" : shortDate(due);
    const isDue = due <= today;
    items.push({
      key: `p-${plan.id}-${due}`,
      title: `${plan.name} ${bal(plan.amount)} ${label}`,
      detail: `Rencana · ${plan.source_account_name ?? ""}`.trim(),
      action: isDue ? (
        <button type="button" className={`${ACTION} ws-tone-neutral`} onClick={onOpenPlans} aria-label={`Atur ${plan.name}`}>
          Atur
        </button>
      ) : (
        <Link href="/goals" className={`${ACTION} ws-tone-neutral`} aria-label={`Lihat ${plan.name}`}>
          Lihat
        </Link>
      ),
    });
  }
  return items;
}

export function TodoList({ items }: { items: TodoItem[] }) {
  return (
    <ul className="ws-todo">
      {items.map((item) => (
        <li key={item.key} className="ws-todo-item">
          <div className="ws-todo-t">
            <b>{item.title}</b>
            <span className="ws-todo-sub">{item.detail}</span>
          </div>
          {item.action}
        </li>
      ))}
    </ul>
  );
}

export function AutoRowsNote({ count }: { count: number }) {
  if (!count) return null;
  return (
    <p className="ws-caption mt-2">
      {count} dicatat otomatis hari ini. Boleh dilewati,{" "}
      <Link href="/ledger?review=auto" className="ws-linkbtn">
        cek kalau mau
      </Link>
      .
    </p>
  );
}

export function TodoCard({
  attention,
  items,
  autoToday,
  error,
}: {
  attention: number;
  items: TodoItem[];
  autoToday: number;
  error: boolean;
}) {
  return (
    <Card labelledBy="beranda-todo">
      <CardHeader
        id="beranda-todo"
        title="Perlu tindakan"
        count={attention || undefined}
        countLabel={`${attention} perlu ditinjau`}
        action={
          attention ? (
            <Link href="/review" className="ws-btn ws-btn-sm">
              Buka Tinjau
            </Link>
          ) : undefined
        }
      />
      {error && <p className="ws-caption mt-3">Daftar Tinjau belum bisa dimuat.</p>}
      {!attention && !error && (
        <ul className="ws-todo">
          <li className="ws-todo-item">
            <div className="ws-todo-t">
              <b>Semua sudah ditinjau</b>
              <span className="ws-todo-sub">Tidak ada yang perlu dijawab.</span>
            </div>
            <Pill tone="ok" icon="check">
              Selesai
            </Pill>
          </li>
        </ul>
      )}
      {items.length > 0 && <TodoList items={items} />}
      <AutoRowsNote count={autoToday} />
    </Card>
  );
}
