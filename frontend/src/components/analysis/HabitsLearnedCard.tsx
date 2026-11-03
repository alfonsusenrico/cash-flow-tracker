"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import type { HabitList } from "@/lib/habits";
import { Card, CardHeader, ErrorState, Skeleton } from "@/components/workspace/primitives";

/** "Kebiasaan dihafal" on Ringkasan (phase 4 OD42, delivered with GET /api/habits). */
export function HabitsLearnedCard() {
  const query = useQuery<HabitList>({ queryKey: queryKeys.habits.list, queryFn: () => api.get("/habits") });
  const ready = query.data?.habits.filter((habit) => habit.state === "ready") ?? [];
  return (
    <Card labelledBy="habits-learned">
      <CardHeader
        id="habits-learned"
        icon="check"
        title="Kebiasaan dihafal"
        count={query.data ? ready.length : undefined}
        countLabel={`${ready.length} kebiasaan siap`}
        action={<Link href="/settings/kebiasaan" className="ws-linkbtn">Lihat semua</Link>}
      />
      {query.isLoading ? (
        <Skeleton style={{ height: 80 }} />
      ) : query.isError || !query.data ? (
        <ErrorState message="Kebiasaan belum dapat dimuat." onRetry={() => query.refetch()} />
      ) : ready.length === 0 ? (
        <p className="ws-caption">Belum ada kebiasaan yang dipakai untuk mencatat otomatis. Jawaban di Tinjau menjadi bahan belajar.</p>
      ) : (
        <ul className="ws-rows">
          {ready.slice(0, 3).map((habit) => (
            <li key={habit.key}>
              <span>{habit.label}</span>
              <span className="ws-caption">{habit.category.name} · {habit.confirmations}×</span>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}
