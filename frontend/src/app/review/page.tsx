"use client";

import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import {
  chunkIds,
  completionCandidates,
  reviewErrorMessage,
  type AmbiguousItem,
  type CompletionCandidate,
  type ReviewItem,
  type ReviewQueue,
} from "@/lib/review";
import { PurposeQuestionCard } from "@/components/review/PurposeQuestionCard";
import { ReviewGroupCard, type CategoryOption, type GroupActionRequest } from "@/components/review/ReviewGroupCard";
import { AmbiguousMovementCard, AwaitingMovementCard } from "@/components/review/MovementReviewCards";
import { HistorySuggestionsSection } from "@/components/review/HistorySuggestionsSection";
import type { AccountOptionItem } from "@/components/ui/AccountSelectOptions";

const ACTION_DONE: Record<GroupActionRequest["action"], string> = {
  accept: "ditandai sudah ditinjau",
  recategorise: "dipindahkan ke kategori baru",
  rename: "diganti namanya",
  keep_separate: "dibiarkan terpisah",
};

function Section({ id, title, count, description, children }: {
  id: string;
  title: string;
  count: number;
  description: string;
  children: React.ReactNode;
}) {
  return (
    <section aria-labelledby={id} className="space-y-3">
      <div>
        <h2 id={id} className="text-base font-bold tracking-tight text-[var(--text)]">
          {title} <span className="tabular text-[var(--muted)]">({count})</span>
        </h2>
        <p className="text-xs text-[var(--muted)]">{description}</p>
      </div>
      {children}
    </section>
  );
}

export default function ReviewPage() {
  const qc = useQueryClient();
  const [pending, setPending] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [completionChoices, setCompletionChoices] = useState<Record<string, CompletionCandidate[]>>({});

  const { data, isLoading, isError, refetch } = useQuery<ReviewQueue>({
    queryKey: queryKeys.review.queue,
    queryFn: () => api.get("/review/queue"),
  });
  const { data: categoriesData } = useQuery<{ categories: CategoryOption[] }>({
    queryKey: queryKeys.categories,
    queryFn: () => api.get("/categories"),
  });
  const { data: accountsData } = useQuery<{ accounts: AccountOptionItem[] }>({
    queryKey: queryKeys.accounts,
    queryFn: () => api.get("/accounts"),
  });

  const refresh = async () => {
    await Promise.all([
      qc.invalidateQueries({ queryKey: queryKeys.review.all }),
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all }),
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all }),
      qc.invalidateQueries({ queryKey: queryKeys.accounts }),
    ]);
  };

  const run = async (work: () => Promise<string>): Promise<boolean> => {
    setPending(true);
    setError("");
    setStatus("");
    try {
      setStatus(await work());
      return true;
    } catch (failure) {
      setError(reviewErrorMessage(failure));
      return false;
    } finally {
      setPending(false);
      await refresh();
    }
  };

  const groupAction = (request: GroupActionRequest) =>
    run(async () => {
      // Each request is one transaction on the server; a group larger than a batch is sent in parts.
      let done = 0;
      for (const ids of chunkIds(request.ids)) {
        try {
          await api.post("/review/bulk", { action: request.action, transaction_ids: ids, ...request.fields });
        } catch (failure) {
          if (done === 0) throw failure;
          throw new Error(`${done} dari ${request.ids.length} transaksi tersimpan; sisanya gagal: ${reviewErrorMessage(failure)}`);
        }
        done += ids.length;
      }
      return `${request.label}: ${done} transaksi ${ACTION_DONE[request.action]}.`;
    });

  const forgetChoices = (itemId: string) =>
    setCompletionChoices(({ [itemId]: _done, ...rest }) => rest);

  const completeMovement = (item: ReviewItem, accountId: string, createNew = false) =>
    run(async () => {
      try {
        const result = await api.post<{ linked_transaction_id?: string | null }>("/movements/complete", {
          transaction_id: item.id,
          ...(item.type === "expense" ? { target_account_id: accountId } : { source_account_id: accountId }),
          ...(createNew ? { create_new: true } : {}),
        });
        forgetChoices(item.id);
        return result.linked_transaction_id
          ? "Dihubungkan dengan transaksi yang sudah ada di rekening itu."
          : "Pindah saldo dilengkapi.";
      } catch (failure) {
        const candidates = completionCandidates(failure);
        if (!candidates) throw failure;
        // The other leg may already be recorded; creating another would count the money twice.
        setCompletionChoices((choices) => ({ ...choices, [item.id]: candidates }));
        return "Rekening itu sudah punya transaksi yang cocok. Hubungkan, atau buat transaksi baru.";
      }
    });

  const linkExistingLeg = (item: ReviewItem, candidateId: string) =>
    run(async () => {
      const [expense, income] = item.type === "expense" ? [item.id, candidateId] : [candidateId, item.id];
      await api.post("/movements/merge", { expense_transaction_id: expense, income_transaction_id: income });
      forgetChoices(item.id);
      return "Kedua transaksi digabung sebagai pindah saldo.";
    });

  const linkMovement = (item: AmbiguousItem, counterpartId: string) =>
    run(async () => {
      const [expense, income] = item.type === "expense" ? [item.id, counterpartId] : [counterpartId, item.id];
      await api.post("/movements/merge", { expense_transaction_id: expense, income_transaction_id: income });
      return "Kedua transaksi digabung sebagai pindah saldo.";
    });

  const keepSeparate = (item: AmbiguousItem) =>
    run(async () => {
      await api.post("/review/bulk", { action: "keep_separate", transaction_ids: [item.id] });
      return "Transaksi dibiarkan terpisah dan tidak akan dipasangkan otomatis.";
    });

  const answered = async (message: string) => {
    setError("");
    setStatus(message);
    await refresh();
  };

  const categories = categoriesData?.categories ?? [];
  const accounts = accountsData?.accounts ?? [];
  const counts = data?.counts;
  const total = counts ? counts.questions + counts.needs_review + counts.auto + counts.awaiting + counts.ambiguous : 0;

  return (
    <div className="mx-auto max-w-4xl space-y-6">
      <div className="border-b border-[var(--border)] pb-3">
        <p className="text-xs font-bold uppercase tracking-wider text-[var(--muted)]">Tinjauan mingguan</p>
        <h1 className="mt-1 text-2xl font-extrabold tracking-tight text-[var(--text)]">Tinjau Transaksi</h1>
        {counts && total > 0 && (
          <p className="mt-1 text-xs text-[var(--text-secondary)] tabular">
            {counts.questions} pertanyaan · {counts.needs_review} perlu ditinjau · {counts.auto} otomatis · {counts.awaiting + counts.ambiguous} pindah saldo belum lengkap
          </p>
        )}
      </div>

      <div aria-live="polite" className="min-h-0 text-xs">
        {status && <p className="rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 font-medium text-[var(--text)]">{status}</p>}
      </div>
      {error && (
        <p role="alert" className="rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-xs font-medium text-rose-700 dark:text-rose-300">
          {error}
        </p>
      )}

      {isLoading ? (
        <p className="py-16 text-center text-xs text-[var(--muted)]">Memuat daftar tinjauan…</p>
      ) : isError || !data ? (
        <div className="space-y-3 py-12 text-center">
          <p className="text-sm font-semibold text-[var(--text)]">Daftar tinjauan gagal dimuat.</p>
          <button type="button" onClick={() => refetch()} className="min-h-11 rounded-xl border border-[var(--border)] px-4 text-xs font-semibold text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]">
            Coba lagi
          </button>
        </div>
      ) : total === 0 ? (
        <div className="space-y-1 rounded-2xl border border-[var(--border)] bg-[var(--surface)] py-14 text-center">
          <p className="text-sm font-semibold text-[var(--text)]">Semua transaksi sudah ditinjau.</p>
          <p className="text-xs text-[var(--muted)]">Transaksi otomatis baru akan muncul di sini.</p>
        </div>
      ) : (
        <>
          {data.truncated && (
            <p className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-900 dark:text-amber-200">
              Hanya 1.000 transaksi terbaru yang ditampilkan. Selesaikan yang ada untuk melihat sisanya.
            </p>
          )}

          {data.questions.length > 0 && (
            <Section id="review-questions" title="Apa ini?" count={data.questions.length} description="Pembayaran tanpa nama toko. Pilih jawaban atau tulis sendiri.">
              <div className="space-y-3">
                {data.questions.map((entry) => (
                  <PurposeQuestionCard key={entry.event_id} entry={entry} onAnswered={answered} />
                ))}
              </div>
            </Section>
          )}

          {data.groups.length > 0 && (
            <Section
              id="review-groups"
              title="Per toko"
              count={data.counts.needs_review + data.counts.auto}
              description={`${data.counts.needs_review} perlu ditinjau dan ${data.counts.auto} otomatis dalam ${data.groups.length} kelompok. Terima, ubah kategori, atau ganti nama sekaligus.`}
            >
              <div className="space-y-3">
                {data.groups.map((group) => (
                  <ReviewGroupCard key={group.key} group={group} categories={categories} pending={pending} onAction={groupAction} />
                ))}
              </div>
            </Section>
          )}

          {data.awaiting.length > 0 && (
            <Section id="review-awaiting" title="Pindah saldo menunggu pasangan" count={data.awaiting.length} description="Hanya satu sisi yang tercatat. Pilih rekening lainnya untuk melengkapinya.">
              <div className="space-y-3">
                {data.awaiting.map((item) => (
                  <AwaitingMovementCard
                    key={item.id}
                    item={item}
                    accounts={accounts}
                    pending={pending}
                    candidates={completionChoices[item.id]}
                    onComplete={completeMovement}
                    onLinkExisting={linkExistingLeg}
                  />
                ))}
              </div>
            </Section>
          )}

          {data.ambiguous.length > 0 && (
            <Section id="review-ambiguous" title="Pasangan pindah saldo belum pasti" count={data.ambiguous.length} description="Ada beberapa kemungkinan pasangan. Pilih yang benar atau biarkan terpisah.">
              <div className="space-y-3">
                {data.ambiguous.map((item) => (
                  <AmbiguousMovementCard key={item.id} item={item} pending={pending} onLink={linkMovement} onKeepSeparate={keepSeparate} />
                ))}
              </div>
            </Section>
          )}
        </>
      )}

      <HistorySuggestionsSection />
    </div>
  );
}
