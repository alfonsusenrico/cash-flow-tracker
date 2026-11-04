"use client";

import Link from "next/link";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { AmbiguousMovementCard, AwaitingMovementCard, type TopupOptions } from "@/components/review/MovementReviewCards";
import { HistorySuggestionsSection } from "@/components/review/HistorySuggestionsSection";
import { PurposeQuestionCard, type PurposeAnswerBody } from "@/components/review/PurposeQuestionCard";
import { ReviewGroupCard, type CategoryOption, type GroupActionRequest } from "@/components/review/ReviewGroupCard";
import { SuggestionGroupCard, type SuggestionDecisionRequest } from "@/components/review/SuggestionGroupCard";
import type { CycleSummary } from "@/components/beranda/types";
import type { AccountOptionItem } from "@/components/ui/AccountSelectOptions";
import { Icon } from "@/components/ui/Icon";
import { useEscapeLayer } from "@/components/workspace/layers";
import { useToast } from "@/components/workspace/overlays";
import { EmptyState, ErrorState, Skeleton } from "@/components/workspace/primitives";
import { usePageShortcutGroup, useShortcut, useShortcuts, type PageShortcutGroup } from "@/components/workspace/shortcuts";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { isLiquidAccountChoice } from "@/lib/accountOptions";
import { api } from "@/lib/api";
import { chunkSuggestionIds, type SuggestionDecisionResult, type SuggestionList } from "@/lib/historySuggestions";
import {
  canSwitchToAmountTracking,
  listAmountInvestmentProducts,
  type InvestmentAccount,
} from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import {
  chunkIds,
  completionCandidates,
  newReplyId,
  reviewErrorMessage,
  type AmbiguousItem,
  type CompletionCandidate,
  type PurposeQuestionEntry,
  type ReviewItem,
  type ReviewQueue,
  type TopupProductChoice,
} from "@/lib/review";
import {
  batchOf,
  NOTHING_TO_UNDO,
  undoEntry,
  undoMessage,
  UndoStack,
  type UndoBatch,
  type UndoEntry,
  type UndoFields,
  type UndoResponse,
} from "@/lib/reviewUndo";
import {
  buildTriage,
  FOLDER_LABELS,
  folderFromParams,
  transaksiHref,
  folderItems,
  itemFolderLabel,
  itemTitle,
  itemTransactionId,
  neededCount,
  nextAfter,
  resolveDeepLink,
  triageSearch,
  type FolderKey,
  type Triage,
  type TriageItem,
} from "@/lib/triage";
import { TriageList } from "./TriageList";
import { pressChoice, pressMarked } from "./Choice";

/** Main column width at or below which one pane shows at a time (D5, the preview's threshold). */
export const TWO_PANE_MIN = 720;
const RUN_PANEL_ID = "run:suggestions";
const HEADING_ID = "triage-item-heading";
const DIGITS = [1, 2, 3, 4, 5, 6, 7, 8, 9];

const PAGE_KEYS: PageShortcutGroup = {
  name: "Di Tinjau",
  caption: "tombol satu huruf di Tinjau",
  entries: [
    { keys: ["J", "K"], text: "Pindah ke item berikutnya atau sebelumnya" },
    { keys: ["Enter"], text: "Buka item yang dipilih" },
    { keys: ["E"], text: "Terima saran yang disorot" },
    { keys: ["T"], text: "Ini top up investasi" },
    { keys: ["1–9"], text: "Pilih jawaban bernomor" },
    { keys: ["Z"], text: "Urungkan jawaban terakhir" },
  ],
};

const ACTION_DONE: Record<GroupActionRequest["action"], string> = {
  accept: "ditandai sudah ditinjau",
  recategorise: "dipindahkan ke kategori baru",
  rename: "diganti namanya",
  keep_separate: "dibiarkan terpisah",
};

interface ActionOutcome {
  message: string;
  batches: (UndoBatch | null)[];
  /** The item stays (for example when the server asks to choose an existing leg). */
  stay?: boolean;
}

interface Failure {
  message: string;
  transactionId?: string | null;
}

/** "Induk · Produk" choices for top-ups and the unit-tracked funds that must switch first. */
function topupChoices(accounts: InvestmentAccount[]): { products: TopupProductChoice[]; unitFunds: TopupProductChoice[] } {
  const flattened = [...new Map(
    accounts.flatMap((account) => [account, ...(account.children ?? [])]).map((account) => [account.id, account]),
  ).values()];
  const label = (account: InvestmentAccount): TopupProductChoice => {
    const parent = flattened.find((candidate) => candidate.id === account.parent_id);
    return { id: account.id, name: account.name, label: parent ? `${parent.name} · ${account.name}` : account.name };
  };
  return {
    products: listAmountInvestmentProducts(accounts).map(label),
    unitFunds: flattened.filter(canSwitchToAmountTracking).map(label),
  };
}

function focusHeading() {
  window.requestAnimationFrame(() => document.getElementById(HEADING_ID)?.focus());
}

/** The Tinjau triage inbox: folders, a list and one item, keys, auto-advance and Urungkan. */
export function TriageInbox() {
  const qc = useQueryClient();
  const { bal } = useAppCtx();
  const { showToast } = useToast();
  const regionRef = useRef<HTMLElement>(null);
  const listRef = useRef<HTMLElement>(null);
  const itemRef = useRef<HTMLElement>(null);
  const onePane = useWidthAtMost(regionRef, TWO_PANE_MIN);
  const undoStack = useRef(new UndoStack());
  const acting = useRef(false);
  const pushedItem = useRef(false);
  const lastIds = useRef<string[]>([]);
  const deepLinkRead = useRef(false);

  const [folder, setFolder] = useState<FolderKey>("semua");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [pane, setPane] = useState<"list" | "item">("list");
  const [pending, setPending] = useState(false);
  const [notice, setNotice] = useState("");
  const [failure, setFailure] = useState<Failure | null>(null);
  const [foldOpen, setFoldOpen] = useState({ otomatis: false, saran: false });
  const [completionChoices, setCompletionChoices] = useState<Record<string, CompletionCandidate[]>>({});

  usePageShortcutGroup(PAGE_KEYS);
  const { register } = useShortcuts();

  const queueQuery = useQuery<ReviewQueue>({ queryKey: queryKeys.review.queue, queryFn: () => api.get("/review/queue") });
  const suggestionsQuery = useQuery<SuggestionList>({ queryKey: queryKeys.review.suggestions, queryFn: () => api.get("/review/suggestions") });
  const { data: categoriesData } = useQuery<{ categories: CategoryOption[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") });
  const { data: accountsData } = useQuery<{ accounts: AccountOptionItem[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });

  const triage: Triage | null = useMemo(
    () => (queueQuery.data ? buildTriage(queueQuery.data, suggestionsQuery.data) : null),
    [queueQuery.data, suggestionsQuery.data],
  );
  const allDone = triage !== null && neededCount(triage) === 0;
  const summaryQuery = useQuery<CycleSummary>({
    queryKey: queryKeys.dashboard.cycleSummary,
    queryFn: () => api.get("/dashboard/cycle-summary"),
    enabled: allDone,
  });

  const items = useMemo(() => (triage ? folderItems(triage, folder) : []), [triage, folder]);
  const everyItem = useMemo(
    () => (triage ? [...folderItems(triage, "semua"), ...triage.folders.otomatis, ...triage.folders.saran] : []),
    [triage],
  );
  const selected = selectedId === RUN_PANEL_ID ? null : everyItem.find((item) => item.id === selectedId) ?? null;
  const categories = categoriesData?.categories ?? [];
  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData]);
  const choices = useMemo(() => topupChoices(accounts as unknown as InvestmentAccount[]), [accounts]);

  const showItemPane = !onePane || pane === "item";
  const showListPane = !onePane || pane === "list";

  // --- URL state (D7) -----------------------------------------------------------------------
  const writeUrl = useCallback((nextFolder: FolderKey, item: TriageItem | null, push: boolean) => {
    const url = `${window.location.pathname}${triageSearch(nextFolder, item)}`;
    if (push) window.history.pushState({ triageItem: item?.id ?? null }, "", url);
    else window.history.replaceState({ ...(window.history.state ?? {}), triageItem: item?.id ?? null }, "", url);
  }, []);

  // A deep link selects its item once the data is there; the legacy hash is read, then dropped.
  useEffect(() => {
    if (!triage || deepLinkRead.current) return;
    deepLinkRead.current = true;
    const params = new URLSearchParams(window.location.search);
    const transactionId = params.get("item");
    const linkedFolder = folderFromParams(params.get("folder"), window.location.hash);
    if (!transactionId && !linkedFolder) {
      const first = folderItems(triage, "semua")[0] ?? null;
      setSelectedId(first?.id ?? null);
      return;
    }
    const link = resolveDeepLink(triage, transactionId, linkedFolder);
    const target = link.itemId ?? folderItems(triage, link.folder)[0]?.id ?? null;
    setFolder(link.folder);
    setSelectedId(target);
    if (link.folder === "otomatis" || link.folder === "saran") setFoldOpen((open) => ({ ...open, [link.folder]: true }));
    if (link.resolved) setNotice("Item yang dibuka sudah selesai.");
    const item = everyItemOf(triage).find((entry) => entry.id === target) ?? null;
    writeUrl(link.folder, link.itemId ? item : null, false);
    if (link.itemId) {
      setPane("item");
      focusHeading();
    }
  }, [triage, writeUrl]);

  // Selection is held by id across refetches; an item answered elsewhere is replaced by the next one (D6).
  useEffect(() => {
    if (!triage || !deepLinkRead.current) return;
    const ids = items.map((item) => item.id);
    const visible = new Set(everyItemOf(triage).map((item) => item.id));
    if (!acting.current && selectedId && selectedId !== RUN_PANEL_ID && !visible.has(selectedId)) {
      const next = nextAfter(lastIds.current, selectedId, new Set(ids));
      setSelectedId(next);
      setNotice("Item ini sudah diselesaikan di tempat lain.");
    }
    lastIds.current = ids;
  }, [triage, items, selectedId]);

  // On one pane, the browser's back gesture returns from the item to the list (D5).
  useEffect(() => {
    const onPop = () => {
      if (pane !== "item" || !onePane) return;
      pushedItem.current = false;
      setPane("list");
      focusRow(selectedId);
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, [pane, onePane, selectedId]);

  const backToList = useCallback(() => {
    if (pushedItem.current) {
      window.history.back();
      return;
    }
    setPane("list");
    focusRow(selectedId);
  }, [selectedId]);
  useEscapeLayer(onePane && pane === "item", backToList);

  // --- Selection ------------------------------------------------------------------------------
  const open = useCallback((item: TriageItem) => {
    setNotice("");
    setFailure(null);
    setSelectedId(item.id);
    if (item.folder === "otomatis" || item.folder === "saran") setFolder(item.folder);
    else if (folder === "otomatis" || folder === "saran") setFolder("semua");
    const nextFolder = item.folder === "otomatis" || item.folder === "saran" ? item.folder : folder === "otomatis" || folder === "saran" ? "semua" : folder;
    if (onePane) {
      setPane("item");
      pushedItem.current = true;
      writeUrl(nextFolder, item, true);
      focusHeading();
    } else {
      writeUrl(nextFolder, item, false);
    }
  }, [folder, onePane, writeUrl]);

  const chooseFolder = useCallback((next: FolderKey) => {
    if (!triage) return;
    setFolder(next);
    setNotice("");
    const first = folderItems(triage, next)[0] ?? null;
    setSelectedId(first?.id ?? null);
    writeUrl(next, onePane ? null : first, false);
  }, [triage, onePane, writeUrl]);

  const openRunPanel = () => {
    setSelectedId(RUN_PANEL_ID);
    if (onePane) {
      setPane("item");
      pushedItem.current = true;
      window.history.pushState({ triageItem: RUN_PANEL_ID }, "", window.location.href);
    }
    focusHeading();
  };

  const move = useCallback((step: 1 | -1, fromList: boolean) => {
    const index = items.findIndex((item) => item.id === selectedId);
    const next = items[index + step];
    if (!next) {
      setNotice(step > 0 ? "Ini yang terakhir." : "Ini yang pertama.");
      return;
    }
    setNotice("");
    setFailure(null);
    setSelectedId(next.id);
    writeUrl(folder, next, false);
    if (fromList && !onePane) focusRow(next.id);
    else focusHeading();
  }, [items, selectedId, folder, onePane, writeUrl]);

  // --- Actions and auto-advance (D6) -----------------------------------------------------------
  const refresh = useCallback(async () => {
    await Promise.all([
      qc.invalidateQueries({ queryKey: queryKeys.review.all }),
      qc.invalidateQueries({ queryKey: queryKeys.transactions.all }),
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all }),
      qc.invalidateQueries({ queryKey: queryKeys.accounts }),
    ]);
  }, [qc]);

  const latestTriage = useCallback((): Triage | null => {
    const queue = qc.getQueryData<ReviewQueue>(queryKeys.review.queue);
    return queue ? buildTriage(queue, qc.getQueryData<SuggestionList>(queryKeys.review.suggestions)) : null;
  }, [qc]);

  const undo = useRef<() => Promise<void>>(async () => {});

  const act = useCallback(async (item: TriageItem, work: () => Promise<ActionOutcome>): Promise<boolean> => {
    const before = (item.folder === "otomatis" || item.folder === "saran" ? folderItems(triage!, item.folder) : items).map((entry) => entry.id);
    setPending(true);
    setFailure(null);
    setNotice("");
    acting.current = true;
    let outcome: ActionOutcome | null = null;
    let error: unknown = null;
    try {
      outcome = await work();
    } catch (failed) {
      error = failed;
    }
    const saved = (outcome?.batches ?? (error as { savedBatches?: UndoBatch[] } | null)?.savedBatches ?? []).filter(Boolean) as UndoBatch[];
    const entry = saved.length
      ? undoStack.current.push({
        batches: saved,
        label: itemTitle(item),
        transactionId: itemTransactionId(item),
        folderLabel: itemFolderLabel(item),
      })
      : null;
    await refresh();
    acting.current = false;
    setPending(false);
    if (error) {
      setFailure({ message: reviewErrorMessage(error) });
      return false;
    }
    if (!outcome) return false;
    const fresh = latestTriage();
    if (outcome.stay || !fresh) {
      showToast(`Tersimpan: ${outcome.message}`, entry ? { onUndo: () => void undo.current() } : undefined);
      return true;
    }
    let nextFolder: FolderKey = item.folder === "otomatis" || item.folder === "saran" ? item.folder : folder;
    let remaining = folderItems(fresh, nextFolder);
    let nextId = remaining.some((entry) => entry.id === item.id) ? item.id : nextAfter(before, item.id, new Set(remaining.map((entry) => entry.id)));
    if (!nextId && nextFolder !== "semua") {
      nextFolder = "semua";
      remaining = folderItems(fresh, "semua");
      nextId = remaining[0]?.id ?? null;
    }
    const next = remaining.find((entry) => entry.id === nextId) ?? null;
    setFolder(nextFolder);
    setSelectedId(nextId);
    writeUrl(nextFolder, onePane && !next ? null : next, false);
    if (onePane && !next) setPane("item");
    const following = next ? ` Berikutnya: ${itemTitle(next)}.` : neededCount(fresh) === 0 ? " Semua sudah ditinjau." : "";
    showToast(`Tersimpan: ${outcome.message}.${following}`, entry ? { onUndo: () => void undo.current() } : undefined);
    focusHeading();
    return true;
  }, [triage, items, folder, onePane, refresh, latestTriage, showToast, writeUrl]);

  const runUndo = useCallback(async () => {
    const entry: UndoEntry | null = undoStack.current.newest();
    if (!entry) {
      setNotice(NOTHING_TO_UNDO);
      return;
    }
    setPending(true);
    setFailure(null);
    acting.current = true;
    const outcome = await undoEntry(entry, (token) => api.post<UndoResponse>("/review/undo", { undo_token: token }));
    if (outcome.failure !== "network") undoStack.current.drop(entry.id);
    await refresh();
    acting.current = false;
    setPending(false);
    const message = undoMessage(outcome, entry);
    if (outcome.failure) {
      setFailure({ message, transactionId: outcome.failure === "network" ? null : outcome.conflictIds[0] ?? entry.transactionId });
      return;
    }
    const fresh = latestTriage();
    if (fresh && entry.transactionId) {
      const link = resolveDeepLink(fresh, entry.transactionId, null);
      if (link.itemId) {
        setFolder(link.folder);
        setSelectedId(link.itemId);
        if (link.folder === "otomatis" || link.folder === "saran") setFoldOpen((open) => ({ ...open, [link.folder]: true }));
        writeUrl(link.folder, everyItemOf(fresh).find((item) => item.id === link.itemId) ?? null, false);
        if (onePane) setPane("item");
      }
    }
    showToast(message);
    focusHeading();
  }, [refresh, latestTriage, showToast, writeUrl, onePane]);
  useLayoutEffect(() => {
    undo.current = runUndo;
  }, [runUndo]);

  const answerQuestion = (entry: PurposeQuestionEntry, body: PurposeAnswerBody) =>
    act(questionItem(entry), async () => {
      const response = await api.post<{ answer: { status: "applied" | "needs_review"; notes: string | null } } & UndoFields>(
        `/ingest/notifications/${entry.event_id}/answer-purpose`,
        { question_id: entry.question.question_id, reply_id: newReplyId(), ...body },
      );
      return {
        message: response.answer?.status === "applied"
          ? `jawaban ${response.answer.notes ?? "tersimpan"}`
          : "jawaban disimpan sebagai catatan, kategorinya masih perlu dipilih",
        batches: [batchOf(response, 1)],
      };
    });

  const groupAction = (item: TriageItem) => (request: GroupActionRequest) =>
    act(item, async () => {
      // Each request is one database transaction; a group larger than a batch is sent in parts (D14).
      const batches: UndoBatch[] = [];
      let done = 0;
      for (const ids of chunkIds(request.ids)) {
        try {
          const response = await api.post<UndoFields>("/review/bulk", { action: request.action, transaction_ids: ids, ...request.fields });
          const batch = batchOf(response, ids.length);
          if (batch) batches.push(batch);
        } catch (failed) {
          if (done === 0) throw failed;
          const partial = new Error(`${done} dari ${request.ids.length} transaksi tersimpan; sisanya gagal: ${reviewErrorMessage(failed)}`);
          throw Object.assign(partial, { savedBatches: batches });
        }
        done += ids.length;
      }
      return { message: `${request.label}, ${done} transaksi ${ACTION_DONE[request.action]}`, batches };
    });

  const completeMovement = (item: TriageItem) => (leg: ReviewItem, accountId: string, createNew = false) =>
    act(item, async () => {
      try {
        const response = await api.post<{ linked_transaction_id?: string | null } & UndoFields>("/movements/complete", {
          transaction_id: leg.id,
          ...(leg.type === "expense" ? { target_account_id: accountId } : { source_account_id: accountId }),
          ...(createNew ? { create_new: true } : {}),
        });
        setCompletionChoices((current) => withoutKey(current, leg.id));
        return {
          message: response.linked_transaction_id ? "dihubungkan dengan transaksi yang sudah ada" : "pindah saldo dilengkapi",
          batches: [batchOf(response, 2)],
        };
      } catch (failed) {
        const candidates = completionCandidates(failed);
        if (!candidates) throw failed;
        // The other leg may already be recorded; creating another would count the money twice.
        setCompletionChoices((current) => ({ ...current, [leg.id]: candidates }));
        return { message: "rekening itu sudah punya transaksi yang cocok. Hubungkan, atau catat baru", batches: [], stay: true };
      }
    });

  const merge = (item: TriageItem) => async (leg: ReviewItem | AmbiguousItem, counterpartId: string) =>
    act(item, async () => {
      const [expense, income] = leg.type === "expense" ? [leg.id, counterpartId] : [counterpartId, leg.id];
      const response = await api.post<UndoFields>("/movements/merge", { expense_transaction_id: expense, income_transaction_id: income });
      setCompletionChoices((current) => withoutKey(current, leg.id));
      return { message: "kedua transaksi digabung sebagai pindah saldo", batches: [batchOf(response, 2)] };
    });

  const keepSeparate = (item: TriageItem) => (leg: AmbiguousItem) =>
    act(item, async () => {
      const response = await api.post<UndoFields>("/review/bulk", { action: "keep_separate", transaction_ids: [leg.id] });
      return { message: "dibiarkan terpisah", batches: [batchOf(response, 1)] };
    });

  const recordAsExpense = (item: TriageItem) => (leg: ReviewItem, categoryId: string, pillar: string | null) =>
    act(item, async () => {
      const response = await api.post<UndoFields>("/review/bulk", {
        action: "recategorise", transaction_ids: [leg.id], category_id: categoryId, ...(pillar ? { kakeibo_type: pillar } : {}),
      });
      return { message: "dicatat sebagai pengeluaran", batches: [batchOf(response, 1)] };
    });

  const topupFor = (item: TriageItem, leg: ReviewItem): TopupOptions | undefined => {
    const flattened = accounts.flatMap((account) => [account, ...(account.children ?? [])]);
    const source = flattened.find((account) => (account.id ?? account.account_id) === leg.account_id);
    if (leg.type !== "expense" || !source || !isLiquidAccountChoice(source)) return undefined;
    return {
      ...choices,
      onConvert: (debit, productId, suggestedId) => act(item, async () => {
        const response = await api.post<UndoFields>("/investment-topups/from-transaction", {
          transaction_id: debit.id, target_account_id: productId,
          ...(suggestedId ? { suggested_target_account_id: suggestedId } : {}),
        });
        return { message: "debit dicatat sebagai top up investasi", batches: [batchOf(response, 2)] };
      }),
      onReject: (debit, suggestedId) => act(item, async () => {
        const response = await api.post<UndoFields>("/review/topup-suggestions/reject", {
          transaction_id: debit.id, ...(suggestedId ? { suggested_target_account_id: suggestedId } : {}),
        });
        return { message: "bukan top up. Pilih rekening pasangannya", batches: [batchOf(response, 1)], stay: true };
      }),
    };
  };

  const decide = (item: TriageItem) => (request: SuggestionDecisionRequest) =>
    act(item, async () => {
      const batches: UndoBatch[] = [];
      let done = 0;
      let skipped = 0;
      for (const ids of chunkSuggestionIds(request.ids)) {
        try {
          const result = await api.post<SuggestionDecisionResult & UndoFields>(`/review/suggestions/${request.decision}`, { suggestion_ids: ids });
          const count = (request.decision === "accept" ? result.accepted : result.rejected) ?? 0;
          done += count;
          skipped += result.skipped.length;
          const batch = batchOf(result, count);
          if (batch) batches.push(batch);
        } catch (failed) {
          if (done === 0 && skipped === 0) throw failed;
          const verb = request.decision === "accept" ? "diterima" : "ditolak";
          throw Object.assign(new Error(`${done} dari ${request.ids.length} saran ${verb}; sisanya gagal: ${reviewErrorMessage(failed)}`), { savedBatches: batches });
        }
      }
      const note = skipped > 0 ? `; ${skipped} dilewati karena transaksinya sudah berubah atau sudah diputuskan` : "";
      return { message: `${request.label}, ${done} saran ${request.decision === "accept" ? "diterima" : "ditolak"}${note}`, batches };
    });

  // --- Keys (D8) ------------------------------------------------------------------------------
  const inList = () => Boolean(listRef.current?.contains(document.activeElement));
  useShortcut("j", () => move(1, inList()), regionRef);
  useShortcut("k", () => move(-1, inList()), regionRef);
  useShortcut("e", () => {
    if (!pressMarked(itemRef.current, "data-primary-choice")) setNotice("Belum ada saran untuk diterima.");
  }, regionRef);
  useShortcut("t", () => {
    if (!pressMarked(itemRef.current, "data-topup")) setNotice("T hanya untuk pindah saldo keluar yang belum lengkap.");
  }, regionRef);
  useShortcut("z", () => void runUndo(), regionRef);
  useEffect(() => {
    const press = (n: number) => {
      if (!pressChoice(itemRef.current, n)) setNotice(`Tidak ada pilihan nomor ${n}.`);
    };
    const removers = DIGITS.map((n) => register({ key: String(n), scope: regionRef, handler: () => press(n) }));
    return () => removers.forEach((remove) => remove());
  }, [register]);

  // --- Render -----------------------------------------------------------------------------------
  // The region is always rendered, so its width is measured from the first paint (D5).
  if (queueQuery.isLoading) {
    return (
      <section ref={regionRef} className="ws-triage" data-panes={onePane ? "one" : "two"} aria-busy="true" aria-label="Memuat Tinjau">
        <Skeleton className="h-[420px]" />
        {!onePane && <Skeleton className="h-[420px]" />}
      </section>
    );
  }
  if (queueQuery.isError || !triage) {
    return (
      <section ref={regionRef} aria-label="Tinjau">
        <ErrorState message="Daftar Tinjau gagal dimuat." onRetry={() => void queueQuery.refetch()} />
      </section>
    );
  }

  const position = selected ? items.findIndex((item) => item.id === selected.id) : -1;
  const headingProps = { headingId: HEADING_ID, pending };

  const body = () => {
    if (selectedId === RUN_PANEL_ID) {
      return (
        <>
          <h2 id={HEADING_ID} className="ws-tri-title" tabIndex={-1} data-item-heading="">Cari saran dari riwayat</h2>
          <HistorySuggestionsSection headingId={HEADING_ID} />
        </>
      );
    }
    if (!selected) return null;
    switch (selected.kind) {
      case "question":
        return <PurposeQuestionCard key={selected.id} entry={selected.entry} onAnswer={answerQuestion} {...headingProps} />;
      case "group":
        return (
          <ReviewGroupCard
            key={selected.id}
            group={selected.group}
            categories={categories}
            automatic={selected.folder === "otomatis"}
            onAction={groupAction(selected)}
            {...headingProps}
          />
        );
      case "awaiting":
        return (
          <AwaitingMovementCard
            key={selected.id}
            item={selected.item}
            accounts={accounts}
            categories={categories}
            candidates={completionChoices[selected.item.id]}
            onComplete={completeMovement(selected)}
            onLinkExisting={merge(selected)}
            onExpense={recordAsExpense(selected)}
            topup={topupFor(selected, selected.item)}
            {...headingProps}
          />
        );
      case "ambiguous":
        return (
          <AmbiguousMovementCard
            key={selected.id}
            item={selected.item}
            onLink={merge(selected)}
            onKeepSeparate={keepSeparate(selected)}
            {...headingProps}
          />
        );
      case "suggestion":
        return <SuggestionGroupCard key={selected.id} group={selected.group} onDecide={decide(selected)} {...headingProps} />;
    }
  };

  const empty = !selected && selectedId !== RUN_PANEL_ID;
  const summary = summaryQuery.data;
  const doneState = allDone && empty;

  return (
    <section ref={regionRef} className="ws-triage" data-panes={onePane ? "one" : "two"} aria-label="Tinjau">
      {showListPane && (
        <TriageList
          triage={triage}
          folder={folder}
          items={items}
          selectedId={selectedId}
          onFolder={chooseFolder}
          onOpen={open}
          foldOpen={foldOpen}
          onFold={(key, value) => setFoldOpen((current) => (current[key] === value ? current : { ...current, [key]: value }))}
          onSearchSuggestions={openRunPanel}
          searchOpen={selectedId === RUN_PANEL_ID}
          listRef={listRef}
        />
      )}
      {showItemPane && (
        <section ref={itemRef} className="ws-card ws-tri-item" aria-labelledby={empty ? undefined : HEADING_ID} aria-label={empty ? (doneState ? "Semua sudah ditinjau" : "Item") : undefined}>
          {!empty && (
            <div className="ws-tri-bar">
              {onePane && (
                <button type="button" className="ws-btn ws-btn-sm" onClick={backToList}>
                  <Icon name="chevron-left" className="h-4 w-4" />
                  Daftar
                </button>
              )}
              <span className="ws-caption tabular">
                {selectedId === RUN_PANEL_ID ? FOLDER_LABELS.saran : position >= 0 ? `${position + 1} dari ${items.length}` : selected ? FOLDER_LABELS[selected.folder] : ""}
              </span>
              <span className="ws-grow" />
              <button type="button" className="ws-btn ws-btn-sm" aria-label="Item sebelumnya" disabled={position <= 0} onClick={() => move(-1, false)}>
                <Icon name="chevron-down" className="h-4 w-4 rotate-180" />
                <kbd className="ws-kbd" aria-hidden="true">K</kbd>
              </button>
              <button type="button" className="ws-btn ws-btn-sm" aria-label="Item berikutnya" disabled={position < 0 || position >= items.length - 1} onClick={() => move(1, false)}>
                <Icon name="chevron-down" className="h-4 w-4" />
                <kbd className="ws-kbd" aria-hidden="true">J</kbd>
              </button>
            </div>
          )}
          <div className="ws-tri-body">
            <div aria-live="polite">{notice && <p className="ws-notice">{notice}</p>}</div>
            {failure && (
              <p role="alert" className="ws-alert">
                {failure.message}
                {failure.transactionId && (
                  <>
                    {" "}
                    <Link href={transaksiHref(failure.transactionId, folder)}>Buka di Transaksi</Link>
                  </>
                )}
              </p>
            )}
            {empty ? (
              doneState ? (
                <EmptyState title="Semua sudah ditinjau">
                  Tidak ada yang perlu dijawab. Transaksi baru dari HP tetap dicatat.
                </EmptyState>
              ) : (
                <EmptyState icon="inbox" title="Pilih satu dari daftar">Atau tekan J untuk mulai dari atas.</EmptyState>
              )
            ) : (
              body()
            )}
            {doneState && (
              <div className="space-y-3 pb-4 text-center">
                {summary && (
                  <dl className="ws-facts tabular" aria-label="Siklus ini">
                    <dt>Pengeluaran siklus ini</dt>
                    <dd>{bal(summary.spending?.amount ?? summary.keluar.total)}</dd>
                    {/* The period's safe amount, as on Beranda (refine-workspace-layout D5); no per-day figure. */}
                    <dt>Aman dipakai sampai gajian</dt>
                    <dd>{bal(Math.max(0, summary.remaining.total))}</dd>
                    <dt>Belum diklasifikasi</dt>
                    <dd>{bal(summary.keluar.lines.find((line) => line.key === "unclassified")?.amount ?? 0)}</dd>
                  </dl>
                )}
                {triage.folders.otomatis.length > 0 && (
                  <p className="ws-caption">{triage.counts.otomatis} transaksi dicatat otomatis dapat dicek. Opsional.</p>
                )}
                <div className="ws-btn-row">
                  <Link href="/" className="ws-btn">Kembali ke Beranda</Link>
                  {triage.folders.otomatis.length > 0 && (
                    <button type="button" className="ws-btn" onClick={() => {
                      setFoldOpen((current) => ({ ...current, otomatis: true }));
                      const first = triage.folders.otomatis[0];
                      if (first) open(first);
                      focusHeading();
                    }}>
                      Cek yang otomatis
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        </section>
      )}
    </section>
  );
}

function withoutKey<T>(record: Record<string, T>, key: string): Record<string, T> {
  const copy = { ...record };
  delete copy[key];
  return copy;
}

function everyItemOf(triage: Triage): TriageItem[] {
  return [...folderItems(triage, "semua"), ...triage.folders.otomatis, ...triage.folders.saran];
}

function questionItem(entry: PurposeQuestionEntry): TriageItem {
  return { kind: "question", id: `q:${entry.event_id}`, folder: "apa", transactionIds: [entry.transaction.id], entry };
}

function focusRow(id: string | null) {
  window.requestAnimationFrame(() => {
    const row = id ? document.querySelector<HTMLElement>(`[data-row="${CSS.escape(id)}"]`) : null;
    row?.focus();
  });
}
