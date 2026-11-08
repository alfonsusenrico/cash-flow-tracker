"use client";

import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useAppCtx } from "@/components/layout/AppLayout";
import { HistorySuggestionsDialog } from "@/components/review/HistorySuggestionsDialog";
import type { CategoriesState, CategoryOption } from "@/components/review/CategoryDialog";
import type { AccountOptionItem } from "@/components/ui/AccountSelectOptions";
import { Icon } from "@/components/ui/Icon";
import { PageActions } from "@/components/workspace/context";
import { HeroFigure } from "@/components/workspace/figures";
import { PHONE_QUERY, useMediaQuery } from "@/components/workspace/layers";
import { useMoney } from "@/components/workspace/money";
import { useToast } from "@/components/workspace/overlays";
import { EmptyState } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { usePageShortcutGroup, useShortcut, useShortcuts, type PageShortcutGroup } from "@/components/workspace/shortcuts";
import { isLiquidAccountChoice } from "@/lib/accountOptions";
import { api } from "@/lib/api";
import { chunkSuggestionIds, type SuggestionDecisionResult, type SuggestionList } from "@/lib/historySuggestions";
import { canSwitchToAmountTracking, listAmountInvestmentProducts, type InvestmentAccount } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";
import {
  chunkIds,
  completionCandidates,
  newReplyId,
  reviewErrorMessage,
  type AmbiguousItem,
  type CompletionCandidate,
  type LearnedAccountLink,
  type PurposeQuestionEntry,
  type ReviewItem,
  type ReviewQueue,
  type SeenTwiceItem,
  type TopupProductChoice,
} from "@/lib/review";
import { batchOf, undoEntry, undoMessage, undoText, UndoStack, type UndoBatch, type UndoEntry, type UndoFields, type UndoResponse } from "@/lib/reviewUndo";
import {
  buildTriage,
  folderFromParams,
  folderItems,
  isOptionalFolder,
  itemTransactionId,
  neededCount,
  nextAfter,
  resolveDeepLink,
  triageSearch,
  type FolderKey,
  type Triage,
  type TriageItem,
} from "@/lib/triage";
import { pressChoice, pressMarked } from "./Answers";
import { HEADING_ID, ItemBody, ItemHeadRow, StatusLine, type Failure } from "./ItemBody";
import { TriageList, TriageRail } from "./TriageList";
import { folderName, itemTitle, kindName, useTriageWords } from "./copy";
import { groupParts, type GroupActionRequest } from "./items/GroupItem";
import { ambiguousParts, awaitingParts, type TopupOptions } from "./items/MovementItems";
import { questionParts, type PurposeAnswerBody } from "./items/QuestionItem";
import { seenTwiceParts } from "./items/SeenTwiceItem";
import { suggestionParts, type SuggestionDecisionRequest } from "./items/SuggestionItem";
import type { ItemContext, ItemParts, SaveResult } from "./types";

const DIGITS = [1, 2, 3, 4, 5, 6, 7, 8, 9];
/** The server lists at most this many rows; a truncated queue says so (review-queue "Review Queue"). */
const QUEUE_LIMIT = 1000;

interface ActionOutcome {
  /** The saved action in words, inside "Tersimpan: …". */
  message: string;
  batches: (UndoBatch | null)[];
  /** The item stays (for example when the server asks to choose an existing leg). */
  stay?: boolean;
}

/** "Induk · Produk" choices for top-ups and the unit-tracked funds that must switch first. */
function topupChoices(accounts: InvestmentAccount[]): { products: TopupProductChoice[]; unitFunds: TopupProductChoice[] } {
  const flattened = [...new Map(accounts.flatMap((account) => [account, ...(account.children ?? [])]).map((account) => [account.id, account])).values()];
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

function focusRow(id: string | null, fallback: HTMLElement | null) {
  window.requestAnimationFrame(() => {
    const row = id ? document.querySelector<HTMLElement>(`[data-row="${CSS.escape(id)}"]`) : null;
    (row ?? fallback?.querySelector<HTMLElement>("[data-row]"))?.focus();
  });
}

/** A dialog opened over the item (category, answer, help) owns the keys; the phone's item sheet does not. */
function insideItemDialog(): boolean {
  const dialog = document.activeElement?.closest("dialog");
  return Boolean(dialog && !dialog.classList.contains("tj-sheet"));
}

function everyItemOf(triage: Triage): TriageItem[] {
  return [...folderItems(triage, "semua"), ...triage.folders.otomatis, ...triage.folders.saran, ...triage.folders.dua, ...triage.folders.lewat];
}

function questionItem(entry: PurposeQuestionEntry): TriageItem {
  return { kind: "question", id: `q:${entry.event_id}`, folder: "apa", transactionIds: [entry.transaction.id], entry };
}

function withoutKey<T>(record: Record<string, T>, key: string): Record<string, T> {
  const copy = { ...record };
  delete copy[key];
  return copy;
}

/** What each kind's answers send; every action goes through the page's `act` (auto-advance, toast, Urungkan). */
interface ItemHandlers {
  accounts: AccountOptionItem[];
  categories: CategoriesState;
  completionChoices: Record<string, CompletionCandidate[]>;
  answerQuestion: (entry: PurposeQuestionEntry) => (body: PurposeAnswerBody) => Promise<SaveResult>;
  groupAction: (item: TriageItem) => (request: GroupActionRequest) => Promise<SaveResult>;
  completeMovement: (item: TriageItem) => (leg: ReviewItem, accountId: string, createNew?: boolean) => Promise<SaveResult>;
  merge: (item: TriageItem) => (leg: ReviewItem | AmbiguousItem, counterpartId: string) => Promise<SaveResult>;
  keepSeparate: (item: TriageItem) => (leg: AmbiguousItem) => Promise<SaveResult>;
  recordAsSpending: (item: TriageItem) => (leg: ReviewItem, fields: Record<string, string>) => Promise<SaveResult>;
  topupFor: (item: TriageItem, leg: ReviewItem) => TopupOptions | undefined;
  acknowledgeSeenTwice: (item: TriageItem) => (entry: SeenTwiceItem) => Promise<SaveResult>;
  recordSeenTwice: (item: TriageItem) => (entry: SeenTwiceItem) => Promise<SaveResult>;
  decide: (item: TriageItem) => (request: SuggestionDecisionRequest) => Promise<SaveResult>;
}

/** The selected item's body: its kind's parts in the one item pattern (D7, D9). */
function ItemContent({ item, ctx, status, handlers }: { item: TriageItem; ctx: ItemContext; status: ReactNode; handlers: ItemHandlers }) {
  const parts = ((): ItemParts => {
    switch (item.kind) {
      case "question":
        return questionParts(item.entry, ctx, handlers.answerQuestion(item.entry));
      case "group":
        return groupParts(item.group, item.folder === "otomatis", ctx, handlers.categories, handlers.groupAction(item));
      case "awaiting":
        return awaitingParts(item.item, ctx, {
          accounts: handlers.accounts,
          categories: handlers.categories,
          candidates: handlers.completionChoices[item.item.id],
          onComplete: handlers.completeMovement(item),
          onLinkExisting: handlers.merge(item),
          onSpending: handlers.recordAsSpending(item),
          topup: handlers.topupFor(item, item.item),
        });
      case "ambiguous":
        return ambiguousParts(item.item, ctx, handlers.merge(item), handlers.keepSeparate(item));
      case "suggestion":
        return suggestionParts(item.group, ctx, handlers.decide(item));
      case "seen_twice":
        return seenTwiceParts(item.item, ctx, handlers.acknowledgeSeenTwice(item), handlers.recordSeenTwice(item));
    }
  })();
  return <ItemBody parts={parts} status={status} />;
}

/** Static placeholders in the panes' final geometry (D15): no hero, count or amount, nothing pulses. */
function TriageSkeleton({ phone }: { phone: boolean }) {
  return (
    <>
      <div className="tj-rail rail is-chips" aria-hidden="true">
        {[64, 56, 72, 60, 52].map((width) => (
          <span key={width} className="rail-skel">
            <span className="skel" style={{ width: `${width}%` }} />
          </span>
        ))}
      </div>
      <div className="tj-list" aria-hidden="true">
        {[46, 38, 52, 30, 44, 36].map((width) => (
          <div key={width} className="skel-row is-stacked">
            <span className="skel" style={{ width: 40 }} />
            <span className="skel" style={{ width: `${width}%` }} />
            <span className="grow" />
            <span className="skel" style={{ width: 96 }} />
          </div>
        ))}
      </div>
      {!phone && (
        <div className="tj-detail" aria-hidden="true">
          {["30%", "70%", "50%"].map((width) => (
            <span key={width} className="skel tj-skel-line" style={{ width }} />
          ))}
        </div>
      )}
    </>
  );
}

/**
 * Tinjau (apply-markas-tinjau): the title block with "…", the hero, and the folder rail, item list and item in one
 * keyboard region. Folder membership, auto-advance, Urungkan, deep links and the keys are unchanged; on a phone the
 * item opens in a full-height sheet with its own history entry.
 */
export function TriageInbox() {
  const qc = useQueryClient();
  const words = useTriageWords();
  const { t, format } = words;
  const money = useMoney();
  const { hideBalances } = useAppCtx();
  const { showToast } = useToast();
  const phone = useMediaQuery(PHONE_QUERY);
  const regionRef = useRef<HTMLElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const itemRef = useRef<HTMLDivElement>(null);
  const undoStack = useRef(new UndoStack());
  const acting = useRef(false);
  const pushedItem = useRef(false);
  const lastIds = useRef<string[]>([]);
  const deepLinkRead = useRef(false);

  const [folder, setFolder] = useState<FolderKey>("semua");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [sheetOpen, setSheetOpen] = useState(false);
  const [pending, setPending] = useState(false);
  const [notice, setNotice] = useState("");
  const [failure, setFailure] = useState<Failure | null>(null);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [completionChoices, setCompletionChoices] = useState<Record<string, CompletionCandidate[]>>({});

  const pageKeys = useMemo<PageShortcutGroup>(
    () => ({
      name: t("review.keys.group"),
      translated: true,
      entries: [
        { keys: ["J", "K"], text: t("review.keys.move") },
        { keys: ["Enter"], text: t("review.keys.open") },
        { keys: ["E"], text: t("review.keys.accept") },
        { keys: ["T"], text: t("review.keys.topup") },
        { keys: ["1–9"], text: t("review.keys.numbered") },
        { keys: ["Z"], text: t("review.keys.undo") },
      ],
    }),
    [t],
  );
  usePageShortcutGroup(pageKeys);
  const { register, openHelp } = useShortcuts();

  const queueQuery = useQuery<ReviewQueue>({ queryKey: queryKeys.review.queue, queryFn: () => api.get("/review/queue") });
  const suggestionsQuery = useQuery<SuggestionList>({ queryKey: queryKeys.review.suggestions, queryFn: () => api.get("/review/suggestions") });
  const categoriesQuery = useQuery<{ categories: CategoryOption[] }>({ queryKey: queryKeys.categories, queryFn: () => api.get("/categories") });
  const { data: accountsData } = useQuery<{ accounts: AccountOptionItem[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });

  const triage: Triage | null = useMemo(
    () => (queueQuery.data ? buildTriage(queueQuery.data, suggestionsQuery.data) : null),
    [queueQuery.data, suggestionsQuery.data],
  );
  const items = useMemo(() => (triage ? folderItems(triage, folder) : []), [triage, folder]);
  const everyItem = useMemo(() => (triage ? everyItemOf(triage) : []), [triage]);
  const selected = everyItem.find((item) => item.id === selectedId) ?? null;
  const categories: CategoriesState = {
    categories: categoriesQuery.data?.categories ?? [],
    failed: categoriesQuery.isError,
    retry: () => void categoriesQuery.refetch(),
  };
  const accounts = useMemo(() => accountsData?.accounts ?? [], [accountsData]);
  const choices = useMemo(() => topupChoices(accounts as unknown as InvestmentAccount[]), [accounts]);

  // --- URL state (redesign-review-triage D7) ----------------------------------------------------
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
      setSelectedId(folderItems(triage, "semua")[0]?.id ?? null);
      return;
    }
    const link = resolveDeepLink(triage, transactionId, linkedFolder);
    const target = link.itemId ?? folderItems(triage, link.folder)[0]?.id ?? null;
    setFolder(link.folder);
    setSelectedId(target);
    if (link.resolved) setNotice(t("review.notice.linkDone"));
    const item = everyItemOf(triage).find((entry) => entry.id === target) ?? null;
    writeUrl(link.folder, link.itemId ? item : null, false);
    if (link.itemId) {
      setSheetOpen(true);
      focusHeading();
    }
  }, [triage, writeUrl, t]);

  // Selection is held by id across refetches; an item answered elsewhere is replaced by the next one.
  useEffect(() => {
    if (!triage || !deepLinkRead.current) return;
    const ids = items.map((item) => item.id);
    const visible = new Set(everyItemOf(triage).map((item) => item.id));
    if (!acting.current && selectedId && !visible.has(selectedId)) {
      setSelectedId(nextAfter(lastIds.current, selectedId, new Set(ids)));
      setNotice(t("review.notice.doneElsewhere"));
    }
    lastIds.current = ids;
  }, [triage, items, selectedId, t]);

  // On the phone the browser's back gesture closes the item's sheet (D13).
  useEffect(() => {
    const onPop = () => {
      if (!sheetOpen || !pushedItem.current) return;
      pushedItem.current = false;
      setSheetOpen(false);
      focusRow(selectedId, listRef.current);
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, [sheetOpen, selectedId]);

  const closeSheet = useCallback(() => {
    if (pushedItem.current) {
      window.history.back();
      return;
    }
    setSheetOpen(false);
    focusRow(selectedId, listRef.current);
  }, [selectedId]);

  // --- Selection ------------------------------------------------------------------------------
  const open = useCallback((item: TriageItem) => {
    setNotice("");
    setFailure(null);
    setSelectedId(item.id);
    const nextFolder = isOptionalFolder(item.folder) ? item.folder : isOptionalFolder(folder) ? "semua" : folder;
    setFolder(nextFolder);
    if (phone) {
      setSheetOpen(true);
      pushedItem.current = true;
      writeUrl(nextFolder, item, true);
      focusHeading();
    } else {
      writeUrl(nextFolder, item, false);
    }
  }, [folder, phone, writeUrl]);

  const chooseFolder = useCallback((next: FolderKey) => {
    if (!triage) return;
    setFolder(next);
    setNotice("");
    const first = folderItems(triage, next)[0] ?? null;
    setSelectedId(first?.id ?? null);
    writeUrl(next, phone ? null : first, false);
  }, [triage, phone, writeUrl]);

  const move = useCallback((step: 1 | -1, fromList: boolean) => {
    const index = items.findIndex((item) => item.id === selectedId);
    const next = items[index + step];
    if (!next) {
      setNotice(step > 0 ? t("review.notice.last") : t("review.notice.first"));
      return;
    }
    setNotice("");
    setFailure(null);
    setSelectedId(next.id);
    writeUrl(folder, next, false);
    if (fromList) focusRow(next.id, listRef.current);
    else focusHeading();
  }, [items, selectedId, folder, writeUrl, t]);

  // --- Actions and auto-advance (redesign-review-triage D6) ------------------------------------
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

  const errorText = useCallback((error: unknown) => reviewErrorMessage(error) ?? t("review.notice.requestFailed"), [t]);

  const undo = useRef<() => Promise<void>>(async () => {});

  const act = useCallback(async (item: TriageItem, work: () => Promise<ActionOutcome>): Promise<SaveResult> => {
    const before = (isOptionalFolder(item.folder) && triage ? folderItems(triage, item.folder) : items).map((entry) => entry.id);
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
          label: itemTitle(t, item),
          transactionId: itemTransactionId(item),
          folderLabel: folderName(t, item.folder),
        })
      : null;
    await refresh();
    acting.current = false;
    setPending(false);
    const withUndo = entry ? { onUndo: () => void undo.current() } : undefined;
    if (error || !outcome) {
      const message = errorText(error);
      setFailure({ message });
      return { saved: false, error: message };
    }
    const fresh = latestTriage();
    if (outcome.stay || !fresh) {
      showToast(t("review.toast.saved", { message: outcome.message }), withUndo);
      return { saved: true };
    }
    let nextFolder: FolderKey = isOptionalFolder(item.folder) ? item.folder : folder;
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
    writeUrl(nextFolder, phone && !next ? null : next, false);
    if (phone && !next) {
      if (pushedItem.current) {
        pushedItem.current = false;
        window.history.back();
      }
      setSheetOpen(false);
      focusRow(null, listRef.current);
    }
    const text = next
      ? t("review.toast.savedNext", { message: outcome.message, title: itemTitle(t, next) })
      : neededCount(fresh) === 0
        ? t("review.toast.savedAllDone", { message: outcome.message })
        : t("review.toast.saved", { message: outcome.message });
    showToast(text, withUndo);
    if (next) focusHeading();
    return { saved: true };
  }, [triage, items, folder, phone, refresh, latestTriage, showToast, writeUrl, errorText, t]);

  const runUndo = useCallback(async () => {
    const entry: UndoEntry | null = undoStack.current.newest();
    if (!entry) {
      setNotice(t("review.undo.nothing"));
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
    const message = undoText(t, undoMessage(outcome, entry));
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
        writeUrl(link.folder, everyItemOf(fresh).find((item) => item.id === link.itemId) ?? null, false);
        if (phone) setSheetOpen(true);
      }
    }
    showToast(message);
    focusHeading();
  }, [refresh, latestTriage, showToast, writeUrl, phone, t]);
  useLayoutEffect(() => {
    undo.current = runUndo;
  }, [runUndo]);

  const answerQuestion = (entry: PurposeQuestionEntry) => (body: PurposeAnswerBody) =>
    act(questionItem(entry), async () => {
      const response = await api.post<{ answer: { status: "applied" | "needs_review"; notes: string | null } } & UndoFields>(
        `/ingest/notifications/${entry.event_id}/answer-purpose`,
        { question_id: entry.question.question_id, reply_id: newReplyId(), ...body },
      );
      const applied = response.answer?.status === "applied";
      return {
        message: !applied
          ? t("review.toast.answerAsNote")
          : response.answer.notes
            ? t("review.toast.answerApplied", { notes: response.answer.notes })
            : t("review.toast.answerSaved"),
        batches: [batchOf(response, 1)],
      };
    });

  const groupAction = (item: TriageItem) => (request: GroupActionRequest) =>
    act(item, async () => {
      // Each request is one database transaction; a group larger than a batch is sent in parts.
      const batches: UndoBatch[] = [];
      let done = 0;
      for (const ids of chunkIds(request.ids)) {
        try {
          const response = await api.post<UndoFields>("/review/bulk", { action: request.action, transaction_ids: ids, ...request.fields });
          const batch = batchOf(response, ids.length);
          if (batch) batches.push(batch);
        } catch (failed) {
          if (done === 0) throw failed;
          const partial = new Error(t("review.toast.partialRows", { done: format.integer(done), total: format.integer(request.ids.length), error: errorText(failed) }));
          throw Object.assign(partial, { savedBatches: batches });
        }
        done += ids.length;
      }
      const values = { label: request.label, count: done };
      const message =
        request.action === "recategorise"
          ? t("review.toast.groupRecategorised", values)
          : request.action === "rename"
            ? t("review.toast.groupRenamed", values)
            : t("review.toast.groupAccepted", values);
      return { message, batches };
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
          message: response.linked_transaction_id ? t("review.toast.linkedExisting") : t("review.toast.transferCompleted"),
          batches: [batchOf(response, 2)],
        };
      } catch (failed) {
        const candidates = completionCandidates(failed);
        if (!candidates) throw failed;
        // The other leg may already be recorded; creating another would count the money twice.
        setCompletionChoices((current) => ({ ...current, [leg.id]: candidates }));
        return { message: t("review.toast.candidatesFound"), batches: [], stay: true };
      }
    });

  const merge = (item: TriageItem) => (leg: ReviewItem | AmbiguousItem, counterpartId: string) =>
    act(item, async () => {
      const [expense, income] = leg.type === "expense" ? [leg.id, counterpartId] : [counterpartId, leg.id];
      const response = await api.post<UndoFields>("/movements/merge", { expense_transaction_id: expense, income_transaction_id: income });
      setCompletionChoices((current) => withoutKey(current, leg.id));
      return { message: t("review.toast.merged"), batches: [batchOf(response, 2)] };
    });

  const keepSeparate = (item: TriageItem) => (leg: AmbiguousItem) =>
    act(item, async () => {
      const response = await api.post<UndoFields>("/review/bulk", { action: "keep_separate", transaction_ids: [leg.id] });
      return { message: t("review.toast.keptSeparate"), batches: [batchOf(response, 1)] };
    });

  const recordAsSpending = (item: TriageItem) => (leg: ReviewItem, fields: Record<string, string>) =>
    act(item, async () => {
      const response = await api.post<UndoFields>("/review/bulk", { action: "recategorise", transaction_ids: [leg.id], ...fields });
      return { message: t("review.toast.recordedSpending"), batches: [batchOf(response, 1)] };
    });

  const topupFor = (item: TriageItem, leg: ReviewItem): TopupOptions | undefined => {
    const flattened = accounts.flatMap((account) => [account, ...(account.children ?? [])]);
    const source = flattened.find((account) => (account.id ?? account.account_id) === leg.account_id);
    if (leg.type !== "expense" || !source || !isLiquidAccountChoice(source)) return undefined;
    return {
      ...choices,
      onConvert: (debit, productId, suggestedId) => act(item, async () => {
        const response = await api.post<UndoFields>("/investment-topups/from-transaction", {
          transaction_id: debit.id,
          target_account_id: productId,
          ...(suggestedId ? { suggested_target_account_id: suggestedId } : {}),
        });
        return { message: t("review.toast.topupRecorded"), batches: [batchOf(response, 2)] };
      }),
      onReject: (debit, suggestedId) => act(item, async () => {
        const response = await api.post<UndoFields>("/review/topup-suggestions/reject", {
          transaction_id: debit.id,
          ...(suggestedId ? { suggested_target_account_id: suggestedId } : {}),
        });
        return { message: t("review.toast.notTopup"), batches: [batchOf(response, 1)], stay: true };
      }),
    };
  };

  // "Batalkan" on the learned-link toast stops the link; the acknowledged report stays settled.
  const stopLearnedLink = async (link: LearnedAccountLink) => {
    try {
      await api.del(`/ingest/account-links/${link.id}`);
      await qc.invalidateQueries({ queryKey: ["account-links"] });
      showToast(t("review.toast.linkCancelled", { institution: link.institution_label }));
    } catch (error) {
      setFailure({ message: errorText(error) });
    }
  };

  const acknowledgeSeenTwice = (item: TriageItem) => async (entry: SeenTwiceItem) => {
    const learned: { link: LearnedAccountLink | null } = { link: null };
    const result = await act(item, async () => {
      const response = await api.post<{ link: LearnedAccountLink | null }>(`/review/seen-twice/${entry.event_id}/acknowledge`, {});
      learned.link = response.link?.created ? response.link : null;
      return { message: t("review.toast.acknowledged"), batches: [] };
    });
    const link = learned.link;
    if (result.saved && link) {
      void qc.invalidateQueries({ queryKey: ["account-links"] });
      showToast(t("review.toast.learnedLink", { account: link.account_label ?? t("review.answer.account"), institution: link.institution_label }), {
        actionLabel: t("review.toast.cancelLink"),
        onUndo: () => void stopLearnedLink(link),
      });
    }
    return result;
  };

  const recordSeenTwice = (item: TriageItem) => (entry: SeenTwiceItem) =>
    act(item, async () => {
      const response = await api.post<UndoFields>(`/review/seen-twice/${entry.event_id}/record-separately`, {});
      return { message: t("review.toast.recordedSeparately"), batches: [batchOf(response, 1)] };
    });

  const decide = (item: TriageItem) => (request: SuggestionDecisionRequest) =>
    act(item, async () => {
      const batches: UndoBatch[] = [];
      const accepting = request.decision === "accept";
      let done = 0;
      let skipped = 0;
      for (const ids of chunkSuggestionIds(request.ids)) {
        try {
          const result = await api.post<SuggestionDecisionResult & UndoFields>(`/review/suggestions/${request.decision}`, { suggestion_ids: ids });
          const count = (accepting ? result.accepted : result.rejected) ?? 0;
          done += count;
          skipped += result.skipped.length;
          const batch = batchOf(result, count);
          if (batch) batches.push(batch);
        } catch (failed) {
          if (done === 0 && skipped === 0) throw failed;
          const values = { done: format.integer(done), total: format.integer(request.ids.length), error: errorText(failed) };
          const partial = new Error(accepting ? t("review.toast.partialAccepted", values) : t("review.toast.partialRejected", values));
          throw Object.assign(partial, { savedBatches: batches });
        }
      }
      const values = { label: request.label, count: done, skipped: format.integer(skipped) };
      const message = skipped > 0
        ? accepting ? t("review.toast.suggestionsAcceptedSkipped", values) : t("review.toast.suggestionsRejectedSkipped", values)
        : accepting ? t("review.toast.suggestionsAccepted", values) : t("review.toast.suggestionsRejected", values);
      return { message, batches };
    });

  // --- Keys (redesign-review-triage D8, apply-markas-tinjau D16) --------------------------------
  const inList = () => Boolean(listRef.current?.contains(document.activeElement));
  useShortcut("j", () => {
    if (!insideItemDialog()) move(1, inList());
  }, regionRef);
  useShortcut("k", () => {
    if (!insideItemDialog()) move(-1, inList());
  }, regionRef);
  useShortcut("e", () => {
    if (insideItemDialog()) return;
    if (!pressMarked(itemRef.current, "data-primary-choice")) setNotice(t("review.notice.noSuggested"));
  }, regionRef);
  useShortcut("t", () => {
    if (insideItemDialog()) return;
    if (!pressMarked(itemRef.current, "data-topup")) setNotice(t("review.notice.topupOnly"));
  }, regionRef);
  useShortcut("z", () => {
    if (!insideItemDialog()) void runUndo();
  }, regionRef);
  useEffect(() => {
    const press = (n: number) => {
      if (insideItemDialog()) return;
      if (!pressChoice(itemRef.current, n)) setNotice(t("review.notice.noAnswer", { n: String(n) }));
    };
    const removers = DIGITS.map((n) => register({ key: String(n), scope: regionRef, handler: () => press(n) }));
    return () => removers.forEach((remove) => remove());
  }, [register, t]);

  // --- Render -----------------------------------------------------------------------------------
  const actions = (
    <PageActions
      moreLabel={t("review.page.more")}
      more={[
        { key: "suggestions", label: t("review.page.findSuggestions"), icon: "search", onSelect: () => setHistoryOpen(true) },
        { key: "help", label: t("review.page.help"), icon: "help", onSelect: openHelp },
      ]}
    />
  );
  const historyDialog = <HistorySuggestionsDialog open={historyOpen} onClose={() => setHistoryOpen(false)} />;

  if (queueQuery.isLoading) {
    return (
      <>
        {actions}
        <section ref={regionRef} className="tj" aria-busy="true" aria-label={t("review.states.loading")}>
          <TriageSkeleton phone={phone} />
        </section>
        {historyDialog}
      </>
    );
  }
  if (queueQuery.isError || !triage) {
    return (
      <>
        {actions}
        <section ref={regionRef} aria-label={t("review.page.region")}>
          <div className="error-box" role="alert">
            <Icon name="alert" className="h-5 w-5" />
            <div className="grow">
              <p className="t-strong">{t("review.states.error")}</p>
              <p className="t-meta">{t("review.states.errorDetail")}</p>
            </div>
            <button type="button" className="ws-btn" onClick={() => void queueQuery.refetch()}>
              {t("common.tryAgain")}
            </button>
          </div>
        </section>
        {historyDialog}
      </>
    );
  }

  const nothingAtAll = everyItem.length === 0;
  const position = selected ? items.findIndex((item) => item.id === selected.id) : -1;
  const ctx: ItemContext = { words, money, hidden: hideBalances, pending, headingId: HEADING_ID };
  const status = <StatusLine notice={notice} failure={failure} folder={folder} />;

  const handlers: ItemHandlers = {
    accounts,
    categories,
    completionChoices,
    answerQuestion,
    groupAction,
    completeMovement,
    merge,
    keepSeparate,
    recordAsSpending,
    topupFor,
    acknowledgeSeenTwice,
    recordSeenTwice,
    decide,
  };

  const checkAutomatic = () => {
    const first = triage.folders.otomatis[0];
    if (first) open(first);
    focusHeading();
  };
  const doneState = (
    <EmptyState
      title={t("review.states.done.title")}
      action={
        triage.folders.otomatis.length > 0 ? (
          <button type="button" className="ws-btn" onClick={checkAutomatic}>
            {t("review.states.done.checkAuto")}
          </button>
        ) : undefined
      }
    >
      {t("review.states.done.line")}
    </EmptyState>
  );

  if (nothingAtAll) {
    return (
      <>
        {actions}
        <section ref={regionRef} className="tj-empty" aria-label={t("review.page.region")}>
          <EmptyState title={t("review.states.done.title")}>{t("review.states.done.line")}</EmptyState>
        </section>
        {historyDialog}
      </>
    );
  }

  const detail = selected ? (
    <>
      {!phone && <ItemHeadRow kind={kindName(t, selected)} index={position} total={items.length} onMove={(step) => move(step, false)} />}
      <ItemContent item={selected} ctx={ctx} status={status} handlers={handlers} />
    </>
  ) : (
    <>
      {status}
      {neededCount(triage) === 0 && folder === "semua" ? doneState : null}
    </>
  );

  return (
    <>
      {actions}
      <HeroFigure
        id="tinjau-hero"
        label={t("review.hero.label")}
        figure={format.integer(triage.attention)}
        line={t("review.hero.line", { unreviewed: format.integer(triage.unreviewed) })}
      >
        {triage.truncated && <p className="hero-sub t-body">{t("review.hero.truncated", { limit: format.integer(QUEUE_LIMIT) })}</p>}
      </HeroFigure>
      <section ref={regionRef} className="tj" aria-label={t("review.page.region")}>
        <TriageRail triage={triage} folder={folder} onFolder={chooseFolder} words={words} />
        {phone && !sheetOpen && status}
        <TriageList
          triage={triage}
          folder={folder}
          items={items}
          selectedId={phone && !sheetOpen ? null : selectedId}
          onOpen={open}
          listRef={listRef}
          words={words}
          empty={phone && folder === "semua" && neededCount(triage) === 0 ? doneState : undefined}
        />
        {phone ? (
          <ShellDialog
            open={sheetOpen && Boolean(selected)}
            onClose={closeSheet}
            title={selected ? kindName(t, selected) : ""}
            width={880}
            closeLabel={t("review.sheet.close")}
            className="tj-sheet"
            fullHeight
          >
            <div ref={itemRef} className="tj-detail">
              {selected && detail}
            </div>
          </ShellDialog>
        ) : (
          <section className="tj-detail" aria-label={t("review.detail.region")}>
            <div ref={itemRef}>{detail}</div>
          </section>
        )}
      </section>
      {historyDialog}
    </>
  );
}
