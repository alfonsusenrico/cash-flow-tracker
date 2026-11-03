"use client";

import { useCallback, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useToast } from "@/components/workspace/overlays";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { NOTHING_TO_UNDO, UndoStack, batchOf, undoEntry, undoMessage, type UndoEntry, type UndoFields, type UndoResponse } from "@/lib/reviewUndo";

/** Every view a drawer action can change (transaction-ledger-management "Consistent Post-Mutation Refresh"). */
export function useRefreshTransactionViews() {
  const qc = useQueryClient();
  /**
   * `deletedId`: a row that no longer exists; its detail is not refetched into a 404. `drop` also removes it
   * from the cache, which only a drawer that is closing may do (an open one would fetch it again).
   */
  return useCallback((deletedId?: string, drop = false) => {
    if (deletedId && drop) qc.removeQueries({ queryKey: queryKeys.transactions.detail(deletedId) });
    const deletedDetail = deletedId ? queryKeys.transactions.detail(deletedId).join("/") : null;
    for (const key of [queryKeys.transactions.all, queryKeys.review.all, queryKeys.dashboard.all, queryKeys.accounts,
      queryKeys.goals, queryKeys.obligations, queryKeys.pulse, queryKeys.insights]) {
      qc.invalidateQueries({ queryKey: key, predicate: (query) => query.queryKey.join("/") !== deletedDetail });
    }
  }, [qc]);
}

/**
 * The undo of a page's drawer actions ("Tandai sudah dicek", top-up conversion): the server's token from
 * phase 2's review undo, kept in memory only, offered in the toast and by Z (D9, D12).
 */
export function useTransactionUndo() {
  const stack = useRef(new UndoStack());
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();

  const undo = useCallback(async (entry: UndoEntry) => {
    const outcome = await undoEntry(entry, (token) => api.post<UndoResponse>("/review/undo", { undo_token: token }));
    if (outcome.failure !== "network") stack.current.drop(entry.id);
    refresh();
    showToast(undoMessage(outcome, entry));
  }, [refresh, showToast]);

  /** Shows the toast for a saved action and keeps its token for "Urungkan" and Z. */
  const record = useCallback((text: string, label: string, response: UndoFields | null | undefined, transactionId: string) => {
    const batch = batchOf(response, 1);
    const entry = batch ? stack.current.push({ batches: [batch], label, transactionId, folderLabel: "" }) : null;
    showToast(text, entry ? { onUndo: () => void undo(entry) } : undefined);
  }, [showToast, undo]);

  const undoNewest = useCallback(() => {
    const entry = stack.current.newest();
    if (entry) void undo(entry);
    else showToast(NOTHING_TO_UNDO);
  }, [showToast, undo]);

  return { record, undoNewest };
}
