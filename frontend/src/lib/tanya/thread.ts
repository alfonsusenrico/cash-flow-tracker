import { useCallback, useSyncExternalStore } from "react";
import type { ThreadMessage } from "./types";

/**
 * Tanya's conversation, held in module memory only (add-tanya-conversation D7, OD64). It survives in-app
 * navigation within the tab and is gone on reload, tab close and sign-out (a full page load). It never
 * touches localStorage, sessionStorage, IndexedDB, cookies or the URL, and it resets when the signed-in
 * user changes.
 */

export const THREAD_LIMIT = 100;

interface ThreadState {
  userId: string | null;
  messages: ThreadMessage[];
}

const EMPTY: ThreadMessage[] = [];
let state: ThreadState = { userId: null, messages: EMPTY };
const listeners = new Set<() => void>();

function emit(next: ThreadState) {
  state = next;
  for (const listener of listeners) listener();
}

export function subscribeThread(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function threadMessages(): ThreadMessage[] {
  return state.messages;
}

/** Binds the store to the signed-in user; another user's id empties it first. */
export function bindThreadUser(userId: string): void {
  if (state.userId === userId) return;
  emit({ userId, messages: EMPTY });
}

export function appendMessage(message: ThreadMessage): void {
  const messages = [...state.messages, message];
  emit({ ...state, messages: messages.length > THREAD_LIMIT ? messages.slice(messages.length - THREAD_LIMIT) : messages });
}

export function updateMessage(id: string, change: (message: ThreadMessage) => ThreadMessage): void {
  if (!state.messages.some((message) => message.id === id)) return;
  emit({ ...state, messages: state.messages.map((message) => (message.id === id ? change(message) : message)) });
}

export function clearThread(): void {
  emit({ ...state, messages: EMPTY });
}

/** The messages of the signed-in user. */
export function useThread(): ThreadMessage[] {
  const read = useCallback(() => state.messages, []);
  return useSyncExternalStore(subscribeThread, read, () => EMPTY);
}
