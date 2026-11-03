import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { THREAD_LIMIT, appendMessage, bindThreadUser, clearThread, threadMessages, updateMessage } from "./thread";
import type { UserMessage } from "./types";

const message = (id: string): UserMessage => ({ id, role: "user", at: "2026-11-03T07:00:00Z", text: `pesan ${id}`, clientCaptureId: `c-${id}`, failed: false });

describe("thread store", () => {
  let writes: string[];

  beforeEach(() => {
    writes = [];
    bindThreadUser("user-a");
    clearThread();
    vi.spyOn(Storage.prototype, "setItem").mockImplementation((key) => { writes.push(`storage:${key}`); });
    Object.defineProperty(document, "cookie", { configurable: true, get: () => "", set: (value: string) => { writes.push(`cookie:${value}`); } });
    vi.stubGlobal("indexedDB", { open: vi.fn(() => { writes.push("indexedDB"); return {}; }) });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    delete (document as unknown as Record<string, unknown>).cookie;
  });

  it("keeps messages in memory and writes nothing to any browser storage", () => {
    appendMessage(message("1"));
    updateMessage("1", (m) => ({ ...m, failed: true }));
    clearThread();
    appendMessage(message("2"));
    expect(threadMessages().map((m) => m.id)).toEqual(["2"]);
    expect(writes).toEqual([]);
  });

  it("empties the thread when another user signs in", () => {
    appendMessage(message("1"));
    bindThreadUser("user-a");
    expect(threadMessages()).toHaveLength(1);
    bindThreadUser("user-b");
    expect(threadMessages()).toEqual([]);
  });

  it("drops the oldest messages beyond the cap", () => {
    for (let i = 0; i < THREAD_LIMIT + 5; i++) appendMessage(message(String(i)));
    const ids = threadMessages().map((m) => m.id);
    expect(ids).toHaveLength(THREAD_LIMIT);
    expect(ids[0]).toBe("5");
    expect(ids.at(-1)).toBe(String(THREAD_LIMIT + 4));
  });

  it("clears on Hapus percakapan", () => {
    appendMessage(message("1"));
    clearThread();
    expect(threadMessages()).toEqual([]);
  });

  it("updates a message in place and ignores an unknown id", () => {
    appendMessage(message("1"));
    updateMessage("1", (m) => ({ ...m, failed: true }));
    updateMessage("nope", (m) => m);
    expect((threadMessages()[0] as UserMessage).failed).toBe(true);
  });
});
