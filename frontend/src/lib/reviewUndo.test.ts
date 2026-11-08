import { describe, expect, it, vi } from "vitest";
import { testWords } from "@/lib/i18n/testing";
import { batchOf, undoEntry, undoMessage, undoText, UndoStack, type UndoResponse } from "./reviewUndo";

const NOW = Date.parse("2026-11-03T05:00:00Z");
const later = (minutes: number) => new Date(NOW + minutes * 60_000).toISOString();
const ok: UndoResponse = { ok: true, kind: "review_accept", already_undone: false, transaction_ids: [], removed_transaction_ids: [], restored: [] };

function refusal(status: number, detail: Record<string, unknown> = {}) {
  return Object.assign(new Error("refused"), { status, detail: { code: "x", ...detail } });
}

describe("undo stack", () => {
  it("keeps the newest first, at most twenty, until each expires", () => {
    let now = NOW;
    const stack = new UndoStack(() => now);
    for (let index = 0; index < 22; index += 1) {
      stack.push({ batches: [batchOf({ undo_token: `t${index}`, expires_at: later(10) }, 1)!], label: `Item ${index}`, transactionId: null, folderLabel: "Semua" });
    }
    expect(stack.newest()?.label).toBe("Item 21");
    // "Z after the toast closed": the entry is still there two minutes later.
    now = NOW + 2 * 60_000;
    expect(stack.newest()?.label).toBe("Item 21");
    now = NOW + 10 * 60_000;
    expect(stack.newest()).toBeNull();
  });

  it("pushes nothing for a response without a token", () => {
    const stack = new UndoStack(() => NOW);
    expect(batchOf({ undo_token: null, expires_at: null }, 2)).toBeNull();
    expect(batchOf({}, 2)).toBeNull();
    expect(stack.push({ batches: [], label: "Kedai Awan", transactionId: "tx", folderLabel: "Perlu jawaban" })).toBeNull();
    expect(stack.newest()).toBeNull();
    expect(testWords("id").t("review.undo.nothing")).toBe("Tidak ada yang dapat dibatalkan.");
  });

  it("writes nothing to storage or the URL", () => {
    const before = { local: { ...localStorage }, session: { ...sessionStorage }, url: window.location.href };
    const stack = new UndoStack(() => NOW);
    stack.push({ batches: [batchOf({ undo_token: "secret-token", expires_at: later(10) }, 1)!], label: "Kedai Awan", transactionId: "tx", folderLabel: "Perlu jawaban" });
    expect({ local: { ...localStorage }, session: { ...sessionStorage }, url: window.location.href }).toEqual(before);
    expect(JSON.stringify(localStorage) + JSON.stringify(sessionStorage) + window.location.href).not.toContain("secret-token");
  });
});

describe("undoing an entry", () => {
  const entry = (rows: number[]) => {
    const stack = new UndoStack(() => NOW);
    return stack.push({
      batches: rows.map((count, index) => batchOf({ undo_token: `token-${index}`, expires_at: later(10) }, count)!),
      label: "Kedai Awan", transactionId: "tx-1", folderLabel: "Perlu jawaban",
    })!;
  };

  it("undoes batches newest first and reports a refused older batch", async () => {
    const post = vi.fn(async (token: string) => {
      if (token === "token-0") throw refusal(409, { transaction_ids: ["tx-1"] });
      return ok;
    });
    const subject = entry([200, 50]);
    const outcome = await undoEntry(subject, post);
    expect(post.mock.calls.map(([token]) => token)).toEqual(["token-1", "token-0"]);
    expect(outcome).toEqual({ restored: 50, total: 250, failure: "conflict", conflictIds: ["tx-1"] });
    expect(undoText(testWords("id").t, undoMessage(outcome, subject))).toBe("50 dari 250 transaksi dibatalkan. Sisanya: Tidak dapat dibatalkan: transaksi sudah berubah sejak itu.");
    expect(undoText(testWords("en").t, undoMessage(outcome, subject))).toBe("50 of 250 transactions undone. The rest: It cannot be undone: the transaction changed since then.");
  });

  it("maps 404, 409, 410 and a network failure", async () => {
    const subject = entry([1]);
    const cases: [unknown, string][] = [
      [refusal(410), "Sudah lewat 10 menit, jadi tidak dapat dibatalkan di sini."],
      [refusal(409), "Tidak dapat dibatalkan: transaksi sudah berubah sejak itu."],
      [refusal(404), "Tidak dapat dibatalkan lagi di sini."],
      [new TypeError("Failed to fetch"), "Gagal membatalkan. Coba lagi."],
    ];
    for (const [error, text] of cases) {
      const outcome = await undoEntry(subject, async () => { throw error; });
      expect(undoText(testWords("id").t, undoMessage(outcome, subject))).toBe(text);
    }
    const success = await undoEntry(subject, async () => ok);
    expect(undoText(testWords("id").t, undoMessage(success, subject))).toBe("Dibatalkan. Kedai Awan kembali ke Perlu jawaban.");
  });
});
