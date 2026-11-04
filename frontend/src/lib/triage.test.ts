import { describe, expect, it } from "vitest";
import type { SuggestionList } from "@/lib/historySuggestions";
import type { ReviewGroup, ReviewItem, ReviewQueue, SeenTwiceItem } from "@/lib/review";
import {
  buildTriage,
  folderFromParams,
  folderItems,
  nextAfter,
  resolveDeepLink,
  triageSearch,
} from "./triage";

const row = (id: string, status: ReviewItem["review_status"] = "needs_review", overrides: Partial<ReviewItem> = {}): ReviewItem => ({
  id, type: "expense", amount: 25_000, date: "2026-11-02T05:00:00Z", account_id: "acc-dompet", account_name: "Dompet Contoh",
  category_id: "cat-makan", category_name: "Makan di Luar", kakeibo_type: "want", merchant: "Kedai Awan", notes: "Kedai Awan",
  review_status: status, source: "notification", classified_by: "ai", movement_id: null, movement_role: null,
  fee_for_movement_id: null, reasons: [], ...overrides,
});

const group = (key: string, rows: ReviewItem[]): ReviewGroup => ({
  key, kind: "merchant", type: "expense", label: rows[0].merchant ?? key, count: rows.length,
  total: rows.reduce((sum, item) => sum + item.amount, 0),
  needs_review_count: rows.filter((item) => item.review_status === "needs_review").length,
  auto_count: rows.filter((item) => item.review_status === "auto").length,
  first_date: rows[rows.length - 1].date, last_date: rows[0].date, reasons: [], category: null, transactions: rows,
});

/** One open question, two needs-review rows in one group, one awaiting send and one ambiguous credit. */
function queue(): ReviewQueue {
  return {
    ok: true,
    truncated: false,
    counts: { attention: 5, unreviewed: 2, questions: 1, needs_review: 2, auto: 2, groups: 2, awaiting: 1, ambiguous: 1 },
    questions: [{
      event_id: "ev-1", post_time: "2026-11-02T09:00:00Z",
      question: { question_id: "q-1", prompt: "Apa ini? Rp45.000 dari Dompet Contoh", suggestions: [] },
      transaction: { id: "tx-q", type: "expense", amount: 45_000, date: "2026-11-02T09:00:00Z", account_name: "Dompet Contoh", notes: null, merchant: null, category_name: null },
    }],
    groups: [
      group("expense|merchant:kedai awan", [row("tx-k1"), row("tx-k2")]),
      group("expense|merchant:kopi pagi", [row("tx-a1", "auto", { merchant: "Kopi Pagi" }), row("tx-a2", "auto", { merchant: "Kopi Pagi" })]),
    ],
    awaiting: [{ ...row("tx-send", "needs_review", { category_name: "Internal Movement", reasons: ["awaiting_counterpart"] }), topup_suggestion: null }],
    ambiguous: [{ ...row("tx-credit", "needs_review", { type: "income", reasons: ["ambiguous_movement"] }), candidates: [] }],
  };
}

const suggestions: SuggestionList = {
  ok: true, truncated: false, pending: 3,
  groups: [{
    key: "expense|kafe", type: "expense", label: "Kafe Senja Contoh", merchant: "Kafe Senja Contoh",
    category: { id: "cat-jajan", name: "Jajan & Nongkrong" }, kakeibo: "want", offset_category_names: [], sources: ["rule"],
    before: [{ label: "Kafe Senja Contoh", category_name: "Kopi & Jajan", count: 2 }], count: 2, total: 77_000,
    first_date: "2026-10-30T09:00:00Z", last_date: "2026-10-31T09:00:00Z",
    suggestions: ["tx-c1", "tx-c2"].map((tx, index) => ({
      id: `sg-${index}`, transaction_id: tx, type: "expense", amount: 35_000, date: "2026-10-31T09:00:00Z", account_name: "Dompet Contoh",
      source: "rule", confidence: null, reason: "Kafe", before: { merchant: null, notes: null, category_id: null, category_name: null, kakeibo: null },
      after: { merchant: null, notes: null, category_id: null, category_name: null, kakeibo: null, offset_category_id: null, offset_category_name: null },
    })),
  }],
};

describe("triage folders", () => {
  it("Counts match the badge", () => {
    const triage = buildTriage(queue(), suggestions);
    expect([triage.counts.apa, triage.counts.jawab, triage.counts.pindah]).toEqual([1, 2, 2]);
    expect(triage.counts.apa + triage.counts.jawab + triage.counts.pindah).toBe(triage.attention);
    expect(folderItems(triage, "semua").map((item) => item.id)).toEqual([
      "q:ev-1", "g:expense|merchant:kedai awan", "a:tx-send", "m:tx-credit",
    ]);
    // Suggestions are folded and optional: not in "Semua" and not in the badge.
    expect(triage.counts.saran).toBe(3);
    expect(triage.counts.otomatis).toBe(2);
  });

  it("explains automatic rows inside a mixed group", () => {
    const data = queue();
    data.groups[0] = group("expense|merchant:kedai awan", [row("tx-k1"), row("tx-k2"), row("tx-k3", "auto"), row("tx-k4", "auto")]);
    expect(buildTriage(data).autoInMixedGroups).toBe(2);
  });

  it("keeps the truncation flag and counts only the rows received", () => {
    const data = { ...queue(), truncated: true };
    const triage = buildTriage(data);
    expect(triage.truncated).toBe(true);
    expect(triage.counts.jawab).toBe(2);
  });
});

describe("next item", () => {
  const before = ["a", "b", "c"];
  it("takes the following item, else the previous one", () => {
    expect(nextAfter(before, "a", new Set(["b", "c"]))).toBe("b");
    expect(nextAfter(before, "b", new Set(["a", "c"]))).toBe("c");
    expect(nextAfter(before, "c", new Set(["a", "b"]))).toBe("b");
  });
  it("is null when the folder emptied", () => {
    expect(nextAfter(before, "b", new Set())).toBeNull();
  });
});

describe("deep links", () => {
  const triage = buildTriage(queue(), suggestions);
  it("resolves an id in its folder from the legacy hash or the folder parameter", () => {
    expect(resolveDeepLink(triage, "tx-send", folderFromParams(null, "#review-awaiting"))).toEqual({ folder: "pindah", itemId: "a:tx-send", resolved: false });
    expect(resolveDeepLink(triage, "tx-credit", folderFromParams(null, "#review-ambiguous"))).toEqual({ folder: "pindah", itemId: "m:tx-credit", resolved: false });
    expect(resolveDeepLink(triage, "tx-q", folderFromParams(null, "#review-questions")).itemId).toBe("q:ev-1");
    expect(resolveDeepLink(triage, "tx-k2", folderFromParams(null, "#review-groups")).itemId).toBe("g:expense|merchant:kedai awan");
    expect(resolveDeepLink(triage, "tx-a1", folderFromParams("otomatis", "#review-groups")).itemId).toBe("g:expense|merchant:kopi pagi");
    expect(resolveDeepLink(triage, "tx-c2", folderFromParams("saran", null))).toEqual({ folder: "saran", itemId: "s:expense|kafe", resolved: false });
  });
  it("finds an id without a folder and says when it is already resolved", () => {
    expect(resolveDeepLink(triage, "tx-k1", null)).toEqual({ folder: "semua", itemId: "g:expense|merchant:kedai awan", resolved: false });
    expect(resolveDeepLink(triage, "tx-gone", folderFromParams(null, "#review-questions"))).toEqual({ folder: "apa", itemId: null, resolved: true });
    const empty = buildTriage({ ...queue(), questions: [] });
    expect(resolveDeepLink(empty, "tx-q", "apa")).toEqual({ folder: "semua", itemId: null, resolved: true });
  });
  it("ignores unknown folder values and writes the current place", () => {
    expect(folderFromParams("bogus", null)).toBeNull();
    expect(triageSearch("pindah", folderItems(triage, "pindah")[0])).toBe("?item=tx-send&folder=pindah");
    expect(triageSearch("semua", null)).toBe("");
  });
});

const seenTwice = (overrides: Partial<SeenTwiceItem> = {}): SeenTwiceItem => ({
  event_id: "ev-jago", kind: "duplicate", app: "Bank Jago", type: "income", amount: 500_000,
  date: "2026-10-31T11:39:18Z", account_name: "Bank Jago · Kantong Utama",
  kept: {
    transaction_id: "tx-gopay", type: "income", amount: 500_000, date: "2026-10-31T11:39:18Z", account_name: "GoPay",
    movement: { source: "BCA Harian", target: "GoPay" },
  },
  ...overrides,
});

describe("Terlihat dua kali", () => {
  it("is an optional folder outside the badge, linked to the kept leg", () => {
    const data = { ...queue(), seen_twice: [seenTwice(), seenTwice({ event_id: "ev-gone", kept: null })] };
    const triage = buildTriage(data);
    expect(triage.counts.dua).toBe(2);
    expect(triage.counts.apa + triage.counts.jawab + triage.counts.pindah).toBe(triage.attention);
    expect(folderItems(triage, "semua").some((item) => item.kind === "seen_twice")).toBe(false);
    expect(triage.folders.dua.map((item) => [item.id, item.transactionIds])).toEqual([
      ["d:ev-jago", ["tx-gopay"]], ["d:ev-gone", []],
    ]);
    expect(resolveDeepLink(triage, "tx-gopay", "dua")).toEqual({ folder: "dua", itemId: "d:ev-jago", resolved: false });
    expect(folderFromParams("dua", null)).toBe("dua");
  });

  it("reads an older queue without the section", () => {
    expect(buildTriage(queue()).folders.dua).toEqual([]);
  });
});

describe("Dilewati otomatis", () => {
  const skipped = seenTwice({
    event_id: "ev-skip", kind: "skipped",
    pattern: { id: "pattern-1", redundant_label: "Bank Jago", direction: "income", kept_label: "GoPay" },
  });

  it("is its own optional folder, never counted as work", () => {
    const triage = buildTriage({ ...queue(), skipped: [skipped] });
    expect(triage.counts.lewat).toBe(1);
    expect(triage.folders.lewat.map((item) => [item.id, item.folder, item.transactionIds])).toEqual([
      ["l:ev-skip", "lewat", ["tx-gopay"]],
    ]);
    expect(folderItems(triage, "semua").some((item) => item.folder === "lewat")).toBe(false);
    expect(triage.counts.apa + triage.counts.jawab + triage.counts.pindah).toBe(triage.attention);
    expect(folderFromParams("lewat", null)).toBe("lewat");
  });

  it("reads an older queue without skipped reports", () => {
    expect(buildTriage(queue()).folders.lewat).toEqual([]);
  });
});
