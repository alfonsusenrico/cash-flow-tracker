import { describe, expect, it } from "vitest";
import { ACTIVITY_LIMIT, groupByDay, recentActivity } from "./activity";
import type { ActivityRow } from "./types";

function spend(id: string, date: string, extra: Partial<ActivityRow> = {}): ActivityRow {
  return {
    id, account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago", category_name: "Makan", category_system_key: null,
    type: "expense", amount: 30_000, date, notes: null, merchant: "Kedai Awan", kakeibo_type: "need", review_status: "reviewed", ...extra,
  };
}

describe("Beranda's recent activity", () => {
  it("groups rows by their Asia/Jakarta day across midnight, keeping the order", () => {
    // 16.59 UTC is 23.59 in Jakarta, 17.01 UTC is 00.01 the next day.
    const rows = [spend("after", "2026-11-04T17:01:00Z"), spend("before", "2026-11-04T16:59:00Z"), spend("earlier", "2026-11-04T01:00:00Z")];
    expect(groupByDay(rows).map((group) => [group.day, group.rows.map((row) => row.id)])).toEqual([
      ["2026-11-05", ["after"]],
      ["2026-11-04", ["before", "earlier"]],
    ]);
  });

  it("leaves out rows waiting for an answer before keeping the newest eight", () => {
    const rows = [
      spend("waiting", "2026-11-05T05:00:00Z", { review_status: "needs_review" }),
      ...Array.from({ length: 9 }, (_, index) => spend(`r${index}`, `2026-11-05T0${4 - Math.min(index, 4)}:00:00Z`)),
    ];
    const kept = recentActivity(rows);
    expect(kept).toHaveLength(ACTIVITY_LIMIT);
    expect(kept.map((row) => row.id)).toEqual(["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"]);
  });
});
