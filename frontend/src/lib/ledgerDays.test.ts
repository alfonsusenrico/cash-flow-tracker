import { describe, expect, it } from "vitest";
import { groupByDay, localDay, uniqueRows, type DailyTotal } from "./ledgerDays";

const total = (date: string, count: number): DailyTotal => ({ date, count, inflow: 0, outflow: 1000 * count, spending: 1000 * count, movements: 0, waiting_count: 0, waiting_amount: 0 });

describe("Transaksi day groups", () => {
  it("groups by the server's local day and never sums rows on the client", () => {
    const rows = [
      { id: "a", date: "2026-11-02T16:30:00Z", local_date: "2026-11-02" },
      { id: "b", date: "2026-11-02T01:00:00Z", local_date: "2026-11-02" },
      { id: "c", date: "2026-11-01T10:00:00Z", local_date: "2026-11-01" },
    ];
    const groups = groupByDay(rows, rows, [total("2026-11-02", 2), total("2026-11-01", 4)]);
    expect(groups.map((group) => [group.date, group.rows.length, group.totals?.outflow, group.remaining])).toEqual([
      ["2026-11-02", 2, 2000, 0],
      ["2026-11-01", 1, 4000, 3],
    ]);
  });

  it("leaves the totals out when the server sent none (an older API)", () => {
    const rows = [{ id: "a", date: "2026-11-02T01:00:00Z" }];
    expect(groupByDay(rows, rows, undefined)).toEqual([{ date: "2026-11-02", rows, totals: null, remaining: 0 }]);
  });

  it("counts paired legs behind one shown movement as loaded", () => {
    const legs = [{ id: "out", date: "2026-11-02T01:00:00Z", local_date: "2026-11-02" }, { id: "in", date: "2026-11-02T01:00:00Z", local_date: "2026-11-02" }];
    expect(groupByDay([legs[0]], legs, [total("2026-11-02", 2)])[0].remaining).toBe(0);
  });

  it("falls back to the Jakarta day and drops repeats across pages", () => {
    expect(localDay({ id: "a", date: "2026-11-02T17:30:00Z" })).toBe("2026-11-03");
    expect(uniqueRows([[{ id: "a" }, { id: "b" }], [{ id: "b" }, { id: "c" }]]).map((item) => item.id)).toEqual(["a", "b", "c"]);
  });
});
