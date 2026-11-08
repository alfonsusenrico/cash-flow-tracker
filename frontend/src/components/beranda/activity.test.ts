import { describe, expect, it } from "vitest";
import {
  ACTIVITY_LIMIT, UNPAIRED_LABEL, activityMeta, activityPhoneStatus, groupByDay, movementDirection, movementRoute, recentActivity, routeIncomplete,
  rowCategory, rowStatus, rowTime, rowTitle,
} from "./activity";
import type { ActivityRow } from "./types";

// Synthetic accounts: "Main Pocket" and "Tabungan" are pockets of Jago; "ATM" is a pocket of BCA; Gopay has no parent.
// The owner renamed the movement category to "Transfer": the key, not the name, makes a row a movement.
function leg(extra: Partial<ActivityRow>): ActivityRow {
  return {
    id: "row", account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago", category_name: "Transfer",
    category_system_key: "internal_movement",
    type: "expense", amount: 500_000, date: "2026-11-04T01:39:00+07:00", notes: null, ...extra,
  };
}

const outboundLeg = leg({
  movement_id: "mv", movement_role: "outbound", account_name: "ATM", account_parent_name: "BCA",
  transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
});
const inboundLeg = leg({
  type: "income", movement_id: "mv", movement_role: "inbound", account_name: "Tabungan", account_parent_name: "Jago",
  transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
});

describe("movementRoute", () => {
  it("reads a merged row from its own account to the target", () => {
    const merged = { ...outboundLeg, is_consolidated_transfer: true, target_account_name: "Gopay", target_account_parent_name: null };
    expect(movementRoute(merged)).toEqual({ source: "BCA · ATM", target: "Gopay" });
    expect(rowTitle(merged)).toBe("BCA · ATM → Gopay");
  });

  it("reads an inferred pair the same way, with the target's bank", () => {
    const inferred = leg({ is_inferred_transfer: true, target_account_name: "Tabungan", target_account_parent_name: "Jago" });
    expect(rowTitle(inferred)).toBe("Jago · Main Pocket → Jago · Tabungan");
  });

  it("reads an outbound leg from its account to its partner", () => {
    expect(rowTitle(outboundLeg)).toBe("BCA · ATM → Gopay");
  });

  it("reads an inbound leg from its partner to its account, not backwards", () => {
    expect(movementRoute(inboundLeg)).toEqual({ source: "Gopay", target: "Jago · Tabungan" });
    expect(rowTitle(inboundLeg)).toBe("Gopay → Jago · Tabungan");
  });

  it("leaves the unknown side of an unpaired leg open, by its type", () => {
    const income = leg({ type: "income", notes: "Pindah uang dari BCA" });
    expect(movementRoute(income)).toEqual({ source: null, target: "Jago · Main Pocket" });
    expect(rowTitle(income)).toBe("? → Jago · Main Pocket");
    expect(routeIncomplete(income)).toBe(true);

    const expense = leg({ account_name: "ShopeePay", account_parent_name: null });
    expect(rowTitle(expense)).toBe("ShopeePay → ?");
  });

  it("names an account without a parent by its own name", () => {
    const plain = leg({ ...outboundLeg, account_name: "Cash", account_parent_name: null });
    expect(rowTitle(plain)).toBe("Cash → Gopay");
    expect(routeIncomplete(plain)).toBe(false);
  });

  it("keeps a row from an older API without parent fields readable", () => {
    const old = leg({ movement_id: "mv", movement_role: "inbound", type: "income", account_parent_name: undefined, transfer_target_account_name: "Gopay" });
    expect(rowTitle(old)).toBe("Gopay → Main Pocket");
  });
});

describe("movementDirection", () => {
  it("says where the money came from on the receiving account", () => {
    expect(movementDirection(inboundLeg)).toBe("Masuk dari Gopay");
  });

  it("says where the money went on the sending account", () => {
    expect(movementDirection(outboundLeg)).toBe("Keluar ke Gopay");
    const toPocket = leg({ movement_id: "mv", movement_role: "outbound", transfer_target_account_name: "Dana Darurat", transfer_target_account_parent_name: "Jago" });
    expect(movementDirection(toPocket)).toBe("Keluar ke Jago · Dana Darurat");
  });

  it("says the partner is missing when nobody paired the leg", () => {
    expect(movementDirection(leg({ type: "income" }))).toBe(UNPAIRED_LABEL);
  });
});

describe("rowCategory", () => {
  const spend = leg({ category_name: "Makan di Luar", category_system_key: null, merchant: "Kedai Awan", review_status: "reviewed" });

  it("names the owner's category for an expense and an income", () => {
    expect(rowCategory(spend)).toBe("Makan di Luar");
    expect(rowCategory({ ...spend, type: "income", category_name: "Pendapatan Lain" })).toBe("Pendapatan Lain");
  });

  it("says when an expense or income has no category", () => {
    expect(rowCategory({ ...spend, category_name: null })).toBe("Tanpa kategori");
    expect(rowCategory({ ...spend, type: "income", category_name: null })).toBe("Tanpa kategori");
  });

  it("shows a dash for movements, top-ups and waiting rows without a category", () => {
    expect(rowCategory(outboundLeg)).toBe("—");
    expect(rowCategory(leg({}))).toBe("—");
    expect(rowCategory({ ...spend, investment_topup_id: "topup", category_name: "Transfer", category_system_key: "internal_movement" })).toBe("—");
    expect(rowCategory({ ...spend, category_name: null, review_status: "needs_review" })).toBe("—");
  });
});

describe("Beranda's recent activity", () => {
  const spend = (id: string, date: string, extra: Partial<ActivityRow> = {}) =>
    leg({ id, date, category_name: "Makan", category_system_key: null, merchant: "Kedai Awan", kakeibo_type: "need", review_status: "reviewed", ...extra });

  it("groups rows by their Asia/Jakarta day across midnight, keeping the order", () => {
    // 16.59 UTC is 23.59 in Jakarta, 17.01 UTC is 00.01 the next day.
    const rows = [spend("after", "2026-11-04T17:01:00Z"), spend("before", "2026-11-04T16:59:00Z"), spend("earlier", "2026-11-04T01:00:00Z")];
    expect(groupByDay(rows).map((group) => [group.day, group.rows.map((row) => row.id)])).toEqual([
      ["2026-11-05", ["after"]],
      ["2026-11-04", ["before", "earlier"]],
    ]);
    expect(rowTime(rows[0])).toBe("00.01");
    expect(rowTime(rows[1])).toBe("23.59");
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

  it("words the meta and the status of each kind", () => {
    const expense = spend("e", "2026-11-05T05:00:00Z");
    expect(activityMeta(expense)).toEqual({ text: "Jago · Main Pocket" });
    expect(rowStatus(expense).text).toBe("Kebutuhan");
    expect(activityPhoneStatus(expense)).toEqual({ text: "Kebutuhan" });

    const income = leg({ type: "income", category_name: "Gaji", category_system_key: null, account_name: "Utama", account_parent_name: "BCA", review_status: "reviewed" });
    expect(activityMeta(income)).toEqual({ text: "BCA · Utama" });
    expect(rowStatus(income).text).toBe("Uang masuk");
    expect(activityPhoneStatus(income)).toEqual({ text: "Gaji" });

    const movement = { ...outboundLeg, notes: "Isi GoPay" };
    expect(activityMeta(movement)).toEqual({ text: "Isi GoPay" });
    expect(rowStatus(movement).text).toBe("Tidak dihitung");
    expect(activityPhoneStatus(movement)).toEqual({ text: "Pindah saldo" });

    const unpaired = leg({ type: "income", notes: "Pindah uang dari BCA" });
    expect(activityMeta(unpaired)).toEqual({ text: UNPAIRED_LABEL, warning: true });
    expect(activityPhoneStatus(unpaired)).toEqual({ text: UNPAIRED_LABEL, warning: true });

    const topup = leg({ investment_topup_id: "t1", account_name: "RDN", account_parent_name: "Stockbit", notes: "Tambah reksa dana" });
    expect(rowTitle(topup)).toBe("Top up Investasi");
    expect(activityMeta(topup)).toEqual({ text: "Stockbit · RDN" });
    expect(rowStatus(topup)).toMatchObject({ text: "Tabungan", bucket: "saving", color: "var(--k-saving)" });
    expect(activityPhoneStatus(topup)).toEqual({ text: "Tabungan" });
  });
});
