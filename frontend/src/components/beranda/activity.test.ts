import { describe, expect, it } from "vitest";
import { UNPAIRED_LABEL, movementDirection, movementRoute, routeIncomplete, rowCategory, rowTitle } from "./activity";
import type { ActivityRow } from "./types";

// Synthetic accounts: "Main Pocket" and "Tabungan" are pockets of Jago; "ATM" is a pocket of BCA; Gopay has no parent.
function leg(extra: Partial<ActivityRow>): ActivityRow {
  return {
    id: "row", account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago", category_name: "Internal Movement",
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
  const spend = leg({ category_name: "Makan di Luar", merchant: "Kedai Awan", review_status: "reviewed" });

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
    expect(rowCategory({ ...spend, investment_topup_id: "topup", category_name: "Internal Movement" })).toBe("—");
    expect(rowCategory({ ...spend, category_name: null, review_status: "needs_review" })).toBe("—");
  });
});
