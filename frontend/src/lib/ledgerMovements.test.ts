import { describe, expect, it } from "vitest";
import { consolidateLedgerMovements, type LedgerMovementRow } from "./ledgerMovements";

const outbound: LedgerMovementRow = {
  id: "out",
  account_id: "source",
  account_name: "Source",
  // Renamed by the owner; the key keeps it a movement leg.
  category_name: "Transfer",
  category_system_key: "internal_movement",
  type: "expense",
  amount: 500,
  date: "2026-09-20T10:00:00.000Z",
  notes: "Payment",
};
const inbound: LedgerMovementRow = {
  ...outbound,
  id: "in",
  account_id: "target",
  account_name: "Target",
  type: "income",
  date: "2026-09-20T10:00:29.000Z",
  notes: "Internal Movement",
};

describe("ledger movement display", () => {
  it("infers one unique legacy pair less than 30 seconds apart without linking it", () => {
    const [display] = consolidateLedgerMovements([inbound, outbound], true);

    expect(display).toMatchObject({
      id: "out",
      partner_id: "in",
      is_inferred_transfer: true,
      target_account_name: "Target",
    });
    expect(display.movement_id).toBeUndefined();
  });

  it("keeps exact 30-second and ambiguous candidates separate", () => {
    const atBoundary = { ...inbound, date: "2026-09-20T10:00:30.000Z" };
    expect(consolidateLedgerMovements([outbound, atBoundary], true)).toHaveLength(2);

    const anotherInbound = { ...inbound, id: "in-2", account_id: "another-target" };
    expect(consolidateLedgerMovements([outbound, inbound, anotherInbound], true)).toHaveLength(3);
  });

  it("does not infer from ordinary or already linked rows", () => {
    expect(consolidateLedgerMovements([outbound, { ...inbound, category_name: "Other", category_system_key: null }], true)).toHaveLength(2);
    // An ordinary category that happens to carry the seed name is not a movement leg.
    const sameName = { ...inbound, category_name: "Internal Movement", category_system_key: null };
    expect(consolidateLedgerMovements([outbound, sameName], true)).toHaveLength(2);
    expect(consolidateLedgerMovements([outbound, { ...inbound, movement_id: "stored" }], true)).toHaveLength(2);
  });

  it("uses the server partner for a paginated linked outbound row", () => {
    const [display] = consolidateLedgerMovements([{
      ...outbound,
      movement_id: "stored",
      movement_role: "outbound",
      partner_id: "in",
      transfer_target_account_id: "target",
      transfer_target_account_name: "Target",
    }], true);

    expect(display).toMatchObject({
      id: "out",
      is_consolidated_transfer: true,
      target_account_id: "target",
      target_account_name: "Target",
    });
  });

  it("keeps one linked row when both legs are returned by a search", () => {
    const linkedOutbound = {
      ...outbound,
      movement_id: "stored",
      movement_role: "outbound" as const,
      partner_id: "in",
    };
    const linkedInbound = {
      ...inbound,
      movement_id: "stored",
      movement_role: "inbound" as const,
      partner_id: "out",
    };

    expect(consolidateLedgerMovements([linkedInbound, linkedOutbound], true)).toMatchObject([
      { id: "out", is_consolidated_transfer: true, target_account_name: "Target" },
    ]);
  });

  it("carries the target's bank onto the merged row", () => {
    const linkedOut: LedgerMovementRow = {
      ...outbound, account_parent_name: "BCA", movement_id: "mv", movement_role: "outbound", partner_id: "in",
      transfer_target_account_name: "Tabungan", transfer_target_account_parent_name: "Jago",
    };
    const linkedIn: LedgerMovementRow = { ...inbound, account_parent_name: "Jago", movement_id: "mv", movement_role: "inbound", partner_id: "out" };
    expect(consolidateLedgerMovements([linkedIn, linkedOut], true)).toEqual([
      expect.objectContaining({ id: "out", target_account_name: "Tabungan", target_account_parent_name: "Jago" }),
    ]);

    const inferred = consolidateLedgerMovements([{ ...inbound, account_parent_name: "Jago" }, outbound], true);
    expect(inferred[0]).toMatchObject({ is_inferred_transfer: true, target_account_parent_name: "Jago" });
  });
});
