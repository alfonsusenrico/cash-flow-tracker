import { describe, expect, it } from "vitest";
import { consolidateLedgerMovements, type LedgerMovementRow } from "./ledgerMovements";

const outbound: LedgerMovementRow = {
  id: "out",
  account_id: "source",
  account_name: "Source",
  category_name: "Internal Movement",
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
    expect(consolidateLedgerMovements([outbound, { ...inbound, category_name: "Other" }], true)).toHaveLength(2);
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
});
