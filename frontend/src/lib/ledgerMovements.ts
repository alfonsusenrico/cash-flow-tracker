export interface LedgerMovementRow {
  id: string;
  account_id: string;
  account_name: string;
  category_name: string | null;
  type: "expense" | "income";
  amount: number;
  date: string;
  notes: string | null;
  movement_id?: string | null;
  movement_role?: "outbound" | "inbound" | null;
  partner_id?: string | null;
  transfer_target_account_id?: string | null;
  transfer_target_account_name?: string | null;
  target_account_id?: string;
  target_account_name?: string;
  is_consolidated_transfer?: boolean;
  is_inferred_transfer?: boolean;
}

function legacyCandidates<T extends LedgerMovementRow>(row: T, items: T[]): T[] {
  if (row.movement_id || row.category_name !== "Internal Movement") return [];

  const timestamp = new Date(row.date).getTime();
  if (!Number.isFinite(timestamp)) return [];

  return items.filter((candidate) => {
    if (candidate.id === row.id || candidate.movement_id) return false;
    if (candidate.category_name !== "Internal Movement") return false;
    if (candidate.type === row.type || candidate.amount !== row.amount || row.amount <= 0) return false;
    if (candidate.account_id === row.account_id) return false;

    const otherTimestamp = new Date(candidate.date).getTime();
    return Number.isFinite(otherTimestamp) && Math.abs(timestamp - otherTimestamp) < 30_000;
  });
}

export function consolidateLedgerMovements<T extends LedgerMovementRow>(
  items: T[],
  enabled: boolean,
): T[] {
  if (!enabled) return items;

  const result: T[] = [];
  const consumed = new Set<string>();
  const byId = new Map(items.map((item) => [item.id, item]));

  for (const row of items) {
    if (consumed.has(row.id)) continue;

    if (row.movement_id) {
      if (row.movement_role === "inbound" && row.partner_id && byId.has(row.partner_id)) {
        consumed.add(row.id);
        continue;
      }
      if (row.movement_role === "outbound" && row.partner_id) {
        const partner = byId.get(row.partner_id);
        if (partner) consumed.add(partner.id);
        result.push({
          ...row,
          is_consolidated_transfer: true,
          target_account_id: row.transfer_target_account_id ?? partner?.account_id,
          target_account_name: row.transfer_target_account_name ?? partner?.account_name,
        });
      } else {
        result.push(row);
      }
      continue;
    }

    const candidates = legacyCandidates(row, items);
    if (candidates.length !== 1 || legacyCandidates(candidates[0], items).length !== 1) {
      result.push(row);
      continue;
    }

    const partner = candidates[0];
    consumed.add(row.id);
    consumed.add(partner.id);
    const outgoing = row.type === "expense" ? row : partner;
    const incoming = row.type === "income" ? row : partner;
    result.push({
      ...outgoing,
      partner_id: incoming.id,
      is_inferred_transfer: true,
      target_account_id: incoming.account_id,
      target_account_name: incoming.account_name,
    });
  }

  return result;
}
