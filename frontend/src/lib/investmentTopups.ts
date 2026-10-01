export interface InvestmentAccount {
  id: string;
  name: string;
  type: string;
  parent_id?: string | null;
  default_funding_account_id?: string | null;
  instrument_type?: string | null;
  units?: number | null;
  balance?: number;
  cost_basis?: number | null;
  investment_tracking_mode?: string | null;
  investment_value_estimated?: boolean;
  is_archived?: boolean;
  is_parent?: boolean;
  children?: InvestmentAccount[];
}

export function listAmountInvestmentProducts<T extends InvestmentAccount>(accounts: T[]): T[] {
  const flattened = accounts.flatMap((account) => [account, ...((account.children ?? []) as T[])]);
  const unique = [...new Map(flattened.map((account) => [account.id, account])).values()];
  return unique.filter((account) =>
    account.instrument_type === "mutual_fund" && account.units == null && !account.is_archived &&
    !account.is_parent && !account.children?.length && !unique.some((child) => child.parent_id === account.id),
  );
}

export interface InvestmentTopup {
  id: string;
  source_account_id: string;
  target_account_id: string;
  amount: number;
  date: string;
  notes: string | null;
  is_deleted?: boolean;
}

export interface InvestmentScheduleDraft {
  sourceAccountId: string;
  targetAccountId: string;
  amount: number;
  productName: string;
}

const LIQUID_ACCOUNT_TYPES = new Set(["cash", "bank", "wallet", "ewallet"]);

/** A unit-tracked mutual-fund product that can switch, once, to amount tracking. */
export function canSwitchToAmountTracking(account: InvestmentAccount): boolean {
  return account.instrument_type === "mutual_fund" && account.units != null &&
    account.investment_tracking_mode !== "amount" && !account.is_archived &&
    !account.is_parent && !account.children?.length;
}

export interface ConvertibleExpense {
  type: string;
  account_id: string;
  movement_id?: string | null;
  goal_id?: string | null;
  obligation_id?: string | null;
  obligation_allocations?: unknown[];
  recurring_rule_id?: string | null;
}

/** A recorded standalone expense from a liquid account that can become the debit side of a top-up. */
export function canConvertExpenseToTopup(transaction: ConvertibleExpense, accounts: InvestmentAccount[]): boolean {
  if (transaction.type !== "expense" || transaction.movement_id || transaction.goal_id || transaction.obligation_id) return false;
  if (transaction.obligation_allocations?.length || transaction.recurring_rule_id) return false;
  const flattened = accounts.flatMap((account) => [account, ...(account.children ?? [])]);
  const source = flattened.find((account) => account.id === transaction.account_id);
  return Boolean(source && LIQUID_ACCOUNT_TYPES.has(source.type) && !source.instrument_type && !source.is_archived);
}
