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
