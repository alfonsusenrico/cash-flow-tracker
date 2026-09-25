const LIQUID_ACCOUNT_TYPES = new Set(["cash", "bank", "ewallet", "wallet"]);

export interface AccountChoice {
  id?: string;
  account_id?: string;
  parent_id?: string | null;
  type?: string;
  instrument_type?: string | null;
  balance?: number;
  children?: AccountChoice[];
}

export function isLiquidAccountChoice(account: AccountChoice): boolean {
  return (
    LIQUID_ACCOUNT_TYPES.has(account.type ?? "") &&
    account.type !== "investment" &&
    !account.instrument_type
  );
}

export function filterLiquidAccountChoices<T extends AccountChoice>(accounts: T[]): T[] {
  return accounts.flatMap((account) => {
    if (!isLiquidAccountChoice(account)) return [];
    const children = account.children
      ? filterLiquidAccountChoices(account.children as T[])
      : undefined;
    return [{ ...account, ...(children ? { children } : {}) }];
  });
}

export function listLiquidAccountChoices<T extends AccountChoice>(accounts: T[]): T[] {
  const nested = accounts.some((account) => (account.children?.length ?? 0) > 0);
  const candidates = nested
    ? accounts.flatMap((account) => [account, ...((account.children ?? []) as T[])])
    : accounts;
  return candidates.filter(isLiquidAccountChoice);
}
