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

function choiceId(account: AccountChoice): string | undefined {
  return account.id ?? account.account_id;
}

export function listLiquidAccountChoices<T extends AccountChoice>(accounts: T[]): T[] {
  const nested = accounts.some((account) => (account.children?.length ?? 0) > 0);
  if (!nested) return accounts.filter(isLiquidAccountChoice);
  // GET /api/accounts lists each pocket at the top level and again in its parent's `children`;
  // keep one entry per pocket, placed directly under its parent.
  const nestedIds = new Set(
    accounts.flatMap((account) => (account.children ?? []).map(choiceId)).filter(Boolean),
  );
  return accounts
    .filter((account) => !nestedIds.has(choiceId(account)))
    .flatMap((account) => [account, ...((account.children ?? []) as T[])])
    .filter(isLiquidAccountChoice);
}
