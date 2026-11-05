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

/** A credit account (paylater, credit card): money is owed on it, so it is never liquid. */
export function isCreditAccountChoice(account: AccountChoice): boolean {
  return account.type === "credit";
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

/**
 * Credit accounts for the pickers that opt in to a "Kredit" group (add-credit-accounts D7). A credit
 * account is always top-level, so each one is listed once.
 */
export function listCreditAccountChoices<T extends AccountChoice>(accounts: T[]): T[] {
  const seen = new Set<string>();
  return accounts.filter((account) => {
    const id = choiceId(account);
    if (!isCreditAccountChoice(account) || account.parent_id || !id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}
