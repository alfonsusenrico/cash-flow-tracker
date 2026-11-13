import React from "react";
import { isCreditAccountChoice, isLiquidAccountChoice, listCreditAccountChoices } from "@/lib/accountOptions";

export interface AccountOptionItem {
  id?: string;
  account_id?: string;
  name?: string;
  account_name?: string;
  type?: string;
  parent_id?: string | null;
  is_parent?: boolean;
  children?: AccountOptionItem[];
  balance?: number;
  [key: string]: any;
}

/** The words of the options; translated pages pass the catalogue's, the rest keep today's Indonesian. */
export interface AccountOptionLabels {
  creditGroup: string;
  parent: (name: string) => string;
  parentDefault: (name: string, pocket: string) => string;
  groupTotal: (name: string, amount: string) => string;
  owed: (amount: string) => string;
  noOther: string;
  noLiquid: string;
}

export const INDONESIAN_ACCOUNT_OPTION_LABELS: AccountOptionLabels = {
  creditGroup: "Kredit",
  parent: (name) => `${name} (Akun Induk)`,
  parentDefault: (name, pocket) => `${name} (Default: ${pocket})`,
  groupTotal: (name, amount) => `${name} — Total: ${amount}`,
  owed: (amount) => `utang ${amount}`,
  noOther: "Tidak ada rekening lain yang tersedia",
  noLiquid: "Tidak ada rekening likuid yang tersedia",
};

interface Props {
  accounts: AccountOptionItem[];
  formatBalance?: (n: number) => string;
  allowParentSelection?: boolean;
  excludeAccountId?: string;
  liquidOnly?: boolean;
  /**
   * Adds credit accounts in their own "Kredit" group. Without it no credit account is offered, so only
   * the pickers that may book on credit list them (add-credit-accounts D7).
   */
  includeCredit?: boolean;
  /** Lists only the credit accounts, as plain options: the picker under Catat's "Bayar nanti". */
  creditOnly?: boolean;
  labels?: AccountOptionLabels;
}

export function AccountSelectOptions({
  accounts,
  formatBalance,
  allowParentSelection = false,
  excludeAccountId,
  liquidOnly = false,
  includeCredit = false,
  creditOnly = false,
  labels = INDONESIAN_ACCOUNT_OPTION_LABELS,
}: Props) {
  const getId = (a: AccountOptionItem) => (a.id || a.account_id || "") as string;
  const getName = (a: AccountOptionItem) => (a.name || a.account_name || "") as string;
  const creditOption = (account: AccountOptionItem) => {
    const owed = creditOwed(account);
    return (
      <option key={getId(account)} value={getId(account)}>
        {getName(account)}
        {formatBalance && owed > 0 ? ` (${labels.owed(formatBalance(owed))})` : ""}
      </option>
    );
  };

  if (creditOnly) {
    return <>{listCreditAccountChoices(accounts).filter((account) => getId(account) !== excludeAccountId).map(creditOption)}</>;
  }

  // Top-level accounts have parent_id null or undefined; credit accounts only appear in the "Kredit" group.
  const topAccounts = accounts.filter((account) => !account.parent_id && !isCreditAccountChoice(account));
  const creditAccounts = includeCredit
    ? listCreditAccountChoices(accounts).filter((account) => getId(account) !== excludeAccountId)
    : [];
  const hasAvailableAccount = creditAccounts.length > 0 || accounts
    .flatMap((account) => [account, ...(account.children ?? [])])
    .some((account) => (
      getId(account)
      && getId(account) !== excludeAccountId
      && !isCreditAccountChoice(account)
      && (!liquidOnly || isLiquidAccountChoice(account))
    ));

  if (liquidOnly && !hasAvailableAccount) {
    return (
      <option value="">
        {excludeAccountId ? labels.noOther : labels.noLiquid}
      </option>
    );
  }

  return (
    <>
      {topAccounts.map((parent) => {
        const parentId = getId(parent);
        const parentName = getName(parent);
        const childrenByParent =
          parent.children && parent.children.length > 0
            ? parent.children
            : accounts.filter((a) => a.parent_id === parentId);
        const rawChildren = liquidOnly
          ? childrenByParent.filter(isLiquidAccountChoice)
          : childrenByParent;
        const children = excludeAccountId
          ? rawChildren.filter((c) => getId(c) !== excludeAccountId)
          : rawChildren;
        const hasChildren = rawChildren.length > 0;
        const canShowParent = allowParentSelection && parentId !== excludeAccountId && (!liquidOnly || isLiquidAccountChoice(parent));

        if (liquidOnly && !canShowParent && children.length === 0) return null;

        if (hasChildren) {
          if (!canShowParent && children.length === 0) {
            return null;
          }

          return (
            <optgroup
              key={parentId}
              label={formatBalance && parent.balance !== undefined ? labels.groupTotal(parentName, formatBalance(parent.balance)) : parentName}
            >
              {canShowParent && (
                <option value={parentId}>
                  {parent.default_pocket_name ? labels.parentDefault(parentName, parent.default_pocket_name) : labels.parent(parentName)}
                  {formatBalance && parent.balance !== undefined
                    ? ` (${formatBalance(parent.balance)})`
                    : ""}
                </option>
              )}
              {children.map((child) => {
                const childId = getId(child);
                const childName = getName(child);
                return (
                  <option key={childId} value={childId}>
                    {childName}
                    {formatBalance && child.balance !== undefined
                      ? ` (${formatBalance(child.balance)})`
                      : ""}
                  </option>
                );
              })}
            </optgroup>
          );
        }

        if (parentId === excludeAccountId) {
          return null;
        }

        return (
          <option key={parentId} value={parentId}>
            {parentName}
            {formatBalance && parent.balance !== undefined
              ? ` (${formatBalance(parent.balance)})`
              : ""}
          </option>
        );
      })}
      {creditAccounts.length > 0 && (
        <optgroup label={labels.creditGroup}>
          {creditAccounts.map(creditOption)}
        </optgroup>
      )}
    </>
  );
}

/** What a credit account owes as a positive amount; the API sends it, older rows fall back to the balance. */
function creditOwed(account: AccountOptionItem): number {
  if (typeof account.credit_owed === "number") return account.credit_owed;
  return Math.max(0, -(account.balance ?? 0));
}
