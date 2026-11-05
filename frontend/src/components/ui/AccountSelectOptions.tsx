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
}

export function AccountSelectOptions({
  accounts,
  formatBalance,
  allowParentSelection = false,
  excludeAccountId,
  liquidOnly = false,
  includeCredit = false,
}: Props) {
  const getId = (a: AccountOptionItem) => (a.id || a.account_id || "") as string;
  const getName = (a: AccountOptionItem) => (a.name || a.account_name || "") as string;

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
        {excludeAccountId
          ? "Tidak ada rekening lain yang tersedia"
          : "Tidak ada rekening likuid yang tersedia"}
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
              label={`${parentName}${
                formatBalance && parent.balance !== undefined
                  ? ` — Total: ${formatBalance(parent.balance)}`
                  : ""
              }`}
            >
              {canShowParent && (
                <option value={parentId}>
                  {parentName} {parent.default_pocket_name ? `(Default: ${parent.default_pocket_name})` : "(Akun Induk)"}
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
        <optgroup label="Kredit">
          {creditAccounts.map((account) => {
            const owed = creditOwed(account);
            return (
              <option key={getId(account)} value={getId(account)}>
                {getName(account)}
                {formatBalance && owed > 0 ? ` (utang ${formatBalance(owed)})` : ""}
              </option>
            );
          })}
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
