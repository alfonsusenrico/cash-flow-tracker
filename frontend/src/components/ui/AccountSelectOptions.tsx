import React from "react";

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
}

export function AccountSelectOptions({
  accounts,
  formatBalance,
  allowParentSelection = false,
  excludeAccountId,
}: Props) {
  const getId = (a: AccountOptionItem) => (a.id || a.account_id || "") as string;
  const getName = (a: AccountOptionItem) => (a.name || a.account_name || "") as string;

  // Top-level accounts have parent_id null or undefined
  const topAccounts = accounts.filter((a) => !a.parent_id);

  return (
    <>
      {topAccounts.map((parent) => {
        const parentId = getId(parent);
        const parentName = getName(parent);
        const rawChildren =
          parent.children && parent.children.length > 0
            ? parent.children
            : accounts.filter((a) => a.parent_id === parentId);
        const children = excludeAccountId
          ? rawChildren.filter((c) => getId(c) !== excludeAccountId)
          : rawChildren;
        const hasChildren = rawChildren.length > 0;
        const canShowParent = allowParentSelection && parentId !== excludeAccountId;

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
    </>
  );
}
