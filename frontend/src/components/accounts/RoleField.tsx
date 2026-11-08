import { useTranslations } from "next-intl";
import type { AccountRole } from "./types";

/** The uses a cash, bank or e-wallet account can take; credit is always Utang and a product always Investasi (D1). */
export type CashRole = Extract<AccountRole, "harian" | "tabungan" | "dana_investasi">;

const CASH_ROLES: { value: CashRole; key: "harian" | "tabungan" | "dana" }[] = [
  { value: "harian", key: "harian" },
  { value: "tabungan", key: "tabungan" },
  { value: "dana_investasi", key: "dana" },
];

export function cashRoleOf(role: AccountRole | undefined): CashRole {
  return role === "tabungan" || role === "dana_investasi" ? role : "harian";
}

/** The role a type fixes, or null when the owner chooses it. */
export function fixedRole(type: string, isProduct: boolean): AccountRole | null {
  if (type === "credit") return "utang";
  if (type === "investment" || isProduct) return "investasi";
  return null;
}

/** "Kegunaan": a choice for cash accounts, a fixed line for credit and investment products (SPEC field). */
export function RoleField({
  id,
  value,
  onChange,
  fixed,
  error,
}: {
  id: string;
  value: CashRole;
  onChange: (role: CashRole) => void;
  fixed: AccountRole | null;
  error?: string;
}) {
  const t = useTranslations("accounts");
  if (fixed) {
    return (
      <div className="field">
        <span className="flabel">{t("form.role")}</span>
        <p className="t-body">{fixed === "utang" ? t("groups.utang") : t("groups.investasi")}</p>
        <p className="hint">{t("form.roleFixed")}</p>
      </div>
    );
  }
  const hintId = `${id}-hint`;
  const errorId = `${id}-error`;
  return (
    <div className="field">
      <label htmlFor={id}>{t("form.role")}</label>
      <select
        id={id}
        name="role"
        value={value}
        onChange={(event) => onChange(event.target.value as CashRole)}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? `${hintId} ${errorId}` : hintId}
        className="field-control"
      >
        {CASH_ROLES.map((role) => (
          <option key={role.value} value={role.value}>{t(`groups.${role.key}`)}</option>
        ))}
      </select>
      <p id={hintId} className="hint">{t("form.roleHint")}</p>
      {error && <p id={errorId} className="err">{error}</p>}
    </div>
  );
}
