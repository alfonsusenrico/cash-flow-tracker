import type { AccountRole } from "./types";

/** The uses a cash, bank or e-wallet account can take; credit is always Utang and a product always Investasi (D1). */
export type CashRole = Extract<AccountRole, "harian" | "tabungan" | "dana_investasi">;

const CASH_ROLES: { value: CashRole; label: string }[] = [
  { value: "harian", label: "Uang harian" },
  { value: "tabungan", label: "Tabungan" },
  { value: "dana_investasi", label: "Dana investasi" },
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

const fieldClass = "w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm text-[var(--text)]";

/** "Kegunaan": a choice for cash accounts, a fixed line for credit and investment products. */
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
  if (fixed) {
    return (
      <div>
        <p className="font-medium text-[var(--muted)] block mb-1">Kegunaan</p>
        <p className="text-sm font-semibold text-[var(--text)]">{fixed === "utang" ? "Utang" : "Investasi"}</p>
        <p className="mt-1 text-xs text-[var(--muted)]">Ditentukan oleh jenis rekening.</p>
      </div>
    );
  }
  const hintId = `${id}-hint`;
  const errorId = `${id}-error`;
  return (
    <div>
      <label htmlFor={id} className="font-medium text-[var(--muted)] block mb-1">Kegunaan</label>
      <select
        id={id}
        name="role"
        value={value}
        onChange={(event) => onChange(event.target.value as CashRole)}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? `${hintId} ${errorId}` : hintId}
        className={fieldClass}
      >
        {CASH_ROLES.map((role) => (
          <option key={role.value} value={role.value}>{role.label}</option>
        ))}
      </select>
      <p id={hintId} className="mt-1 text-xs text-[var(--muted)]">
        Uang harian dan Tabungan masuk Saldo tersedia. Dana investasi adalah uang yang belum dibelikan, misalnya RDN.
      </p>
      {error && <p id={errorId} className="mt-1 text-xs text-out-fg">{error}</p>}
    </div>
  );
}
