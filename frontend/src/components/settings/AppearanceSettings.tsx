"use client";

import { THEME_CHOICES } from "@/lib/theme";
import { useAppCtx } from "@/components/layout/AppLayout";
import { useWorkspace } from "@/components/workspace/context";
import { useShortcuts } from "@/components/workspace/shortcuts";
import { SettingsCard } from "@/components/settings/SettingsCard";
import styles from "@/components/accounts/money.module.css";

/** Tampilan (D9): the same theme, shortcut and hidden-balance state the account menu and help dialog use. */
export function AppearanceSettings() {
  const { themeChoice, setThemeChoice } = useWorkspace();
  const { singleKeysEnabled, setSingleKeysEnabled } = useShortcuts();
  const { hideBalances, setHideBalances } = useAppCtx();
  return (
    <SettingsCard section="tampilan">
      <fieldset className={styles.themePick}>
        <legend>Tema</legend>
        {THEME_CHOICES.map((choice) => (
          <label key={choice.value}>
            <input type="radio" name="theme-choice" value={choice.value} checked={themeChoice === choice.value} onChange={() => setThemeChoice(choice.value)} />
            <span className={styles.swatch} data-theme-preview={choice.value} aria-hidden="true" />
            {choice.label}
          </label>
        ))}
      </fieldset>
      <SwitchRow
        id="setting-single-keys"
        label="Pintasan satu tombol"
        caption="J, K, E dan tombol lain di Tinjau dan Transaksi. ⌘K dan Esc tetap berfungsi."
        checked={singleKeysEnabled}
        onChange={setSingleKeysEnabled}
      />
      <SwitchRow
        id="setting-hide-balances"
        label="Sembunyikan saldo"
        caption="Angka uang diganti titik di semua halaman sampai diaktifkan lagi."
        checked={hideBalances}
        onChange={setHideBalances}
      />
    </SettingsCard>
  );
}

function SwitchRow({ id, label, caption, checked, onChange }: { id: string; label: string; caption: string; checked: boolean; onChange: (value: boolean) => void }) {
  return (
    <div className={styles.switchRow}>
      <div>
        <b id={`${id}-label`}>{label}</b>
        <span id={`${id}-caption`}>{caption}</span>
      </div>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-labelledby={`${id}-label`}
        aria-describedby={`${id}-caption`}
        className="ws-switch"
        onClick={() => onChange(!checked)}
      >
        <span className="ws-switch-track" aria-hidden="true" />
      </button>
    </div>
  );
}
