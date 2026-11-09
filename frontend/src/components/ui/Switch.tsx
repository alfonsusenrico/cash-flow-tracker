"use client";

/**
 * The prototype's `.switch` (option-6 styles.css, apply-markas-pengaturan D11): a 36 × 20 `role="switch"` button
 * named by its row title and announcing its state through `aria-checked`; Space and Enter toggle it.
 */
export function Switch({
  checked,
  onChange,
  labelledBy,
  describedBy,
}: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  labelledBy: string;
  describedBy?: string;
}) {
  return (
    <button
      type="button"
      role="switch"
      className="switch"
      aria-checked={checked}
      aria-labelledby={labelledBy}
      aria-describedby={describedBy}
      onClick={() => onChange(!checked)}
    />
  );
}
