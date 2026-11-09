"use client";

import { useTranslations } from "next-intl";

/**
 * The merge's selection bar (apply-markas-transaksi D9): "{n} dari 2 dipilih", "Batal gabungkan" and "Gabungkan jadi
 * pindah saldo", sticky under the top bar while selecting so the action stays in view (deviation 6).
 */
export function MergeBar({
  count,
  error,
  pending,
  onCancel,
  onMerge,
}: {
  count: number;
  error: string;
  pending: boolean;
  onCancel: () => void;
  onMerge: () => void;
}) {
  const t = useTranslations("transactions");
  return (
    <div className="sel-bar is-sticky" role="region" aria-label={t("merge.region")}>
      <span className="t-body grow tabular-nums" aria-live="polite">{t("merge.count", { selected: String(count) })}</span>
      <button type="button" className="ws-btn ws-btn-quiet" onClick={onCancel}>{t("page.mergeStop")}</button>
      <button type="button" className="ws-btn ws-btn-ink" disabled={count !== 2 || pending} onClick={onMerge}>
        {pending ? t("merge.saving") : t("merge.action")}
      </button>
      {error && <p role="alert" className="err">{error}</p>}
    </div>
  );
}
