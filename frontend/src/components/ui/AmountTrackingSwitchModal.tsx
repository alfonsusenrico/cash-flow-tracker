"use client";

import { useEffect, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { MaskedMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { UntranslatedRegion } from "@/components/workspace/untranslated";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { InvestmentAccount } from "@/lib/investmentTopups";
import { queryKeys } from "@/lib/queryKeys";

interface Props {
  product: InvestmentAccount | null;
  onClose: () => void;
}

/**
 * "Ubah ke pelacakan nominal" (apply-markas-rekening D10): a 520 px shell dialog naming the value and cost that carry
 * over and that the change cannot be undone. The request is unchanged.
 */
export function AmountTrackingSwitchModal({ product, onClose }: Props) {
  const qc = useQueryClient();
  const t = useTranslations("accounts.amountTracking");
  const common = useTranslations("common");
  const format = useFormat();
  const [error, setError] = useState("");
  useEffect(() => setError(""), [product?.id]);
  const mutation = useMutation({
    mutationFn: (productId: string) => api.post(`/accounts/${productId}/amount-tracking`),
    onSuccess: () => {
      [queryKeys.accounts, queryKeys.dashboard.all, queryKeys.pulse, queryKeys.insights]
        .forEach((key) => qc.invalidateQueries({ queryKey: key }));
      onClose();
    },
    onError: (failure: Error) => setError(failure.message || t("failed")),
  });
  if (!product) return null;
  const busy = mutation.isPending;

  const footer = (
    <>
      <button type="button" className="ws-btn" onClick={onClose} disabled={busy}>{common("cancel")}</button>
      <button type="button" className="ws-btn ws-btn-ink" onClick={() => { setError(""); mutation.mutate(product.id); }} disabled={busy}>
        {busy ? t("saving") : t("confirm")}
      </button>
    </>
  );

  return (
    <UntranslatedRegion active={false}>
      <ShellDialog open onClose={() => { if (!busy) onClose(); }} title={t("title")} meta={product.name} width={520} closeLabel={common("close")} footer={footer}>
        <dl className="dl">
          <dt>{t("value")}</dt>
          <dd className="tabular-nums"><MaskedMoney text={format.money(product.balance ?? 0)} /></dd>
          <dt>{t("cost")}</dt>
          <dd className="tabular-nums">{product.cost_basis == null ? t("costUnknown") : <MaskedMoney text={format.money(product.cost_basis)} />}</dd>
        </dl>
        <p className="t-body">{t("body")}</p>
        <p className="t-body t-strong">{t("note")}</p>
        {error && <p role="alert" className="form-alert">{error}</p>}
      </ShellDialog>
    </UntranslatedRegion>
  );
}
