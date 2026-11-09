"use client";

import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { FigureBlock } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { api } from "@/lib/api";
import type { Format } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";

/** `GET /api/credit/accounts/{id}` (add-credit-spending-flow D4, H5): what one credit account owes and its period totals. */
export interface CreditOverview {
  ok: boolean;
  account: { id: string; name: string; owed: number };
  next_due: { due_date: string; amount: number; left_to_pay: number; status: string } | null;
  period: { from: string; to: string };
  totals: { spent_on_credit: number; paid_to_credit: number };
}

/**
 * The provider view's header (apply-markas-transaksi D30, H5 "Provider Filter"): the account's Utang, next bill,
 * "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" for the page's period, all from the credit overview.
 */
export function CreditHeader({
  accountId,
  from,
  to,
  money,
  format,
}: {
  accountId: string;
  /** The page's period as local days; both null on "Semua waktu" without a range, where the server's cycle is used. */
  from: string | null;
  to: string | null;
  money: (amount: number) => string;
  format: Format;
}) {
  const t = useTranslations("transactions.provider");
  const common = useTranslations("common");
  const row = useTranslations("transactions.row");
  const query = new URLSearchParams();
  if (from) query.set("from", from);
  if (to) query.set("to", to);
  const search = query.toString();
  const overview = useQuery<CreditOverview>({
    queryKey: queryKeys.transactions.creditOverview(accountId, search),
    queryFn: () => api.get(`/credit/accounts/${accountId}${search ? `?${search}` : ""}`),
  });
  const data = overview.data;
  const range = data ? `${format.shortDate(data.period.from)} – ${format.shortDate(data.period.to)}` : null;
  const figure = (value: number) => <MaskedMoney text={money(value)} />;

  return (
    <section className="tx-credit" aria-labelledby="tx-credit-heading" aria-busy={overview.isLoading || undefined}>
      <SectionHeader id="tx-credit-heading" label={data && range ? t("heading", { provider: data.account.name, range }) : t("label")} />
      {overview.isError ? (
        <p className="err t-meta" role="alert">
          {t("loadError")}
          <button type="button" className="ws-btn ws-btn-sm" onClick={() => overview.refetch()}>{common("tryAgain")}</button>
        </p>
      ) : (
        <div className="supports">
          <FigureBlock bar={false} label={t("owed")} figure={data ? figure(data.account.owed) : <span className="skel is-fig" aria-hidden="true" />} />
          <FigureBlock
            bar={false}
            label={t("nextBill")}
            figure={data
              ? data.next_due ? figure(data.next_due.left_to_pay) : <span className="is-words">{t("noBill")}</span>
              : <span className="skel is-fig" aria-hidden="true" />}
            caption={data?.next_due ? row("due", { date: format.shortDate(data.next_due.due_date) }) : undefined}
          />
          <FigureBlock bar={false} label={t("spentOnCredit")} figure={data ? figure(data.totals.spent_on_credit) : <span className="skel is-fig" aria-hidden="true" />} />
          <FigureBlock bar={false} label={t("paidToCredit")} figure={data ? figure(data.totals.paid_to_credit) : <span className="skel is-fig" aria-hidden="true" />} />
        </div>
      )}
    </section>
  );
}
