"use client";

import { useId } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import type { CreditOverview } from "@/components/accounts/CreditBills";
import type { AccountItem } from "@/components/accounts/types";
import { KindAmount, RowAmount, RowStatus, jakartaDay, rowMeta, rowPhoneLine, rowTitle, useRowLabels, type RowLine } from "@/components/transactions/rows";
import { ListRow } from "@/components/workspace/lists";
import { ErrorState } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import type { ActivityRow } from "./types";

/** What a credit account owes now: its `credit_owed`, else what its negative balance says. */
export function creditOwed(account: Pick<AccountItem, "balance" | "credit_owed">): number {
  if (typeof account.credit_owed === "number") return account.credit_owed;
  return Math.max(0, -account.balance);
}

/** The active top-level credit accounts, most owed first, then by name (open-beranda-figures D5). */
export function creditAccountsOf(accounts: AccountItem[]): AccountItem[] {
  return accounts
    .filter((account) => account.type === "credit" && !account.is_archived && !account.parent_id)
    .sort((a, b) => creditOwed(b) - creditOwed(a) || a.name.localeCompare(b.name));
}

export interface CreditFigure {
  owed: number;
  /** The nearest bill still asking for money across the accounts, or null. */
  nearest: { due_date: string; left_to_pay: number } | null;
  /** Something is owed but no account has a due day, so no bill can be named. */
  noDueDay: boolean;
}

/** Beranda's "Tagihan kredit" (D3): the sum owed and the nearest unpaid bill; every amount is the server's. */
export function creditFigure(accounts: AccountItem[]): CreditFigure {
  const owed = accounts.reduce((sum, account) => sum + creditOwed(account), 0);
  const nearest = accounts
    .map((account) => account.next_due)
    .filter((due): due is NonNullable<AccountItem["next_due"]> => Boolean(due && due.left_to_pay > 0))
    .sort((a, b) => a.due_date.localeCompare(b.due_date))[0];
  return {
    owed,
    nearest: nearest ? { due_date: nearest.due_date, left_to_pay: nearest.left_to_pay } : null,
    noDueDay: owed > 0 && !nearest,
  };
}

/** The first local day of the oldest item still unpaid, or null when nothing is left to pay (D5). */
export function oldestUnpaidDay(overview: Pick<CreditOverview, "bills">): string | null {
  const days = overview.bills.flatMap((bill) => bill.items.filter((item) => item.left_to_pay > 0).map((item) => item.date));
  return days.sort()[0] ?? null;
}

function Line({ text, warning }: RowLine) {
  return warning ? <span className="c-warning">{text}</span> : <>{text}</>;
}

const ROWS_LIMIT = 200;

/** The account's list from the start of that Jakarta day, or its latest rows without one. */
function rowsPath(accountId: string, day: string | null): string {
  const params = new URLSearchParams({ account_id: accountId, limit: String(ROWS_LIMIT) });
  if (day) params.set("from_date", `${day}T00:00:00+07:00`);
  return `/transactions?${params.toString()}`;
}

function CreditAccountSection({
  account,
  money,
  onSelect,
}: {
  account: AccountItem;
  money: (amount: number) => string;
  onSelect: (row: ActivityRow) => void;
}) {
  const t = useTranslations("home.creditBills");
  const format = useFormat();
  const labels = useRowLabels();
  const headingId = useId();
  const owed = creditOwed(account);
  const overviewQuery = useQuery<CreditOverview>({
    queryKey: queryKeys.transactions.creditOverview(account.id, ""),
    queryFn: () => api.get(`/credit/accounts/${account.id}`),
  });
  const overview = overviewQuery.data;
  // Without due days there are no bills to date from, so the list shows the account's latest rows instead.
  const start = overview ? oldestUnpaidDay(overview) : null;
  const wantsRows = Boolean(overview) && (start !== null || (owed > 0 && overview?.due_dates_unavailable === true));
  const rowsQuery = useQuery<{ transactions: ActivityRow[] }>({
    queryKey: [...queryKeys.transactions.all, "credit-running", account.id, start ?? ""],
    queryFn: () => api.get(rowsPath(account.id, start)),
    enabled: wantsRows,
  });
  const rows = [...(rowsQuery.data?.transactions ?? [])].sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));
  // Opening debt has no transaction of its own, so it is listed from its bill item (an "opening" item).
  const opening = (overview?.bills ?? []).flatMap((bill) => bill.items).filter((item) => item.kind === "opening" && item.left_to_pay > 0);
  const failed = overviewQuery.isError || rowsQuery.isError;
  const loading = !overview || (wantsRows && !rowsQuery.data);
  // Opening debt and the account's rows in one list by Jakarta day, oldest first; opening debt leads its day.
  const entries = [
    ...opening.map((item) => ({
      day: item.date,
      order: 0,
      node: (
        <li key={`opening-${item.date}-${item.due_date}`}>
          <ListRow
            time={format.shortDate(item.date)}
            timeIsDate
            title={t("opening")}
            meta={t("openingDue", { date: format.shortDate(item.due_date) })}
            phoneMeta={t("openingDue", { date: format.shortDate(item.due_date) })}
            amount={<KindAmount kind="credit" amount={item.left_to_pay} money={money} />}
          />
        </li>
      ),
    })),
    ...rows.map((row) => {
      const meta = rowMeta(row, labels, format, true);
      const phone = rowPhoneLine(row, labels, format, true);
      const day = format.shortDateAt(row.date);
      return {
        day: jakartaDay(row.date),
        order: 1,
        node: (
          <li key={row.id}>
            <ListRow
              onClick={() => onSelect(row)}
              time={day}
              timeIsDate
              title={rowTitle(row, labels)}
              meta={meta.text ? <Line {...meta} /> : undefined}
              phoneMeta={<>{day} · <Line {...phone} /></>}
              status={<RowStatus row={row} labels={labels} />}
              amount={<RowAmount row={row} money={money} />}
            />
          </li>
        ),
      };
    }),
  ].sort((a, b) => a.day.localeCompare(b.day) || a.order - b.order);
  const retry = () => {
    if (overviewQuery.isError) overviewQuery.refetch();
    if (rowsQuery.isError) rowsQuery.refetch();
  };
  const next = account.next_due && account.next_due.left_to_pay > 0 ? account.next_due : null;
  const limitLine = account.over_limit
    ? t("overLimit")
    : typeof account.limit_left === "number" ? t("limitLeft", { amount: money(account.limit_left) }) : null;

  return (
    <section className="credit-running" aria-labelledby={headingId}>
      <div className="credit-running-h">
        <h3 id={headingId} className="t-label">{account.name}</h3>
        <p className="t-fig tabular-nums">{t("owed", { amount: money(owed) })}</p>
        {limitLine && <p className={account.over_limit ? "t-meta c-out" : "t-meta"}>{limitLine}</p>}
        {next && <p className="t-meta">{t("nextBill", { date: format.shortDate(next.due_date), amount: money(next.left_to_pay) })}</p>}
      </div>
      {!failed && overview?.due_dates_unavailable && owed > 0 && <p className="t-meta c-warning">{t("noDueDayLine")}</p>}
      {failed ? (
        <ErrorState message={t("rowsFailed", { account: account.name })} onRetry={retry} />
      ) : loading ? (
        <p className="t-meta" aria-busy="true">…</p>
      ) : entries.length === 0 ? (
        <p className="t-body muted">{wantsRows ? t("noRows") : t("nothingRunning")}</p>
      ) : (
        <ul className="list">
          {entries.map((entry) => entry.node)}
        </ul>
      )}
      <div className="list-foot">
        <Link href={`/ledger?account=${encodeURIComponent(account.id)}`} className="ws-btn">
          {t("allInLedger")}
        </Link>
      </div>
    </section>
  );
}

/**
 * "Tagihan kredit" opened from Beranda (open-beranda-figures D5, D6): one section per credit account, most owed
 * first, each with its facts and its transactions from the oldest unpaid item to today, oldest first. A row opens
 * the shared transaction dialog through `onSelect`, where a payment shows what it paid.
 */
export function CreditBillsDialog({
  open,
  onClose,
  accounts,
  failed,
  onRetry,
  money,
  onSelect,
}: {
  open: boolean;
  onClose: () => void;
  accounts: AccountItem[];
  failed: boolean;
  onRetry: () => void;
  money: (amount: number) => string;
  onSelect: (row: ActivityRow) => void;
}) {
  const t = useTranslations("home.creditBills");
  const terms = useTranslations("terms");
  return (
    <ShellDialog open={open} onClose={onClose} title={terms("creditBills")} meta={t("meta")} width={640} closeLabel={t("close")} scrollFocusable>
      {failed ? (
        <ErrorState message={t("accountsFailed")} onRetry={onRetry} />
      ) : (
        <div className="stack">
          {accounts.map((account) => (
            <CreditAccountSection key={account.id} account={account} money={money} onSelect={onSelect} />
          ))}
        </div>
      )}
    </ShellDialog>
  );
}
