"use client";

import { useContext, useEffect, useId, useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { canSwitchToAmountTracking, listAmountInvestmentProducts } from "@/lib/investmentTopups";
import type { ActivityRow } from "@/components/beranda/types";
import { RowAmount, rowTitle, useRowLabels } from "@/components/transactions/rows";
import { TransactionDialog } from "@/components/transactions/TransactionDialog";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { Icon } from "@/components/ui/Icon";
import { GainText, ProgressBar } from "@/components/workspace/figures";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MenuButton, type MenuEntry } from "@/components/workspace/Menu";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { WorkspaceContext } from "@/components/workspace/context";
import { isCredit, isHolding, isPosition } from "./accountFacts";
import { AccountLinks } from "./AccountLinks";
import { useAccountDialogs } from "./AccountDialogs";
import { useAccountText } from "./accountText";
import { BalanceCheckDialog, canCheckBalance } from "./BalanceCheckDialog";
import { creditFacts } from "./credit";
import { CreditBills, type CreditOverview } from "./CreditBills";
import { roleOf } from "./wallet";
import type { AccountItem, AccountsResponse } from "./types";

const RECENT_LIMIT = 4;

interface Action {
  key: string;
  label: string;
  onSelect: () => void;
}

/**
 * The detail of an account, pocket, product, platform or credit account (D8): a centered 560 px dialog, a bottom
 * sheet below 768 px. It loads its own data through the shared query keys, so other pages (phase 9) can open it
 * by id. Its footer has one primary action, one secondary action and the rest under "Lainnya". A pocket or
 * product opens in the same dialog, and "Kembali" returns. The caller returns focus to the opener on close.
 */
export function AccountDetailDialog({ accountId, onClose }: { accountId: string | null; onClose: () => void }) {
  const dialogs = useAccountDialogs();
  const [trail, setTrail] = useState<string[]>([]);
  const shownId = trail.length ? trail[trail.length - 1] : accountId;
  useEffect(() => setTrail([]), [accountId]);

  const accountsQuery = useQuery<AccountsResponse>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });
  const all = useMemo(() => accountsQuery.data?.accounts ?? [], [accountsQuery.data]);
  const byId = useMemo(() => {
    const map = new Map<string, AccountItem>();
    for (const account of all) {
      map.set(account.id, account);
      for (const pocket of account.children ?? []) map.set(pocket.id, pocket);
    }
    return map;
  }, [all]);
  const amountProducts = useMemo(() => new Set(listAmountInvestmentProducts(all).map((item) => item.id)), [all]);
  const account = shownId ? byId.get(shownId) ?? null : null;
  const missing = Boolean(accountId && accountsQuery.isSuccess && (!account || account.is_archived));
  useEffect(() => {
    if (missing) onClose();
  }, [missing, onClose]);

  return (
    <>
      {account && !account.is_archived && (
        <DetailBody
          key={account.id}
          account={account}
          parent={account.parent_id ? byId.get(account.parent_id) ?? null : null}
          amountProducts={amountProducts}
          dialogs={dialogs}
          back={trail.length ? byId.get(trail.length > 1 ? trail[trail.length - 2] : accountId!)?.name ?? null : null}
          onBack={() => setTrail((current) => current.slice(0, -1))}
          onOpenAccount={(id) => setTrail((current) => [...current, id])}
          onClose={onClose}
        />
      )}
      {dialogs.dialogs}
    </>
  );
}

function DetailBody({
  account,
  parent,
  amountProducts,
  dialogs,
  back,
  onBack,
  onOpenAccount,
  onClose,
}: {
  account: AccountItem;
  parent: AccountItem | null;
  amountProducts: Set<string>;
  dialogs: ReturnType<typeof useAccountDialogs>;
  back: string | null;
  onBack: () => void;
  onOpenAccount: (id: string) => void;
  onClose: () => void;
}) {
  const { t, format, roleLabel, typeLabel, methodLine, holdingLine } = useAccountText();
  const money = useMoney();
  const labels = useRowLabels();
  // Outside the shell (a test harness) there is no capture form to open, so "Bayar tagihan" only closes.
  const openMovementCapture = useContext(WorkspaceContext)?.openMovementCapture;
  const { record } = useTransactionUndo();
  const [checking, setChecking] = useState(false);
  const [confirmArchive, setConfirmArchive] = useState(false);
  const [openTx, setOpenTx] = useState<string | null>(null);
  const factsId = useId();

  const pockets = (account.children ?? []).filter((pocket) => !pocket.is_archived);
  const credit = isCredit(account);
  const holding = isHolding(account);
  const position = isPosition(account);
  const amountTracked = amountProducts.has(account.id);
  const isPocket = Boolean(account.parent_id);
  const platform = holding && pockets.length > 0;
  const checkable = canCheckBalance(account);
  const canReconcile = !pockets.length && !credit && account.investment_tracking_mode !== "amount";
  const facts = credit ? creditFacts(account) : null;

  const recentQuery = useQuery<{ transactions: ActivityRow[] }>({
    queryKey: [...queryKeys.transactions.all, "account", account.id],
    queryFn: () => api.get(`/transactions?account_id=${account.id}&limit=${RECENT_LIMIT}`),
  });
  const creditQuery = useQuery<CreditOverview>({
    queryKey: queryKeys.transactions.creditOverview(account.id, ""),
    queryFn: () => api.get(`/credit/accounts/${account.id}`),
    enabled: credit,
  });

  const meta = isPocket && parent
    ? t(holding ? "detail.kind.productOf" : "detail.kind.pocketOf", { parent: parent.name })
    : credit ? t("detail.kind.credit") : account.type === "bank" ? t("detail.kind.bank") : typeLabel(account);

  // Actions per kind (D8): one primary, one secondary, the rest under "Lainnya".
  const edit: Action = {
    key: "edit",
    label: credit ? t("actions.editCredit") : isPocket ? t("actions.editPocket") : t("actions.editAccount"),
    onSelect: () => dialogs.openEditAccount(account),
  };
  const archive: Action = { key: "archive", label: t("actions.archive"), onSelect: () => setConfirmArchive(true) };
  const openParent: Action | null = parent ? { key: "parent", label: t("actions.openParent", { parent: parent.name }), onSelect: () => onOpenAccount(parent.id) } : null;
  const addPocket: Action = { key: "pocket", label: t("actions.addPocket"), onSelect: () => dialogs.openNewPocket(account) };
  const check: Action = { key: "check", label: t("actions.checkBalance"), onSelect: () => setChecking(true) };
  const adjust: Action = { key: "adjust", label: t("actions.adjustBalance"), onSelect: () => dialogs.openReconciliation(account) };
  const revalue: Action = { key: "revalue", label: t("actions.revalue"), onSelect: () => dialogs.openValuation(account) };
  const list = (...items: (Action | null | false)[]) => items.filter(Boolean) as Action[];

  let primary: Action | null;
  let secondary: Action | null;
  let more: Action[];
  if (credit) {
    const due = facts?.nextDue;
    primary = {
      key: "pay",
      label: t("actions.payBill"),
      onSelect: () => {
        onClose();
        openMovementCapture?.({ targetAccountId: account.id, amount: due && due.left_to_pay > 0 ? due.left_to_pay : undefined });
      },
    };
    secondary = edit;
    more = list(archive);
  } else if (position && amountTracked) {
    primary = { key: "topup", label: t("actions.topUp"), onSelect: () => dialogs.openTopup(account) };
    secondary = revalue;
    more = list(edit, openParent, archive);
  } else if (position) {
    primary = { key: "buy", label: t("actions.buy"), onSelect: () => dialogs.openTrade(account, "buy") };
    secondary = { key: "sell", label: t("actions.sell"), onSelect: () => dialogs.openTrade(account, "sell") };
    more = list(
      revalue,
      canReconcile && adjust,
      canSwitchToAmountTracking(account) && { key: "amount", label: t("actions.toAmount"), onSelect: () => dialogs.openAmountSwitch(account) },
      edit,
      openParent,
      archive,
    );
  } else if (pockets.length) {
    primary = addPocket;
    secondary = edit;
    more = list(!platform && checkable && check, archive);
  } else if (checkable) {
    primary = check;
    secondary = edit;
    more = list(canReconcile && adjust, !isPocket && addPocket, openParent, archive);
  } else {
    primary = edit;
    secondary = openParent;
    more = list(canReconcile && adjust, !isPocket && !holding && addPocket, archive);
  }

  const recent = recentQuery.data?.transactions ?? [];
  const footer = confirmArchive ? (
    <>
      <p className="grow-text t-body" role="alert">
        {t("actions.archiveConfirm", { kind: isPocket ? "pocket" : "account", name: account.name, withPockets: pockets.length ? "true" : "false" })}
      </p>
      <button type="button" className="ws-btn" onClick={() => setConfirmArchive(false)} autoFocus>
        {t("detail.close")}
      </button>
      <button
        type="button"
        className="ws-btn ws-btn-accent"
        onClick={() => {
          dialogs.archiveMutation.mutate(account.id);
          onClose();
        }}
      >
        {t("actions.archiveYes")}
      </button>
    </>
  ) : (
    <>
      {more.length > 0 && (
        <MenuButton
          label={t("actions.moreFor", { name: account.name })}
          className="ws-btn ws-btn-quiet"
          menuLabel={t("actions.more")}
          width={260}
          entries={more.map((action): MenuEntry => ({ key: action.key, label: action.label, onSelect: action.onSelect }))}
        >
          {t("actions.more")}
          <Icon name="chevD" className="h-4 w-4" />
        </MenuButton>
      )}
      <span className="grow" />
      {secondary && <button type="button" className="ws-btn" onClick={secondary.onSelect}>{secondary.label}</button>}
      {primary && <button type="button" className="ws-btn ws-btn-accent" onClick={primary.onSelect}>{primary.label}</button>}
    </>
  );

  return (
    <>
      <ShellDialog open onClose={onClose} title={account.name} meta={meta} width={560} closeLabel={t("detail.close")} className="rk-detail" footer={footer}>
        {back && (
          <button type="button" className="ws-linkbtn" onClick={onBack}>
            <Icon name="chevron-left" className="h-4 w-4" />
            {t("detail.back", { name: back })}
          </button>
        )}
        <Figure account={account} facts={facts} holding={holding} platform={platform} />
        <dl className="dl" aria-label={t("detail.facts", { name: account.name })} id={factsId}>
          {facts ? (
            <CreditFactsList facts={facts} overview={creditQuery.data} />
          ) : holding && !pockets.length ? (
            <>
              <Fact label={t("detail.cost")}>
                {account.cost_basis != null ? <MaskedMoney text={format.money(account.cost_basis)} /> : t("detail.notSet")}
              </Fact>
              <Fact label={t("detail.gain")}>
                <GainText gain={account.capital_gain != null ? { amount: account.capital_gain, cost: account.balance - account.capital_gain } : "unknown"} />
              </Fact>
              <Fact label={t("detail.method")}>{methodLine(account, amountProducts)}</Fact>
              {account.units != null && account.investment_tracking_mode !== "amount" && (
                <Fact label={t("detail.amount")}>
                  {account.instrument_type === "stock"
                    ? t("detail.shares", { units: format.decimal(account.units, 3), lots: format.decimal(account.units / 100, 3) })
                    : holdingLine(account, amountProducts)}
                </Fact>
              )}
              {account.last_price != null && <Fact label={t("detail.lastPrice")}><MaskedMoney text={format.money(account.last_price)} /></Fact>}
              {account.avg_buy_price != null && <Fact label={t("detail.avgPrice")}><MaskedMoney text={format.money(account.avg_buy_price)} /></Fact>}
              {account.instrument_symbol && <Fact label={t("detail.code")}>{account.instrument_symbol}</Fact>}
              {account.default_funding_account_name && <Fact label={t("detail.fundingAccount")}>{account.default_funding_account_name}</Fact>}
              {account.reconciliation_required && <Fact label={t("detail.check")}><Flag account={account} onAdjust={canReconcile ? adjust.onSelect : undefined} /></Fact>}
            </>
          ) : platform ? (
            <>
              <Fact label={t("detail.productCount")}>{format.integer(pockets.length)}</Fact>
              {account.default_funding_account_name && <Fact label={t("detail.fundingAccount")}>{account.default_funding_account_name}</Fact>}
            </>
          ) : (
            <>
              <Fact label={t("detail.use")}>{roleLabel(roleOf(account))}</Fact>
              {parent && <Fact label={t("detail.parent")}>{parent.name}</Fact>}
              {pockets.length > 0 && <Fact label={t("detail.mainPocket")}>{pockets[0].name}</Fact>}
              {account.reconciliation_required && <Fact label={t("detail.check")}><Flag account={account} onAdjust={canReconcile ? adjust.onSelect : undefined} /></Fact>}
            </>
          )}
        </dl>
        {credit && (
          <>
            <p className="t-meta">{t("detail.creditNote")}</p>
            <CreditBills
              accountId={account.id}
              overview={creditQuery.data}
              failed={creditQuery.isError}
              onRetry={() => creditQuery.refetch()}
              onEditCredit={edit.onSelect}
            />
          </>
        )}
        {pockets.length > 0 && (
          <section aria-labelledby={`${factsId}-pockets`}>
            <SectionHeader id={`${factsId}-pockets`} label={holding ? t("detail.products") : t("detail.pockets")} aside={format.integer(pockets.length)} />
            <ul className="list">
              {pockets.map((pocket) => (
                <li key={pocket.id}>
                  <ListRow
                    onClick={() => onOpenAccount(pocket.id)}
                    title={pocket.name}
                    meta={
                      holding ? (
                        <GainText gain={pocket.cost_basis != null ? { amount: pocket.balance - pocket.cost_basis, cost: pocket.cost_basis } : "unknown"} />
                      ) : account.default_pocket_id === pocket.id ? (
                        t("tile.withDefault", { meta: roleLabel(roleOf(pocket)), default: t("tile.default") })
                      ) : (
                        roleLabel(roleOf(pocket))
                      )
                    }
                    amount={<MaskedMoney text={format.money(pocket.balance)} className="t-amt" />}
                    chevron
                  />
                </li>
              ))}
            </ul>
          </section>
        )}
        <AccountLinks accountId={account.id} accountName={account.name} />
        <section aria-labelledby={`${factsId}-recent`}>
          <SectionHeader id={`${factsId}-recent`} label={t("detail.recent")} />
          {recentQuery.isError && <p className="t-meta">{t("detail.recentFailed")}</p>}
          {recentQuery.isSuccess && recent.length === 0 && <p className="t-meta">{t("detail.noRecent")}</p>}
          {recent.length > 0 && (
            <ul className="list">
              {recent.map((row) => (
                <li key={row.id}>
                  <ListRow
                    onClick={() => setOpenTx(row.id)}
                    title={rowTitle(row, labels)}
                    meta={t("detail.when", { date: format.shortDateAt(row.date), time: format.time(row.date) })}
                    amount={<RowAmount row={row} money={money} />}
                  />
                </li>
              ))}
            </ul>
          )}
        </section>
      </ShellDialog>
      {checking && (
        <BalanceCheckDialog
          account={account}
          money={money}
          onClose={() => setChecking(false)}
          onAdjust={(actual, onReconciled) => dialogs.openReconciliation(account, { actual, onReconciled })}
        />
      )}
      <TransactionDialog transactionId={openTx} onClose={() => setOpenTx(null)} record={record} />
    </>
  );
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </>
  );
}

function Flag({ account, onAdjust }: { account: AccountItem; onAdjust?: () => void }) {
  const { t } = useAccountText();
  if (!onAdjust) {
    return (
      <span className="c-warning">
        <Icon name="alert-triangle" className="mr-1 inline h-3.5 w-3.5 align-[-2px]" />
        {t("tile.needsCheck")}
      </span>
    );
  }
  return (
    <button type="button" className="ws-linkbtn c-warning" aria-label={t("tile.needsCheckAction", { name: account.name })} onClick={onAdjust}>
      <Icon name="alert-triangle" className="h-3.5 w-3.5" />
      {t("tile.needsCheck")}
    </button>
  );
}

/** The figure block: "Saldo", "Nilai sekarang" with the gain, or "Utang" with the 8 px bar of owed over limit. */
function Figure({ account, facts, holding, platform }: { account: AccountItem; facts: ReturnType<typeof creditFacts> | null; holding: boolean; platform: boolean }) {
  const { t, format } = useAccountText();
  if (facts) {
    const overpaid = facts.overpaid > 0;
    return (
      <div className="dlg-fig">
        <span className="t-meta">{overpaid ? t("detail.overpaid") : t("detail.debt")}</span>
        <MaskedMoney
          text={overpaid ? format.money(facts.overpaid) : facts.owed > 0 ? `−${format.money(facts.owed)}` : format.money(0)}
          className={overpaid ? "t-fig-l c-income" : "t-fig-l c-expense"}
        />
        {facts.limit ? <ProgressBar value={facts.owed} max={facts.limit} thick color="var(--out-solid)" /> : null}
      </div>
    );
  }
  const gain = platform
    ? account.group_capital_gain != null && account.group_cost_basis
      ? { amount: account.group_capital_gain, cost: account.group_cost_basis }
      : ("unknown" as const)
    : holding && account.capital_gain !== undefined
      ? account.capital_gain != null ? { amount: account.capital_gain, cost: account.balance - account.capital_gain } : ("unknown" as const)
      : null;
  return (
    <div className="dlg-fig">
      <span className="t-meta">{holding ? t("detail.value") : t("detail.balance")}</span>
      <MaskedMoney text={format.money(account.balance)} className="t-fig-l" />
      {holding && <GainText gain={gain} unknownLabel={platform ? t("tile.costIncomplete") : undefined} />}
    </div>
  );
}

function CreditFactsList({ facts, overview }: { facts: ReturnType<typeof creditFacts>; overview: CreditOverview | undefined }) {
  const { t, format } = useAccountText();
  const due = facts.nextDue;
  const day = (value: number | null) => (value ? t("detail.dayOfMonth", { day: value }) : t("detail.notSet"));
  return (
    <>
      <Fact label={t("detail.limit")}>{facts.limit ? <MaskedMoney text={format.money(facts.limit)} /> : t("credit.noLimit")}</Fact>
      <Fact label={t("detail.limitLeft")}>
        {facts.limit === null ? t("credit.noLimit") : facts.overLimit ? <span className="c-expense">{t("credit.overLimit")}</span> : <MaskedMoney text={format.money(facts.limitLeft ?? 0)} />}
      </Fact>
      <Fact label={t("detail.nextBill")}>
        {!facts.paymentDay ? t("credit.noDueDay") : due ? (
          <span className={due.status === "due_today" || due.status === "overdue" ? "c-warning" : undefined}>
            {t.rich("detail.nextBillValue", {
              amount: () => <MaskedMoney text={format.money(due.left_to_pay)} />,
              date: due.status === "due_today" ? t("credit.dueToday") : format.shortDate(due.due_date),
            })}
            {due.status === "overdue" && ` · ${t("credit.overdue")}`}
          </span>
        ) : t("detail.noBill")}
      </Fact>
      <Fact label={t("detail.statementDay")}>{day(facts.statementDay)}</Fact>
      <Fact label={t("detail.paymentDay")}>{day(facts.paymentDay)}</Fact>
      {facts.overpaid > 0 && <Fact label={t("detail.overpaid")}><MaskedMoney text={format.money(facts.overpaid)} /></Fact>}
      {overview && (
        <>
          <Fact label={t("detail.spentOnCredit")}><MaskedMoney text={format.money(overview.totals.spent_on_credit)} /></Fact>
          <Fact label={t("detail.paidToCredit")}><MaskedMoney text={format.money(overview.totals.paid_to_credit)} /></Fact>
          <Fact label={t("detail.onTime")}>
            {t("detail.onTimeValue", { onTime: format.integer(overview.totals.items_settled_on_time), settled: format.integer(overview.totals.items_settled) })}
          </Fact>
        </>
      )}
    </>
  );
}
