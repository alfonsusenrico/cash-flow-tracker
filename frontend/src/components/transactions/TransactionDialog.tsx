"use client";

import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { useToast } from "@/components/workspace/overlays";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { ApiError, api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { isPlaceholderNote } from "@/lib/placeholderNotes";
import { isHabitRecorded } from "@/lib/provenance";
import { queryKeys } from "@/lib/queryKeys";
import type { UndoFields } from "@/lib/reviewUndo";
import {
  RowAmount, accountLabel, bucketName, maskedTag, movementRoute, rowKind, rowTitle, savingFlowLabel, useRowLabels, type RowKind,
} from "./rows";
import type { CreditBlock, CreditSettledItem, LedgerRowDetail, ReimbursementLinkLine, ReimbursementLinks } from "./types";
import { useRefreshTransactionViews } from "./useTransactionUndo";

// The editors load when first used, so Beranda, which also opens this dialog, stays light.
const TransactionEditDialog = dynamic(() => import("./TransactionEditDialog").then((module) => module.TransactionEditDialog), { ssr: false });
const MovementEditDialog = dynamic(() => import("./MovementEditDialog").then((module) => module.MovementEditDialog), { ssr: false });
const ExpenseTopupConversionModal = dynamic(() => import("@/components/ui/ExpenseTopupConversionModal").then((module) => module.ExpenseTopupConversionModal), { ssr: false });
const InvestmentTopupModal = dynamic(() => import("@/components/ui/InvestmentTopupModal").then((module) => module.InvestmentTopupModal), { ssr: false });

export type RecordUndo = (text: string, label: string, response: UndoFields | null | undefined, transactionId: string) => void;

const SOURCES = ["manual", "notification", "recurring", "payroll", "movement", "reconcile", "investment", "import", "capture"] as const;
const CLASSIFIERS = ["rule", "ai", "owner", "system", "habit"] as const;
const BILL_STATUSES = ["open", "upcoming", "due_today", "overdue", "paid", "paid_late"] as const;

/** Whether a value the server sent is one the catalogue names; an unknown one is shown as sent. */
function known<T extends string>(values: readonly T[], value: string | null | undefined): value is T {
  return Boolean(value) && (values as readonly string[]).includes(value as string);
}

/** Rows the owner can mark as checked here: automatic ones; waiting rows are answered in Tinjau. */
function checkable(row: LedgerRowDetail): boolean {
  return !row.waiting && row.review_status === "auto";
}

/** The path each row kind is deleted through: a top-up only through its own lifecycle. */
function deletePath(row: LedgerRowDetail): string {
  if (row.investment_topup_id) return `/investment-topups/${row.investment_topup_id}`;
  if (row.movement_id) return `/movements/${row.movement_id}`;
  return `/transactions/${row.id}`;
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </>
  );
}

/**
 * The transaction dialog of Transaksi, Beranda, Kalender and Peta Aliran (apply-markas-transaksi D14, D30): the
 * amount, a label and value list per kind, the owner's source text, the Tinjau note, and "Hapus" and "Ubah".
 * "Ubah" opens the edit dialog in its place; delete confirms in the footer with its effects listed.
 */
export function TransactionDialog({
  transactionId,
  onClose,
  record,
  onPickPair,
  onDeleted,
}: {
  transactionId: string | null;
  onClose: () => void;
  record: RecordUndo;
  /** Starts the two-row merge with this row selected; absent where the page has no list to pick from. */
  onPickPair?: (row: LedgerRowDetail) => void;
  onDeleted?: (row: LedgerRowDetail) => void;
}) {
  const t = useTranslations();
  const money = useMoney();
  const format = useFormat();
  const labels = useRowLabels();
  const { showToast } = useToast();
  const refresh = useRefreshTransactionViews();
  const [shownId, setShownId] = useState<string | null>(transactionId);
  const [mode, setMode] = useState<"view" | "delete">("view");
  const [editing, setEditing] = useState(false);
  const [topupOpen, setTopupOpen] = useState(false);
  const [actionError, setActionError] = useState("");
  const cancelDeleteRef = useRef<HTMLButtonElement>(null);
  const editButtonRef = useRef<HTMLButtonElement>(null);
  const returnToEdit = useRef(false);
  const amountRef = useRef<HTMLDivElement>(null);
  const focusOpenedRow = useRef(false);

  // Focus returns to whatever opened the dialog once it closes (ShellDialog leaves that to its caller); Transaksi
  // moves it to the row again after its list settles.
  const opener = useRef<HTMLElement | null>(null);
  useEffect(() => {
    if (transactionId) return;
    const target = opener.current;
    opener.current = null;
    if (target?.isConnected) target.focus();
    // While closed, remember the last focused element: the listener is removed before the opened dialog moves focus.
    const track = (event: FocusEvent) => {
      if (event.target instanceof HTMLElement) opener.current = event.target;
    };
    document.addEventListener("focusin", track);
    return () => document.removeEventListener("focusin", track);
  }, [transactionId]);

  useEffect(() => {
    setShownId(transactionId);
    setMode("view");
    setEditing(false);
    setActionError("");
    focusOpenedRow.current = false;
  }, [transactionId]);

  const accounts = useQuery<{ accounts: any[] }>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts"), enabled: Boolean(shownId) }).data?.accounts ?? [];
  const detail = useQuery<{ transaction: LedgerRowDetail }>({
    queryKey: queryKeys.transactions.detail(shownId ?? ""),
    queryFn: () => api.get(`/transactions/${shownId}`),
    enabled: Boolean(shownId),
    retry: (count, error) => !(error instanceof ApiError && error.status === 404) && count < 2,
  });
  const row = detail.data?.transaction ?? null;
  const missing = detail.error instanceof ApiError && detail.error.status === 404;

  useEffect(() => {
    if (mode === "delete") cancelDeleteRef.current?.focus();
  }, [mode]);

  // A row deleted elsewhere: the lists behind the dialog still show it, so they reload.
  useEffect(() => {
    if (missing) refresh(shownId ?? undefined);
  }, [missing, refresh, shownId]);

  // The control that opened another row is gone with the row it sat in, so focus moves to the new row's body.
  useEffect(() => {
    if (!row || !focusOpenedRow.current) return;
    focusOpenedRow.current = false;
    const body = amountRef.current?.closest<HTMLElement>(".dlg-b");
    if (!body) return;
    body.scrollTop = 0;
    body.focus({ preventScroll: true });
  }, [row]);

  // Closing the edit returns to the detail with focus on "Ubah" (D15).
  useEffect(() => {
    if (editing || !returnToEdit.current || !row) return;
    returnToEdit.current = false;
    const timer = window.setTimeout(() => editButtonRef.current?.focus(), 0);
    return () => window.clearTimeout(timer);
  }, [editing, row]);

  const markChecked = useMutation({
    mutationFn: (target: LedgerRowDetail) => api.post<UndoFields>("/review/bulk", { action: "accept", transaction_ids: [target.id] }),
    onSuccess: (response, target) => {
      refresh();
      record(t("transactions.toast.checked"), rowTitle(target, labels), response, target.id);
    },
    onError: (error: Error) => setActionError(error.message || t("transactions.toast.markFailed")),
  });

  const remove = useMutation({
    mutationFn: (target: LedgerRowDetail) => api.del(deletePath(target)),
    onSuccess: (_result, target) => {
      onClose();
      refresh(target.id, true);
      showToast(t("transactions.toast.deleted"));
      onDeleted?.(target);
    },
    onError: (error: Error) => setActionError(error.message || t("transactions.toast.deleteFailed")),
  });

  const open = Boolean(transactionId) && !editing;
  const kind = row ? rowKind(row) : null;
  const title = row ? rowTitle(row, labels) : missing ? t("transactions.dialog.missingTitle") : t("transactions.dialog.title");
  const when = row ? (() => {
    const date = format.weekdayAt(row.date);
    const time = format.time(row.date);
    return kind === "expense" && row.category_name
      ? t("transactions.dialog.whenCategory", { date, time, category: row.category_name })
      : t("transactions.dialog.when", { date, time });
  })() : undefined;

  const openRow = (id: string) => {
    setMode("view");
    focusOpenedRow.current = true;
    setShownId(id);
  };
  const startEdit = () => {
    setActionError("");
    if (row) setEditing(true);
  };
  const endEdit = () => {
    returnToEdit.current = true;
    setEditing(false);
  };

  const footer = row ? (
    mode === "delete" ? (
      <>
        <p className="t-body grow-text">{t.rich("transactions.delete.question", { amount: () => <MaskedMoney text={money(row.amount)} /> })}</p>
        <button ref={cancelDeleteRef} type="button" className="ws-btn" onClick={() => setMode("view")}>
          {t("common.cancel")}
        </button>
        <button type="button" className="ws-btn ws-btn-danger" disabled={remove.isPending} onClick={() => remove.mutate(row)}>
          {remove.isPending ? t("transactions.delete.deleting") : t("transactions.delete.confirm")}
        </button>
      </>
    ) : (
      <>
        <button type="button" className="ws-btn ws-btn-quiet c-expense" onClick={() => { setActionError(""); setMode("delete"); }}>
          <Icon name="trash" className="h-4 w-4" />
          {t("transactions.detail.delete")}
        </button>
        <span className="grow" />
        <button ref={editButtonRef} type="button" className="ws-btn" onClick={startEdit}>
          <Icon name="edit" className="h-4 w-4" />
          {t("transactions.detail.edit")}
        </button>
      </>
    )
  ) : undefined;

  return (
    <>
      <ShellDialog open={open} onClose={onClose} title={title} meta={when} width={560} closeLabel={t("transactions.dialog.close")} footer={footer} className="tx-dialog" scrollFocusable>
        {detail.isLoading && (
          <div aria-busy="true" aria-label={t("transactions.dialog.loading")}>
            <span className="skel is-hero" />
            <span className="skel is-line" />
            <span className="skel is-line" />
          </div>
        )}
        {missing && <p role="alert">{t("transactions.dialog.missing")}</p>}
        {detail.isError && !missing && (
          <div className="error-box" role="alert">
            <Icon name="alert" className="h-5 w-5" />
            <p className="grow">{t("transactions.dialog.loadError")}</p>
            <button type="button" className="ws-btn" onClick={() => detail.refetch()}>{t("common.tryAgain")}</button>
          </div>
        )}
        {row && kind && (
          <>
            <div ref={amountRef} className="tx-amount">
              <span className="t-meta">{t("transactions.dialog.amount")}</span>
              <RowAmount row={row} money={money} className="t-fig-l" />
            </div>
            {row.waiting && (
              <div className="box" role="note">
                <Icon name="help" className="h-5 w-5 c-warning" />
                <div className="grow">
                  <p>{t("transactions.detail.waiting")}</p>
                  <Link className="ws-btn" href={`/review?item=${row.id}`}>{t("transactions.detail.answer")}</Link>
                </div>
              </div>
            )}
            {row.credit?.role === "payment" && <PaymentSettlement credit={row.credit} onOpenRow={openRow} />}
            {row.reimbursement && <ReimbursementView links={row.reimbursement} onOpenRow={openRow} />}
            <Facts row={row} kind={kind} onOpenFee={openRow} onMarkChecked={() => markChecked.mutate(row)} marking={markChecked.isPending} />
            {row.origin && (
              <figure className="raw">
                <figcaption className="t-meta">
                  {row.origin.kind === "capture"
                    ? t("transactions.detail.originTyped")
                    : row.origin.app
                      ? t("transactions.detail.originNotificationApp", { app: row.origin.app })
                      : t("transactions.detail.originNotification")}
                </figcaption>
                <blockquote>{row.origin.text}</blockquote>
              </figure>
            )}
            {mode === "delete" && (
              <div className="box" role="group" aria-label={t("transactions.detail.delete")}>
                <div className="grow">
                  <DeleteEffects row={row} kind={kind} />
                  <p className="t-meta">{t("transactions.delete.final")}</p>
                </div>
              </div>
            )}
            {actionError && <p role="alert" className="form-alert">{actionError}</p>}
          </>
        )}
      </ShellDialog>

      {row && editing && kind === "topup" && (
        <InvestmentTopupModal
          open
          onClose={() => { endEdit(); refresh(); }}
          product={accounts.flatMap((account) => [account, ...(account.children ?? [])]).find((account) => account.id === (row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id)) ?? {
            id: row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id ?? "",
            name: (row.movement_role === "inbound" ? row.account_name : row.transfer_target_account_name) ?? labels.investmentTopUp,
            type: "investment",
          }}
          allAccounts={accounts}
          editingTopup={{
            id: row.investment_topup_id!,
            source_account_id: row.movement_role === "inbound" ? row.transfer_target_account_id ?? "" : row.account_id,
            target_account_id: row.movement_role === "inbound" ? row.account_id : row.transfer_target_account_id ?? "",
            amount: row.amount,
            date: row.date,
            notes: row.notes,
          }}
        />
      )}
      {row && editing && kind === "movement" && row.movement_id && (
        <MovementEditDialog
          row={row}
          route={movementRoute(row)}
          onClose={endEdit}
          onSaved={() => {
            endEdit();
            refresh();
            showToast(t("transactions.toast.saved"));
          }}
          onSplit={() => {
            setEditing(false);
            onClose();
            refresh(row.id, true);
            showToast(t("transactions.toast.split"));
          }}
        />
      )}
      {row && editing && (kind === "expense" || kind === "income") && (
        <TransactionEditDialog
          tx={row}
          onClose={endEdit}
          onSaved={() => {
            endEdit();
            refresh();
            showToast(t("transactions.toast.saved"));
          }}
          onPickPair={onPickPair ? () => {
            setEditing(false);
            onPickPair(row);
          } : undefined}
          onConvertToTopup={() => {
            setEditing(false);
            setTopupOpen(true);
          }}
        />
      )}
      {row && topupOpen && (
        <ExpenseTopupConversionModal
          expense={row}
          accounts={accounts}
          onClose={() => {
            setTopupOpen(false);
            returnToEdit.current = true;
          }}
          onConverted={(response) => {
            setTopupOpen(false);
            refresh();
            record(t("transactions.toast.topup"), rowTitle(row, labels), response, row.id);
          }}
        />
      )}
    </>
  );
}

/** The label and value list of one row, by its kind (D14, D30). */
function Facts({
  row, kind, onOpenFee, onMarkChecked, marking,
}: {
  row: LedgerRowDetail;
  kind: RowKind;
  onOpenFee: (id: string) => void;
  onMarkChecked: () => void;
  marking: boolean;
}) {
  const t = useTranslations();
  const money = useMoney();
  const format = useFormat();
  const labels = useRowLabels();
  const none = t("transactions.detail.none");
  const amount = (value: number) => maskedTag(money(value));
  const source = row.source;
  const classifier = row.classified_by;
  const recorded = known(SOURCES, source) ? t(`transactions.detail.sources.${source}`) : source || none;
  const classified = known(CLASSIFIERS, classifier) ? t(`transactions.detail.classifiers.${classifier}`) : classifier || none;
  const checked = row.review_status === "auto" || row.waiting
    ? (
      <>
        {t("transactions.detail.checkedNo")}
        {checkable(row) && (
          <>
            {" · "}
            <button type="button" className="ws-linkbtn" disabled={marking} onClick={onMarkChecked}>
              {marking ? t("transactions.detail.marking") : t("transactions.detail.markChecked")}
            </button>
          </>
        )}
      </>
    )
    : t("transactions.detail.checkedYes");

  if (kind === "movement" || kind === "topup") {
    const route = movementRoute(row);
    const complete = Boolean(route.source && route.target);
    return (
      <dl className="dl">
        <Fact label={t("transactions.detail.from")}>{route.source ?? <span className="c-warning">{labels.unpaired}</span>}</Fact>
        <Fact label={t("transactions.detail.to")}>{route.target ?? <span className="c-warning">{labels.unpaired}</span>}</Fact>
        {row.movement_fee && (
          <Fact label={t("transactions.detail.fee")}>
            <MaskedMoney text={money(row.movement_fee.amount)} />
            {" · "}
            <button type="button" className="ws-linkbtn" onClick={() => onOpenFee(row.movement_fee!.transaction_id)}>
              {t("transactions.detail.feeOpen")}
            </button>
          </Fact>
        )}
        {row.credit?.role === "payment" && (
          <Fact label={t("transactions.detail.credit.label")}>{t("transactions.detail.credit.billPayment", { provider: row.credit.account_name })}</Fact>
        )}
        <Fact label={t("transactions.detail.pairStatus")}>
          {complete ? t("transactions.detail.pairComplete") : <span className="c-warning">{labels.unpaired}</span>}
        </Fact>
        <Fact label={t("transactions.detail.savingFlow")}>{savingFlowLabel(row, labels) ?? none}</Fact>
        <Fact label={t("transactions.detail.notes")}>{row.notes || none}</Fact>
        <Fact label={t("transactions.detail.recorded")}>{recorded}</Fact>
        <Fact label={t("transactions.detail.classified")}>{classified}</Fact>
        <Fact label={t("transactions.detail.checked")}>{checked}</Fact>
      </dl>
    );
  }

  const allocations = row.obligation_allocations?.length
    ? row.obligation_allocations
    : row.obligation_name ? [{ obligation_id: row.obligation_id ?? "", obligation_name: row.obligation_name, amount: row.amount }] : [];
  const waitingWithoutCategory = row.waiting && !row.category_name;
  return (
    <dl className="dl">
      <Fact label={t("transactions.detail.type")}>{kind === "income" ? t("terms.income") : t("terms.spending")}</Fact>
      <Fact label={t("transactions.detail.account")}>{accountLabel(row.account_name, row.account_parent_name) ?? none}</Fact>
      {row.credit?.role === "purchase" && <CreditFacts row={row} />}
      <Fact label={t("transactions.detail.category")}>
        {waitingWithoutCategory ? <span className="c-warning">{labels.waiting}</span> : row.category_name ?? t("terms.noCategory")}
      </Fact>
      {kind === "expense" && <Fact label={t("transactions.detail.kakeibo")}>{bucketName(t, row.kakeibo_type || "unclassified")}</Fact>}
      <Fact label={t("transactions.detail.merchant")}>{row.merchant || none}</Fact>
      {row.fee_movement && (
        <Fact label={t("transactions.detail.feeFor")}>
          {t.rich("transactions.detail.feeForValue", {
            amount: amount(row.fee_movement.amount),
            route: [row.fee_movement.source_account_name, row.fee_movement.target_account_name].filter(Boolean).join(" → "),
          })}
        </Fact>
      )}
      <Fact label={t("transactions.detail.notes")}>{row.notes || none}</Fact>
      <Fact label={t("transactions.detail.recorded")}>{recorded}</Fact>
      <Fact label={t("transactions.detail.classified")}>{classified}</Fact>
      {isHabitRecorded(row.classified_by, row.habit_basis) && (
        <Fact label={t("transactions.detail.habit")}>
          {t("transactions.detail.habitBasis", { agreeing: format.integer(row.habit_basis.agreeing), support: format.integer(row.habit_basis.support) })}
        </Fact>
      )}
      {row.goal_name && <Fact label={t("transactions.detail.goal")}>{row.goal_name}</Fact>}
      {allocations.map((item) => (
        <Fact key={item.obligation_id} label={t("transactions.detail.bill")}>
          {t.rich("transactions.detail.billValue", { name: item.obligation_name, amount: amount(item.amount) })}
        </Fact>
      ))}
      <Fact label={t("transactions.detail.checked")}>{checked}</Fact>
      <Fact label={t("transactions.detail.receipt")}>{row.receipt_path ? t("transactions.detail.receiptSaved") : t("transactions.detail.receiptNone")}</Fact>
    </dl>
  );
}

/** A credit purchase's terms and settlement, from the server's schedule (H5): facts only, never a score. */
function CreditFacts({ row }: { row: LedgerRowDetail }) {
  const t = useTranslations("transactions.detail.credit");
  const money = useMoney();
  const format = useFormat();
  const credit = row.credit!;
  const count = credit.installment_count ?? 1;
  const items = credit.installments ?? [];
  return (
    <>
      <Fact label={t("payment")}>{count > 1 ? t("installments", { count }) : t("inFull")}</Fact>
      {credit.settled_on ? (
        <Fact label={t("settled")}>
          {t("settledValue", { date: format.shortDate(credit.settled_on), days: credit.days_to_settle ?? 0 })}
        </Fact>
      ) : (
        <Fact label={t("due")}>
          {credit.next_due_date ? format.shortDate(credit.next_due_date) : credit.due_dates_unavailable ? t("unavailable") : "—"}
        </Fact>
      )}
      {count > 1 && items.length > 0 && (
        <Fact label={t("schedule")}>
          <ul className="tx-installments">
            {items.map((item) => {
              const number = item.installment?.number ?? 1;
              const total = item.installment?.count ?? count;
              const billStatus = item.bill_status;
              const status = known(BILL_STATUSES, billStatus) ? t(`status.${billStatus}`) : null;
              return (
                <li key={`${item.due_date}-${number}`}>
                  <span>{t("item", { k: String(number), n: String(total), date: format.shortDate(item.due_date) })}</span>
                  <MaskedMoney text={money(item.amount)} className="t-amt" />
                  <span className="muted">
                    {item.settled_on
                      ? t("itemPaid", { date: format.shortDate(item.settled_on) })
                      : t.rich("itemLeft", { amount: () => <MaskedMoney text={money(item.left_to_pay)} /> })}
                    {status && !item.settled_on ? ` · ${status}` : null}
                  </span>
                </li>
              );
            })}
          </ul>
        </Fact>
      )}
    </>
  );
}

/**
 * Both sides of a reimbursement link (link-reimbursements-to-expenses D8): on an expense "Diganti Rp X" with what
 * repaid it, on a reimbursement what it repays and what no expense holds. Each row opens that transaction.
 */
function ReimbursementView({ links, onOpenRow }: { links: ReimbursementLinks; onOpenRow: (id: string) => void }) {
  const t = useTranslations("transactions.detail.reimbursement");
  const money = useMoney();
  const repaidId = useId();
  const repaysId = useId();
  const unallocated = links.unallocated ?? 0;
  return (
    <>
      {links.repaid_by.length > 0 && (
        <section className="tx-settled" aria-labelledby={repaidId}>
          <h3 id={repaidId} className="t-label">{t.rich("repaidHeading", { amount: maskedTag(money(links.repaid_total)) })}</h3>
          <ul>{links.repaid_by.map((line) => <li key={line.transaction_id}><LinkRow line={line} onOpenRow={onOpenRow} /></li>)}</ul>
        </section>
      )}
      {links.repays.length > 0 && (
        <section className="tx-settled" aria-labelledby={repaysId}>
          <h3 id={repaysId} className="t-label">{t("repaysHeading")}</h3>
          <ul>
            {links.repays.map((line) => <li key={line.transaction_id}><LinkRow line={line} onOpenRow={onOpenRow} /></li>)}
            {unallocated > 0 && (
              <li>
                <div className="tx-settled-row is-static">
                  <span className="tx-settled-main">{t("notCounted")}</span>
                  <span className="tx-settled-amount"><MaskedMoney text={money(unallocated)} className="t-amt" /></span>
                </div>
              </li>
            )}
          </ul>
        </section>
      )}
    </>
  );
}

function LinkRow({ line, onOpenRow }: { line: ReimbursementLinkLine; onOpenRow: (id: string) => void }) {
  const t = useTranslations("transactions.detail.reimbursement");
  const money = useMoney();
  const format = useFormat();
  const labels = useRowLabels();
  const title = line.notes?.trim() || line.merchant || labels.undescribed;
  return (
    <button type="button" className="tx-settled-row" onClick={() => onOpenRow(line.transaction_id)}>
      <span className="tx-settled-main">
        <span className="tx-settled-title">{title}</span>
        <span className="tx-settled-meta">{format.shortDateAt(line.date)}</span>
      </span>
      <span className="tx-settled-amount">
        <MaskedMoney text={money(line.amount)} className="t-amt" />
        {line.expense_amount !== undefined && line.amount < line.expense_amount && (
          <span className="tx-settled-meta">{t.rich("shareOf", { amount: maskedTag(money(line.expense_amount)) })}</span>
        )}
      </span>
    </button>
  );
}

/** What a credit payment paid, from the server's schedule (show-credit-status-and-payment-settlement D9, D10). */
function PaymentSettlement({ credit, onOpenRow }: { credit: CreditBlock; onOpenRow: (id: string) => void }) {
  const t = useTranslations("transactions.detail.credit");
  const money = useMoney();
  const headingId = useId();
  const items = credit.settled_items ?? [];
  const kept = credit.kept_for_later ?? 0;
  if (items.length === 0 && kept === 0) return null;
  return (
    <section className="tx-settled" aria-labelledby={headingId}>
      <h3 id={headingId} className="t-label">{t("settledHeading")}</h3>
      <ul>
        {items.map((item) => (
          <li key={`${item.kind}-${item.transaction_id ?? "opening"}-${item.installment?.number ?? 1}`}>
            <SettledItem item={item} onOpenRow={onOpenRow} />
          </li>
        ))}
        {kept > 0 && (
          <li>
            <div className="tx-settled-row is-static">
              <span className="tx-settled-main">{t("keptForLater")}</span>
              <span className="tx-settled-amount"><MaskedMoney text={money(kept)} className="t-amt" /></span>
            </div>
          </li>
        )}
      </ul>
    </section>
  );
}

/** One settled item: a whole-row button opening its transaction; opening debt has none and stays a plain row. */
function SettledItem({ item, onOpenRow }: { item: CreditSettledItem; onOpenRow: (id: string) => void }) {
  const t = useTranslations("transactions.detail.credit");
  const kinds = useTranslations("accounts.detail.item");
  const money = useMoney();
  const format = useFormat();
  const labels = useRowLabels();
  const title = settledItemTitle(item, {
    opening: t("openingDebt"), fee: kinds("fee"), cashAdvance: kinds("cash_advance"), undescribed: labels.undescribed,
  });
  const date = format.shortDate(item.due_date);
  const installment = item.installment && item.installment.count > 1 ? item.installment : null;
  const content = (
    <>
      <span className="tx-settled-main">
        <span className="tx-settled-title">{title}</span>
        <span className="tx-settled-meta">
          {installment ? t("item", { k: String(installment.number), n: String(installment.count), date }) : t("itemDue", { date })}
        </span>
      </span>
      <span className="tx-settled-amount">
        <MaskedMoney text={money(item.amount)} className="t-amt" />
        {item.amount < item.item_amount && (
          <span className="tx-settled-meta">{t.rich("shareOf", { amount: maskedTag(money(item.item_amount)) })}</span>
        )}
      </span>
    </>
  );
  const transactionId = item.transaction_id;
  if (!transactionId) return <div className="tx-settled-row is-static">{content}</div>;
  return (
    <button type="button" className="tx-settled-row" onClick={() => onOpenRow(transactionId)}>
      {content}
      <Icon name="chevron-right" className="h-4 w-4 chev" />
    </button>
  );
}

/** An item by its own row's title; a cash advance carrying only the server's default note is named by its kind. */
function settledItemTitle(
  item: CreditSettledItem,
  names: { opening: string; fee: string; cashAdvance: string; undescribed: string },
): string {
  if (item.kind === "opening") return names.opening;
  if (item.kind === "cash_advance") return isPlaceholderNote(item.title) ? names.cashAdvance : (item.title as string);
  return item.title || (item.kind === "fee" ? names.fee : names.undescribed);
}

/** What a delete undoes, named before it happens (deviation 8). */
function DeleteEffects({ row, kind }: { row: LedgerRowDetail; kind: RowKind }) {
  const t = useTranslations("transactions.delete");
  const money = useMoney();
  const amount = (value: number) => maskedTag(money(value));
  const effects: ReactNode[] = [];
  if (kind === "topup") {
    effects.push(t.rich("topupValue", { amount: amount(row.amount) }), t("topupDebit"));
  } else if (kind === "movement" && row.movement_id) {
    const route = movementRoute(row);
    const sides = route.source && route.target ? t("accountsJoin", { first: route.source, second: route.target }) : route.source ?? route.target ?? row.account_name;
    effects.push(t("movementBalances", { accounts: sides }));
  } else {
    effects.push(row.type === "expense"
      ? t.rich("expenseBalance", { account: row.account_name, amount: amount(row.amount) })
      : t.rich("incomeBalance", { account: row.account_name, amount: amount(row.amount) }));
    for (const item of row.obligation_allocations ?? []) {
      effects.push(t.rich("debtAllocation", { name: item.obligation_name, amount: amount(item.amount) }));
    }
    if (!row.obligation_allocations?.length && row.obligation_name) {
      effects.push(row.type === "expense" ? t("debtPayment", { name: row.obligation_name }) : t("debtIncome", { name: row.obligation_name }));
    }
  }
  return (
    <ul>
      {effects.map((effect, index) => <li key={index}>{effect}</li>)}
    </ul>
  );
}
