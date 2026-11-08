"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { fmtMoney } from "@/lib/utils";
import { Icon } from "@/components/ui/Icon";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { Modal } from "@/components/ui/Modal";
import { ReconciliationFlag } from "@/components/ui/ReconciliationFlag";
import { canSwitchToAmountTracking } from "@/lib/investmentTopups";
import { RowAmount, movementDirection, rowKind, rowTitle } from "@/components/transactions/rows";
import { createFormat } from "@/lib/format";
import { INDONESIAN_ROW_LABELS } from "@/lib/untranslatedRowLabels";

/** Rekening is translated in phase 8; its detail keeps Indonesian words and dates until then. */
const INDONESIAN_FORMAT = createFormat({ language: "id", region: "ID" });

function rowWhen(date: string): string {
  return `${INDONESIAN_FORMAT.shortDateAt(date)} · ${INDONESIAN_FORMAT.time(date)}`;
}
import type { ActivityRow } from "@/components/beranda/types";
import { AccountLinks } from "./AccountLinks";
import type { useAccountDialogs } from "./AccountDialogs";
import { BalanceCheckDialog, canCheckBalance } from "./BalanceCheckDialog";
import { KIND, initials, isCredit, isHolding, isPosition, owedOn, trackingText } from "./accountFacts";
import { badgeInk } from "./displayColours";
import { roleInfo, roleOf } from "./wallet";
import type { AccountItem } from "./types";
import styles from "./money.module.css";

type Money = (value: number) => string;

function GainLine({ gain, pct, money }: { gain: number | null | undefined; pct: number | null | undefined; money: Money }) {
  if (gain == null) return <span className={styles.gainUnknown}>Modal belum diisi</span>;
  const up = gain >= 0;
  return (
    <span className={up ? styles.gainUp : styles.gainDown}>
      <span aria-hidden="true">{up ? "▲" : "▼"} </span>
      {up ? "+" : "−"}
      {money(Math.abs(gain))}
      {pct != null ? ` (${up ? "+" : "−"}${Math.abs(pct).toLocaleString("id-ID", { maximumFractionDigits: 2 })}%)` : ""}
      <span className="sr-only">{up ? " untung" : " rugi"}</span>
    </span>
  );
}

/** The three headline facts of the detail, as in the approved preview: value and gain for a product, pockets for a
    parent, and use and parent for a plain account or pocket. */
function detailFacts(account: AccountItem, parent: AccountItem | null, money: Money): { label: string; value: React.ReactNode }[] {
  const pockets = account.children ?? [];
  if (isCredit(account)) {
    const overpaid = account.balance > 0;
    return [
      { label: overpaid ? "Lebih bayar" : "Utang", value: money(overpaid ? account.balance : owedOn(account)) },
      { label: "Limit", value: account.credit_limit ? money(account.credit_limit) : "Tanpa limit" },
      { label: "Tanggal bayar", value: account.payment_day ? `Tiap tgl ${account.payment_day}` : "—" },
    ];
  }
  if (isHolding(account) && !pockets.length) {
    const cost = account.cost_basis ?? (account.capital_gain != null ? account.balance - account.capital_gain : null);
    return [
      { label: "Nilai sekarang", value: money(account.balance) },
      { label: "Modal", value: cost == null ? "Belum diisi" : money(cost) },
      { label: "Untung/rugi", value: <GainLine gain={account.capital_gain} pct={account.capital_gain_pct} money={money} /> },
    ];
  }
  if (isHolding(account)) {
    const gain = account.group_capital_gain;
    return [
      { label: "Nilai", value: money(account.balance) },
      { label: "Produk", value: String(pockets.length) },
      { label: "Untung/rugi", value: <GainLine gain={gain} pct={gain != null && account.group_cost_basis ? (gain / account.group_cost_basis) * 100 : null} money={money} /> },
    ];
  }
  if (pockets.length) {
    return [
      { label: "Saldo", value: money(account.balance) },
      { label: "Kantong", value: String(pockets.length) },
      { label: "Kantong utama", value: pockets[0].name },
    ];
  }
  return [
    { label: "Saldo", value: money(account.balance) },
    { label: "Kegunaan", value: roleInfo(roleOf(account)).label },
    { label: "Induk", value: parent ? parent.name : "—" },
  ];
}

const RECENT_LIMIT = 4;

/** The detail of an account, pocket or product: a centered dialog, a bottom sheet on phones (refine-workspace-layout D7). */
export function AccountDetail({
  account,
  parent,
  colour,
  money,
  amountProducts,
  dialogs,
  onClose,
  onOpenAccount,
}: {
  account: AccountItem | null;
  parent: AccountItem | null;
  colour: string;
  money: Money;
  amountProducts: Set<string>;
  dialogs: ReturnType<typeof useAccountDialogs>;
  onClose: () => void;
  onOpenAccount: (id: string) => void;
}) {
  // "Cocokkan saldo" belongs to the account on screen; another account starts without it.
  const [checkingId, setCheckingId] = useState<string | null>(null);
  const checking = Boolean(account && checkingId === account.id);
  const setChecking = (open: boolean) => setCheckingId(open && account ? account.id : null);
  const recentQuery = useQuery<{ transactions: ActivityRow[] }>({
    queryKey: ["transactions", "account", account?.id],
    queryFn: () => api.get(`/transactions?account_id=${account!.id}&limit=${RECENT_LIMIT}`),
    enabled: Boolean(account),
  });
  if (!account) return null;
  const pockets = account.children ?? [];
  const checkable = canCheckBalance(account);
  const holding = isHolding(account);
  const position = isPosition(account);
  const amountTracked = amountProducts.has(account.id);
  const credit = isCredit(account);
  // "Sesuaikan Saldo" takes a positive cash balance; a credit account's owed amount is not one (add-credit-accounts D8).
  const canReconcile = !pockets.length && !credit && account.investment_tracking_mode !== "amount";
  const isPocket = Boolean(account.parent_id);
  const kind = isPocket ? (holding ? "Produk" : "Kantong") : KIND[account.type];
  const recent = recentQuery.data?.transactions ?? [];
  const units = account.units != null && account.investment_tracking_mode !== "amount";
  const hasMoreFacts = Boolean(account.instrument_symbol || (holding && !pockets.length) || units || account.last_price != null || account.avg_buy_price != null || account.default_funding_account_name);
  return (
    <Modal
      open
      onClose={onClose}
      title={account.name}
      subtitle={parent ? `${kind} di ${parent.name}` : kind}
      leading={<span className={styles.detailMark} style={{ background: colour, color: badgeInk(colour) }} aria-hidden="true">{initials(account.name)}</span>}
      wide
    >
      <div className={styles.drawerBody}>
        <dl className={styles.detailFacts}>
          {detailFacts(account, parent, money).map((fact) => (
            <div key={fact.label}>
              <dt>{fact.label}</dt>
              <dd>{fact.value}</dd>
            </div>
          ))}
        </dl>
        {credit && account.over_limit && (
          <p className="ws-pill ws-tone-warn self-start">
            <Icon name="alert-triangle" className="h-3.5 w-3.5" />
            Melewati limit
          </p>
        )}
        {account.reconciliation_required && (
          <ReconciliationFlag name={account.name} onReconcile={canReconcile ? () => dialogs.openReconciliation(account) : undefined} />
        )}
        {hasMoreFacts && (
          <dl className={styles.drawerFacts}>
            {account.instrument_symbol && (<><dt>Kode</dt><dd>{account.instrument_symbol}</dd></>)}
            {holding && !pockets.length && (<><dt>Cara hitung</dt><dd>{trackingText(account, amountProducts)}{account.investment_value_estimated ? " · nilai estimasi" : ""}</dd></>)}
            {units && (<><dt>Jumlah</dt><dd>{account.units!.toLocaleString("id-ID")} {account.instrument_type === "stock" ? `lembar (${(account.units! / 100).toLocaleString("id-ID")} lot)` : "unit"}</dd></>)}
            {account.last_price != null && (<><dt>Harga terakhir</dt><dd>{fmtMoney(account.last_price)}</dd></>)}
            {account.avg_buy_price != null && (<><dt>Modal beli rata-rata</dt><dd>{fmtMoney(account.avg_buy_price)}</dd></>)}
            {account.default_funding_account_name && (<><dt>Rekening dana</dt><dd>{account.default_funding_account_name}</dd></>)}
          </dl>
        )}
        <AccountLinks accountId={account.id} accountName={account.name} />
        {pockets.length > 0 && (
          <section aria-labelledby={`detail-pockets-${account.id}`}>
            <h3 id={`detail-pockets-${account.id}`} className={styles.detailHeading}>{holding ? "Produk" : "Kantong"}</h3>
            <ul className={styles.detailRows}>
              {pockets.map((pocket) => (
                <li key={pocket.id}>
                  <i style={{ background: colour }} aria-hidden="true" />
                  <span>{pocket.name}</span>
                  <b>{money(pocket.balance)}</b>
                </li>
              ))}
            </ul>
          </section>
        )}
        <section aria-labelledby={`detail-recent-${account.id}`}>
          <h3 id={`detail-recent-${account.id}`} className={styles.detailHeading}>Transaksi terakhir</h3>
          {recentQuery.isError && <p className="ws-caption">Transaksi terakhir belum bisa dimuat.</p>}
          {recentQuery.isSuccess && recent.length === 0 && <p className="ws-caption">Belum ada transaksi di {isPocket ? (holding ? "produk" : "kantong") : "rekening"} ini.</p>}
          {recent.length > 0 && (
            <ul className={styles.detailRows}>
              {recent.map((row) => (
                <li key={row.id}>
                  <span className={styles.detailTx}>
                    <span>{rowTitle(row, INDONESIAN_ROW_LABELS)}</span>
                    <small>{rowKind(row) === "movement" ? `${rowWhen(row.date)} · ${movementDirection(row, INDONESIAN_ROW_LABELS)}` : rowWhen(row.date)}</small>
                  </span>
                  <RowAmount row={row} money={money} />
                </li>
              ))}
            </ul>
          )}
        </section>
        <div className={styles.detailActions} role="group" aria-label={`Tindakan ${account.name}`}>
          {position && amountTracked ? (
            <button type="button" className="ws-btn ws-btn-accent" onClick={() => dialogs.openTopup(account)}>Top up</button>
          ) : position ? (
            <>
              <button type="button" className="ws-btn ws-btn-accent" onClick={() => dialogs.openTrade(account, "buy")}>Beli</button>
              <button type="button" className="ws-btn" onClick={() => dialogs.openTrade(account, "sell")}>Jual</button>
            </>
          ) : !isPocket && !credit ? (
            <button type="button" className="ws-btn ws-btn-accent" onClick={() => dialogs.openNewPocket(account)}>
              <Icon name="plus" className="h-4 w-4" />
              Tambah kantong
            </button>
          ) : null}
          {checkable && <button type="button" className="ws-btn" onClick={() => setChecking(true)}>Cocokkan saldo</button>}
          {canReconcile && <button type="button" className="ws-btn" onClick={() => dialogs.openReconciliation(account)}>Sesuaikan Saldo</button>}
          {position && <button type="button" className="ws-btn" onClick={() => dialogs.openValuation(account)}>Perbarui nilai</button>}
          {canSwitchToAmountTracking(account) && <button type="button" className="ws-btn" onClick={() => dialogs.openAmountSwitch(account)}>Ubah ke pelacakan nominal</button>}
          <button type="button" className="ws-btn" onClick={() => dialogs.openEditAccount(account)}>{isPocket ? "Ubah kantong" : "Ubah rekening"}</button>
          {parent && <button type="button" className="ws-btn" onClick={() => onOpenAccount(parent.id)}>Buka {parent.name}</button>}
          <ConfirmActionButton
            label={`Arsipkan ${isPocket ? "kantong" : "rekening"} ${account.name}`}
            confirmation={`Arsipkan ${isPocket ? "kantong" : "rekening"} "${account.name}"${pockets.length ? " beserta seluruh kantong di dalamnya" : ""}?`}
            onConfirm={() => { dialogs.archiveMutation.mutate(account.id); onClose(); }}
            className={styles.dangerLink}
          >
            Arsipkan
          </ConfirmActionButton>
        </div>
      </div>
      {checking && (
        <BalanceCheckDialog
          account={account}
          money={money}
          onClose={() => setChecking(false)}
          onAdjust={(actual, onReconciled) => dialogs.openReconciliation(account, { actual, onReconciled })}
        />
      )}
    </Modal>
  );
}
