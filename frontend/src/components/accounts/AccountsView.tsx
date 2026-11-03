"use client";

import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, fmtMoney } from "@/lib/utils";
import { replaceSearch } from "@/lib/urlState";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { ReconciliationFlag } from "@/components/ui/ReconciliationFlag";
import { Card, CardHeader, ErrorState, MoneyBar, Pill, Segmented, Skeleton } from "@/components/workspace/primitives";
import { Drawer } from "@/components/workspace/overlays";
import { ExplainableFigure, type ExplanationData } from "@/components/workspace/explain";
import { canSwitchToAmountTracking, listAmountInvestmentProducts } from "@/lib/investmentTopups";
import type { ReviewQueue } from "@/lib/review";
import { useAccountDialogs } from "./AccountDialogs";
import type { AccountItem, AccountsResponse, NetWorthResponse } from "./types";
import styles from "./money.module.css";

type Show = "semua" | "siap" | "investasi";
const SHOW_OPTIONS: { value: Show; label: string }[] = [
  { value: "semua", label: "Semua" },
  { value: "siap", label: "Siap pakai" },
  { value: "investasi", label: "Investasi" },
];

const KIND: Record<AccountItem["type"], string> = { bank: "Bank", wallet: "E-wallet", cash: "Tunai", investment: "Investasi" };

function isHolding(account: AccountItem): boolean {
  return account.type === "investment" || Boolean(account.instrument_type);
}

function isPosition(account: AccountItem): boolean {
  return !account.children?.length && Boolean(
    account.instrument_type || account.instrument_symbol || account.units != null || account.avg_buy_price != null || account.last_price != null,
  );
}

function parseShow(value: string | null): Show {
  return value === "siap" || value === "investasi" ? value : "semua";
}

function showHref(pathname: string, search: URLSearchParams, show: Show, account: string | null): string {
  const next = new URLSearchParams(search.toString());
  if (show === "semua") next.delete("tampil");
  else next.set("tampil", show);
  if (account) next.set("akun", account);
  else next.delete("akun");
  const query = next.toString();
  return query ? `${pathname}?${query}` : pathname;
}

export function AccountsView() {
  const qc = useQueryClient();
  const { bal } = useAppCtx();
  const pathname = usePathname();
  const search = useSearchParams();
  const show = parseShow(search.get("tampil"));
  const openId = search.get("akun");
  const dialogs = useAccountDialogs();
  const reorderButton = useRef<HTMLButtonElement>(null);
  const [reordering, setReordering] = useState(false);
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const accountsQuery = useQuery<AccountsResponse>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });
  const netWorthQuery = useQuery<NetWorthResponse>({ queryKey: queryKeys.dashboard.netWorth, queryFn: () => api.get("/dashboard/net-worth") });
  const queueQuery = useQuery<ReviewQueue>({ queryKey: queryKeys.review.queue, queryFn: () => api.get("/review/queue") });

  const accounts = useMemo(() => (accountsQuery.data?.accounts ?? []).filter((account) => !account.is_archived), [accountsQuery.data]);
  const byId = useMemo(() => {
    const map = new Map<string, AccountItem>();
    for (const account of accounts) {
      map.set(account.id, account);
      for (const pocket of account.children ?? []) map.set(pocket.id, pocket);
    }
    return map;
  }, [accounts]);
  const amountProducts = useMemo(() => new Set(listAmountInvestmentProducts(accountsQuery.data?.accounts ?? []).map((item) => item.id)), [accountsQuery.data]);
  const top = accounts.filter((account) => !account.parent_id);
  const liquid = top.filter((account) => !isHolding(account));
  const holdings = top.filter((account) => isHolding(account));
  // Awaiting outflows per account, counted from the queue; amounts stay in Tinjau (OD59).
  const awaiting = useMemo(() => {
    const counts = new Map<string, number>();
    for (const item of queueQuery.data?.awaiting ?? []) counts.set(item.account_id, (counts.get(item.account_id) ?? 0) + 1);
    return counts;
  }, [queueQuery.data]);
  const awaitingTotal = queueQuery.data?.awaiting?.length ?? 0;

  const setShow = (value: Show) => replaceSearch(showHref(pathname, search, value, openId));
  const openAccount = (id: string | null) => replaceSearch(showHref(pathname, search, show, id));
  const selected = openId ? byId.get(openId) ?? null : null;

  // --- Reorder (moved from the page; same request bodies) ------------------------------------
  const [order, setOrder] = useState<Record<string, string[]>>({});
  const [orderStatus, setOrderStatus] = useState("");
  const [orderError, setOrderError] = useState("");
  const [dragged, setDragged] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState<string | null>(null);
  const reorderMutation = useMutation({
    mutationFn: async (accountIds: string[]) => api.post("/accounts/reorder", { account_ids: accountIds }),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: queryKeys.accounts });
      setOrder({});
      setOrderError("");
      setOrderStatus("Urutan rekening tersimpan.");
    },
    onError: () => {
      setOrder({});
      setOrderError("Urutan gagal disimpan. Coba lagi.");
      setOrderStatus("");
    },
  });
  const ordered = (list: AccountItem[], key: string) => {
    const ids = order[key];
    if (!ids) return list;
    return [...list].sort((left, right) => ids.indexOf(left.id) - ids.indexOf(right.id));
  };
  const move = (list: AccountItem[], key: string, id: string, destination: number) => {
    const ids = list.map((item) => item.id);
    const source = ids.indexOf(id);
    if (source < 0 || destination < 0 || destination >= ids.length || source === destination) return;
    ids.splice(source, 1);
    ids.splice(destination, 0, id);
    setOrderError("");
    setOrderStatus("");
    setOrder((current) => ({ ...current, [key]: ids }));
    reorderMutation.mutate(ids);
  };
  const finishReorder = () => {
    setReordering(false);
    requestAnimationFrame(() => reorderButton.current?.focus());
  };

  useEffect(() => {
    if (openId && accountsQuery.data && !byId.has(openId)) openAccount(null);
    // Only a stale ?akun= is cleared; the handler itself is stable enough for this check.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openId, accountsQuery.data, byId]);

  const header = (
    <div className={styles.toolbar}>
      <Segmented label="Tampilkan" options={SHOW_OPTIONS} value={show} onChange={setShow} />
      <span className={styles.grow} />
      <button ref={reorderButton} type="button" className="ws-btn" aria-pressed={reordering} onClick={() => (reordering ? finishReorder() : setReordering(true))}>
        <Icon name="arrow-up-down" className="h-4 w-4" />
        {reordering ? "Selesai" : "Ubah urutan"}
      </button>
      <button type="button" className="ws-btn" onClick={() => dialogs.syncPricesMutation.mutate()} disabled={dialogs.syncPricesMutation.isPending}>
        <Icon name="refresh-cw" className="h-4 w-4" />
        {dialogs.syncPricesMutation.isPending ? "Memperbarui harga…" : "Perbarui harga"}
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openPlans}>
        <Icon name="calendar" className="h-4 w-4" />
        Rencana
      </button>
      <button type="button" className="ws-btn ws-btn-ink" onClick={dialogs.openNewAccount}>
        <Icon name="plus" className="h-4 w-4" />
        Tambah rekening
      </button>
    </div>
  );

  const status = (
    <>
      <p className="sr-only" role="status">{dialogs.syncMessage ?? orderStatus}</p>
      {dialogs.syncMessage && <p className={styles.notice} aria-hidden="true">{dialogs.syncMessage}</p>}
      {(orderError || dialogs.accountActionError) && <p className={styles.alert} role="alert">{orderError || dialogs.accountActionError}</p>}
    </>
  );

  if (accountsQuery.isLoading) {
    return (
      <div className={styles.root}>
        {header}
        <div className={styles.grid}>
          <div className={styles.stack}><Skeleton style={{ height: 320 }} /><Skeleton style={{ height: 240 }} /></div>
          <Skeleton style={{ height: 360 }} />
        </div>
        {dialogs.dialogs}
      </div>
    );
  }
  if (accountsQuery.isError) {
    return (
      <div className={styles.root}>
        {header}
        <ErrorState message="Daftar rekening gagal dimuat." onRetry={() => accountsQuery.refetch()} />
        {dialogs.dialogs}
      </div>
    );
  }

  const summary = (
    <TotalAssets
      query={netWorthQuery}
      money={bal}
      awaitingTotal={awaitingTotal}
    />
  );

  const liquidCard = (show === "semua" || show === "siap") && (
    <Card labelledBy="accounts-liquid">
      <CardHeader id="accounts-liquid" icon="wallet" title="Uang siap pakai" extra={<span className={styles.headTotal}>{netWorthQuery.data?.liquid != null ? bal(netWorthQuery.data.liquid) : null}</span>} />
      {liquid.length === 0 ? (
        <p className="ws-caption">Belum ada rekening bank, e-wallet atau tunai.</p>
      ) : (
        <table className={cn("ws-tbl", styles.table)}>
          <caption className="sr-only">Rekening dan kantong</caption>
          <thead>
            <tr><th scope="col">Rekening</th><th scope="col" className={styles.kindCol}>Jenis</th><th scope="col" className="r">Saldo</th></tr>
          </thead>
          <tbody>
            {ordered(liquid, "top").map((account) => {
              const pockets = ordered(account.children ?? [], account.id);
              const open = !collapsed[account.id];
              const flagged = pockets.filter((pocket) => pocket.reconciliation_required).length;
              return (
                <Fragment key={account.id}>
                  <AccountRow
                    account={account}
                    kind={KIND[account.type]}
                    secondary={[
                      pockets.length ? `${pockets.length} kantong` : null,
                      awaiting.get(account.id) ? `${awaiting.get(account.id)} pindah saldo belum lengkap` : null,
                    ]}
                    money={bal}
                    onOpen={() => openAccount(account.id)}
                    flag={account.reconciliation_required ? <ReconciliationFlag name={account.name} onReconcile={!pockets.length && account.investment_tracking_mode !== "amount" ? () => dialogs.openReconciliation(account) : undefined} /> : flagged ? <ReconciliationFlag name={account.name} pocketCount={flagged} /> : null}
                    disclosure={pockets.length ? (
                      <button
                        type="button"
                        className={styles.disclosure}
                        aria-expanded={open}
                        aria-label={`${open ? "Tutup" : "Buka"} kantong ${account.name}`}
                        onClick={() => setCollapsed((current) => ({ ...current, [account.id]: open }))}
                      >
                        <Icon name={open ? "chevron-down" : "chevron-right"} className="h-4 w-4" />
                      </button>
                    ) : null}
                  />
                  {open && pockets.map((pocket) => (
                    <AccountRow
                      key={pocket.id}
                      account={pocket}
                      pocketOf={account.name}
                      kind={isHolding(pocket) ? "Investasi" : "Kantong"}
                      secondary={[`Kantong di ${account.name}`, awaiting.get(pocket.id) ? `${awaiting.get(pocket.id)} pindah saldo belum lengkap` : null]}
                      money={bal}
                      onOpen={() => openAccount(pocket.id)}
                      flag={pocket.reconciliation_required ? <ReconciliationFlag name={pocket.name} onReconcile={pocket.investment_tracking_mode !== "amount" ? () => dialogs.openReconciliation(pocket) : undefined} /> : null}
                    />
                  ))}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      )}
    </Card>
  );

  const investmentCard = (show === "semua" || show === "investasi") && (
    <Card labelledBy="accounts-investment">
      <CardHeader id="accounts-investment" icon="trending-up" title="Investasi" extra={<span className={styles.headTotal}>{netWorthQuery.data?.investment != null ? bal(netWorthQuery.data.investment) : null}</span>} />
      {holdings.length === 0 ? (
        <p className="ws-caption">Belum ada produk investasi.</p>
      ) : (
        <table className={cn("ws-tbl", styles.table)}>
          <caption className="sr-only">Produk investasi</caption>
          <thead>
            <tr><th scope="col">Produk</th><th scope="col" className={styles.kindCol}>Cara hitung</th><th scope="col" className="r">Nilai</th></tr>
          </thead>
          {ordered(holdings, "top").map((platform) => {
            const products = ordered(platform.children ?? [], platform.id);
            if (!products.length) {
              const standalone = isPosition(platform) || platform.investment_tracking_mode === "amount" || amountProducts.has(platform.id);
              return (
                <tbody key={platform.id}>
                  <AccountRow
                    account={platform}
                    kind={trackingPill(platform, amountProducts)}
                    secondary={[standalone ? trackingText(platform, amountProducts) : "Belum ada produk"]}
                    money={bal}
                    onOpen={() => openAccount(platform.id)}
                    flag={platform.reconciliation_required ? <ReconciliationFlag name={platform.name} /> : null}
                  />
                </tbody>
              );
            }
            return (
              <tbody key={platform.id}>
                <tr className={styles.groupRow}>
                  <th colSpan={3} scope="rowgroup">
                    <button type="button" className="ws-rowbtn" onClick={() => openAccount(platform.id)}>
                      {platform.name}
                    </button>
                    <span className="ws-caption"> · {products.length} produk · {bal(platform.balance)}</span>
                  </th>
                </tr>
                {products.map((product) => (
                  <AccountRow
                    key={product.id}
                    account={product}
                    pocketOf={platform.name}
                    kind={isHolding(product) ? trackingPill(product, amountProducts) : "Kantong kas"}
                    secondary={[isHolding(product) ? trackingText(product, amountProducts) : `Kantong di ${platform.name}`]}
                    money={bal}
                    onOpen={() => openAccount(product.id)}
                    flag={product.reconciliation_required ? <ReconciliationFlag name={product.name} onReconcile={!isHolding(product) ? () => dialogs.openReconciliation(product) : undefined} /> : null}
                  />
                ))}
              </tbody>
            );
          })}
        </table>
      )}
      <p className="ws-caption">Reksa dana bertambah per nominal setiap top up. Saham dihitung dari lot × harga terakhir, sehingga nilainya bisa berubah tanpa transaksi.</p>
    </Card>
  );

  return (
    <div className={styles.root}>
      {header}
      {status}
      <div className={styles.grid}>
        <div className={styles.summaryFirst}>{summary}</div>
        <div className={styles.stack}>
          {reordering ? (
            <Card labelledBy="accounts-reorder">
              <CardHeader id="accounts-reorder" icon="arrow-up-down" title="Ubah urutan" caption="Seret baris atau pakai tombol naik dan turun. Urutan langsung tersimpan." />
              <ReorderList
                rows={ordered(top, "top")}
                childrenOf={(account) => ordered(account.children ?? [], account.id)}
                move={move}
                pending={reorderMutation.isPending}
                dragged={dragged}
                dragOver={dragOver}
                setDragged={setDragged}
                setDragOver={setDragOver}
              />
            </Card>
          ) : (
            <>
              {liquidCard}
              {investmentCard}
            </>
          )}
        </div>
      </div>
      <AccountDrawer
        account={selected}
        parent={selected?.parent_id ? byId.get(selected.parent_id) ?? null : null}
        money={bal}
        amountProducts={amountProducts}
        dialogs={dialogs}
        onClose={() => openAccount(null)}
      />
      {dialogs.dialogs}
    </div>
  );
}

function trackingText(account: AccountItem, amountProducts: Set<string>): string {
  if (amountProducts.has(account.id) || account.investment_tracking_mode === "amount") return "Dicatat per nominal";
  if (account.instrument_type === "stock" && account.units) {
    const lots = account.units / 100;
    return `${lots.toLocaleString("id-ID")} lot × 100 lembar${account.last_price ? ` × ${fmtMoney(account.last_price)}` : ""}`;
  }
  if (account.units) return `${account.units.toLocaleString("id-ID")} unit`;
  return "Dicatat per nominal";
}

function trackingPill(account: AccountItem, amountProducts: Set<string>): string {
  if (account.instrument_type === "stock") return "Saham";
  if (account.instrument_type === "mutual_fund") return amountProducts.has(account.id) || account.investment_tracking_mode === "amount" ? "Reksa dana · nominal" : "Reksa dana · unit";
  if (account.instrument_type === "gold") return "Emas";
  if (account.instrument_type === "crypto") return "Kripto";
  if (account.instrument_type === "deposit") return "Deposito";
  return "Investasi";
}

function AccountRow({
  account,
  pocketOf,
  kind,
  secondary,
  money,
  onOpen,
  flag,
  disclosure,
}: {
  account: AccountItem;
  pocketOf?: string;
  kind: string;
  secondary: (string | null)[];
  money: (value: number) => string;
  onOpen: () => void;
  flag?: React.ReactNode;
  disclosure?: React.ReactNode;
}) {
  const lines = secondary.filter(Boolean);
  return (
    <tr className={cn(pocketOf && styles.pocketRow)} data-account-row={account.id}>
      <td>
        <div className={styles.who}>
          {disclosure ?? (pocketOf ? null : <span className={styles.disclosureSpace} aria-hidden="true" />)}
          <span className={styles.mark} style={{ background: account.color || "var(--muted)" }} aria-hidden="true" />
          <div className="ws-who-t">
            <button type="button" className="ws-rowbtn" onClick={onOpen} aria-label={pocketOf ? `${account.name}, kantong di ${pocketOf}` : account.name}>
              {account.name}
            </button>
            {lines.length > 0 && <span>{lines.join(" · ")}</span>}
            <span className={styles.phoneKind}>{kind}</span>
            {flag && <div className={styles.flag}>{flag}</div>}
          </div>
        </div>
      </td>
      <td className={styles.kindCol}><Pill>{kind}</Pill></td>
      <td className={cn("r", styles.amount)}>{money(account.balance)}</td>
    </tr>
  );
}

function ReorderList({
  rows,
  childrenOf,
  move,
  pending,
  dragged,
  dragOver,
  setDragged,
  setDragOver,
}: {
  rows: AccountItem[];
  childrenOf: (account: AccountItem) => AccountItem[];
  move: (list: AccountItem[], key: string, id: string, destination: number) => void;
  pending: boolean;
  dragged: string | null;
  dragOver: string | null;
  setDragged: (id: string | null) => void;
  setDragOver: (id: string | null) => void;
}) {
  const moveTop = (id: string, step: number) => move(rows, "top", id, rows.findIndex((row) => row.id === id) + step);
  const dropTop = (target: AccountItem) => {
    if (!dragged || dragged === target.id || !rows.some((row) => row.id === dragged)) return;
    move(rows, "top", dragged, rows.findIndex((row) => row.id === target.id));
  };
  return (
    <ul className={styles.reorder} aria-label="Ubah urutan rekening">
      {rows.map((account, index) => {
        const pockets = childrenOf(account);
        return (
          <li
            key={account.id}
            className={cn(styles.reorderItem, dragOver === account.id && styles.dropTarget, dragged === account.id && styles.dragging)}
            onDragOver={(event) => { if (dragged && rows.some((row) => row.id === dragged)) { event.preventDefault(); setDragOver(account.id); } }}
            onDragLeave={() => setDragOver(null)}
            onDrop={(event) => { event.preventDefault(); dropTop(account); setDragged(null); setDragOver(null); }}
          >
            <div className={styles.reorderRow}>
              <span
                className={styles.handle}
                draggable
                aria-hidden="true"
                onDragStart={(event) => { setDragged(account.id); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", account.id); }}
                onDragEnd={() => { setDragged(null); setDragOver(null); }}
              >
                <Icon name="grip-vertical" className="h-4 w-4" />
              </span>
              <span className={styles.reorderName}>{account.name}{dragged === account.id ? " (dipindah)" : ""}</span>
              <MoveButtons name={account.name} first={index === 0} last={index === rows.length - 1} pending={pending} onMove={(step) => moveTop(account.id, step)} />
            </div>
            {pockets.length > 0 && (
              <ul className={styles.reorderPockets} aria-label={`Ubah urutan kantong ${account.name}`}>
                {pockets.map((pocket, pocketIndex) => (
                  <li
                    key={pocket.id}
                    className={cn(styles.reorderRow, dragOver === pocket.id && styles.dropTarget, dragged === pocket.id && styles.dragging)}
                    onDragOver={(event) => { if (dragged && pockets.some((item) => item.id === dragged)) { event.preventDefault(); event.stopPropagation(); setDragOver(pocket.id); } }}
                    onDrop={(event) => {
                      if (!dragged || !pockets.some((item) => item.id === dragged)) return;
                      event.preventDefault();
                      event.stopPropagation();
                      if (dragged !== pocket.id) move(pockets, account.id, dragged, pocketIndex);
                      setDragged(null);
                      setDragOver(null);
                    }}
                  >
                    <span
                      className={styles.handle}
                      draggable
                      aria-hidden="true"
                      onDragStart={(event) => { event.stopPropagation(); setDragged(pocket.id); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", pocket.id); }}
                      onDragEnd={(event) => { event.stopPropagation(); setDragged(null); setDragOver(null); }}
                    >
                      <Icon name="grip-vertical" className="h-4 w-4" />
                    </span>
                    <span className={styles.reorderName}>{pocket.name}{dragged === pocket.id ? " (dipindah)" : ""}</span>
                    <MoveButtons name={pocket.name} first={pocketIndex === 0} last={pocketIndex === pockets.length - 1} pending={pending} onMove={(step) => move(pockets, account.id, pocket.id, pocketIndex + step)} />
                  </li>
                ))}
              </ul>
            )}
          </li>
        );
      })}
    </ul>
  );
}

function MoveButtons({ name, first, last, pending, onMove }: { name: string; first: boolean; last: boolean; pending: boolean; onMove: (step: number) => void }) {
  return (
    <span className={styles.moveButtons}>
      <button type="button" className="ws-icon-btn" aria-label={`Naikkan ${name}`} disabled={first || pending} onClick={() => onMove(-1)}>
        <Icon name="chevron-up" className="h-4 w-4" />
      </button>
      <button type="button" className="ws-icon-btn" aria-label={`Turunkan ${name}`} disabled={last || pending} onClick={() => onMove(1)}>
        <Icon name="chevron-down" className="h-4 w-4" />
      </button>
    </span>
  );
}

function TotalAssets({
  query,
  money,
  awaitingTotal,
}: {
  query: { data?: NetWorthResponse; isLoading: boolean; isError: boolean; refetch: () => unknown };
  money: (value: number) => string;
  awaitingTotal: number;
}) {
  if (query.isLoading) return <Card labelledBy="accounts-total"><h2 id="accounts-total" className={styles.plainTitle}>Total aset</h2><Skeleton style={{ height: 220 }} /></Card>;
  const data = query.data;
  if (query.isError || !data) {
    return (
      <Card labelledBy="accounts-total">
        <h2 id="accounts-total" className={styles.plainTitle}>Total aset</h2>
        <ErrorState message="Kekayaan bersih belum dapat dihitung." onRetry={() => query.refetch()} />
      </Card>
    );
  }
  const share = (part: number) => (data.total_assets > 0 ? Math.round((Math.max(0, part) / data.total_assets) * 100) : 0);
  // An API from before the shared net worth rule has no lines; the facts then show only the totals.
  const lines = data.lines ?? [];
  const assetLines = lines.filter((line) => line.key === "liquid" || line.key === "investment");
  const owedLines = lines.filter((line) => line.key !== "liquid" && line.key !== "investment");
  const assetsExplanation: ExplanationData = {
    title: "Total aset",
    rule: "Uang siap pakai ditambah investasi. Saldo kantong dihitung sekali, tidak dihitung dua kali dengan rekening induknya.",
    lines: assetLines.map((line) => ({ label: line.key === "liquid" ? "Uang siap pakai" : "Investasi", amount: money(line.amount) })),
    total: { label: "Total aset", amount: money(data.total_assets) },
  };
  const netExplanation: ExplanationData = {
    title: "Kekayaan bersih",
    rule: "Total aset dikurangi saldo minus yang perlu dicocokkan dan sisa utang aktif. Angka yang sama dengan Beranda.",
    lines: lines.map((line) => ({ label: line.key === "liquid" ? "Uang siap pakai" : line.key === "investment" ? "Investasi" : line.label, amount: money(line.amount) })),
    total: { label: "Kekayaan bersih", amount: money(data.net_worth) },
  };
  return (
    <Card labelledBy="accounts-total">
      <h2 id="accounts-total" className={styles.plainTitle}>Total aset</h2>
      <div className="ws-bigline">
        <ExplainableFigure label="Total aset" amountText={money(data.total_assets)} explanation={assetsExplanation} />
      </div>
      <div className={styles.bar}>
        <MoneyBar
          label="Komposisi aset"
          segments={[
            { key: "liquid", label: `Siap pakai · ${share(data.liquid ?? 0)}%`, amount: Math.max(0, data.liquid ?? 0), display: money(data.liquid ?? 0), className: styles.segLiquid },
            { key: "investment", label: `Investasi · ${share(data.investment ?? 0)}%`, amount: Math.max(0, data.investment ?? 0), display: money(data.investment ?? 0), className: styles.segInvest },
          ]}
        />
      </div>
      <dl className={styles.facts}>
        <dt>Total aset</dt>
        <dd>{money(data.total_assets)}</dd>
        {owedLines.map((line) => (
          <Fragment key={line.key}>
            <dt>{line.key === "reconciliation_pending" ? line.label : `Utang ${line.label}`}</dt>
            <dd>{money(line.amount)}</dd>
          </Fragment>
        ))}
        <dt className={styles.strong}>Kekayaan bersih</dt>
        <dd className={styles.strong}>
          <ExplainableFigure label="Kekayaan bersih" amountText={money(data.net_worth)} explanation={netExplanation} />
        </dd>
      </dl>
      <p className="ws-caption">Saldo kantong dihitung sekali, tidak dihitung dua kali dengan rekening induknya.</p>
      {awaitingTotal > 0 && (
        <p className="ws-caption">
          {awaitingTotal} pindah saldo belum lengkap, jadi tujuannya belum masuk aset mana pun.{" "}
          <Link href="/review?folder=pindah" className="ws-linkbtn">Buka Tinjau</Link>
        </p>
      )}
      <p className={styles.linkRow}><Link href="/goals" className="ws-linkbtn">Lihat Ketahanan Dana</Link></p>
    </Card>
  );
}

function AccountDrawer({
  account,
  parent,
  money,
  amountProducts,
  dialogs,
  onClose,
}: {
  account: AccountItem | null;
  parent: AccountItem | null;
  money: (value: number) => string;
  amountProducts: Set<string>;
  dialogs: ReturnType<typeof useAccountDialogs>;
  onClose: () => void;
}) {
  if (!account) return null;
  const pockets = account.children ?? [];
  const holding = isHolding(account);
  const position = isPosition(account);
  const amountTracked = amountProducts.has(account.id);
  const canReconcile = !pockets.length && account.investment_tracking_mode !== "amount";
  const isPocket = Boolean(account.parent_id);
  const kind = isPocket ? (holding ? "Produk investasi" : "Kantong") : KIND[account.type];
  return (
    <Drawer open onClose={onClose} title={account.name} subtitle={parent ? `${kind} di ${parent.name}` : kind}>
      <div className={styles.drawerBody}>
        <div className={styles.drawerBalance}>
          <span className="ws-caption">{holding ? "Nilai" : "Saldo"}</span>
          <b>{money(account.balance)}</b>
        </div>
        {account.reconciliation_required && (
          <ReconciliationFlag name={account.name} onReconcile={canReconcile ? () => dialogs.openReconciliation(account) : undefined} />
        )}
        <dl className={styles.drawerFacts}>
          {account.instrument_symbol && (<><dt>Kode</dt><dd>{account.instrument_symbol}</dd></>)}
          {holding && (<><dt>Cara hitung</dt><dd>{trackingText(account, amountProducts)}</dd></>)}
          {account.investment_tracking_mode === "amount" && (<><dt>Modal</dt><dd>{account.cost_basis == null ? "Belum diisi" : money(account.cost_basis)}{account.investment_value_estimated ? " · nilai estimasi" : ""}</dd></>)}
          {account.units != null && account.investment_tracking_mode !== "amount" && (<><dt>Jumlah</dt><dd>{account.units.toLocaleString("id-ID")} {account.instrument_type === "stock" ? `lembar (${(account.units / 100).toLocaleString("id-ID")} lot)` : "unit"}</dd></>)}
          {account.last_price != null && (<><dt>Harga terakhir</dt><dd>{fmtMoney(account.last_price)}</dd></>)}
          {account.avg_buy_price != null && (<><dt>Modal beli rata-rata</dt><dd>{fmtMoney(account.avg_buy_price)}</dd></>)}
          {account.default_funding_account_name && (<><dt>Rekening dana</dt><dd>{account.default_funding_account_name}</dd></>)}
          {pockets.length > 0 && (<><dt>Kantong</dt><dd>{pockets.length}</dd></>)}
        </dl>
        {account.capital_gain != null && (
          <p className={styles.performance}>
            Untung/rugi: {account.capital_gain >= 0 ? "+" : ""}{money(account.capital_gain)} ({account.capital_gain_pct ?? 0}%)
          </p>
        )}
        <div className={styles.drawerActions} role="group" aria-label={`Tindakan ${account.name}`}>
          {position && amountTracked ? (
            <button type="button" className="ws-btn ws-btn-ink" onClick={() => dialogs.openTopup(account)}>Top up</button>
          ) : position ? (
            <>
              <button type="button" className="ws-btn ws-btn-ink" onClick={() => dialogs.openTrade(account, "buy")}>Beli</button>
              <button type="button" className="ws-btn" onClick={() => dialogs.openTrade(account, "sell")}>Jual</button>
            </>
          ) : !isPocket ? (
            <button type="button" className="ws-btn ws-btn-ink" onClick={() => dialogs.openNewPocket(account)}>
              <Icon name="plus" className="h-4 w-4" />
              Tambah kantong
            </button>
          ) : null}
          {canReconcile && <button type="button" className="ws-btn" onClick={() => dialogs.openReconciliation(account)}>Sesuaikan Saldo</button>}
          {position && <button type="button" className="ws-btn" onClick={() => dialogs.openValuation(account)}>Perbarui nilai</button>}
          {canSwitchToAmountTracking(account) && <button type="button" className="ws-btn" onClick={() => dialogs.openAmountSwitch(account)}>Ubah ke pelacakan nominal</button>}
          <button type="button" className="ws-btn" onClick={() => dialogs.openEditAccount(account)}>{isPocket ? "Ubah kantong" : "Ubah rekening"}</button>
          <ConfirmActionButton
            label={`Arsipkan ${isPocket ? "kantong" : "rekening"} ${account.name}`}
            confirmation={`Arsipkan ${isPocket ? "kantong" : "rekening"} "${account.name}"${pockets.length ? " beserta seluruh kantong di dalamnya" : ""}?`}
            onConfirm={() => { dialogs.archiveMutation.mutate(account.id); onClose(); }}
            className="ws-btn ws-btn-danger"
          >
            {isPocket ? "Arsipkan kantong" : "Arsipkan rekening"}
          </ConfirmActionButton>
        </div>
      </div>
    </Drawer>
  );
}
