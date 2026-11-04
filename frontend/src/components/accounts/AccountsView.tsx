"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { cn, fmtMoney } from "@/lib/utils";
import { replaceSearch } from "@/lib/urlState";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { ConfirmActionButton } from "@/components/ui/ConfirmActionButton";
import { Modal } from "@/components/ui/Modal";
import { ReconciliationFlag } from "@/components/ui/ReconciliationFlag";
import { Card, CardHeader, ErrorState, KpiCard, Skeleton } from "@/components/workspace/primitives";
import { TopBarControls } from "@/components/workspace/context";
import { ExplainableFigure, type ExplanationData } from "@/components/workspace/explain";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { canSwitchToAmountTracking, listAmountInvestmentProducts } from "@/lib/investmentTopups";
import type { ReviewQueue } from "@/lib/review";
import { usePathname, useSearchParams } from "next/navigation";
import { useAccountDialogs } from "./AccountDialogs";
import type { AccountItem, AccountsResponse, NetWorthResponse } from "./types";
import styles from "./money.module.css";

const KIND: Record<AccountItem["type"], string> = { bank: "Bank", wallet: "E-wallet", cash: "Tunai", investment: "Investasi" };
// Phones reorder in a mode: dragging would fight the page scroll (refine-workspace-layout D9).
const PHONE_WIDTH = 760;

function isHolding(account: AccountItem): boolean {
  return account.type === "investment" || Boolean(account.instrument_type);
}

function isPosition(account: AccountItem): boolean {
  return !account.children?.length && Boolean(
    account.instrument_type || account.instrument_symbol || account.units != null || account.avg_buy_price != null || account.last_price != null,
  );
}

/** The stored default of new accounts; such accounts get a distinct display hue (D7). Nothing is written. */
const DEFAULT_COLOURS = new Set(["#3b82f6", "#2563eb"]);
// Hues apart from each other and from the semantic colours; every one passes 3:1 as a mark on the surfaces.
const ACCOUNT_HUES = ["#2E7DF6", "#F08A24", "#16A34A", "#E0A800", "#7C6CF2", "#0FB5AE", "#E5484D", "#A855F7", "#64748B", "#D9467F"];

function hueOf(hex: string): number | null {
  const match = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  if (!match) return null;
  const [r, g, b] = match.slice(1).map((part) => parseInt(part, 16) / 255);
  const max = Math.max(r, g, b);
  const chroma = max - Math.min(r, g, b);
  if (chroma < 0.15) return null;
  const hue = max === r ? ((g - b) / chroma) % 6 : max === g ? (b - r) / chroma + 2 : (r - g) / chroma + 4;
  return (hue * 60 + 360) % 360;
}

/** Display colours of the top-level accounts: stored colours stay, default ones get a hue no other account is near. */
export function displayColours(accounts: AccountItem[]): Map<string, string> {
  const colours = new Map<string, string>();
  const stored = accounts.map((account) => account.color?.toLowerCase()).filter((colour): colour is string => Boolean(colour) && !DEFAULT_COLOURS.has(colour!));
  const taken = stored.map(hueOf).filter((hue): hue is number => hue !== null);
  const near = (hue: number | null) => hue !== null && taken.some((other) => Math.min(Math.abs(hue - other), 360 - Math.abs(hue - other)) < 25);
  const free = ACCOUNT_HUES.filter((colour) => !stored.includes(colour.toLowerCase()) && !near(hueOf(colour)));
  let next = 0;
  for (const account of accounts) {
    const stored = account.color?.toLowerCase();
    if (stored && !DEFAULT_COLOURS.has(stored)) colours.set(account.id, account.color!);
    else colours.set(account.id, free.length ? free[next++ % free.length] : ACCOUNT_HUES[next++ % ACCOUNT_HUES.length]);
  }
  return colours;
}

/** The badge ink with the higher WCAG contrast on an account colour (white or the dark text ink). */
function badgeInk(colour: string): string {
  const match = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(colour);
  if (!match) return "#FFFFFF";
  const [r, g, b] = match.slice(1).map((part) => {
    const value = parseInt(part, 16) / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return (1.05 / (luminance + 0.05)) >= ((luminance + 0.05) / 0.0536) ? "#FFFFFF" : "#0A0D14";
}

function initials(name: string): string {
  const words = name.trim().split(/\s+/);
  return (words.length > 1 ? words[0][0] + words[1][0] : name.slice(0, 2)).toUpperCase();
}

function GainLine({ gain, pct, money }: { gain: number | null | undefined; pct: number | null | undefined; money: (value: number) => string }) {
  if (gain == null) return <span className={styles.gainUnknown}>Modal belum dicatat</span>;
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

export function AccountsView() {
  const qc = useQueryClient();
  const { bal } = useAppCtx();
  const pathname = usePathname();
  const search = useSearchParams();
  const openId = search.get("akun");
  const dialogs = useAccountDialogs();
  const rootRef = useRef<HTMLDivElement>(null);
  const phone = useWidthAtMost(rootRef, PHONE_WIDTH);
  const reorderButton = useRef<HTMLButtonElement>(null);
  const [reordering, setReordering] = useState(false);

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
  const colours = useMemo(() => displayColours(top), [top]);
  // Awaiting outflows per account, counted from the queue; amounts stay in Tinjau (OD59).
  const awaiting = useMemo(() => {
    const counts = new Map<string, number>();
    for (const item of queueQuery.data?.awaiting ?? []) counts.set(item.account_id, (counts.get(item.account_id) ?? 0) + 1);
    return counts;
  }, [queueQuery.data]);

  const openAccount = (id: string | null) => {
    const next = new URLSearchParams(search.toString());
    if (id) next.set("akun", id);
    else next.delete("akun");
    const query = next.toString();
    // Replacing the address never scrolls the page (D7); the dialog opens over the list where it is.
    replaceSearch(query ? `${pathname}?${query}` : pathname);
  };
  const selected = openId ? byId.get(openId) ?? null : null;

  // --- Reorder: the same requests as before, from drag handles, move buttons or the phone mode ------
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
    },
    onError: () => {
      setOrder({});
      setOrderError("Urutan tidak tersimpan. Coba lagi.");
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
    const name = list.find((item) => item.id === id)?.name ?? "";
    setOrderStatus(`${name} dipindah ke posisi ${destination + 1} dari ${ids.length}.`);
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

  // The page's actions sit in the top bar beside the title, as in the old app (refine-workspace-layout D7).
  const header = (
    <TopBarControls>
    <div className={styles.toolbar}>
      <button type="button" className="ws-btn" onClick={() => dialogs.syncPricesMutation.mutate()} disabled={dialogs.syncPricesMutation.isPending}>
        <Icon name="refresh-cw" className="h-4 w-4" />
        {dialogs.syncPricesMutation.isPending ? "Memperbarui harga…" : "Perbarui harga"}
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openPlans}>
        <Icon name="calendar" className="h-4 w-4" />
        Rencana
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openNewAccount}>
        <Icon name="plus" className="h-4 w-4" />
        Tambah rekening
      </button>
    </div>
    </TopBarControls>
  );

  const status = (
    <>
      <p className="sr-only" role="status" aria-live="polite">{dialogs.syncMessage ?? orderStatus}</p>
      {dialogs.syncMessage && <p className={styles.notice} aria-hidden="true">{dialogs.syncMessage}</p>}
      {(orderError || dialogs.accountActionError) && <p className={styles.alert} role="alert">{orderError || dialogs.accountActionError}</p>}
    </>
  );

  if (accountsQuery.isLoading) {
    return (
      <div className={styles.root} ref={rootRef}>
        {header}
        <Skeleton style={{ height: 120 }} />
        <div className={styles.cards}><Skeleton style={{ height: 320 }} /><Skeleton style={{ height: 240 }} /></div>
        {dialogs.dialogs}
      </div>
    );
  }
  if (accountsQuery.isError) {
    return (
      <div className={styles.root} ref={rootRef}>
        {header}
        <ErrorState message="Daftar rekening gagal dimuat." onRetry={() => accountsQuery.refetch()} />
        {dialogs.dialogs}
      </div>
    );
  }

  const topOrdered = ordered(top, "top");
  const pocketCount = top.reduce((count, account) => count + (account.children?.length ?? 0), 0);

  return (
    <div className={styles.root} ref={rootRef}>
      {header}
      {status}
      <AssetKpis query={netWorthQuery} money={bal} accountCount={top.length} pocketCount={pocketCount} accounts={top} />
      <Composition accounts={topOrdered} colours={colours} money={bal} />
      {phone && (
        <div className={styles.phoneReorder}>
          <button ref={reorderButton} type="button" className="ws-btn" aria-pressed={reordering} onClick={() => (reordering ? finishReorder() : setReordering(true))}>
            <Icon name="arrow-up-down" className="h-4 w-4" />
            {reordering ? "Selesai" : "Ubah urutan"}
          </button>
        </div>
      )}
      {phone && reordering ? (
        <Card labelledBy="accounts-reorder">
          <CardHeader id="accounts-reorder" icon="arrow-up-down" title="Ubah urutan" caption="Pakai tombol naik dan turun. Urutan langsung tersimpan." />
          <ReorderList
            rows={topOrdered}
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
          {!phone && <p className={styles.reorderHint}>Seret ⠿ untuk mengubah urutan rekening dan kantong, atau pakai tombol ▲▼ yang muncul saat baris disorot.</p>}
          <div className={styles.cards}>
            {topOrdered.map((account, index) => (
              <AccountCard
                key={account.id}
                account={account}
                colour={colours.get(account.id) ?? "var(--muted)"}
                pockets={ordered(account.children ?? [], account.id)}
                index={index}
                count={topOrdered.length}
                money={bal}
                amountProducts={amountProducts}
                awaiting={awaiting}
                draggable={!phone}
                onOpen={openAccount}
                onAdd={() => dialogs.openNewPocket(account)}
                onReconcile={dialogs.openReconciliation}
                onMove={(id, list, key, destination) => move(list, key, id, destination)}
                onMoveTop={(step) => move(topOrdered, "top", account.id, index + step)}
                drag={{ dragged, dragOver, setDragged, setDragOver, dropTop: (target) => {
                  if (!dragged || dragged === target || !topOrdered.some((row) => row.id === dragged)) return;
                  move(topOrdered, "top", dragged, topOrdered.findIndex((row) => row.id === target));
                } }}
              />
            ))}
          </div>
        </>
      )}
      <AccountDetail
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

interface DragState {
  dragged: string | null;
  dragOver: string | null;
  setDragged: (id: string | null) => void;
  setDragOver: (id: string | null) => void;
  dropTop: (target: string) => void;
}

/** One account as a card: its head and every pocket or product are buttons that open the detail (D7). */
function AccountCard({
  account,
  colour,
  pockets,
  index,
  count,
  money,
  amountProducts,
  awaiting,
  draggable,
  onOpen,
  onAdd,
  onReconcile,
  onMove,
  onMoveTop,
  drag,
}: {
  account: AccountItem;
  colour: string;
  pockets: AccountItem[];
  index: number;
  count: number;
  money: (value: number) => string;
  amountProducts: Set<string>;
  awaiting: Map<string, number>;
  draggable: boolean;
  onOpen: (id: string) => void;
  onAdd: () => void;
  onReconcile: (account: AccountItem) => void;
  onMove: (id: string, list: AccountItem[], key: string, destination: number) => void;
  onMoveTop: (step: number) => void;
  drag: DragState;
}) {
  const holding = isHolding(account);
  const flagged = pockets.filter((pocket) => pocket.reconciliation_required).length;
  const kind = holding ? (pockets.length ? `${pockets.length} produk` : trackingPill(account, amountProducts)) : `${KIND[account.type]}${pockets.length ? ` · ${pockets.length} kantong` : ""}`;
  const waiting = awaiting.get(account.id);
  const groupGain = account.group_capital_gain;
  return (
    <section
      className={cn(styles.acard, drag.dragOver === account.id && styles.dropTarget, drag.dragged === account.id && styles.dragging)}
      style={{ ["--ac" as string]: colour, ["--ac-ink" as string]: badgeInk(colour) }}
      aria-label={account.name}
      data-account-row={account.id}
      onDragOver={(event) => {
        if (drag.dragged && drag.dragged !== account.id && !pockets.some((pocket) => pocket.id === drag.dragged)) {
          event.preventDefault();
          drag.setDragOver(account.id);
        }
      }}
      onDragLeave={() => drag.setDragOver(null)}
      onDrop={(event) => {
        event.preventDefault();
        drag.dropTop(account.id);
        drag.setDragged(null);
        drag.setDragOver(null);
      }}
    >
      <div className={styles.acardHead}>
        {draggable && (
          <span
            className={styles.handle}
            draggable
            aria-hidden="true"
            onDragStart={(event) => { drag.setDragged(account.id); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", account.id); }}
            onDragEnd={() => { drag.setDragged(null); drag.setDragOver(null); }}
          >
            <Icon name="grip-vertical" className="h-4 w-4" />
          </span>
        )}
        <button type="button" className={styles.acardOpen} onClick={() => onOpen(account.id)} aria-label={account.name} aria-describedby={`acct-${account.id}`}>
          <span className={styles.badge}>
            <span className={styles.badgeMark} aria-hidden="true">{initials(account.name)}</span>
            {account.name}
          </span>
          <span id={`acct-${account.id}`} className={styles.acardFacts}>
            <span className={styles.acardKind}>{kind}</span>
            <span className={styles.acardBalance}>{money(account.balance)}</span>
          </span>
          {holding && pockets.length > 0 && (
            <span className={styles.acardGain}>
              {groupGain === undefined ? null : <GainLine gain={groupGain} pct={account.group_cost_basis ? (groupGain! / account.group_cost_basis) * 100 : null} money={money} />}
            </span>
          )}
        </button>
        <span className={styles.acardTools}>
          {draggable && count > 1 && <MoveButtons name={account.name} first={index === 0} last={index === count - 1} pending={false} onMove={onMoveTop} />}
          <button type="button" className="ws-icon-btn" aria-label={holding ? `Tambah produk ${account.name}` : `Tambah kantong ${account.name}`} onClick={onAdd}>
            <Icon name="plus" className="h-4 w-4" />
          </button>
        </span>
      </div>
      {(account.reconciliation_required || flagged > 0 || waiting) && (
        <div className={styles.acardNotes}>
          {account.reconciliation_required && <ReconciliationFlag name={account.name} onReconcile={!pockets.length && account.investment_tracking_mode !== "amount" ? () => onReconcile(account) : undefined} />}
          {!account.reconciliation_required && flagged > 0 && <ReconciliationFlag name={account.name} pocketCount={flagged} />}
          {waiting ? <span className="ws-caption">{waiting} pindah saldo belum lengkap</span> : null}
        </div>
      )}
      {pockets.length > 0 && (
        <ul className={styles.pockets} aria-label={holding ? `Produk ${account.name}` : `Kantong ${account.name}`}>
          {pockets.map((pocket, pocketIndex) => {
            const product = isHolding(pocket);
            return (
              <li
                key={pocket.id}
                data-account-row={pocket.id}
                className={cn(styles.pocket, drag.dragOver === pocket.id && styles.dropTarget, drag.dragged === pocket.id && styles.dragging)}
                onDragOver={(event) => { if (drag.dragged && pockets.some((item) => item.id === drag.dragged)) { event.preventDefault(); event.stopPropagation(); drag.setDragOver(pocket.id); } }}
                onDrop={(event) => {
                  if (!drag.dragged || !pockets.some((item) => item.id === drag.dragged)) return;
                  event.preventDefault();
                  event.stopPropagation();
                  if (drag.dragged !== pocket.id) onMove(drag.dragged, pockets, account.id, pocketIndex);
                  drag.setDragged(null);
                  drag.setDragOver(null);
                }}
              >
                {draggable && (
                  <span
                    className={styles.handle}
                    draggable
                    aria-hidden="true"
                    onDragStart={(event) => { event.stopPropagation(); drag.setDragged(pocket.id); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", pocket.id); }}
                    onDragEnd={(event) => { event.stopPropagation(); drag.setDragged(null); drag.setDragOver(null); }}
                  >
                    <Icon name="grip-vertical" className="h-4 w-4" />
                  </span>
                )}
                <button
                  type="button"
                  className={styles.pocketOpen}
                  onClick={() => onOpen(pocket.id)}
                  aria-label={`${pocket.name}, ${product ? "produk" : "kantong"} di ${account.name}`}
                  aria-describedby={`acct-${pocket.id}`}
                >
                  <span className={styles.pocketName}>
                    <b>{pocket.name}</b>
                    {product && <span>{productLine(pocket, amountProducts, money)}</span>}
                  </span>
                  <span id={`acct-${pocket.id}`} className={styles.pocketValue}>
                    <b>{money(pocket.balance)}</b>
                    {product && <GainLine gain={pocket.capital_gain} pct={pocket.capital_gain_pct} money={money} />}
                  </span>
                </button>
                {pocket.reconciliation_required && (
                  <ReconciliationFlag
                    name={pocket.name}
                    onReconcile={!product && pocket.investment_tracking_mode !== "amount" ? () => onReconcile(pocket) : undefined}
                  />
                )}
                {draggable && pockets.length > 1 && (
                  <MoveButtons
                    name={pocket.name}
                    first={pocketIndex === 0}
                    last={pocketIndex === pockets.length - 1}
                    pending={false}
                    onMove={(step) => onMove(pocket.id, pockets, account.id, pocketIndex + step)}
                  />
                )}
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

function productLine(account: AccountItem, amountProducts: Set<string>, money: (value: number) => string): string {
  const basis = account.cost_basis != null ? ` · modal ${money(account.cost_basis)}` : "";
  if (account.instrument_type === "stock" && account.units) {
    return `${(account.units / 100).toLocaleString("id-ID")} lot${account.last_price ? ` · harga ${fmtMoney(account.last_price)}` : ""}${account.avg_buy_price ? ` · modal ${fmtMoney(account.avg_buy_price)}` : ""}`;
  }
  return `${trackingPill(account, amountProducts)}${basis}`;
}

/** KPI cards on top (D7): Total aset, Siap pakai, Investasi with its gain, Kekayaan bersih. */
function AssetKpis({
  query,
  money,
  accountCount,
  pocketCount,
  accounts,
}: {
  query: { data?: NetWorthResponse; isLoading: boolean; isError: boolean; refetch: () => unknown };
  money: (value: number) => string;
  accountCount: number;
  pocketCount: number;
  accounts: AccountItem[];
}) {
  if (query.isLoading) return <Skeleton style={{ height: 120 }} />;
  const data = query.data;
  if (query.isError || !data) return <ErrorState message="Kekayaan bersih belum dapat dihitung." onRetry={() => query.refetch()} />;
  const lines = data.lines ?? [];
  const assetsExplanation: ExplanationData = {
    title: "Total aset",
    rule: "Uang siap pakai ditambah investasi. Saldo kantong dihitung sekali, tidak dihitung dua kali dengan rekening induknya.",
    lines: lines.filter((line) => line.key === "liquid" || line.key === "investment").map((line) => ({ label: line.key === "liquid" ? "Uang siap pakai" : "Investasi", amount: money(line.amount) })),
    total: { label: "Total aset", amount: money(data.total_assets) },
  };
  const netExplanation: ExplanationData = {
    title: "Kekayaan bersih",
    rule: "Total aset dikurangi saldo minus yang perlu dicocokkan dan sisa utang aktif. Angka yang sama dengan Beranda.",
    lines: lines.map((line) => ({ label: line.key === "liquid" ? "Uang siap pakai" : line.key === "investment" ? "Investasi" : line.label, amount: money(line.amount) })),
    total: { label: "Kekayaan bersih", amount: money(data.net_worth) },
  };
  // The investment gain is the sum of the platforms' server totals; null when any platform lacks a basis.
  const platforms = accounts.filter((account) => isHolding(account) && (account.children?.length ?? 0) > 0);
  const gains = platforms.map((account) => account.group_capital_gain);
  const known = platforms.length > 0 && gains.every((gain) => gain != null);
  const gain = known ? gains.reduce((sum: number, value) => sum + (value ?? 0), 0) : null;
  const basis = known ? platforms.reduce((sum, account) => sum + (account.group_cost_basis ?? 0), 0) : null;
  // An API from before the shared net worth rule has no liquid and investment parts; those cards then say so.
  const parts = data.liquid != null && data.investment != null;
  const share = parts && data.total_assets > 0 ? Math.round((Math.max(0, data.liquid) / data.total_assets) * 100) : null;
  return (
    <section className="ws-kcards ws-kcards-4" aria-label="Ringkasan aset">
      <KpiCard
        tone="neutral"
        label="Total aset"
        value={<ExplainableFigure label="Total aset" amountText={money(data.total_assets)} explanation={assetsExplanation} />}
        caption={`${accountCount} rekening · ${pocketCount} kantong`}
      />
      <KpiCard tone="in" label="Siap pakai" valueIsWords={!parts} value={parts ? money(data.liquid) : "Belum tersedia"} caption={share != null ? `${share}% dari aset` : ""} />
      <KpiCard
        tone="inv"
        label="Investasi"
        valueIsWords={!parts}
        value={parts ? money(data.investment) : "Belum tersedia"}
        caption={gain != null ? <GainLine gain={gain} pct={basis ? (gain / basis) * 100 : null} money={money} /> : platforms.length ? "Modal belum lengkap" : "Belum ada investasi"}
      />
      <KpiCard
        tone="net"
        label="Kekayaan bersih"
        value={<ExplainableFigure label="Kekayaan bersih" amountText={money(data.net_worth)} explanation={netExplanation} />}
        caption={data.total_liabilities > 0 ? `setelah utang ${money(data.total_liabilities)}` : "tanpa utang aktif"}
      />
    </section>
  );
}

/** Komposisi aset (D7): each top-level account's share of the assets, without debt. Shares are display only. */
function Composition({ accounts, colours, money }: { accounts: AccountItem[]; colours: Map<string, string>; money: (value: number) => string }) {
  const parts = accounts.filter((account) => account.balance > 0).sort((a, b) => b.balance - a.balance);
  const total = parts.reduce((sum, account) => sum + account.balance, 0);
  if (!total) return null;
  return (
    <Card labelledBy="accounts-composition">
      <CardHeader id="accounts-composition" title="Komposisi aset" extra={<span className={styles.headTotal}>{money(total)}</span>} />
      <div className={styles.compBar} aria-hidden="true">
        {parts.map((account) => (
          <i key={account.id} style={{ flex: account.balance, background: colours.get(account.id) }} />
        ))}
      </div>
      <ul className={styles.compLegend} aria-label="Komposisi aset per rekening">
        {parts.map((account) => (
          <li key={account.id} style={{ ["--ac" as string]: colours.get(account.id) }}>
            <span className={styles.compName}>{account.name}</span>
            <span className={styles.compPct}>{((account.balance / total) * 100).toLocaleString("id-ID", { maximumFractionDigits: 1 })}%</span>
            <b>{money(account.balance)}</b>
          </li>
        ))}
      </ul>
    </Card>
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

/** The detail of an account, pocket or product: a centered dialog, a bottom sheet on phones (D7). */
function AccountDetail({
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
    <Modal open onClose={onClose} title={account.name}>
      <div className={styles.drawerBody}>
        <p className="ws-caption">{parent ? `${kind} di ${parent.name}` : kind}</p>
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
        {holding && (
          <p className={styles.performance}>
            Untung/rugi: <GainLine gain={account.capital_gain} pct={account.capital_gain_pct} money={money} />
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
    </Modal>
  );
}
