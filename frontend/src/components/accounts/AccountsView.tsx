"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { replaceSearch } from "@/lib/urlState";
import { listAmountInvestmentProducts } from "@/lib/investmentTopups";
import type { ReviewQueue } from "@/lib/review";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Icon } from "@/components/ui/Icon";
import { ErrorState, Segmented, Skeleton } from "@/components/workspace/primitives";
import { PageActions } from "@/components/workspace/context";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { AccountDetail } from "./AccountDetail";
import { useAccountDialogs } from "./AccountDialogs";
import { displayColours } from "./displayColours";
import { ReorderPanel } from "./ReorderPanel";
import { useGrouping, useWalletLayout } from "./useWalletLayout";
import { WalletChart } from "./WalletChart";
import { WalletRails, cardDomId, railHandleId, railHeadingId } from "./WalletRails";
import { WalletSummary } from "./WalletSummary";
import { applyLayout, billsOf, buildHoldings, isShown, layoutOf, moved, sectionsFor, type AssetUse } from "./wallet";
import type { AccountItem, AccountsResponse, LayoutGrouping, NetWorthResponse } from "./types";
import styles from "./money.module.css";
import walletStyles from "./wallet.module.css";

// Phones reorder in a mode: dragging would fight the page scroll (refine-workspace-layout D9).
const PHONE_WIDTH = 767;

type ViewTab = "kartu" | "grafik";
const TABS: { value: ViewTab; label: string }[] = [
  { value: "kartu", label: "Kartu" },
  { value: "grafik", label: "Grafik" },
];
const GROUPINGS: { value: LayoutGrouping; label: string }[] = [
  { value: "use", label: "Per kegunaan" },
  { value: "rekening", label: "Per rekening" },
];

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === "function" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

/** Rekening & Kantong (organize-accounts-by-use D7): summary, Kartu and Grafik, by use or by rekening. */
export function AccountsView() {
  const { bal } = useAppCtx();
  const pathname = usePathname();
  const search = useSearchParams();
  const openId = search.get("akun");
  const dialogs = useAccountDialogs();
  const rootRef = useRef<HTMLDivElement>(null);
  const phone = useWidthAtMost(rootRef, PHONE_WIDTH);
  const reorderButton = useRef<HTMLButtonElement>(null);
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const [reordering, setReordering] = useState(false);
  const [tab, setTab] = useState<ViewTab>("kartu");
  const [grouping, setGrouping] = useGrouping();
  const [orderStatus, setOrderStatus] = useState("");
  const [focusRequest, setFocusRequest] = useState<string | null>(null);

  const accountsQuery = useQuery<AccountsResponse>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });
  const netWorthQuery = useQuery<NetWorthResponse>({ queryKey: queryKeys.dashboard.netWorth, queryFn: () => api.get("/dashboard/net-worth") });
  const queueQuery = useQuery<ReviewQueue>({ queryKey: queryKeys.review.queue, queryFn: () => api.get("/review/queue") });
  const { layout, save: saveLayout, failed: orderFailed } = useWalletLayout(grouping);

  const accounts = useMemo(() => (accountsQuery.data?.accounts ?? []).filter((account) => !account.is_archived), [accountsQuery.data]);
  const top = useMemo(() => accounts.filter((account) => !account.parent_id), [accounts]);
  const byId = useMemo(() => {
    const map = new Map<string, AccountItem>();
    for (const account of accounts) {
      map.set(account.id, account);
      for (const pocket of account.children ?? []) map.set(pocket.id, pocket);
    }
    return map;
  }, [accounts]);
  const amountProducts = useMemo(() => new Set(listAmountInvestmentProducts(accountsQuery.data?.accounts ?? []).map((item) => item.id)), [accountsQuery.data]);
  const colours = useMemo(() => displayColours(top), [top]);
  const holdings = useMemo(() => buildHoldings(top), [top]);
  const allSections = useMemo(() => applyLayout(sectionsFor(grouping, top, holdings), layout), [grouping, top, holdings, layout]);
  const bills = useMemo(() => billsOf(netWorthQuery.data?.obligations), [netWorthQuery.data]);
  const sections = useMemo(() => allSections.filter((section) => isShown(section, grouping, bills)), [allSections, grouping, bills]);
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
    // Replacing the address never scrolls the page (refine-workspace-layout D7); the dialog opens over the list.
    replaceSearch(query ? `${pathname}?${query}` : pathname);
  };
  const selected = openId ? byId.get(openId) ?? null : null;

  useEffect(() => {
    if (openId && accountsQuery.data && !byId.has(openId)) openAccount(null);
    // Only a stale ?akun= is cleared; the handler itself is stable enough for this check.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openId, accountsQuery.data, byId]);

  // A keyboard move re-renders the rows; the moved card or handle keeps the focus.
  useEffect(() => {
    if (!focusRequest) return;
    document.getElementById(focusRequest)?.focus();
    setFocusRequest(null);
  }, [focusRequest, allSections]);

  const moveSection = (key: string, destination: number, keyboard: boolean) => {
    const from = sections.findIndex((section) => section.key === key);
    const nextVisible = moved(sections, from, destination);
    if (nextVisible === sections) return;
    // Empty rows are not shown; they keep their places in the saved order.
    let visibleIndex = 0;
    const full = allSections.map((section) => (isShown(section, grouping, bills) ? nextVisible[visibleIndex++] : section));
    saveLayout(layoutOf(grouping, full));
    setOrderStatus(`Baris ${sections[from].title} dipindah ke posisi ${destination + 1} dari ${sections.length}.`);
    if (keyboard) setFocusRequest(railHandleId(key));
  };
  const moveCard = (sectionKey: string, id: string, destination: number, keyboard: boolean) => {
    const section = allSections.find((entry) => entry.key === sectionKey);
    if (!section) return;
    const from = section.holdings.findIndex((holding) => holding.id === id);
    const nextHoldings = moved(section.holdings, from, destination);
    if (nextHoldings === section.holdings) return;
    saveLayout(layoutOf(grouping, allSections.map((entry) => (entry === section ? { ...entry, holdings: nextHoldings } : entry))));
    setOrderStatus(`${section.holdings[from].account.name} dipindah ke posisi ${destination + 1} dari ${nextHoldings.length}.`);
    if (keyboard) setFocusRequest(cardDomId(id));
  };
  const jumpToUse = (use: AssetUse) => {
    const heading = document.getElementById(railHeadingId(use));
    heading?.scrollIntoView?.({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" });
    heading?.focus({ preventScroll: true });
  };
  const finishReorder = () => {
    setReordering(false);
    requestAnimationFrame(() => reorderButton.current?.focus());
  };
  const onTabKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const index = TABS.findIndex((entry) => entry.value === tab);
    let next = index;
    if (event.key === "ArrowRight") next = (index + 1) % TABS.length;
    else if (event.key === "ArrowLeft") next = (index - 1 + TABS.length) % TABS.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = TABS.length - 1;
    else return;
    event.preventDefault();
    setTab(TABS[next].value);
    tabRefs.current[next]?.focus();
  };

  const syncPrices = () => dialogs.syncPricesMutation.mutate();
  // The title block's actions in the prototype's order (apply-markas-shell D4): Perbarui harga moves into "…"
  // on the phone, Rencana is always there.
  const header = (
    <PageActions
      more={[
        { key: "harga", label: "Perbarui harga", icon: "refresh-cw", onSelect: syncPrices, phoneOnly: true },
        { key: "rencana", label: "Rencana", icon: "calendar", onSelect: dialogs.openPlans },
      ]}
    >
      <button type="button" className="ws-btn ph-hide" onClick={syncPrices} disabled={dialogs.syncPricesMutation.isPending}>
        <Icon name="refresh-cw" className="h-4 w-4" />
        {dialogs.syncPricesMutation.isPending ? "Memperbarui harga…" : "Perbarui harga"}
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openNewAccount}>
        <Icon name="plus" className="h-4 w-4" />
        Tambah rekening
      </button>
    </PageActions>
  );

  if (accountsQuery.isLoading || accountsQuery.isError) {
    return (
      <div className={styles.root} ref={rootRef}>
        {header}
        {accountsQuery.isLoading ? (
          <>
            <Skeleton style={{ height: 160 }} />
            <Skeleton style={{ height: 180 }} />
          </>
        ) : (
          <ErrorState message="Daftar rekening gagal dimuat." onRetry={() => accountsQuery.refetch()} />
        )}
        {dialogs.dialogs}
      </div>
    );
  }

  const announcement = dialogs.syncMessage ?? (orderFailed ? "" : orderStatus);
  return (
    <div className={styles.root} ref={rootRef}>
      {header}
      <p className="sr-only" role="status" aria-live="polite">{announcement}</p>
      {dialogs.syncMessage && <p className={styles.notice} aria-hidden="true">{dialogs.syncMessage}</p>}
      {orderFailed && <p className={styles.alert} role="alert">Urutan gagal disimpan</p>}
      {dialogs.accountActionError && <p className={styles.alert} role="alert">{dialogs.accountActionError}</p>}
      <div className={walletStyles.controls}>
        <div className={walletStyles.tabs} role="tablist" aria-label="Tampilan" onKeyDown={onTabKey}>
          {TABS.map((entry, index) => (
            <button
              key={entry.value}
              ref={(element) => { tabRefs.current[index] = element; }}
              type="button"
              role="tab"
              id={`wallet-tab-${entry.value}`}
              aria-selected={entry.value === tab}
              aria-controls="wallet-panel"
              tabIndex={entry.value === tab ? 0 : -1}
              onClick={() => setTab(entry.value)}
            >
              {entry.label}
            </button>
          ))}
        </div>
        <Segmented label="Kelompokkan" options={GROUPINGS} value={grouping} onChange={setGrouping} />
      </div>
      <div id="wallet-panel" role="tabpanel" aria-labelledby={`wallet-tab-${tab}`} className={walletStyles.panelStack}>
        <WalletSummary
          netWorth={netWorthQuery}
          holdings={holdings}
          grouping={grouping}
          money={bal}
          colours={colours}
          rekeningSections={sections}
          side={tab === "kartu"}
          onJump={jumpToUse}
        />
        {tab === "grafik" ? (
          <WalletChart
            holdings={sections.flatMap((section) => section.holdings)}
            grouping={grouping}
            money={bal}
            colours={colours}
            total={netWorthQuery.data?.total_assets}
            onOpen={openAccount}
          />
        ) : (
          <>
            {phone ? (
              <div className={walletStyles.phoneReorder}>
                <button ref={reorderButton} type="button" className="ws-btn" aria-pressed={reordering} onClick={() => (reordering ? finishReorder() : setReordering(true))}>
                  <Icon name="arrow-up-down" className="h-4 w-4" />
                  {reordering ? "Selesai" : "Ubah urutan"}
                </button>
              </div>
            ) : (
              <p className={walletStyles.hint}>Seret kartu dalam baris, atau seret ⋮⋮ untuk memindah baris. Dengan keyboard: Alt + panah. Urutan diingat.</p>
            )}
            {phone && reordering ? (
              <ReorderPanel
                sections={sections}
                onMoveSection={(key, destination) => moveSection(key, destination, false)}
                onMoveCard={(key, id, destination) => moveCard(key, id, destination, false)}
              />
            ) : (
              <WalletRails
                sections={sections}
                grouping={grouping}
                money={bal}
                colours={colours}
                draggable={!phone}
                amountProducts={amountProducts}
                awaiting={awaiting}
                bills={bills}
                actions={{ onOpen: openAccount, onReconcile: dialogs.openReconciliation, onMoveCard: moveCard, onMoveSection: moveSection }}
              />
            )}
          </>
        )}
      </div>
      <AccountDetail
        account={selected}
        parent={selected?.parent_id ? byId.get(selected.parent_id) ?? null : null}
        colour={(selected && colours.get(selected.parent_id ?? selected.id)) ?? "var(--muted)"}
        money={bal}
        amountProducts={amountProducts}
        dialogs={dialogs}
        onClose={() => openAccount(null)}
        onOpenAccount={openAccount}
      />
      {dialogs.dialogs}
    </div>
  );
}
