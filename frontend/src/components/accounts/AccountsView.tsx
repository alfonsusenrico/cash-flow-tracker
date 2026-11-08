"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { replaceSearch } from "@/lib/urlState";
import { listAmountInvestmentProducts } from "@/lib/investmentTopups";
import type { ReviewQueue } from "@/lib/review";
import { Icon } from "@/components/ui/Icon";
import { PageActions } from "@/components/workspace/context";
import { PHONE_QUERY, useMediaQuery } from "@/components/workspace/layers";
import { useToast } from "@/components/workspace/overlays";
import { EmptyState, ErrorState, Segmented, Skeleton } from "@/components/workspace/primitives";
import { AccountDetailDialog } from "./AccountDetailDialog";
import { useAccountDialogs } from "./AccountDialogs";
import { useAccountText } from "./accountText";
import { displayColours } from "./displayColours";
import { RekeningChart } from "./RekeningChart";
import { RekeningGroups, groupHandleId } from "./RekeningGroups";
import { RekeningHero, RekeningHeroSkeleton } from "./RekeningHero";
import { tileDomId } from "./Tile";
import { useGrouping, useWalletLayout } from "./useWalletLayout";
import { applyLayout, buildHoldings, isShown, layoutOf, moved, obligationsOutside, sectionsFor } from "./wallet";
import type { AccountItem, AccountsResponse, LayoutGrouping, NetWorthResponse } from "./types";

type View = "kartu" | "grafik";
const VIEW_PARAM = "tampilan";

/**
 * Rekening & Kantong (apply-markas-rekening): the hero "Kekayaan bersih", the two controls, then Kartu (groups by
 * use or by rekening) or Grafik. "Atur urutan" in "…" turns on the reorder mode; a tile opens the account's
 * detail, kept in the address as `?akun=<id>`, and Grafik as `?tampilan=grafik`.
 */
export function AccountsView() {
  const { t } = useAccountText();
  const { showToast } = useToast();
  const pathname = usePathname();
  const search = useSearchParams();
  const openId = search.get("akun");
  const view: View = search.get(VIEW_PARAM) === "grafik" ? "grafik" : "kartu";
  const dialogs = useAccountDialogs();
  const phone = useMediaQuery(PHONE_QUERY);
  const [grouping, setGrouping] = useGrouping();
  const [reordering, setReordering] = useState(false);
  const [orderStatus, setOrderStatus] = useState("");
  const [focusRequest, setFocusRequest] = useState<string | null>(null);
  const opener = useRef<HTMLElement | null>(null);

  const accountsQuery = useQuery<AccountsResponse>({ queryKey: queryKeys.accounts, queryFn: () => api.get("/accounts") });
  const netWorthQuery = useQuery<NetWorthResponse>({ queryKey: queryKeys.dashboard.netWorth, queryFn: () => api.get("/dashboard/net-worth") });
  const queueQuery = useQuery<ReviewQueue>({ queryKey: queryKeys.review.queue, queryFn: () => api.get("/review/queue") });
  const { layout, save: saveLayout, failed: orderFailed } = useWalletLayout(grouping);

  const accounts = useMemo(() => (accountsQuery.data?.accounts ?? []).filter((account) => !account.is_archived), [accountsQuery.data]);
  const top = useMemo(() => accounts.filter((account) => !account.parent_id), [accounts]);
  const amountProducts = useMemo(() => new Set(listAmountInvestmentProducts(accountsQuery.data?.accounts ?? []).map((item) => item.id)), [accountsQuery.data]);
  const colours = useMemo(() => displayColours(top), [top]);
  const holdings = useMemo(() => buildHoldings(top), [top]);
  const allSections = useMemo(() => applyLayout(sectionsFor(grouping, top, holdings), layout), [grouping, top, holdings, layout]);
  const sections = useMemo(() => allSections.filter(isShown), [allSections]);
  const obligations = useMemo(() => obligationsOutside(netWorthQuery.data), [netWorthQuery.data]);
  // Awaiting outflows per account, counted from the queue; amounts stay in Tinjau (OD59).
  const awaiting = useMemo(() => {
    const counts = new Map<string, number>();
    for (const item of queueQuery.data?.awaiting ?? []) counts.set(item.account_id, (counts.get(item.account_id) ?? 0) + 1);
    return counts;
  }, [queueQuery.data]);

  const writeParams = useCallback((change: (params: URLSearchParams) => void) => {
    const next = new URLSearchParams(window.location.search);
    change(next);
    const query = next.toString();
    // Replacing the address never scrolls the page (refine-workspace-layout D7); the dialog opens over the list.
    replaceSearch(query ? `${pathname}?${query}` : pathname);
  }, [pathname]);
  const openAccount = (id: string) => {
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    writeParams((params) => params.set("akun", id));
  };
  const closeAccount = useCallback(() => {
    writeParams((params) => params.delete("akun"));
    const target = opener.current;
    opener.current = null;
    if (target) requestAnimationFrame(() => target.focus());
  }, [writeParams]);
  const chooseView = (next: View) => writeParams((params) => (next === "grafik" ? params.set(VIEW_PARAM, "grafik") : params.delete(VIEW_PARAM)));

  useEffect(() => {
    if (orderFailed) showToast(t("toast.orderFailed"));
  }, [orderFailed, showToast, t]);

  // A keyboard move re-renders the groups; the moved tile or handle keeps the focus.
  useEffect(() => {
    if (!focusRequest) return;
    document.getElementById(focusRequest)?.focus();
    setFocusRequest(null);
  }, [focusRequest, allSections]);

  // Esc leaves Atur urutan when no dialog or menu is open above the page.
  useEffect(() => {
    if (!reordering) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || event.defaultPrevented || document.querySelector("dialog[open], [role='menu']")) return;
      setReordering(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [reordering]);

  const titleOf = (key: string) => {
    const section = sections.find((entry) => entry.key === key);
    if (!section) return key;
    return section.rekening ? section.rekening.name : t(`groups.${section.use!.labelKey}`);
  };
  const moveSection = (key: string, destination: number, keyboard: boolean) => {
    const from = sections.findIndex((section) => section.key === key);
    const nextVisible = moved(sections, from, destination);
    if (nextVisible === sections) return;
    // Empty rows are not shown; they keep their places in the saved order.
    let visibleIndex = 0;
    const full = allSections.map((section) => (isShown(section) ? nextVisible[visibleIndex++] : section));
    saveLayout(layoutOf(grouping, full));
    setOrderStatus(t("announce.rowMoved", { title: titleOf(key), position: String(destination + 1), total: String(sections.length) }));
    if (keyboard) setFocusRequest(groupHandleId(key));
  };
  const moveCard = (sectionKey: string, id: string, destination: number, keyboard: boolean) => {
    const section = allSections.find((entry) => entry.key === sectionKey);
    if (!section) return;
    const from = section.holdings.findIndex((holding) => holding.id === id);
    const nextHoldings = moved(section.holdings, from, destination);
    if (nextHoldings === section.holdings) return;
    saveLayout(layoutOf(grouping, allSections.map((entry) => (entry === section ? { ...entry, holdings: nextHoldings } : entry))));
    const holding = section.holdings[from];
    const name = holding.ownBalance ? holding.rekening.name : holding.account.name;
    setOrderStatus(t("announce.cardMoved", { name, position: String(destination + 1), total: String(nextHoldings.length) }));
    if (keyboard) setFocusRequest(tileDomId(id));
  };

  const syncPrices = () => dialogs.syncPricesMutation.mutate();
  const header = (
    <PageActions
      moreLabel={t("page.moreLabel")}
      more={[
        { key: "harga", label: t("page.updatePrices"), icon: "refresh-cw", onSelect: syncPrices, phoneOnly: true },
        { key: "urutan", label: reordering ? t("page.arrangeDone") : t("page.arrange"), icon: "grip-vertical", onSelect: () => setReordering((on) => !on) },
        { key: "rencana", label: t("page.plans"), icon: "calendar", onSelect: dialogs.openPlans },
      ]}
    >
      <button type="button" className="ws-btn ph-hide" onClick={syncPrices} disabled={dialogs.syncPricesMutation.isPending}>
        <Icon name="refresh-cw" className="h-4 w-4" />
        {dialogs.syncPricesMutation.isPending ? t("page.updatingPrices") : t("page.updatePrices")}
      </button>
      <button type="button" className="ws-btn" onClick={dialogs.openNewAccount}>
        <Icon name="plus" className="h-4 w-4" />
        {t("page.addAccount")}
      </button>
    </PageActions>
  );

  if (accountsQuery.isLoading) {
    return (
      <div className="rk-page">
        {header}
        <RekeningHeroSkeleton />
        <div className="rk-skel" aria-busy="true" aria-label={t("states.loading")}>
          {[0, 1].map((group) => (
            <div key={group} className="grp">
              <Skeleton style={{ width: 120, height: 12 }} />
              <div className="tiles">
                {[0, 1, 2].map((tile) => (
                  <div key={tile} className="tile">
                    <Skeleton style={{ width: "60%", height: 12 }} />
                    <Skeleton style={{ width: "40%", height: 20, marginTop: "auto" }} />
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
        {dialogs.dialogs}
      </div>
    );
  }
  if (accountsQuery.isError) {
    return (
      <div className="rk-page">
        {header}
        <ErrorState message={t("states.loadFailed")} onRetry={() => accountsQuery.refetch()} />
        {dialogs.dialogs}
      </div>
    );
  }
  if (!top.length) {
    return (
      <div className="rk-page">
        {header}
        <EmptyState
          title={t("states.emptyTitle")}
          action={
            <button type="button" className="ws-btn" onClick={dialogs.openNewAccount}>
              <Icon name="plus" className="h-4 w-4" />
              {t("page.addAccount")}
            </button>
          }
        >
          {t("states.emptyLine")}
        </EmptyState>
        {dialogs.dialogs}
      </div>
    );
  }

  const groupings: { value: LayoutGrouping; label: string }[] = [
    { value: "use", label: t("controls.byUse") },
    { value: "rekening", label: t("controls.byAccount") },
  ];
  return (
    <div className="rk-page">
      {header}
      <p className="sr-only" role="status" aria-live="polite">{orderFailed ? "" : orderStatus}</p>
      <RekeningHero netWorth={netWorthQuery} holdings={holdings} />
      <div className="toolbar rk-toolbar">
        <Segmented<View>
          label={t("controls.view")}
          options={[{ value: "kartu", label: t("controls.cards") }, { value: "grafik", label: t("controls.chart") }]}
          value={view}
          onChange={chooseView}
        />
        {view === "kartu" && <Segmented<LayoutGrouping> label={t("controls.grouping")} options={groupings} value={grouping} onChange={setGrouping} />}
      </div>
      {reordering && (
        <div className="sel-bar rk-sel-bar">
          <p className="t-body">{phone ? t("reorder.hintPhone") : t("reorder.hint")}</p>
          <button type="button" className="ws-btn ws-btn-accent" onClick={() => setReordering(false)}>
            {t("reorder.done")}
          </button>
        </div>
      )}
      {view === "grafik" ? (
        <RekeningChart
          holdings={sections.flatMap((section) => section.holdings)}
          grouping={grouping}
          colours={colours}
          totalAssets={netWorthQuery.data?.total_assets}
          onOpen={openAccount}
        />
      ) : (
        <RekeningGroups
          sections={sections}
          grouping={grouping}
          colours={colours}
          amountProducts={amountProducts}
          awaiting={awaiting}
          obligations={obligations}
          reordering={reordering}
          phone={phone}
          actions={{ onOpen: openAccount, onReconcile: (account: AccountItem) => dialogs.openReconciliation(account), onMoveCard: moveCard, onMoveSection: moveSection }}
        />
      )}
      <AccountDetailDialog accountId={openId} onClose={closeAccount} />
      {dialogs.dialogs}
    </div>
  );
}
