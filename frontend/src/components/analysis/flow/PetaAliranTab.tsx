"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import Link from "next/link";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { KindAmount, type AmountKind } from "@/components/transactions/rows";
import { TransactionDialog } from "@/components/transactions/TransactionDialog";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { FigureBlock, HeroFigure } from "@/components/workspace/figures";
import { PHONE_QUERY, useMediaQuery } from "@/components/workspace/layers";
import { ListRow } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { EmptyState, ErrorState, Segmented } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import type { FlowLink, FlowLinkKind, FlowNode, FlowResponse, FlowRowsResponse, FlowView, PeriodBasis } from "../types";
import { BALANCE_KINDS, buildFlowModel, type DrawnLink, type Direction, type Highlight, highlightMatches, layoutFlow, nextNode } from "./sankey";

type Money = (amount: number) => string;

/** Map geometry (option-6 `M.analisisPeta`): 8 px ink nodes, 24 px slots 8 apart, at most 320 px tall, a 220 px label gutter. */
export const MAP = { top: 32, height: 320, node: 8, slot: 24, gap: 8, gutter: 220, minWidth: 720, labelGap: 8 } as const;
/** An estimate of a 12 px Inter character, a little wide, so a cut label always fits its room. */
const CHAR_WIDTH = 7;

const BAND_COLOUR: Record<FlowLinkKind, string> = {
  income: "var(--in-solid)",
  transfer: "var(--tr-solid)",
  spend: "var(--out-solid)",
  saving: "var(--inv-solid)",
  investment: "var(--inv-solid)",
  waiting: "var(--warn-solid)",
  other: "var(--k-none)",
  opening: "var(--k-none)",
  negative_opening: "var(--k-none)",
  negative_closing: "var(--k-none)",
  remaining: "var(--k-none)",
};

const HIGHLIGHTS: { value: Highlight; key: "all" | "spending" | "transfers" | "savings" | "waiting" }[] = [
  { value: "all", key: "all" },
  { value: "spend", key: "spending" },
  { value: "transfer", key: "transfers" },
  { value: "save", key: "savings" },
  { value: "waiting", key: "waiting" },
];

export function flowQueryKey(basis: PeriodBasis, offset: number) {
  return ["dashboard", "flow", basis, offset, "default"] as const;
}

/** Money in and out of a node over the server's links, without the balance links (for its name). */
export function nodeTotals(nodeId: string, links: FlowLink[]): { inbound: number; outbound: number } {
  let inbound = 0;
  let outbound = 0;
  for (const link of links) {
    if (BALANCE_KINDS.has(link.kind)) continue;
    if (link.to === nodeId) inbound += link.amount;
    if (link.from === nodeId) outbound += link.amount;
  }
  return { inbound, outbound };
}

/** A name cut with "…" to fit `room` pixels at 12 px; the full name stays in the node's accessible name. */
export function fitLabel(name: string, room: number): string {
  const fits = Math.floor(room / CHAR_WIDTH);
  if (name.length <= fits) return name;
  return fits <= 1 ? "…" : `${name.slice(0, fits - 1).trimEnd()}…`;
}

export function labelWidth(text: string): number {
  return text.length * CHAR_WIDTH;
}

/** A band's pattern, so colour is never the only cue: transfers striped, savings and investments dotted, waiting hatched. */
function bandPattern(link: DrawnLink): "stripe" | "dots" | "hatch" | null {
  if (link.kind === "saving") return "dots";
  return link.pattern;
}

type Target = { type: "node"; id: string } | { type: "link"; key: string };

/**
 * Peta Aliran (apply-markas-analisis D14 to D17): Masuk with four supporting figures, "Sorot aliran", the four-column
 * map with every label inside its frame and a legend, the table of every link, and the node and link details in a
 * centered dialog. Balances are never drawn; the table lists them.
 */
export function PetaAliranTab({
  basis,
  offset,
  view,
  onView,
  onWindowLabel,
}: {
  basis: PeriodBasis;
  offset: number;
  view: FlowView | null;
  onView: (view: FlowView) => void;
  onWindowLabel: (label: string) => void;
}) {
  const t = useTranslations("analysis");
  const terms = useTranslations("terms");
  const money = useMoney();
  const phone = useMediaQuery(PHONE_QUERY);
  const { record } = useTransactionUndo();
  const [highlight, setHighlight] = useState<Highlight>("all");
  const [target, setTarget] = useState<Target | null>(null);
  const [txId, setTxId] = useState<string | null>(null);
  const opener = useRef<HTMLElement | null>(null);
  const query = useQuery<FlowResponse>({
    queryKey: flowQueryKey(basis, offset),
    queryFn: () => api.get(`/dashboard/flow?timeframe=${basis}&cycle_offset=${offset}`),
  });
  const label = query.data?.window.label;
  useEffect(() => {
    if (label) onWindowLabel(label);
  }, [label, onWindowLabel]);

  if (query.isPending) {
    return (
      <div className="stack" role="status" aria-busy="true">
        <div>
          <span className="skel is-label" style={{ width: 200 }} />
          <span className="skel is-hero" style={{ width: 240 }} />
        </div>
        <span className="skel an-skel-map" />
      </div>
    );
  }
  if (query.isError) return <ErrorState message={t("states.flowError")} onRetry={() => query.refetch()} />;

  const data = query.data;
  const nodes = new Map(data.nodes.map((node) => [node.id, node]));
  const empty = data.links.every((link) => BALANCE_KINDS.has(link.kind));
  const shown: FlowView = view ?? (phone ? "tabel" : "peta");
  const { summary } = data;
  const remaining = summary.remaining < 0 ? `−${money(-summary.remaining)}` : money(summary.remaining);
  const open = (next: Target) => {
    if (!target) opener.current = document.activeElement as HTMLElement | null;
    setTarget(next);
  };
  const close = () => {
    setTarget(null);
    opener.current?.focus();
    opener.current = null;
  };

  return (
    <div className="flow-tab">
      <HeroFigure
        id="an-flow-hero"
        label={t("flow.heroLabel")}
        figure={<MaskedMoney text={money(summary.masuk)} />}
      >
        <div className="supports flow-supports">
          <FigureBlock
            bar={false}
            label={terms("spending")}
            figure={<MaskedMoney text={money(summary.spending)} />}
            caption={summary.reimbursement_offset > 0 ? t.rich("flow.grossNote", { gross: () => <MaskedMoney text={money(summary.spend_gross)} />, offset: () => <MaskedMoney text={money(summary.reimbursement_offset)} /> }) : undefined}
          />
          <FigureBlock bar={false} label={t("flow.savedToppedUp")} figure={<MaskedMoney text={money(summary.saving_net + summary.investment_net)} />} />
          <FigureBlock
            bar={false}
            label={terms("waitingForPair")}
            figure={<MaskedMoney text={money(summary.waiting_out + summary.waiting_in)} />}
            caption={summary.waiting_in_buckets > 0 ? t.rich("flow.waitingNote", { amount: () => <MaskedMoney text={money(summary.waiting_in_buckets)} /> }) : undefined}
          />
          <FigureBlock bar={false} label={terms("stillLeft")} figure={<MaskedMoney text={remaining} />} />
        </div>
      </HeroFigure>
      <section className="flow-map-section" aria-labelledby="an-flow-map">
        <div className="toolbar flow-toolbar">
          <h2 className="t-label" id="an-flow-map">{t("flow.map")}</h2>
          <Segmented
            label={t("flow.highlight")}
            options={HIGHLIGHTS.map((option) => ({ value: option.value, label: t(`flow.${option.key}`) }))}
            value={highlight}
            onChange={setHighlight}
          />
          <Segmented
            label={t("flow.view")}
            options={[{ value: "peta" as FlowView, label: t("flow.map") }, { value: "tabel" as FlowView, label: t("flow.table") }]}
            value={shown}
            onChange={onView}
          />
        </div>
        {empty ? (
          <EmptyState title={t("flow.empty")} />
        ) : shown === "peta" ? (
          <>
            <FlowMap data={data} highlight={highlight} money={money} onNode={(id) => open({ type: "node", id })} />
            <FlowLegend />
            <p className="caption">{t("flow.caption")}</p>
          </>
        ) : (
          <FlowTable data={data} nodes={nodes} money={money} highlight={highlight} onLink={(key) => open({ type: "link", key })} />
        )}
      </section>
      <FlowDetails
        target={txId ? null : target}
        data={data}
        nodes={nodes}
        basis={basis}
        offset={offset}
        money={money}
        onClose={close}
        onLink={(key) => setTarget({ type: "link", key })}
        onRow={setTxId}
      />
      <TransactionDialog transactionId={txId} onClose={() => setTxId(null)} record={record} />
    </div>
  );
}

function FlowLegend() {
  const t = useTranslations("analysis.flow");
  const terms = useTranslations("terms");
  const items: { key: string; label: string; colour: string; pattern?: string }[] = [
    { key: "income", label: terms("income"), colour: "var(--in-solid)" },
    { key: "transfer", label: t("legendTransfer"), colour: "var(--tr-solid)", pattern: "is-stripe" },
    { key: "spend", label: terms("spending"), colour: "var(--out-solid)" },
    { key: "save", label: t("legendSavings"), colour: "var(--inv-solid)", pattern: "is-dots" },
    { key: "waiting", label: t("legendWaiting"), colour: "var(--warn-solid)", pattern: "is-hatch" },
  ];
  return (
    <div className="chart-foot flow-legend">
      {items.map((item) => (
        <span key={item.key} className="t-meta leg">
          <span className={`leg-band ${item.pattern ?? ""}`} style={{ "--band": item.colour } as React.CSSProperties} aria-hidden="true" />
          {item.label}
        </span>
      ))}
    </div>
  );
}

const DIRECTIONS: Record<string, Direction> = { ArrowUp: "up", ArrowDown: "down", ArrowLeft: "left", ArrowRight: "right" };

function useHostWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(960);
  useEffect(() => {
    const host = ref.current;
    if (!host || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[entries.length - 1];
      if (entry) setWidth(Math.max(MAP.minWidth, Math.round(entry.contentRect.width)));
    });
    observer.observe(host);
    return () => observer.disconnect();
  }, []);
  return { ref, width };
}

function FlowMap({ data, highlight, money, onNode }: { data: FlowResponse; highlight: Highlight; money: Money; onNode: (id: string) => void }) {
  const t = useTranslations("analysis.flow");
  const { ref, width } = useHostWidth();
  const [focusKey, setFocusKey] = useState<string | null>(null);
  const [hover, setHover] = useState<string | null>(null);
  const model = useMemo(() => buildFlowModel(data.nodes, data.links, { balances: false, income: true, maxDepth: 2 }), [data]);
  const layout = useMemo(
    () => layoutFlow(model, {
      width, height: MAP.height, stationWidth: MAP.node, stationMin: 2, barWidth: MAP.node, barMin: 2, pad: MAP.gap,
      leftLabel: 0, rightLabel: MAP.gutter, slot: MAP.slot,
    }),
    [model, width],
  );
  const drawnNodes = [...model.nodes.values()];
  const firstKey = drawnNodes.find((node) => node.node.kind === "account")?.key ?? drawnNodes[0]?.key ?? null;
  const active = focusKey && model.nodes.has(focusKey) ? focusKey : firstKey;
  const litLinks = new Set<string>();
  if (hover) {
    const node = model.nodes.get(hover);
    node?.inbound.forEach((link) => litLinks.add(link.key));
    node?.outbound.forEach((link) => litLinks.add(link.key));
  }
  const bottom = Math.max(0, ...[...layout.nodes.values()].map((node) => node.y1), ...drawnNodes.map((node) => layout.nodes.get(node.key)!.y0 + MAP.slot));
  const height = MAP.top + bottom + 8;
  const accountColumns = drawnNodes.filter((node) => node.node.kind === "account").map((node) => node.column);
  const firstAccount = accountColumns.length ? Math.min(...accountColumns) : -1;

  const head = (column: number): string => {
    const sample = drawnNodes.filter((node) => node.column === column);
    if (sample.some((node) => node.node.kind === "account")) return column === firstAccount ? t("accounts") : t("then");
    if (column === 0) return t("from");
    return t("usedOrKept");
  };
  // Each column's labels may run to the next column's bar, the last column's to the gutter's edge.
  const room = (column: number, x1: number): number => {
    const next = layout.columnX[column + 1];
    return (next === undefined ? width - 4 : next - MAP.labelGap) - (x1 + MAP.labelGap);
  };

  const onKey = (event: KeyboardEvent<SVGGElement>, key: string, nodeId: string) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onNode(nodeId);
      return;
    }
    const direction = DIRECTIONS[event.key];
    if (!direction) return;
    event.preventDefault();
    const next = nextNode(layout, key, direction);
    if (!next) return;
    setFocusKey(next);
    ref.current?.querySelector<SVGGElement>(`[data-node="${CSS.escape(next)}"]`)?.focus();
  };
  const dimmed = (link: DrawnLink) => (highlight === "all" ? hover !== null && !litLinks.has(link.key) : !highlightMatches(link.kind, highlight));
  const serverNodes = new Map(data.nodes.map((node) => [node.id, node]));
  const name = (key: string) => model.nodes.get(key)?.node.label ?? key;

  return (
    <div ref={ref} className="flow">
      <svg width={width} height={height} viewBox={`-4 0 ${width + 8} ${height}`} role="group" aria-label={t("mapLabel")}>
        <defs>
          <pattern id="flow-stripe" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <rect width="2" height="6" fill="var(--tr-solid)" />
          </pattern>
          <pattern id="flow-dots" width="5" height="5" patternUnits="userSpaceOnUse">
            <circle cx="2.5" cy="2.5" r="1.1" fill="var(--inv-solid)" />
          </pattern>
          <pattern id="flow-hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)">
            <rect width="1.5" height="5" fill="var(--warn-solid)" />
          </pattern>
        </defs>
        {layout.columnX.map((x, column) => (
          <text key={column} x={x} y={12} className="lbl-amt col-head" aria-hidden="true">{head(column).toLocaleUpperCase()}</text>
        ))}
        <g transform={`translate(0,${MAP.top})`}>
          <g aria-hidden="true">
            {model.links.map((link) => {
              const placed = layout.links.find((item) => item.key === link.key);
              if (!placed) return null;
              const off = dimmed(link);
              const pattern = bandPattern(link);
              return (
                <g key={link.key} data-band={link.kind}>
                  <path d={placed.path} fill={BAND_COLOUR[link.kind]} fillOpacity={off ? 0.06 : 0.3}>
                    <title>{`${name(link.source)} → ${name(link.target)}: ${money(link.value)}`}</title>
                  </path>
                  {pattern && <path d={placed.path} fill={`url(#flow-${pattern})`} opacity={off ? 0.15 : 0.7} pointerEvents="none" />}
                </g>
              );
            })}
          </g>
          {drawnNodes.map((node) => {
            const placed = layout.nodes.get(node.key)!;
            const totals = nodeTotals(node.nodeId, data.links);
            const full = node.node.kind === "source"
              ? t("sourceName", { label: node.node.label, amount: money(totals.outbound) })
              : t("nodeName", { label: node.node.label, in: money(totals.inbound), out: money(totals.outbound) });
            const amount = money(node.value);
            const space = room(placed.column, placed.x1) - labelWidth(amount) - MAP.labelGap;
            const shownName = fitLabel(node.node.label, space);
            const server = serverNodes.get(node.nodeId);
            return (
              <g
                key={node.key}
                data-node={node.key}
                role="button"
                tabIndex={node.key === active ? 0 : -1}
                aria-label={full}
                className="flow-node"
                onClick={() => onNode(node.nodeId)}
                onKeyDown={(event) => onKey(event, node.key, node.nodeId)}
                onFocus={() => {
                  setFocusKey(node.key);
                  setHover(node.key);
                }}
                onBlur={() => setHover(null)}
                onMouseEnter={() => setHover(node.key)}
                onMouseLeave={() => setHover(null)}
              >
                <title>{server?.label ?? node.node.label}</title>
                <rect x={placed.x0} y={placed.y0} width={placed.x1 - placed.x0} height={Math.max(placed.y1 - placed.y0, 2)} className="flow-bar" />
                <text x={placed.x1 + MAP.labelGap} y={placed.y0 + 14} className="flow-label" aria-hidden="true" data-room={Math.round(room(placed.column, placed.x1))}>
                  <tspan className="lbl-node">{shownName}</tspan>
                  <tspan className="lbl-amt" dx={MAP.labelGap}>{amount}</tspan>
                </text>
                <rect x={placed.x0 - 4} y={placed.y0 - 4} width={placed.x1 - placed.x0 + 8} height={Math.max(placed.y1 - placed.y0, 2) + 8} className="flow-ring" />
              </g>
            );
          })}
        </g>
      </svg>
      {model.hiddenLinkKeys.some((key) => data.links.find((link) => link.key === key)?.kind === "transfer") && (
        <p className="caption">{t("hiddenTransfers")}</p>
      )}
    </div>
  );
}

function kindLabel(t: ReturnType<typeof useTranslations<"analysis.flow">>, kind: FlowLinkKind): string {
  return t(`kinds.${kind}`);
}

function FlowTable({ data, nodes, money, highlight, onLink }: { data: FlowResponse; nodes: Map<string, FlowNode>; money: Money; highlight: Highlight; onLink: (key: string) => void }) {
  const t = useTranslations("analysis.flow");
  const table = useTranslations("analysis.flowTable");
  const format = useFormat();
  const rows = [...data.links].sort((a, b) => b.amount - a.amount || a.key.localeCompare(b.key)).filter((link) => highlight === "all" || highlightMatches(link.kind, highlight));
  return (
    <div className="tbl-scroll">
      <table className="tbl flow-tbl">
        <caption>{table("caption", { date: format.longDate(data.window.through) })}</caption>
        <thead>
          <tr>
            <th scope="col">{table("fromTo")}</th>
            <th scope="col">{table("kind")}</th>
            <th scope="col" className="num">{table("amount")}</th>
            <th scope="col" className="num ph-hide">{table("count")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((link) => (
            <tr key={link.key}>
              <td>
                <button type="button" className="tbl-link" aria-haspopup="dialog" onClick={() => onLink(link.key)}>
                  {nodes.get(link.from)?.label ?? link.from} → {nodes.get(link.to)?.label ?? link.to}
                </button>
              </td>
              <td className="t-meta">
                {kindLabel(t, link.kind)}
                {link.waiting_count > 0 && link.kind !== "waiting" && ` · ${table("waitingPart", { count: link.waiting_count })}`}
              </td>
              <td className="num"><MaskedMoney text={money(link.amount)} className="t-amt" /></td>
              <td className="num ph-hide t-amt">{BALANCE_KINDS.has(link.kind) ? "–" : format.integer(link.count)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FlowDetails({
  target,
  data,
  nodes,
  basis,
  offset,
  money,
  onClose,
  onLink,
  onRow,
}: {
  target: Target | null;
  data: FlowResponse;
  nodes: Map<string, FlowNode>;
  basis: PeriodBasis;
  offset: number;
  money: Money;
  onClose: () => void;
  onLink: (key: string) => void;
  onRow: (id: string) => void;
}) {
  const t = useTranslations("analysis");
  const common = useTranslations("common");
  const format = useFormat();
  const through = format.longDate(data.window.through);
  let title = "";
  let meta: string | undefined;
  let body: React.ReactNode = null;
  if (target?.type === "node") {
    const node = nodes.get(target.id);
    if (node) {
      const inbound = data.links.filter((link) => link.to === node.id);
      const outbound = data.links.filter((link) => link.from === node.id);
      const totals = nodeTotals(node.id, data.links);
      const holder = node.kind === "account" || node.kind === "saving" || node.kind === "investment";
      const periodQuery = `period=${basis}${offset ? `&offset=${offset}` : ""}`;
      title = node.label;
      meta = t("flowDetail.until", { date: through });
      body = (
        <div className="form-stack">
          <dl className="dl">
            <dt>{t("flowDetail.in")}</dt><dd><MaskedMoney text={money(totals.inbound)} className="t-amt" /></dd>
            <dt>{t("flowDetail.out")}</dt><dd><MaskedMoney text={money(totals.outbound)} className="t-amt" /></dd>
            {holder && <><dt>{t("flowDetail.opening")}</dt><dd><MaskedMoney text={money(node.opening ?? 0)} className="t-amt" /></dd></>}
            {holder && <><dt>{t("flowDetail.closing")}</dt><dd><MaskedMoney text={money(node.closing ?? 0)} className="t-amt" /></dd></>}
          </dl>
          {holder && <p className="t-meta">{t("flowDetail.balanceRule")}</p>}
          {node.members && node.members.length > 0 && <p className="t-meta">{t("flowDetail.members", { names: node.members.join(", ") })}</p>}
          <LinkList title={t("flowDetail.inFrom")} links={inbound} nodes={nodes} money={money} side="from" onLink={onLink} />
          <LinkList title={t("flowDetail.outTo")} links={outbound} nodes={nodes} money={money} side="to" onLink={onLink} />
          {(node.kind === "waiting" || holder) && (
            <div className="panel-actions">
              {node.kind === "waiting" && <Link className="ws-btn" href="/review?folder=pindah">{t("flowDetail.answer")}</Link>}
              {holder && node.account_id && <Link className="ws-btn" href={`/ledger?account=${node.account_id}&${periodQuery}`}>{t("flowDetail.allRows")}</Link>}
            </div>
          )}
        </div>
      );
    }
  } else if (target?.type === "link") {
    const link = data.links.find((item) => item.key === target.key);
    if (link) {
      title = `${nodes.get(link.from)?.label ?? link.from} → ${nodes.get(link.to)?.label ?? link.to}`;
      meta = `${t(`flow.kinds.${link.kind}`)} · ${t("flowDetail.until", { date: through })}`;
      body = (
        <div className="form-stack">
          <dl className="dl">
            <dt>{t("flowDetail.amount")}</dt><dd><MaskedMoney text={money(link.amount)} className="t-amt" /></dd>
            {!BALANCE_KINDS.has(link.kind) && <><dt>{t("flowDetail.count")}</dt><dd className="t-amt">{format.integer(link.count)}</dd></>}
            {link.waiting_count > 0 && (
              <><dt>{t("flowDetail.waiting")}</dt><dd>{t.rich("flowDetail.waitingValue", { count: link.waiting_count, amount: () => <MaskedMoney text={money(link.waiting_amount)} className="t-amt" /> })}</dd></>
            )}
          </dl>
          {BALANCE_KINDS.has(link.kind) ? (
            <p className="t-meta">{link.kind === "opening" || link.kind === "negative_opening" ? t("flowDetail.openingNote") : t("flowDetail.closingNote")}</p>
          ) : (
            <LinkRows linkKey={link.key} basis={basis} offset={offset} through={data.window.through} money={money} onRow={onRow} />
          )}
          {link.kind === "waiting" && <div className="panel-actions"><Link className="ws-btn" href="/review?folder=pindah">{t("flowDetail.answer")}</Link></div>}
        </div>
      );
    }
  }
  return (
    <ShellDialog open={Boolean(target && body)} onClose={onClose} title={title} meta={meta} width={560} closeLabel={common("close")}>
      {body}
    </ShellDialog>
  );
}

function LinkList({ title, links, nodes, money, side, onLink }: { title: string; links: FlowLink[]; nodes: Map<string, FlowNode>; money: Money; side: "from" | "to"; onLink: (key: string) => void }) {
  const t = useTranslations("analysis.flow");
  if (!links.length) return null;
  return (
    <div>
      <h3 className="t-label dlg-sub">{title}</h3>
      <ul className="list">
        {[...links].sort((a, b) => b.amount - a.amount).map((link) => (
          <li key={link.key}>
            <ListRow
              onClick={() => onLink(link.key)}
              layout="stacked"
              title={nodes.get(side === "from" ? link.from : link.to)?.label ?? ""}
              meta={kindLabel(t, link.kind)}
              amount={<MaskedMoney text={money(link.amount)} className="t-amt" />}
            />
          </li>
        ))}
      </ul>
    </div>
  );
}

function rowKindOf(type: string): AmountKind {
  if (type === "expense") return "expense";
  if (type === "income") return "income";
  if (type === "topup") return "topup";
  return "movement";
}

function LinkRows({ linkKey, basis, offset, through, money, onRow }: { linkKey: string; basis: PeriodBasis; offset: number; through: string; money: Money; onRow: (id: string) => void }) {
  const t = useTranslations("analysis.flowDetail");
  const format = useFormat();
  const query = useInfiniteQuery<FlowRowsResponse>({
    queryKey: ["dashboard", "flow-rows", basis, offset, through, linkKey],
    initialPageParam: 0,
    queryFn: ({ pageParam }) => api.get(`/dashboard/flow/rows?timeframe=${basis}&cycle_offset=${offset}&through=${through}&link=${encodeURIComponent(linkKey)}&offset=${pageParam}`),
    getNextPageParam: (last) => (last.offset + last.limit < last.total_count ? last.offset + last.limit : undefined),
  });
  if (query.isPending) return <div role="status" aria-busy="true"><span className="skel is-line" /><span className="skel is-line" /></div>;
  if (query.isError) return <ErrorState message={t("rowsFailed")} onRetry={() => query.refetch()} />;
  const rows = query.data.pages.flatMap((page) => page.rows);
  return (
    <div>
      <h3 className="t-label dlg-sub">{t("rows", { count: query.data.pages[0].total_count })}</h3>
      <ul className="list">
        {rows.map((row) => (
          <li key={`${row.id}-${row.link}`}>
            <ListRow
              onClick={() => onRow(row.id)}
              layout="stacked"
              rowKey={row.id}
              title={row.merchant || row.notes || row.category_name || t("noDescription")}
              meta={`${format.shortDate(row.local_date)} · ${row.account_name}${row.waiting ? ` · ${t("waitingRow")}` : ""}`}
              amount={<KindAmount kind={rowKindOf(row.type)} amount={row.amount} money={money} />}
            />
          </li>
        ))}
      </ul>
      {query.hasNextPage && (
        <div className="panel-actions">
          <button type="button" className="ws-btn" disabled={query.isFetchingNextPage} onClick={() => query.fetchNextPage()}>
            {query.isFetchingNextPage ? t("loadingMore") : t("more")}
          </button>
        </div>
      )}
    </div>
  );
}
