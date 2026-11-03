"use client";

import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import Link from "next/link";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";
import { TransactionDrawer } from "@/components/transactions/TransactionDrawer";
import { useTransactionUndo } from "@/components/transactions/useTransactionUndo";
import { Icon } from "@/components/ui/Icon";
import { Drawer } from "@/components/workspace/overlays";
import { Card, CardHeader, EmptyState, ErrorState, Segmented, Skeleton } from "@/components/workspace/primitives";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";
import { addDays, dayDiff, longDay, shortDay, todayIso } from "../format";
import styles from "../analysis.module.css";
import type { FlowLink, FlowLinkKind, FlowNode, FlowResponse, FlowRowsResponse, PeriodBasis } from "../types";
import { BALANCE_KINDS, buildFlowModel, type Direction, type Highlight, highlightMatches, layoutFlow, nextNode } from "./sankey";

type Money = (amount: number) => string;
export type FlowView = "peta" | "tabel";

const FOLD_KEY = "analysis.flow.folded";
const REPLAY_MS = 800;

const KIND_LABEL: Record<FlowLinkKind, string> = {
  income: "Uang masuk",
  opening: "Saldo sebelum periode",
  negative_opening: "Saldo minus sebelum periode",
  negative_closing: "Saldo minus di akhir",
  transfer: "Pindah saldo",
  spend: "Belanja",
  saving: "Tabungan",
  investment: "Investasi",
  other: "Tidak dihitung belanja",
  waiting: "Menunggu pasangan",
  remaining: "Masih ada",
};

const HIGHLIGHTS: { value: Highlight; label: string }[] = [
  { value: "all", label: "Semua" },
  { value: "spend", label: "Belanja" },
  { value: "transfer", label: "Pindah saldo" },
  { value: "save", label: "Tabungan & investasi" },
  { value: "waiting", label: "Menunggu pasangan" },
];

const PILLAR_COLOR: Record<string, string> = {
  need: "var(--k-need)", want: "var(--k-want)", culture: "var(--k-culture)", unexpected: "var(--k-unexpected)",
  saving: "var(--k-saving)", unclassified: "var(--k-none)",
};

function nodeColor(node: FlowNode): string {
  if (node.kind === "pillar" && node.pillar) return PILLAR_COLOR[node.pillar];
  if (node.kind === "saving") return "var(--k-saving)";
  if (node.kind === "investment") return "var(--flow-inv)";
  if (node.kind === "waiting") return "var(--flow-warn)";
  if (node.kind === "other_out") return "var(--flow-other)";
  if (node.kind === "remaining" || node.id === "opening" || node.id === "neg_closing" || node.kind === "negative_opening") return "var(--flow-left)";
  return "var(--flow-in)";
}

function linkColor(kind: FlowLinkKind, target: FlowNode | undefined): string {
  if (kind === "spend" && target) return nodeColor(target);
  if (kind === "transfer") return "var(--flow-tr)";
  if (kind === "saving") return "var(--k-saving)";
  if (kind === "investment") return "var(--flow-inv)";
  if (kind === "waiting") return "var(--flow-warn)";
  if (kind === "other") return "var(--flow-other)";
  if (kind === "income") return "var(--flow-in)";
  return "var(--flow-left)";
}

function readFold(): boolean {
  try {
    return window.localStorage.getItem(FOLD_KEY) !== "open";
  } catch {
    return true;
  }
}

function writeFold(folded: boolean) {
  try {
    window.localStorage.setItem(FOLD_KEY, folded ? "folded" : "open");
  } catch {
    // A blocked storage only forgets the preference; the map still works.
  }
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

function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const query = window.matchMedia("(prefers-reduced-motion: reduce)");
    setReduced(query.matches);
    const listener = () => setReduced(query.matches);
    query.addEventListener?.("change", listener);
    return () => query.removeEventListener?.("change", listener);
  }, []);
  return reduced;
}

type DrawerTarget = { type: "node"; id: string } | { type: "link"; key: string };

export function flowQueryKey(basis: PeriodBasis, offset: number, through: string | null) {
  return ["dashboard", "flow", basis, offset, through ?? "default"] as const;
}

function flowPath(basis: PeriodBasis, offset: number, through: string | null): string {
  return `/dashboard/flow?timeframe=${basis}&cycle_offset=${offset}${through ? `&through=${through}` : ""}`;
}

export function PetaAliranTab({
  basis,
  offset,
  money,
  view,
  onView,
  onLabel,
}: {
  basis: PeriodBasis;
  offset: number;
  money: Money;
  view: FlowView;
  onView: (view: FlowView) => void;
  onLabel: (label: string) => void;
}) {
  const queryClient = useQueryClient();
  const rootRef = useRef<HTMLDivElement>(null);
  const phone = useWidthAtMost(rootRef, 760);
  const reduced = usePrefersReducedMotion();
  const { record } = useTransactionUndo();
  const [through, setThrough] = useState<string | null>(null);
  const [shown, setShown] = useState<FlowResponse | null>(null);
  const [playing, setPlaying] = useState(false);
  const [highlight, setHighlight] = useState<Highlight>("all");
  const [folded, setFolded] = useState(true);
  const [drawer, setDrawer] = useState<DrawerTarget | null>(null);
  const [txId, setTxId] = useState<string | null>(null);
  const [replayError, setReplayError] = useState(false);

  useEffect(() => setFolded(readFold()), []);
  useEffect(() => {
    setThrough(null);
    setPlaying(false);
    setShown(null);
  }, [basis, offset]);

  const query = useQuery<FlowResponse>({
    queryKey: flowQueryKey(basis, offset, through),
    queryFn: () => api.get(flowPath(basis, offset, through)),
  });
  useEffect(() => {
    if (query.data) {
      setShown(query.data);
      setReplayError(false);
      if (query.data.window.label) onLabel(query.data.window.label);
    }
  }, [query.data, onLabel]);
  useEffect(() => {
    if (query.isError && shown) {
      setReplayError(true);
      setPlaying(false);
    }
  }, [query.isError, shown]);

  const data = shown;
  const lastDay = useMemo(() => {
    if (!data) return null;
    const today = todayIso();
    return data.window.is_current && today < data.window.end ? today : data.window.end;
  }, [data]);
  const current = data ? data.window.through : null;

  // Replay waits for each day to load before moving on (OD45); the next day is fetched ahead.
  useEffect(() => {
    if (!playing || !data || !lastDay || query.isFetching) return;
    if (data.window.through >= lastDay) {
      setPlaying(false);
      return;
    }
    const next = addDays(data.window.through, 1);
    void queryClient.prefetchQuery({ queryKey: flowQueryKey(basis, offset, next), queryFn: () => api.get(flowPath(basis, offset, next)) });
    const timer = window.setTimeout(() => setThrough(next), REPLAY_MS);
    return () => window.clearTimeout(timer);
  }, [playing, data, lastDay, query.isFetching, basis, offset, queryClient]);

  const toggleFold = () => {
    setFolded((value) => {
      writeFold(!value);
      return !value;
    });
  };

  if (!data) {
    if (query.isError) return <ErrorState message="Peta aliran belum bisa dimuat." onRetry={() => query.refetch()} />;
    return <div className={styles.stack} aria-busy="true"><Skeleton style={{ height: 460 }} /></div>;
  }

  const days = lastDay ? dayDiff(data.window.start, lastDay) + 1 : 1;
  const index = current ? dayDiff(data.window.start, current) + 1 : days;
  const nodes = new Map(data.nodes.map((node) => [node.id, node]));
  const remaining = data.summary.remaining;
  const empty = data.links.every((link) => BALANCE_KINDS.has(link.kind));
  const showMap = view === "peta" && !phone;

  return (
    <div ref={rootRef} className={styles.stack}>
      <Card labelledBy="flow-heading">
        <CardHeader
          id="flow-heading"
          icon="layers"
          title="Aliran uang periode ini"
          caption="Tebal aliran menunjukkan jumlah uang. Pilih titik untuk melihat transaksinya; Tab ke peta lalu pakai tombol panah."
          action={<Segmented label="Tampilan" options={[{ value: "peta", label: "Peta" }, { value: "tabel", label: "Tabel" }]} value={view} onChange={onView} />}
        />
        <div className={styles.flowTools}>
          <div className="ws-chips" role="group" aria-label="Sorot aliran">
            {HIGHLIGHTS.map((option) => (
              <button key={option.value} type="button" className="ws-chip" aria-pressed={highlight === option.value} onClick={() => setHighlight(option.value)}>
                {option.label}
              </button>
            ))}
          </div>
          <button type="button" className="ws-btn ws-btn-sm" aria-pressed={!folded} onClick={toggleFold}>
            <Icon name={folded ? "chevron-right" : "chevron-down"} className="h-4 w-4" />
            {folded ? "Tampilkan uang masuk, saldo dan Masih ada" : "Ciutkan uang masuk, saldo dan Masih ada"} · {money(remaining)}
          </button>
        </div>
        <p className={styles.flowNote}>
          <Icon name="info" className="h-4 w-4" />
          <span>
            {folded
              ? "Peta hanya menampilkan uang yang keluar dan dipindah, agar aliran belanja terbaca. Uang masuk, saldo awal dan Masih ada tetap tercantum di tabel."
              : "Uang masuk dan saldo sebelum periode datang dari kiri, dan Masih ada adalah saldo tiap rekening di akhir hari yang dipilih."}
          </span>
        </p>
        {empty ? (
          <EmptyState icon="layers" title="Belum ada uang yang bergerak">Aliran muncul setelah ada transaksi di periode ini.</EmptyState>
        ) : (
          <>
            {showMap && (
              <FlowMap
                data={data}
                folded={folded}
                highlight={highlight}
                money={money}
                onNode={(id) => setDrawer({ type: "node", id })}
              />
            )}
            {phone && view === "peta" && <MiniMap data={data} folded={folded} />}
            {(view === "tabel" || phone) && (
              <FlowTable data={data} nodes={nodes} money={money} highlight={highlight} onLink={(key) => setDrawer({ type: "link", key })} />
            )}
            <FlowLegend />
          </>
        )}
        <div className={styles.scrub}>
          <button
            type="button"
            className="ws-btn ws-btn-sm"
            disabled={reduced || days <= 1}
            aria-pressed={playing}
            onClick={() => {
              if (!playing && current && lastDay && current >= lastDay) setThrough(data.window.start);
              setPlaying((value) => !value);
            }}
          >
            {playing ? "Jeda" : "Putar ulang"}
          </button>
          <div>
            <label className="sr-only" htmlFor="flow-replay">Putar ulang per hari</label>
            <input
              id="flow-replay"
              type="range"
              min={1}
              max={days}
              step={1}
              value={index}
              aria-valuetext={current ? `${longDay(current)}, hari ke-${index}` : undefined}
              onChange={(event) => {
                setPlaying(false);
                setThrough(addDays(data.window.start, Number(event.target.value) - 1));
              }}
            />
          </div>
          <div className={styles.scrubRead} aria-live="polite">
            <b>{current ? longDay(current) : ""}</b>
            hari ke-{index} dari {days}
            {replayError && <span role="alert"> · Hari ini belum bisa dimuat.</span>}
          </div>
        </div>
        {reduced && <p className={styles.caption} style={{ marginTop: 6 }}>Gerakan dikurangi: geser tanggal untuk melihat hari per hari.</p>}
      </Card>
      <div className={styles.sums}>
        <Card className={styles.sum} label="Belanja">
          <span className={styles.caption}>Belanja</span>
          <b>{money(data.summary.belanja)}</b>
          {data.summary.reimbursement_offset > 0 && (
            <span className={styles.caption}>Peta menampilkan {money(data.summary.spend_gross)} sebelum penggantian {money(data.summary.reimbursement_offset)}.</span>
          )}
        </Card>
        <Card className={styles.sum} label="Ditabung dan top up">
          <span className={styles.caption}>Ditabung & top up</span>
          <b>{money(data.summary.saving_net + data.summary.investment_net)}</b>
        </Card>
        <Card className={styles.sum} label="Menunggu pasangan">
          <span className={styles.caption}>Menunggu pasangan</span>
          <b>{money(data.summary.waiting_out + data.summary.waiting_in)}</b>
          {data.summary.waiting_in_buckets > 0 && (
            <span className={styles.caption}>Ditambah {money(data.summary.waiting_in_buckets)} menunggu jawaban di dalam pilarnya.</span>
          )}
        </Card>
        <Card className={styles.sum} label="Masih ada">
          <span className={styles.caption}>Masih ada</span>
          <b>{money(remaining)}</b>
        </Card>
      </div>
      {drawer && !txId && (
        <FlowDrawer
          target={drawer}
          data={data}
          nodes={nodes}
          basis={basis}
          offset={offset}
          money={money}
          onClose={() => setDrawer(null)}
          onLink={(key) => setDrawer({ type: "link", key })}
          onRow={setTxId}
        />
      )}
      <TransactionDrawer transactionId={txId} onClose={() => setTxId(null)} record={record} />
    </div>
  );
}

function FlowLegend() {
  return (
    <div className={styles.flowLegend}>
      <span><i className={styles.swatch} style={{ background: "var(--flow-in)" }} />Uang masuk</span>
      <span><i className={cn(styles.swatch, styles.stripe)} style={{ background: "var(--flow-tr)" }} />Pindah saldo (garis miring)</span>
      <span><i className={styles.swatch} style={{ background: "var(--k-need)" }} />Belanja, warna per pilar</span>
      <span><i className={styles.swatch} style={{ background: "var(--k-saving)" }} />Tabungan</span>
      <span><i className={cn(styles.swatch, styles.dots)} style={{ background: "var(--flow-inv)" }} />Investasi (titik)</span>
      <span><i className={cn(styles.swatch, styles.hatch)} style={{ background: "var(--flow-warn)" }} />Menunggu jawaban atau pasangan (arsir)</span>
    </div>
  );
}

const DIRECTIONS: Record<string, Direction> = { ArrowUp: "up", ArrowDown: "down", ArrowLeft: "left", ArrowRight: "right" };

function FlowMap({
  data,
  folded,
  highlight,
  money,
  onNode,
}: {
  data: FlowResponse;
  folded: boolean;
  highlight: Highlight;
  money: Money;
  onNode: (id: string) => void;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(960);
  const [focusKey, setFocusKey] = useState<string | null>(null);
  const [hover, setHover] = useState<string | null>(null);
  useEffect(() => {
    const host = hostRef.current;
    if (!host || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[entries.length - 1];
      if (entry) setWidth(Math.max(720, Math.round(entry.contentRect.width)));
    });
    observer.observe(host);
    return () => observer.disconnect();
  }, []);

  const model = useMemo(() => buildFlowModel(data.nodes, data.links, { folded }), [data, folded]);
  const height = folded ? 420 : 520;
  const layout = useMemo(
    () => layoutFlow(model, {
      width, height, stationWidth: 118, stationMin: 46, barWidth: 12, barMin: 3, pad: 12, rightLabel: 220,
      // Room for labels on the left only when something is drawn there.
      leftLabel: [...model.nodes.values()].some((node) => node.column === 0 && node.node.kind !== "account") ? 150 : 8,
    }),
    [model, width, height],
  );
  const serverNodes = useMemo(() => new Map(data.nodes.map((node) => [node.id, node])), [data.nodes]);
  const drawnNodes = [...model.nodes.values()];
  const firstKey = drawnNodes.find((node) => node.node.kind === "account")?.key ?? drawnNodes[0]?.key ?? null;
  const active = focusKey && model.nodes.has(focusKey) ? focusKey : firstKey;
  const lit = hover ?? null;
  const litLinks = new Set<string>();
  if (lit) {
    const node = model.nodes.get(lit);
    node?.inbound.forEach((link) => litLinks.add(link.key));
    node?.outbound.forEach((link) => litLinks.add(link.key));
  }
  const dim = Boolean(lit) || highlight !== "all";
  const top = 28;

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
    hostRef.current?.querySelector<SVGGElement>(`[data-node="${CSS.escape(next)}"]`)?.focus();
  };

  // Labels: sources on the left of their bar, everything else on the right; pushed apart to stay legible.
  const labels = drawnNodes
    .filter((node) => node.node.kind !== "account")
    .map((node) => {
      const placed = layout.nodes.get(node.key)!;
      const left = node.column === 0;
      return { node, placed, left, y: (placed.y0 + placed.y1) / 2 };
    })
    .sort((a, b) => a.y - b.y);
  for (const side of [true, false]) {
    const set = labels.filter((label) => label.left === side);
    for (let index = 1; index < set.length; index++) {
      if (set[index].placed.column === set[index - 1].placed.column && set[index].y < set[index - 1].y + 30) set[index].y = set[index - 1].y + 30;
    }
  }

  return (
    <div ref={hostRef} className={styles.mapWide}>
      <svg className={cn(styles.map, dim && styles.dim)} viewBox={`0 0 ${width} ${height + top + 12}`} role="group" aria-label="Peta aliran uang. Tab ke titik pertama, lalu pakai tombol panah.">
        <defs>
          <pattern id="flow-stripe" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="6" className={styles.patternInk} strokeWidth="2" /></pattern>
          <pattern id="flow-dots" width="5" height="5" patternUnits="userSpaceOnUse"><circle cx="2.5" cy="2.5" r="1.05" className={styles.patternDot} /></pattern>
          <pattern id="flow-hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)"><line x1="0" y1="0" x2="0" y2="5" className={styles.patternInk} strokeWidth="1.4" /></pattern>
        </defs>
        <g transform={`translate(0,${top})`}>
          {layout.columnX.map((x, column) => {
            const sample = drawnNodes.find((node) => node.column === column);
            if (!sample) return null;
            const kind = sample.node.kind;
            const firstAccount = drawnNodes.filter((node) => node.node.kind === "account").reduce((min, node) => Math.min(min, node.column), Infinity);
            const text = kind === "source" || sample.key.endsWith("@in") ? "Dari" : kind === "account" ? (column === firstAccount ? "Rekening" : "Lalu ke") : kind === "remaining" ? "Masih ada" : "Dipakai atau disimpan";
            const left = text === "Dari";
            return <text key={column} x={left ? x + layout.columnWidth[column] : x} y={-12} className={styles.columnHead} textAnchor={left ? "end" : "start"} aria-hidden="true">{text}</text>;
          })}
          <g aria-hidden="true">
            {model.links.map((link) => {
              const placed = layout.links.find((item) => item.key === link.key);
              if (!placed) return null;
              const target = serverNodes.get(model.nodes.get(link.target)?.nodeId ?? "");
              const match = highlight === "all" ? litLinks.has(link.key) : highlightMatches(link.kind, highlight);
              return (
                <g key={link.key}>
                  <path d={placed.path} className={cn(styles.ribbon, match && styles.on)} style={{ fill: linkColor(link.kind, target) }} />
                  {link.pattern && <path d={placed.path} className={cn(styles.ribbonPattern, match && styles.on)} fill={`url(#flow-${link.pattern})`} />}
                </g>
              );
            })}
          </g>
          {drawnNodes.map((node) => {
            const placed = layout.nodes.get(node.key)!;
            const totals = nodeTotals(node.nodeId, data.links);
            const name = node.node.kind === "source"
              ? `${node.node.label}: masuk ${money(totals.outbound)}`
              : `${node.node.label}: masuk ${money(totals.inbound)}, keluar ${money(totals.outbound)}`;
            const match = highlight === "all"
              ? lit === node.key || [...node.inbound, ...node.outbound].some((link) => litLinks.has(link.key))
              : [...node.inbound, ...node.outbound].some((link) => highlightMatches(link.kind, highlight));
            const station = node.node.kind === "account";
            const label = labels.find((item) => item.node.key === node.key);
            return (
              <g
                key={node.key}
                data-node={node.key}
                role="button"
                tabIndex={node.key === active ? 0 : -1}
                aria-label={`${name}. Enter untuk rincian.`}
                className={cn(styles.node, match && styles.on)}
                onClick={() => onNode(node.nodeId)}
                onKeyDown={(event) => onKey(event, node.key, node.nodeId)}
                onFocus={() => { setFocusKey(node.key); setHover(node.key); }}
                onBlur={() => setHover(null)}
                onMouseEnter={() => setHover(node.key)}
                onMouseLeave={() => setHover(null)}
              >
                {station ? (
                  <>
                    <rect x={placed.x0} y={placed.y0} width={placed.x1 - placed.x0} height={placed.y1 - placed.y0} rx={8} className={styles.station} />
                    {placed.y1 - placed.y0 >= 30 && (
                      <>
                        <text x={placed.x0 + 10} y={placed.y0 + 17} className={styles.labelName} aria-hidden="true">{node.node.label.length > 15 ? `${node.node.label.slice(0, 14)}…` : node.node.label}</text>
                        <text x={placed.x0 + 10} y={placed.y0 + 31} className={styles.labelAmount} aria-hidden="true">{money(node.node.closing ?? 0)}</text>
                      </>
                    )}
                  </>
                ) : (
                  <rect x={placed.x0} y={placed.y0} width={placed.x1 - placed.x0} height={Math.max(placed.y1 - placed.y0, 1.5)} rx={2} style={{ fill: nodeColor(node.node) }} />
                )}
                {node.node.kind === "waiting" && (
                  <rect x={placed.x0 - 1.5} y={placed.y0 - 1.5} width={placed.x1 - placed.x0 + 3} height={Math.max(placed.y1 - placed.y0, 1.5) + 3} rx={2} className={styles.pendingOutline} />
                )}
                {label && (
                  <g aria-hidden="true">
                    <text x={label.left ? placed.x0 - 10 : placed.x1 + 10} y={label.y - 2} textAnchor={label.left ? "end" : "start"} className={cn(styles.labelName, styles.halo)}>{node.node.label}</text>
                    <text x={label.left ? placed.x0 - 10 : placed.x1 + 10} y={label.y + 12} textAnchor={label.left ? "end" : "start"} className={cn(styles.labelAmount, styles.halo)}>{money(node.value)}</text>
                  </g>
                )}
                <rect x={placed.x0 - 4} y={placed.y0 - 4} width={placed.x1 - placed.x0 + 8} height={Math.max(placed.y1 - placed.y0, 2) + 8} rx={6} className={styles.ring} />
              </g>
            );
          })}
        </g>
      </svg>
      {model.hiddenLinkKeys.some((key) => data.links.find((link) => link.key === key)?.kind === "transfer") && (
        <p className={styles.caption}>Sebagian pindah saldo yang berputar antar rekening hanya tercantum di tabel.</p>
      )}
    </div>
  );
}

/** A small decorative map above the phone table; the table carries every figure (D10). */
function MiniMap({ data, folded }: { data: FlowResponse; folded: boolean }) {
  const model = useMemo(() => buildFlowModel(data.nodes, data.links, { folded }), [data, folded]);
  const layout = useMemo(() => layoutFlow(model, { width: 640, height: 260, stationWidth: 40, stationMin: 16, barWidth: 8, barMin: 2, pad: 8, leftLabel: 4, rightLabel: 4 }), [model]);
  const serverNodes = new Map(data.nodes.map((node) => [node.id, node]));
  return (
    <div className={styles.mini} aria-hidden="true">
      <svg className={styles.map} viewBox="0 0 640 270">
        {model.links.map((link) => {
          const placed = layout.links.find((item) => item.key === link.key);
          const target = serverNodes.get(model.nodes.get(link.target)?.nodeId ?? "");
          return placed ? <path key={link.key} d={placed.path} className={styles.ribbon} style={{ fill: linkColor(link.kind, target) }} /> : null;
        })}
        {[...model.nodes.values()].map((node) => {
          const placed = layout.nodes.get(node.key)!;
          return <rect key={node.key} x={placed.x0} y={placed.y0} width={placed.x1 - placed.x0} height={Math.max(placed.y1 - placed.y0, 1.5)} rx={3} className={node.node.kind === "account" ? styles.station : undefined} style={node.node.kind === "account" ? undefined : { fill: nodeColor(node.node) }} />;
        })}
      </svg>
    </div>
  );
}

function FlowTable({
  data,
  nodes,
  money,
  highlight,
  onLink,
}: {
  data: FlowResponse;
  nodes: Map<string, FlowNode>;
  money: Money;
  highlight: Highlight;
  onLink: (key: string) => void;
}) {
  const rows = [...data.links].sort((a, b) => b.amount - a.amount || a.key.localeCompare(b.key))
    .filter((link) => highlight === "all" || highlightMatches(link.kind, highlight));
  return (
    <div className={styles.tableScroll}>
      <table className={styles.table}>
        <caption>Semua aliran sampai {longDay(data.window.through)}, termasuk saldo awal dan Masih ada</caption>
        <thead>
          <tr><th scope="col">Dari → Ke</th><th scope="col">Jenis</th><th scope="col" className={styles.r}>Jumlah</th><th scope="col" className={styles.r}>Transaksi</th></tr>
        </thead>
        <tbody>
          {rows.map((link) => (
            <tr key={link.key}>
              <td>
                <button type="button" className={styles.linkButton} onClick={() => onLink(link.key)}>
                  {nodes.get(link.from)?.label ?? link.from} → {nodes.get(link.to)?.label ?? link.to}
                </button>
              </td>
              <td>
                {KIND_LABEL[link.kind]}
                {link.waiting_count > 0 && link.kind !== "waiting" && <span className={styles.caption}> · termasuk {link.waiting_count} menunggu jawaban</span>}
              </td>
              <td className={styles.r}>{money(link.amount)}</td>
              <td className={styles.r}>{BALANCE_KINDS.has(link.kind) ? "–" : link.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FlowDrawer({
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
  target: DrawerTarget;
  data: FlowResponse;
  nodes: Map<string, FlowNode>;
  basis: PeriodBasis;
  offset: number;
  money: Money;
  onClose: () => void;
  onLink: (key: string) => void;
  onRow: (id: string) => void;
}) {
  if (target.type === "node") {
    const node = nodes.get(target.id);
    if (!node) return null;
    const inbound = data.links.filter((link) => link.to === node.id);
    const outbound = data.links.filter((link) => link.from === node.id);
    const totals = nodeTotals(node.id, data.links);
    const holder = node.kind === "account" || node.kind === "saving" || node.kind === "investment";
    const periodQuery = `period=${basis}${offset ? `&offset=${offset}` : ""}`;
    return (
      <Drawer open onClose={onClose} title={node.label} subtitle={`Sampai ${longDay(data.window.through)}`}>
        <dl className={styles.drawerFacts}>
          <dt>Masuk</dt><dd>{money(totals.inbound)}</dd>
          <dt>Keluar</dt><dd>{money(totals.outbound)}</dd>
          {holder && <><dt>Saldo sebelum periode</dt><dd>{money(node.opening ?? 0)}</dd></>}
          {holder && <><dt>Saldo di akhir hari</dt><dd>{money(node.closing ?? 0)}</dd></>}
        </dl>
        {holder && <p className={styles.caption}>Saldo awal ditambah yang masuk sama dengan yang keluar ditambah saldo di akhir hari.</p>}
        {node.members && node.members.length > 0 && <p className={styles.caption}>Termasuk {node.members.join(", ")}.</p>}
        <LinkList title="Masuk dari" links={inbound} nodes={nodes} money={money} side="from" onLink={onLink} />
        <LinkList title="Keluar ke" links={outbound} nodes={nodes} money={money} side="to" onLink={onLink} />
        {node.kind === "waiting" && <p style={{ marginTop: 12 }}><Link className="ws-btn ws-btn-sm" href="/review?folder=pindah">Jawab di Tinjau</Link></p>}
        {holder && node.account_id && (
          <p style={{ marginTop: 12 }}><Link className="ws-linkbtn" href={`/ledger?account=${node.account_id}&${periodQuery}`}>Lihat semua transaksi rekening ini</Link></p>
        )}
      </Drawer>
    );
  }
  const link = data.links.find((item) => item.key === target.key);
  if (!link) return null;
  const from = nodes.get(link.from)?.label ?? link.from;
  const to = nodes.get(link.to)?.label ?? link.to;
  return (
    <Drawer open onClose={onClose} title={`${from} → ${to}`} subtitle={`${KIND_LABEL[link.kind]} · sampai ${longDay(data.window.through)}`}>
      <dl className={styles.drawerFacts}>
        <dt>Jumlah</dt><dd>{money(link.amount)}</dd>
        {!BALANCE_KINDS.has(link.kind) && <><dt>Transaksi</dt><dd>{link.count}</dd></>}
        {link.waiting_count > 0 && <><dt>Menunggu jawaban</dt><dd>{link.waiting_count} · {money(link.waiting_amount)}</dd></>}
      </dl>
      {BALANCE_KINDS.has(link.kind) ? (
        <p className={styles.caption}>
          {link.kind === "opening" || link.kind === "negative_opening"
            ? "Saldo buku kas rekening ini sebelum periode dimulai."
            : "Saldo buku kas rekening ini di akhir hari yang dipilih."}
        </p>
      ) : (
        <LinkRows linkKey={link.key} basis={basis} offset={offset} through={data.window.through} money={money} onRow={onRow} />
      )}
      {link.kind === "waiting" && <p style={{ marginTop: 12 }}><Link className="ws-btn ws-btn-sm" href="/review?folder=pindah">Jawab di Tinjau</Link></p>}
    </Drawer>
  );
}

function LinkList({ title, links, nodes, money, side, onLink }: { title: string; links: FlowLink[]; nodes: Map<string, FlowNode>; money: Money; side: "from" | "to"; onLink: (key: string) => void }) {
  if (!links.length) return null;
  return (
    <>
      <h3 className={styles.sheetHeading}>{title}</h3>
      <div className={styles.sheetRows}>
        {[...links].sort((a, b) => b.amount - a.amount).map((link) => (
          <button key={link.key} type="button" className={styles.sheetRow} onClick={() => onLink(link.key)}>
            <span><b>{nodes.get(side === "from" ? link.from : link.to)?.label}</b><span>{KIND_LABEL[link.kind]}</span></span>
            <span className={styles.amount}>{money(link.amount)}</span>
          </button>
        ))}
      </div>
    </>
  );
}

function LinkRows({ linkKey, basis, offset, through, money, onRow }: { linkKey: string; basis: PeriodBasis; offset: number; through: string; money: Money; onRow: (id: string) => void }) {
  const query = useInfiniteQuery<FlowRowsResponse>({
    queryKey: ["dashboard", "flow-rows", basis, offset, through, linkKey],
    initialPageParam: 0,
    queryFn: ({ pageParam }) => api.get(`/dashboard/flow/rows?timeframe=${basis}&cycle_offset=${offset}&through=${through}&link=${encodeURIComponent(linkKey)}&offset=${pageParam}`),
    getNextPageParam: (last) => (last.offset + last.limit < last.total_count ? last.offset + last.limit : undefined),
  });
  const open = useCallback((id: string) => onRow(id), [onRow]);
  if (query.isPending) return <Skeleton style={{ height: 120 }} />;
  if (query.isError) return <ErrorState message="Transaksi aliran ini belum bisa dimuat." onRetry={() => query.refetch()} />;
  const rows = query.data.pages.flatMap((page) => page.rows);
  return (
    <>
      <h3 className={styles.sheetHeading}>Transaksi · {query.data.pages[0].total_count}</h3>
      <div className={styles.sheetRows}>
        {rows.map((row) => (
          <button key={`${row.id}-${row.link}`} type="button" className={styles.sheetRow} onClick={() => open(row.id)}>
            <span>
              <b>{row.merchant || row.notes || row.category_name || "Tanpa keterangan"}</b>
              <span>{shortDay(row.local_date)} · {row.account_name}{row.waiting ? " · menunggu jawaban" : ""}</span>
            </span>
            <span className={styles.amount}>{money(row.amount)}</span>
          </button>
        ))}
      </div>
      {query.hasNextPage && (
        <button type="button" className="ws-btn ws-btn-sm" style={{ marginTop: 10 }} disabled={query.isFetchingNextPage} onClick={() => query.fetchNextPage()}>
          {query.isFetchingNextPage ? "Memuat…" : "Muat lagi"}
        </button>
      )}
    </>
  );
}
