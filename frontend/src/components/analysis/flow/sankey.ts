/**
 * Peta Aliran layout: a typed port of the approved preview's Sankey (phase plan D21, redesign D10).
 *
 * The server's nodes and links are exact; this module only decides where they are drawn. Columns run from
 * sources, through the liquid accounts (deeper when money moves on between them), to saving and investment
 * nodes, then to the buckets and "Masih ada". A node whose money also flows back to an account (a goal
 * pocket paying out, an investment sold, a waiting incoming leg) is drawn a second time on the source side
 * under the same id, so every drawn link runs left to right. Links the map cannot draw stay in the table.
 */
import type { FlowLink, FlowLinkKind, FlowNode } from "../types";

export const BALANCE_KINDS: ReadonlySet<FlowLinkKind> = new Set(["opening", "remaining", "negative_opening", "negative_closing"]);

const FOLDED_INBOUND: ReadonlySet<FlowLinkKind> = new Set(["income"]);

export type Highlight = "all" | "spend" | "transfer" | "save" | "waiting";

export function highlightMatches(kind: FlowLinkKind, highlight: Highlight): boolean {
  switch (highlight) {
    case "spend":
      return kind === "spend";
    case "transfer":
      return kind === "transfer";
    case "save":
      return kind === "saving" || kind === "investment";
    case "waiting":
      return kind === "waiting";
    default:
      return true;
  }
}

export interface DrawnNode {
  /** Unique per drawn copy; `nodeId` is the server id the drawer and the keyboard report. */
  key: string;
  nodeId: string;
  node: FlowNode;
  column: number;
  value: number;
  inbound: DrawnLink[];
  outbound: DrawnLink[];
}

export interface DrawnLink {
  key: string;
  /** The server links this ribbon stands for: one, or two opposite transfers drawn as their net. */
  linkKeys: string[];
  kind: FlowLinkKind;
  source: string;
  target: string;
  value: number;
  pattern: "stripe" | "dots" | "hatch" | null;
  waiting: boolean;
}

export interface FlowModel {
  nodes: Map<string, DrawnNode>;
  links: DrawnLink[];
  /** Server links that are not drawn: folded balances, or a cycle between accounts. */
  hiddenLinkKeys: string[];
  columns: number;
}

const SINK_ORDER: Record<string, number> = { pillar: 0, other_out: 1, waiting: 2, negative_opening: 3, remaining: 4 };

function patternFor(kind: FlowLinkKind, waiting: boolean): DrawnLink["pattern"] {
  if (kind === "transfer") return "stripe";
  if (kind === "investment") return "dots";
  if (kind === "waiting" || waiting) return "hatch";
  return null;
}

/** The drawable model for the visible links: columns, mirrored copies and netted opposite transfers.
 * Folded hides the balance and income links; the table always lists every link. */
export function buildFlowModel(nodes: FlowNode[], links: FlowLink[], { folded }: { folded: boolean }): FlowModel {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const hiddenLinkKeys: string[] = [];
  // Folded, the map shows only money that left or moved, so spending sets the scale (the approved preview):
  // balances and income are hidden from the drawing and stay in the table and the Masuk figure.
  const visible = links.filter((link) => {
    const hide = folded && (BALANCE_KINDS.has(link.kind) || FOLDED_INBOUND.has(link.kind));
    if (hide) hiddenLinkKeys.push(link.key);
    return !hide && byId.has(link.from) && byId.has(link.to);
  });

  // Opposite transfers between the same two accounts are drawn as their net; the table keeps both.
  const transfers = new Map<string, { a: string; b: string; net: number; keys: string[]; waiting: boolean }>();
  const others: FlowLink[] = [];
  for (const link of visible) {
    if (link.kind !== "transfer") {
      others.push(link);
      continue;
    }
    const [a, b] = [link.from, link.to].sort();
    const entry = transfers.get(`${a}|${b}`) ?? { a, b, net: 0, keys: [], waiting: false };
    entry.net += link.from === a ? link.amount : -link.amount;
    entry.keys.push(link.key);
    entry.waiting ||= link.waiting_count > 0;
    transfers.set(`${a}|${b}`, entry);
  }

  const depth = new Map<string, number>();
  for (const node of nodes) if (node.kind === "account") depth.set(node.id, 1);
  const drawnTransfers: { from: string; to: string; value: number; keys: string[]; waiting: boolean }[] = [];
  for (const entry of transfers.values()) {
    if (entry.net > 0) drawnTransfers.push({ from: entry.a, to: entry.b, value: entry.net, keys: entry.keys, waiting: entry.waiting });
    else if (entry.net < 0) drawnTransfers.push({ from: entry.b, to: entry.a, value: -entry.net, keys: entry.keys, waiting: entry.waiting });
  }
  drawnTransfers.sort((left, right) => (left.from + left.to).localeCompare(right.from + right.to));
  for (let round = 0; round < 8; round++) {
    for (const transfer of drawnTransfers) {
      const from = depth.get(transfer.from) ?? 1;
      depth.set(transfer.to, Math.max(depth.get(transfer.to) ?? 1, Math.min(from + 1, 4)));
    }
  }
  const accountDepth = Math.max(1, ...depth.values());
  // Saving and investment nodes share the sink column with the buckets, so their labels stack together;
  // only "Masih ada" sits one column further, since saving nodes also flow into it.
  const holdColumn = accountDepth + 1;
  const sinkColumn = accountDepth + 1;
  const remainingColumn = accountDepth + 2;

  const drawn = new Map<string, DrawnNode>();
  const copy = (node: FlowNode, side: "main" | "source"): DrawnNode => {
    const key = side === "source" && node.kind !== "source" ? `${node.id}@in` : node.id;
    let entry = drawn.get(key);
    if (!entry) {
      let column = node.kind === "remaining" ? remainingColumn : sinkColumn;
      if (side === "source" || node.kind === "source") column = 0;
      else if (node.kind === "account") column = depth.get(node.id) ?? 1;
      else if (node.kind === "saving" || node.kind === "investment") column = holdColumn;
      entry = { key, nodeId: node.id, node, column, value: 0, inbound: [], outbound: [] };
      drawn.set(key, entry);
    }
    return entry;
  };

  const drawnLinks: DrawnLink[] = [];
  const connect = (from: DrawnNode, to: DrawnNode, kind: FlowLinkKind, value: number, keys: string[], waiting: boolean) => {
    if (to.column <= from.column) {
      hiddenLinkKeys.push(...keys);
      return;
    }
    const link: DrawnLink = {
      key: keys.join("+"), linkKeys: keys, kind, source: from.key, target: to.key, value,
      pattern: patternFor(kind, waiting), waiting,
    };
    from.outbound.push(link);
    to.inbound.push(link);
    drawnLinks.push(link);
  };

  for (const transfer of drawnTransfers) {
    connect(copy(byId.get(transfer.from)!, "main"), copy(byId.get(transfer.to)!, "main"), "transfer", transfer.value, transfer.keys, transfer.waiting);
  }
  for (const link of others) {
    const source = byId.get(link.from)!;
    const target = byId.get(link.to)!;
    // Money leaving a holding or waiting node back to an account starts from its source-side copy.
    const backToAccount = target.kind === "account" && (source.kind === "saving" || source.kind === "investment" || source.kind === "waiting");
    const from = copy(source, backToAccount ? "source" : "main");
    connect(from, copy(target, "main"), link.kind, link.amount, [link.key], link.waiting_count > 0);
  }
  for (const node of drawn.values()) {
    const moved = (list: DrawnLink[]) => list.reduce((sum, link) => sum + link.value, 0);
    node.value = Math.max(moved(node.inbound), moved(node.outbound));
  }
  for (const [key, node] of drawn) if (node.value <= 0) drawn.delete(key);
  // Folded, nothing may come from the left: the first column is then dropped so the map uses the width.
  if (drawn.size && ![...drawn.values()].some((node) => node.column === 0)) {
    for (const node of drawn.values()) node.column -= 1;
  }
  const columns = drawn.size ? Math.max(...[...drawn.values()].map((node) => node.column)) + 1 : 0;
  return { nodes: drawn, links: drawnLinks, hiddenLinkKeys, columns };
}

export interface LayoutOptions {
  width: number;
  height: number;
  stationWidth: number;
  stationMin: number;
  barWidth: number;
  barMin: number;
  pad: number;
  leftLabel: number;
  rightLabel: number;
}

export interface PlacedNode {
  key: string;
  x0: number;
  x1: number;
  y0: number;
  y1: number;
  column: number;
}

export interface PlacedLink {
  key: string;
  path: string;
}

export interface FlowLayout {
  nodes: Map<string, PlacedNode>;
  links: PlacedLink[];
  columnX: number[];
  columnWidth: number[];
  scale: number;
}

interface ColumnItem {
  type: "node" | "dummy";
  key: string;
  value: number;
  group: number;
  rank: number;
  column: number;
  isStation: boolean;
  source?: string;
  h: number;
  x0: number;
  x1: number;
  y0: number;
  y1: number;
}

function nodeRank(node: DrawnNode): number {
  if (node.node.kind === "account" || node.node.kind === "saving" || node.node.kind === "investment" || node.node.kind === "source") {
    return node.node.rank + (node.key.endsWith("@in") ? 5000 : 0);
  }
  return (SINK_ORDER[node.node.kind] ?? 9) * 1000 + node.node.rank;
}

/** Deterministic positions and ribbon paths for a model, scaled so the tallest column fits `height`. */
export function layoutFlow(model: FlowModel, options: LayoutOptions): FlowLayout {
  const columns: ColumnItem[][] = Array.from({ length: model.columns }, () => []);
  const items = new Map<string, ColumnItem>();
  for (const node of model.nodes.values()) {
    const item: ColumnItem = {
      type: "node", key: node.key, value: node.value, group: 1, rank: nodeRank(node), column: node.column,
      isStation: node.node.kind === "account", h: 0, x0: 0, x1: 0, y0: 0, y1: 0,
    };
    items.set(node.key, item);
    columns[node.column].push(item);
  }
  const chains: { link: DrawnLink; chain: ColumnItem[]; sy0: number; sy1: number; ty0: number; ty1: number }[] = [];
  for (const link of model.links) {
    const from = items.get(link.source);
    const to = items.get(link.target);
    if (!from || !to || to.column <= from.column) continue;
    const chain = [from];
    for (let column = from.column + 1; column < to.column; column++) {
      const dummy: ColumnItem = {
        type: "dummy", key: `${link.key}#${column}`, value: link.value, column, group: from.isStation ? 0 : 2,
        rank: from.rank * 100 + to.rank, isStation: false, source: link.source, h: 0, x0: 0, x1: 0, y0: 0, y1: 0,
      };
      columns[column].push(dummy);
      chain.push(dummy);
    }
    chain.push(to);
    chains.push({ link, chain, sy0: 0, sy1: 0, ty0: 0, ty1: 0 });
  }
  for (const column of columns) column.sort((left, right) => left.group - right.group || left.rank - right.rank || left.key.localeCompare(right.key));

  const columnWidth = columns.map((column) => (column.some((item) => item.isStation) ? options.stationWidth : options.barWidth));
  const weights: number[] = [];
  for (let column = 0; column < model.columns - 1; column++) weights.push(column === 0 ? 0.8 : 1);
  const available = options.width - options.leftLabel - options.rightLabel - columnWidth.reduce((sum, width) => sum + width, 0);
  const unit = available / Math.max(1, weights.reduce((sum, weight) => sum + weight, 0));
  const columnX: number[] = [];
  let x = options.leftLabel;
  for (let column = 0; column < model.columns; column++) {
    columnX[column] = x;
    x += columnWidth[column] + (weights[column] ?? 0) * unit;
  }
  const minHeight = (item: ColumnItem) => (item.type === "dummy" ? 0 : item.isStation ? options.stationMin : options.barMin);
  const gap = (previous: ColumnItem, next: ColumnItem) =>
    previous.type === "dummy" && next.type === "dummy" && previous.source === next.source ? 0 : options.pad;
  const columnHeight = (column: ColumnItem[], scale: number) =>
    column.reduce((sum, item, index) => sum + Math.max(item.value * scale, minHeight(item)) + (index ? gap(column[index - 1], item) : 0), 0);
  let scale = options.height / Math.max(1, ...columns.map((column) => column.reduce((sum, item) => sum + item.value, 0)));
  for (let round = 0; round < 30; round++) {
    const worst = Math.max(...columns.map((column) => columnHeight(column, scale) / options.height));
    if (worst <= 1.0005) break;
    scale = scale / (worst + 0.002);
  }
  columns.forEach((column, index) => {
    let y = 0;
    column.forEach((item, position) => {
      if (position) y += gap(column[position - 1], item);
      item.h = Math.max(item.value * scale, minHeight(item));
      item.x0 = columnX[index];
      item.x1 = columnX[index] + columnWidth[index];
      item.y0 = y;
      item.y1 = y + item.h;
      y = item.y1;
    });
  });

  const middle = (item: ColumnItem) => (item.y0 + item.y1) / 2;
  const outgoing = new Map<string, typeof chains>();
  const incoming = new Map<string, typeof chains>();
  for (const entry of chains) {
    const first = entry.chain[0].key;
    const last = entry.chain[entry.chain.length - 1].key;
    outgoing.set(first, [...(outgoing.get(first) ?? []), entry]);
    incoming.set(last, [...(incoming.get(last) ?? []), entry]);
  }
  for (const [key, list] of outgoing) {
    const item = items.get(key)!;
    list.sort((left, right) => middle(left.chain[1]) - middle(right.chain[1]) || left.link.key.localeCompare(right.link.key));
    const total = list.reduce((sum, entry) => sum + entry.link.value * scale, 0);
    let y = item.y0 + (item.h - total) / 2;
    for (const entry of list) {
      entry.sy0 = y;
      y += entry.link.value * scale;
      entry.sy1 = y;
    }
  }
  for (const [key, list] of incoming) {
    const item = items.get(key)!;
    list.sort((left, right) => middle(left.chain[left.chain.length - 2]) - middle(right.chain[right.chain.length - 2]) || left.link.key.localeCompare(right.link.key));
    const total = list.reduce((sum, entry) => sum + entry.link.value * scale, 0);
    let y = item.y0 + (item.h - total) / 2;
    for (const entry of list) {
      entry.ty0 = y;
      y += entry.link.value * scale;
      entry.ty1 = y;
    }
  }
  const round = (value: number) => Math.round(value * 100) / 100;
  const links: PlacedLink[] = chains.map((entry) => {
    const points = entry.chain.map((item, index) => {
      if (index === 0) return { left: item.x0, right: item.x1, top: entry.sy0, bottom: entry.sy1 };
      if (index === entry.chain.length - 1) return { left: item.x0, right: item.x1, top: entry.ty0, bottom: entry.ty1 };
      return { left: item.x0, right: item.x1, top: item.y0, bottom: item.y1 };
    });
    let path = `M${round(points[0].right)},${round(points[0].top)}`;
    for (let index = 1; index < points.length; index++) {
      const mid = (points[index - 1].right + points[index].left) / 2;
      path += `C${round(mid)},${round(points[index - 1].top)} ${round(mid)},${round(points[index].top)} ${round(points[index].left)},${round(points[index].top)}`;
      if (index < points.length - 1) path += `L${round(points[index].right)},${round(points[index].top)}`;
    }
    const last = points.length - 1;
    path += `L${round(points[last].left)},${round(points[last].bottom)}`;
    for (let index = last; index >= 1; index--) {
      if (index < last) path += `L${round(points[index].left)},${round(points[index].bottom)}`;
      const mid = (points[index - 1].right + points[index].left) / 2;
      path += `C${round(mid)},${round(points[index].bottom)} ${round(mid)},${round(points[index - 1].bottom)} ${round(points[index - 1].right)},${round(points[index - 1].bottom)}`;
    }
    return { key: entry.link.key, path: `${path}Z` };
  });
  const nodes = new Map<string, PlacedNode>();
  for (const item of items.values()) {
    nodes.set(item.key, { key: item.key, x0: item.x0, x1: item.x1, y0: item.y0, y1: item.y1, column: item.column });
  }
  return { nodes, links, columnX, columnWidth, scale };
}

export type Direction = "up" | "down" | "left" | "right";

/** The node to move to from `current` with an arrow key: the same column for up and down, the nearest
 * node of the next column for left and right. Null when there is none. */
export function nextNode(layout: FlowLayout, current: string, direction: Direction): string | null {
  const from = layout.nodes.get(current);
  if (!from) return null;
  const cy = (from.y0 + from.y1) / 2;
  let best: string | null = null;
  let bestScore = Infinity;
  for (const node of layout.nodes.values()) {
    if (node.key === current) continue;
    const dy = (node.y0 + node.y1) / 2 - cy;
    let score: number;
    if (direction === "up" || direction === "down") {
      if (node.column !== from.column || Math.sign(dy) !== (direction === "down" ? 1 : -1)) continue;
      score = Math.abs(dy);
    } else {
      const wanted = direction === "right" ? 1 : -1;
      if (Math.sign(node.column - from.column) !== wanted) continue;
      score = Math.abs(node.column - from.column) * 100000 + Math.abs(dy);
    }
    if (score < bestScore) {
      bestScore = score;
      best = node.key;
    }
  }
  return best;
}
