import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ExplainProvider } from "@/components/workspace/explain";
import { ToastProvider } from "@/components/workspace/overlays";
import { api } from "@/lib/api";
import { KalenderTab, dayName, heatLevels } from "./calendar/KalenderTab";
import { PetaAliranTab, nodeTotals } from "./flow/PetaAliranTab";
import { buildFlowModel, layoutFlow, nextNode } from "./flow/sankey";
import { DailySpendCard, KakeiboCard } from "./RingkasanTab";
import type { CalendarDay, CalendarResponse, FlowResponse } from "./types";
import { analysisHref, parseTab } from "./url";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
  ApiError: class ApiError extends Error {
    status = 0;
  },
}));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const money = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;

function wrap(children: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <ToastProvider>
        <ExplainProvider>{children}</ExplainProvider>
      </ToastProvider>
    </QueryClientProvider>,
  );
}

function stubWidth(width: number) {
  class FixedWidthObserver {
    constructor(private readonly callback: ResizeObserverCallback) {}
    observe() {
      this.callback([{ contentRect: { width } } as ResizeObserverEntry], this as unknown as ResizeObserver);
    }
    unobserve() {}
    disconnect() {}
  }
  vi.stubGlobal("ResizeObserver", FixedWidthObserver);
}

afterEach(() => vi.unstubAllGlobals());

describe("Analisis URL", () => {
  it("falls back to Ringkasan and keeps day and view only on their tab", () => {
    expect(parseTab(null)).toBe("ringkasan");
    expect(parseTab("bandingkan")).toBe("ringkasan");
    expect(parseTab("aliran")).toBe("aliran");
    expect(analysisHref("ringkasan", { day: "2026-11-02" })).toBe("/insights");
    expect(analysisHref("kalender", { day: "2026-11-02" })).toBe("/insights?tab=kalender&day=2026-11-02");
    expect(analysisHref("aliran", { view: "tabel" })).toBe("/insights?tab=aliran&view=tabel");
    expect(analysisHref("aliran", { view: "peta" })).toBe("/insights?tab=aliran");
  });
});

describe("Ringkasan cards", () => {
  beforeEach(() => vi.clearAllMocks());

  it("labels the peak and today and offers a table instead of the bars", async () => {
    const user = userEvent.setup();
    const days = [
      { date: "2026-11-01", expense: 40_000 },
      { date: "2026-11-02", expense: 109_000 },
      { date: "2026-11-03", expense: 12_000 },
      { date: "2026-11-04", expense: 0 },
    ];
    wrap(<DailySpendCard days={days} money={money} today="2026-11-03" />);
    expect(screen.getByText("2 Nov Rp 109.000")).toBeInTheDocument();
    expect(screen.getByText("Rp 12.000")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Lihat sebagai tabel" }));
    const rows = within(screen.getByRole("table")).getAllByRole("row");
    // Header plus the three days up to today; the future day is absent.
    expect(rows).toHaveLength(4);
    expect(rows[2]).toHaveTextContent("tertinggi");
    expect(rows[3]).toHaveTextContent("hari ini, masih berjalan");
  });

  it("compares Kakeibo as amounts and asks for a pillar where spending is unclassified", async () => {
    vi.mocked(api.get).mockResolvedValue({
      label: "November 2026", compared_days: 3,
      current: { start: "", end: "", start_date: "2026-11-01", end_date: "2026-11-03", inflow: 0, outflow: 0, net: 0 },
      previous: { start: "", end: "", start_date: "2026-10-01", end_date: "2026-10-03", inflow: 0, outflow: 0, net: 0 },
      previous_label: "Oktober 2026", previous_has_data: true,
      kakeibo: {
        current: { need: 300_000, want: 50_000, culture: 0, unexpected: 0, saving: 400_000, unclassified: 25_000 },
        previous: { need: 200_000, want: 80_000, culture: 0, unexpected: 0, saving: 100_000, unclassified: 0 },
      },
    });
    const { container } = wrap(<KakeiboCard basis="month" offset={0} money={money} />);
    expect(await screen.findByText("1–3 Nov dibanding 1–3 Okt")).toBeInTheDocument();
    expect(screen.getByText("Naik Rp 100.000")).toBeInTheDocument();
    expect(screen.getByText("Turun Rp 30.000")).toBeInTheDocument();
    expect(screen.getByText("Naik Rp 300.000")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Perlu dipilih" })).toHaveAttribute("href", "/laporan?lengkapi=1");
    expect(screen.getByRole("img", { name: "Kebutuhan: Rp 300.000, periode lalu Rp 200.000" })).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/%/);
    expect((await axe(container)).violations).toHaveLength(0);
  });
});

function day(date: string, overrides: Partial<CalendarDay> = {}): CalendarDay {
  return {
    date, spend: 0, spend_count: 0, heat: 0, income_total: 0, income_count: 0, movement_total: 0, movement_count: 0,
    topup_total: 0, topup_count: 0, waiting_count: 0, waiting_amount: 0, waiting: [], plans: [], ...overrides,
  };
}

function calendarFixture(): CalendarResponse {
  const days: CalendarDay[] = [];
  for (let index = 0; index < 30; index++) {
    const date = `2026-11-${String(index + 1).padStart(2, "0")}`;
    days.push(day(date));
  }
  days[1] = day("2026-11-02", {
    spend: 109_000, spend_count: 3, heat: 3, waiting_count: 1, waiting_amount: 45_000, movement_count: 1, movement_total: 500_000,
    waiting: [{ transaction_id: "tx-q", section: "questions", label: "QRIS Contoh", account_name: "Dompet Digital", type: "expense", amount: 45_000 }],
  });
  days[4] = day("2026-11-05", { plans: [{ rule_id: "r1", label: "Internet Rumah", amount: 350_000, status: "due" }] });
  return {
    window: { start: "2026-11-01", end: "2026-11-30", label: "November 2026", basis: "month", today: "2026-11-03", day_index: 3, days_total: 30, is_current: true, open: false },
    expected_salary: { date: null, available: false, status: "unknown", reason: "no_cycle" },
    totals: { spend: 109_000, gross_spend: 109_000, offset_applied: 0 },
    days,
    upcoming: [{ date: "2026-11-05", rule_id: "r1", label: "Internet Rumah", amount: 350_000, status: "due" }],
    upcoming_total: 350_000,
    upcoming_basis: "window_end",
    heat_thresholds: [50_000, 80_000, 100_000, 150_000],
  };
}

describe("Kalender", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/calendar")) return calendarFixture();
      if (path.startsWith("/transactions?")) return { transactions: [], total: 0 };
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("names each day with its spending, waiting rows and plans", () => {
    const fixture = calendarFixture();
    expect(dayName(fixture.days[1], money, "2026-11-03")).toBe("Senin, 2 November: belanja Rp 109.000, 1 menunggu jawaban, pindah saldo Rp 500.000");
    expect(dayName(fixture.days[4], money, "2026-11-03")).toBe("Kamis, 5 November: tagihan Internet Rumah");
    expect(heatLevels([50_000, 80_000, 100_000, 150_000], money).map((level) => level.text)).toEqual([
      "sampai Rp 50.000", "Rp 50.000 – Rp 80.000", "Rp 80.000 – Rp 100.000", "Rp 100.000 – Rp 150.000", "di atas Rp 150.000",
    ]);
  });

  it("moves through the grid with the arrow keys and selects a day with Enter", async () => {
    const user = userEvent.setup();
    const onSelectDay = vi.fn();
    const { container } = wrap(<KalenderTab basis="month" offset={0} money={money} day="2026-11-02" onSelectDay={onSelectDay} onLabel={() => {}} />);
    const grid = await screen.findByRole("grid", { name: "Kalender" });
    // Weeks start on Monday: November 2026 starts on a Sunday, so six blank cells come first.
    expect(within(grid).getAllByRole("row")[0].querySelectorAll("[aria-hidden='true'][role='gridcell']")).toHaveLength(6);
    const selected = within(grid).getByRole("button", { name: /Senin, 2 November/ });
    expect(selected).toHaveAttribute("tabindex", "0");
    expect(selected.closest("[role='gridcell']")).toHaveAttribute("aria-selected", "true");
    selected.focus();
    await user.keyboard("{ArrowRight}");
    expect(within(grid).getByRole("button", { name: /Selasa, 3 November/ })).toHaveFocus();
    await user.keyboard("{Enter}");
    expect(onSelectDay).toHaveBeenCalledWith("2026-11-03");
    await user.keyboard("{PageDown}");
    expect(within(grid).getByRole("button", { name: /Selasa, 10 November/ })).toHaveFocus();
    await user.keyboard("{End}");
    expect(within(grid).getByRole("button", { name: /Minggu, 15 November/ })).toHaveFocus();

    const sheet = screen.getByRole("region", { name: "Senin, 2 November" });
    expect(within(sheet).getByRole("link", { name: /QRIS Contoh/ })).toHaveAttribute("href", "/review?item=tx-q");
    const upcoming = screen.getByRole("region", { name: "Sampai akhir periode" });
    expect(within(upcoming).getByText("Total").parentElement).toHaveTextContent("TotalRp 350.000");
    expect(screen.getByRole("list", { name: "Tingkat belanja per hari" })).toHaveTextContent("di atas Rp 150.000");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows a week strip on the phone that moves inside the window", async () => {
    stubWidth(390);
    const user = userEvent.setup();
    const onSelectDay = vi.fn();
    wrap(<KalenderTab basis="month" offset={0} money={money} day="2026-11-03" onSelectDay={onSelectDay} onLabel={() => {}} />);
    const strip = await screen.findByRole("group", { name: /Minggu/ });
    const days = within(strip).getAllByRole("button", { pressed: false }).concat(within(strip).getAllByRole("button", { pressed: true }));
    expect(days).toHaveLength(7);
    expect(within(strip).getByRole("button", { name: /Senin, 2 November/ })).toHaveTextContent("109rb");
    await user.click(within(strip).getByRole("button", { name: "Minggu berikutnya" }));
    expect(onSelectDay).toHaveBeenCalledWith("2026-11-09");
    expect(screen.queryByRole("grid")).not.toBeInTheDocument();
  });
});

function flowFixture(through = "2026-10-31"): FlowResponse {
  return {
    window: { start: "2026-10-01", end: "2026-10-31", through, label: "Oktober 2026", is_current: false },
    nodes: [
      { id: "src:gaji", kind: "source", label: "Gaji", rank: 0 },
      { id: "opening", kind: "source", label: "Saldo sebelum periode", rank: 950, folded_by_default: true },
      { id: "acc:bank", kind: "account", label: "Bank Utama", rank: 0, account_id: "bank", opening: 1_000_000, closing: 6_100_000 },
      { id: "acc:wallet", kind: "account", label: "Dompet Digital", rank: 1, account_id: "wallet", opening: 0, closing: 300_000 },
      { id: "sav:dd", kind: "saving", label: "Dana Darurat · Kantong", rank: 2, account_id: "dd", opening: 0, closing: 900_000 },
      { id: "pillar:need", kind: "pillar", label: "Kebutuhan", rank: 0, pillar: "need" },
      { id: "waiting", kind: "waiting", label: "Menunggu pasangan", rank: 20 },
      { id: "remaining", kind: "remaining", label: "Masih ada", rank: 30, folded_by_default: true },
    ],
    links: [
      { key: "src:gaji>acc:bank", from: "src:gaji", to: "acc:bank", kind: "income", amount: 8_000_000, count: 1, waiting_count: 0, waiting_amount: 0 },
      { key: "opening>acc:bank", from: "opening", to: "acc:bank", kind: "opening", amount: 1_000_000, count: 0, waiting_count: 0, waiting_amount: 0 },
      { key: "acc:bank>acc:wallet", from: "acc:bank", to: "acc:wallet", kind: "transfer", amount: 500_000, count: 1, waiting_count: 0, waiting_amount: 0 },
      { key: "acc:bank>sav:dd", from: "acc:bank", to: "sav:dd", kind: "saving", amount: 1_000_000, count: 1, waiting_count: 0, waiting_amount: 0 },
      { key: "sav:dd>acc:bank", from: "sav:dd", to: "acc:bank", kind: "saving", amount: 100_000, count: 1, waiting_count: 0, waiting_amount: 0 },
      { key: "acc:bank>pillar:need", from: "acc:bank", to: "pillar:need", kind: "spend", amount: 1_500_000, count: 4, waiting_count: 1, waiting_amount: 45_000 },
      { key: "acc:wallet>waiting", from: "acc:wallet", to: "waiting", kind: "waiting", amount: 200_000, count: 1, waiting_count: 1, waiting_amount: 200_000 },
      { key: "acc:bank>remaining", from: "acc:bank", to: "remaining", kind: "remaining", amount: 6_100_000, count: 0, waiting_count: 0, waiting_amount: 0 },
      { key: "acc:wallet>remaining", from: "acc:wallet", to: "remaining", kind: "remaining", amount: 300_000, count: 0, waiting_count: 0, waiting_amount: 0 },
      { key: "sav:dd>remaining", from: "sav:dd", to: "remaining", kind: "remaining", amount: 900_000, count: 0, waiting_count: 0, waiting_amount: 0 },
    ],
    summary: {
      masuk: 8_000_000, spend_gross: 1_500_000, belanja_gross: 1_500_000, reimbursement_offset: 0, belanja: 1_500_000, keluar: 2_400_000,
      saving_net: 900_000, investment_net: 0, waiting_out: 200_000, waiting_in: 0, waiting_in_buckets: 45_000, remaining: 7_300_000, other_out: 0,
    },
  };
}

const LAYOUT = { width: 960, height: 420, stationWidth: 118, stationMin: 46, barWidth: 12, barMin: 3, pad: 12, leftLabel: 150, rightLabel: 220 };

describe("Peta Aliran layout", () => {
  it("folds balances, mirrors money flowing back and places every node deterministically", () => {
    const fixture = flowFixture();
    const folded = buildFlowModel(fixture.nodes, fixture.links, { folded: true });
    // Folded hides balances and income, so spending sets the scale; the table keeps them.
    expect(folded.hiddenLinkKeys.sort()).toEqual(["acc:bank>remaining", "acc:wallet>remaining", "opening>acc:bank", "sav:dd>remaining", "src:gaji>acc:bank"]);
    const open = buildFlowModel(fixture.nodes, fixture.links, { folded: false });
    expect(open.nodes.get("remaining")?.column).toBe(open.columns - 1);
    expect(open.nodes.get("sav:dd")?.column).toBe(open.nodes.get("pillar:need")?.column);
    expect(folded.nodes.has("remaining")).toBe(false);
    // The goal pocket paying back to the bank is drawn from its source-side copy.
    expect(folded.nodes.get("sav:dd@in")?.column).toBe(0);
    expect(folded.nodes.get("acc:wallet")?.column).toBe(2);
    expect(folded.links.every((link) => folded.nodes.get(link.target)!.column > folded.nodes.get(link.source)!.column)).toBe(true);

    const first = layoutFlow(folded, LAYOUT);
    const second = layoutFlow(buildFlowModel(fixture.nodes, fixture.links, { folded: true }), LAYOUT);
    expect([...first.nodes.values()]).toEqual([...second.nodes.values()]);
    expect(first.links.map((link) => link.path)).toEqual(second.links.map((link) => link.path));
    for (let column = 0; column < folded.columns; column++) {
      const stacked = [...first.nodes.values()].filter((node) => node.column === column).sort((a, b) => a.y0 - b.y0);
      for (let index = 1; index < stacked.length; index++) expect(stacked[index].y0).toBeGreaterThanOrEqual(stacked[index - 1].y1);
      for (const node of stacked) expect(node.y1).toBeLessThanOrEqual(LAYOUT.height + 0.5);
    }
    for (const width of [320, 720, 1440]) {
      const placed = layoutFlow(folded, { ...LAYOUT, width });
      for (const node of placed.nodes.values()) expect(Number.isFinite(node.x0) && Number.isFinite(node.y0)).toBe(true);
    }
    expect(nextNode(first, "acc:bank", "right")).toBe("acc:wallet");
    expect(nextNode(first, "acc:bank", "left")).not.toBeNull();
  });

  it("handles an empty period and a single account", () => {
    const empty = buildFlowModel([], [], { folded: true });
    expect(empty.columns).toBe(0);
    expect(layoutFlow(empty, LAYOUT).nodes.size).toBe(0);
    const fixture = flowFixture();
    const one = buildFlowModel(fixture.nodes, fixture.links.filter((link) => link.key === "src:gaji>acc:bank"), { folded: false });
    expect(one.columns).toBe(2);
  });

  it("totals a node without its balance links", () => {
    expect(nodeTotals("acc:bank", flowFixture().links)).toEqual({ inbound: 8_100_000, outbound: 3_000_000 });
  });
});

describe("Peta Aliran tab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    const store = new Map<string, string>();
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: { getItem: (key: string) => store.get(key) ?? null, setItem: (key: string, value: string) => store.set(key, value), clear: () => store.clear() },
    });
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/flow/rows")) return { rows: [{ id: "tx-1", link: "acc:bank>pillar:need", type: "expense", amount: 1_500_000, row_amount: 1_500_000, date: "2026-10-02T02:00:00Z", local_date: "2026-10-02", account_id: "bank", account_name: "Bank Utama", category_name: "Makan", merchant: "Pasar Contoh", notes: null, movement_id: null, waiting: false }], total_count: 1, total_amount: 1_500_000, offset: 0, limit: 50 };
      if (path.startsWith("/dashboard/flow")) {
        const through = new URLSearchParams(path.split("?")[1]).get("through");
        return flowFixture(through ?? "2026-10-31");
      }
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("lists every link once in the table, with the waiting part, and opens its rows", async () => {
    const user = userEvent.setup();
    const { container } = wrap(<PetaAliranTab basis="month" offset={-1} money={money} view="tabel" onView={() => {}} onLabel={() => {}} />);
    const table = await screen.findByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(flowFixture().links.length);
    expect(within(table).getByText(/termasuk 1 menunggu jawaban/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(within(table).getByRole("button", { name: "Bank Utama → Kebutuhan" }));
    const drawer = await screen.findByRole("dialog", { name: /Bank Utama → Kebutuhan/ });
    expect(await within(drawer).findByRole("button", { name: /Pasar Contoh/ })).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith(expect.stringContaining("link=acc%3Abank%3Epillar%3Aneed"));
  });

  it("keeps the fold per viewer, filters by highlight and steps the replay by day", async () => {
    const user = userEvent.setup();
    wrap(<PetaAliranTab basis="month" offset={-1} money={money} view="tabel" onView={() => {}} onLabel={() => {}} />);
    const fold = await screen.findByRole("button", { name: /Tampilkan uang masuk, saldo dan Masih ada/ });
    await user.click(fold);
    expect(window.localStorage.getItem("analysis.flow.folded")).toBe("open");
    await user.click(screen.getByRole("button", { name: "Menunggu pasangan" }));
    expect(within(screen.getByRole("table")).getAllByRole("row")).toHaveLength(2);
    const slider = screen.getByRole("slider", { name: "Putar ulang per hari" });
    expect(slider).toHaveAttribute("aria-valuetext", "Sabtu, 31 Oktober, hari ke-31");
    fireEvent.change(slider, { target: { value: "30" } });
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/dashboard/flow?timeframe=month&cycle_offset=-1&through=2026-10-30"));
  });

  it("turns replay off under reduced motion", async () => {
    vi.stubGlobal("matchMedia", (query: string) => ({ matches: query.includes("reduce"), addEventListener() {}, removeEventListener() {} }));
    wrap(<PetaAliranTab basis="month" offset={-1} money={money} view="tabel" onView={() => {}} onLabel={() => {}} />);
    expect(await screen.findByRole("button", { name: "Putar ulang" })).toBeDisabled();
    expect(screen.getByText(/Gerakan dikurangi/)).toBeInTheDocument();
  });
});
