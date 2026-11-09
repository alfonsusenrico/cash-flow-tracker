import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { ChangeFigure, ProgressBar } from "@/components/workspace/figures";
import { KalenderTab } from "./calendar/KalenderTab";
import { DailySpendingChart, barGeometry, dateLabelIndexes, niceMax } from "./DailySpendingChart";
import { MAP, PetaAliranTab, fitLabel, labelWidth, nodeTotals } from "./flow/PetaAliranTab";
import { buildFlowModel, layoutFlow, nextNode } from "./flow/sankey";
import type { CalendarDay, CalendarResponse, FlowResponse } from "./types";
import { analysisHref, parseTab } from "./url";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
  ApiError: class ApiError extends Error {
    status = 0;
  },
}));
const ctx = vi.hoisted(() => ({ hide: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: ctx.hide, bal: (amount: number) => (ctx.hide ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`) }),
}));
vi.mock("@/components/transactions/TransactionDialog", () => ({
  TransactionDialog: ({ transactionId, onClose }: { transactionId: string | null; onClose: () => void }) =>
    transactionId ? (
      <div role="dialog" aria-label="Transaksi">
        <button type="button" onClick={onClose}>Tutup transaksi</button>
      </div>
    ) : null,
}));

const money = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;

function client() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

function wrap(children: ReactNode) {
  return render(<QueryClientProvider client={client()}>{children}</QueryClientProvider>);
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

function stubPhone(matches: boolean) {
  vi.stubGlobal("matchMedia", (query: string) => ({ matches: matches && query.includes("767"), addEventListener() {}, removeEventListener() {} }));
}

afterEach(() => {
  vi.unstubAllGlobals();
  ctx.hide = false;
});

describe("Analisis URL", () => {
  it("falls back to Ringkasan and keeps every other parameter", () => {
    expect(parseTab(null)).toBe("ringkasan");
    expect(parseTab("grafik")).toBe("ringkasan");
    expect(parseTab("aliran")).toBe("aliran");
    expect(analysisHref("ringkasan")).toBe("/insights");
    expect(analysisHref("ringkasan", {}, new URLSearchParams("tab=aliran&view=tabel"))).toBe("/insights?tab=ringkasan");
    expect(analysisHref("kalender", { day: "2026-11-02" }, new URLSearchParams("q=1"))).toBe("/insights?q=1&tab=kalender&day=2026-11-02");
    expect(analysisHref("aliran", { view: "tabel" })).toBe("/insights?tab=aliran&view=tabel");
    expect(analysisHref("aliran", { view: "peta" }, new URLSearchParams("tab=aliran&view=tabel"))).toBe("/insights?tab=aliran");
  });
});

describe("ProgressBar and ChangeFigure", () => {
  it("takes a percent by default, clamps it, paints its colour, keeps a width and stays hidden from assistive technology", () => {
    const { container } = render(
      <><ProgressBar value={140} width={160} color="var(--out-solid)" /><ProgressBar value={-5} /><ProgressBar value={Number.NaN} /><ProgressBar value={42} /></>,
    );
    const bars = container.querySelectorAll<HTMLElement>(".prog");
    const [over, under, invalid, plain] = container.querySelectorAll<HTMLElement>(".prog > span");
    expect(over.style.width).toBe("100%");
    expect(over.style.background).toBe("var(--out-solid)");
    expect(bars[0].style.width).toBe("160px");
    expect(under.style.width).toBe("0%");
    expect(invalid.style.width).toBe("0%");
    expect(plain.style.width).toBe("42%");
    expect(plain.style.background).toBe("");
    expect([...bars].every((bar) => bar.getAttribute("aria-hidden") === "true" && bar.tagName === "SPAN")).toBe(true);
  });

  it("writes a rise in the expense ink, a fall in the income ink and the words for screen readers", () => {
    const { container } = render(<><ChangeFigure change={753_830} money={money} /><ChangeFigure change={-120_000} money={money} /><ChangeFigure change={0} money={money} /></>);
    const [rise, fall] = container.querySelectorAll(".chg");
    expect(rise).toHaveClass("c-expense");
    expect(rise).toHaveTextContent("▲ naik Rp 753.830");
    expect(fall).toHaveClass("c-income");
    expect(fall).toHaveTextContent("▼ turun Rp 120.000");
    expect(container).toHaveTextContent("sama");
    expect(container.querySelector(".chg [aria-hidden='true']")).toHaveTextContent("▲");
  });

  it("masks the amount under hidden balances and speaks English", () => {
    ctx.hide = true;
    const { container } = renderWithIntl(<ChangeFigure change={753_830} money={money} />, { language: "en" });
    expect(container).toHaveTextContent("up");
    expect(container).not.toHaveTextContent("753");
    expect(screen.getByRole("img", { name: "Hidden" })).toBeInTheDocument();
  });
});

const DAYS = Array.from({ length: 14 }, (_, index) => ({ date: `2026-11-${String(index + 1).padStart(2, "0")}`, amount: index === 4 ? 527_000 : index === 0 ? 40_000 : 20_000 }));

describe("Pengeluaran harian", () => {
  it("draws the highest day in the expense mark, hatches today and names both in the legend", async () => {
    stubWidth(728);
    const { container } = wrap(<DailySpendingChart days={DAYS} money={money} today="2026-11-14" />);
    expect(container.querySelector("[data-bar='peak']")).toHaveAttribute("fill", "var(--out-solid)");
    expect(container.querySelector("[data-bar='today']")).toHaveAttribute("fill", "var(--k-none)");
    expect(container.querySelector("rect[fill^='url(#']")).not.toBeNull();
    expect(screen.getByRole("img", { name: "Pengeluaran harian, di luar tabungan dan investasi. Tertinggi 5 Nov Rp 527.000." })).toBeInTheDocument();
    const legend = container.querySelector(".chart-foot")!;
    expect(legend).toHaveTextContent("Tertinggi: 5 Nov Rp 527.000");
    expect(legend).toHaveTextContent("Hari ini, masih berjalan: Rp 20.000");
    expect(legend).toHaveTextContent("Di luar tabungan dan investasi");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("swaps to a table with the same values, keeping focus on the toggle", async () => {
    stubWidth(728);
    const user = userEvent.setup();
    const { container } = wrap(<DailySpendingChart days={DAYS} money={money} today="2026-11-14" />);
    const toggle = screen.getByRole("button", { name: "Lihat sebagai tabel" });
    expect(toggle).toHaveAttribute("aria-pressed", "false");
    await user.click(toggle);
    expect(toggle).toHaveFocus();
    expect(toggle).toHaveAttribute("aria-pressed", "true");
    expect(toggle).toHaveTextContent("Lihat sebagai grafik");
    const table = screen.getByRole("table", { name: "Pengeluaran per hari" });
    expect(within(table).getAllByRole("row")).toHaveLength(15);
    expect(table).toHaveTextContent("Kamis, 5 November · tertinggi");
    expect(table).toHaveTextContent("Sabtu, 14 November · hari ini, masih berjalan");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows a day's date and amount on hover or tap, and hides them on leave, a second tap or Esc", () => {
    stubWidth(728);
    // jsdom has no PointerEvent, so the pointer type would be lost without this stand-in.
    class PointerEventStandIn extends MouseEvent {
      pointerType: string;
      constructor(type: string, init: PointerEventInit = {}) {
        super(type, init);
        this.pointerType = init.pointerType ?? "";
      }
    }
    vi.stubGlobal("PointerEvent", PointerEventStandIn);
    const { container } = wrap(<DailySpendingChart days={DAYS} money={money} today="2026-11-14" />);
    const first = container.querySelector("[data-day='2026-11-01']")!;
    fireEvent.pointerEnter(first, { pointerType: "mouse" });
    expect(screen.getByTestId("daily-spend-tip")).toHaveTextContent("1 NovRp 40.000");
    fireEvent.pointerLeave(container.querySelector(".chart")!, { pointerType: "mouse" });
    expect(screen.queryByTestId("daily-spend-tip")).not.toBeInTheDocument();
    const today = container.querySelector("[data-day='2026-11-14']")!;
    fireEvent.pointerDown(today, { pointerType: "touch" });
    expect(screen.getByTestId("daily-spend-tip")).toHaveTextContent("14 Nov · hari ini");
    fireEvent.pointerDown(today, { pointerType: "touch" });
    expect(screen.queryByTestId("daily-spend-tip")).not.toBeInTheDocument();
    fireEvent.pointerEnter(first, { pointerType: "mouse" });
    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByTestId("daily-spend-tip")).not.toBeInTheDocument();
  });

  it("uses the prototype's bar width and scale, and never lets two date labels touch at any width", () => {
    expect(barGeometry(720, 30).barWidth).toBe(Math.max(4, Math.floor(712 / 30) - 4));
    expect(barGeometry(288, 33).barWidth).toBe(4);
    expect(niceMax(527_000)).toBe(1_000_000);
    expect(niceMax(180_000)).toBe(200_000);
    expect(niceMax(0)).toBe(100_000);
    for (const width of [728, 600, 358, 288]) {
      for (const count of [14, 31, 33]) {
        const texts = Array.from({ length: count }, (_, index) => `${index + 1} Nov`);
        const today = count - 1;
        const chosen = dateLabelIndexes(texts, width, today);
        expect(chosen).toContain(today);
        const { step } = barGeometry(width, count);
        for (let index = 1; index < chosen.length; index++) {
          const gap = (chosen[index] - chosen[index - 1]) * step;
          const half = ((texts[chosen[index]].length + texts[chosen[index - 1]].length) * 6.5) / 2;
          expect(gap, `${count} days at ${width}px`).toBeGreaterThanOrEqual(half);
        }
      }
    }
  });

  it("draws at the measured width and leaves the ticks blank under hidden balances", () => {
    stubWidth(320);
    ctx.hide = true;
    const { container } = wrap(<DailySpendingChart days={DAYS} money={money} today="2026-11-14" />);
    expect(container.querySelector("svg")).toHaveAttribute("width", "320");
    const ticks = [...container.querySelectorAll("text.ax")].map((node) => node.textContent ?? "");
    expect(ticks.some((text) => /jt|rb/.test(text))).toBe(false);
  });
});

function day(date: string, overrides: Partial<CalendarDay> = {}): CalendarDay {
  return {
    date, spend: 0, spending: 0, spending_heat: 0, spend_count: 0, heat: 0, income_total: 0, income_count: 0, movement_total: 0, movement_count: 0,
    topup_total: 0, topup_count: 0, waiting_count: 0, waiting_amount: 0, waiting: [], plans: [], ...overrides,
  };
}

function calendarFixture(): CalendarResponse {
  const days: CalendarDay[] = [];
  for (let index = 0; index < 30; index++) days.push(day(`2026-11-${String(index + 1).padStart(2, "0")}`));
  days[1] = day("2026-11-02", {
    spend: 409_000, spending: 109_000, spending_heat: 2, spend_count: 3, heat: 4, waiting_count: 1, waiting_amount: 45_000, movement_count: 1, movement_total: 500_000,
    waiting: [{ transaction_id: "tx-q", section: "questions", label: "QRIS Contoh", account_name: "Dompet Digital", type: "expense", amount: 45_000 }],
  });
  days[2] = day("2026-11-03", { spend: 1_303_500, spending: 1_303_500, spending_heat: 5, spend_count: 1, heat: 5, income_count: 1, income_total: 9_000_000 });
  days[4] = day("2026-11-05", { plans: [{ rule_id: "r1", label: "Internet Rumah", amount: 350_000, status: "due" }] });
  return {
    window: { start: "2026-11-01", end: "2026-11-30", label: "November 2026", basis: "month", today: "2026-11-03", day_index: 3, days_total: 30, is_current: true, open: false },
    expected_salary: { date: null, available: false, status: "unknown", reason: "no_cycle" },
    // Belanja carries a Rp 300.000 saving-pillar expense on 2 November; Kalender shows Pengeluaran (D31).
    totals: { spend: 1_712_500, gross_spend: 1_712_500, offset_applied: 0, spending: 1_412_500 },
    days,
    upcoming: [{ date: "2026-11-05", rule_id: "r1", label: "Internet Rumah", amount: 350_000, status: "due" }],
    upcoming_total: 350_000,
    upcoming_basis: "window_end",
    heat_thresholds: [60_000, 90_000, 400_000, 900_000],
    spending_heat_thresholds: [50_000, 80_000, 100_000, 150_000],
  };
}

const expenseRow = {
  id: "tx-1", type: "expense", amount: 45_000, date: "2026-11-02T05:00:00Z", account_id: "a", account_name: "Dompet Digital",
  merchant: "Kedai Contoh", notes: null, category_name: "Makan", kakeibo_type: "need", review_status: "reviewed",
};

describe("Kalender", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    stubPhone(false);
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/calendar")) return calendarFixture();
      if (path.startsWith("/transactions?")) return { transactions: path.includes("2026-11-02") ? [expenseRow] : [], total: 0 };
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("names each cell, writes compact amounts, markers and the heat strip from the server's level", async () => {
    const { container } = wrap(<KalenderTab basis="month" offset={0} day="2026-11-02" onSelectDay={vi.fn()} onWindowLabel={() => {}} />);
    const grid = await screen.findByRole("grid", { name: "Kalender November 2026" });
    // The hero, the cells and the strip read Pengeluaran, not Belanja (D31).
    expect(screen.getByRole("region", { name: "Pengeluaran periode ini" }).querySelector(".hero-fig")).toHaveTextContent("Rp 1.412.500");
    const second = within(grid).getByRole("gridcell", { name: /Senin, 2 November/ });
    expect(second).toHaveAccessibleName("Senin, 2 November. Pengeluaran Rp 109.000. 1 menunggu jawaban. Pindah.");
    expect(second).toHaveAttribute("aria-selected", "true");
    expect(second).toHaveTextContent("109rb");
    expect(second.querySelector(".mark.is-outline")).not.toBeNull();
    expect((second.querySelector(".heat") as HTMLElement).style.opacity).toBe("0.4");
    const third = within(grid).getByRole("gridcell", { name: /Selasa, 3 November/ });
    expect(third).toHaveTextContent("1,3jt");
    expect(third).toHaveAttribute("aria-current", "date");
    expect(within(grid).getByRole("gridcell", { name: /Kamis, 5 November/ })).toHaveAccessibleName("Kamis, 5 November. Tagihan Internet Rumah.");
    expect(within(grid).getByRole("gridcell", { name: /Rabu, 4 November/ }).querySelector(".heat")).toBeNull();
    // Weeks start on Monday: November 2026 starts on a Sunday, so six days of October come first, out of the period.
    expect(container.querySelectorAll(".cal-row")[1].querySelectorAll(".cal-cell.is-out")).toHaveLength(6);
    const legend = container.querySelector(".cal-legend")!;
    expect(legend).toHaveTextContent("sampai Rp 50.000");
    expect(legend).toHaveTextContent("di atas Rp 150.000");
    expect(legend.querySelectorAll(".heat-steps > span")).toHaveLength(5);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("walks the grid with the arrow keys, Home, End and the page keys, whatever the shortcut setting", async () => {
    const user = userEvent.setup();
    const onSelectDay = vi.fn();
    wrap(<KalenderTab basis="month" offset={0} day="2026-11-02" onSelectDay={onSelectDay} onWindowLabel={() => {}} />);
    const grid = await screen.findByRole("grid");
    within(grid).getByRole("gridcell", { name: /Senin, 2 November/ }).focus();
    await user.keyboard("{ArrowRight}");
    expect(within(grid).getByRole("gridcell", { name: /Selasa, 3 November/ })).toHaveFocus();
    expect(onSelectDay).toHaveBeenLastCalledWith("2026-11-03");
    await user.keyboard("{ArrowDown}");
    expect(onSelectDay).toHaveBeenLastCalledWith("2026-11-10");
    await user.keyboard("{End}");
    expect(onSelectDay).toHaveBeenLastCalledWith("2026-11-15");
    await user.keyboard("{Home}");
    expect(onSelectDay).toHaveBeenLastCalledWith("2026-11-09");
    await user.keyboard("{PageUp}");
    expect(onSelectDay).toHaveBeenLastCalledWith("2026-11-02");
    await user.keyboard("{ArrowUp}");
    expect(onSelectDay).toHaveBeenLastCalledWith("2026-11-01");
  });

  it("shows the day's waiting rows, opens a transaction and returns focus, and links Transaksi to that day only", async () => {
    const user = userEvent.setup();
    wrap(<KalenderTab basis="month" offset={0} day="2026-11-02" onSelectDay={vi.fn()} onWindowLabel={() => {}} />);
    const panel = await screen.findByRole("complementary", { name: "Detail hari" });
    expect(within(panel).getByRole("link", { name: /QRIS Contoh/ })).toHaveAttribute("href", "/review?item=tx-q");
    const row = await within(panel).findByRole("button", { name: /Kedai Contoh/ });
    expect(row.querySelector(".c-expense")).toHaveTextContent("−Rp 45.000");
    await user.click(row);
    await user.click(screen.getByRole("button", { name: "Tutup transaksi" }));
    expect(row).toHaveFocus();
    const ledger = within(panel).getByRole("link", { name: "Lihat di Transaksi" });
    const href = decodeURIComponent(ledger.getAttribute("href") ?? "");
    expect(href).toContain("from=2026-11-02T00:00:00+07:00");
    expect(href).toContain("to=2026-11-02T23:59:59.999+07:00");
    expect(within(panel).getByText("Sampai akhir periode")).toBeInTheDocument();
    expect(within(panel).getByText("Total").parentElement).toHaveTextContent("TotalRp 350.000");
  });

  it("says why the next payday cannot be estimated", async () => {
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/calendar")) {
        const fixture = calendarFixture();
        return { ...fixture, window: { ...fixture.window, basis: "cycle" }, expected_salary: { date: null, available: false, status: "unknown", reason: "too_few_receipts" } };
      }
      return { transactions: [], total: 0 };
    });
    wrap(<KalenderTab basis="cycle" offset={0} day={null} onSelectDay={vi.fn()} onWindowLabel={() => {}} />);
    expect(await screen.findByText("Hari ke-3 dari 30 · Tanggal gaji berikutnya belum bisa diperkirakan: baru sedikit gaji yang tercatat.")).toBeInTheDocument();
  });

  it("keeps the whole grid on the phone and drops the decimal below 360 px", async () => {
    vi.stubGlobal("matchMedia", (query: string) => ({ matches: query.includes("767") || query.includes("359"), addEventListener() {}, removeEventListener() {} }));
    wrap(<KalenderTab basis="month" offset={0} day="2026-11-03" onSelectDay={vi.fn()} onWindowLabel={() => {}} />);
    const grid = await screen.findByRole("grid");
    // The column heads and six weeks: 26 Oct to 6 Dec.
    expect(within(grid).getAllByRole("row")).toHaveLength(7);
    await waitFor(() => expect(within(grid).getByRole("gridcell", { name: /Selasa, 3 November/ })).toHaveTextContent("1jt"));
    expect(screen.queryByRole("group", { name: /Minggu/ })).not.toBeInTheDocument();
  });

  it("names the grid and the markers in English", async () => {
    renderWithIntl(<QueryClientProvider client={client()}><KalenderTab basis="month" offset={0} day="2026-11-02" onSelectDay={vi.fn()} onWindowLabel={() => {}} /></QueryClientProvider>, { language: "en" });
    const grid = await screen.findByRole("grid", { name: "Calendar for November 2026" });
    expect(within(grid).getByRole("gridcell", { name: /Monday, 2 November/ })).toHaveAccessibleName("Monday, 2 November. Spending Rp 109.000. 1 waiting for an answer. Transfer.");
    expect(within(grid).getByRole("gridcell", { name: /Tuesday, 3 November/ })).toHaveTextContent("1,3M");
    expect(screen.getByText("Spending this period")).toBeInTheDocument();
  });
});

function flowFixture(): FlowResponse {
  return {
    window: { start: "2026-10-01", end: "2026-10-31", through: "2026-10-31", label: "Oktober 2026", is_current: false },
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
      spending: 1_200_000, saving_net: 900_000, investment_net: 0, waiting_out: 200_000, waiting_in: 0, waiting_in_buckets: 45_000, remaining: 7_300_000, other_out: 0,
    },
  };
}

const LAYOUT = { width: 960, height: 420, stationWidth: 118, stationMin: 46, barWidth: 12, barMin: 3, pad: 12, leftLabel: 150, rightLabel: 220 };

describe("Peta Aliran layout", () => {
  it("hides balances and income when asked, mirrors money flowing back and places every node deterministically", () => {
    const fixture = flowFixture();
    const bare = buildFlowModel(fixture.nodes, fixture.links, { balances: false, income: false });
    expect(bare.hiddenLinkKeys.sort()).toEqual(["acc:bank>remaining", "acc:wallet>remaining", "opening>acc:bank", "sav:dd>remaining", "src:gaji>acc:bank"]);
    const open = buildFlowModel(fixture.nodes, fixture.links, { balances: true, income: true });
    expect(open.nodes.get("remaining")?.column).toBe(open.columns - 1);
    expect(open.nodes.get("sav:dd")?.column).toBe(open.nodes.get("pillar:need")?.column);
    expect(bare.nodes.has("remaining")).toBe(false);
    // The goal pocket paying back to the bank is drawn from its source-side copy.
    expect(bare.nodes.get("sav:dd@in")?.column).toBe(0);
    expect(bare.nodes.get("acc:wallet")?.column).toBe(2);
    expect(bare.links.every((link) => bare.nodes.get(link.target)!.column > bare.nodes.get(link.source)!.column)).toBe(true);

    const first = layoutFlow(bare, LAYOUT);
    const second = layoutFlow(buildFlowModel(fixture.nodes, fixture.links, { balances: false, income: false }), LAYOUT);
    expect([...first.nodes.values()]).toEqual([...second.nodes.values()]);
    expect(first.links.map((link) => link.path)).toEqual(second.links.map((link) => link.path));
    for (let column = 0; column < bare.columns; column++) {
      const stacked = [...first.nodes.values()].filter((node) => node.column === column).sort((a, b) => a.y0 - b.y0);
      for (let index = 1; index < stacked.length; index++) expect(stacked[index].y0).toBeGreaterThanOrEqual(stacked[index - 1].y1);
      for (const node of stacked) expect(node.y1).toBeLessThanOrEqual(LAYOUT.height + 0.5);
    }
    expect(nextNode(first, "acc:bank", "right")).toBe("acc:wallet");
    expect(nextNode(first, "acc:bank", "left")).not.toBeNull();
  });

  it("draws income and never the balances on the web map, in at most four columns with 24 px slots", () => {
    const fixture = flowFixture();
    const model = buildFlowModel(fixture.nodes, fixture.links, { balances: false, income: true, maxDepth: 2 });
    expect(model.nodes.has("src:gaji")).toBe(true);
    expect(model.nodes.has("remaining")).toBe(false);
    expect(model.links.some((link) => link.kind === "opening" || link.kind === "remaining")).toBe(false);
    expect(model.columns).toBeLessThanOrEqual(4);
    const placed = layoutFlow(model, { width: 960, height: 320, stationWidth: 8, stationMin: 2, barWidth: 8, barMin: 2, pad: 8, leftLabel: 0, rightLabel: 220, slot: 24 });
    for (let column = 0; column < model.columns; column++) {
      const stacked = [...placed.nodes.values()].filter((node) => node.column === column).sort((a, b) => a.y0 - b.y0);
      for (let index = 1; index < stacked.length; index++) expect(stacked[index].y0 - stacked[index - 1].y0).toBeGreaterThanOrEqual(24);
    }
  });

  it("handles an empty period and a single account", () => {
    const empty = buildFlowModel([], [], { balances: false, income: false });
    expect(empty.columns).toBe(0);
    expect(layoutFlow(empty, LAYOUT).nodes.size).toBe(0);
    const fixture = flowFixture();
    const one = buildFlowModel(fixture.nodes, fixture.links.filter((link) => link.key === "src:gaji>acc:bank"), { balances: true, income: true });
    expect(one.columns).toBe(2);
  });

  it("totals a node without its balance links and cuts a long name to its room", () => {
    expect(nodeTotals("acc:bank", flowFixture().links)).toEqual({ inbound: 8_100_000, outbound: 3_000_000 });
    const name = "Rekening Tabungan Pendidikan Anak Pertama";
    const cut = fitLabel(name, 120);
    expect(cut.endsWith("…")).toBe(true);
    expect(labelWidth(cut)).toBeLessThanOrEqual(120);
    expect(fitLabel("Gaji", 120)).toBe("Gaji");
  });
});

describe("Peta Aliran tab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    stubPhone(false);
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/flow/rows")) return { rows: [{ id: "tx-1", link: "acc:bank>pillar:need", type: "expense", amount: 1_500_000, row_amount: 1_500_000, date: "2026-10-02T02:00:00Z", local_date: "2026-10-02", account_id: "bank", account_name: "Bank Utama", category_name: "Makan", merchant: "Pasar Contoh", notes: null, movement_id: null, waiting: false }], total_count: 1, total_amount: 1_500_000, offset: 0, limit: 50 };
      if (path.startsWith("/dashboard/flow")) return flowFixture();
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("shows Masuk with four supporting figures from the summary and never asks for a replay day", async () => {
    stubWidth(1104);
    const { container } = wrap(<PetaAliranTab basis="month" offset={-1} view={null} onView={() => {}} onWindowLabel={() => {}} />);
    const hero = await screen.findByRole("region", { name: "Aliran uang periode ini · masuk" });
    expect(hero.querySelector(".hero-fig")).toHaveTextContent("Rp 8.000.000");
    const figures = [...hero.querySelectorAll(".fig-block")].map((block) => block.textContent);
    expect(figures).toEqual([
      "PengeluaranRp 1.200.000", "Ditabung & top upRp 900.000", "Menunggu pasanganRp 200.000Ditambah Rp 45.000 menunggu jawaban di dalam pilarnya.", "Masih adaRp 7.300.000",
    ]);
    expect(vi.mocked(api.get).mock.calls.every(([path]) => !String(path).includes("through"))).toBe(true);
    expect(screen.queryByRole("button", { name: /Tampilkan uang masuk|Putar ulang/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("slider")).not.toBeInTheDocument();
    expect(container.querySelector("[data-band='income']")).not.toBeNull();
    expect(container.querySelector("[data-band='opening'], [data-band='remaining']")).toBeNull();
    const legend = container.querySelector(".flow-legend")!;
    for (const name of ["Masuk", "Pindah saldo (garis miring)", "Pengeluaran", "Tabungan & investasi (titik)", "Menunggu pasangan (arsir)"]) {
      expect(legend).toHaveTextContent(name);
    }
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("dims the other bands to 0.06 when a kind is highlighted", async () => {
    stubWidth(1104);
    const user = userEvent.setup();
    const { container } = wrap(<PetaAliranTab basis="month" offset={-1} view="peta" onView={() => {}} onWindowLabel={() => {}} />);
    await screen.findByRole("group", { name: "Peta aliran uang. Tabel tersedia lewat tombol Tabel." });
    expect(container.querySelector("[data-band='transfer'] path")).toHaveAttribute("fill-opacity", "0.3");
    await user.click(within(screen.getByRole("group", { name: "Sorot aliran" })).getByRole("button", { name: "Pengeluaran" }));
    expect(container.querySelector("[data-band='spend'] path")).toHaveAttribute("fill-opacity", "0.3");
    expect(container.querySelector("[data-band='transfer'] path")).toHaveAttribute("fill-opacity", "0.06");
  });

  it("keeps every label inside the frame at 1440, 1200 and 1024 px with a 40-character account name", async () => {
    const long = "Rekening Tabungan Pendidikan Anak Pertama";
    for (const width of [1104, 864, 688]) {
      stubWidth(width);
      vi.mocked(api.get).mockImplementation(async () => {
        const fixture = flowFixture();
        return { ...fixture, nodes: fixture.nodes.map((node) => (node.id === "acc:wallet" ? { ...node, label: long } : node)) };
      });
      const view = wrap(<PetaAliranTab basis="month" offset={-1} view="peta" onView={() => {}} onWindowLabel={() => {}} />);
      await screen.findByRole("group", { name: /Peta aliran uang/ });
      const svg = view.container.querySelector("svg")!;
      const frame = Number(svg.getAttribute("width"));
      expect(frame).toBe(Math.max(MAP.minWidth, width));
      for (const label of view.container.querySelectorAll<SVGTextElement>(".flow-label")) {
        const x = Number(label.getAttribute("x"));
        const [name, amount] = [...label.querySelectorAll("tspan")].map((span) => span.textContent ?? "");
        expect(x + labelWidth(name) + MAP.labelGap + labelWidth(amount), `${name} at ${width}`).toBeLessThanOrEqual(frame);
      }
      const wallet = view.container.querySelector("[data-node='acc:wallet']")!;
      expect(wallet.querySelector(".lbl-node")?.textContent?.endsWith("…")).toBe(true);
      expect(wallet).toHaveAccessibleName(`${long}: masuk Rp 500.000, keluar Rp 200.000.`);
      view.unmount();
    }
  });

  it("walks the nodes with the arrow keys and opens a centered dialog that returns focus", async () => {
    stubWidth(1104);
    const user = userEvent.setup();
    wrap(<PetaAliranTab basis="month" offset={-1} view="peta" onView={() => {}} onWindowLabel={() => {}} />);
    const map = await screen.findByRole("group", { name: /Peta aliran uang/ });
    const bank = within(map).getByRole("button", { name: /^Bank Utama/ });
    bank.focus();
    await user.keyboard("{ArrowRight}");
    const next = within(map).getByRole("button", { name: /^Dompet Digital/ });
    expect(next).toHaveFocus();
    await user.keyboard("{Enter}");
    const dialog = await screen.findByRole("dialog", { name: "Dompet Digital" });
    expect(within(dialog).getByText("Masuk dari")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Tutup" }));
    await waitFor(() => expect(next).toHaveFocus());
  });

  it("lists every link once in the table, filters by highlight and lists a link's rows in red", async () => {
    const user = userEvent.setup();
    const { container } = wrap(<PetaAliranTab basis="month" offset={-1} view="tabel" onView={() => {}} onWindowLabel={() => {}} />);
    const table = await screen.findByRole("table");
    expect(within(table).getAllByRole("row").slice(1)).toHaveLength(flowFixture().links.length);
    expect(table).toHaveTextContent("Saldo sebelum periode");
    expect(table).toHaveTextContent("Masih ada");
    expect(within(table).getByText(/termasuk 1 menunggu jawaban/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(within(table).getByRole("button", { name: "Bank Utama → Kebutuhan" }));
    const dialog = await screen.findByRole("dialog", { name: /Bank Utama → Kebutuhan/ });
    const row = await within(dialog).findByRole("button", { name: /Pasar Contoh/ });
    expect(row.querySelector(".c-expense")).toHaveTextContent("−Rp 1.500.000");
    expect(api.get).toHaveBeenCalledWith(expect.stringContaining("link=acc%3Abank%3Epillar%3Aneed"));
    await user.click(within(dialog).getByRole("button", { name: "Tutup" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(within(table).getByRole("button", { name: "Bank Utama → Kebutuhan" })).toHaveFocus();
    await user.click(within(screen.getByRole("group", { name: "Sorot aliran" })).getByRole("button", { name: "Pindah saldo" }));
    expect(within(screen.getByRole("table")).getAllByRole("row")).toHaveLength(2);
  });

  it("opens the table by default on the phone", async () => {
    stubPhone(true);
    wrap(<PetaAliranTab basis="month" offset={-1} view={null} onView={() => {}} onWindowLabel={() => {}} />);
    expect(await screen.findByRole("table")).toBeInTheDocument();
  });
});
