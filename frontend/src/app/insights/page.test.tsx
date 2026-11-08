import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import AnalyticsPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const navigation = vi.hoisted(() => ({ params: new URLSearchParams() }));
vi.mock("next/navigation", () => ({
  useSearchParams: () => navigation.params,
  usePathname: () => "/insights",
  useRouter: () => ({ replace: vi.fn(), push: vi.fn() }),
}));
const urlState = vi.hoisted(() => ({ replaceSearch: vi.fn() }));
vi.mock("@/lib/urlState", () => urlState);
const app = vi.hoisted(() => ({ timeframe: "month", cycleOffset: 0, hide: false, setTimeframe: vi.fn(), setCycleOffset: vi.fn() }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    timeframe: app.timeframe, setTimeframe: app.setTimeframe, cycleOffset: app.cycleOffset, setCycleOffset: app.setCycleOffset,
    hideBalances: app.hide, bal: (amount: number) => (app.hide ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`),
  }),
}));
vi.mock("@/components/recurring/RecurringRulesModal", () => ({
  RecurringRulesModal: ({ open, expenseDraft }: { open: boolean; expenseDraft?: { name: string; amount: number } | null }) =>
    open ? <div role="dialog" aria-label="Rencana baru">{expenseDraft?.name} {expenseDraft?.amount}</div> : null,
}));

/** Synthetic data: the running November, the whole October and the three months before it. */
const periods = {
  basis: "month", cycle_available: true,
  periods: [
    { kind: "month", basis: "month", label: "November 2026", start: "2026-11-01T00:00:00+07:00", end: "2026-11-14T12:00:00+07:00", start_date: "2026-11-01", end_date: "2026-11-14", days: 14, complete: false, tracking_started_inside: false, first_recorded_date: null, url: "" },
    { kind: "month", basis: "month", label: "Oktober 2026", start: "2026-10-01T00:00:00+07:00", end: "2026-10-31T23:59:59+07:00", start_date: "2026-10-01", end_date: "2026-10-31", days: 31, complete: true, tracking_started_inside: false, first_recorded_date: null, url: "" },
  ],
};

const history = [
  { label: "Agustus 2026", start: "2026-07-31T17:00:00Z", end: "2026-08-31T16:59:59Z", complete: true, partial: false, viewed: false, spending: 5_000_000, pillars: {} },
  { label: "September 2026", start: "2026-08-31T17:00:00Z", end: "2026-09-30T16:59:59Z", complete: true, partial: false, viewed: false, spending: 5_240_000, pillars: {} },
  { label: "Oktober 2026", start: "2026-09-30T17:00:00Z", end: "2026-10-31T16:59:59Z", complete: true, partial: false, viewed: false, spending: 5_452_830, pillars: {} },
];

const pillarRef = (pillar: string) => ({ from: "2026-11-01T00:00:00+07:00", to: "2026-11-14T12:00:00+07:00", kakeibo_type: pillar });

function runningComparison(withBaseline = true) {
  return {
    empty: false, cycle_available: true,
    comparison: {
      basis: "month",
      period: { label: "November 2026", start: "2026-11-01T00:00:00+07:00", end: "2026-11-14T12:00:00+07:00", start_date: "2026-11-01", end_date: "2026-11-14", complete: false, partial: false, tracking_from: null },
      running: true,
      totals: { spending: 3_502_990, inflow: 9_518_400, fresh_saving: 500_000, difference: 6_015_410 },
      previous: {
        label: "Oktober 2026", start: "2026-10-01T00:00:00+07:00", end: "2026-10-31T23:59:59+07:00", start_date: "2026-10-01", end_date: "2026-10-31", complete: true, partial: false, tracking_from: null,
        totals: { spending: 5_452_830, inflow: 9_000_000, fresh_saving: 0, difference: 0 }, change: { spending: null, inflow: null, fresh_saving: null, difference: null },
      },
      baseline: withBaseline
        ? { period_count: 3, labels: ["Agustus 2026", "September 2026", "Oktober 2026"], totals: { spending: 5_404_610, inflow: 9_000_000, fresh_saving: 0, difference: 0 }, change: { spending: null, inflow: null, fresh_saving: null, difference: null } }
        : null,
      progress: {
        previous_label: "Oktober 2026", previous_spending: 5_452_830, spending: 3_502_990, share: 64, baseline_share: withBaseline ? 65 : null,
        pillars: [
          { pillar: "need", amount: 2_000_000, previous: 3_000_000, share: 67 },
          { pillar: "want", amount: 1_402_990, previous: 2_000_000, share: 70 },
          { pillar: "culture", amount: 0, previous: 452_830, share: 0 },
          { pillar: "unexpected", amount: 0, previous: 0, share: null },
          { pillar: "unclassified", amount: 100_000, previous: 0, share: null },
        ],
      },
      pillars: [
        { pillar: "need", amount: 2_000_000, share: 57, previous: 3_000_000, change: null, baseline: 3_000_000, baseline_change: null, ref: pillarRef("need"), categories: [] },
        { pillar: "want", amount: 1_402_990, share: 40, previous: 2_000_000, change: null, baseline: 2_000_000, baseline_change: null, ref: pillarRef("want"), categories: [] },
        { pillar: "culture", amount: 0, share: 0, previous: 452_830, change: null, baseline: 404_610, baseline_change: null, ref: pillarRef("culture"), categories: [] },
        { pillar: "unexpected", amount: 0, share: 0, previous: 0, change: null, baseline: 0, baseline_change: null, ref: pillarRef("unexpected"), categories: [] },
        { pillar: "unclassified", amount: 100_000, share: 3, previous: 0, change: null, baseline: 0, baseline_change: null, ref: { ...pillarRef("unclassified"), kakeibo_type: "unclassified" }, categories: [] },
      ],
      facts: [], history,
    },
  };
}

function completedComparison() {
  return {
    empty: false, cycle_available: true,
    comparison: {
      basis: "month",
      period: { label: "Oktober 2026", start: "2026-10-01T00:00:00+07:00", end: "2026-10-31T23:59:59+07:00", start_date: "2026-10-01", end_date: "2026-10-31", complete: true, partial: false, tracking_from: null },
      running: false,
      totals: { spending: 5_993_830, inflow: 9_000_000, fresh_saving: 0, difference: 3_006_170 },
      previous: {
        label: "September 2026", start: "2026-09-01T00:00:00+07:00", end: "2026-09-30T23:59:59+07:00", start_date: "2026-09-01", end_date: "2026-09-30", complete: true, partial: false, tracking_from: null,
        totals: { spending: 5_240_000, inflow: 9_000_000, fresh_saving: 0, difference: 0 }, change: { spending: 753_830, inflow: 0, fresh_saving: 0, difference: 0 },
      },
      baseline: { period_count: 3, labels: ["Juli 2026", "Agustus 2026", "September 2026"], totals: { spending: 4_943_333, inflow: 9_000_000, fresh_saving: 0, difference: 0 }, change: { spending: 1_050_497, inflow: 0, fresh_saving: 0, difference: 0 } },
      progress: null,
      pillars: [
        { pillar: "need", amount: 3_993_830, share: 67, previous: 3_000_000, change: 993_830, baseline: 3_100_000, baseline_change: 893_830, ref: pillarRef("need"), categories: [] },
        { pillar: "want", amount: 2_000_000, share: 33, previous: 2_240_000, change: -240_000, baseline: 2_120_000, baseline_change: -120_000, ref: pillarRef("want"), categories: [] },
      ],
      facts: [],
      history: [
        { label: "Juli 2026", start: "2026-06-30T17:00:00Z", end: "2026-07-31T16:59:59Z", complete: true, partial: false, viewed: false, spending: 4_800_000, pillars: {} },
        { label: "Agustus 2026", start: "2026-07-31T17:00:00Z", end: "2026-08-31T16:59:59Z", complete: true, partial: false, viewed: false, spending: 4_790_000, pillars: {} },
        { label: "September 2026", start: "2026-08-31T17:00:00Z", end: "2026-09-30T16:59:59Z", complete: true, partial: false, viewed: false, spending: 5_240_000, pillars: {} },
        { label: "Oktober 2026", start: "2026-09-30T17:00:00Z", end: "2026-10-31T16:59:59Z", complete: true, partial: false, viewed: true, spending: 5_993_830, pillars: {} },
      ],
    },
  };
}

const category = (overrides: Record<string, unknown>) => ({
  id: "c", name: "Kategori", kind: "expense", kakeibo_type: "need", budget: null, monthly_budget: null, spent: 0, percentage_used: null, status: "unlimited", ...overrides,
});

const variance = [
  category({ id: "dapur", name: "Belanja Dapur", budget: 1_000_000, monthly_budget: 1_000_000, spent: 590_000, percentage_used: 59, status: "ok" }),
  category({ id: "cicilan", name: "Cicilan", spent: 850_000 }),
  category({ id: "jajan", name: "Jajan", kakeibo_type: "want", budget: 500_000, monthly_budget: 500_000, spent: 527_000, percentage_used: 105.4, status: "over_budget" }),
  category({ id: "lama", name: "Langganan Lama", kakeibo_type: "want", is_archived: true, spent: 30_000 }),
  category({ id: null, name: "Tanpa kategori", kakeibo_type: null, is_uncategorized: true, spent: 70_000 }),
  category({ id: "buku", name: "Buku", kakeibo_type: "culture" }),
  category({ id: "hadiah", name: "Hadiah", kakeibo_type: "want" }),
];

const spendingExplanation = {
  total: 3_502_990, caveat: "Penggantian dikurangkan dari pilarnya.", href: "/ledger?period=month&type=expense",
  lines: [{ key: "need", label: "Kebutuhan", amount: 2_100_000, note: null, href: null }, { key: "want", label: "Keinginan", amount: 1_402_990, note: "dikurangi ganti uang Rp 40.000", href: null }],
};

const charges = {
  window: { start: "2026-05-01T00:00:00+07:00", end: "2026-11-14T12:00:00+07:00", start_date: "2026-05-01", end_date: "2026-11-14", months: 6 },
  monthly_equivalent_total: 163_323, monthly_equivalent_without_plan: 54_990,
  charges: [
    { key: "musik", merchant: "Musik Contoh", search_text: "Musik Contoh", cadence: "monthly", occurrences: 5, typical_amount: 54_990, last_amount: 60_000, previous_amount: 54_990, amount_changed: true,
      last_date: "2026-11-05", next_expected_date: "2026-12-05", monthly_equivalent: 54_990, account_name: "Kas Contoh", existing_plan: null,
      plan_draft: { name: "Musik Contoh", type: "expense", amount: 54_990, source_account_id: "a", category_id: null, schedule_type: "monthly_day", schedule_day: 5 } },
    { key: "kopi", merchant: "Kopi Contoh", search_text: "Kopi Contoh", cadence: "weekly", occurrences: 6, typical_amount: 25_000, last_amount: 25_000, previous_amount: 25_000, amount_changed: false,
      last_date: "2026-11-09", next_expected_date: "2026-11-16", monthly_equivalent: 108_333, account_name: "Kas Contoh", existing_plan: { id: "rule-1", name: "Kopi mingguan" },
      plan_draft: { name: "Kopi Contoh", type: "expense", amount: 25_000, source_account_id: "a", category_id: null, schedule_type: "weekly", schedule_day: 5 } },
  ],
};

const managed = [
  { id: "dapur", name: "Belanja Dapur", kind: "expense", kakeibo_type: "need", monthly_budget: 1_000_000, icon: "shopping-cart", color: "#10b981", is_archived: false, system_key: null },
  { id: "hobi", name: "Hobi", kind: "expense", kakeibo_type: "want", monthly_budget: 500_000, icon: "film", color: "#f97316", is_archived: false, system_key: null },
  { id: "gaji", name: "Gaji", kind: "income", kakeibo_type: null, monthly_budget: null, icon: "wallet", color: "#3b82f6", is_archived: false, system_key: null },
  { id: "lama", name: "Langganan Lama", kind: "expense", kakeibo_type: "want", monthly_budget: null, icon: "tag", color: "#64748b", is_archived: true, system_key: null },
  { id: "pindah", name: "Pindah Saldo", kind: "expense", kakeibo_type: null, monthly_budget: null, icon: "tag", color: "#64748b", is_archived: false, system_key: "internal_movement" },
];

interface Serve {
  comparison?: unknown;
  analytics?: Record<string, unknown>;
  latest?: unknown;
}

function serve({ comparison = runningComparison(), analytics = {}, latest = { ready: true, basis: "month", period: { label: "Oktober 2026", start_date: "2026-10-01", end_date: "2026-10-31" }, url: "/laporan?basis=month&start=2026-10-01" } }: Serve = {}) {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path.startsWith("/dashboard/analytics?")) {
      return {
        timeframe: "month", timeframe_label: "November 2026", category_variance: variance,
        daily_cadence: [{ date: "2026-11-01", label: "", expense: 40_000, income: 0 }, { date: "2026-11-02", label: "", expense: 120_000, income: 0 }],
        explanations: { spending: spendingExplanation }, ...analytics,
      };
    }
    if (path.startsWith("/reports/monthly/periods?")) return periods;
    if (path.startsWith("/reports/monthly/comparison?")) return comparison;
    if (path === "/reports/monthly/latest") return latest;
    if (path === "/dashboard/recurring-charges") return charges;
    if (path === "/categories?include_archived=true") return { categories: managed };
    if (path === "/categories?kind=expense") return { categories: managed.filter((row) => row.kind === "expense").map((row) => ({ ...row, is_excluded_from_budget: false })) };
    if (path.startsWith("/dashboard/period-comparison")) throw new Error("Analisis must not ask for the same-day comparison");
    throw new Error(`Unexpected GET ${path}`);
  });
}

function queryClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

function renderPage() {
  return render(<QueryClientProvider client={queryClient()}><AnalyticsPage /></QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
  navigation.params = new URLSearchParams();
  app.timeframe = "month";
  app.cycleOffset = 0;
  app.hide = false;
  vi.mocked(api.post).mockResolvedValue({ ok: true });
  vi.mocked(api.patch).mockResolvedValue({ ok: true });
  vi.mocked(api.del).mockResolvedValue({ ok: true });
  serve();
});

describe("Analisis tabs", () => {
  it("selects Ringkasan for an old link or an unknown tab and links the monthly report", async () => {
    navigation.params = new URLSearchParams("tab=grafik");
    renderPage();
    const tabs = screen.getByRole("tablist", { name: "Bagian analisis" });
    expect(within(tabs).getByRole("tab", { name: "Ringkasan" })).toHaveAttribute("aria-selected", "true");
    expect(within(tabs).getByRole("tab", { name: "Kalender" })).toHaveAttribute("tabindex", "-1");
    expect(screen.getByRole("link", { name: "Baca Laporan Bulanan" })).toHaveAttribute("href", "/laporan");
    expect(await screen.findByRole("tabpanel")).toHaveAttribute("aria-labelledby", "analysis-tab-ringkasan");
  });

  it("moves with the arrow keys, Home and End and keeps every other parameter in the URL", async () => {
    const user = userEvent.setup();
    navigation.params = new URLSearchParams("tab=aliran&from=beranda");
    renderPage();
    const flow = screen.getByRole("tab", { name: "Peta Aliran" });
    flow.focus();
    // The mocked URL stays at Peta Aliran, so each key moves from there.
    await user.keyboard("{ArrowLeft}");
    expect(urlState.replaceSearch).toHaveBeenLastCalledWith("/insights?tab=kalender&from=beranda");
    expect(screen.getByRole("tab", { name: "Kalender" })).toHaveFocus();
    await user.keyboard("{Home}");
    expect(urlState.replaceSearch).toHaveBeenLastCalledWith("/insights?tab=ringkasan&from=beranda");
    expect(screen.getByRole("tab", { name: "Ringkasan" })).toHaveFocus();
    await user.keyboard("{End}");
    expect(urlState.replaceSearch).toHaveBeenLastCalledWith("/insights?tab=aliran&from=beranda");
  });

  it("names the period from its dates in English and keeps the basis segment", async () => {
    app.cycleOffset = -1;
    const user = userEvent.setup();
    renderWithIntl(<QueryClientProvider client={queryClient()}><AnalyticsPage /></QueryClientProvider>, { language: "en" });
    const stepper = await screen.findByRole("group", { name: "Period" });
    await waitFor(() => expect(stepper).toHaveTextContent("Oct 2026"));
    expect(stepper).not.toHaveTextContent("Oktober");
    const basis = screen.getByRole("group", { name: "Period basis" });
    expect(within(basis).getByRole("button", { name: "Calendar month" })).toHaveAttribute("aria-pressed", "true");
    await user.click(within(basis).getByRole("button", { name: "Pay cycle" }));
    expect(app.setTimeframe).toHaveBeenCalledWith("cycle");
  });

  it("says when a rolling window is shown as this cycle, without changing the shared value", async () => {
    app.timeframe = "rolling30";
    renderPage();
    expect(screen.getByText("Analisis memakai siklus gaji atau bulan kalender. Yang tampil: siklus ini.")).toBeInTheDocument();
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/dashboard/analytics?timeframe=cycle&cycle_offset=0"));
    expect(app.setTimeframe).not.toHaveBeenCalled();
  });
});

describe("Ringkasan", () => {
  it("shows Pengeluaran as the one hero figure with one line, the running shares and no verdict", async () => {
    const { container } = renderPage();
    const hero = await screen.findByRole("region", { name: "Pengeluaran · November 2026 sejauh ini" });
    expect(hero.querySelector(".hero-fig")).toHaveTextContent("Rp 3.502.990");
    expect(hero.querySelector(".hero-sub")).toHaveTextContent("Masuk Rp 9.518.400 · Ditabung & investasi Rp 500.000");
    expect(container.querySelectorAll(".t-hero")).toHaveLength(1);
    expect(screen.queryByRole("region", { name: "Arus kas tercatat" })).not.toBeInTheDocument();
    expect(container.querySelector(".ws-explain, [data-explainable]")).toBeNull();
    expect(hero).toHaveTextContent("64% dari total Oktober 2026");
    expect(hero).toHaveTextContent("65% dari rata-rata 3 bulan (Agu–Okt)");
    expect(hero).toHaveTextContent("Rp 5.452.830");
    expect(hero).toHaveTextContent("Rp 5.404.610");
    expect(hero).toHaveTextContent("Dibanding total periode lalu; tanpa penilaian sampai periodenya selesai.");
    expect(hero.textContent).not.toMatch(/[▲▼]|naik|turun/);
    expect(hero.querySelectorAll(".prog")).toHaveLength(2);
    expect(api.get).toHaveBeenCalledWith("/reports/monthly/comparison?basis=month&start_date=2026-11-01");
    expect(screen.getAllByRole("button", { name: /Rincian/ })).toHaveLength(1);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("opens the Rincian with the server's lines only when they add up to the figure, and returns focus", async () => {
    const user = userEvent.setup();
    renderPage();
    const trigger = await screen.findByRole("button", { name: "Rincian pengeluaran" });
    await user.click(trigger);
    const dialog = await screen.findByRole("dialog", { name: "Pengeluaran · November 2026 sejauh ini" });
    expect(dialog).toHaveTextContent("Kebutuhan");
    expect(dialog).toHaveTextContent("Rp 2.100.000");
    expect(dialog).toHaveTextContent("Keinginan: dikurangi ganti uang Rp 40.000");
    expect(dialog).toHaveTextContent("Total pengeluaran");
    await user.click(within(dialog).getByRole("button", { name: "Tutup rincian" }));
    expect(trigger).toHaveFocus();
  });

  it("offers no Rincian for an older API without explanations and shows no Rp 0 in place of a figure", async () => {
    serve({ analytics: { explanations: undefined } });
    renderPage();
    const hero = await screen.findByRole("region", { name: /Pengeluaran · November 2026/ });
    expect(hero.querySelector(".hero-fig")).toHaveTextContent("Rp 3.502.990");
    expect(screen.queryByRole("button", { name: "Rincian pengeluaran" })).not.toBeInTheDocument();
  });

  it("reads a completed period as changes against the previous total and the average", async () => {
    app.cycleOffset = -1;
    serve({ comparison: completedComparison() });
    renderPage();
    const hero = await screen.findByRole("region", { name: "Pengeluaran · Oktober 2026" });
    expect(hero).toHaveTextContent("Dibanding total September 2026");
    expect(hero).toHaveTextContent("▲ naik Rp 753.830");
    expect(hero).toHaveTextContent("Dibanding rata-rata 3 bulan (Jul–Sep)");
    expect(hero).toHaveTextContent("▲ naik Rp 1.050.497");
    const kakeibo = screen.getByRole("region", { name: "Kakeibo · per pilar" });
    expect(kakeibo).toHaveTextContent("jumlah · porsi · vs rata-rata 3 bulan");
    expect(within(kakeibo).getByRole("link", { name: /Keinginan/ })).toHaveTextContent("▼ turun Rp 120.000");
  });

  it("says when the average appears instead of showing a Rp 0 average", async () => {
    serve({ comparison: runningComparison(false) });
    renderPage();
    const hero = await screen.findByRole("region", { name: /Pengeluaran · November 2026/ });
    expect(hero).toHaveTextContent("Rata-rata muncul setelah 2 bulan lengkap");
    expect(hero).not.toHaveTextContent("Rp 0");
  });

  it("lists Kakeibo per pillar as ledger links and offers Perlu dipilih for unclassified spending", async () => {
    renderPage();
    const kakeibo = await screen.findByRole("region", { name: "Kakeibo · per pilar" });
    expect(kakeibo).toHaveTextContent("periode ini · porsi · periode lalu");
    const need = within(kakeibo).getByRole("link", { name: /Kebutuhan/ });
    expect(need.getAttribute("href")).toContain("kakeibo=need");
    expect(need).toHaveTextContent("Rp 2.000.000");
    expect(need).toHaveTextContent("57%");
    expect(need).toHaveTextContent("Rp 3.000.000");
    // Culture had spending only last period; Tak Terduga had none in either and is left out.
    expect(within(kakeibo).getByRole("link", { name: /Budaya/ })).toBeInTheDocument();
    expect(within(kakeibo).queryByRole("link", { name: /Tak Terduga/ })).not.toBeInTheDocument();
    expect(within(kakeibo).getByRole("link", { name: "Perlu dipilih" })).toHaveAttribute("href", "/laporan?lengkapi=1&basis=month&start=2026-11-01");
    expect(within(kakeibo).getByRole("link", { name: "Rp 100.000" }).getAttribute("href")).toContain("kakeibo=unclassified");
  });

  it("groups spending by category under pillar rails with budget bars, and links each row to the ledger", async () => {
    const user = userEvent.setup();
    renderPage();
    const section = await screen.findByRole("region", { name: "Pengeluaran per kategori" });
    const need = section.querySelector(".grp")!;
    expect(need.querySelector(".grp-h")).toHaveTextContent("Kebutuhan2 kategori");
    const rows = [...need.querySelectorAll(".cat-row")].map((row) => row.querySelector(".r-title")?.textContent);
    expect(rows).toEqual(["Cicilan", "Belanja Dapur"]);
    const dapur = within(section).getByRole("link", { name: /Belanja Dapur/ });
    expect(dapur).toHaveAttribute("href", "/ledger?period=month&category=dapur&type=expense");
    expect(dapur).toHaveTextContent("59% dari Rp 1.000.000");
    expect(within(section).getByRole("link", { name: /Cicilan/ })).toHaveTextContent("tanpa anggaran");
    const jajan = within(section).getByRole("link", { name: /Jajan/ });
    expect(jajan.querySelector(".c-expense")).toHaveTextContent("105%");
    expect((jajan.querySelector(".prog > span") as HTMLElement).style.background).toBe("var(--out-solid)");
    expect(within(section).getByRole("link", { name: /Langganan Lama/ })).toHaveTextContent("diarsipkan · tanpa anggaran");
    expect(within(section).getByRole("link", { name: /Tanpa kategori/ })).toHaveAttribute("href", "/ledger?period=month&category=uncategorized&type=expense");
    const more = within(section).getByRole("button", { name: "Tampilkan 2 kategori tanpa pengeluaran" });
    await user.click(more);
    expect(within(section).getByRole("link", { name: /Buku/ })).toBeInTheDocument();
    expect(more).toHaveTextContent("Sembunyikan 2 kategori tanpa pengeluaran");
  });

  it("shows spending only while no category has a budget", async () => {
    serve({ analytics: { category_variance: variance.map((row) => ({ ...row, budget: null, monthly_budget: null, percentage_used: null })) } });
    renderPage();
    const section = await screen.findByRole("region", { name: "Pengeluaran per kategori" });
    expect(section).not.toHaveTextContent("tanpa anggaran");
    expect(section.querySelector(".prog")).toBeNull();
  });

  it("shows the bills panel with its count, four rows, every charge in Lihat semua and a plan only after confirmation", async () => {
    const user = userEvent.setup();
    renderPage();
    const panel = await screen.findByRole("complementary", { name: "Konteks" });
    expect(await within(panel).findByText("2 · Rp 163.323/bln")).toBeInTheDocument();
    expect(within(panel).getByRole("link", { name: /Kopi Contoh/ })).toHaveTextContent("mingguan · berikutnya 16 Nov");
    await user.click(within(panel).getByRole("button", { name: "Lihat semua" }));
    const dialog = await screen.findByRole("dialog", { name: "Tagihan & langganan terdeteksi" });
    expect(dialog).toHaveTextContent("Sudah ada rencana: Kopi mingguan");
    expect(dialog).toHaveTextContent("Nominal terakhir berubah: Rp 54.990 → Rp 60.000");
    await user.click(within(dialog).getByRole("button", { name: "Jadikan rencana: Musik Contoh" }));
    expect(screen.getByRole("dialog", { name: "Rencana baru" })).toHaveTextContent("Musik Contoh 54990");
    expect(api.post).not.toHaveBeenCalled();
    expect(within(panel).getByRole("link", { name: /Laporan Oktober 2026 siap dibaca/ })).toHaveAttribute("href", "/laporan?basis=month&start=2026-10-01");
  });

  it("leaves the report block out while no report is ready", async () => {
    serve({ latest: { ready: false, period: null, url: null } });
    renderPage();
    const panel = await screen.findByRole("complementary", { name: "Konteks" });
    await within(panel).findByText("2 · Rp 163.323/bln");
    expect(within(panel).queryByText(/siap dibaca/)).not.toBeInTheDocument();
  });

  it("says when the period has no spending instead of showing Rp 0 figures", async () => {
    const empty = runningComparison();
    empty.comparison.totals = { spending: 0, inflow: 0, fresh_saving: 0, difference: 0 };
    serve({ comparison: empty, analytics: { daily_cadence: [{ date: "2026-11-01", label: "", expense: 0, income: 0 }], explanations: undefined } });
    renderPage();
    expect(await screen.findByText("Belum ada pengeluaran di periode ini.")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: /Pengeluaran · November/ })).not.toBeInTheDocument();
  });

  it("keeps the stepper and the tabs working when the comparison fails", async () => {
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/reports/monthly/comparison?")) throw new Error("down");
      if (path.startsWith("/reports/monthly/periods?")) return periods;
      if (path.startsWith("/dashboard/analytics?")) return { timeframe_label: "November 2026", category_variance: [], daily_cadence: [] };
      if (path === "/dashboard/recurring-charges") return charges;
      if (path === "/reports/monthly/latest") return { ready: false, period: null, url: null };
      throw new Error(`Unexpected GET ${path}`);
    });
    renderPage();
    expect(await screen.findByText("Perbandingan periode belum bisa dimuat.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    expect(screen.getByRole("tablist")).toBeInTheDocument();
  });

  it("reads in English with the same figures", async () => {
    const { container } = renderWithIntl(<QueryClientProvider client={queryClient()}><AnalyticsPage /></QueryClientProvider>, { language: "en" });
    const hero = await screen.findByRole("region", { name: "Spending · Nov 2026 so far" });
    expect(hero.querySelector(".hero-fig")).toHaveTextContent("Rp 3.502.990");
    expect(hero).toHaveTextContent("64% of the total for Oktober 2026");
    expect(hero).toHaveTextContent("65% of the 3-month average (Aug–Oct)");
    for (const name of ["Overview", "Calendar", "Money flow"]) expect(screen.getByRole("tab", { name })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Daily spending" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Spending by category" })).toBeInTheDocument();
    expect(container.querySelector("[lang='id']")).toBeNull();
  });
});

describe("Category dialogs", () => {
  async function openManager(user: ReturnType<typeof userEvent.setup>) {
    renderPage();
    await user.click(await screen.findByRole("button", { name: "Kelola kategori" }));
    return screen.findByRole("dialog", { name: "Kelola kategori" });
  }

  it("lists categories by pillar, income and archived ones, without system categories, and returns focus", async () => {
    const user = userEvent.setup();
    const dialog = await openManager(user);
    expect(await within(dialog).findByRole("button", { name: "Ubah Belanja Dapur" })).toBeInTheDocument();
    expect(dialog).toHaveTextContent("anggaran Rp 1.000.000");
    expect(within(dialog).getByRole("heading", { name: "Pemasukan" })).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Pulihkan kategori Langganan Lama" })).toBeInTheDocument();
    expect(within(dialog).queryByText("Pindah Saldo")).not.toBeInTheDocument();
    expect(within(dialog).queryByText("Tanpa kategori")).not.toBeInTheDocument();
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Tutup" }));
    expect(screen.getByRole("button", { name: "Kelola kategori" })).toHaveFocus();
  });

  it("creates an expense category with named icon, colour, pillar and budget", async () => {
    const user = userEvent.setup();
    const dialog = await openManager(user);
    await user.click(await within(dialog).findByRole("button", { name: "Tambah kategori" }));
    const form = await screen.findByRole("dialog", { name: "Tambah kategori" });
    expect(within(form).getByRole("textbox", { name: "Nama" })).toHaveFocus();
    await user.type(within(form).getByRole("textbox", { name: "Nama" }), "Transport");
    await user.click(within(form).getByRole("radio", { name: "Keinginan" }));
    await user.click(within(form).getByRole("button", { name: "Transportasi" }));
    await user.click(within(form).getByRole("button", { name: "Oranye" }));
    await user.type(within(form).getByRole("textbox", { name: "Anggaran bulanan" }), "500000");
    await user.click(within(form).getByRole("button", { name: "Buat kategori" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/categories", {
      name: "Transport", kind: "expense", kakeibo_type: "want", icon: "car", color: "#f97316", monthly_budget: 500_000,
    }));
    expect(await screen.findByRole("dialog", { name: "Kelola kategori" })).toBeInTheDocument();
  });

  it("loads the stored values when editing, and sends an explicit null pillar for Belum diklasifikasi", async () => {
    const user = userEvent.setup();
    const dialog = await openManager(user);
    await user.click(await within(dialog).findByRole("button", { name: "Ubah Hobi" }));
    const form = await screen.findByRole("dialog", { name: "Ubah Hobi" });
    expect(within(form).getByRole("textbox", { name: "Nama" })).toHaveValue("Hobi");
    expect(within(form).getByRole("radio", { name: "Keinginan" })).toHaveAttribute("aria-checked", "true");
    expect(within(form).getByRole("button", { name: "Hiburan" })).toHaveAttribute("aria-pressed", "true");
    expect(within(form).getByRole("button", { name: "Oranye" })).toHaveAttribute("aria-pressed", "true");
    expect(within(form).getByRole("textbox", { name: "Anggaran bulanan" })).toHaveValue("500.000");
    await user.click(within(form).getByRole("radio", { name: "Belum diklasifikasi" }));
    await user.click(within(form).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/categories/hobi", expect.objectContaining({ kakeibo_type: null, icon: "film", color: "#f97316", monthly_budget: 500_000 })));
    await waitFor(() => expect(screen.getByRole("button", { name: "Ubah Hobi" })).toHaveFocus());
  });

  it("hides the pillar and budget for an income category", async () => {
    const user = userEvent.setup();
    const dialog = await openManager(user);
    await user.click(await within(dialog).findByRole("button", { name: "Tambah kategori pemasukan" }));
    const form = await screen.findByRole("dialog", { name: "Tambah kategori pemasukan" });
    expect(within(form).queryByRole("radiogroup", { name: "Pilar" })).not.toBeInTheDocument();
    expect(within(form).queryByRole("textbox", { name: "Anggaran bulanan" })).not.toBeInTheDocument();
    await user.type(within(form).getByRole("textbox", { name: "Nama" }), "Bonus");
    await user.click(within(form).getByRole("button", { name: "Buat kategori" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/categories", expect.objectContaining({ name: "Bonus", kind: "income", kakeibo_type: null, monthly_budget: null })));
  });

  it("asks before archiving and restores an archived category", async () => {
    const user = userEvent.setup();
    const dialog = await openManager(user);
    await user.click(await within(dialog).findByRole("button", { name: "Arsipkan Hobi" }));
    expect(api.del).not.toHaveBeenCalled();
    await user.click(within(dialog).getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/categories/hobi"));
    await user.click(within(dialog).getByRole("button", { name: "Pulihkan kategori Langganan Lama" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/categories/lama", { is_archived: false }));
  });

  it("sets and clears budgets in Atur anggaran with one PATCH per changed category", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(await screen.findByRole("button", { name: "Atur anggaran" }));
    const dialog = await screen.findByRole("dialog", { name: "Atur anggaran" });
    const dapur = await within(dialog).findByLabelText("Belanja Dapur");
    expect(dapur).toHaveValue("1.000.000");
    await user.clear(dapur);
    await user.type(within(dialog).getByLabelText("Hobi"), "0");
    await user.clear(within(dialog).getByLabelText("Hobi"));
    await user.type(within(dialog).getByLabelText("Hobi"), "3000000");
    await user.click(within(dialog).getByRole("button", { name: "Simpan anggaran" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledTimes(2));
    expect(api.patch).toHaveBeenCalledWith("/categories/dapur", { monthly_budget: null });
    expect(api.patch).toHaveBeenCalledWith("/categories/hobi", { monthly_budget: 3_000_000 });
    await waitFor(() => expect(screen.getByRole("button", { name: "Atur anggaran" })).toHaveFocus());
  });
});
