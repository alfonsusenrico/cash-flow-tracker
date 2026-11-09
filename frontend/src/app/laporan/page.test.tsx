import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { Language } from "@/i18n/language";
import { renderWithIntl } from "@/lib/i18n/testing";
import type { MonthlyReport, ReportComparison } from "@/lib/monthlyReport";
import { BAR_AREA, barHeight } from "@/components/report/SpendingHistory";
import LaporanPage from "./page";

const navigation = vi.hoisted(() => ({ replace: vi.fn(), params: new URLSearchParams() }));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: navigation.replace, push: vi.fn() }),
  useSearchParams: () => navigation.params,
  usePathname: () => "/laporan",
}));
vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  ApiError: class ApiError extends Error {
    status = 0;
  },
}));
const openQuickAdd = vi.fn();
const ctx = vi.hoisted(() => ({ hide: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    openQuickAdd,
    hideBalances: ctx.hide,
    bal: (amount: number) => (ctx.hide ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`),
  }),
}));
vi.mock("@/components/transactions/TransactionDialog", () => ({
  TransactionDialog: ({ transactionId, onClose }: { transactionId: string | null; onClose: () => void }) =>
    transactionId ? (
      <div role="dialog" aria-label={`Transaksi ${transactionId}`}>
        <button type="button" onClick={onClose}>Tutup transaksi</button>
      </div>
    ) : null,
}));

/** Synthetic data: the October 2026 report, its previous September and the running November. */
const START = "2026-09-30T17:00:00+00:00";
const END = "2026-10-31T16:59:59.999999+00:00";
const period = {
  kind: "month", basis: "month" as const, label: "Oktober 2026", start: START, end: END, start_date: "2026-10-01",
  end_date: "2026-10-31", days: 31, complete: true, tracking_started_inside: false, first_recorded_date: null,
  url: "/laporan?basis=month&start=2026-10-01",
};
const previousPeriod = {
  ...period, label: "September 2026", start: "2026-08-31T17:00:00+00:00", end: "2026-09-30T16:59:59.999999+00:00",
  start_date: "2026-09-01", end_date: "2026-09-30", days: 30,
};
const ref = (extra: Record<string, unknown> = {}) => ({ from: START, to: END, ...extra });
const noChange = { spending: null, inflow: null, fresh_saving: null, difference: null };
const emptyPillars = { need: 0, want: 0, culture: 0, unexpected: 0, unclassified: 0 };

function makeComparison(overrides: Partial<ReportComparison> = {}): ReportComparison {
  return {
    basis: "month",
    period: { label: "Oktober 2026", start: START, end: END, start_date: "2026-10-01", end_date: "2026-10-31", complete: true, partial: false, tracking_from: null },
    running: false,
    totals: { spending: 5_993_830, inflow: 11_068_400, fresh_saving: 2_750_000, difference: 5_074_570 },
    previous: {
      label: "September 2026", start: previousPeriod.start, end: previousPeriod.end, start_date: "2026-09-01", end_date: "2026-09-30",
      complete: true, partial: false, tracking_from: null,
      totals: { spending: 5_240_000, inflow: 10_000_000, fresh_saving: 2_000_000, difference: 4_760_000 },
      change: { spending: 753_830, inflow: 1_068_400, fresh_saving: 750_000, difference: 314_570 },
    },
    baseline: {
      period_count: 3, labels: ["Juli 2026", "Agustus 2026", "September 2026"],
      totals: { spending: 4_943_333, inflow: 10_000_000, fresh_saving: 2_000_000, difference: 5_056_667 },
      change: { spending: 1_050_497, inflow: 1_068_400, fresh_saving: 750_000, difference: 17_903 },
    },
    progress: null,
    pillars: [
      {
        pillar: "need", amount: 4_173_340, share: 70, previous: 3_420_000, change: 753_340, baseline: 3_500_000, baseline_change: 673_340,
        ref: ref({ kakeibo_type: "need" }),
        categories: [
          { category_id: "c-dapur", name: "Belanja Dapur", amount: 1_438_000, share: 24, previous: 0, change: 1_438_000, baseline: 900_000, baseline_change: 538_000, ref: ref({ category_id: "c-dapur", kakeibo_type: "need" }) },
          { category_id: "c-transport", name: "Transportasi", amount: 519_500, share: 9, previous: 610_000, change: -90_500, baseline: 600_000, baseline_change: -80_500, ref: ref({ category_id: "c-transport", kakeibo_type: "need" }) },
        ],
      },
      {
        pillar: "want", amount: 1_122_490, share: 19, previous: 1_310_000, change: -187_510, baseline: 1_100_000, baseline_change: 22_490,
        ref: ref({ kakeibo_type: "want" }),
        categories: [{ category_id: "c-jajan", name: "Jajan & Nongkrong", amount: 1_122_490, share: 19, previous: 1_310_000, change: -187_510, baseline: 1_100_000, baseline_change: 22_490, ref: ref({ category_id: "c-jajan", kakeibo_type: "want" }) }],
      },
      {
        pillar: "unexpected", amount: 698_000, share: 12, previous: 510_000, change: 188_000, baseline: 343_333, baseline_change: 354_667,
        ref: ref({ kakeibo_type: "unexpected" }),
        categories: [{ category_id: "c-servis", name: "Perbaikan & Darurat", amount: 698_000, share: 12, previous: 510_000, change: 188_000, baseline: 343_333, baseline_change: 354_667, ref: ref({ category_id: "c-servis", kakeibo_type: "unexpected" }) }],
      },
    ],
    facts: [
      { kind: "category_above_average", label: "Belanja Dapur di atas rata-rata 3 bulan", amount: 1_438_000, difference: 538_000, baseline: 900_000, ref: ref({ category_id: "c-dapur" }) },
      { kind: "top_merchant", label: "Tempat belanja terbesar: Reksa Contoh", amount: 1_500_000, ref: ref({ q: "Reksa Contoh" }) },
      { kind: "recurring", label: "3 tagihan dan langganan berulang", amount: 635_000, count: 3, ref: null },
    ],
    spending_top_merchant: { kind: "top_merchant", label: "Tempat belanja terbesar: Swalayan Hemat", amount: 1_087_000, ref: ref({ q: "Swalayan Hemat" }) },
    history: [
      { label: "Juli 2026", start: "2026-06-30T17:00:00Z", end: "2026-07-31T16:59:59Z", complete: true, partial: false, viewed: false, spending: 4_980_000, pillars: emptyPillars },
      { label: "September 2026", start: "2026-08-31T17:00:00Z", end: "2026-09-30T16:59:59Z", complete: true, partial: false, viewed: false, spending: 5_240_000, pillars: emptyPillars },
      { label: "Oktober 2026", start: START, end: END, complete: true, partial: false, viewed: true, spending: 5_993_830, pillars: emptyPillars },
      { label: "November 2026", start: "2026-10-31T17:00:00Z", end: "2026-11-04T16:59:59Z", complete: false, partial: false, viewed: false, spending: 2_463_990, pillars: emptyPillars },
    ],
    ...overrides,
  };
}

function makeReport(overrides: Partial<MonthlyReport> = {}): MonthlyReport {
  return {
    empty: false, basis: "month", cycle_available: false, period,
    flow: { inflow: 11_068_400, gross_outflow: 7_543_830, reimbursement_offset: 50_000, outflow: 7_493_830, net: 3_574_570, spending_reimbursement_offset: 40_000 },
    income: { total: 11_068_400, sources: [{ category_id: "c-gaji", name: "Gaji", amount: 9_500_000, count: 1 }, { category_id: null, name: "Tanpa kategori", amount: 1_568_400, count: 2 }] },
    savings: { fresh_saving: 2_750_000, saving_expenses: 1_500_000, net_saving_transfers: 1_250_000, surplus: 3_574_570 },
    spending: {
      total: 7_493_830,
      pillars: [],
      top_categories: [],
      top_merchants: [{ name: "Reksa Contoh", search_text: "Reksa Contoh", amount: 1_500_000 }, { name: "Swalayan Hemat", search_text: "Swalayan Hemat", amount: 1_087_000 }],
      top_spending_merchants: [
        { name: "Swalayan Hemat", search_text: "Swalayan Hemat", amount: 1_087_000 },
        { name: "Leasing Motor Sentosa", search_text: "Leasing Motor Sentosa", amount: 850_000 },
      ],
      unexpected: { amount: 698_000, count: 12, items: [{ id: "t-repair", date: "2026-10-08T05:00:00+00:00", amount: 425_000, merchant: "Bengkel Motor Jaya", notes: null, category_name: "Perbaikan & Darurat" }] },
      recurring: {
        charges: [{ merchant: "Internet Rumah Cepat", search_text: "Internet Rumah Cepat", cadence: "monthly", typical_amount: 385_000, last_amount: 385_000, amount_changed: true, last_date: "2026-10-05", monthly_equivalent: 385_000, existing_plan: { id: "p1", name: "Internet" }, category_name: null }],
        monthly_equivalent_total: 385_000,
      },
      essential: { amount: 2_300_000, living_amount: 2_300_000, debt_minimum: 0, method: "median_need_periods", limited_history: true, period_count: 3, coverage_months: 3.2, target_months: 6, status: "critical", balance_basis: "all_liquid" },
    },
    comparison: makeComparison(),
    data_quality: {
      pending_review: 2, needs_attention: 1, unreviewed: 1, unclassified: { count: 2, amount: 150_000 },
      possible_duplicates: 2, days_without_records: { count: 2, dates: ["2026-10-13", "2026-10-14"] },
    },
    budget: null,
    reflection: null,
    previous_reflection: null,
    ...overrides,
  };
}

const cleanQuality = { pending_review: 0, needs_attention: 0, unreviewed: 0, unclassified: { count: 0, amount: 0 }, possible_duplicates: 0, days_without_records: { count: 0, dates: [] } };
const overBudget = {
  period_basis: "month" as const,
  categories: [{ category_id: "c-jajan", name: "Jajan", budget: 500_000, spent: 920_000, remaining: -420_000, spending: 620_000, spending_remaining: -120_000 }],
  total_budget: 500_000, total_spent: 920_000, total_spending: 620_000,
  overall: { budget: 6_000_000, spent: 7_493_830, remaining: -1_493_830, spending: 5_993_830, spending_remaining: 6_170 },
};

function mockApi(report: MonthlyReport | { empty: true; basis: "month"; cycle_available: false }) {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path.startsWith("/reports/monthly/periods")) {
      return {
        basis: "month", cycle_available: false,
        periods: [
          { ...period, complete: false, label: "November 2026", start_date: "2026-11-01", has_reflection: false },
          { ...period, has_reflection: false },
          { ...previousPeriod, has_reflection: true },
        ],
      };
    }
    if (path.startsWith("/reports/monthly?")) return report;
    if (path.startsWith("/classification/gaps")) {
      return {
        categories: [{ id: "c-misc", name: "Lain-lain", unclassified_count: 1, unclassified_amount: 100_000 }],
        transactions: [
          { id: "t-misc", date: "2026-10-06T05:00:00+00:00", amount: 100_000, merchant: null, notes: "Contoh", category_name: "Lain-lain", account_name: "Rekening Contoh" },
          { id: "t-none", date: "2026-10-07T05:00:00+00:00", amount: 50_000, merchant: "Toko Contoh", notes: null, category_name: null, account_name: "Rekening Contoh" },
        ],
        transaction_count: 2, transaction_limit: 200,
      };
    }
    if (path.startsWith("/categories")) {
      return { categories: [{ id: "c-dinner", name: "Makan Bareng", kind: "expense", monthly_budget: null, is_archived: false, is_excluded_from_budget: false, system_key: null }] };
    }
    throw new Error(`Unexpected GET ${path}`);
  });
}

function renderPage(language: Language = "id") {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return renderWithIntl(<QueryClientProvider client={queryClient}><LaporanPage /></QueryClientProvider>, { language });
}

const hero = async () => screen.findByRole("region", { name: /^(Pengeluaran|Spending for) \S+ 20\d\d$/ });

describe("Laporan", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    ctx.hide = false;
    navigation.params = new URLSearchParams();
    vi.mocked(api.put).mockResolvedValue({ ok: true, reflection: { note: "Kurangi jajan kopi" } });
    vi.mocked(api.post).mockResolvedValue({ ok: true, corrections: 1, transaction_count: 2 });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
  });

  it("opens with one hero against both whole comparisons, then four numbered questions", async () => {
    mockApi(makeReport());
    const { container } = renderPage();
    const section = await hero();
    expect(within(section).getByRole("heading", { level: 2, name: "Pengeluaran Oktober 2026" })).toBeInTheDocument();
    expect(section).toHaveTextContent("Rp 5.993.830");
    expect(section).toHaveTextContent("▲ naik Rp 753.830 vs September · ▲ naik Rp 1.050.497 vs rata-rata 3 bulan");
    expect(container.querySelectorAll(".t-hero")).toHaveLength(1);
    const questions = [...container.querySelectorAll("h2.q-h")].map((heading) => heading.textContent);
    expect(questions).toEqual(["Berapa uang masuk?", "Berapa yang ditabung?", "Berapa pengeluarannya?", "Apa yang mau diperbaiki bulan depan?"]);
    expect([...container.querySelectorAll(".q-num")].map((node) => node.textContent)).toEqual(["1", "2", "3", "4"]);
    expect(container.querySelector(".ws-kpis, .ws-tile")).toBeNull();
    expect((await axe(container)).violations).toEqual([]);
  });

  it("answers Masuk with green income sources and Ditabung with its two parts", async () => {
    mockApi(makeReport());
    renderPage();
    const income = await screen.findByRole("region", { name: "Berapa uang masuk?" });
    expect(income).toHaveTextContent("Rp 11.068.400");
    const gaji = within(income).getByRole("link", { name: /Gaji/ });
    expect(gaji.getAttribute("href")).toContain("type=income");
    expect(gaji.getAttribute("href")).toContain("category=c-gaji");
    expect(within(gaji).getByText("+Rp 9.500.000")).toHaveClass("c-income");
    const saving = screen.getByRole("region", { name: "Berapa yang ditabung?" });
    expect(saving).toHaveTextContent("Rp 2.750.000· 25% dari uang masuk");
    expect(within(saving).getByRole("link", { name: /Pengeluaran berpilar Tabungan/ }).getAttribute("href")).toContain("kakeibo=saving");
    expect(within(saving).queryByRole("link", { name: /Pindah bersih/ })).toBeNull();
    expect(within(saving).getByText("Rp 1.250.000")).toHaveClass("c-invest");
  });

  it("lists pillars and categories in the table with whole-row links, changes and the total", async () => {
    mockApi(makeReport());
    renderPage();
    const table = await screen.findByRole("table", { name: "Pengeluaran Oktober 2026 per pilar dan kategori" });
    const rows = within(table).getAllByRole("row").map((row) => row.textContent ?? "");
    const order = ["Kebutuhan", "Belanja Dapur", "Transportasi", "Keinginan", "Jajan & Nongkrong"].map((name) => rows.findIndex((row) => row.startsWith(name)));
    expect(order.every((index, i) => index > 0 && (i === 0 || index > order[i - 1]))).toBe(true);
    expect(within(table).getByRole("columnheader", { name: "vs September" })).toBeInTheDocument();
    const dapur = within(table).getByRole("link", { name: "Belanja Dapur: Rp 1.438.000, lihat transaksi" });
    expect(dapur.getAttribute("href")).toContain("category=c-dapur");
    expect(dapur.getAttribute("href")).toContain("kakeibo=need");
    expect(dapur.closest("tr")).toHaveTextContent("Rp 1.438.00024%▲ naik Rp 1.438.000");
    expect(within(table).getByRole("row", { name: /Total pengeluaran/ })).toHaveTextContent("Rp 5.993.830100%");
    expect(within(table).getAllByRole("rowheader")).toHaveLength(8);
  });

  it("lists the top places of Pengeluaran in red, never a merchant of the Tabungan pillar", async () => {
    mockApi(makeReport());
    renderPage();
    const places = await screen.findByRole("region", { name: "Tempat belanja terbesar" });
    const links = within(places).getAllByRole("link");
    expect(links.map((link) => link.textContent)).toEqual(["1Swalayan Hemat−Rp 1.087.000", "2Leasing Motor Sentosa−Rp 850.000"]);
    expect(links[0].getAttribute("href")).toContain("q=Swalayan+Hemat");
    expect(within(links[0]).getByText("−Rp 1.087.000")).toHaveClass("c-expense");
    expect(places).not.toHaveTextContent("Reksa Contoh");
    const facts = screen.getByRole("region", { name: "Yang bisa diperbaiki" });
    expect(facts).toHaveTextContent("Tempat belanja terbesar: Swalayan HematRp 1.087.000");
    expect(facts).not.toHaveTextContent("Reksa Contoh");
  });

  it("folds the spending details, opens an unexpected item and closes it again", async () => {
    mockApi(makeReport());
    renderPage();
    const summary = (await screen.findByText("Rincian pengeluaran")).closest("summary")!;
    const details = summary.closest("details")!;
    expect(details.open).toBe(false);
    expect(screen.getByText(/Sudah dikurangi ganti uang/)).toHaveTextContent("Sudah dikurangi ganti uang Rp 40.000.");
    await userEvent.click(summary);
    expect(details.open).toBe(true);
    const repair = within(details).getByRole("button", { name: /Bengkel Motor Jaya/ });
    expect(within(repair).getByText("−Rp 425.000")).toHaveClass("c-expense");
    expect(within(details).getByRole("link", { name: "Lihat semua 12 transaksi tak terduga" }).getAttribute("href")).toContain("kakeibo=unexpected");
    expect(details).toHaveTextContent("bulanan · sudah ada rencana · nominal berubah");
    expect(details).toHaveTextContent("Rp 2.300.000 per bulan · Median 3 periode");
    await userEvent.click(repair);
    const dialog = await screen.findByRole("dialog", { name: "Transaksi t-repair" });
    await userEvent.click(within(dialog).getByRole("button", { name: "Tutup transaksi" }));
    expect(screen.queryByRole("dialog", { name: "Transaksi t-repair" })).toBeNull();
  });

  it("shows no budget figure without a budget and opens the shared budget dialog", async () => {
    mockApi(makeReport());
    renderPage();
    const budget = await screen.findByRole("region", { name: "Anggaran" });
    expect(budget).toHaveTextContent("Belum ada anggaran. Anggaran tidak wajib; laporan tetap lengkap tanpanya.");
    expect(within(budget).queryByText(/Rp/)).toBeNull();
    await userEvent.click(within(budget).getByRole("button", { name: "Atur anggaran" }));
    const dialog = await screen.findByRole("dialog", { name: "Atur anggaran" });
    const input = await within(dialog).findByLabelText("Makan Bareng");
    await userEvent.type(input, "500000");
    await userEvent.click(within(dialog).getByRole("button", { name: "Simpan anggaran" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/categories/c-dinner", { monthly_budget: 500_000 }));
  });

  it("reads a budget on the Pengeluaran basis with its amounts and what is over", async () => {
    mockApi(makeReport({ budget: overBudget }));
    renderPage();
    const budget = await screen.findByRole("region", { name: "Anggaran" });
    const row = within(budget).getByRole("link", { name: /Jajan/ });
    expect(row).toHaveTextContent("Rp 620.000 dari Rp 500.000lebih Rp 120.000");
    expect(within(row).getByText("lebih Rp 120.000")).toHaveClass("c-expense");
    expect(row.getAttribute("href")).toContain("category=c-jajan");
    expect(budget).toHaveTextContent("Total: Rp 5.993.830 dari Rp 6.000.000, sisa Rp 6.170.");
    expect(budget).not.toHaveTextContent("920.000");
  });

  it("draws one series of proportional bars with a legend and the same values as a table", async () => {
    expect([4_980_000, 5_240_000, 5_993_830, 2_463_990].map((value) => barHeight(value, 5_993_830))).toEqual([93, 98, BAR_AREA, 46]);
    mockApi(makeReport());
    renderPage();
    const history = await screen.findByRole("region", { name: "Pengeluaran per bulan" });
    const chart = within(history).getByRole("img");
    expect(chart).toHaveAccessibleName("Pengeluaran per bulan: Juli 2026 Rp 4.980.000, September 2026 Rp 5.240.000, Oktober 2026 Rp 5.993.830, November 2026 (berjalan) Rp 2.463.990");
    expect(history).toHaveTextContent("Nov · berjalan");
    expect(history).toHaveTextContent("Oktober 2026 (dilihat)");
    expect(history).toHaveTextContent("Berjalan");
    expect(history).toHaveTextContent("Bulan berjalan tidak dibandingkan sampai bulannya selesai.");
    await userEvent.click(within(history).getByRole("button", { name: "Lihat sebagai tabel" }));
    const table = within(history).getByRole("table");
    expect(within(table).getByRole("row", { name: /Oktober 2026 · dilihat/ })).toHaveTextContent("Rp 5.993.830");
    expect(within(table).getByRole("row", { name: /November 2026 · berjalan/ })).toBeInTheDocument();
  });

  it("lists the facts as whole-row links", async () => {
    mockApi(makeReport());
    renderPage();
    const facts = await screen.findByRole("region", { name: "Yang bisa diperbaiki" });
    const above = within(facts).getByRole("link", { name: /Belanja Dapur di atas rata-rata 3 bulan/ });
    expect(above).toHaveTextContent("+Rp 538.000");
    expect(above.getAttribute("href")).toContain("category=c-dapur");
    const recurring = within(facts).getByRole("link", { name: /3 tagihan dan langganan berulang/ });
    expect(recurring).toHaveAttribute("href", "/insights?tab=ringkasan");
    expect(recurring).toHaveTextContent("Rp 635.000/blnLihat daftar");
  });

  it("offers a first note without an empty form, saves it and moves focus to Ubah", async () => {
    mockApi(makeReport());
    renderPage();
    const improve = await screen.findByRole("region", { name: "Apa yang mau diperbaiki bulan depan?" });
    expect(within(improve).queryByRole("textbox")).toBeNull();
    expect(improve).not.toHaveTextContent("Catatan September");
    await userEvent.click(within(improve).getByRole("button", { name: "Tulis catatan untuk Oktober 2026" }));
    const field = within(improve).getByLabelText("Catatan untuk Oktober 2026");
    await waitFor(() => expect(field).toHaveFocus());
    expect(within(improve).getByRole("button", { name: "Simpan catatan" })).toBeDisabled();
    await userEvent.type(field, "Kurangi jajan kopi");
    expect(improve).toHaveTextContent("18/1.000");
    await userEvent.click(within(improve).getByRole("button", { name: "Simpan catatan" }));
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/reports/monthly/reflection", { basis: "month", start_date: "2026-10-01", note: "Kurangi jajan kopi" }));
    const edit = await within(improve).findByRole("button", { name: "Ubah" });
    await waitFor(() => expect(edit).toHaveFocus());
    expect(within(improve).queryByRole("textbox")).toBeNull();
    expect(improve).toHaveTextContent("Kurangi jajan kopi");
    expect(within(improve).getByRole("status")).toHaveTextContent("Catatan tersimpan.");
  });

  it("shows a saved note read-only, deletes it, and shows last period's note", async () => {
    mockApi(makeReport({
      reflection: { note: "Masak di rumah", start_date: "2026-10-01", end_date: "2026-10-31", updated_at: "2026-11-01T00:00:00Z" },
      previous_reflection: { note: "Kurangi jajan kopi", start_date: "2026-09-01", end_date: "2026-09-30", updated_at: "2026-10-01T00:00:00Z", label: "September 2026" },
    }));
    renderPage();
    const improve = await screen.findByRole("region", { name: "Apa yang mau diperbaiki bulan depan?" });
    expect(improve).toHaveTextContent("Catatan September 2026: Kurangi jajan kopi");
    expect(within(improve).queryByRole("textbox")).toBeNull();
    await userEvent.click(within(improve).getByRole("button", { name: "Ubah" }));
    expect(within(improve).getByRole("textbox")).toHaveValue("Masak di rumah");
    await userEvent.click(within(improve).getByRole("button", { name: "Hapus catatan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/reports/monthly/reflection?basis=month&start_date=2026-10-01"));
    const write = await within(improve).findByRole("button", { name: "Tulis catatan untuk Oktober 2026" });
    await waitFor(() => expect(write).toHaveFocus());
    expect(within(improve).getByRole("status")).toHaveTextContent("Catatan dihapus.");
  });

  it("says when a note was not saved and keeps the text", async () => {
    mockApi(makeReport());
    vi.mocked(api.put).mockRejectedValue(new Error(""));
    renderPage();
    const improve = await screen.findByRole("region", { name: "Apa yang mau diperbaiki bulan depan?" });
    await userEvent.click(within(improve).getByRole("button", { name: "Tulis catatan untuk Oktober 2026" }));
    await userEvent.type(within(improve).getByRole("textbox"), "Masak");
    await userEvent.click(within(improve).getByRole("button", { name: "Simpan catatan" }));
    expect(await within(improve).findByText("Catatan belum tersimpan. Coba lagi.")).toBeInTheDocument();
    expect(within(improve).getByRole("textbox")).toHaveValue("Masak");
  });

  it("shows only progress while the period runs: both shares with a bar, no arrow or verdict", async () => {
    const base = makeComparison();
    const running = makeComparison({
      running: true,
      period: { ...base.period, label: "November 2026", start_date: "2026-11-01", complete: false },
      totals: { spending: 1_450_000, inflow: 0, fresh_saving: 200_000, difference: -1_450_000 },
      previous: { ...base.previous!, label: "Oktober 2026", start_date: "2026-10-01", change: noChange },
      baseline: { ...base.baseline!, totals: { ...base.baseline!.totals, spending: 3_910_167 }, change: noChange },
      progress: {
        previous_label: "Oktober 2026", previous_spending: 4_802_000, spending: 1_450_000, share: 30, baseline_share: 37,
        pillars: [{ pillar: "need", amount: 1_000_000, previous: 3_375_500, share: 30 }],
      },
      pillars: base.pillars.map((pillar) => ({ ...pillar, change: null, baseline_change: null, categories: pillar.categories.map((item) => ({ ...item, change: null, baseline_change: null })) })),
      facts: [{ kind: "unexpected", label: "1 pengeluaran tak terduga", amount: 80_000, count: 1, ref: ref({ kakeibo_type: "unexpected" }) }],
    });
    mockApi(makeReport({ period: { ...period, label: "November 2026", complete: false, start_date: "2026-11-01" }, comparison: running }));
    const { container } = renderPage();
    const section = await hero();
    expect(within(section).getByRole("heading", { name: "Pengeluaran November 2026" })).toBeInTheDocument();
    expect(section).toHaveTextContent("30% dari total Oktober 2026 · 37% dari rata-rata 3 bulan");
    expect(section).toHaveTextContent("Periode berjalan · dibandingkan setelah selesai");
    expect(section.querySelector(".prog")).not.toBeNull();
    expect(screen.getByRole("region", { name: "Sejauh ini" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "dari Oktober" })).toBeInTheDocument();
    expect(screen.queryByText("Selisih")).toBeNull();
    const text = container.textContent ?? "";
    expect(text).not.toMatch(/[▲▼]/);
    expect(text).not.toMatch(/\b(naik|turun)\b/i);
  });

  it("says incomplete against a partial previous month and when the average appears", async () => {
    const base = makeComparison();
    mockApi(makeReport({ comparison: { ...base, previous: { ...base.previous!, partial: true, tracking_from: "2026-09-15", change: noChange }, baseline: null } }));
    renderPage();
    const section = await hero();
    expect(section).toHaveTextContent("September 2026 tercatat mulai 15 Sep · belum lengkap · Rata-rata muncul setelah 2 bulan lengkap");
    expect(screen.getByRole("row", { name: /^Kebutuhan/ })).toHaveTextContent("belum lengkap");
  });

  it("shows no comparison line without a previous whole period", async () => {
    mockApi(makeReport({ comparison: makeComparison({ previous: null, baseline: null }) }));
    renderPage();
    const section = await hero();
    expect(section.querySelector(".hero-sub")).toBeNull();
    expect(screen.queryByRole("columnheader", { name: /^vs/ })).toBeNull();
  });

  it("puts data quality, Selisih and the earlier reports in the context panel", async () => {
    mockApi(makeReport());
    renderPage();
    const panel = await screen.findByRole("complementary", { name: "Konteks laporan" });
    expect(panel).toHaveTextContent("2 menunggu di Tinjau · 2 belum diklasifikasi · 2 kemungkinan duplikat · 2 hari tanpa catatan");
    expect(within(panel).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
    expect(within(panel).getByRole("link", { name: "Periksa duplikat" }).getAttribute("href")).toContain("dup=1");
    const record = within(panel).getByRole("button", { name: "Catat transaksi" });
    expect(record).toHaveAccessibleDescription("Hari tanpa catatan: 13 Okt, 14 Okt");
    await userEvent.click(record);
    expect(openQuickAdd).toHaveBeenCalledWith("expense");
    expect(panel).toHaveTextContent("Selisih+Rp 5.074.570masuk dikurangi pengeluaran");
    const past = within(panel).getAllByRole("link").filter((link) => link.getAttribute("href")?.startsWith("/laporan"));
    expect(past.map((link) => link.textContent)).toEqual(["November 2026Berjalan", "Oktober 2026Lengkap", "September 2026Lengkap · ada catatan"]);
    expect(past[1]).toHaveAttribute("aria-current", "page");
    const replaceState = vi.spyOn(window.history, "replaceState");
    await userEvent.click(past[2]);
    expect(replaceState).toHaveBeenCalledWith(null, "", "/laporan?basis=month&start=2026-09-01");
    replaceState.mockRestore();
  });

  it("says the data is complete in one neutral sentence when nothing is open", async () => {
    mockApi(makeReport({ data_quality: cleanQuality }));
    renderPage();
    const panel = await screen.findByRole("complementary", { name: "Konteks laporan" });
    expect(panel).toHaveTextContent("Data lengkap. Semua transaksi periode ini sudah ditinjau dan diklasifikasi.");
    expect(within(panel).queryByRole("button")).toBeNull();
  });

  it("completes classification per category and for several rows, and returns focus", async () => {
    mockApi(makeReport());
    renderPage();
    const panel = await screen.findByRole("complementary", { name: "Konteks laporan" });
    const opener = within(panel).getByRole("button", { name: "Lengkapi klasifikasi" });
    await userEvent.click(opener);
    const dialog = await screen.findByRole("dialog", { name: "Lengkapi klasifikasi" });
    await userEvent.selectOptions(await within(dialog).findByLabelText("Pilar untuk Lain-lain"), "need");
    await userEvent.click(within(dialog).getByRole("button", { name: "Simpan pilar Lain-lain" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/classification/categories/c-misc", { kakeibo_type: "need" }));
    expect(await within(dialog).findByText("Lain-lain disimpan. 1 transaksi ikut terklasifikasi.")).toBeInTheDocument();
    await userEvent.click(within(dialog).getByRole("checkbox", { name: "Pilih semua" }));
    await userEvent.selectOptions(within(dialog).getByLabelText("Pilar untuk transaksi yang dipilih"), "unexpected");
    await userEvent.click(within(dialog).getByRole("button", { name: "Terapkan ke 2 transaksi" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/classification/transactions", { transaction_ids: ["t-misc", "t-none"], kakeibo_type: "unexpected" }));
    expect((await axe(dialog)).violations).toEqual([]);
    await userEvent.click(within(dialog).getByRole("button", { name: "Selesai" }));
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it("opens the classification and budget dialogs from their deep links", async () => {
    navigation.params = new URLSearchParams("lengkapi=1&atur=anggaran");
    mockApi(makeReport());
    renderPage();
    expect(await screen.findByRole("dialog", { name: "Lengkapi klasifikasi" })).toBeInTheDocument();
    expect(await screen.findByRole("dialog", { name: "Atur anggaran" })).toBeInTheDocument();
  });

  it("steps through reports in the URL and offers no cycle without salary cycles", async () => {
    navigation.params = new URLSearchParams("basis=month&start=2026-10-01");
    mockApi(makeReport());
    renderPage();
    const previous = await screen.findByRole("button", { name: "Bulan sebelumnya" });
    const replaceState = vi.spyOn(window.history, "replaceState");
    await userEvent.click(previous);
    expect(replaceState).toHaveBeenCalledWith(null, "", "/laporan?basis=month&start=2026-09-01");
    await userEvent.click(screen.getByRole("button", { name: "Bulan berikutnya" }));
    // The running month is reachable: without `start` the API would open the latest closed month.
    expect(replaceState).toHaveBeenLastCalledWith(null, "", "/laporan?basis=month&start=2026-11-01");
    expect(navigation.replace).not.toHaveBeenCalled();
    replaceState.mockRestore();
    expect(screen.getByRole("group", { name: "Dasar laporan" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Siklus gaji" })).toBeNull();
  });

  it("masks every amount, change and chart description while balances are hidden", async () => {
    ctx.hide = true;
    mockApi(makeReport({ budget: overBudget }));
    const { container } = renderPage();
    await hero();
    expect(container.textContent).not.toMatch(/\d{1,3}\.\d{3}/);
    expect(screen.getByRole("region", { name: "Pengeluaran per bulan" }).querySelector("[role=img]")?.getAttribute("aria-label")).not.toMatch(/\d\.\d{3}/);
    expect(container.textContent).toContain("25% dari uang masuk");
  });

  it("shows a busy skeleton while loading and a retry on failure", async () => {
    vi.mocked(api.get).mockImplementation(() => new Promise(() => {}));
    const first = renderPage();
    expect(screen.getByRole("status", { name: "Menyiapkan laporan…" })).toHaveAttribute("aria-busy", "true");
    first.unmount();
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/reports/monthly/periods")) return { basis: "month", cycle_available: false, periods: [] };
      throw new Error("");
    });
    renderPage();
    expect(await screen.findByRole("alert")).toHaveTextContent("Laporan belum bisa dimuat.");
    expect(screen.getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
  });

  it("explains an empty ledger without zero figures", async () => {
    mockApi({ empty: true, basis: "month", cycle_available: false });
    renderPage();
    expect(await screen.findByRole("heading", { name: "Belum ada laporan" })).toBeInTheDocument();
    expect(screen.queryByText(/Rp 0/)).toBeNull();
  });

  it("reads in plain English with the same figures", async () => {
    mockApi(makeReport());
    const english = renderPage("en");
    const section = await hero();
    expect(within(section).getByRole("heading", { name: "Spending for Oktober 2026" })).toBeInTheDocument();
    expect(section).toHaveTextContent("▲ up Rp 753.830 vs September · ▲ up Rp 1.050.497 vs the 3-month average");
    expect(screen.getByRole("heading", { level: 2, name: "How much came in?" })).toBeInTheDocument();
    expect(screen.getByRole("complementary", { name: "Report context" })).toHaveTextContent("Data quality");
    expect(english.container.querySelector("[lang=id]")).toBeNull();
    const englishFigures = (english.container.textContent ?? "").match(/Rp \d{1,3}(?:\.\d{3})*|\d+%/g);
    english.unmount();
    const indonesian = renderPage("id");
    await hero();
    expect((indonesian.container.textContent ?? "").match(/Rp \d{1,3}(?:\.\d{3})*|\d+%/g)).toEqual(englishFigures);
  });
});
