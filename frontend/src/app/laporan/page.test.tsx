import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { MonthlyReport, ReportComparison } from "@/lib/monthlyReport";
import LaporanPage from "./page";

const navigation = vi.hoisted(() => ({ replace: vi.fn(), params: new URLSearchParams() }));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: navigation.replace }),
  useSearchParams: () => navigation.params,
  usePathname: () => "/laporan",
}));
vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const openQuickAdd = vi.fn();
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ openQuickAdd, bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const START = "2026-09-30T17:00:00+00:00";
const END = "2026-10-31T16:59:59.999999+00:00";
const period = {
  kind: "month", basis: "month" as const, label: "Oktober 2026", start: START, end: END, start_date: "2026-10-01",
  end_date: "2026-10-31", days: 31, complete: true, tracking_started_inside: false, first_recorded_date: null,
  url: "/laporan?basis=month&start=2026-10-01",
};
const previousPeriod = {
  ...period, label: "September 2026", start: "2026-08-31T17:00:00+00:00", end: "2026-09-30T16:59:59.999999+00:00",
  start_date: "2026-09-01", end_date: "2026-09-30", days: 30, tracking_started_inside: true, first_recorded_date: "2026-09-15",
};

const ref = (extra: Record<string, unknown> = {}) => ({ from: START, to: END, ...extra });

function makeComparison(overrides: Partial<ReportComparison> = {}): ReportComparison {
  const septemberChange = { spending: 1_583_500, inflow: 40_000, fresh_saving: 0, difference: -1_543_500 };
  return {
    basis: "month",
    period: { label: "Oktober 2026", start: START, end: END, start_date: "2026-10-01", end_date: "2026-10-31", complete: true, partial: false, tracking_from: null },
    running: false,
    totals: { spending: 4_802_000, inflow: 7_476_000, fresh_saving: 3_100_000, difference: 2_674_000 },
    previous: {
      label: "September 2026", start: previousPeriod.start, end: previousPeriod.end, start_date: "2026-09-01", end_date: "2026-09-30",
      complete: true, partial: false, tracking_from: null,
      totals: { spending: 3_218_500, inflow: 7_436_000, fresh_saving: 3_100_000, difference: 4_217_500 }, change: septemberChange,
    },
    baseline: {
      period_count: 3, labels: ["Juli 2026", "Agustus 2026", "September 2026"],
      totals: { spending: 3_579_500, inflow: 7_436_000, fresh_saving: 3_000_000, difference: 3_856_500 },
      change: { spending: 1_222_500, inflow: 40_000, fresh_saving: 100_000, difference: -1_182_500 },
    },
    progress: null,
    pillars: [
      {
        pillar: "need", amount: 3_375_500, share: 70, previous: 2_384_000, change: 991_500, baseline: 2_500_000, baseline_change: 875_500,
        ref: ref({ kakeibo_type: "need" }),
        categories: [
          { category_id: "c-harian", name: "Belanja Harian", amount: 1_452_500, share: 30, previous: 980_000, change: 472_500, baseline: 1_133_333, baseline_change: 319_167, ref: ref({ category_id: "c-harian", kakeibo_type: "need" }) },
          { category_id: "c-transport", name: "Transportasi", amount: 779_000, share: 16, previous: 620_000, change: 159_000, baseline: 700_000, baseline_change: 79_000, ref: ref({ category_id: "c-transport", kakeibo_type: "need" }) },
        ],
      },
      {
        pillar: "want", amount: 830_500, share: 17, previous: 468_500, change: 362_000, baseline: 600_000, baseline_change: 230_500,
        ref: ref({ kakeibo_type: "want" }),
        categories: [{ category_id: "c-hiburan", name: "Hiburan", amount: 830_500, share: 17, previous: 468_500, change: 362_000, baseline: 600_000, baseline_change: 230_500, ref: ref({ category_id: "c-hiburan", kakeibo_type: "want" }) }],
      },
    ],
    facts: [
      { kind: "category_above_average", label: "Belanja Harian di atas rata-rata 3 bulan", amount: 1_452_500, difference: 319_167, baseline: 1_133_333, ref: ref({ category_id: "c-harian" }) },
      { kind: "top_merchant", label: "Tempat belanja terbesar: Pasar Swalayan Contoh", amount: 1_356_000, ref: ref({ q: "pasar swalayan contoh" }) },
      { kind: "recurring", label: "3 tagihan dan langganan berulang", amount: 635_000, count: 3, ref: null },
    ],
    history: [
      { label: "Juli 2026", start: "a", end: "a", complete: true, partial: false, viewed: false, spending: 3_810_000, pillars: { need: 3_000_000, want: 810_000, culture: 0, unexpected: 0, unclassified: 0 } },
      { label: "Oktober 2026", start: START, end: END, complete: true, partial: false, viewed: true, spending: 4_802_000, pillars: { need: 3_375_500, want: 830_500, culture: 185_000, unexpected: 411_000, unclassified: 0 } },
      { label: "November 2026", start: "b", end: "b", complete: false, partial: false, viewed: false, spending: 1_450_000, pillars: { need: 1_000_000, want: 450_000, culture: 0, unexpected: 0, unclassified: 0 } },
    ],
    ...overrides,
  };
}

function makeReport(overrides: Partial<MonthlyReport> = {}): MonthlyReport {
  return {
    empty: false, basis: "month", cycle_available: false, period,
    flow: { inflow: 5_250_000, gross_outflow: 882_500, reimbursement_offset: 50_000, outflow: 832_500, net: 4_417_500 },
    income: { total: 5_250_000, sources: [{ category_id: "c-salary", name: "Gaji", amount: 5_000_000, count: 1 }, { category_id: null, name: "Tanpa kategori", amount: 250_000, count: 1 }] },
    savings: { fresh_saving: 40_000, saving_expenses: 40_000, net_saving_transfers: 0, surplus: 4_417_500 },
    spending: {
      total: 832_500,
      pillars: [
        { pillar: "need", amount: 412_500 }, { pillar: "want", amount: 150_000 }, { pillar: "culture", amount: 0 },
        { pillar: "unexpected", amount: 80_000 }, { pillar: "saving", amount: 40_000 }, { pillar: "unclassified", amount: 150_000 },
      ],
      top_categories: [
        { category_id: "c-groceries", name: "Belanja Dapur", amount: 410_000, is_uncategorized: false, kakeibo_type: "need" },
        { category_id: null, name: "Tanpa kategori", amount: 50_000, is_uncategorized: true, kakeibo_type: null },
      ],
      top_merchants: [{ name: "Pasar Contoh 12", search_text: "Pasar Contoh", amount: 300_000 }],
      unexpected: { amount: 80_000, count: 1, items: [{ id: "t-repair", date: "2026-10-08T05:00:00+00:00", amount: 80_000, merchant: "Bengkel Contoh", notes: null, category_name: "Servis" }] },
      recurring: {
        charges: [{ merchant: "Musik Contoh", search_text: "Musik Contoh", cadence: "monthly", typical_amount: 55_000, last_amount: 55_000, amount_changed: false, last_date: "2026-10-05", monthly_equivalent: 55_000, existing_plan: null, category_name: null }],
        monthly_equivalent_total: 55_000,
      },
      essential: { amount: 2_300_000, method: "median_need_periods", limited_history: true, period_count: 1, coverage_months: 3.2, target_months: 6, status: "critical", balance_basis: "all_liquid" },
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

function mockApi(report: MonthlyReport | { empty: true; basis: "month"; cycle_available: false }) {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path.startsWith("/reports/monthly/periods")) {
      return { basis: "month", cycle_available: false, periods: [{ ...period, complete: false, label: "November 2026", start_date: "2026-11-01" }, { ...period, has_reflection: false }, { ...previousPeriod, has_reflection: true }] };
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
      return { categories: [{ id: "c-dinner", name: "Makan Bareng", kind: "expense", monthly_budget: null, is_archived: false, is_excluded_from_budget: false }] };
    }
    throw new Error(`Unexpected GET ${path}`);
  });
}

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}><LaporanPage /></QueryClientProvider>);
}

describe("Laporan Bulanan page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    navigation.params = new URLSearchParams();
    vi.mocked(api.put).mockResolvedValue({ ok: true, reflection: { note: "Masak di rumah" } });
    vi.mocked(api.post).mockResolvedValue({ ok: true, corrections: 1, transaction_count: 2 });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("leads with whole-month figures, the pillar table and the facts, without a written lead", async () => {
    mockApi(makeReport());
    const { container } = renderPage();
    expect(await screen.findByRole("heading", { level: 2, name: "Laporan Oktober 2026" })).toBeInTheDocument();
    expect(screen.queryByTestId("report-lead")).not.toBeInTheDocument();
    const kpis = screen.getByRole("region", { name: "Ringkasan Oktober 2026" });
    expect(within(kpis).getByText("Rp 4.802.000")).toBeInTheDocument();
    expect(kpis).toHaveTextContent("▲ Rp 1.583.500vs Sep");
    expect(kpis).toHaveTextContent("▲ Rp 1.222.500vs rata-rata 3 bulan");
    expect(kpis).toHaveTextContent("masuk dikurangi pengeluaran");

    const table = screen.getByRole("table", { name: /Pengeluaran Oktober 2026 per pilar/ });
    const rows = within(table).getAllByRole("row").map((row) => row.textContent ?? "");
    const order = ["Kebutuhan", "Belanja Harian", "Transportasi", "Keinginan", "Hiburan"].map((name) => rows.findIndex((row) => row.startsWith(name)));
    expect(order.every((index, i) => index > 0 && (i === 0 || index > order[i - 1]))).toBe(true);
    const harian = within(table).getByRole("link", { name: "Rp 1.452.500, lihat transaksi Belanja Harian" });
    expect(harian.getAttribute("href")).toContain("category=c-harian");
    expect(harian.getAttribute("href")).toContain("kakeibo=need");

    const facts = screen.getByRole("region", { name: "Yang bisa diperbaiki" });
    expect(facts).toHaveTextContent("Belanja Harian di atas rata-rata 3 bulan+Rp 319.167");
    expect(within(facts).getByRole("link", { name: "Lihat transaksi: Belanja Harian di atas rata-rata 3 bulan" }).getAttribute("href")).toContain("category=c-harian");
    expect(facts).toHaveTextContent("Rp 635.000/bln");

    const history = screen.getByRole("region", { name: "Pengeluaran per bulan" });
    expect(history).toHaveTextContent("November 2026 (berjalan): Rp 1.450.000");
    expect(history).toHaveTextContent("Bulan berjalan tidak dibandingkan sampai bulannya selesai.");
    expect(screen.queryByRole("region", { name: "Apa yang mau diperbaiki bulan depan?" })).not.toBeInTheDocument();
    expect(screen.queryByText(/surplus arus kas/)).not.toBeInTheDocument();
    expect((await axe(container)).violations).toEqual([]);
  });

  it("shows only progress for the running month, with no arrow or verdict", async () => {
    const running = makeComparison({
      running: true,
      period: { label: "November 2026", start: "s", end: "e", start_date: "2026-11-01", end_date: "2026-11-04", complete: false, partial: false, tracking_from: null },
      totals: { spending: 1_450_000, inflow: 0, fresh_saving: 200_000, difference: -1_450_000 },
      previous: { ...makeComparison().previous!, change: { spending: null, inflow: null, fresh_saving: null, difference: null } },
      baseline: { ...makeComparison().baseline!, change: { spending: null, inflow: null, fresh_saving: null, difference: null } },
      progress: { previous_label: "Oktober 2026", previous_spending: 4_802_000, spending: 1_450_000, share: 30, pillars: [{ pillar: "need", amount: 1_000_000, previous: 3_375_500, share: 30 }] },
      pillars: makeComparison().pillars.map((pillar) => ({ ...pillar, change: null, baseline_change: null, categories: pillar.categories.map((item) => ({ ...item, change: null, baseline_change: null })) })),
      facts: [{ kind: "top_merchant", label: "Tempat belanja terbesar: Pasar Swalayan Contoh", amount: 500_000, ref: ref({ q: "pasar" }) }],
    });
    mockApi(makeReport({ period: { ...period, label: "November 2026", complete: false, start_date: "2026-11-01" }, comparison: running }));
    renderPage();
    const kpis = await screen.findByRole("region", { name: "Ringkasan November 2026" });
    expect(kpis).toHaveTextContent("Pengeluaran sejauh ini");
    expect(kpis).toHaveTextContent("30% dari total Oktober 2026");
    expect(screen.getByRole("region", { name: "Sejauh ini" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Yang bisa diperbaiki" })).not.toBeInTheDocument();
    expect(screen.getByText(/November 2026 masih berjalan/)).toBeInTheDocument();
    const main = document.body.textContent ?? "";
    expect(main).not.toMatch(/[▲▼]/);
    expect(main).not.toMatch(/\b(naik|turun)\b/i);
  });

  it("says belum lengkap against a partial month and when the average appears", async () => {
    const base = makeComparison();
    mockApi(makeReport({ comparison: { ...base, previous: { ...base.previous!, partial: true, tracking_from: "2026-09-15", change: { spending: null, inflow: null, fresh_saving: null, difference: null } }, baseline: null } }));
    renderPage();
    const kpis = await screen.findByRole("region", { name: "Ringkasan Oktober 2026" });
    expect(kpis).toHaveTextContent("belum lengkap vs Sep");
    expect(kpis).toHaveTextContent("Rata-rata muncul setelah 2 bulan lengkap");
    expect(kpis.textContent).not.toMatch(/Rp 0\b/);
  });

  it("shows data quality as one line with its fixes", async () => {
    mockApi(makeReport());
    renderPage();
    const line = (await screen.findByText(/Kualitas data:/)).closest("div")!;
    expect(line).toHaveTextContent("2 menunggu di Tinjau · 2 belum diklasifikasi · 2 kemungkinan duplikat · 2 hari tanpa catatan");
    expect(within(line).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
    expect(within(line).getByRole("link", { name: "Periksa duplikat" }).getAttribute("href")).toContain("dup=1");
    await userEvent.click(within(line).getByRole("button", { name: "Catat transaksi" }));
    expect(openQuickAdd).toHaveBeenCalledWith("expense");
  });

  it("says the data is complete in one green line when nothing is open", async () => {
    mockApi(makeReport({ data_quality: { pending_review: 0, needs_attention: 0, unreviewed: 0, unclassified: { count: 0, amount: 0 }, possible_duplicates: 0, days_without_records: { count: 0, dates: [] } } }));
    renderPage();
    expect(await screen.findByText(/Data lengkap\./)).toBeInTheDocument();
  });

  it("hides budget figures until one exists and offers Atur anggaran", async () => {
    mockApi(makeReport());
    renderPage();
    const budget = await screen.findByRole("region", { name: "Anggaran" });
    expect(within(budget).queryByText(/Sisa|Lebih/)).not.toBeInTheDocument();
    await userEvent.click(within(budget).getByRole("button", { name: "Atur anggaran" }));
    const dialog = await screen.findByRole("dialog", { name: "Atur anggaran" });
    const input = await within(dialog).findByLabelText("Makan Bareng");
    await userEvent.type(input, "500000");
    expect(input).toHaveValue("500.000");
    expect((await axe(dialog)).violations).toEqual([]);
    await userEvent.click(within(dialog).getByRole("button", { name: "Simpan anggaran" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/categories/c-dinner", { monthly_budget: 500_000 }));
  });

  it("shows budget against actual once a budget is set", async () => {
    mockApi(makeReport({
      budget: { period_basis: "month", categories: [{ category_id: "c-dinner", name: "Makan Bareng", budget: 100_000, spent: 150_000, remaining: -50_000 }], total_budget: 100_000, total_spent: 150_000, overall: null },
    }));
    renderPage();
    const budget = await screen.findByRole("region", { name: "Anggaran dan realisasi" });
    expect(within(budget).getByText("Lebih Rp 50.000")).toBeInTheDocument();
  });

  it("completes classification per category and for several rows at once", async () => {
    mockApi(makeReport());
    renderPage();
    const line = (await screen.findByText(/Kualitas data:/)).closest("div")!;
    await userEvent.click(within(line).getByRole("button", { name: "Lengkapi klasifikasi" }));
    const dialog = await screen.findByRole("dialog", { name: "Lengkapi klasifikasi" });
    await userEvent.selectOptions(await within(dialog).findByLabelText("Pilar untuk Lain-lain"), "need");
    await userEvent.click(within(dialog).getByRole("button", { name: "Simpan pilar Lain-lain" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/classification/categories/c-misc", { kakeibo_type: "need" }));

    await userEvent.click(within(dialog).getByRole("checkbox", { name: "Pilih semua" }));
    await userEvent.selectOptions(within(dialog).getByLabelText("Pilar untuk transaksi yang dipilih"), "unexpected");
    await userEvent.click(within(dialog).getByRole("button", { name: "Terapkan ke 2 transaksi" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/classification/transactions", { transaction_ids: ["t-misc", "t-none"], kakeibo_type: "unexpected" }));
    expect((await axe(dialog)).violations).toEqual([]);
  });

  it("hides the reflection without a note and shows a saved note read-only with Ubah", async () => {
    mockApi(makeReport({ reflection: { note: "Masak di rumah", start_date: "2026-10-01", end_date: "2026-10-31", updated_at: "2026-11-01T00:00:00Z" } }));
    renderPage();
    const block = await screen.findByRole("region", { name: "Apa yang mau diperbaiki bulan depan?" });
    expect(within(block).getByText("Masak di rumah")).toBeInTheDocument();
    expect(within(block).queryByRole("textbox")).not.toBeInTheDocument();
    await userEvent.click(within(block).getByRole("button", { name: "Ubah" }));
    const field = within(block).getByLabelText("Catatan untuk Oktober 2026");
    await userEvent.clear(field);
    await userEvent.type(field, "Masak tiga kali seminggu");
    await userEvent.click(within(block).getByRole("button", { name: "Simpan catatan" }));
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/reports/monthly/reflection", { basis: "month", start_date: "2026-10-01", note: "Masak tiga kali seminggu" }));
  });

  it("switches periods through the URL and keeps the cycle switch unavailable without salary cycles", async () => {
    mockApi(makeReport());
    renderPage();
    const previous = await screen.findByRole("button", { name: "Bulan sebelumnya" });
    const replaceState = vi.spyOn(window.history, "replaceState");
    await userEvent.click(previous);
    // The history API, because Next.js commits no same-path navigation from a URL with a query (D13).
    expect(replaceState).toHaveBeenCalledWith(null, "", "/laporan?basis=month&start=2026-09-01");
    await userEvent.click(screen.getByRole("button", { name: "Bulan berikutnya" }));
    expect(replaceState).toHaveBeenLastCalledWith(null, "", "/laporan?basis=month");
    expect(navigation.replace).not.toHaveBeenCalled();
    replaceState.mockRestore();
    // Without salary cycles the basis switch offers only the calendar month.
    expect(screen.queryByRole("button", { name: "Siklus gaji" })).not.toBeInTheDocument();
    const past = screen.getByRole("region", { name: "Laporan sebelumnya" });
    expect(within(past).getByRole("link", { name: /September 2026/ })).toHaveAttribute("href", "/laporan?basis=month&start=2026-09-01");
    expect(within(past).getByRole("link", { name: /Oktober 2026/ })).toHaveAttribute("aria-current", "page");
  });

  it("lists the facts right after the figures in one column, in reading order", async () => {
    class NarrowObserver {
      constructor(private readonly callback: ResizeObserverCallback) {}
      observe() { this.callback([{ contentRect: { width: 800 } } as ResizeObserverEntry], this as unknown as ResizeObserver); }
      unobserve() {}
      disconnect() {}
    }
    vi.stubGlobal("ResizeObserver", NarrowObserver);
    mockApi(makeReport());
    renderPage();
    const facts = await screen.findByRole("region", { name: "Yang bisa diperbaiki" });
    const table = screen.getByRole("region", { name: "Pengeluaran per pilar dan kategori" });
    expect(facts.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    vi.unstubAllGlobals();
  });

  it("explains an empty ledger without zero figures", async () => {
    mockApi({ empty: true, basis: "month", cycle_available: false });
    renderPage();
    expect(await screen.findByRole("heading", { name: "Belum ada laporan" })).toBeInTheDocument();
    expect(screen.queryByText("Rp 0")).not.toBeInTheDocument();
  });
});
