import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { MonthlyReport } from "@/lib/monthlyReport";
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
    comparison: {
      previous: {
        compared_days: 30, current_truncated: true,
        current: { start: START, end: "2026-10-30T16:59:59+00:00", inflow: 5_250_000, outflow: 800_000, net: 4_450_000 },
        previous: { start: previousPeriod.start, end: previousPeriod.end, inflow: 0, outflow: 100_000, net: -100_000 },
        previous_label: "September 2026", previous_has_data: true, pillars: [],
      },
      previous_partial: true, previous_period: previousPeriod,
      median: { available: false, period_count: 0, required: 3 },
    },
    data_quality: {
      pending_review: 2, needs_attention: 1, unreviewed: 1, unclassified: { count: 2, amount: 150_000 },
      possible_duplicates: 2, days_without_records: { count: 2, dates: ["2026-10-13", "2026-10-14"] },
    },
    budget: null,
    reflection: null,
    previous_reflection: { note: "Kurangi jajan kopi", start_date: "2026-09-01", end_date: "2026-09-30", updated_at: "2026-10-01T00:00:00Z", label: "September 2026" },
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

  it("answers the four questions in order with drill-down links and no axe violations", async () => {
    mockApi(makeReport());
    const { container } = renderPage();
    expect(await screen.findByRole("heading", { level: 1, name: "Oktober 2026" })).toBeInTheDocument();
    const questions = screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent);
    const order = ["Berapa uang masuk?", "Berapa yang ditabung?", "Berapa yang dibelanjakan?", "Apa yang mau diperbaiki bulan depan?"]
      .map((question) => questions.findIndex((text) => text?.includes(question)));
    expect(order.every((index, i) => index >= 0 && (i === 0 || index > order[i - 1]))).toBe(true);

    const unclassified = screen.getByRole("link", { name: "Rp 150.000, lihat transaksi Belum diklasifikasi" });
    expect(unclassified.getAttribute("href")).toBe(`/ledger?kakeibo=unclassified&type=expense&from=${encodeURIComponent(START)}&to=${encodeURIComponent(END)}`);
    expect(screen.getByRole("link", { name: "Rp 832.500, lihat semua uang keluar" }).getAttribute("href")).toContain("type=expense");
    expect(screen.getByRole("link", { name: "Rp 250.000, lihat uang masuk Tanpa kategori" }).getAttribute("href")).toContain("category=uncategorized");
    expect(screen.getByText(/surplus arus kas, bukan bukti uang sudah disisihkan/)).toBeInTheDocument();
    expect(screen.getByText("Kurangi jajan kopi")).toBeInTheDocument();
    expect(screen.getByText(/September 2026 baru tercatat sejak 15 Sep/)).toBeInTheDocument();
    expect(screen.queryByText(/%.*(naik|turun)/)).not.toBeInTheDocument();
    expect((await axe(container)).violations).toEqual([]);
  });

  it("links every data-quality count to its fix", async () => {
    mockApi(makeReport());
    renderPage();
    const box = await screen.findByRole("region", { name: "Kualitas data" });
    expect(within(box).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
    expect(within(box).getByRole("link", { name: "Periksa di Transaksi" }).getAttribute("href")).toContain("dup=1");
    await userEvent.click(within(box).getByRole("button", { name: "Catat transaksi" }));
    expect(openQuickAdd).toHaveBeenCalledWith("expense");
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
    const box = await screen.findByRole("region", { name: "Kualitas data" });
    await userEvent.click(within(box).getByRole("button", { name: "Lengkapi klasifikasi" }));
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

  it("saves the reflection note for the period", async () => {
    mockApi(makeReport());
    renderPage();
    const field = await screen.findByLabelText("Catatan untuk Oktober 2026");
    await userEvent.type(field, "Masak di rumah");
    await userEvent.click(screen.getByRole("button", { name: "Simpan catatan" }));
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/reports/monthly/reflection", { basis: "month", start_date: "2026-10-01", note: "Masak di rumah" }));
    expect(await screen.findByText("Catatan tersimpan.")).toBeInTheDocument();
  });

  it("switches periods through the URL and keeps the cycle switch unavailable without salary cycles", async () => {
    mockApi(makeReport());
    renderPage();
    const select = await screen.findByLabelText("Laporan lain");
    const replaceState = vi.spyOn(window.history, "replaceState");
    await userEvent.selectOptions(select, "2026-09-01");
    // The history API, because Next.js commits no same-path navigation from a URL with a query (D13).
    expect(replaceState).toHaveBeenCalledWith(null, "", "/laporan?basis=month&start=2026-09-01");
    expect(navigation.replace).not.toHaveBeenCalled();
    replaceState.mockRestore();
    expect(screen.getByRole("button", { name: "Siklus gaji" })).toBeDisabled();
  });

  it("explains an empty ledger without zero figures", async () => {
    mockApi({ empty: true, basis: "month", cycle_available: false });
    renderPage();
    expect(await screen.findByRole("heading", { name: "Belum ada laporan" })).toBeInTheDocument();
    expect(screen.queryByText("Rp 0")).not.toBeInTheDocument();
  });
});
