import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { ExplainProvider } from "@/components/workspace/explain";
import AnalyticsPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const navigation = vi.hoisted(() => ({ params: new URLSearchParams() }));
vi.mock("next/navigation", () => ({
  useSearchParams: () => navigation.params,
  usePathname: () => "/insights",
  useRouter: () => ({ replace: vi.fn(), push: vi.fn() }),
}));
const setTimeframe = vi.fn();
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    timeframe: "month", setTimeframe, cycleOffset: -1, bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`,
  }),
}));
vi.mock("@/hooks/useAnimatedCounter", () => ({ useAnimatedCounter: (value: number) => value }));
vi.mock("@/components/recurring/RecurringRulesModal", () => ({
  RecurringRulesModal: ({ open, expenseDraft }: { open: boolean; expenseDraft?: { name: string; amount: number } | null }) =>
    open ? <div role="dialog" aria-label="Rencana baru">{expenseDraft?.name} {expenseDraft?.amount}</div> : null,
}));

const expenseCategory = {
  id: "category-1",
  name: "Hobi",
  kind: "expense",
  icon: "film",
  color: "#f97316",
  is_primary: false,
  kakeibo_type: "want",
  budget: 500_000,
  monthly_budget: 500_000,
  spent: 100_000,
  variance: 400_000,
  percentage_used: 20,
  status: "ok",
};

function renderAnalytics() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(<QueryClientProvider client={queryClient}><ExplainProvider><AnalyticsPage /></ExplainProvider></QueryClientProvider>);
}

describe("category management form", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    navigation.params = new URLSearchParams();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return { category_variance: [expenseCategory] };
      if (path === "/categories?include_archived=true") return { categories: [expenseCategory] };
      const insight = insightResponse(path);
      if (insight) return insight;
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
  });

  it("creates an expense category with named icon, color, pillar and budget", async () => {
    const user = userEvent.setup();
    renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Tambah Kategori" }));

    await user.type(screen.getByRole("textbox", { name: "Nama Kategori" }), "Transport");
    await user.selectOptions(screen.getByRole("combobox", { name: "Pilar Kakeibo bawaan" }), "want");
    await user.click(screen.getByRole("button", { name: "Ikon Transportasi" }));
    await user.click(screen.getByRole("button", { name: "Warna Oranye" }));
    await user.type(screen.getByRole("textbox", { name: "Batas Anggaran Bulanan (IDR)" }), "500000");
    await user.click(screen.getByRole("button", { name: "Buat Kategori" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/categories", expect.objectContaining({
      name: "Transport",
      kind: "expense",
      kakeibo_type: "want",
      icon: "car",
      color: "#f97316",
      monthly_budget: 500_000,
    })));
  });

  it("keeps a category without a pillar unclassified when it is edited", async () => {
    const user = userEvent.setup();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return { category_variance: [{ ...expenseCategory, kakeibo_type: null }] };
      if (path === "/categories?include_archived=true") return { categories: [{ ...expenseCategory, kakeibo_type: null }] };
      throw new Error(`Unexpected GET ${path}`);
    });
    renderAnalytics();
    await user.click((await screen.findAllByRole("button", { name: "Edit kategori Hobi" }))[0]);
    expect(screen.getByRole("combobox", { name: "Pilar Kakeibo bawaan" })).toHaveValue("");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/categories/category-1", expect.objectContaining({ kakeibo_type: null })));
  });

  it("clears a pillar by sending an explicit null when Belum diklasifikasi is chosen", async () => {
    const user = userEvent.setup();
    renderAnalytics();
    await user.click((await screen.findAllByRole("button", { name: "Edit kategori Hobi" }))[0]);
    const pillar = screen.getByRole("combobox", { name: "Pilar Kakeibo bawaan" });
    expect(pillar).toHaveValue("want");
    await user.selectOptions(pillar, "Belum diklasifikasi");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledTimes(1));
    const [path, body] = vi.mocked(api.patch).mock.calls[0];
    expect(path).toBe("/categories/category-1");
    expect(body).toHaveProperty("kakeibo_type", null);
  });

  it("shows spending only, without budget columns, while no category has a budget", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) {
        return { category_variance: [{ ...expenseCategory, budget: null, monthly_budget: null, status: "unlimited", percentage_used: null, variance: null }] };
      }
      if (path === "/categories?include_archived=true") return { categories: [{ ...expenseCategory, monthly_budget: null }] };
      throw new Error(`Unexpected GET ${path}`);
    });
    renderAnalytics();
    expect(await screen.findByRole("heading", { name: "Pengeluaran per Kategori" })).toBeInTheDocument();
    expect(screen.queryByText("Tanpa Batas")).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Batas Anggaran" })).not.toBeInTheDocument();
    expect(screen.queryByText(/Atur batas/i)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Laporan Bulanan" })).toHaveAttribute("href", "/laporan?atur=anggaran");
  });

  it("hides expense-only inputs for an income category", async () => {
    const user = userEvent.setup();
    renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Tambah Kategori Pemasukan" }));

    expect(screen.queryByRole("combobox", { name: "Pilar Kakeibo bawaan" })).not.toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Batas Anggaran Bulanan (IDR)" })).not.toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Nama Kategori" }), "Bonus");
    await user.click(screen.getByRole("button", { name: "Buat Kategori" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/categories", expect.objectContaining({
      name: "Bonus",
      kind: "income",
      kakeibo_type: null,
      monthly_budget: null,
    })));
  });

  it("loads persisted attributes when editing an existing category", async () => {
    const user = userEvent.setup();
    renderAnalytics();
    await user.click((await screen.findAllByRole("button", { name: "Edit kategori Hobi" }))[0]);

    expect(screen.getByRole("textbox", { name: "Nama Kategori" })).toHaveValue("Hobi");
    expect(screen.getByRole("combobox", { name: "Pilar Kakeibo bawaan" })).toHaveValue("want");
    expect(screen.getByRole("button", { name: "Ikon Hiburan" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "Warna Oranye" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("textbox", { name: "Batas Anggaran Bulanan (IDR)" })).toHaveValue("500.000");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/categories/category-1", expect.objectContaining({
      kakeibo_type: "want",
      icon: "film",
      color: "#f97316",
      monthly_budget: 500_000,
    })));
  });

  it("requires confirmation before archiving a category", async () => {
    const user = userEvent.setup();
    renderAnalytics();
    await user.click((await screen.findAllByRole("button", { name: "Arsipkan kategori Hobi" }))[0]);
    expect(api.del).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Ya, lanjutkan" }));

    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/categories/category-1"));
  });
});

const FROM = "2026-10-01T00:00:00+07:00";
const TO = "2026-10-10T18:00:00+07:00";

const reportPeriods = {
  basis: "month", cycle_available: false,
  periods: [
    { kind: "month", basis: "month", label: "Oktober 2026", start: FROM, end: TO, start_date: "2026-10-01", end_date: "2026-10-10", days: 10, complete: false, tracking_started_inside: false, first_recorded_date: null, url: "" },
    { kind: "month", basis: "month", label: "September 2026", start: "2026-09-01T00:00:00+07:00", end: "2026-09-30T23:59:59+07:00", start_date: "2026-09-01", end_date: "2026-09-30", days: 30, complete: true, tracking_started_inside: false, first_recorded_date: null, url: "" },
  ],
};

const septemberComparison = (withBaseline = true) => ({
  empty: false, cycle_available: false,
  comparison: {
    basis: "month",
    period: { label: "September 2026", start: "2026-09-01T00:00:00+07:00", end: "2026-09-30T23:59:59+07:00", start_date: "2026-09-01", end_date: "2026-09-30", complete: true, partial: false, tracking_from: null },
    running: false,
    totals: { spending: 100_000, inflow: 5_000_000, fresh_saving: 0, difference: 4_900_000 },
    previous: null,
    baseline: withBaseline ? { period_count: 3, labels: ["Juni 2026", "Juli 2026", "Agustus 2026"], totals: { spending: 80_000, inflow: 5_000_000, fresh_saving: 0, difference: 4_920_000 }, change: { spending: 20_000, inflow: 0, fresh_saving: 0, difference: -20_000 } } : null,
    progress: null,
    pillars: [{
      pillar: "want", amount: 100_000, share: 100, previous: null, change: null, baseline: 80_000, baseline_change: withBaseline ? 20_000 : null,
      ref: { from: "2026-09-01T00:00:00+07:00", to: "2026-09-30T23:59:59+07:00", kakeibo_type: "want" }, categories: [],
    }],
    facts: [], history: [],
  },
});

const chargesData = {
  window: { start: "2026-04-01T00:00:00+07:00", end: TO, start_date: "2026-04-01", end_date: "2026-10-10", months: 6 },
  monthly_equivalent_total: 163_323, monthly_equivalent_without_plan: 54_990,
  charges: [
    { key: "musik contoh", merchant: "Musik Contoh 0", search_text: "Musik Contoh", cadence: "monthly", occurrences: 5,
      typical_amount: 54_990, last_amount: 60_000, previous_amount: 54_990, amount_changed: true,
      last_date: "2026-10-05", next_expected_date: "2026-11-05", monthly_equivalent: 54_990, account_name: "Kas Contoh",
      existing_plan: null,
      plan_draft: { name: "Musik Contoh 0", type: "expense", amount: 54_990, source_account_id: "a", category_id: null,
        schedule_type: "monthly_day", schedule_day: 5 } },
    { key: "kopi contoh", merchant: "Kopi Contoh", search_text: "Kopi Contoh", cadence: "weekly", occurrences: 6,
      typical_amount: 25_000, last_amount: 25_000, previous_amount: 25_000, amount_changed: false,
      last_date: "2026-10-09", next_expected_date: "2026-10-16", monthly_equivalent: 108_333, account_name: "Kas Contoh",
      existing_plan: { id: "rule-1", name: "Kopi mingguan" },
      plan_draft: { name: "Kopi Contoh", type: "expense", amount: 25_000, source_account_id: "a", category_id: null,
        schedule_type: "weekly", schedule_day: 5 } },
  ],
};

function insightResponse(path: string, withBaseline = true) {
  if (path.startsWith("/dashboard/period-comparison?")) throw new Error("Analisis must not ask for the same-day comparison");
  if (path.startsWith("/reports/monthly/periods?")) return reportPeriods;
  if (path.startsWith("/reports/monthly/comparison?")) return septemberComparison(withBaseline);
  if (path === "/dashboard/recurring-charges") return chargesData;
  if (path === "/dashboard/question-rate?weeks=8") return { weeks: [], has_data: false };
  return null;
}

describe("observed insights", () => {
  function mockInsights(withBaseline = true) {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return { timeframe: "month", timeframe_label: "Oktober 2026", category_variance: [] };
      if (path === "/categories?include_archived=true") return { categories: [] };
      const insight = insightResponse(path, withBaseline);
      if (insight) return insight;
      throw new Error(`Unexpected GET ${path}`);
    });
  }

  beforeEach(() => vi.clearAllMocks());

  it("compares the last completed month with its average, never the same elapsed days", async () => {
    mockInsights();
    const { container } = renderAnalytics();
    expect(await screen.findByRole("heading", { name: "September 2026 vs rata-rata 3 bulan" })).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith("/reports/monthly/comparison?basis=month&start_date=2026-09-01");
    expect(vi.mocked(api.get).mock.calls.some(([path]) => String(path).includes("period-comparison"))).toBe(false);
    const want = screen.getByRole("link", { name: "Keinginan" });
    expect(want.getAttribute("href")).toContain("kakeibo=want");
    expect(screen.queryByText(/hari pertama/)).not.toBeInTheDocument();
    expect(await screen.findByRole("region", { name: "Seberapa sering ditanya" })).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("says when the average appears while too few whole months exist", async () => {
    mockInsights(false);
    renderAnalytics();
    expect(await screen.findByText("Rata-rata muncul setelah 2 bulan lengkap")).toBeInTheDocument();
  });

  it("marks charges with a plan and turns another into a plan only after confirmation", async () => {
    mockInsights();
    const user = userEvent.setup();
    const { container } = renderAnalytics();
    const charges = await screen.findByRole("region", { name: "Tagihan & langganan terdeteksi" });
    expect(await within(charges).findByText("Sudah ada rencana: Kopi mingguan")).toBeInTheDocument();
    expect(within(charges).getAllByText(/berikutnya perkiraan/)).toHaveLength(2);
    expect(within(charges).getByText(/Nominal terakhir berubah/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(within(charges).getByRole("button", { name: "Jadikan rencana: Musik Contoh 0" }));
    expect(screen.getByRole("dialog", { name: "Rencana baru" })).toHaveTextContent("Musik Contoh 0 54990");
    expect(api.post).not.toHaveBeenCalled();
  });
});

describe("report drill-down", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) {
        return {
          timeframe: "month",
          timeframe_label: "September 2026",
          category_variance: [
            expenseCategory,
            { ...expenseCategory, id: "category-old", name: "Langganan Lama", is_archived: true, budget: null, spent: 30_000, status: "unlimited" },
            { ...expenseCategory, id: null, name: "Tanpa kategori", is_uncategorized: true, budget: null, spent: 70_000, status: "unlimited" },
          ],
          kakeibo: { need_spent: 0, want_spent: 100_000, culture_spent: 0, unexpected_spent: 0, saving_spent: 0 },
        };
      }
      if (path === "/categories?include_archived=true") return { categories: [expenseCategory] };
      const insight = insightResponse(path);
      if (insight) return insight;
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("shows observed cash flow instead of average and projection tiles", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return {
        timeframe_label: "Oktober 2026", category_variance: [],
        comparison: { total_inflow: 5_000_000, total_outflow: 300_000, net_cashflow: 4_700_000 },
      };
      if (path === "/categories?include_archived=true") return { categories: [] };
      const insight = insightResponse(path);
      if (insight) return insight;
      throw new Error(`Unexpected GET ${path}`);
    });
    renderAnalytics();
    expect(await screen.findByRole("region", { name: "Arus kas tercatat" })).toBeInTheDocument();
    expect(await screen.findByText("Rp 5.000.000")).toBeInTheDocument();
    expect(screen.queryByText(/Rata-rata 7 Hari|Rata-rata 30 Hari|Proyeksi Akhir Siklus/)).not.toBeInTheDocument();
  });

  it("links category figures, the Tanpa kategori bucket and archived spending to the same period", async () => {
    const { container } = renderAnalytics();
    // The label shows in the page header and in the period stepper of the top-bar controls.
    expect((await screen.findAllByText("September 2026")).length).toBeGreaterThanOrEqual(1);
    expect(api.get).toHaveBeenCalledWith("/dashboard/analytics?timeframe=month&cycle_offset=-1");
    const [hobby] = screen.getAllByRole("link", { name: "Rp 100.000, lihat transaksi Hobi" });
    expect(hobby).toHaveAttribute("href", "/ledger?period=month&offset=-1&category=category-1&type=expense");
    const [bucket] = screen.getAllByRole("link", { name: "Rp 70.000, lihat transaksi Tanpa kategori" });
    expect(bucket).toHaveAttribute("href", "/ledger?period=month&offset=-1&category=uncategorized&type=expense");
    expect(screen.getAllByText("Diarsipkan").length).toBeGreaterThan(0);
    // Only real active categories can be edited or archived.
    expect(screen.queryAllByRole("button", { name: "Edit kategori Tanpa kategori" })).toHaveLength(0);
    expect(screen.queryAllByRole("button", { name: "Arsipkan kategori Langganan Lama" })).toHaveLength(0);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("switches the report basis and explains Pengeluaran from the server's lines", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return {
        timeframe_label: "September 2026", category_variance: [],
        comparison: { total_inflow: 5_000_000, total_outflow: 100_000, net_cashflow: 4_900_000 },
        kakeibo: { total_allocated: 100_000 },
        explanations: {
          keluar: { total: 100_000, caveat: null, href: null, lines: [{ key: "want", label: "Keinginan", amount: 100_000, note: null, href: "/ledger?period=month&offset=-1&kakeibo=want" }] },
          belanja: { total: 100_000, caveat: null, href: null, lines: [{ key: "gross", label: "Belanja tercatat", amount: 100_000, note: null, href: null }] },
        },
      };
      if (path === "/categories?include_archived=true") return { categories: [] };
      const insight = insightResponse(path);
      if (insight) return insight;
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Siklus gaji" }));
    expect(setTimeframe).toHaveBeenCalledWith("cycle");
    expect(screen.getByRole("button", { name: "Bulan kalender" })).toHaveAttribute("aria-pressed", "true");
    await user.click(await screen.findByRole("button", { name: "Pengeluaran Rp 100.000: lihat rincian" }));
    const panel = await screen.findByRole("dialog", { name: /Pengeluaran periode ini/ });
    expect(within(panel).getByText("Belanja tercatat")).toBeInTheDocument();
  });
});
