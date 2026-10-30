import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import AnalyticsPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const setTimeframe = vi.fn();
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    timeframe: "month", setTimeframe, cycleOffset: -1, bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`,
  }),
}));
vi.mock("@/components/dashboard/BurnCadenceChart", () => ({ BurnCadenceChart: () => null }));
vi.mock("@/components/dashboard/SpendingHeatmap", () => ({ SpendingHeatmap: () => null }));
vi.mock("@/components/dashboard/NarrativeInsightCard", () => ({ NarrativeInsightCard: () => null }));
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
  return render(<QueryClientProvider client={queryClient}><AnalyticsPage /></QueryClientProvider>);
}

describe("category management form", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return { category_variance: [expenseCategory] };
      if (path === "/categories?include_archived=true") return { categories: [expenseCategory] };
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

const comparisonData = (previousHasData = true) => ({
  label: "Oktober 2026", compared_days: 10, current_truncated: false,
  current: { start: FROM, end: TO, start_date: "2026-10-01", end_date: "2026-10-10", inflow: 5_000_000, outflow: 290_000, net: 4_710_000 },
  previous: { start: "2026-09-01T00:00:00+07:00", end: "2026-09-10T18:00:00+07:00", start_date: "2026-09-01", end_date: "2026-09-10",
    inflow: previousHasData ? 5_000_000 : 0, outflow: previousHasData ? 100_000 : 0, net: previousHasData ? 4_900_000 : 0 },
  previous_label: "September 2026", previous_has_data: previousHasData,
  pillars: ["need", "want", "culture", "unexpected", "saving"].map((pillar) => ({
    pillar, current: pillar === "need" ? 250_000 : pillar === "want" ? 40_000 : 0,
    previous: previousHasData ? (pillar === "need" ? 100_000 : 0) : null,
    change: previousHasData ? (pillar === "need" ? 150_000 : pillar === "want" ? 40_000 : 0) : null,
  })),
  categories: {
    increases: previousHasData ? [{ key: "category-1", id: "category-1", name: "Belanja Contoh", current: 250_000, previous: 100_000, change: 150_000 }] : [],
    decreases: [],
  },
  merchants: {
    increases: previousHasData ? [{ key: "pasar contoh", id: null, name: "Pasar Contoh 12", search_text: "Pasar Contoh", current: 250_000, previous: 100_000, change: 150_000 }] : [],
    decreases: [],
  },
});

const historyData = {
  cycle_status: "not_configured",
  periods: [
    ...["April", "Mei", "Juni", "Juli", "Agustus", "September"].map((month, index) => ({
      kind: "month", label: `${month} 2026`, start: `2026-0${index + 4}-01T00:00:00+07:00`, end: `2026-0${index + 4}-28T23:59:59+07:00`,
      start_date: `2026-0${index + 4}-01`, end_date: `2026-0${index + 4}-28`, in_progress: false,
      inflow: 5_000_000, outflow: 3_000_000 + index, net: 2_000_000 - index,
    })),
    { kind: "month", label: "Oktober 2026", start: FROM, end: TO, start_date: "2026-10-01", end_date: "2026-10-10",
      in_progress: true, inflow: 0, outflow: 290_000, net: -290_000 },
  ],
};

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

function insightResponse(path: string, previousHasData = true) {
  if (path.startsWith("/dashboard/period-comparison?")) return comparisonData(previousHasData);
  if (path.startsWith("/dashboard/cash-flow-history?")) return historyData;
  if (path === "/dashboard/recurring-charges") return chargesData;
  return null;
}

describe("observed insights", () => {
  function mockInsights(previousHasData = true) {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path.startsWith("/dashboard/analytics?")) return { timeframe: "month", timeframe_label: "Oktober 2026", category_variance: [] };
      if (path === "/categories?include_archived=true") return { categories: [] };
      const insight = insightResponse(path, previousHasData);
      if (insight) return insight;
      throw new Error(`Unexpected GET ${path}`);
    });
  }

  beforeEach(() => vi.clearAllMocks());

  it("compares the same elapsed days and links each change to its exact window", async () => {
    mockInsights();
    const user = userEvent.setup();
    const { container } = renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Bandingkan & Kakeibo" }));
    expect(await screen.findByText(/sama-sama 10 hari pertama/)).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith("/dashboard/period-comparison?timeframe=month&cycle_offset=-1");
    const window = `from=${encodeURIComponent(FROM)}&to=${encodeURIComponent(TO)}`;
    expect(screen.getByRole("link", { name: "Belanja Contoh" })).toHaveAttribute("href", `/ledger?category=category-1&type=expense&${window}`);
    expect(screen.getByRole("link", { name: "Pasar Contoh 12" })).toHaveAttribute("href", `/ledger?type=expense&${window}&q=Pasar+Contoh`);
    const need = screen.getAllByRole("link", { name: "Kebutuhan" }).find((link) => link.getAttribute("href")?.includes("from="));
    expect(need).toHaveAttribute("href", `/ledger?kakeibo=need&type=expense&${window}`);
    expect(within(screen.getByRole("region", { name: "Periode ini vs periode lalu" })).queryByText(/%/)).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("says so when the previous period has no data", async () => {
    mockInsights(false);
    const user = userEvent.setup();
    renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Bandingkan & Kakeibo" }));
    expect(await screen.findByText(/Periode lalu belum punya transaksi tercatat/)).toBeInTheDocument();
    expect(screen.queryByText("Kategori yang berubah")).not.toBeInTheDocument();
  });

  it("lists cash flow per period with the current one in progress", async () => {
    mockInsights();
    const user = userEvent.setup();
    renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Bandingkan & Kakeibo" }));
    const history = await screen.findByRole("region", { name: "Arus kas per periode" });
    expect(within(history).getAllByRole("listitem")).toHaveLength(7);
    expect(within(history).getByText("berjalan")).toBeInTheDocument();
    expect(within(history).getByRole("link", { name: "Oktober 2026" })).toHaveAttribute(
      "href", `/ledger?from=${encodeURIComponent(FROM)}&to=${encodeURIComponent(TO)}`,
    );
  });

  it("marks charges with a plan and turns another into a plan only after confirmation", async () => {
    mockInsights();
    const user = userEvent.setup();
    const { container } = renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Bandingkan & Kakeibo" }));
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
      throw new Error(`Unexpected GET ${path}`);
    });
    renderAnalytics();
    expect(await screen.findByRole("region", { name: "Arus kas tercatat" })).toBeInTheDocument();
    expect(screen.getByText("Rp 5.000.000")).toBeInTheDocument();
    expect(screen.queryByText(/Rata-rata 7 Hari|Rata-rata 30 Hari|Proyeksi Akhir Siklus/)).not.toBeInTheDocument();
  });

  it("links category figures, the Tanpa kategori bucket and archived spending to the same period", async () => {
    const { container } = renderAnalytics();
    expect(await screen.findByText("September 2026")).toBeInTheDocument();
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

  it("switches the report basis and links Kakeibo pillars", async () => {
    const user = userEvent.setup();
    renderAnalytics();
    await user.click(await screen.findByRole("button", { name: "Siklus gajian" }));
    expect(setTimeframe).toHaveBeenCalledWith("cycle");
    expect(screen.getByRole("button", { name: "Bulan kalender" })).toHaveAttribute("aria-pressed", "true");
    await user.click(screen.getByRole("button", { name: "Bandingkan & Kakeibo" }));
    expect(screen.getByRole("link", { name: "Keinginan Rp 100.000, lihat transaksinya" })).toHaveAttribute(
      "href", "/ledger?period=month&offset=-1&kakeibo=want&type=expense",
    );
  });
});
