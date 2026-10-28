import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
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
    await user.click(screen.getByRole("checkbox", { name: "Hitung sebagai pengeluaran utama" }));
    await user.click(screen.getByRole("button", { name: "Ikon Transportasi" }));
    await user.click(screen.getByRole("button", { name: "Warna Oranye" }));
    await user.type(screen.getByRole("textbox", { name: "Batas Anggaran Bulanan (IDR)" }), "500000");
    await user.click(screen.getByRole("button", { name: "Buat Kategori" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/categories", expect.objectContaining({
      name: "Transport",
      kind: "expense",
      kakeibo_type: "want",
      is_primary: false,
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
      throw new Error(`Unexpected GET ${path}`);
    });
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
    expect(screen.getByRole("link", { name: "Rp 100.000, lihat transaksi Keinginan" })).toHaveAttribute(
      "href", "/ledger?period=month&offset=-1&kakeibo=want&type=expense",
    );
  });
});
