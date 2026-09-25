import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import AnalyticsPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ timeframe: "month", cycleOffset: 0, bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
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
