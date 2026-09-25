import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { RecurringRulesModal } from "./RecurringRulesModal";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
}));

const weeklyExpense = {
  id: "rule-1",
  name: "Belanja mingguan",
  type: "expense",
  amount: 50_000,
  source_account_id: "account-1",
  source_account_name: "BCA",
  target_account_id: null,
  category_id: "category-expense",
  category_name: "Makan",
  obligation_id: null,
  schedule_type: "weekly",
  schedule_day: 1,
  is_payroll_allocation: false,
  auto_post: false,
  is_active: true,
  next_due_date: "2026-09-28",
  notes: null,
};

function renderRules() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <RecurringRulesModal open onClose={() => {}} />
    </QueryClientProvider>,
  );
}

describe("RecurringRulesModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/recurring") return { ok: true, rules: [weeklyExpense] };
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "account-2", name: "Jago", type: "bank", balance: 25_000 },
        { id: "position-1", name: "BBCA", type: "investment", balance: 50_000 },
      ] };
      if (path === "/categories") return { categories: [
        { id: "category-expense", name: "Makan", kind: "expense" },
        { id: "category-income", name: "Gaji", kind: "income" },
      ] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("creates a weekly liquid transfer with payroll inclusion off by default", async () => {
    const user = userEvent.setup();
    renderRules();
    await user.click(screen.getByRole("button", { name: "+ Tambah Aturan" }));

    const source = screen.getByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(within(source).getByRole("option", { name: "BCA" })).toBeInTheDocument());
    await user.selectOptions(source, "account-1");
    const target = screen.getByRole("combobox", { name: "Ke Rekening / Kantong Tujuan" });
    await user.selectOptions(target, "account-2");
    expect(screen.queryByRole("option", { name: "BBCA" })).not.toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Masukkan ke alokasi gaji" })).not.toBeChecked();
    await user.type(screen.getByRole("textbox", { name: "Nama Aturan / Transaksi" }), "Pindah ke Jago");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "10000");
    await user.selectOptions(screen.getByRole("combobox", { name: "Jadwal Pengulangan" }), "weekly");
    await user.selectOptions(screen.getByRole("combobox", { name: "Hari pengulangan" }), "5");
    await user.click(screen.getByRole("button", { name: "Simpan Aturan" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring", expect.objectContaining({
      type: "transfer",
      target_account_id: "account-2",
      category_id: null,
      schedule_type: "weekly",
      schedule_day: 5,
      is_payroll_allocation: false,
    })));
  });

  it("edits the weekday of an existing weekly expense", async () => {
    const user = userEvent.setup();
    renderRules();
    await user.click(await screen.findByRole("button", { name: "Ubah aturan Belanja mingguan" }));

    expect(screen.getByRole("combobox", { name: "Hari pengulangan" })).toHaveValue("1");
    await user.selectOptions(screen.getByRole("combobox", { name: "Hari pengulangan" }), "5");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/recurring/rule-1", expect.objectContaining({
      category_id: "category-expense",
      schedule_type: "weekly",
      schedule_day: 5,
    })));
  });

  it("clears an expense category before saving a switched transfer", async () => {
    const user = userEvent.setup();
    renderRules();
    await user.click(await screen.findByRole("button", { name: "Ubah aturan Belanja mingguan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Jenis Transaksi" }), "transfer");
    expect(screen.queryByRole("combobox", { name: "Kategori Pengeluaran" })).not.toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Ke Rekening / Kantong Tujuan" }), "account-2");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/recurring/rule-1", expect.objectContaining({
      type: "transfer",
      category_id: null,
      obligation_id: null,
    })));
  });
});
