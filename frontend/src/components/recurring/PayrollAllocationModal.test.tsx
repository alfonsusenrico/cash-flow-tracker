import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { PayrollAllocationModal } from "./PayrollAllocationModal";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn() } }));

const rules = [
  {
    id: "rule-1",
    name: "Tabungan Jago",
    type: "transfer",
    amount: 50_000,
    source_account_id: "account-1",
    target_account_id: "account-2",
    target_account_name: "Jago",
    schedule_type: "payday",
    schedule_day: null,
    is_payroll_allocation: true,
    auto_post: false,
    is_active: true,
    next_due_date: "2026-09-25",
  },
  {
    id: "rule-2",
    name: "Posisi BBCA lama",
    type: "transfer",
    amount: 10_000,
    source_account_id: "account-1",
    target_account_id: "position-1",
    target_account_name: "BBCA",
    schedule_type: "payday",
    schedule_day: null,
    is_payroll_allocation: true,
    auto_post: false,
    is_active: true,
    next_due_date: "2026-09-25",
  },
];

function renderPayroll() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <PayrollAllocationModal open onClose={() => {}} />
    </QueryClientProvider>,
  );
}

describe("PayrollAllocationModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "account-2", name: "Jago", type: "bank", balance: 20_000 },
        { id: "position-1", name: "BBCA", type: "investment", balance: 50_000 },
      ] };
      if (path === "/recurring?is_payroll=true") return { ok: true, rules };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true, allocated_items_count: 1 });
  });

  it("shows affordable balance and excludes an old position transfer from the batch", async () => {
    const user = userEvent.setup();
    renderPayroll();

    const eligible = await screen.findByRole("checkbox", { name: "Sertakan Tabungan Jago dalam alokasi" });
    const ineligible = screen.getByRole("checkbox", { name: "Sertakan Posisi BBCA lama dalam alokasi" });
    expect(eligible).toBeChecked();
    expect(ineligible).toBeDisabled();
    expect(screen.getAllByText("Rp 50.000")).toHaveLength(2);
    const amount = screen.getByRole("textbox", { name: "Nominal alokasi Tabungan Jago" });
    await user.clear(amount);
    await user.type(amount, "120000");
    expect(screen.getByRole("alert")).toHaveTextContent("Alokasi melebihi saldo sumber");
    expect(screen.getByRole("button", { name: /Jalankan Alokasi Sekarang/ })).toBeDisabled();

    await user.clear(amount);
    await user.type(amount, "40000");
    await user.click(screen.getByRole("button", { name: /Jalankan Alokasi Sekarang/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/recurring/payroll/execute", {
      items: [expect.objectContaining({
        rule_id: "rule-1",
        source_account_id: "account-1",
        target_account_id: "account-2",
        amount: 40_000,
      })],
    }));
  });

  it("keeps the selection and amount after an atomic server rejection", async () => {
    vi.mocked(api.post).mockRejectedValueOnce(new Error("Saldo sumber tidak cukup"));
    const user = userEvent.setup();
    renderPayroll();

    const amount = await screen.findByRole("textbox", { name: "Nominal alokasi Tabungan Jago" });
    await user.click(screen.getByRole("button", { name: /Jalankan Alokasi Sekarang/ }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Saldo sumber tidak cukup");
    expect(amount).toHaveValue("50.000");
    expect(screen.getByRole("checkbox", { name: "Sertakan Tabungan Jago dalam alokasi" })).toBeChecked();
  });
});
