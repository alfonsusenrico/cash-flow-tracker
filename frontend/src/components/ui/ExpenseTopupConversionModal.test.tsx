import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { ExpenseTopupConversionModal } from "./ExpenseTopupConversionModal";

vi.mock("@/lib/api", () => ({ api: { post: vi.fn() } }));
const fund = (id: string, name: string) => ({ id, name, type: "investment", instrument_type: "mutual_fund", units: null, parent_id: "bibit" });
const accounts = [
  { id: "bibit", name: "Bibit", type: "investment", children: [fund("sucor", "Sucorinvest Money Market Fund"), fund("trim", "TRIM Kas 2 Kelas A")] },
  { id: "rdn", name: "RDN BCA", type: "bank" },
];
const expense = { id: "debit", amount: 112_590, account_name: "RDN BCA" };

function show(overrides: Partial<React.ComponentProps<typeof ExpenseTopupConversionModal>> = {}) {
  const qc = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const close = vi.fn();
  const converted = vi.fn();
  const view = render(
    <QueryClientProvider client={qc}>
      <ExpenseTopupConversionModal expense={expense} accounts={accounts} onClose={close} onConverted={converted} {...overrides} />
    </QueryClientProvider>,
  );
  return { ...view, close, converted };
}

describe("turning a recorded debit into a top-up", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("asks for the product and converts the existing debit", async () => {
    const user = userEvent.setup();
    const { converted } = show();
    expect(screen.getByText(/112\.590/)).toBeInTheDocument();
    expect((await axe(screen.getByRole("dialog"))).violations).toEqual([]);
    await user.click(screen.getByRole("button", { name: "Jadikan top up" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pilih produk reksadana tujuan.");
    expect(screen.getByRole("combobox", { name: "Produk tujuan" })).toHaveFocus();
    expect(api.post).not.toHaveBeenCalled();
    await user.selectOptions(screen.getByRole("combobox", { name: "Produk tujuan" }), "trim");
    await user.click(screen.getByRole("button", { name: "Jadikan top up" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", { transaction_id: "debit", target_account_id: "trim" }));
    expect(converted).toHaveBeenCalledOnce();
  });

  it("explains how to make a product eligible when none is amount-tracked", () => {
    show({ accounts: [{ id: "bibit", name: "Bibit", type: "investment", children: [{ ...fund("sucor", "Sucor"), units: 10 }] }] });
    expect(screen.getByText(/Ubah ke pelacakan nominal/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Jadikan top up" })).toBeDisabled();
  });

  it("keeps the selection and announces a failure", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValueOnce(new Error("Transaksi ini tidak dapat dijadikan top up."));
    const { converted } = show();
    await user.selectOptions(screen.getByRole("combobox", { name: "Produk tujuan" }), "sucor");
    await user.click(screen.getByRole("button", { name: "Jadikan top up" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("tidak dapat dijadikan top up");
    expect(screen.getByRole("combobox", { name: "Produk tujuan" })).toHaveValue("sucor");
    expect(converted).not.toHaveBeenCalled();
  });
});
