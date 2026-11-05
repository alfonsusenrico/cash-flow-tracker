import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { TransactionEditForm } from "./TransactionEditForm";
import type { LedgerRowDetail } from "./types";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), patch: vi.fn() }, uploadTransactionReceipt: vi.fn() }));
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }) }));

const accounts = [
  { id: "jago", name: "Jago", type: "bank", balance: 500_000 },
  { id: "spaylater", name: "SPayLater", type: "credit", balance: -470_000, credit_owed: 470_000 },
];

function row(overrides: Partial<LedgerRowDetail>): LedgerRowDetail {
  return {
    id: "tx-1", type: "expense", amount: 120_000, account_id: "jago", account_name: "Jago", category_id: null, notes: null,
    date: "2026-11-05T12:00:00.000Z", ...overrides,
  } as LedgerRowDetail;
}

function renderForm(tx: LedgerRowDetail) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <TransactionEditForm tx={tx} onSaved={vi.fn()} onCancel={vi.fn()} onPickPair={vi.fn()} onConvertToTopup={vi.fn()} />
    </QueryClientProvider>,
  );
}

describe("TransactionEditForm with credit accounts", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [{ id: "debt-1", name: "Cicilan Contoh", remaining_amount: 900_000, is_archived: false }] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("offers credit for an expense, drops the debt link on it, and keeps it out of an income", async () => {
    const user = userEvent.setup();
    const { container } = renderForm(row({}));
    const account = screen.getByRole("combobox", { name: "Bayar dari" });
    await waitFor(() => expect(within(account).getByRole("group", { name: "Kredit" })).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "+ Tambah tagihan" })).toBeInTheDocument();
    await user.selectOptions(account, "spaylater");
    expect(screen.queryByRole("button", { name: "+ Tambah tagihan" })).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/transactions/tx-1", expect.objectContaining({
      type: "expense", account_id: "spaylater", obligation_id: null,
    })));

    await user.click(screen.getByRole("button", { name: "Masuk" }));
    const income = screen.getByRole("combobox", { name: "Masuk ke" });
    expect(within(income).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
    expect(income).toHaveValue("jago");
  });

  it("keeps the credit account of an existing refund", async () => {
    renderForm(row({ type: "income", account_id: "spaylater", account_name: "SPayLater" }));
    const account = screen.getByRole("combobox", { name: "Masuk ke" });
    await waitFor(() => expect(account).toHaveValue("spaylater"));
    expect(within(account).getByRole("group", { name: "Kredit" })).toBeInTheDocument();
    expect(screen.queryByRole("combobox", { name: /Kaitkan ke tagihan/ })).not.toBeInTheDocument();
  });
});
