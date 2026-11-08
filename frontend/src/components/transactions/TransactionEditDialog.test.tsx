import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { TransactionEditDialog } from "./TransactionEditDialog";
import type { LedgerRowDetail } from "./types";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), patch: vi.fn(), put: vi.fn(), del: vi.fn() }, uploadTransactionReceipt: vi.fn() }));
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: false, bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }) }));

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

function renderDialog(tx: LedgerRowDetail, handlers: { onPickPair?: () => void; onSaved?: () => void } = {}) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <TransactionEditDialog tx={tx} onSaved={handlers.onSaved ?? vi.fn()} onClose={vi.fn()} onPickPair={handlers.onPickPair} onConvertToTopup={vi.fn()} />
    </QueryClientProvider>,
  );
}

describe("Ubah transaksi (D15)", () => {
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
    vi.mocked(api.put).mockResolvedValue({ ok: true });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
  });

  it("offers credit for an expense, drops the debt link on it, and keeps it out of an income", async () => {
    const user = userEvent.setup();
    renderDialog(row({}));
    const dialog = await screen.findByRole("dialog", { name: "Ubah transaksi" });
    const account = within(dialog).getByRole("combobox", { name: "Bayar dari" });
    await waitFor(() => expect(within(account).getByRole("group", { name: "Kredit" })).toBeInTheDocument());
    expect(within(dialog).getByRole("button", { name: "+ Tambah tagihan" })).toBeInTheDocument();
    await user.selectOptions(account, "spaylater");
    expect(within(dialog).queryByRole("button", { name: "+ Tambah tagihan" })).not.toBeInTheDocument();
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/transactions/tx-1", expect.objectContaining({
      type: "expense", account_id: "spaylater", obligation_id: null,
    })));
    // Moving onto credit with the default terms sends no terms request.
    expect(api.put).not.toHaveBeenCalled();

    await user.click(within(dialog).getByRole("radio", { name: "Masuk" }));
    const income = within(dialog).getByRole("combobox", { name: "Masuk ke" });
    expect(within(income).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
    expect(income).toHaveValue("jago");
  });

  it("keeps the credit account of an existing refund", async () => {
    renderDialog(row({ type: "income", account_id: "spaylater", account_name: "SPayLater" }));
    const account = await screen.findByRole("combobox", { name: "Masuk ke" });
    await waitFor(() => expect(account).toHaveValue("spaylater"));
    expect(within(account).getByRole("group", { name: "Kredit" })).toBeInTheDocument();
    expect(screen.queryByRole("combobox", { name: /Kaitkan ke tagihan/ })).not.toBeInTheDocument();
  });

  it("chooses the pillar from a segmented control with one hint line and no info icon", async () => {
    const user = userEvent.setup();
    renderDialog(row({ kakeibo_type: null }));
    const pillar = await screen.findByRole("radiogroup", { name: "Pilar Kakeibo" });
    expect(within(pillar).getAllByRole("radio").every((radio) => radio.getAttribute("aria-checked") === "false")).toBe(true);
    expect(screen.getByText("Pilih pilar agar transaksi ini masuk ke ringkasan Kakeibo.")).toBeInTheDocument();
    await user.click(within(pillar).getByRole("radio", { name: "Keinginan" }));
    expect(screen.getByText(/Bisa ditunda/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Pilar Kakeibo" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/transactions/tx-1", expect.objectContaining({ kakeibo_type: "want" })));
  });

  it("starts the merge from Pindah saldo instead of saving", async () => {
    const user = userEvent.setup();
    const onPickPair = vi.fn();
    renderDialog(row({}), { onPickPair });
    await user.click(await screen.findByRole("radio", { name: "Pindah saldo" }));
    expect(onPickPair).toHaveBeenCalled();
    expect(api.patch).not.toHaveBeenCalled();
  });

  it("changes a credit purchase's instalments, returns it to paid in full, and checks the count", async () => {
    const user = userEvent.setup();
    const purchase = row({ account_id: "spaylater", account_name: "SPayLater", credit: { role: "purchase", account_id: "spaylater", account_name: "SPayLater", installment_count: 3 } });
    renderDialog(purchase);
    const plan = await screen.findByRole("radiogroup", { name: "Cara bayar" });
    await waitFor(() => expect(within(plan).getByRole("radio", { name: "3x" })).toHaveAttribute("aria-checked", "true"));
    await user.click(within(plan).getByRole("radio", { name: "Sekali bayar" }));
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/transactions/tx-1/credit-terms"));

    await user.click(within(plan).getByRole("radio", { name: "Lainnya" }));
    const count = screen.getByRole("spinbutton", { name: "Jumlah cicilan" });
    await user.clear(count);
    await user.type(count, "40");
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pilih 2 sampai 36 cicilan.");
    await user.clear(count);
    await user.type(count, "4");
    await user.type(screen.getByLabelText("Jatuh tempo pertama (opsional)"), "2026-12-20");
    await user.click(screen.getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 4, first_due_date: "2026-12-20" }));
  });
});
