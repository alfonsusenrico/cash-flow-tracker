import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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

  describe("an owner-set first due date", () => {
    const owned = () => row({
      account_id: "spaylater", account_name: "SPayLater",
      credit: { role: "purchase", account_id: "spaylater", account_name: "SPayLater", installment_count: 3, first_due_date: "2026-12-20", first_due_date_set_by: "owner" },
    });

    it("starts from the stored date and sends nothing when the terms are unchanged", async () => {
      const user = userEvent.setup();
      renderDialog(owned());
      expect(await screen.findByLabelText("Jatuh tempo pertama (opsional)")).toHaveValue("2026-12-20");
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.patch).toHaveBeenCalled());
      expect(api.put).not.toHaveBeenCalled();
      expect(api.del).not.toHaveBeenCalled();
    });

    it("keeps the date when only the instalment count changes", async () => {
      const user = userEvent.setup();
      renderDialog(owned());
      await user.click(await screen.findByRole("radio", { name: "6x" }));
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 6, first_due_date: "2026-12-20" }));
    });

    it("follows the account again when the owner clears the date", async () => {
      const user = userEvent.setup();
      renderDialog(owned());
      await user.clear(await screen.findByLabelText("Jatuh tempo pertama (opsional)"));
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 3, first_due_date: null }));
    });
  });

  describe("a first due date the system kept (apply-markas-cleanup F1)", () => {
    const kept = (installment_count = 1) => row({
      account_id: "spaylater", account_name: "SPayLater",
      credit: { role: "purchase", account_id: "spaylater", account_name: "SPayLater", installment_count, first_due_date: "2026-11-11", first_due_date_set_by: "system" },
    });

    it("does not show it, and sends nothing when the terms are unchanged", async () => {
      const user = userEvent.setup();
      renderDialog(kept());
      expect(await screen.findByLabelText("Jatuh tempo pertama (opsional)")).toHaveValue("");
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.patch).toHaveBeenCalled());
      expect(api.put).not.toHaveBeenCalled();
      expect(api.del).not.toHaveBeenCalled();
    });

    it("sends it back when only the instalment count changes", async () => {
      const user = userEvent.setup();
      renderDialog(kept());
      await user.click(await screen.findByRole("radio", { name: "3x" }));
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 3, first_due_date: "2026-11-11" }));
      expect(api.del).not.toHaveBeenCalled();
    });

    it("sends it back with Sekali bayar instead of deleting the terms", async () => {
      const user = userEvent.setup();
      renderDialog(kept(3));
      await user.click(await screen.findByRole("radio", { name: "Sekali bayar" }));
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 1, first_due_date: "2026-11-11" }));
      expect(api.del).not.toHaveBeenCalled();
    });

    it("lets the server clear it when the purchase date moves", async () => {
      const user = userEvent.setup();
      renderDialog(kept());
      fireEvent.change(await screen.findByLabelText("Tanggal dan waktu"), { target: { value: "2026-11-07T19:00:00" } });
      await user.click(screen.getByRole("radio", { name: "3x" }));
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 3, first_due_date: null }));
    });

    it("is replaced by a date the owner types", async () => {
      const user = userEvent.setup();
      renderDialog(kept());
      await user.type(await screen.findByLabelText("Jatuh tempo pertama (opsional)"), "2026-12-01");
      await user.click(screen.getByRole("button", { name: "Simpan" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/tx-1/credit-terms", { installment_count: 1, first_due_date: "2026-12-01" }));
    });
  });
});

describe("Mengganti transaksi (link-reimbursements-to-expenses)", () => {
  const categories = [
    { id: "patungan", name: "Patungan & Ganti Uang", kind: "income", offsets_spending: true },
    { id: "lain", name: "Pendapatan Lain", kind: "income", offsets_spending: false },
    { id: "belanja", name: "Belanja", kind: "expense", kakeibo_type: "need" },
  ];
  const coffee = { id: "coffee", type: "expense", amount: 25_500, notes: "beli kopi titipan mama", merchant: null, account_id: "jago", account_name: "Jago",
    category_name: "Belanja", date: "2026-10-28T11:34:00.000Z" };
  const search: string[] = [];

  beforeEach(() => {
    vi.clearAllMocks();
    search.length = 0;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts };
      if (path === "/categories") return { categories };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) {
        search.push(path);
        return { transactions: [coffee] };
      }
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(api.put).mockResolvedValue({ ok: true });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
  });

  const motherMoney = row({ id: "mama", type: "income", amount: 100_000, category_id: "patungan", notes: null });

  it("links the coffee, records the rest as income, and leaves the category link out", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    renderDialog(motherMoney, { onSaved });
    const dialog = await screen.findByRole("dialog", { name: "Ubah transaksi" });
    const field = await within(dialog).findByRole("group", { name: "Mengganti transaksi (opsional)" });
    await user.click(await within(field).findByRole("button", { name: "Tambah beli kopi titipan mama" }));
    expect(within(field).getByRole("list", { name: "Pengeluaran yang diganti" })).toHaveTextContent("beli kopi titipan mama");
    expect(within(dialog).queryByLabelText("Mengurangi pengeluaran")).not.toBeInTheDocument();
    expect(search[0]).toMatch(/^\/transactions\?type=expense&from_date=\d{4}-\d{2}-\d{2}T00%3A00%3A00%2B07%3A00&limit=20$/);
    await user.click(within(field).getByRole("radio", { name: "Dicatat sebagai uang masuk" }));
    expect(within(field).getByRole("combobox", { name: "Kategori uang masuk" })).toHaveValue("lain");
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/transactions/mama/reimburses", { expense_ids: ["coffee"], rest_category_id: "lain" }));
    expect(vi.mocked(api.patch).mock.calls[0][1]).toMatchObject({ offset_category_id: null });
    expect(onSaved).toHaveBeenCalled();
  });

  it("searches by text and unlinks when every expense is removed", async () => {
    const user = userEvent.setup();
    const linked = { ...motherMoney, reimbursement: { repays: [{ transaction_id: "coffee", amount: 25_500, expense_amount: 25_500, date: coffee.date,
      notes: "beli kopi titipan mama", merchant: null }], unallocated: 74_500, repaid_by: [], repaid_total: 0 } } as LedgerRowDetail;
    renderDialog(linked);
    const dialog = await screen.findByRole("dialog", { name: "Ubah transaksi" });
    const field = await within(dialog).findByRole("group", { name: "Mengganti transaksi (opsional)" });
    await user.type(within(field).getByRole("searchbox", { name: "Cari pengeluaran 90 hari terakhir" }), "kopi");
    await waitFor(() => expect(search.at(-1)).toContain("q=kopi"));
    await user.click(within(field).getByRole("button", { name: "Hapus beli kopi titipan mama" }));
    expect(within(dialog).getByLabelText("Mengurangi pengeluaran")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/transactions/mama/reimburses"));
    expect(api.put).not.toHaveBeenCalled();
  });

  it("does not offer linking on an ordinary income", async () => {
    renderDialog(row({ id: "gaji", type: "income", amount: 1_000_000, category_id: "lain" }));
    const dialog = await screen.findByRole("dialog", { name: "Ubah transaksi" });
    await waitFor(() => expect(within(dialog).getByRole("option", { name: "Pendapatan Lain" })).toBeInTheDocument());
    expect(within(dialog).queryByRole("group", { name: "Mengganti transaksi (opsional)" })).not.toBeInTheDocument();
  });
});
