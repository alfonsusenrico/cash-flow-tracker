import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, updateMovement, uploadTransactionReceipt } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import LedgerPage from "./page";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), patch: vi.fn(), del: vi.fn() },
  createMovement: vi.fn(),
  deleteMovement: vi.fn(),
  updateMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
}));

vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ openQuickAdd: vi.fn(), bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

vi.mock("@/components/recurring/PendingScheduledBanner", () => ({ PendingScheduledBanner: () => null }));
vi.mock("@/components/recurring/RecurringRulesModal", () => ({ RecurringRulesModal: () => null }));
vi.mock("@/components/ledger/MobileLedgerFeed", () => ({ MobileLedgerFeed: () => null }));

const transaction = {
  id: "transaction-1",
  account_id: "account-1",
  account_name: "BCA",
  category_id: "category-1",
  category_name: "Makan",
  category_icon: null,
  category_color: null,
  goal_id: null,
  goal_name: null,
  obligation_id: null,
  obligation_name: null,
  obligation_allocations: [] as { obligation_id: string; obligation_name: string; amount: number }[],
  type: "expense",
  kakeibo_type: "need",
  amount: 25_000,
  notes: "Makan siang",
  date: "2026-09-20T12:00:00.000Z",
  receipt_path: "old-receipt.jpg",
  created_at: "2026-09-20T12:00:00.000Z",
};
type MovementTransaction = typeof transaction & {
  movement_id: string;
  movement_role: "outbound";
  target_account_id: string;
  is_consolidated_transfer: boolean;
};
let currentTransactions: Array<typeof transaction | MovementTransaction> = [transaction];

describe("Ledger edit receipt recovery", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentTransactions = [transaction];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 100_000 }] };
      if (path === "/categories") return { categories: [
        { id: "category-1", name: "Makan", kind: "expense", kakeibo_type: "need" },
        { id: "category-2", name: "Hobi", kind: "expense", kakeibo_type: "want" },
      ] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) return { ok: true, total: currentTransactions.length, transactions: currentTransactions };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(uploadTransactionReceipt)
      .mockRejectedValueOnce(new Error("Bukti tidak valid"))
      .mockResolvedValueOnce({ ok: true, receipt_path: "receipt.jpg" });
  });

  it("retries only the attachment after the financial edit has saved", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={queryClient}>
        <LedgerPage />
      </QueryClientProvider>,
    );

    await user.click((await screen.findAllByRole("button", { name: "Ubah" }))[0]);
    await user.upload(
      screen.getByLabelText("Ganti / tambahkan bukti"),
      new File(["receipt"], "receipt.jpg", { type: "image/jpeg" }),
    );
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Perubahan tersimpan, tetapi bukti gagal diunggah");
    expect(api.patch).toHaveBeenCalledTimes(1);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.transactions.all });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.accounts });
    expect(api.patch).toHaveBeenCalledWith("/transactions/transaction-1", expect.objectContaining({
      type: "expense",
      amount: 25_000,
      account_id: "account-1",
      category_id: "category-1",
      kakeibo_type: "need",
      notes: "Makan siang",
    }));
    await user.click(screen.getByRole("button", { name: "Coba unggah bukti lagi" }));

    await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledTimes(2));
    expect(api.patch).toHaveBeenCalledTimes(1);
  });

  it("loads an archived multi-debt split and saves changed amounts as one transaction", async () => {
    currentTransactions = [{
      ...transaction,
      amount: 100_000,
      obligation_allocations: [
        { obligation_id: "debt-a", obligation_name: "Kartu A", amount: 60_000 },
        { obligation_id: "debt-b", obligation_name: "Kartu B", amount: 40_000 },
      ],
    }];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 200_000 }] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [
        { id: "debt-a", name: "Kartu A", remaining_amount: 0, is_archived: true },
        { id: "debt-b", name: "Kartu B", remaining_amount: 40_000, is_archived: false },
      ] };
      if (path.startsWith("/transactions?")) return { ok: true, total: 1, transactions: currentTransactions };
      throw new Error(`Unexpected GET ${path}`);
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);
    expect(await screen.findByText(/2 tagihan/)).toBeInTheDocument();
    await user.click((await screen.findAllByRole("button", { name: "Ubah" }))[0]);
    expect(screen.getByRole("option", { name: "Kartu A (tersimpan)" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("100.000");
    expect(screen.getByText("Dibayar Rp 60.000")).toBeInTheDocument();
    expect(screen.getByText("Dibayar Rp 40.000")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
    await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }));
    await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }), "50000");
    await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" }));
    await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" }), "30000");
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("80.000");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/transactions/transaction-1", expect.objectContaining({
      amount: 80_000,
      obligation_id: null,
      obligation_allocations: [
        { obligation_id: "debt-a", amount: 50_000 },
        { obligation_id: "debt-b", amount: 30_000 },
      ],
    })));
  });

  it("confirms that deleting an allocated payment restores every debt", async () => {
    currentTransactions = [{
      ...transaction,
      amount: 100_000,
      obligation_allocations: [
        { obligation_id: "debt-a", obligation_name: "Kartu A", amount: 60_000 },
        { obligation_id: "debt-b", obligation_name: "Kartu B", amount: 40_000 },
      ],
    }];
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);
    await user.click((await screen.findAllByRole("button", { name: "Ubah" }))[0]);
    await user.click(screen.getByRole("button", { name: "Hapus transaksi" }));
    const confirmation = screen.getByRole("group", { name: /seluruh pembagian tagihan dikembalikan/ });
    await user.click(within(confirmation).getByRole("button", { name: "Batal" }));
    expect(api.del).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Hapus transaksi" }));
    await user.click(screen.getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/transactions/transaction-1"));
  });

  it("routes a linked movement edit through the atomic movement operation", async () => {
    currentTransactions = [{
      ...transaction,
      id: "movement-out",
      category_name: "Internal Movement",
      movement_id: "movement-1",
      movement_role: "outbound",
      target_account_id: "account-2",
      is_consolidated_transfer: true,
    }];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "account-2", name: "Jago", type: "bank", balance: 50_000 },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) return { ok: true, total: 1, transactions: currentTransactions };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(updateMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-1",
      expense_transaction_id: "movement-out",
      income_transaction_id: "movement-in",
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await user.click((await screen.findAllByRole("button", { name: "Ubah" }))[0]);
    expect(await screen.findByRole("dialog", { name: "Ubah pindah saldo" })).toBeVisible();
    await user.clear(screen.getByRole("textbox", { name: "Nominal (IDR)" }));
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "30000");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(updateMovement).toHaveBeenCalledWith("movement-1", expect.objectContaining({
      source_account_id: "account-1",
      target_account_id: "account-2",
      amount: 30_000,
    })));
    expect(api.patch).not.toHaveBeenCalled();
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.transactions.all });
  });

  it("uses the new category pillar unless the user explicitly overrides it", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await user.click((await screen.findAllByRole("button", { name: "Ubah" }))[0]);
    const category = screen.getByRole("combobox", { name: "Kategori" });
    const pillar = screen.getByRole("combobox", { name: "Pilar Kakeibo" });
    await user.selectOptions(category, "category-2");
    expect(pillar).toHaveValue("want");
    await user.selectOptions(pillar, "need");
    await user.selectOptions(category, "category-1");
    await user.selectOptions(category, "category-2");
    expect(pillar).toHaveValue("need");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/transactions/transaction-1", expect.objectContaining({
      category_id: "category-2",
      kakeibo_type: "need",
    })));
  });
});
