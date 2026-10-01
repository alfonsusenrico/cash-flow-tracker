import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, updateMovement, uploadTransactionReceipt } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import LedgerPage from "./page";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
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
  date: "2026-09-20T12:00:27.000Z",
  receipt_path: "old-receipt.jpg",
  created_at: "2026-09-20T12:00:00.000Z",
};
type MovementTransaction = typeof transaction & {
  movement_id: string;
  movement_role: "outbound" | "inbound";
  target_account_id?: string;
  transfer_target_account_id?: string;
  is_consolidated_transfer?: boolean;
  investment_topup_id?: string;
};
let currentTransactions: Array<typeof transaction | MovementTransaction> = [transaction];

const longPressRow = async (row: HTMLElement) => {
  vi.useFakeTimers();
  try {
    // JSDOM lacks PointerEvent, so MouseEvent preserves the pointer button fields React uses.
    fireEvent(row, new MouseEvent("pointerdown", { bubbles: true, button: 0, clientX: 20, clientY: 20 }));
    await act(async () => vi.advanceTimersByTime(1000));
    fireEvent(row, new MouseEvent("pointerup", { bubbles: true, button: 0, clientX: 20, clientY: 20 }));
    fireEvent.click(row);
    await act(async () => vi.advanceTimersByTime(750));
  } finally {
    vi.useRealTimers();
  }
};

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

    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
    expect(screen.getByLabelText("Tanggal & Waktu")).toHaveAttribute("step", "1");
    expect((screen.getByLabelText("Tanggal & Waktu") as HTMLInputElement).value).toMatch(/:27(?:\.000)?$/);
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
    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
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
    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
    await user.click(screen.getByRole("button", { name: "Hapus transaksi" }));
    const confirmation = screen.getByRole("group", { name: /seluruh pembagian tagihan dikembalikan/ });
    await user.click(within(confirmation).getByRole("button", { name: "Batal" }));
    expect(api.del).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Hapus transaksi" }));
    await user.click(screen.getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/transactions/transaction-1"));
  });

  it("opens a consolidated top-up through the dedicated correction endpoint", async () => {
    currentTransactions = [{ ...transaction, id: "topup-out", notes: "Monthly Bibit", movement_id: "topup-1", movement_role: "outbound", target_account_id: "fund-1", is_consolidated_transfer: true, investment_topup_id: "topup-1" }];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "fund-1", name: "Bibit Fund", type: "investment", instrument_type: "mutual_fund", units: null },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) return { total: 1, transactions: currentTransactions };
      throw new Error(`Unexpected GET ${path}`);
    });
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const invalidate = vi.spyOn(qc, "invalidateQueries");
    const user = userEvent.setup();
    render(<QueryClientProvider client={qc}><LedgerPage /></QueryClientProvider>);
    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Monthly Bibit/ }));
    expect(screen.getByRole("dialog", { name: "Ubah Top up Investasi" })).toBeVisible();
    expect(screen.queryByRole("dialog", { name: "Ubah pindah saldo" })).not.toBeInTheDocument();
    await user.clear(screen.getByLabelText("Nominal top up (IDR)"));
    await user.type(screen.getByLabelText("Nominal top up (IDR)"), "20000");
    await user.click(screen.getByRole("button", { name: "Simpan perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/investment-topups/topup-1", expect.objectContaining({ amount: 20_000 })));
    expect(updateMovement).not.toHaveBeenCalled();
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.transactions.all });
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

    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
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

  it("uses the source partner when editing a filtered inbound movement", async () => {
    currentTransactions = [{
      ...transaction,
      id: "movement-in",
      account_id: "account-2",
      account_name: "Jago",
      category_name: "Internal Movement",
      type: "income",
      movement_id: "movement-1",
      movement_role: "inbound",
      transfer_target_account_id: "account-1",
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
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
    expect(await screen.findByRole("dialog", { name: "Ubah pindah saldo" })).toBeVisible();
    expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("account-1");
    expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("account-2");
  });

  it("uses the new category pillar unless the user explicitly overrides it", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await user.click(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
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

describe("Ledger movement selection", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("keeps a linked movement consolidated and visibly marked while selecting", async () => {
    const outbound = {
      ...transaction,
      id: "movement-out",
      notes: "Pindah tabungan",
      category_name: "Internal Movement",
      movement_id: "movement-1",
      movement_role: "outbound",
      partner_id: "movement-in",
      transfer_target_account_name: "Jago",
    };
    const inbound = {
      ...outbound,
      id: "movement-in",
      account_id: "account-2",
      account_name: "Jago",
      type: "income",
      movement_role: "inbound",
      partner_id: "movement-out",
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) {
        const isLogical = new URLSearchParams(path.split("?")[1]).get("logical_movements") === "true";
        return { ok: true, total: isLogical ? 2 : 3, transactions: isLogical ? [outbound, transaction] : [outbound, inbound, transaction] };
      }
      throw new Error(`Unexpected GET ${path}`);
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await longPressRow(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));

    expect(screen.getByText("Tergabung")).toBeInTheDocument();
    expect(screen.getAllByText("Pindah Saldo").length).toBeGreaterThan(0);
    expect(screen.queryByText("Pindah Saldo (Masuk)")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Gabungkan transaksi" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Aksi" })).not.toBeInTheDocument();
    expect(vi.mocked(api.get).mock.calls.filter(([path]) => path.startsWith("/transactions?")).every(
      ([path]) => new URLSearchParams(path.split("?")[1]).get("logical_movements") === "true",
    )).toBe(true);
  });

  it("keeps inferred legacy pairs consolidated while selecting both records as one row", async () => {
    const outbound = { ...transaction, category_name: "Internal Movement", notes: "Pindah tabungan" };
    const inbound = {
      ...outbound,
      id: "transaction-2",
      account_id: "account-2",
      account_name: "Jago",
      type: "income",
      notes: "Internal Movement",
      date: "2026-09-20T12:00:45.000Z",
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank" },
        { id: "account-2", name: "Jago", type: "bank" },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) {
        const account = new URLSearchParams(path.split("?")[1]).get("account_id");
        return { ok: true, total: account ? 1 : 2, transactions: account ? [inbound] : [outbound, inbound] };
      }
      throw new Error(`Unexpected GET ${path}`);
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    expect((await screen.findAllByText("Pindah Saldo")).length).toBeGreaterThan(0);
    await longPressRow(await screen.findByRole("row", { name: /Buka atau ubah: Pindah tabungan/ }));
    expect(screen.getAllByRole("row", { name: /Dipilih: Pindah tabungan/ })).toHaveLength(1);
    expect(screen.getByText("2/2 dipilih · klik baris untuk mengubah pilihan")).toBeInTheDocument();
    expect(screen.queryByText(/perkiraan|belum tertaut/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Jadikan Pindah Saldo" })).toBeEnabled();
    await user.click(screen.getByRole("row", { name: /Dipilih: Pindah tabungan/ }));
    expect(screen.queryByRole("button", { name: "Jadikan Pindah Saldo" })).not.toBeInTheDocument();
    expect(screen.getByText("Tahan baris 1 detik untuk mulai memilih")).toBeInTheDocument();
    expect(screen.getAllByRole("row", { name: /Buka atau ubah: Pindah tabungan/ })).toHaveLength(1);
  });

  it("links equal-value selected transactions directly without a confirmation preview", async () => {
    const incoming = {
      ...transaction,
      id: "transaction-2",
      account_id: "account-2",
      account_name: "Jago",
      category_name: "Transfer diterima",
      type: "income",
      notes: "Masuk rekening",
      date: "2026-09-20T12:02:45.000Z",
      receipt_path: null,
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [
        { id: "account-1", name: "BCA", type: "bank", balance: 100_000 },
        { id: "account-2", name: "Jago", type: "bank", balance: 25_000 },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) {
        const offset = new URLSearchParams(path.split("?")[1]).get("offset");
        return { ok: true, total: 51, transactions: offset === "0" ? [transaction] : [incoming] };
      }
      if (path === "/transactions/transaction-1") return { transaction };
      if (path === "/transactions/transaction-2") return { transaction: incoming };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await longPressRow(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
    await user.click(screen.getAllByRole("button", { name: "Berikutnya" }).at(-1)!);
    await user.click(await screen.findByRole("row", { name: /Belum dipilih: Masuk rekening/ }));
    await user.click(screen.getByRole("button", { name: "Jadikan Pindah Saldo" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", {
      expense_transaction_id: "transaction-1",
      income_transaction_id: "transaction-2",
    }));
    expect(screen.queryByRole("dialog", { name: "Gabungkan sebagai Pindah Saldo" })).not.toBeInTheDocument();
  });

  it("rejects different amounts with a centered single-button warning without calling the endpoint", async () => {
    const incoming = {
      ...transaction,
      id: "transaction-2",
      account_id: "account-2",
      account_name: "Jago",
      type: "income",
      notes: "Masuk rekening",
      amount: 40_000,
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) return { ok: true, total: 2, transactions: [transaction, incoming] };
      if (path === "/transactions/transaction-1") return { transaction };
      if (path === "/transactions/transaction-2") return { transaction: incoming };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><LedgerPage /></QueryClientProvider>);

    await longPressRow(await screen.findByRole("row", { name: /Buka atau ubah: Makan siang/ }));
    await user.click(await screen.findByRole("row", { name: /Belum dipilih: Masuk rekening/ }));
    await user.click(screen.getByRole("button", { name: "Jadikan Pindah Saldo" }));
    const warning = await screen.findByRole("dialog", { name: "Nominal tidak sama" });

    expect(within(warning).getByRole("alert")).toHaveTextContent("Pindah saldo hanya dapat dibuat jika nominalnya sama");
    expect(within(warning).getAllByRole("button")).toHaveLength(1);
    expect(within(warning).getByRole("button", { name: "Ok" })).toBeVisible();
    expect(api.post).not.toHaveBeenCalled();
  });
});

describe("Ledger summary metrics (running cycle and cumulative)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 100_000 }] };
      if (path === "/categories") return { categories: [{ id: "category-1", name: "Makan", kind: "expense", kakeibo_type: "need" }] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path.startsWith("/transactions?")) {
        return {
          ok: true,
          total: 10,
          summary: {
            cycle: {
              start: "2026-09-24T17:00:00.000Z",
              end: "2026-10-24T16:59:59.000Z",
              inflow: 7_436_000,
              outflow: 1_250_000,
              net: 6_186_000,
            },
            cumulative: {
              inflow: 54_200_000,
              outflow: 42_100_000,
              net: 12_100_000,
            },
          },
          transactions: [transaction],
        };
      }
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("renders running cycle metrics by default and switches to cumulative metrics on toggle", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={queryClient}>
        <LedgerPage />
      </QueryClientProvider>,
    );

    // Verify Cycle Date Range Badge
    expect(await screen.findByText(/Siklus 25 Sep – 24 Okt/)).toBeInTheDocument();

    // Default scope: Cycle Berjalan
    // Inflow: +Rp 7.436.000 (primary on mobile & desktop) & Kumulatif: +Rp 54.200.000 (secondary on desktop)
    expect(screen.getAllByText("+Rp 7.436.000")).toHaveLength(2);
    expect(screen.getAllByText("-Rp 1.250.000")).toHaveLength(2);
    expect(screen.getAllByText("+Rp 6.186.000")).toHaveLength(2);

    expect(screen.getAllByText("Uang Masuk (Siklus)")).toHaveLength(1);
    expect(screen.getAllByText("Uang Keluar (Siklus)")).toHaveLength(1);
    expect(screen.getAllByText("Selisih Bersih (Siklus)")).toHaveLength(1);

    // Switch to Cumulative mode
    const cumulativeBtn = screen.getByRole("button", { name: "Total Kumulatif" });
    await user.click(cumulativeBtn);

    // Primary numbers now reflect cumulative on both mobile & desktop
    expect(screen.getAllByText("+Rp 54.200.000")).toHaveLength(2);
    expect(screen.getAllByText("-Rp 42.100.000")).toHaveLength(2);
    expect(screen.getAllByText("+Rp 12.100.000")).toHaveLength(2);

    expect(screen.getAllByText("Uang Masuk (Kumulatif)")).toHaveLength(1);
    expect(screen.getAllByText("Uang Keluar (Kumulatif)")).toHaveLength(1);
    expect(screen.getAllByText("Selisih Bersih (Kumulatif)")).toHaveLength(1);

    // Secondary footers now show "Siklus ini:"
    expect(screen.getAllByText("Siklus ini:")).toHaveLength(3);

    // Switch back to Cycle mode
    const cycleBtn = screen.getByRole("button", { name: "Siklus Berjalan" });
    await user.click(cycleBtn);

    expect(screen.getAllByText("+Rp 7.436.000")).toHaveLength(2);
    expect(screen.getAllByText("Uang Masuk (Siklus)")).toHaveLength(1);
  });
});
