import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api, createMovement, uploadTransactionReceipt } from "@/lib/api";
import { QuickCaptureModal } from "./QuickCaptureModal";

vi.mock("@/lib/api", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
  },
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
}));

function renderQuickCapture(props: { defaultType?: "expense" | "income" | "movement" } = {}) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

  const onClose = vi.fn();
  const view = render(
    <QueryClientProvider client={queryClient}>
      <QuickCaptureModal open onClose={onClose} {...props} />
    </QueryClientProvider>,
  );
  return { ...view, onClose };
}

describe("QuickCaptureModal", () => {
  afterEach(() => vi.unstubAllGlobals());

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") {
        return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 100_000 }] };
      }
      if (path === "/categories") {
        return {
          categories: [
            { id: "want-category", name: "Hobi", kind: "expense", kakeibo_type: "want" },
            { id: "need-category", name: "Makan", kind: "expense", kakeibo_type: "need" },
            { id: "income-category", name: "Gaji", kind: "income" },
          ],
        };
      }
      if (path === "/goals") return { goals: [{ id: "goal-1", name: "Dana Darurat", is_archived: false }] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true, transaction_id: "transaction-1" });
    vi.mocked(createMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-1",
      expense_transaction_id: "movement-out",
      income_transaction_id: "movement-in",
    });
    vi.mocked(uploadTransactionReceipt).mockResolvedValue({ ok: true, receipt_path: "receipt.jpg" });
  });

  it("inherits category classification until the user deliberately overrides it", async () => {
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();

    const categorySelect = await screen.findByRole("combobox", { name: "Kategori" });
    expect(screen.queryByRole("option", { name: "Gaji" })).not.toBeInTheDocument();
    await user.selectOptions(categorySelect, "want-category");
    expect(screen.getByRole("button", { name: "Keinginan" })).toHaveAttribute("aria-pressed", "true");

    await user.click(screen.getByRole("button", { name: "Kebutuhan" }));
    await user.selectOptions(categorySelect, "need-category");
    expect(screen.getByRole("button", { name: "Kebutuhan" })).toHaveAttribute("aria-pressed", "true");

    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "15000");
    await user.type(screen.getByRole("textbox", { name: "Catatan (opsional)" }), "Lunch");
    await user.keyboard("{Enter}");

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      account_id: "account-1",
      amount: 15_000,
      category_id: "need-category",
      kakeibo_type: "need",
      notes: "Lunch",
    })));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("submits the selected transaction time with seconds", async () => {
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();
    await user.selectOptions(await screen.findByRole("combobox", { name: "Kategori" }), "need-category");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "1000");
    const date = screen.getByLabelText("Waktu Transaksi");
    expect(date).toHaveAttribute("step", "1");
    fireEvent.change(date, { target: { value: "2026-09-20T12:00:27" } });
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      date: expect.any(String),
    })));
    const payload = vi.mocked(api.post).mock.calls[0][1] as { date: string };
    expect(new Date(payload.date).toISOString()).toBe(new Date("2026-09-20T12:00:27").toISOString());
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("filters categories after switching transaction type and keeps the form accessible", async () => {
    const user = userEvent.setup();
    const { container } = renderQuickCapture();
    const categorySelect = await screen.findByRole("combobox", { name: "Kategori" });
    await user.click(screen.getByRole("button", { name: "Pemasukan" }));

    expect(screen.getByRole("option", { name: "Gaji" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Hobi" })).not.toBeInTheDocument();
    expect(screen.queryByText("Pilar Kakeibo")).not.toBeInTheDocument();
    expect(categorySelect).toHaveValue("");
    expect((await axe(container.ownerDocument.body)).violations).toHaveLength(0);
  });

  it("treats goal linkage as metadata without changing the chosen classification", async () => {
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();

    await user.selectOptions(await screen.findByRole("combobox", { name: "Kategori" }), "want-category");
    await user.selectOptions(screen.getByRole("combobox", { name: "Kaitkan ke target (opsional)" }), "goal-1");
    expect(screen.getByRole("button", { name: "Keinginan" })).toHaveAttribute("aria-pressed", "true");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "10000");
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      goal_id: "goal-1",
      kakeibo_type: "want",
      notes: null,
    })));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("derives a partial multi-debt payment from edited debt amounts", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 200_000 }] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [
        { id: "debt-1", name: "Kartu A", remaining_amount: 80_000, is_archived: false },
        { id: "debt-2", name: "Kartu B", remaining_amount: 70_000, is_archived: false },
      ] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();
    await screen.findByRole("button", { name: "+ Tambah tagihan" });
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "debt-1");
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 2" }), "debt-2");
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("150.000");
    expect(screen.queryByRole("textbox", { name: /Dibayar untuk tagihan/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
    await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }));
    await user.click(screen.getByRole("button", { name: "Selesai ubah jumlah" }));
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Isi nominal positif");
    expect(screen.getByRole("button", { name: "Ubah jumlah" })).toHaveFocus();
    expect(api.post).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
    await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }), "60000");
    await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" }));
    await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" }), "40000");
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("100.000");
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      amount: 100_000,
      obligation_id: null,
      obligation_allocations: [
        { obligation_id: "debt-1", amount: 60_000 },
        { obligation_id: "debt-2", amount: 40_000 },
      ],
    })));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("records selected debts at full outstanding amounts without extra inputs", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 200_000 }] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [
        { id: "debt-1", name: "Kartu A", remaining_amount: 80_000, is_archived: false },
        { id: "debt-2", name: "Kartu B", remaining_amount: 70_000, is_archived: false },
      ] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();
    await screen.findByRole("button", { name: "+ Tambah tagihan" });
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "debt-1");
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 2" }), "debt-2");
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("150.000");
    expect(screen.queryByRole("textbox", { name: /Dibayar untuk tagihan/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      amount: 150_000,
      obligation_id: null,
      obligation_allocations: [
        { obligation_id: "debt-1", amount: 80_000 },
        { obligation_id: "debt-2", amount: 70_000 },
      ],
    })));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("keeps a single debt compatible with a partial legacy payment", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 200_000 }] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [
        { id: "debt-1", name: "Kartu A", remaining_amount: 80_000, is_archived: false },
      ] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();
    await screen.findByRole("button", { name: "+ Tambah tagihan" });
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "debt-1");
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("80.000");
    await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
    await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }));
    await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }), "30000");
    expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("30.000");
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      amount: 30_000,
      obligation_id: "debt-1",
    })));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("retries an allocated payment receipt without submitting the payment twice", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 200_000 }] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [
        { id: "debt-1", name: "Kartu A", remaining_amount: 80_000, is_archived: false },
        { id: "debt-2", name: "Kartu B", remaining_amount: 70_000, is_archived: false },
      ] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(uploadTransactionReceipt).mockRejectedValueOnce(new Error("Bukti gagal")).mockResolvedValueOnce({ ok: true, receipt_path: "receipt.jpg" });
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();
    await screen.findByRole("button", { name: "+ Tambah tagihan" });
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "debt-1");
    await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 2" }), "debt-2");
    await user.upload(screen.getByLabelText("Bukti transaksi (opsional)"), new File(["receipt"], "receipt.jpg", { type: "image/jpeg" }));
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Transaksi tersimpan, tetapi bukti belum terunggah");
    await user.click(screen.getByRole("button", { name: "Coba Unggah Lagi" }));
    await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledTimes(2));
    expect(api.post).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("keeps a created transaction and retries a failed receipt without duplicating it", async () => {
    const user = userEvent.setup();
    vi.mocked(uploadTransactionReceipt)
      .mockRejectedValueOnce(new Error("Bukti tidak valid"))
      .mockResolvedValueOnce({ ok: true, receipt_path: "replacement.jpg" });
    const { onClose } = renderQuickCapture();

    const accountSelect = await screen.findByRole("combobox", { name: "Bayar dari" });
    await user.selectOptions(accountSelect, "account-1");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "25000");
    const receiptInput = screen.getByLabelText("Bukti transaksi (opsional)");
    await user.upload(receiptInput, new File(["first"], "receipt.jpg", { type: "image/jpeg" }));
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Transaksi tersimpan, tetapi bukti belum terunggah");
    expect(api.post).toHaveBeenCalledTimes(1);

    await user.upload(receiptInput, new File(["replacement"], "replacement.jpg", { type: "image/jpeg" }));
    await user.click(screen.getByRole("button", { name: "Coba Unggah Lagi" }));

    await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledTimes(2));
    expect(api.post).toHaveBeenCalledTimes(1);
    expect(uploadTransactionReceipt).toHaveBeenLastCalledWith(
      "transaction-1",
      expect.objectContaining({ name: "replacement.jpg" }),
    );
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("attaches a valid receipt to the newly created transaction", async () => {
    const user = userEvent.setup();
    const { onClose } = renderQuickCapture();

    await screen.findByRole("combobox", { name: "Bayar dari" });
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "12000");
    await user.upload(
      screen.getByLabelText("Bukti transaksi (opsional)"),
      new File(["receipt"], "receipt.jpg", { type: "image/jpeg" }),
    );
    await user.click(screen.getByRole("button", { name: "Simpan Transaksi" }));

    await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledWith(
      "transaction-1",
      expect.objectContaining({ name: "receipt.jpg" }),
    ));
    expect(api.post).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("creates an internal movement from the shared form without transaction-only fields", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") {
        return { accounts: [
          { id: "cash-a", name: "Cash A", type: "cash", balance: 100_000 },
          { id: "cash-b", name: "Bank B", type: "bank", balance: 0 },
          { id: "stock", name: "Stock", type: "investment", balance: 500_000 },
        ] };
      }
      if (path === "/categories") {
        return { categories: [{ id: "need-category", name: "Makan", kind: "expense", kakeibo_type: "need" }] };
      }
      if (path === "/goals") return { goals: [{ id: "goal-1", name: "Dana Darurat", is_archived: false }] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    const { container, onClose } = renderQuickCapture();

    await user.selectOptions(await screen.findByRole("combobox", { name: "Kategori" }), "need-category");
    await user.selectOptions(screen.getByRole("combobox", { name: "Kaitkan ke target (opsional)" }), "goal-1");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "15000");
    await user.type(screen.getByRole("textbox", { name: "Catatan (opsional)" }), "Move savings");
    await user.click(screen.getByRole("button", { name: "Perpindahan" }));

    expect(screen.getByRole("textbox", { name: "Nominal (IDR)" })).toHaveValue("15.000");
    expect(screen.getByRole("textbox", { name: "Catatan (opsional)" })).toHaveValue("Move savings");
    expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a");
    expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("cash-b");
    expect(screen.queryByRole("combobox", { name: "Kategori" })).not.toBeInTheDocument();
    expect(screen.queryByRole("combobox", { name: "Kaitkan ke target (opsional)" })).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Bukti transaksi (opsional)")).not.toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    expect((await axe(container.ownerDocument.body)).violations).toHaveLength(0);

    const dateInput = screen.getByLabelText("Waktu Pemindahan");
    expect(dateInput).toHaveAttribute("step", "1");
    fireEvent.change(dateInput, { target: { value: "2026-09-20T12:00:27" } });
    await user.click(screen.getByRole("button", { name: "Pindahkan Saldo" }));

    await waitFor(() => expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "cash-a",
      target_account_id: "cash-b",
      amount: 15_000,
      notes: "Move savings",
    })));
    const payload = vi.mocked(createMovement).mock.calls[0][0];
    expect(new Date(payload.date!).toISOString()).toBe(new Date("2026-09-20T12:00:27").toISOString());
    expect(api.post).not.toHaveBeenCalled();
    expect(uploadTransactionReceipt).not.toHaveBeenCalled();
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("focuses and explains the destination when fewer than two liquid accounts exist", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") {
        return { accounts: [
          { id: "cash-a", name: "Cash A", type: "cash", balance: 100_000 },
          { id: "stock", name: "Stock", type: "investment", balance: 500_000 },
        ] };
      }
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    renderQuickCapture({ defaultType: "movement" });

    await user.type(await screen.findByRole("textbox", { name: "Nominal (IDR)" }), "1000");
    const targetSelect = screen.getByRole("combobox", { name: "Ke rekening" });
    expect(targetSelect).toHaveValue("");
    expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Pindahkan Saldo" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Pilih rekening tujuan");
    expect(targetSelect).toHaveFocus();
    expect(createMovement).not.toHaveBeenCalled();
  });

  it("rejects a zero amount without creating a movement", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") {
        return { accounts: [
          { id: "cash-a", name: "Cash A", type: "cash", balance: 100_000 },
          { id: "cash-b", name: "Bank B", type: "bank", balance: 0 },
          { id: "stock", name: "Stock", type: "investment", balance: 500_000 },
        ] };
      }
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    renderQuickCapture({ defaultType: "movement" });

    await waitFor(() => expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a"));
    const amountInput = screen.getByRole("textbox", { name: "Nominal (IDR)" });
    fireEvent.submit(document.querySelector("form")!);

    expect(await screen.findByRole("alert")).toHaveTextContent("Masukkan nominal yang valid");
    expect(amountInput).toHaveFocus();
    expect(createMovement).not.toHaveBeenCalled();
    expect(api.post).not.toHaveBeenCalled();
  });

  it("retains values and focuses the account after a financial rejection", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValueOnce(new Error("Saldo tidak mencukupi."));
    renderQuickCapture();
    const accountSelect = await screen.findByRole("combobox", { name: "Bayar dari" });
    await user.selectOptions(accountSelect, "account-1");
    const amountInput = screen.getByRole("textbox", { name: "Nominal (IDR)" });
    await user.type(amountInput, "30000");
    fireEvent.submit(document.querySelector("form")!);

    expect(await screen.findByRole("alert")).toHaveTextContent("Saldo tidak mencukupi.");
    expect(amountInput).toHaveValue("30.000");
    expect(accountSelect).toHaveFocus();
  });

  it("does not open the mobile sheet with the numeric keyboard forced", async () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: false }));
    renderQuickCapture();

    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Tutup" })).toHaveFocus();
    expect(screen.getByRole("textbox", { name: "Nominal (IDR)" })).not.toHaveFocus();
    expect(screen.getByLabelText("Bukti transaksi (opsional)")).toBeInTheDocument();
  });
});
