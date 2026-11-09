import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api, createMovement, uploadTransactionReceipt } from "@/lib/api";
import { ToastProvider } from "@/components/workspace/overlays";
import CaptureDialog from "./CaptureDialog";
import type { CaptureMode } from "./types";

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: { get: vi.fn(), post: vi.fn() },
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
}));
const appCtx = { hideBalances: false, bal: (amount: number) => (appCtx.hideBalances ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`) };
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => appCtx }));

type Defaults = { mode?: CaptureMode; sourceAccountId?: string; targetAccountId?: string; amount?: number };

function renderCapture({ mode = "expense", ...rest }: Defaults = {}) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const onClose = vi.fn();
  const view = render(
    <QueryClientProvider client={queryClient}>
      <ToastProvider>
        <CaptureDialog request={{ view: "manual", mode, ...rest }} onClose={onClose} />
      </ToastProvider>
    </QueryClientProvider>,
  );
  return { ...view, onClose, user: userEvent.setup() };
}

const amountField = () => screen.getByRole("textbox", { name: /^(Nominal|Total bayar) \(IDR\)$/ });
const openDetails = async (user: ReturnType<typeof userEvent.setup>) => {
  const details = document.querySelector("details.capture-details") as HTMLDetailsElement;
  if (!details.open) await user.click(within(details).getByText(/^Detail tambahan/));
};
const submitButton = (name: RegExp | string) => screen.getByRole("button", { name });

afterEach(() => {
  vi.unstubAllGlobals();
  appCtx.hideBalances = false;
});

describe("Catat transaksi (manual view)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 100_000 }] };
      if (path === "/categories") {
        return {
          categories: [
            { id: "want-category", name: "Hobi", kind: "expense", kakeibo_type: "want" },
            { id: "need-category", name: "Makan", kind: "expense", kakeibo_type: "need" },
            { id: "culture-category", name: "Olahraga", kind: "expense", kakeibo_type: "culture" },
            { id: "income-category", name: "Gaji", kind: "income" },
          ],
        };
      }
      if (path === "/goals") return { goals: [{ id: "goal-1", name: "Dana Darurat", is_archived: false }] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true, transaction_id: "transaction-1" });
    vi.mocked(createMovement).mockResolvedValue({ ok: true, movement_id: "movement-1", expense_transaction_id: "movement-out", income_transaction_id: "movement-in" });
    vi.mocked(uploadTransactionReceipt).mockResolvedValue({ ok: true, receipt_path: "receipt.jpg" });
  });

  it("inherits category classification until the user deliberately overrides it", async () => {
    const { user, onClose } = renderCapture();
    const categorySelect = await screen.findByRole("combobox", { name: "Kategori" });
    await screen.findByRole("option", { name: "Hobi" });
    expect(screen.queryByRole("option", { name: "Gaji" })).not.toBeInTheDocument();
    expect(within(categorySelect).getByRole("group", { name: "Keinginan" })).toBeInTheDocument();
    await user.selectOptions(categorySelect, "want-category");
    expect(screen.getByRole("radio", { name: "Keinginan" })).toHaveAttribute("aria-checked", "true");

    await user.click(screen.getByRole("radio", { name: "Kebutuhan" }));
    await user.selectOptions(categorySelect, "need-category");
    expect(screen.getByRole("radio", { name: "Kebutuhan" })).toHaveAttribute("aria-checked", "true");

    await user.type(amountField(), "15000");
    await openDetails(user);
    await user.type(screen.getByRole("textbox", { name: "Catatan (opsional)" }), "Lunch");
    await user.keyboard("{Enter}");

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      account_id: "account-1", amount: 15_000, category_id: "need-category", kakeibo_type: "need", notes: "Lunch",
    })));
    const body = vi.mocked(api.post).mock.calls[0][1] as Record<string, unknown>;
    expect(Object.keys(body).sort()).toEqual(["account_id", "amount", "category_id", "date", "goal_id", "kakeibo_type", "notes", "obligation_id", "type"]);
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("follows a culture category default and offers every Kakeibo pillar with its hint", async () => {
    const { user } = renderCapture();
    await screen.findByRole("option", { name: "Olahraga" });
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori" }), "culture-category");
    for (const label of ["Kebutuhan", "Keinginan", "Budaya", "Tak Terduga", "Tabungan"]) {
      expect(screen.getByRole("radio", { name: label })).toBeInTheDocument();
    }
    expect(screen.getByRole("radio", { name: "Budaya" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByText(/Pengembangan diri dan relasi/)).toBeVisible();
  });

  it("submits an unexpected pillar chosen by the user", async () => {
    const { user } = renderCapture();
    await screen.findByRole("option", { name: "Makan" });
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori" }), "need-category");
    await user.click(screen.getByRole("radio", { name: "Tak Terduga" }));
    await user.type(amountField(), "250000");
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({ category_id: "need-category", kakeibo_type: "unexpected" })));
  });

  it("submits the selected transaction time with seconds", async () => {
    const { user, onClose } = renderCapture();
    await screen.findByRole("option", { name: "Makan" });
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori" }), "need-category");
    await user.type(amountField(), "1000");
    await openDetails(user);
    const date = screen.getByLabelText("Waktu transaksi");
    expect(date).toHaveAttribute("step", "1");
    fireEvent.change(date, { target: { value: "2026-09-20T12:00:27" } });
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    const payload = vi.mocked(api.post).mock.calls[0][1] as { date: string };
    expect(new Date(payload.date).toISOString()).toBe(new Date("2026-09-20T12:00:27").toISOString());
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("evaluates inline arithmetic and shows the result only while an operator is typed", async () => {
    const { user } = renderCapture();
    await screen.findByRole("option", { name: "Makan" });
    await user.type(amountField(), "35000");
    expect(amountField()).toHaveValue("35.000");
    expect(screen.queryByText(/^= /)).not.toBeInTheDocument();
    await user.clear(amountField());
    await user.type(amountField(), "25000+15000");
    expect(screen.getByText("= Rp 40.000")).toBeInTheDocument();
    expect(amountField()).toHaveAccessibleDescription("= Rp 40.000");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({ amount: 40_000 })));
  });

  it("adds Tambah cepat amounts and has no Reset chip", async () => {
    const { user } = renderCapture();
    await screen.findByRole("option", { name: "Makan" });
    const chips = screen.getByRole("group", { name: "Tambah cepat" });
    expect(within(chips).getAllByRole("button").map((chip) => chip.textContent)).toEqual(["+10k", "+20k", "+50k", "+100k", "+500k"]);
    await user.click(within(chips).getByRole("button", { name: "+50k" }));
    await user.click(within(chips).getByRole("button", { name: "+10k" }));
    expect(amountField()).toHaveValue("60.000");
    expect(screen.queryByRole("button", { name: "Reset" })).not.toBeInTheDocument();
  });

  it("asks for the amount under its field when Enter is pressed without one", async () => {
    const { user } = renderCapture();
    await screen.findByRole("option", { name: "Makan" });
    fireEvent.submit(document.getElementById(amountField().closest("form")!.id)!);
    expect(await screen.findByText("Isi nominalnya dulu.")).toBeInTheDocument();
    expect(amountField()).toHaveAttribute("aria-invalid", "true");
    await waitFor(() => expect(amountField()).toHaveFocus());
    expect(api.post).not.toHaveBeenCalled();
    expect(submitButton("Simpan transaksi")).toBeDisabled();
    void user;
  });

  it("filters categories after switching transaction type and keeps the form accessible", async () => {
    const { user, baseElement } = renderCapture();
    await screen.findByRole("option", { name: "Makan" });
    await user.click(screen.getByRole("radio", { name: "Pemasukan" }));
    const categorySelect = screen.getByRole("combobox", { name: "Kategori pemasukan" });
    expect(screen.getByRole("option", { name: "Gaji" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Hobi" })).not.toBeInTheDocument();
    expect(screen.queryByText("Pilar Kakeibo")).not.toBeInTheDocument();
    expect(categorySelect).toHaveValue("");
    expect((await axe(baseElement)).violations).toHaveLength(0);
  });

  it("treats goal linkage as metadata without changing the chosen classification", async () => {
    const { user, onClose } = renderCapture();
    await screen.findByRole("option", { name: "Hobi" });
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori" }), "want-category");
    await openDetails(user);
    const goal = screen.getByRole("combobox", { name: "Kaitkan ke target (opsional)" });
    expect(within(goal).getByRole("option", { name: "Dana Darurat" })).toHaveTextContent(/^Dana Darurat$/);
    expect(goal).toHaveAccessibleDescription("Kaitan ini tidak menambah progres target. Target yang terhubung ke rekening mengikuti saldonya.");
    await user.selectOptions(goal, "goal-1");
    expect(screen.getByRole("radio", { name: "Keinginan" })).toHaveAttribute("aria-checked", "true");
    await user.type(amountField(), "10000");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({ goal_id: "goal-1", kakeibo_type: "want", notes: null })));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  describe("debt splits", () => {
    beforeEach(() => {
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
    });

    const addTwoDebts = async (user: ReturnType<typeof userEvent.setup>) => {
      await openDetails(user);
      await user.click(await screen.findByRole("button", { name: "+ Tambah tagihan" }));
      await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "debt-1");
      await user.click(screen.getByRole("button", { name: "+ Tambah tagihan" }));
      await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 2" }), "debt-2");
    };

    it("derives a partial multi-debt payment from edited debt amounts", async () => {
      const { user, onClose } = renderCapture();
      await screen.findByRole("option", { name: /BCA/ });
      await addTwoDebts(user);
      expect(amountField()).toHaveValue("150.000");
      expect(screen.queryByRole("group", { name: "Tambah cepat" })).not.toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
      await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }));
      await user.click(screen.getByRole("button", { name: "Selesai ubah jumlah" }));
      await user.click(submitButton("Simpan transaksi"));
      expect(await screen.findByText("Isi nominal positif untuk setiap tagihan.")).toBeInTheDocument();
      await waitFor(() => expect(screen.getByRole("button", { name: "Ubah jumlah" })).toHaveFocus());
      expect(api.post).not.toHaveBeenCalled();
      await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
      await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }), "60000");
      await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" }));
      await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 2 (IDR)" }), "40000");
      expect(amountField()).toHaveValue("100.000");
      await user.click(submitButton("Simpan transaksi"));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
        amount: 100_000, obligation_id: null,
        obligation_allocations: [{ obligation_id: "debt-1", amount: 60_000 }, { obligation_id: "debt-2", amount: 40_000 }],
      })));
      await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    });

    it("records selected debts at full outstanding amounts without extra inputs", async () => {
      const { user, onClose } = renderCapture();
      await screen.findByRole("option", { name: /BCA/ });
      await addTwoDebts(user);
      expect(screen.getByRole("textbox", { name: "Total bayar (IDR)" })).toHaveValue("150.000");
      expect(screen.queryByRole("textbox", { name: /Dibayar untuk tagihan/ })).not.toBeInTheDocument();
      await user.click(submitButton("Simpan transaksi"));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
        amount: 150_000, obligation_id: null,
        obligation_allocations: [{ obligation_id: "debt-1", amount: 80_000 }, { obligation_id: "debt-2", amount: 70_000 }],
      })));
      await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    });

    it("keeps a single debt compatible with a partial legacy payment", async () => {
      const { user, onClose } = renderCapture();
      await screen.findByRole("option", { name: /BCA/ });
      await openDetails(user);
      await user.click(await screen.findByRole("button", { name: "+ Tambah tagihan" }));
      await user.selectOptions(screen.getByRole("combobox", { name: "Tagihan 1" }), "debt-1");
      expect(amountField()).toHaveValue("80.000");
      await user.click(screen.getByRole("button", { name: "Ubah jumlah" }));
      await user.clear(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }));
      await user.type(screen.getByRole("textbox", { name: "Dibayar untuk tagihan 1 (IDR)" }), "30000");
      expect(amountField()).toHaveValue("30.000");
      await user.click(submitButton("Simpan transaksi"));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({ amount: 30_000, obligation_id: "debt-1" })));
      await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    });

    it("retries an allocated payment receipt without submitting the payment twice", async () => {
      vi.mocked(uploadTransactionReceipt).mockRejectedValueOnce(new Error("Bukti gagal")).mockResolvedValueOnce({ ok: true, receipt_path: "receipt.jpg" });
      const { user, onClose } = renderCapture();
      await screen.findByRole("option", { name: /BCA/ });
      await addTwoDebts(user);
      await user.upload(screen.getByLabelText("Bukti transaksi (opsional)"), new File(["receipt"], "receipt.jpg", { type: "image/jpeg" }));
      await user.click(submitButton("Simpan transaksi"));
      expect(await screen.findByRole("alert")).toHaveTextContent("Transaksi tersimpan, tetapi bukti belum terunggah: Bukti gagal");
      await user.click(submitButton("Coba unggah lagi"));
      await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledTimes(2));
      expect(api.post).toHaveBeenCalledTimes(1);
      await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    });
  });

  it("keeps a created transaction and retries a failed receipt without duplicating it", async () => {
    vi.mocked(uploadTransactionReceipt).mockRejectedValueOnce(new Error("Bukti tidak valid")).mockResolvedValueOnce({ ok: true, receipt_path: "replacement.jpg" });
    const { user, onClose } = renderCapture();
    const accountSelect = await screen.findByRole("combobox", { name: "Bayar dari" });
    await screen.findByRole("option", { name: /BCA/ });
    await user.selectOptions(accountSelect, "account-1");
    await user.type(amountField(), "25000");
    await openDetails(user);
    const receiptInput = screen.getByLabelText("Bukti transaksi (opsional)");
    expect(receiptInput).toHaveAccessibleDescription("JPG, PNG, WebP atau PDF, paling besar 5 MB. Transaksi tetap tersimpan jika unggahan gagal.");
    await user.upload(receiptInput, new File(["first"], "receipt.jpg", { type: "image/jpeg" }));
    await user.click(submitButton("Simpan transaksi"));
    expect(await screen.findByRole("alert")).toHaveTextContent("Transaksi tersimpan, tetapi bukti belum terunggah");
    expect(api.post).toHaveBeenCalledTimes(1);
    await user.upload(receiptInput, new File(["replacement"], "replacement.jpg", { type: "image/jpeg" }));
    await user.click(submitButton("Coba unggah lagi"));
    await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledTimes(2));
    expect(api.post).toHaveBeenCalledTimes(1);
    expect(uploadTransactionReceipt).toHaveBeenLastCalledWith("transaction-1", expect.objectContaining({ name: "replacement.jpg" }));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("attaches a valid receipt to the newly created transaction", async () => {
    const { user, onClose } = renderCapture();
    await screen.findByRole("option", { name: /BCA/ });
    await user.type(amountField(), "12000");
    await openDetails(user);
    await user.upload(screen.getByLabelText("Bukti transaksi (opsional)"), new File(["receipt"], "receipt.jpg", { type: "image/jpeg" }));
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(uploadTransactionReceipt).toHaveBeenCalledWith("transaction-1", expect.objectContaining({ name: "receipt.jpg" })));
    expect(api.post).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("shows the save confirmation without the zoom animation, then closes", async () => {
    const { user, onClose } = renderCapture();
    await screen.findByRole("option", { name: /BCA/ });
    await user.type(amountField(), "12000");
    await user.click(submitButton("Simpan transaksi"));
    const saved = await screen.findByRole("button", { name: "✓ Tersimpan!" });
    expect(saved).toBeDisabled();
    expect(saved.querySelector("[class*='zoom']")).toBeNull();
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  describe("Perpindahan", () => {
    beforeEach(() => {
      vi.mocked(api.get).mockImplementation(async (path) => {
        if (path === "/accounts") return { accounts: [
          { id: "cash-a", name: "Cash A", type: "cash", balance: 100_000 },
          { id: "cash-b", name: "Bank B", type: "bank", balance: 0 },
          { id: "stock", name: "Stock", type: "investment", balance: 500_000 },
        ] };
        if (path === "/categories") return { categories: [{ id: "need-category", name: "Makan", kind: "expense", kakeibo_type: "need" }] };
        if (path === "/goals") return { goals: [{ id: "goal-1", name: "Dana Darurat", is_archived: false }] };
        if (path === "/obligations") return { obligations: [] };
        throw new Error(`Unexpected GET ${path}`);
      });
    });

    it("creates an internal movement from the shared form without transaction-only fields", async () => {
      const { user, onClose, baseElement } = renderCapture();
      await screen.findByRole("option", { name: "Makan" });
      await user.selectOptions(screen.getByRole("combobox", { name: "Kategori" }), "need-category");
      await openDetails(user);
      await user.selectOptions(screen.getByRole("combobox", { name: "Kaitkan ke target (opsional)" }), "goal-1");
      await user.type(amountField(), "15000");
      await user.type(screen.getByRole("textbox", { name: "Catatan (opsional)" }), "Move savings");
      await user.click(screen.getByRole("radio", { name: "Perpindahan" }));

      expect(amountField()).toHaveValue("15.000");
      expect(screen.getByRole("textbox", { name: "Catatan (opsional)" })).toHaveValue("Move savings");
      expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a");
      expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("cash-b");
      expect(screen.queryByRole("combobox", { name: "Kategori" })).not.toBeInTheDocument();
      expect(screen.queryByRole("combobox", { name: "Kaitkan ke target (opsional)" })).not.toBeInTheDocument();
      expect(screen.queryByLabelText("Bukti transaksi (opsional)")).not.toBeInTheDocument();
      expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
      expect((await axe(baseElement)).violations).toHaveLength(0);

      const dateInput = screen.getByLabelText("Waktu pemindahan");
      expect(dateInput).toHaveAttribute("step", "1");
      fireEvent.change(dateInput, { target: { value: "2026-09-20T12:00:27" } });
      await user.click(submitButton("Pindahkan saldo"));

      await waitFor(() => expect(createMovement).toHaveBeenCalled());
      const payload = vi.mocked(createMovement).mock.calls[0][0];
      expect(Object.keys(payload).sort()).toEqual(["amount", "date", "notes", "source_account_id", "target_account_id"]);
      expect(payload).toMatchObject({ source_account_id: "cash-a", target_account_id: "cash-b", amount: 15_000, notes: "Move savings" });
      expect(new Date(payload.date!).toISOString()).toBe(new Date("2026-09-20T12:00:27").toISOString());
      expect(api.post).not.toHaveBeenCalled();
      expect(uploadTransactionReceipt).not.toHaveBeenCalled();
      expect(await screen.findByRole("button", { name: "✓ Berhasil dipindahkan" })).toBeInTheDocument();
      await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    });

    it("preselects a Ke the server accepts when the first account is a parent with its default pocket next (D13)", async () => {
      vi.mocked(api.get).mockImplementation(async (path) => {
        if (path === "/accounts") return { accounts: [
          { id: "bca", name: "BCA", type: "bank", balance: 0, default_pocket_id: "utama", children: [
            { id: "utama", name: "Utama", type: "bank", parent_id: "bca", balance: 300_000 },
            { id: "harian", name: "Harian", type: "bank", parent_id: "bca", balance: 50_000 },
          ] },
          { id: "utama", name: "Utama", type: "bank", parent_id: "bca", balance: 300_000 },
          { id: "harian", name: "Harian", type: "bank", parent_id: "bca", balance: 50_000 },
          { id: "jago", name: "Jago", type: "bank", balance: 10_000 },
        ] };
        if (path === "/categories" || path === "/goals" || path === "/obligations") return { categories: [], goals: [], obligations: [] };
        throw new Error(`Unexpected GET ${path}`);
      });
      const { user } = renderCapture({ mode: "movement" });
      await waitFor(() => expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("bca"));
      expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("harian");
      await user.type(amountField(), "25000");
      await user.click(submitButton("Pindahkan saldo"));
      await waitFor(() => expect(createMovement).toHaveBeenCalled());
      expect(vi.mocked(createMovement).mock.calls.at(-1)![0]).toMatchObject({ source_account_id: "bca", target_account_id: "harian", amount: 25_000 });
    });

    it("rejects a zero amount without creating a movement", async () => {
      renderCapture({ mode: "movement" });
      await waitFor(() => expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a"));
      fireEvent.submit(amountField().closest("form")!);
      expect(await screen.findByText("Isi nominalnya dulu.")).toBeInTheDocument();
      await waitFor(() => expect(amountField()).toHaveFocus());
      expect(createMovement).not.toHaveBeenCalled();
      expect(api.post).not.toHaveBeenCalled();
    });

    it("stacks Dari and Ke with a direction arrow and labels both", async () => {
      renderCapture({ mode: "movement" });
      const direction = await screen.findByRole("group", { name: "Arah perpindahan" });
      expect(within(direction).getAllByRole("combobox").map((select) => select.getAttribute("aria-label"))).toEqual(["Dari rekening", "Ke rekening"]);
      expect(direction.querySelector(".capture-direction-arrow")).toHaveAttribute("aria-hidden", "true");
    });
  });

  it("focuses and explains the destination when fewer than two liquid accounts exist", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [
        { id: "cash-a", name: "Cash A", type: "cash", balance: 100_000 },
        { id: "stock", name: "Stock", type: "investment", balance: 500_000 },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const { user } = renderCapture({ mode: "movement" });
    await user.type(amountField(), "1000");
    const targetSelect = await screen.findByRole("combobox", { name: "Ke rekening" });
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a"));
    expect(targetSelect).toHaveValue("");
    expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    await user.click(submitButton("Pindahkan saldo"));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pilih rekening tujuan");
    await waitFor(() => expect(targetSelect).toHaveFocus());
    expect(targetSelect).toHaveAttribute("aria-invalid", "true");
    expect(createMovement).not.toHaveBeenCalled();
  });

  it("retains values and focuses the account after a financial rejection", async () => {
    vi.mocked(api.post).mockRejectedValueOnce(new Error("Saldo tidak mencukupi."));
    const { user } = renderCapture();
    const accountSelect = await screen.findByRole("combobox", { name: "Bayar dari" });
    await screen.findByRole("option", { name: /BCA/ });
    await user.selectOptions(accountSelect, "account-1");
    await user.type(amountField(), "30000");
    await user.click(submitButton("Simpan transaksi"));
    expect(await screen.findByRole("alert")).toHaveTextContent("Saldo tidak mencukupi.");
    expect(amountField()).toHaveValue("30.000");
    expect(accountSelect).toHaveFocus();
  });

  it("puts focus on the title of the phone sheet, not on the numeric field", async () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    renderCapture();
    const dialog = await screen.findByRole("dialog", { name: "Catat transaksi" });
    expect(within(dialog).getByRole("heading", { name: "Catat transaksi" })).toHaveFocus();
    expect(amountField()).not.toHaveFocus();
  });

  it("puts focus on the amount from 768 px", async () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    renderCapture();
    await waitFor(() => expect(amountField()).toHaveFocus());
  });

  it("offers the offset category only for income that repays spending", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 100_000 }] };
      if (path === "/categories") {
        return { categories: [
          { id: "dinner", name: "Makan Bareng", kind: "expense", kakeibo_type: "want" },
          { id: "movement", name: "Internal Movement", kind: "expense", is_excluded_from_budget: true },
          { id: "old", name: "Lama", kind: "expense", is_archived: true },
          { id: "salary", name: "Gaji", kind: "income" },
          { id: "repayment", name: "Patungan & Ganti Uang", kind: "income", offsets_spending: true },
        ] };
      }
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    const { user, baseElement } = renderCapture({ mode: "income" });
    const categorySelect = await screen.findByRole("combobox", { name: "Kategori pemasukan" });
    await screen.findByRole("option", { name: "Gaji" });
    await user.selectOptions(categorySelect, "salary");
    expect(screen.queryByRole("combobox", { name: "Mengurangi pengeluaran" })).not.toBeInTheDocument();
    await user.selectOptions(categorySelect, "repayment");
    const offset = screen.getByRole("combobox", { name: "Mengurangi pengeluaran" });
    expect(Array.from((offset as HTMLSelectElement).options).map((option) => option.textContent)).toEqual(["Belum dipilih", "Makan Bareng"]);
    expect(offset).toHaveAccessibleDescription("Uang ganti ini bukan pemasukan. Pilih kategori pengeluaran yang dibayar kembali.");
    expect((await axe(baseElement)).violations).toHaveLength(0);
    await user.selectOptions(offset, "dinner");
    await user.type(amountField(), "200000");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      type: "income", category_id: "repayment", offset_category_id: "dinner", kakeibo_type: null,
    })));
  });

  it("does not send an offset for ordinary income", async () => {
    const { user } = renderCapture({ mode: "income" });
    await screen.findByRole("option", { name: "Gaji" });
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori pemasukan" }), "income-category");
    await user.type(amountField(), "5000000");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("offset_category_id");
  });

  it("lists accounts without balances while balances are hidden", async () => {
    appCtx.hideBalances = true;
    renderCapture();
    const option = await screen.findByRole("option", { name: "BCA" });
    expect(option.textContent).toBe("BCA");
  });

  it("has no information control: Pilar, target and receipt each carry one hint line", async () => {
    const { user } = renderCapture();
    await screen.findByRole("option", { name: /BCA/ });
    await openDetails(user);
    expect(screen.queryByRole("button", { name: /^Info / })).not.toBeInTheDocument();
    expect(screen.getByRole("radiogroup", { name: "Pilar Kakeibo" })).toBeInTheDocument();
    expect(screen.getByText(/Pokok sehari-hari/)).toHaveClass("hint");
    expect(document.querySelectorAll(".capture-manual .hint").length).toBeGreaterThanOrEqual(3);
    expect(document.body.textContent).not.toContain("🎯");
  });
});

describe("Catat transaksi with credit accounts (H11)", () => {
  const bank = { id: "jago", name: "Jago", type: "bank", balance: 500_000, children: [
    { id: "kantong", name: "Kantong Harian", type: "bank", parent_id: "jago", balance: 500_000 },
  ] };
  const pocket = bank.children[0];
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
  const spaylater = {
    id: "spaylater", name: "SPayLater", type: "credit", balance: -1_314_000, credit_owed: 1_314_000, credit_limit: 3_000_000,
    limit_left: 1_686_000, over_limit: false, statement_day: 1, payment_day: 11,
    next_due: { due_date: today, amount: 434_000, left_to_pay: 434_000, status: "due_today" },
  };
  const kredivo = { id: "kredivo", name: "Kredivo", type: "credit", balance: 0, credit_owed: 0, credit_limit: null, over_limit: false,
    statement_day: null, payment_day: null, next_due: null };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: [bank, pocket, spaylater, kredivo] };
      if (path === "/categories") return { categories: [{ id: "makan", name: "Makan", kind: "expense", kakeibo_type: "need" }, { id: "refund", name: "Refund", kind: "income" }] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [{ id: "debt-1", name: "Cicilan Contoh", remaining_amount: 900_000, is_archived: false }] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true, transaction_id: "purchase" });
    vi.mocked(createMovement).mockResolvedValue({ ok: true, movement_id: "m1", expense_transaction_id: "out", income_transaction_id: "in" });
  });

  const setTime = (value: string) => fireEvent.change(screen.getByLabelText("Waktu transaksi"), { target: { value } });

  const chooseSpaylater = async (user: ReturnType<typeof userEvent.setup>) => {
    const account = await screen.findByRole("combobox", { name: "Bayar dari" });
    await within(account).findByRole("group", { name: "Kredit" });
    await user.selectOptions(account, "spaylater");
    return account;
  };

  it("books an expense on credit from a Kredit group, without a debt link, and tells when it passes the limit", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, transaction_id: "dinner", over_limit: true });
    const { user, baseElement } = renderCapture();
    const account = await screen.findByRole("combobox", { name: "Bayar dari" });
    const group = await within(account).findByRole("group", { name: "Kredit" });
    expect(within(group).getAllByRole("option").map((option) => option.textContent)).toEqual(["SPayLater (utang Rp 1.314.000)", "Kredivo"]);
    await openDetails(user);
    expect(screen.getByRole("button", { name: "+ Tambah tagihan" })).toBeInTheDocument();
    await user.selectOptions(account, "spaylater");
    expect(screen.queryByRole("button", { name: "+ Tambah tagihan" })).not.toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Kategori" }), "makan");
    await user.type(amountField(), "120000");
    expect((await axe(baseElement)).violations).toHaveLength(0);
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      type: "expense", account_id: "spaylater", amount: 120_000, category_id: "makan", obligation_id: null, credit_terms: { installment_count: 1 },
    })));
    expect(await screen.findByText("Melewati limit SPayLater")).toBeInTheDocument();
  });

  it("Choosing instalments: limit left, the first due date and 6x in the request", async () => {
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await openDetails(user);
    setTime("2026-11-05T12:00:00");
    const credit = screen.getByRole("group", { name: /Bayar pakai kredit/ });
    expect(within(credit).getByText("Sisa limit Rp 1.686.000")).toBeInTheDocument();
    expect(within(credit).getByLabelText("Jatuh tempo pertama")).toHaveValue("2026-12-11");
    await user.type(amountField(), "1200000");
    await user.click(within(credit).getByRole("radio", { name: "6x" }));
    expect(within(credit).getByText("dibayar nanti · 6 cicilan mulai 11 Des · Rp 200.000 per cicilan · tanpa bunga")).toBeInTheDocument();
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(vi.mocked(api.post).mock.calls[0][1]).toMatchObject({ credit_terms: { installment_count: 6 } });
    expect((vi.mocked(api.post).mock.calls[0][1] as { credit_terms: object }).credit_terms).not.toHaveProperty("first_due_date");
  });

  it("Pay later line and the instalment line with a remainder", async () => {
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await openDetails(user);
    setTime("2026-11-05T12:00:00");
    const credit = screen.getByRole("group", { name: /Bayar pakai kredit/ });
    await user.type(amountField(), "120000");
    expect(within(credit).getByText("dibayar nanti · SPayLater · jatuh tempo 11 Des")).toBeInTheDocument();
    await user.clear(amountField());
    await user.type(amountField(), "1000000");
    await user.click(within(credit).getByRole("radio", { name: "3x" }));
    expect(within(credit).getByText("dibayar nanti · 3 cicilan mulai 11 Des · Rp 333.334 lalu Rp 333.333 per cicilan · tanpa bunga")).toBeInTheDocument();
  });

  it("Other count: 18 is sent, 1 and 40 are refused in the form", async () => {
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await user.type(amountField(), "900000");
    const credit = screen.getByRole("group", { name: /Bayar pakai kredit/ });
    await user.click(within(credit).getByRole("radio", { name: "Lainnya" }));
    const count = screen.getByRole("spinbutton", { name: "Jumlah cicilan" });
    for (const refused of ["1", "40"]) {
      await user.clear(count);
      await user.type(count, refused);
      await user.click(submitButton("Simpan transaksi"));
      expect(await screen.findByText("Isi 2 sampai 36 cicilan.")).toBeInTheDocument();
      expect(api.post).not.toHaveBeenCalled();
    }
    await user.clear(count);
    await user.type(count, "18");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(vi.mocked(api.post).mock.calls[0][1]).toMatchObject({ credit_terms: { installment_count: 18 } });
  });

  it("Owner's date: sent when changed, refused before the purchase, and Pakai tanggal tagihan restores the bill date", async () => {
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await openDetails(user);
    setTime("2026-11-05T12:00:00");
    await user.type(amountField(), "120000");
    const due = screen.getByLabelText("Jatuh tempo pertama");
    fireEvent.change(due, { target: { value: "2026-11-01" } });
    await user.click(submitButton("Simpan transaksi"));
    expect(await screen.findByText("Jatuh tempo pertama tidak boleh sebelum tanggal transaksi.")).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Pakai tanggal tagihan" }));
    expect(due).toHaveValue("2026-12-11");
    fireEvent.change(due, { target: { value: "2026-12-01" } });
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(vi.mocked(api.post).mock.calls[0][1]).toMatchObject({ credit_terms: { installment_count: 1, first_due_date: "2026-12-01" } });
  });

  it("No payment day: the date starts empty with a hint, and nothing is sent without one", async () => {
    const { user } = renderCapture();
    const account = await screen.findByRole("combobox", { name: "Bayar dari" });
    await within(account).findByRole("group", { name: "Kredit" });
    await user.selectOptions(account, "kredivo");
    const due = screen.getByLabelText("Jatuh tempo pertama");
    expect(due).toHaveValue("");
    expect(due).toHaveAccessibleDescription(/Tanggal bayar rekening ini belum diatur/);
    expect(screen.getByText("dibayar nanti · Kredivo · jatuh tempo belum diatur")).toBeInTheDocument();
    expect(screen.queryByText(/Sisa limit/)).not.toBeInTheDocument();
    await user.type(amountField(), "50000");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect((vi.mocked(api.post).mock.calls[0][1] as { credit_terms: object }).credit_terms).toEqual({ installment_count: 1 });
  });

  it("Not a credit account: the credit choices disappear and no credit_terms is sent", async () => {
    const { user } = renderCapture();
    const account = await chooseSpaylater(user);
    expect(screen.getByRole("group", { name: /Bayar pakai kredit/ })).toBeInTheDocument();
    await user.selectOptions(account, "jago");
    expect(screen.queryByRole("group", { name: /Bayar pakai kredit/ })).not.toBeInTheDocument();
    await user.type(amountField(), "50000");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("credit_terms");
  });

  it("warns over the limit but still saves", async () => {
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await user.type(amountField(), "1806000");
    expect(screen.getByText("Melewati limit Rp 120.000")).toHaveClass("c-warning");
    await user.click(submitButton("Simpan transaksi"));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
  });

  it("hides the limit and the instalment amounts with Sembunyikan saldo", async () => {
    appCtx.hideBalances = true;
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await user.type(amountField(), "1000000");
    const credit = screen.getByRole("group", { name: /Bayar pakai kredit/ });
    await user.click(within(credit).getByRole("radio", { name: "3x" }));
    expect(within(credit).queryByText(/1\.686\.000|333\.33/)).not.toBeInTheDocument();
    expect(within(credit).getAllByRole("img", { name: "Disembunyikan" }).length).toBeGreaterThanOrEqual(3);
  });

  it("shows a refusal of the terms under the date field", async () => {
    const { ApiError } = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
    vi.mocked(api.post).mockRejectedValueOnce(new ApiError(422, { code: "credit_terms_due_before_purchase", message: "Jatuh tempo pertama tidak boleh sebelum tanggal pembelian, 5 Nov." }));
    const { user } = renderCapture();
    await chooseSpaylater(user);
    await user.type(amountField(), "120000");
    await user.click(submitButton("Simpan transaksi"));
    const error = await screen.findByText("Jatuh tempo pertama tidak boleh sebelum tanggal pembelian, 5 Nov.");
    expect(error.id).toBe("capture-credit-due-error");
    await waitFor(() => expect(screen.getByLabelText("Jatuh tempo pertama")).toHaveFocus());
  });

  it("does not offer credit accounts for an income", async () => {
    const { user } = renderCapture();
    await screen.findByRole("combobox", { name: "Bayar dari" });
    await user.click(screen.getByRole("radio", { name: "Pemasukan" }));
    const account = screen.getByRole("combobox", { name: "Masuk ke" });
    expect(within(account).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
    expect(within(account).queryByRole("option", { name: /SPayLater|Kredivo/ })).not.toBeInTheDocument();
  });

  it("Paying the amount left: the bill line, Bayar sisa tagihan and the fee", async () => {
    const { user, baseElement } = renderCapture({ mode: "movement" });
    const source = await screen.findByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(source).toHaveValue("jago"));
    expect(screen.queryByRole("button", { name: /Bayar sisa tagihan/ })).not.toBeInTheDocument();
    const target = screen.getByRole("combobox", { name: "Ke rekening" });
    expect(within(target).getByRole("group", { name: "Kredit" })).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Biaya / bunga (opsional)" })).not.toBeInTheDocument();
    await user.selectOptions(target, "spaylater");
    const dialog = screen.getByRole("dialog", { name: "Catat transaksi" });
    expect(within(dialog).getByText("Tagihan SPayLater Rp 434.000 jatuh tempo hari ini")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Bayar sisa tagihan Rp 434.000" }));
    expect(amountField()).toHaveValue("434.000");
    expect(createMovement).not.toHaveBeenCalled();
    const fee = screen.getByRole("textbox", { name: "Biaya / bunga (opsional)" });
    expect(fee).toHaveAccessibleDescription("Bagian dari nominal yang menjadi biaya atau bunga. Dicatat sebagai pengeluaran Biaya kredit.");
    await user.type(fee, "2500");
    expect(fee).toHaveValue("2.500");
    expect((await axe(baseElement)).violations).toHaveLength(0);
    await user.click(submitButton("Pindahkan saldo"));
    await waitFor(() => expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "jago", target_account_id: "spaylater", amount: 434_000, fee: 2_500,
    })));
  });

  it("Opened from a due bill: Ke and the amount filled, nothing sent until Pindahkan saldo", async () => {
    renderCapture({ mode: "movement", targetAccountId: "spaylater", amount: 434_000 });
    expect(await screen.findByRole("combobox", { name: "Ke rekening" })).toHaveValue("spaylater");
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("spaylater"));
    expect(amountField()).toHaveValue("434.000");
    expect(screen.getByRole("radio", { name: "Perpindahan" })).toHaveAttribute("aria-checked", "true");
    expect(createMovement).not.toHaveBeenCalled();
    expect(api.post).not.toHaveBeenCalled();
  });

  it("No open bill: no Bayar sisa tagihan and the dialog says so", async () => {
    const { user } = renderCapture({ mode: "movement" });
    const target = await screen.findByRole("combobox", { name: "Ke rekening" });
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("jago"));
    await user.selectOptions(target, "kredivo");
    expect(screen.getByText("Tidak ada tagihan berjalan untuk Kredivo.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Bayar sisa tagihan/ })).not.toBeInTheDocument();
  });

  it("sends no fee between cash accounts and refuses a fee above the amount", async () => {
    const { user } = renderCapture({ mode: "movement" });
    const source = await screen.findByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(source).toHaveValue("jago"));
    const target = screen.getByRole("combobox", { name: "Ke rekening" });
    await user.selectOptions(target, "spaylater");
    await user.type(amountField(), "5000");
    const fee = screen.getByRole("textbox", { name: "Biaya / bunga (opsional)" });
    await user.type(fee, "9000");
    await user.click(submitButton("Pindahkan saldo"));
    expect(await screen.findByText("Biaya tidak boleh melebihi nominal.")).toBeInTheDocument();
    await waitFor(() => expect(fee).toHaveFocus());
    expect(fee).toHaveAttribute("aria-invalid", "true");
    expect(createMovement).not.toHaveBeenCalled();
    await user.selectOptions(target, "kantong");
    expect(screen.queryByRole("textbox", { name: "Biaya / bunga (opsional)" })).not.toBeInTheDocument();
    await user.click(submitButton("Pindahkan saldo"));
    await waitFor(() => expect(createMovement).toHaveBeenCalledTimes(1));
    expect(vi.mocked(createMovement).mock.calls[0][0]).not.toHaveProperty("fee");
  });

  it("keeps a credit source from paying another credit account", async () => {
    const { user } = renderCapture({ mode: "movement" });
    const source = await screen.findByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(source).toHaveValue("jago"));
    await user.selectOptions(source, "spaylater");
    const target = screen.getByRole("combobox", { name: "Ke rekening" });
    expect(within(target).queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
    expect(within(target).queryByRole("option", { name: /Kredivo/ })).not.toBeInTheDocument();
    expect(target).not.toHaveValue("kredivo");
  });

  it("shows the server's reason when a movement with credit is refused", async () => {
    vi.mocked(createMovement).mockRejectedValue(new Error("Salah satu sisi pindah saldo harus rekening bank, e-wallet, atau tunai."));
    const { user } = renderCapture({ mode: "movement" });
    const source = await screen.findByRole("combobox", { name: "Dari rekening" });
    await waitFor(() => expect(source).toHaveValue("jago"));
    await user.selectOptions(screen.getByRole("combobox", { name: "Ke rekening" }), "spaylater");
    await user.type(amountField(), "470000");
    await user.click(submitButton("Pindahkan saldo"));
    expect(await screen.findByRole("alert")).toHaveTextContent("Salah satu sisi pindah saldo harus rekening bank, e-wallet, atau tunai.");
  });
});
