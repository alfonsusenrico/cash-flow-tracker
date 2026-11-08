import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { ToastProvider } from "@/components/workspace/overlays";
import { useAccountDialogs } from "./AccountDialogs";
import type { AccountItem } from "./types";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), put: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: false, bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const base = { initial_balance: 0, balance: 0, is_archived: false, children: [], created_at: "2026-09-01T00:00:00.000Z" };
const jago: AccountItem = { ...base, id: "jago", name: "Jago", type: "bank", balance: 100_000 };
const broker: AccountItem = { ...base, id: "broker", name: "Broker", type: "investment" };
const spaylater: AccountItem = {
  ...base, id: "spaylater", name: "SPayLater", type: "credit", initial_balance: -350_000, balance: -470_000,
  credit_limit: 2_000_000, payment_day: 11, statement_day: 1, credit_owed: 470_000, role: "utang",
};
const mainPocket: AccountItem = { ...base, id: "jago-main", parent_id: "jago", name: "Main Pocket", type: "bank", role: "harian" };
const fund: AccountItem = {
  ...base, id: "fund", parent_id: "broker", name: "RDPU", type: "investment", instrument_type: "mutual_fund",
  balance: 1_050_000, cost_basis: 1_000_000, role: "investasi",
};

/** Opens each form of the hook from a button, as the page and the detail do. */
function Harness() {
  const dialogs = useAccountDialogs();
  return (
    <>
      <button type="button" onClick={dialogs.openNewAccount}>open-new</button>
      <button type="button" onClick={() => dialogs.openEditAccount(spaylater)}>edit-credit</button>
      <button type="button" onClick={() => dialogs.openEditAccount(jago)}>edit-bank</button>
      <button type="button" onClick={() => dialogs.openEditAccount(mainPocket)}>edit-pocket</button>
      <button type="button" onClick={() => dialogs.openNewPocket(jago)}>new-pocket</button>
      <button type="button" onClick={() => dialogs.openValuation(fund)}>valuation</button>
      <button type="button" onClick={() => dialogs.openReconciliation(jago)}>reconcile</button>
      <button type="button" onClick={() => dialogs.syncPricesMutation.mutate()}>sync</button>
      <button type="button" onClick={() => dialogs.archiveMutation.mutate("jago")}>archive</button>
      {dialogs.dialogs}
    </>
  );
}

function renderDialogs(language: "id" | "en" = "id") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const user = userEvent.setup();
  renderWithIntl(
    <QueryClientProvider client={client}>
      <ToastProvider>
        <Harness />
      </ToastProvider>
    </QueryClientProvider>,
    { language },
  );
  return user;
}

async function open(user: ReturnType<typeof userEvent.setup>, opener: string, title: string) {
  await user.click(screen.getByRole("button", { name: opener }));
  return screen.findByRole("dialog", { name: title });
}

const lastBody = (method: "post" | "patch") => vi.mocked(api[method]).mock.calls.at(-1)![1] as Record<string, unknown>;

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path === "/accounts") return { ok: true, accounts: [jago, broker, spaylater], total_balance: 0 };
    return { ok: true, rules: [], results: [] };
  });
  vi.mocked(api.post).mockResolvedValue({ ok: true });
  vi.mocked(api.patch).mockResolvedValue({ ok: true });
  vi.mocked(api.del).mockResolvedValue({ ok: true });
});

describe("account form", () => {
  it("adds a bank account with exactly today's body and no credit fields", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    expect(within(dialog).getByRole("radio", { name: "Bank" })).toHaveAttribute("aria-checked", "true");
    expect(within(dialog).queryByRole("spinbutton", { name: /Jatuh tempo/ })).not.toBeInTheDocument();
    await user.type(within(dialog).getByRole("textbox", { name: "Nama" }), "Bank Contoh");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Kegunaan" }), "dana_investasi");
    await user.type(within(dialog).getByRole("textbox", { name: "Saldo awal (IDR)" }), "1500000");
    await user.click(within(dialog).getByRole("button", { name: "Tambah rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(vi.mocked(api.post).mock.calls[0][0]).toBe("/accounts");
    expect(lastBody("post")).toEqual({
      name: "Bank Contoh", type: "bank", role: "dana_investasi", initial_balance: 1_500_000, instrument_type: null,
      instrument_symbol: null, units: null, avg_buy_price: null, color: "#2563EB", default_funding_account_id: null,
    });
  });

  it("starts an investment platform lavender at zero and keeps a colour the owner picked", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    await user.click(within(dialog).getByRole("radio", { name: "Investasi" }));
    expect(within(dialog).getByRole("radio", { name: "Warna Lavender" })).toHaveAttribute("aria-checked", "true");
    expect(dialog).toHaveTextContent(/Kegunaan\s*Investasi\s*Ditentukan oleh jenis rekening\./);
    expect(within(dialog).getByText(/tambahkan kantong kas\/RDN/i)).toBeVisible();
    await user.click(within(dialog).getByRole("radio", { name: "Warna Teal" }));
    await user.click(within(dialog).getByRole("radio", { name: "Bank" }));
    expect(within(dialog).getByRole("radio", { name: "Warna Teal" })).toHaveAttribute("aria-checked", "true");
    await user.click(within(dialog).getByRole("radio", { name: "Investasi" }));
    await user.type(within(dialog).getByRole("textbox", { name: "Nama" }), "New broker");
    await user.click(within(dialog).getByRole("button", { name: "Tambah rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(lastBody("post")).toEqual({
      name: "New broker", type: "investment", role: "investasi", initial_balance: 0, instrument_type: null,
      instrument_symbol: null, units: null, avg_buy_price: null, color: "#0D9488", default_funding_account_id: null,
    });
  });

  it("moves the Jenis choice with the arrow keys", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    const bank = within(dialog).getByRole("radio", { name: "Bank" });
    bank.focus();
    await user.keyboard("{ArrowRight}");
    expect(within(dialog).getByRole("radio", { name: "E-wallet" })).toHaveAttribute("aria-checked", "true");
    expect(within(dialog).getByRole("radio", { name: "E-wallet" })).toHaveFocus();
  });

  it("refuses an empty name under the field and sends nothing", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    await user.click(within(dialog).getByRole("button", { name: "Tambah rekening" }));
    const name = within(dialog).getByRole("textbox", { name: "Nama" });
    expect(name).toHaveAttribute("aria-invalid", "true");
    expect(name).toHaveAccessibleDescription("Nama wajib diisi.");
    expect(name).toHaveFocus();
    expect(api.post).not.toHaveBeenCalled();
  });

  it("returns focus to the control that opened the form when it is cancelled", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    await user.click(within(dialog).getByRole("button", { name: "Batal" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "open-new" })).toHaveFocus());
  });
});

describe("credit days (H8)", () => {
  it("adds SPayLater with what it owes, a limit and both days, and says past bills keep their dates", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    await user.type(within(dialog).getByRole("textbox", { name: "Nama" }), "SPayLater");
    await user.click(within(dialog).getByRole("radio", { name: "Kredit" }));
    expect(within(dialog).queryByRole("textbox", { name: "Saldo awal (IDR)" })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("radio", { name: "Warna Slate" })).toHaveAttribute("aria-checked", "true");
    expect(dialog).toHaveTextContent("Tagihan yang sudah terbit tetap memakai tanggal jatuh temponya.");
    await user.type(within(dialog).getByRole("textbox", { name: "Utang saat ini (IDR)" }), "350000");
    await user.type(within(dialog).getByRole("textbox", { name: "Limit (opsional)" }), "2000000");
    const statement = within(dialog).getByRole("spinbutton", { name: "Tagihan terbit tiap tgl (opsional)" });
    expect(statement).toHaveAccessibleDescription("Tagihan yang sudah terbit tetap memakai tanggal jatuh temponya.");
    await user.type(statement, "25");
    await user.type(within(dialog).getByRole("spinbutton", { name: "Jatuh tempo tiap tgl (opsional)" }), "5");
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Tambah rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(lastBody("post")).toEqual({
      name: "SPayLater", type: "credit", role: "utang", initial_balance: -350_000, instrument_type: null, instrument_symbol: null,
      units: null, avg_buy_price: null, color: "#475569", default_funding_account_id: null,
      credit_limit: 2_000_000, payment_day: 5, statement_day: 25,
    });
  });

  it("refuses day 32 under its field, sends nothing, then opens at zero with empty days", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "open-new", "Tambah rekening");
    await user.type(within(dialog).getByRole("textbox", { name: "Nama" }), "Kredivo");
    await user.click(within(dialog).getByRole("radio", { name: "Kredit" }));
    const day = within(dialog).getByRole("spinbutton", { name: "Jatuh tempo tiap tgl (opsional)" });
    await user.type(day, "32");
    fireEvent.submit(dialog.querySelector("form")!);
    await waitFor(() => expect(day).toHaveAttribute("aria-invalid", "true"));
    expect(day).toHaveAccessibleDescription(/^Isi tanggal 1 sampai 31\./);
    expect(day).toHaveFocus();
    expect(api.post).not.toHaveBeenCalled();

    await user.clear(day);
    await user.click(within(dialog).getByRole("button", { name: "Tambah rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    const body = lastBody("post");
    expect(body).toMatchObject({ type: "credit", initial_balance: 0, credit_limit: null, payment_day: null, statement_day: null });
    expect(Object.is(body.initial_balance, -0)).toBe(false);
  });

  it("edits both days of a credit account without an owed amount, parent or Kegunaan choice", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "edit-credit", "Ubah rekening");
    expect(within(dialog).queryByRole("textbox", { name: /Utang saat ini/ })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("combobox", { name: "Pindahkan ke rekening" })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("combobox", { name: "Kegunaan" })).not.toBeInTheDocument();
    const statement = within(dialog).getByRole("spinbutton", { name: "Tagihan terbit tiap tgl (opsional)" });
    const payment = within(dialog).getByRole("spinbutton", { name: "Jatuh tempo tiap tgl (opsional)" });
    expect(statement).toHaveValue(1);
    expect(payment).toHaveValue(11);
    await user.clear(statement);
    await user.type(statement, "25");
    await user.clear(payment);
    await user.type(payment, "5");
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalled());
    expect(vi.mocked(api.patch).mock.calls[0][0]).toBe("/accounts/spaylater");
    expect(lastBody("patch")).toEqual({
      credit_limit: 2_000_000, payment_day: 5, statement_day: 25, name: "SPayLater", type: "credit", role: "utang",
      instrument_type: null, instrument_symbol: null, units: null, avg_buy_price: null, color: "#2563EB",
      default_funding_account_id: null, default_pocket_id: null,
    });
  });

  it("clears the limit and both days when a credit account becomes a bank account", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "edit-credit", "Ubah rekening");
    await user.click(within(dialog).getByRole("radio", { name: "Bank" }));
    expect(within(dialog).queryByRole("spinbutton", { name: /tiap tgl/ })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalled());
    expect(lastBody("patch")).toMatchObject({ type: "bank", credit_limit: null, payment_day: null, statement_day: null });
  });

  it("shows no credit days for a bank account and sends neither day", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "edit-bank", "Ubah rekening");
    expect(within(dialog).queryByRole("spinbutton", { name: /tiap tgl/ })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalled());
    expect(lastBody("patch")).toEqual({
      name: "Jago", type: "bank", role: "harian", instrument_type: null, instrument_symbol: null, units: null, avg_buy_price: null,
      color: "#2563EB", default_funding_account_id: null, default_pocket_id: null,
    });
  });
});

describe("pocket form", () => {
  it("adds a pocket with its Kegunaan and exactly today's body", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "new-pocket", "Tambah kantong di Jago");
    await user.type(within(dialog).getByRole("textbox", { name: "Nama kantong" }), "Tabungan");
    const role = within(dialog).getByRole("combobox", { name: "Kegunaan" });
    expect(role).toHaveAccessibleDescription(/Dana investasi adalah uang yang belum dibelikan/);
    await user.selectOptions(role, "tabungan");
    await user.type(within(dialog).getByRole("textbox", { name: "Saldo awal kantong (IDR)" }), "500000");
    await user.click(within(dialog).getByRole("button", { name: "Buat kantong" }));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(lastBody("post")).toEqual({
      name: "Tabungan", type: "bank", parent_id: "jago", initial_balance: 500_000, instrument_type: null,
      instrument_symbol: null, units: null, avg_buy_price: null, role: "tabungan",
    });
  });

  it("keeps a refused pocket name and puts the message under the field", async () => {
    vi.mocked(api.post).mockRejectedValue(new Error("Nama kantong sudah digunakan"));
    const user = renderDialogs();
    const dialog = await open(user, "new-pocket", "Tambah kantong di Jago");
    const name = within(dialog).getByRole("textbox", { name: "Nama kantong" });
    await user.type(name, "Duplicated");
    await user.click(within(dialog).getByRole("button", { name: "Buat kantong" }));
    await waitFor(() => expect(name).toHaveAttribute("aria-invalid", "true"));
    expect(name).toHaveValue("Duplicated");
    expect(name).toHaveFocus();
    expect(name).toHaveAccessibleDescription("Nama kantong sudah digunakan");
  });

  it("puts a Kegunaan refusal by its server code under the Kegunaan field of a pocket", async () => {
    const refusal = Object.assign(new Error("Kegunaan tidak sesuai jenis rekening."), { detail: { code: "account_role_type_mismatch" } });
    vi.mocked(api.patch).mockRejectedValueOnce(refusal);
    const user = renderDialogs();
    const dialog = await open(user, "edit-pocket", "Ubah kantong");
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    const role = within(dialog).getByRole("combobox", { name: "Kegunaan" });
    await waitFor(() => expect(role).toHaveAttribute("aria-invalid", "true"));
    expect(role).toHaveFocus();
    expect(role).toHaveAccessibleDescription(expect.stringContaining("Kegunaan tidak sesuai jenis rekening."));
  });
});

describe("value and balance dialogs", () => {
  it("updates a product's value with today's body and previews the gain with one decimal", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "valuation", "Perbarui nilai");
    expect(dialog).toHaveTextContent("▲ +Rp 50.000 · +5,0% untung");
    await user.click(within(dialog).getByRole("button", { name: "Simpan nilai" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/fund/valuation", {
      current_balance: 1_050_000, cost_basis: 1_000_000, notes: null,
    }));
  });

  it("adjusts a balance with today's body and names the difference as money in", async () => {
    const user = renderDialogs();
    const dialog = await open(user, "reconcile", "Sesuaikan saldo");
    const actual = within(dialog).getByRole("textbox", { name: "Saldo sebenarnya (IDR)" });
    await user.clear(actual);
    await user.type(actual, "250000");
    expect(dialog).toHaveTextContent("+Rp 150.000 · uang masuk");
    await user.click(within(dialog).getByRole("button", { name: "Sesuaikan saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/jago/reconcile", { actual_balance: 250_000, notes: null }));
    expect(api.patch).not.toHaveBeenCalled();
  });
});

describe("toasts (D15)", () => {
  it("reports the price update and a failed archive in the status toast", async () => {
    vi.mocked(api.post).mockResolvedValueOnce({ ok: true, symbols_checked: 3, accounts_updated: 3 });
    const user = renderDialogs();
    await user.click(screen.getByRole("button", { name: "sync" }));
    expect(await screen.findByText(/^Harga diperbarui · 3 produk, per \d{2}\.\d{2}$/)).toBeInTheDocument();

    vi.mocked(api.post).mockRejectedValueOnce(new Error("offline"));
    await user.click(screen.getByRole("button", { name: "sync" }));
    expect(await screen.findByText("Harga belum dapat diperbarui.")).toBeInTheDocument();

    vi.mocked(api.del).mockRejectedValueOnce(new Error(""));
    await user.click(screen.getByRole("button", { name: "archive" }));
    expect(await screen.findByText("Rekening gagal diarsipkan. Coba lagi.")).toBeInTheDocument();
  });
});

describe("in English", () => {
  it("reads the account form in plain English and sends the same body", async () => {
    const user = renderDialogs("en");
    const dialog = await open(user, "open-new", "Add account");
    await user.type(within(dialog).getByRole("textbox", { name: "Name" }), "SPayLater");
    await user.click(within(dialog).getByRole("radio", { name: "Credit" }));
    expect(dialog).toHaveTextContent("Bills already issued keep their due dates.");
    expect(dialog).toHaveTextContent(/Use\s*Debt\s*Set by the account type\./);
    await user.type(within(dialog).getByRole("textbox", { name: "Owed now (IDR)" }), "350000");
    await user.type(within(dialog).getByRole("spinbutton", { name: "Bill issued on day (optional)" }), "25");
    await user.type(within(dialog).getByRole("spinbutton", { name: "Due on day (optional)" }), "40");
    await user.click(within(dialog).getByRole("button", { name: "Add account" }));
    expect(within(dialog).getByRole("spinbutton", { name: "Due on day (optional)" })).toHaveAccessibleDescription(/^Enter a day from 1 to 31\./);
    await user.clear(within(dialog).getByRole("spinbutton", { name: "Due on day (optional)" }));
    await user.click(within(dialog).getByRole("button", { name: "Add account" }));
    await waitFor(() => expect(api.post).toHaveBeenCalled());
    expect(lastBody("post")).toMatchObject({ type: "credit", initial_balance: -350_000, statement_day: 25, payment_day: null });
  });
});
