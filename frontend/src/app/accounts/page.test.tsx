import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, createMovement } from "@/lib/api";
import AccountsPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() }, createMovement: vi.fn() }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));
vi.mock("@/hooks/useAnimatedCounter", () => ({ useAnimatedCounter: (amount: number) => amount }));

const investmentParent = {
  id: "investment-parent",
  name: "Broker",
  type: "investment",
  initial_balance: 0,
  balance: 0,
  is_archived: false,
  children: [],
  created_at: "2026-09-01T00:00:00.000Z",
};

function renderAccounts() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <AccountsPage />
    </QueryClientProvider>,
  );
}

describe("account and pocket forms", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [investmentParent], total_balance: 0 };
      if (path === "/dashboard/net-worth") {
        return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [] };
      }
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(createMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-1",
      expense_transaction_id: "movement-out",
      income_transaction_id: "movement-in",
    });
  });

  it("routes an account transfer through the canonical movement payload", async () => {
    const liquid = [
      { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] },
      { id: "cash-b", name: "Cash B", type: "bank", initial_balance: 0, balance: 0, is_archived: false, children: [] },
    ];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [investmentParent, ...liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 1000, total_assets: 1000, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const cash = (await screen.findAllByRole("region", { name: "Rekening Cash A" }))[0];
    expect(within(cash).getByRole("button", { name: "Tambah kantong Cash A" })).toBeInTheDocument();
    await user.click(within(cash).getByRole("button", { name: "Opsi Cash A" }));
    await user.click(within(screen.getByRole("group", { name: "Opsi Cash A" })).getByRole("button", { name: "Pindah Saldo" }));
    const dialog = screen.getByRole("dialog", { name: "Pindah saldo" });
    expect(within(dialog).getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a");
    expect(within(dialog).getByRole("combobox", { name: "Ke rekening" })).toHaveValue("cash-b");
    expect(within(dialog).queryByRole("option", { name: /Broker/ })).not.toBeInTheDocument();
    await user.type(within(dialog).getByRole("textbox", { name: "Nominal (IDR)" }), "500");
    await user.click(within(dialog).getByRole("button", { name: "Pindahkan Saldo" }));
    await waitFor(() => expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "cash-a",
      target_account_id: "cash-b",
      amount: 500,
    })));
  });

  it("offers the same add-pocket primary action on a liquid account without pockets", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 1000, total_assets: 1000, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    const cash = (await screen.findAllByRole("region", { name: "Rekening Cash A" }))[0];
    await user.click(within(cash).getByRole("button", { name: "Tambah kantong Cash A" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah Kantong di Cash A" });
    await user.type(within(dialog).getByRole("textbox", { name: "Nama kantong" }), "Tabungan");
    await user.click(within(dialog).getByRole("button", { name: "Buat Kantong" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      parent_id: "cash-a",
      name: "Tabungan",
    })));
  });

  it("keeps direct Beli and Jual controls for a top-level investment position", async () => {
    const position = {
      ...investmentParent,
      name: "BBCA",
      instrument_type: "stock",
      instrument_symbol: "BBCA.JK",
      units: 100,
      avg_buy_price: 5000,
      last_price: 6000,
      balance: 600_000,
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [position], total_balance: 600_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 600_000, total_assets: 600_000, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });

    renderAccounts();
    const holding = (await screen.findAllByRole("region", { name: "Rekening BBCA" }))[0];
    expect(within(holding).getByRole("button", { name: "Beli" })).toBeInTheDocument();
    expect(within(holding).getByRole("button", { name: "Jual" })).toBeInTheDocument();
    expect(within(holding).queryByRole("button", { name: "Tambah kantong BBCA" })).not.toBeInTheDocument();
  });

  it("creates a cash funding pocket under an investment platform as liquid", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const addPocket = await screen.findAllByRole("button", { name: "Tambah kantong Broker" });
    await user.click(addPocket[0]);

    const dialog = screen.getByRole("dialog", { name: /tambah kantong/i });
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: /kas \/ tabungan biasa/i }));
    await user.type(within(dialog).getByRole("textbox", { name: /nama kantong/i }), "RDN");
    await user.type(within(dialog).getByRole("textbox", { name: /saldo awal kantong/i }), "500000");
    await user.click(within(dialog).getByRole("button", { name: "Buat Kantong" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      parent_id: "investment-parent",
      name: "RDN",
      type: "bank",
      initial_balance: 500_000,
      instrument_type: null,
    })));
  });

  it("creates a deposit pocket with principal but no rate or purchase price", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const addPocket = await screen.findAllByRole("button", { name: "Tambah kantong Broker" });
    await user.click(addPocket[0]);

    const dialog = screen.getByRole("dialog", { name: /tambah kantong/i });
    await user.type(within(dialog).getByRole("textbox", { name: /nama kantong/i }), "Deposito 2026");
    await user.click(within(dialog).getByRole("button", { name: "Deposito" }));
    await user.type(within(dialog).getByRole("textbox", { name: /total pokok deposito/i }), "1000000");
    await user.click(within(dialog).getByRole("button", { name: "Buat Kantong" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      parent_id: "investment-parent",
      type: "investment",
      instrument_type: "deposit",
      initial_balance: 1_000_000,
      avg_buy_price: undefined,
      units: undefined,
    })));
  });

  it("creates an investment platform without a misleading spendable opening balance", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: /rekening baru/i }));

    const dialog = screen.getByRole("dialog", { name: "Tambah Rekening Baru" });
    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "New broker");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Jenis rekening" }), "investment");
    expect(within(dialog).queryByRole("textbox", { name: /saldo kas \/ rdn awal/i })).not.toBeInTheDocument();
    expect(within(dialog).getByText(/tambahkan kantong kas\/RDN/i)).toBeVisible();
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      name: "New broker",
      type: "investment",
      initial_balance: 0,
      instrument_type: null,
    })));
  });

  it("converts a position pocket to a liquid pocket when the user selects cash", async () => {
    const user = userEvent.setup();
    const position = {
      id: "position-pocket",
      parent_id: "investment-parent",
      name: "Stock",
      type: "investment",
      instrument_type: "stock",
      instrument_symbol: "BBCA",
      initial_balance: 100_000,
      balance: 100_000,
      is_archived: false,
      created_at: "2026-09-01T00:00:00.000Z",
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") {
        return { ok: true, accounts: [{ ...investmentParent, children: [position] }, position], total_balance: 100_000 };
      }
      if (path === "/dashboard/net-worth") {
        return { ok: true, net_worth: 100_000, total_assets: 100_000, total_liabilities: 0, accounts: [], obligations: [] };
      }
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const broker = (await screen.findAllByRole("region", { name: "Rekening Broker" }))[0];
    await user.click(within(broker).getByRole("button", { name: "Opsi Stock" }));
    await user.click(within(screen.getByRole("group", { name: "Opsi Stock" })).getByRole("button", { name: "Ubah kantong" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Kantong: Stock" });
    await user.click(within(dialog).getByRole("button", { name: /kas \/ tabungan biasa/i }));
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/position-pocket", expect.objectContaining({
      type: "bank",
      instrument_type: null,
      instrument_symbol: null,
    })));
  });

  it("edits and clears a parent account's default notification pocket", async () => {
    const user = userEvent.setup();
    const child = {
      id: "bank-pocket",
      parent_id: "bank-parent",
      name: "Daily pocket",
      type: "bank",
      initial_balance: 0,
      balance: 0,
      is_archived: false,
      created_at: "2026-09-01T00:00:00.000Z",
    };
    const parent = {
      id: "bank-parent",
      name: "BCA",
      type: "bank",
      initial_balance: 0,
      balance: 0,
      is_archived: false,
      default_pocket_id: "bank-pocket",
      children: [child],
      created_at: "2026-09-01T00:00:00.000Z",
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [parent, child], total_balance: 0 };
      if (path === "/dashboard/net-worth") {
        return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [] };
      }
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const bank = (await screen.findAllByRole("region", { name: "Rekening BCA" }))[0];
    await user.click(within(bank).getByRole("button", { name: "Opsi BCA" }));
    await user.click(within(screen.getByRole("group", { name: "Opsi BCA" })).getByRole("button", { name: "Ubah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Rekening: BCA" });
    const pocketSelect = within(dialog).getByRole("combobox", { name: /kantong default/i });
    expect(pocketSelect).toHaveValue("bank-pocket");
    await user.selectOptions(pocketSelect, "");
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/bank-parent", expect.objectContaining({
      default_pocket_id: null,
    })));
  });

  it("keeps a rejected pocket name available and associates the error with its field", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValue(new Error("Nama kantong sudah digunakan"));
    renderAccounts();
    const addPocket = await screen.findAllByRole("button", { name: "Tambah kantong Broker" });
    await user.click(addPocket[0]);
    const dialog = screen.getByRole("dialog", { name: /tambah kantong/i });
    const name = within(dialog).getByRole("textbox", { name: "Nama kantong" });
    await user.type(name, "Duplicated");
    await user.click(within(dialog).getByRole("button", { name: "Buat Kantong" }));

    await waitFor(() => expect(name).toHaveAttribute("aria-invalid", "true"));
    expect(name).toHaveValue("Duplicated");
    expect(name).toHaveFocus();
    expect(name).toHaveAccessibleDescription("Nama kantong sudah digunakan");
  });

  it("submits a balance reconciliation without changing the opening balance", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const broker = (await screen.findAllByRole("region", { name: "Rekening Broker" }))[0];
    await user.click(within(broker).getByRole("button", { name: "Opsi Broker" }));
    await user.click(within(screen.getByRole("group", { name: "Opsi Broker" })).getByRole("button", { name: "Sesuaikan Saldo" }));
    const dialog = screen.getByRole("dialog", { name: "Sesuaikan Saldo: Broker" });
    const actualBalance = within(dialog).getByRole("textbox", { name: /saldo sebenarnya/i });
    await user.type(actualBalance, "250000");
    await user.click(within(dialog).getByRole("button", { name: "Konfirmasi & Sesuaikan Saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/investment-parent/reconcile", {
      actual_balance: 250_000,
      notes: null,
    }));
    expect(api.patch).not.toHaveBeenCalled();
  });

  it("keeps cards equal height and twelve pockets in a scrollable region", async () => {
    const pockets = Array.from({ length: 12 }, (_, index) => ({
      id: `pocket-${index}`,
      parent_id: "jago",
      name: `Kantong ${index + 1}`,
      type: "bank",
      initial_balance: 100_000,
      balance: 100_000,
      is_archived: false,
      created_at: "2026-09-01T00:00:00.000Z",
    }));
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [{ ...investmentParent, id: "jago", name: "Jago", type: "bank", color: "#D97706", balance: 1_200_000, children: pockets }, { ...investmentParent, id: "cash", name: "Cash", type: "cash", balance: 42_000 }, ...pockets], total_balance: 1_242_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 1_200_000, total_assets: 1_200_000, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });

    renderAccounts();
    const account = (await screen.findAllByRole("region", { name: "Rekening Jago" }))[0];
    const cash = (await screen.findAllByRole("region", { name: "Rekening Cash" }))[0];
    expect(screen.getByText("2 Rekening Utama (12 Kantong)")).toBeInTheDocument();
    expect(within(account).getAllByRole("group", { name: /^Kantong Kantong/ })).toHaveLength(12);
    expect(within(account).getByRole("region", { name: "12 kantong Jago" })).toHaveClass("overflow-y-auto");
    expect(within(account).getByRole("region", { name: "12 kantong Jago" })).toHaveAttribute("tabindex", "0");
    expect(account).toHaveClass("h-[28rem]");
    expect(cash).toHaveClass("h-[28rem]");
    expect(within(account).getByText("Jago")).toHaveStyle({ color: "#000000" });
    expect(within(account).getByText("Rp 1.200.000")).toBeInTheDocument();
    expect(account).not.toHaveClass("columns-1");
    expect((await axe(account)).violations).toHaveLength(0);
  });

  it("keeps investment and liquid pocket actions specific to their account types", async () => {
    const position = { id: "shares", parent_id: "broker", name: "BBCA", type: "investment", instrument_type: "stock", instrument_symbol: "BBCA.JK", units: 100, balance: 600_000, initial_balance: 500_000, capital_gain: 100_000, capital_gain_pct: 20, is_archived: false, created_at: "2026-09-01T00:00:00.000Z" };
    const funding = { id: "funding", parent_id: "broker", name: "RDN", type: "bank", balance: 500_000, initial_balance: 500_000, is_archived: false, created_at: "2026-09-01T00:00:00.000Z" };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [{ ...investmentParent, id: "broker", balance: 1_100_000, children: [position, funding] }, position, funding], total_balance: 1_100_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 1_100_000, total_assets: 1_100_000, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    const shares = (await screen.findAllByRole("group", { name: "Kantong BBCA" }))[0];
    const rdn = screen.getAllByRole("group", { name: "Kantong RDN" })[0];
    expect(within(shares).getByRole("button", { name: "Beli" })).toBeInTheDocument();
    expect(within(shares).getByRole("button", { name: "Jual" })).toBeInTheDocument();
    expect(within(rdn).queryByRole("button", { name: "Beli" })).not.toBeInTheDocument();
    expect(within(rdn).getByRole("button", { name: "Pindah saldo RDN" })).toBeInTheDocument();
    await user.click(within(shares).getByRole("button", { name: "Opsi BBCA" }));
    const options = screen.getByRole("group", { name: "Opsi BBCA" });
    expect(within(options).getByRole("button", { name: "Update Nilai" })).toBeInTheDocument();
    expect(within(options).getByRole("button", { name: "Sesuaikan Saldo" })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(within(shares).getByRole("button", { name: "Opsi BBCA" })).toHaveFocus();
  });

  it("persists explicit top-level and sibling-pocket moves", async () => {
    const first = { id: "bank-a", name: "Bank A", type: "bank", balance: 0, initial_balance: 0, is_archived: false, created_at: "2026-09-01T00:00:00.000Z", children: [
      { id: "pocket-a", parent_id: "bank-a", name: "Poket A", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
      { id: "pocket-b", parent_id: "bank-a", name: "Poket B", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
    ] };
    const second = { id: "bank-b", name: "Bank B", type: "bank", balance: 0, initial_balance: 0, is_archived: false, created_at: "2026-09-01T00:00:00.000Z", children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [first, second], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    const bank = (await screen.findAllByRole("region", { name: "Rekening Bank A" }))[0];
    await user.click(within(bank).getByRole("button", { name: "Opsi Bank A" }));
    const bankOptions = screen.getByRole("group", { name: "Opsi Bank A" });
    expect(within(bankOptions).getByRole("button", { name: "Naikkan urutan" })).toBeDisabled();
    await user.click(within(bankOptions).getByRole("button", { name: "Turunkan urutan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["bank-b", "bank-a"] }));

    const pocket = within(bank).getByRole("group", { name: "Kantong Poket B" });
    await user.click(within(pocket).getByRole("button", { name: "Opsi Poket B" }));
    await user.click(within(screen.getByRole("group", { name: "Opsi Poket B" })).getByRole("button", { name: "Naikkan urutan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["pocket-b", "pocket-a"] }));
  });

  it("requires archive confirmation and reports a failed archive", async () => {
    const user = userEvent.setup();
    vi.mocked(api.del).mockRejectedValue(new Error("Arsip gagal disimpan"));
    renderAccounts();
    const broker = (await screen.findAllByRole("region", { name: "Rekening Broker" }))[0];
    await user.click(within(broker).getByRole("button", { name: "Opsi Broker" }));
    const options = screen.getByRole("group", { name: "Opsi Broker" });
    await user.click(within(options).getByRole("button", { name: "Arsipkan rekening Broker" }));
    expect(api.del).not.toHaveBeenCalled();
    await user.click(within(options).getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/accounts/investment-parent"));
    expect(await screen.findAllByRole("alert", { name: "" })).toEqual(expect.arrayContaining([
      expect.objectContaining({ textContent: "Arsip gagal disimpan" }),
    ]));
  });

  it("reports reorder failures and restores the previous order", async () => {
    const second = { ...investmentParent, id: "second", name: "Second" };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [investmentParent, second], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    vi.mocked(api.post).mockRejectedValue(new Error("Network unavailable"));
    const user = userEvent.setup();
    renderAccounts();
    const broker = (await screen.findAllByRole("region", { name: "Rekening Broker" }))[0];
    await user.click(within(broker).getByRole("button", { name: "Opsi Broker" }));
    await user.click(within(screen.getByRole("group", { name: "Opsi Broker" })).getByRole("button", { name: "Turunkan urutan" }));
    expect((await screen.findAllByRole("alert")).some((alert) => alert.textContent?.includes("Urutan gagal disimpan"))).toBe(true);
    const groups = screen.getAllByRole("region", { name: /^Rekening / });
    expect(groups[0]).toHaveAccessibleName("Rekening Broker");
  });

  it("keeps pointer drag ordering on account groups and sibling rows", async () => {
    const pockets = [
      { id: "first-pocket", parent_id: "first", name: "First pocket", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
      { id: "second-pocket", parent_id: "first", name: "Second pocket", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
    ];
    const first = { ...investmentParent, id: "first", name: "First", type: "bank", children: pockets };
    const targetPocket = { id: "target-pocket", parent_id: "second", name: "Target pocket", type: "bank", balance: 0, initial_balance: 0, is_archived: false };
    const second = { ...investmentParent, id: "second", name: "Second", type: "bank", children: [targetPocket] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [first, second, ...pockets, targetPocket], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [] };
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    renderAccounts();
    const firstGroup = (await screen.findAllByRole("region", { name: "Rekening First" }))[0];
    const secondGroup = screen.getAllByRole("region", { name: "Rekening Second" })[0];
    const dragData = { setData: vi.fn(), effectAllowed: "move", dropEffect: "move" };

    fireEvent.dragStart(firstGroup.querySelector('[draggable="true"]')!, { dataTransfer: dragData });
    const targetRow = within(secondGroup).getByRole("group", { name: "Kantong Target pocket" });
    fireEvent.dragOver(targetRow, { dataTransfer: dragData });
    fireEvent.drop(targetRow, { dataTransfer: dragData });
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["second", "first"] }));

    const firstPocket = within(firstGroup).getByRole("group", { name: "Kantong First pocket" });
    const secondPocket = within(firstGroup).getByRole("group", { name: "Kantong Second pocket" });
    fireEvent.dragStart(firstPocket.querySelector('[draggable="true"]')!, { dataTransfer: dragData });
    fireEvent.dragOver(secondPocket, { dataTransfer: dragData });
    fireEvent.drop(secondPocket, { dataTransfer: dragData });
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["second-pocket", "first-pocket"] }));
  });
});
