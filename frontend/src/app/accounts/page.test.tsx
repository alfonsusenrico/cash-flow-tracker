import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { ExplainProvider } from "@/components/workspace/explain";
import AccountsPage from "./page";

// Next.js keeps `useSearchParams` in sync with the history API; the mock does the same.
vi.mock("next/navigation", async () => {
  const react = await import("react");
  return {
    usePathname: () => window.location.pathname,
    useSearchParams: () => {
      const [, rerender] = react.useReducer((count: number) => count + 1, 0);
      react.useEffect(() => {
        window.addEventListener("test-history", rerender);
        return () => window.removeEventListener("test-history", rerender);
      }, []);
      return new URLSearchParams(window.location.search);
    },
  };
});

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const hidden = vi.hoisted(() => ({ on: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => (hidden.on ? "••••" : `Rp ${amount.toLocaleString("id-ID")}`) }),
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

const realReplace = window.history.replaceState.bind(window.history);

/** The card of an account or the row of a pocket; pocket buttons name their parent for assistive technology. */
async function row(name: string | RegExp) {
  const button = (await screen.findAllByRole("button", { name }))[0];
  return button.closest("[data-account-row]") as HTMLElement;
}

/** Opens the row's drawer and returns it. */
async function openDrawer(user: ReturnType<typeof userEvent.setup>, name: string | RegExp, title: string) {
  await user.click((await screen.findAllByRole("button", { name }))[0]);
  return screen.findByRole("dialog", { name: new RegExp(`^${title}`) });
}

function renderAccounts() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <ExplainProvider>
        <AccountsPage />
      </ExplainProvider>
    </QueryClientProvider>,
  );
}

describe("account and pocket forms", () => {
  afterEach(() => {
    window.history.replaceState = realReplace;
    realReplace(null, "", "/accounts");
  });

  beforeEach(() => {
    vi.clearAllMocks();
    realReplace(null, "", "/accounts");
    window.history.replaceState = (state, title, url) => {
      realReplace(state, title, url);
      window.dispatchEvent(new Event("test-history"));
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [investmentParent], total_balance: 0 };
      if (path === "/dashboard/net-worth") {
        return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [] };
      }
      return { ok: true, rules: [], accounts: [], results: [] };
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("does not present assets as net worth when net worth is unavailable", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") throw new Error("unavailable");
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    renderAccounts();
    expect(await screen.findByText(/Kekayaan bersih belum dapat dihitung/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /^Kekayaan bersih Rp/ })).not.toBeInTheDocument();
    // The account's own balance stays visible; no client-side sum stands in for the server figure.
    expect(within(await row("Cash A")).getByText("Rp 1.000")).toBeInTheDocument();
  });

  it("shows the server's total assets and net worth as KPI cards, with the owed lines in the explanation", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") return {
        ok: true, net_worth: 450, total_assets: 1000, total_liabilities: 500, accounts: [], obligations: [],
        liquid: 1000, investment: 0, reconciliation_pending: 50,
        lines: [{ key: "liquid", label: "Dana likuid", amount: 1000 }, { key: "investment", label: "Investasi", amount: 0 },
          { key: "reconciliation_pending", label: "Saldo minus perlu dicocokkan", amount: -50 }, { key: "debt:d1", label: "Cicilan Contoh", amount: -500 }],
      };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const kpis = await screen.findByRole("region", { name: "Ringkasan aset" });
    expect(within(kpis).getByRole("button", { name: "Total aset Rp 1.000: lihat rincian" })).toBeInTheDocument();
    expect(kpis).toHaveTextContent("setelah utang Rp 500");
    expect(kpis).toHaveTextContent("100% dari aset");
    await user.click(within(kpis).getByRole("button", { name: "Kekayaan bersih Rp 450: lihat rincian" }));
    const explanation = await screen.findByRole("dialog", { name: "Kekayaan bersih" });
    expect(explanation).toHaveTextContent("Saldo minus perlu dicocokkan");
    expect(explanation).toHaveTextContent("Cicilan Contoh");
  });

  it("removes dedicated transfer controls from account actions", async () => {
    const liquid = [
      { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] },
      { id: "cash-b", name: "Cash B", type: "bank", initial_balance: 0, balance: 0, is_archived: false, children: [] },
    ];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [investmentParent, ...liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const drawer = await openDrawer(user, "Cash A", "Cash A");
    expect(within(drawer).getByRole("button", { name: /Tambah kantong/ })).toBeInTheDocument();
    expect(within(drawer).queryByRole("button", { name: /pindah saldo/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: /pindah saldo/i })).not.toBeInTheDocument();
  });

  it("shows the detail's facts, pockets and the account's last transactions", async () => {
    const pockets = [
      { id: "atm", parent_id: "bca", name: "ATM", type: "bank", initial_balance: 0, balance: 300_000, is_archived: false, children: [] },
      { id: "darurat", parent_id: "bca", name: "Dana Darurat", type: "bank", initial_balance: 0, balance: 700_000, is_archived: false, children: [] },
    ];
    const bca = { id: "bca", name: "BCA", type: "bank", initial_balance: 0, balance: 1_000_000, is_archived: false, children: pockets };
    const paths: string[] = [];
    vi.mocked(api.get).mockImplementation(async (path) => {
      paths.push(path);
      if (path === "/accounts") return { ok: true, accounts: [bca], total_balance: 1_000_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      if (path.startsWith("/transactions?account_id=bca")) {
        return { ok: true, transactions: [{ id: "t1", account_id: "bca", account_name: "BCA", category_name: "Makan", type: "expense", amount: 45_000, date: "2026-11-03T14:04:00.000Z", notes: null, merchant: "Warung Sari" }] };
      }
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "BCA", "BCA");
    expect(detail).toHaveTextContent(/Saldo\s*Rp 1\.000\.000\s*Kantong\s*2\s*Kantong utama\s*ATM/);
    const pocketList = within(detail).getByRole("region", { name: "Kantong" });
    expect(within(pocketList).getAllByRole("listitem").map((item) => item.textContent)).toEqual(["ATMRp 300.000", "Dana DaruratRp 700.000"]);
    const recent = within(detail).getByRole("region", { name: "Transaksi terakhir" });
    expect(await within(recent).findByText("Warung Sari")).toBeInTheDocument();
    expect(paths).toContain("/transactions?account_id=bca&limit=4");
    expect(within(detail).getByRole("button", { name: "Arsipkan rekening BCA" })).toHaveTextContent("Arsipkan");
  });

  it("says where each movement in the detail came from or went to, with the bank", async () => {
    const tab = { id: "tab", parent_id: "jago", name: "Tabungan", type: "bank", initial_balance: 0, balance: 650_000, is_archived: false, children: [] };
    const jago = { id: "jago", name: "Jago", type: "bank", initial_balance: 0, balance: 650_000, is_archived: false, children: [tab] };
    const leg = { category_name: "Internal Movement", notes: null, account_id: "tab", account_name: "Tabungan", account_parent_name: "Jago" };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [jago], total_balance: 650_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      if (path.startsWith("/transactions?account_id=tab")) {
        return { ok: true, transactions: [
          { ...leg, id: "in", type: "income", amount: 500_000, date: "2026-11-03T18:41:00.000Z", movement_id: "m1", movement_role: "inbound",
            transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null },
          { ...leg, id: "out", type: "expense", amount: 150_000, date: "2026-11-03T14:10:00.000Z", movement_id: "m2", movement_role: "outbound",
            transfer_target_account_name: "Tabungan Rumah", transfer_target_account_parent_name: "BCA" },
        ] };
      }
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "Tabungan, kantong di Jago", "Tabungan");
    const recent = within(detail).getByRole("region", { name: "Transaksi terakhir" });
    const rows = await within(recent).findAllByRole("listitem");
    expect(rows[0]).toHaveTextContent("Gopay → Jago · Tabungan");
    expect(rows[0]).toHaveTextContent("Masuk dari Gopay");
    expect(rows[1]).toHaveTextContent("Jago · Tabungan → BCA · Tabungan Rumah");
    expect(rows[1]).toHaveTextContent("Keluar ke BCA · Tabungan Rumah");
  });

  it("offers the same add-pocket primary action on a liquid account without pockets", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    const drawer = await openDrawer(user, "Cash A", "Cash A");
    await user.click(within(drawer).getByRole("button", { name: /Tambah kantong/ }));
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
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    expect(within(await row("BBCA")).getByText("Saham")).toBeInTheDocument();
    const drawer = await openDrawer(user, "BBCA", "BBCA");
    expect(within(drawer).getByRole("button", { name: "Beli" })).toBeInTheDocument();
    expect(within(drawer).getByRole("button", { name: "Jual" })).toBeInTheDocument();
    expect(within(drawer).getByRole("button", { name: "Perbarui nilai" })).toBeInTheDocument();
    expect(within(drawer).queryByRole("button", { name: /Tambah kantong/ })).not.toBeInTheDocument();
  });

  it.each([false, true])("offers nominal top-up for an eligible product (child=%s)", async (child) => {
    const product = { ...investmentParent, id: "fund-1", name: "Bibit Fund", instrument_type: "mutual_fund", units: null, parent_id: child ? investmentParent.id : null };
    const source = { id: "rdn", name: "BCA RDN", type: "bank", balance: 500_000, initial_balance: 500_000, children: [] };
    const parent = { ...investmentParent, default_funding_account_id: "rdn", children: [product] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts: child ? [parent, product, source] : [product, source], total_balance: 500_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const drawer = await openDrawer(user, /^Bibit Fund/, "Bibit Fund");
    await user.click(within(drawer).getByRole("button", { name: "Top up" }));
    const dialog = screen.getByRole("dialog", { name: "Top up Investasi" });
    expect(within(dialog).getByText("Bibit Fund")).toBeInTheDocument();
    expect(within(dialog).queryByLabelText(/Unit Penyertaan|NAB/)).not.toBeInTheDocument();
    if (child) expect(within(dialog).getByRole("combobox", { name: "Dari rekening" })).toHaveValue("rdn");
  });

  it("creates a cash funding pocket under an investment platform as liquid", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(within(await openDrawer(user, "Broker", "Broker")).getByRole("button", { name: /Tambah kantong/ }));

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
    await user.click(within(await openDrawer(user, "Broker", "Broker")).getByRole("button", { name: /Tambah kantong/ }));

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
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));

    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });
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
      color: "#8B5CF6",
    })));
  });

  it("starts a new investment account lavender and a new bank account blue", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });
    const type = within(dialog).getByRole("combobox", { name: "Jenis rekening" });

    expect(within(dialog).getByRole("button", { name: "Warna Biru" })).toHaveAttribute("aria-pressed", "true");
    await user.selectOptions(type, "investment");
    expect(within(dialog).getByRole("button", { name: "Warna Lavender" })).toHaveAttribute("aria-pressed", "true");
    await user.selectOptions(type, "wallet");
    expect(within(dialog).getByRole("button", { name: "Warna Biru" })).toHaveAttribute("aria-pressed", "true");

    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "Dompet contoh");
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({ type: "wallet", color: "#2563EB" })));
  });

  it("keeps the colour the owner picked when the account type changes", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });

    await user.click(within(dialog).getByRole("button", { name: "Warna Teal" }));
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Jenis rekening" }), "investment");
    expect(within(dialog).getByRole("button", { name: "Warna Teal" })).toHaveAttribute("aria-pressed", "true");

    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "Broker contoh");
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({ type: "investment", color: "#0D9488" })));
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
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const drawer = await openDrawer(user, "Stock, produk di Broker", "Stock");
    await user.click(within(drawer).getByRole("button", { name: "Ubah kantong" }));
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
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const drawer = await openDrawer(user, "BCA", "BCA");
    await user.click(within(drawer).getByRole("button", { name: "Ubah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Rekening: BCA" });
    const pocketSelect = within(dialog).getByRole("combobox", { name: /kantong default/i });
    expect(pocketSelect).toHaveValue("bank-pocket");
    await user.selectOptions(pocketSelect, "");
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/bank-parent", expect.objectContaining({
      default_pocket_id: null,
    })));
  });

  it("moves a stand-alone wallet under a bank as a pocket, keeping its history", async () => {
    const user = userEvent.setup();
    const jago = {
      id: "jago", name: "Jago", type: "bank", initial_balance: 0, balance: 0, is_archived: false,
      children: [], created_at: "2026-09-01T00:00:00.000Z",
    };
    const gopay = {
      id: "gopay", name: "GoPay", type: "wallet", initial_balance: 0, balance: 0, is_archived: false,
      children: [], created_at: "2026-09-01T00:00:00.000Z",
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [jago, gopay, investmentParent], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const drawer = await openDrawer(user, "GoPay", "GoPay");
    await user.click(within(drawer).getByRole("button", { name: "Ubah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Rekening: GoPay" });
    const parentSelect = within(dialog).getByRole("combobox", { name: "Jadikan kantong di bawah" });
    expect(parentSelect).toHaveValue("");
    expect(within(parentSelect).queryByRole("option", { name: "Broker" })).not.toBeInTheDocument();
    expect(within(parentSelect).queryByRole("option", { name: "GoPay" })).not.toBeInTheDocument();
    expect(parentSelect).toHaveAccessibleDescription(/GoPay Tabungan/);
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.selectOptions(parentSelect, "jago");
    const name = within(dialog).getByRole("textbox", { name: /nama rekening/i });
    await user.clear(name);
    await user.type(name, "GoPay Tabungan");
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/gopay", expect.objectContaining({
      parent_id: "jago", name: "GoPay Tabungan",
    })));
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("default_pocket_id");
  });

  it("keeps a rejected pocket name available and associates the error with its field", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValue(new Error("Nama kantong sudah digunakan"));
    renderAccounts();
    await user.click(within(await openDrawer(user, "Broker", "Broker")).getByRole("button", { name: /Tambah kantong/ }));
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
    await user.click(within(await openDrawer(user, "Broker", "Broker")).getByRole("button", { name: "Sesuaikan Saldo" }));
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

  it("lists twelve pockets inside their account card, each a button naming the account", async () => {
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
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });

    renderAccounts();
    const jago = await row("Jago");
    expect(jago).toHaveTextContent("Bank · 12 kantong");
    expect(within(jago).getAllByText("Rp 1.200.000").length).toBeGreaterThan(0);
    expect(within(within(jago).getByRole("list", { name: "Kantong Jago" })).getAllByRole("button", { name: /, kantong di Jago$/ })).toHaveLength(12);
    expect(screen.getByRole("button", { name: "Jago" })).toHaveAccessibleDescription(/Bank · 12 kantong/);
    expect((await axe(jago)).violations).toHaveLength(0);
  });

  it("keeps investment and liquid pocket actions specific to their account types", async () => {
    const position = { id: "shares", parent_id: "broker", name: "BBCA", type: "investment", instrument_type: "stock", instrument_symbol: "BBCA.JK", units: 100, balance: 600_000, initial_balance: 500_000, capital_gain: 100_000, capital_gain_pct: 20, is_archived: false, created_at: "2026-09-01T00:00:00.000Z" };
    const funding = { id: "funding", parent_id: "broker", name: "RDN", type: "bank", balance: 500_000, initial_balance: 500_000, is_archived: false, created_at: "2026-09-01T00:00:00.000Z" };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [{ ...investmentParent, id: "broker", balance: 1_100_000, children: [position, funding] }, position, funding], total_balance: 1_100_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    const shares = await openDrawer(user, "BBCA, produk di Broker", "BBCA");
    expect(within(shares).getByRole("button", { name: "Beli" })).toBeInTheDocument();
    expect(within(shares).getByRole("button", { name: "Jual" })).toBeInTheDocument();
    expect(within(shares).getByRole("button", { name: "Perbarui nilai" })).toBeInTheDocument();
    expect(within(shares).getByRole("button", { name: "Sesuaikan Saldo" })).toBeInTheDocument();
    expect(shares).toHaveTextContent(/Modal\s*Rp 500\.000\s*Untung\/rugi\s*▲ \+Rp 100\.000 \(\+20%\)/);
    await user.click(within(shares).getByRole("button", { name: "Tutup" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "BBCA, produk di Broker" })).toHaveFocus());

    const rdn = await openDrawer(user, "RDN, kantong di Broker", "RDN");
    expect(within(rdn).queryByRole("button", { name: "Beli" })).not.toBeInTheDocument();
    expect(within(rdn).queryByRole("button", { name: /pindah saldo/i })).not.toBeInTheDocument();
  });

  it("persists explicit top-level and sibling-pocket moves from the card buttons, with no mode on desktop", async () => {
    const first = { id: "bank-a", name: "Bank A", type: "bank", balance: 0, initial_balance: 0, is_archived: false, created_at: "2026-09-01T00:00:00.000Z", children: [
      { id: "pocket-a", parent_id: "bank-a", name: "Poket A", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
      { id: "pocket-b", parent_id: "bank-a", name: "Poket B", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
    ] };
    const second = { id: "bank-b", name: "Bank B", type: "bank", balance: 0, initial_balance: 0, is_archived: false, created_at: "2026-09-01T00:00:00.000Z", children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [first, second], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });

    const user = userEvent.setup();
    renderAccounts();
    expect(await screen.findByRole("button", { name: "Naikkan Bank A" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Ubah urutan" })).not.toBeInTheDocument();
    const down = screen.getByRole("button", { name: "Turunkan Bank A" });
    await user.click(down);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["bank-b", "bank-a"] }));
    expect(screen.getByRole("status")).toHaveTextContent("Bank A dipindah ke posisi 2 dari 2.");

    await user.click(screen.getByRole("button", { name: "Naikkan Poket B" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["pocket-b", "pocket-a"] }));
  });

  it("switches phones to an Ubah urutan mode with move buttons and returns focus when done", async () => {
    class PhoneWidth {
      constructor(private readonly callback: ResizeObserverCallback) {}
      observe() { this.callback([{ contentRect: { width: 390 } } as ResizeObserverEntry], this as unknown as ResizeObserver); }
      unobserve() {}
      disconnect() {}
    }
    vi.stubGlobal("ResizeObserver", PhoneWidth);
    const bank = { id: "bank-a", name: "Bank A", type: "bank", balance: 0, initial_balance: 0, is_archived: false, children: [] };
    const second = { ...bank, id: "bank-b", name: "Bank B" };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [bank, second], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    try {
      const user = userEvent.setup();
      renderAccounts();
      expect(document.querySelector('[draggable="true"]')).toBeNull();
      await user.click(await screen.findByRole("button", { name: "Ubah urutan" }));
      await user.click(screen.getByRole("button", { name: "Turunkan Bank A" }));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["bank-b", "bank-a"] }));
      await user.click(screen.getByRole("button", { name: "Selesai" }));
      await waitFor(() => expect(screen.getByRole("button", { name: "Ubah urutan" })).toHaveFocus());
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("requires archive confirmation and reports a failed archive", async () => {
    const user = userEvent.setup();
    vi.mocked(api.del).mockRejectedValue(new Error("Arsip gagal disimpan"));
    renderAccounts();
    const drawer = await openDrawer(user, "Broker", "Broker");
    await user.click(within(drawer).getByRole("button", { name: "Arsipkan rekening Broker" }));
    expect(api.del).not.toHaveBeenCalled();
    await user.click(within(drawer).getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/accounts/investment-parent"));
    expect(await screen.findAllByRole("alert", { name: "" })).toEqual(expect.arrayContaining([
      expect.objectContaining({ textContent: "Arsip gagal disimpan" }),
    ]));
  });

  it("reports reorder failures and restores the previous order", async () => {
    const second = { ...investmentParent, id: "second", name: "Second" };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [investmentParent, second], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    vi.mocked(api.post).mockRejectedValue(new Error("Network unavailable"));
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Turunkan Broker" }));
    expect((await screen.findAllByRole("alert")).some((alert) => alert.textContent?.includes("Urutan tidak tersimpan"))).toBe(true);
    const cards = [...document.querySelectorAll("section[data-account-row]")].map((card) => card.getAttribute("aria-label"));
    expect(cards).toEqual(["Broker", "Second"]);
  });

  it("keeps pointer drag ordering on account cards and sibling pockets", async () => {
    const pockets = [
      { id: "first-pocket", parent_id: "first", name: "First pocket", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
      { id: "second-pocket", parent_id: "first", name: "Second pocket", type: "bank", balance: 0, initial_balance: 0, is_archived: false },
    ];
    const first = { ...investmentParent, id: "first", name: "First", type: "bank", children: pockets };
    const targetPocket = { id: "target-pocket", parent_id: "second", name: "Target pocket", type: "bank", balance: 0, initial_balance: 0, is_archived: false };
    const second = { ...investmentParent, id: "second", name: "Second", type: "bank", children: [targetPocket] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [first, second, ...pockets, targetPocket], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    renderAccounts();
    const firstCard = await row("First");
    const secondCard = await row("Second");
    const dragData = { setData: vi.fn(), effectAllowed: "move", dropEffect: "move" };

    fireEvent.dragStart(firstCard.querySelector(':scope > div [draggable="true"]')!, { dataTransfer: dragData });
    fireEvent.dragOver(secondCard, { dataTransfer: dragData });
    fireEvent.drop(secondCard, { dataTransfer: dragData });
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["second", "first"] }));

    const [firstPocket, secondPocket] = within(within(await row("First")).getByRole("list", { name: "Kantong First" })).getAllByRole("listitem");
    fireEvent.dragStart(firstPocket.querySelector('[draggable="true"]')!, { dataTransfer: dragData });
    fireEvent.dragOver(secondPocket, { dataTransfer: dragData });
    fireEvent.drop(secondPocket, { dataTransfer: dragData });
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/reorder", { account_ids: ["second-pocket", "first-pocket"] }));
  });

  it("flags accounts and pockets that need reconciliation and opens Sesuaikan Saldo from the flag", async () => {
    const flaggedPocket = {
      id: "jago-main", parent_id: "jago", name: "Kantong Utama", type: "bank", initial_balance: 0, balance: -25_000,
      is_archived: false, reconciliation_required: true, created_at: "2026-09-01T00:00:00.000Z",
    };
    const calmPocket = { ...flaggedPocket, id: "jago-save", name: "Tabungan", balance: 10_000, reconciliation_required: false };
    const jago = { ...investmentParent, id: "jago", name: "Bank Jago", type: "bank", balance: -15_000, children: [flaggedPocket, calmPocket] };
    const cash = { ...investmentParent, id: "cash", name: "Kas", type: "cash", balance: -5_000, reconciliation_required: true };
    const fine = { ...investmentParent, id: "fine", name: "GoPay", type: "wallet", balance: 5_000, reconciliation_required: false };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [jago, cash, fine, flaggedPocket, calmPocket], total_balance: -15_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();

    expect(within(await row("Kas")).getByRole("button", { name: "Perlu rekonsiliasi: sesuaikan saldo Kas" })).toHaveTextContent("Perlu rekonsiliasi");
    expect(within(await row("Bank Jago")).getByText("1 kantong perlu rekonsiliasi")).toBeInTheDocument();
    const pocketRow = await row("Kantong Utama, kantong di Bank Jago");
    expect(within(pocketRow).getByRole("button", { name: "Perlu rekonsiliasi: sesuaikan saldo Kantong Utama" })).toBeInTheDocument();
    expect(within(await row("Tabungan, kantong di Bank Jago")).queryByText(/rekonsiliasi/i)).not.toBeInTheDocument();
    expect(within(await row("GoPay")).queryByText(/rekonsiliasi/i)).not.toBeInTheDocument();
    expect((await axe(pocketRow.closest("section")!)).violations).toHaveLength(0);

    const pocketFlag = within(pocketRow).getByRole("button", { name: "Perlu rekonsiliasi: sesuaikan saldo Kantong Utama" });
    pocketFlag.focus();
    await user.keyboard("{Enter}");
    const dialog = screen.getByRole("dialog", { name: "Sesuaikan Saldo: Kantong Utama" });
    const actualBalance = within(dialog).getByRole("textbox", { name: /saldo sebenarnya/i });
    await user.clear(actualBalance);
    await user.type(actualBalance, "0");
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") {
        const cleared = { ...flaggedPocket, balance: 0, reconciliation_required: false };
        return { ok: true, accounts: [{ ...jago, children: [cleared, calmPocket] }, cash, fine, cleared, calmPocket], total_balance: 10_000 };
      }
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    await user.click(within(dialog).getByRole("button", { name: "Konfirmasi & Sesuaikan Saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/jago-main/reconcile", { actual_balance: 0, notes: null }));
    await waitFor(() => expect(screen.queryByText("1 kantong perlu rekonsiliasi")).not.toBeInTheDocument());
    expect(screen.queryByRole("button", { name: /sesuaikan saldo Kantong Utama/ })).not.toBeInTheDocument();
  });

  it("shows a flagged amount-tracked product without offering a reconciliation it cannot take", async () => {
    const product = {
      ...investmentParent, id: "fund", name: "Reksa Contoh", investment_tracking_mode: "amount", reconciliation_required: true,
    };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [product], total_balance: 0 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    const user = userEvent.setup();
    renderAccounts();
    const productRow = await row("Reksa Contoh");
    expect(within(productRow).getByText("Perlu rekonsiliasi")).toBeInTheDocument();
    expect(within(productRow).queryByRole("button", { name: /perlu rekonsiliasi/i })).not.toBeInTheDocument();
    const drawer = await openDrawer(user, "Reksa Contoh", "Reksa Contoh");
    expect(within(drawer).getByText("Perlu rekonsiliasi")).toBeInTheDocument();
    expect(within(drawer).queryByRole("button", { name: "Sesuaikan Saldo" })).not.toBeInTheDocument();
  });

  it("counts unfinished movements on the account card without amounts", async () => {
    const bank = { id: "bank", name: "Bank Utama", type: "bank", balance: 1_000, initial_balance: 0, is_archived: false, children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [bank, investmentParent], total_balance: 1_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
      if (path === "/review/queue") return { ok: true, awaiting: [{ id: "t1", account_id: "bank", amount: 50_000 }, { id: "t2", account_id: "bank", amount: 70_000 }] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    renderAccounts();
    expect(within(await row("Bank Utama")).getByText("2 pindah saldo belum lengkap")).toBeInTheDocument();
    expect(screen.queryByText(/Rp 120\.000|Rp 50\.000/)).not.toBeInTheDocument();
  });

  it("shows product and platform gains in the list, and says when a product has no cost basis", async () => {
    const gain = { id: "gain", parent_id: "platform", name: "Saham Naik", type: "investment", instrument_type: "stock", units: 300, last_price: 1200, avg_buy_price: 1000,
      balance: 360_000, initial_balance: 0, capital_gain: 60_000, capital_gain_pct: 20, cost_basis: 300_000, is_archived: false };
    const loss = { ...gain, id: "loss", name: "Saham Turun", balance: 150_000, capital_gain: -50_000, capital_gain_pct: -25, cost_basis: 200_000 };
    const unknown = { ...gain, id: "unknown", parent_id: "other", name: "Produk Tanpa Modal", capital_gain: null, capital_gain_pct: null, cost_basis: null };
    const platform = { ...investmentParent, id: "platform", name: "Platform", balance: 510_000, children: [gain, loss], group_capital_gain: 10_000, group_cost_basis: 500_000 };
    const other = { ...investmentParent, id: "other", name: "Lainnya", balance: 360_000, children: [unknown], group_capital_gain: null, group_cost_basis: null };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [platform, other], total_balance: 870_000 };
      if (path === "/dashboard/net-worth") return { ok: true, net_worth: 870_000, total_assets: 870_000, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 870_000, reconciliation_pending: 0, lines: [] };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    renderAccounts();
    expect(await row("Saham Naik, produk di Platform")).toHaveTextContent("▲ +Rp 60.000 (+20%) untung");
    expect(await row("Saham Turun, produk di Platform")).toHaveTextContent("▼ −Rp 50.000 (−25%) rugi");
    expect(await row("Platform")).toHaveTextContent("▲ +Rp 10.000 (+2%) untung");
    expect(await row("Produk Tanpa Modal, produk di Lainnya")).toHaveTextContent("Modal belum dicatat");
    expect(within(screen.getByRole("region", { name: "Ringkasan aset" })).getByText("Modal belum lengkap")).toBeInTheDocument();
  });

  it("keeps stored account colours and gives default-blue accounts distinct hues for display only", async () => {
    const { displayColours } = await import("@/components/accounts/AccountsView");
    const accounts = [
      { id: "a", name: "A", color: "#3b82f6" }, { id: "b", name: "B", color: "#3B82F6" }, { id: "c", name: "C", color: "#F97316" }, { id: "d", name: "D" },
    ] as never[];
    const colours = displayColours(accounts);
    expect(colours.get("c")).toBe("#F97316");
    const assigned = [colours.get("a"), colours.get("b"), colours.get("d")];
    expect(new Set(assigned).size).toBe(3);
    // No assigned hue sits next to the stored orange.
    expect(assigned).not.toContain("#F08A24");
    // Two accounts stored with the same colour do not look alike.
    const twins = displayColours([{ id: "x", name: "X", color: "#8B5CF6" }, { id: "y", name: "Y", color: "#8B5CF6" }] as never[]);
    expect(twins.get("x")).toBe("#8B5CF6");
    expect(twins.get("y")).not.toBe("#8B5CF6");
  });

  it("opens a drawer from ?akun= and masks every amount when balances are hidden", async () => {
    const bank = { id: "bank", name: "Bank Utama", type: "bank", balance: 1_000, initial_balance: 0, is_archived: false, color: "#2563EB", children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [bank], total_balance: 1_000 };
      if (path === "/dashboard/net-worth") return { ...{ ok: true, net_worth: 1000, total_assets: 1000, total_liabilities: 0, accounts: [], obligations: [], liquid: 1000, investment: 0, reconciliation_pending: 0, lines: [{ key: "liquid", label: "Dana likuid", amount: 1000 }] } };
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [] };
    });
    realReplace(null, "", "/accounts?akun=bank");
    hidden.on = true;
    renderAccounts();
    const drawer = await screen.findByRole("dialog", { name: /^Bank Utama/ });
    expect(within(drawer).getByRole("button", { name: /Tambah kantong/ })).toBeInTheDocument();
    expect(screen.queryByText(/Rp 1\.000/)).not.toBeInTheDocument();
    // The colour mark is decorative; the kind is written beside it.
    expect(within(await row("Bank Utama")).getAllByText("Bank").length).toBeGreaterThan(0);
    hidden.on = false;
  });
});

describe("credit accounts in Rekening & Kantong", () => {
  const jago = { id: "jago", name: "Jago", type: "bank", initial_balance: 0, balance: 500_000, is_archived: false, children: [], created_at: "2026-09-01T00:00:00.000Z" };
  const spaylater = {
    id: "spaylater", name: "SPayLater", type: "credit", initial_balance: -350_000, balance: -470_000, is_archived: false, children: [],
    credit_limit: 2_000_000, payment_day: 11, credit_owed: 470_000, over_limit: false, created_at: "2026-11-05T00:00:00.000Z",
  };
  const netWorth = {
    ok: true, net_worth: 30_000, total_assets: 500_000, total_liabilities: 470_000, accounts: [], obligations: [],
    liquid: 500_000, investment: 0, reconciliation_pending: 0, credit_owed: 470_000,
    lines: [{ key: "liquid", label: "Dana likuid", amount: 500_000 }, { key: "investment", label: "Investasi", amount: 0 },
      { key: "credit:spaylater", label: "SPayLater", amount: -470_000 }],
  };

  function mockAccounts(accounts: unknown[]) {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts, total_balance: 0 };
      if (path === "/dashboard/net-worth") return netWorth;
      return { ok: true, rules: [], accounts: [], results: [], awaiting: [], transactions: [] };
    });
  }

  afterEach(() => {
    window.history.replaceState = realReplace;
    realReplace(null, "", "/accounts");
  });

  beforeEach(() => {
    vi.clearAllMocks();
    realReplace(null, "", "/accounts");
    window.history.replaceState = (state, title, url) => {
      realReplace(state, title, url);
      window.dispatchEvent(new Event("test-history"));
    };
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    mockAccounts([jago, spaylater]);
  });

  it("adds SPayLater as Kredit with what is owed today, a limit and a payment day, and no pocket option", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });
    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "SPayLater");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Jenis rekening" }), "credit");

    expect(within(dialog).queryByRole("textbox", { name: /Saldo Awal/ })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("combobox", { name: /kantong/i })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Warna Slate" })).toHaveAttribute("aria-pressed", "true");
    await user.type(within(dialog).getByRole("textbox", { name: "Utang saat ini (IDR)" }), "350000");
    await user.type(within(dialog).getByRole("textbox", { name: "Limit (opsional)" }), "2000000");
    await user.type(within(dialog).getByRole("spinbutton", { name: "Tanggal bayar tiap bulan (opsional)" }), "11");
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      name: "SPayLater", type: "credit", initial_balance: -350_000, credit_limit: 2_000_000, payment_day: 11, color: "#475569",
    })));
  });

  it("opens a credit account with no debt yet at zero, and refuses a payment day outside 1 to 31", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });
    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "Kredivo");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Jenis rekening" }), "credit");
    const day = within(dialog).getByRole("spinbutton", { name: "Tanggal bayar tiap bulan (opsional)" });
    await user.type(day, "32");
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));

    // The browser's own range check stops the save; the form also refuses it if that check is skipped.
    expect(day).toBeInvalid();
    fireEvent.submit(dialog.querySelector("form")!);
    await waitFor(() => expect(day).toHaveAttribute("aria-invalid", "true"));
    expect(day).toHaveAccessibleDescription("Tanggal bayar harus antara 1 dan 31.");
    expect(day).toHaveFocus();
    expect(api.post).not.toHaveBeenCalledWith("/accounts", expect.anything());

    await user.clear(day);
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      type: "credit", initial_balance: 0, credit_limit: null, payment_day: null,
    })));
    const body = vi.mocked(api.post).mock.calls.at(-1)![1] as { initial_balance: number };
    expect(Object.is(body.initial_balance, -0)).toBe(false);
  });

  it("does not send credit fields for a bank account", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });
    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "Bank Contoh");
    expect(within(dialog).queryByRole("textbox", { name: "Limit (opsional)" })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({ type: "bank" })));
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("credit_limit");
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("payment_day");
  });

  it("edits a credit account's name, limit and payment day without an owed amount or a parent", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "SPayLater", "SPayLater");
    await user.click(within(detail).getByRole("button", { name: "Ubah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Rekening: SPayLater" });
    expect(within(dialog).queryByRole("textbox", { name: /Utang saat ini/ })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("combobox", { name: "Jadikan kantong di bawah" })).not.toBeInTheDocument();
    const limit = within(dialog).getByRole("textbox", { name: "Limit (opsional)" });
    const day = within(dialog).getByRole("spinbutton", { name: "Tanggal bayar tiap bulan (opsional)" });
    expect(limit).toHaveValue("2.000.000");
    expect(day).toHaveValue(11);
    await user.clear(limit);
    await user.type(limit, "3000000");
    await user.clear(day);
    await user.type(day, "25");
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/spaylater", expect.objectContaining({
      name: "SPayLater", type: "credit", credit_limit: 3_000_000, payment_day: 25,
    })));
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("parent_id");
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("initial_balance");
  });

  it("does not offer a credit account as a parent for another account", async () => {
    const gopay = { ...jago, id: "gopay", name: "GoPay", type: "wallet" };
    mockAccounts([jago, gopay, spaylater]);
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "GoPay", "GoPay");
    await user.click(within(detail).getByRole("button", { name: "Ubah rekening" }));
    const parent = within(screen.getByRole("dialog", { name: "Ubah Rekening: GoPay" })).getByRole("combobox", { name: "Jadikan kantong di bawah" });
    expect(within(parent).getByRole("option", { name: "Jago" })).toBeInTheDocument();
    expect(within(parent).queryByRole("option", { name: "SPayLater" })).not.toBeInTheDocument();
  });

  it("shows what is owed, the limit used and the payment day on the card, with no pocket action", async () => {
    renderAccounts();
    const card = await row("SPayLater");
    expect(card).toHaveTextContent("Kredit");
    expect(card).toHaveTextContent("Utang Rp 470.000");
    expect(card).toHaveTextContent("Rp 470.000 dari Rp 2.000.000");
    expect(card).toHaveTextContent("dibayar tiap tgl 11");
    expect(card).not.toHaveTextContent("-Rp 470.000");
    expect(card).not.toHaveTextContent("Melewati limit");
    const bar = within(card).getByRole("progressbar", { name: "Limit terpakai SPayLater" });
    expect(bar).toHaveAttribute("aria-valuenow", "24");
    expect(bar).toHaveAttribute("aria-valuetext", "Rp 470.000 dari Rp 2.000.000, 24%");
    expect(within(card).queryByRole("button", { name: /Tambah kantong/ })).not.toBeInTheDocument();
    expect((await axe(card)).violations).toHaveLength(0);
    // Credit is utang, so it has no share in Komposisi aset.
    expect(within(screen.getByRole("list", { name: "Komposisi aset per rekening" })).queryByText("SPayLater")).not.toBeInTheDocument();
  });

  it("marks a credit account over its limit in words, and shows an overpayment as Lebih bayar", async () => {
    const over = { ...spaylater, balance: -2_100_000, credit_owed: 2_100_000, over_limit: true };
    const overpaid = { ...spaylater, id: "kredivo", name: "Kredivo", balance: 25_000, credit_owed: 0, credit_limit: null, payment_day: null };
    mockAccounts([jago, over, overpaid]);
    renderAccounts();
    const overCard = await row("SPayLater");
    expect(overCard).toHaveTextContent("Utang Rp 2.100.000");
    expect(overCard).toHaveTextContent("Melewati limit");
    expect(within(overCard).getByRole("progressbar")).toHaveAttribute("aria-valuenow", "100");
    const paidCard = await row("Kredivo");
    expect(paidCard).toHaveTextContent("Lebih bayar Rp 25.000");
    expect(paidCard).not.toHaveTextContent("Utang");
    expect(within(paidCard).queryByRole("progressbar")).not.toBeInTheDocument();
  });

  it("offers no Cocokkan saldo, Sesuaikan Saldo or Tambah kantong in a credit account's detail", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "SPayLater", "SPayLater");
    expect(detail).toHaveTextContent(/Utang\s*Rp 470\.000\s*Limit\s*Rp 2\.000\.000\s*Tanggal bayar\s*Tiap tgl 11/);
    expect(within(detail).queryByRole("button", { name: "Cocokkan saldo" })).not.toBeInTheDocument();
    expect(within(detail).queryByRole("button", { name: "Sesuaikan Saldo" })).not.toBeInTheDocument();
    expect(within(detail).queryByRole("button", { name: /Tambah kantong/ })).not.toBeInTheDocument();
    expect(within(detail).getByRole("button", { name: "Ubah rekening" })).toBeInTheDocument();
    expect((await axe(detail)).violations).toHaveLength(0);
  });

  it("counts credit in the server's net worth and names it as utang in the explanation", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const kpis = await screen.findByRole("region", { name: "Ringkasan aset" });
    expect(kpis).toHaveTextContent("setelah utang Rp 470.000");
    await user.click(within(kpis).getByRole("button", { name: "Kekayaan bersih Rp 30.000: lihat rincian" }));
    const explanation = await screen.findByRole("dialog", { name: "Kekayaan bersih" });
    expect(explanation).toHaveTextContent("Utang SPayLater");
  });
});
