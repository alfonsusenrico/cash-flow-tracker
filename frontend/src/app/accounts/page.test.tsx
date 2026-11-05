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

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), put: vi.fn(), del: vi.fn() } }));
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

const EMPTY_NET_WORTH = { ok: true, net_worth: 0, total_assets: 0, total_liabilities: 0, accounts: [], obligations: [], liquid: 0, investment: 0, reconciliation_pending: 0, lines: [] };
const OTHER = { ok: true, rules: [], accounts: [], results: [], awaiting: [], transactions: [] };

const realReplace = window.history.replaceState.bind(window.history);

/** The card of an account or pocket; pocket cards name their rekening for assistive technology. */
async function row(name: string | RegExp) {
  const button = (await screen.findAllByRole("button", { name })).find((element) => element.closest("[data-account-row]"));
  return button!.closest("[data-account-row]") as HTMLElement;
}

/** Opens the detail from a card or a rekening's row title and returns it. */
async function openDrawer(user: ReturnType<typeof userEvent.setup>, name: string | RegExp, title: string) {
  await user.click((await screen.findAllByRole("button", { name }))[0]);
  return screen.findByRole("dialog", { name: new RegExp(`^${title}`) });
}

/** Switches the rows to one row per rekening. */
async function perRekening(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("button", { name: "Per rekening" }));
}

/** The names of a row's cards, in their order. */
function cardsOf(rowTitle: string): (string | null)[] {
  const list = screen.getByRole("list", { name: `Kartu ${rowTitle}` });
  return [...list.querySelectorAll('button[id^="wallet-card-"]')].map((button) => button.getAttribute("aria-label"));
}

/** The rows' keys (uses or rekening ids), in their order. */
function railOrder(): (string | null)[] {
  return [...document.querySelectorAll("section[data-rail]")].map((section) => section.getAttribute("data-rail"));
}

function rail(key: string): HTMLElement {
  return document.querySelector(`section[data-rail="${key}"]`) as HTMLElement;
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

function serve(accounts: unknown[], netWorth: unknown = EMPTY_NET_WORTH, layouts: Record<string, unknown> = {}) {
  vi.mocked(api.get).mockImplementation(async (path) => {
    if (path === "/accounts") return { ok: true, accounts, total_balance: 0 };
    if (path === "/dashboard/net-worth") return netWorth;
    const layout = /^\/accounts\/layout\?grouping=(\w+)$/.exec(path);
    if (layout) return layouts[layout[1]] ?? { grouping: layout[1], sections: [], cards: {} };
    return OTHER;
  });
}

/** Node's own experimental localStorage global shadows jsdom's, so each test gets an in-memory one. */
function installStorage(overrides: Partial<Storage> = {}) {
  const data = new Map<string, string>();
  const storage = {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => void data.set(key, String(value)),
    removeItem: (key: string) => void data.delete(key),
    clear: () => data.clear(),
    key: (index: number) => [...data.keys()][index] ?? null,
    get length() {
      return data.size;
    },
    ...overrides,
  };
  Object.defineProperty(window, "localStorage", { configurable: true, value: storage });
}

function resetPage() {
  vi.clearAllMocks();
  installStorage();
  realReplace(null, "", "/accounts");
  window.history.replaceState = (state, title, url) => {
    realReplace(state, title, url);
    window.dispatchEvent(new Event("test-history"));
  };
}

function restorePage() {
  window.history.replaceState = realReplace;
  realReplace(null, "", "/accounts");
}

describe("account and pocket forms", () => {
  afterEach(restorePage);

  beforeEach(() => {
    resetPage();
    serve([investmentParent]);
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("does not present assets as net worth when net worth is unavailable", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { ok: true, accounts: [liquid], total_balance: 1000 };
      if (path === "/dashboard/net-worth") throw new Error("unavailable");
      return OTHER;
    });
    renderAccounts();
    expect(await screen.findByText(/Kekayaan bersih belum dapat dihitung/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /^Kekayaan bersih Rp/ })).not.toBeInTheDocument();
    // The account's own balance stays visible; no client-side sum stands in for the server figure.
    expect(within(await row("Cash A")).getByText("Rp 1.000")).toBeInTheDocument();
  });

  it("shows the server's net worth and its parts in the summary, with the owed lines in the explanation", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    serve([liquid], {
      ok: true, net_worth: 450, total_assets: 1000, total_liabilities: 500, accounts: [], obligations: [],
      liquid: 1000, dana_investasi: 0, investment: 0, reconciliation_pending: 50,
      lines: [{ key: "liquid", label: "Dana likuid", amount: 1000 }, { key: "investment", label: "Investasi", amount: 0 },
        { key: "reconciliation_pending", label: "Saldo minus perlu dicocokkan", amount: -50 }, { key: "debt:d1", label: "Cicilan Contoh", amount: -500 }],
    });
    const user = userEvent.setup();
    renderAccounts();
    const summary = await screen.findByRole("region", { name: "Ringkasan" });
    expect(await within(summary).findByText("Saldo tersedia")).toBeInTheDocument();
    expect(summary).toHaveTextContent(/Saldo tersedia\s*Rp 1\.000/);
    expect(summary).toHaveTextContent(/Utang\s*−Rp 500/);
    expect(summary).toHaveTextContent("Belum ada investasi");
    expect(within(within(summary).getByRole("list", { name: "Porsi aset per kegunaan" })).getAllByRole("listitem").map((item) => item.textContent))
      .toEqual(["Uang harian 100%", "Tabungan 0%", "Dana investasi 0%", "Investasi 0%"]);
    await user.click(within(summary).getByRole("button", { name: "Kekayaan bersih Rp 450: lihat rincian" }));
    const explanation = await screen.findByRole("dialog", { name: "Kekayaan bersih" });
    expect(explanation).toHaveTextContent("Saldo tersedia");
    expect(explanation).toHaveTextContent("Saldo minus perlu dicocokkan");
    expect(explanation).toHaveTextContent("Cicilan Contoh");
  });

  it("removes dedicated transfer controls from account actions", async () => {
    const liquid = [
      { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] },
      { id: "cash-b", name: "Cash B", type: "bank", initial_balance: 0, balance: 0, is_archived: false, children: [] },
    ];
    serve([investmentParent, ...liquid]);
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
      if (path === "/dashboard/net-worth") return EMPTY_NET_WORTH;
      if (path.startsWith("/transactions?account_id=bca")) {
        return { ok: true, transactions: [{ id: "t1", account_id: "bca", account_name: "BCA", category_name: "Makan", type: "expense", amount: 45_000, date: "2026-11-03T14:04:00.000Z", notes: null, merchant: "Warung Sari" }] };
      }
      return OTHER;
    });
    const user = userEvent.setup();
    renderAccounts();
    // A rekening with pockets has no card of its own; its row title opens it under Per rekening.
    await perRekening(user);
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
      if (path === "/dashboard/net-worth") return EMPTY_NET_WORTH;
      if (path.startsWith("/transactions?account_id=tab")) {
        return { ok: true, transactions: [
          { ...leg, id: "in", type: "income", amount: 500_000, date: "2026-11-03T18:41:00.000Z", movement_id: "m1", movement_role: "inbound",
            transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null },
          { ...leg, id: "out", type: "expense", amount: 150_000, date: "2026-11-03T14:10:00.000Z", movement_id: "m2", movement_role: "outbound",
            transfer_target_account_name: "Tabungan Rumah", transfer_target_account_parent_name: "BCA" },
        ] };
      }
      return OTHER;
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

  it("adds a pocket from the detail with its Kegunaan", async () => {
    const liquid = { id: "cash-a", name: "Cash A", type: "cash", initial_balance: 1000, balance: 1000, is_archived: false, children: [] };
    serve([liquid]);
    const user = userEvent.setup();
    renderAccounts();
    const drawer = await openDrawer(user, "Cash A", "Cash A");
    await user.click(within(drawer).getByRole("button", { name: /Tambah kantong/ }));
    const dialog = screen.getByRole("dialog", { name: "Tambah Kantong di Cash A" });
    await user.type(within(dialog).getByRole("textbox", { name: "Nama kantong" }), "Tabungan");
    const role = within(dialog).getByRole("combobox", { name: "Kegunaan" });
    expect(role).toHaveValue("harian");
    expect(role).toHaveAccessibleDescription(/Dana investasi adalah uang yang belum dibelikan/);
    await user.selectOptions(role, "tabungan");
    await user.click(within(dialog).getByRole("button", { name: "Buat Kantong" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      parent_id: "cash-a",
      name: "Tabungan",
      role: "tabungan",
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
    serve([position]);
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
    serve(child ? [parent, product, source] : [product, source]);
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
    expect(within(dialog).getByText("Investasi")).toBeInTheDocument();
    expect(within(dialog).queryByRole("combobox", { name: "Kegunaan" })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: /kas \/ tabungan biasa/i }));
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.type(within(dialog).getByRole("textbox", { name: /nama kantong/i }), "RDN");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Kegunaan" }), "dana_investasi");
    await user.type(within(dialog).getByRole("textbox", { name: /saldo awal kantong/i }), "500000");
    await user.click(within(dialog).getByRole("button", { name: "Buat Kantong" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({
      parent_id: "investment-parent",
      name: "RDN",
      type: "bank",
      initial_balance: 500_000,
      instrument_type: null,
      role: "dana_investasi",
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
      role: "investasi",
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
      role: "investasi",
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
    serve([{ ...investmentParent, children: [position] }, position]);
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const drawer = await openDrawer(user, "Stock, produk di Broker", "Stock");
    await user.click(within(drawer).getByRole("button", { name: "Ubah kantong" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Kantong: Stock" });
    await user.click(within(dialog).getByRole("button", { name: /kas \/ tabungan biasa/i }));
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/position-pocket", expect.objectContaining({
      type: "bank",
      role: "harian",
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
    serve([parent, child]);
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    // Under Per kegunaan a rekening with pockets is reached from its pocket's detail.
    const pocket = await openDrawer(user, "Daily pocket, kantong di BCA", "Daily pocket");
    await user.click(within(pocket).getByRole("button", { name: "Buka BCA" }));
    const drawer = await screen.findByRole("dialog", { name: /^BCA/ });
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

  it("moves a stand-alone wallet under a bank with Pindahkan ke rekening, keeping its history", async () => {
    const user = userEvent.setup();
    const jago = {
      id: "jago", name: "Jago", type: "bank", initial_balance: 0, balance: 0, is_archived: false,
      children: [], created_at: "2026-09-01T00:00:00.000Z",
    };
    const gopay = {
      id: "gopay", name: "GoPay", type: "wallet", initial_balance: 0, balance: 0, is_archived: false,
      children: [], created_at: "2026-09-01T00:00:00.000Z",
    };
    serve([jago, gopay, investmentParent]);
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    renderAccounts();

    const drawer = await openDrawer(user, "GoPay", "GoPay");
    await user.click(within(drawer).getByRole("button", { name: "Ubah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Rekening: GoPay" });
    expect(within(dialog).queryByText("Jadikan kantong di bawah")).not.toBeInTheDocument();
    const parentSelect = within(dialog).getByRole("combobox", { name: "Pindahkan ke rekening" });
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

  it("keeps investment and liquid pocket actions specific to their account types", async () => {
    const position = { id: "shares", parent_id: "broker", name: "BBCA", type: "investment", instrument_type: "stock", instrument_symbol: "BBCA.JK", units: 100, balance: 600_000, initial_balance: 500_000, capital_gain: 100_000, capital_gain_pct: 20, is_archived: false, created_at: "2026-09-01T00:00:00.000Z" };
    const funding = { id: "funding", parent_id: "broker", name: "RDN", type: "bank", balance: 500_000, initial_balance: 500_000, is_archived: false, created_at: "2026-09-01T00:00:00.000Z" };
    serve([{ ...investmentParent, id: "broker", balance: 1_100_000, children: [position, funding] }, position, funding]);

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

  it("flags accounts and pockets that need reconciliation and opens Sesuaikan Saldo from the flag", async () => {
    const flaggedPocket = {
      id: "jago-main", parent_id: "jago", name: "Kantong Utama", type: "bank", initial_balance: 0, balance: -25_000,
      is_archived: false, reconciliation_required: true, created_at: "2026-09-01T00:00:00.000Z",
    };
    const calmPocket = { ...flaggedPocket, id: "jago-save", name: "Tabungan", balance: 10_000, reconciliation_required: false };
    const jago = { ...investmentParent, id: "jago", name: "Bank Jago", type: "bank", balance: -15_000, children: [flaggedPocket, calmPocket] };
    const cash = { ...investmentParent, id: "cash", name: "Kas", type: "cash", balance: -5_000, reconciliation_required: true };
    const fine = { ...investmentParent, id: "fine", name: "GoPay", type: "wallet", balance: 5_000, reconciliation_required: false };
    serve([jago, cash, fine, flaggedPocket, calmPocket]);
    const user = userEvent.setup();
    renderAccounts();
    await perRekening(user);

    expect(within(await row("Kas")).getByRole("button", { name: "Perlu rekonsiliasi: sesuaikan saldo Kas" })).toHaveTextContent("Perlu rekonsiliasi");
    expect(within(rail("jago")).getByText("1 kantong perlu rekonsiliasi")).toBeInTheDocument();
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
    const cleared = { ...flaggedPocket, balance: 0, reconciliation_required: false };
    serve([{ ...jago, children: [cleared, calmPocket] }, cash, fine, cleared, calmPocket]);
    await user.click(within(dialog).getByRole("button", { name: "Konfirmasi & Sesuaikan Saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts/jago-main/reconcile", { actual_balance: 0, notes: null }));
    await waitFor(() => expect(screen.queryByText("1 kantong perlu rekonsiliasi")).not.toBeInTheDocument());
    expect(screen.queryByRole("button", { name: /sesuaikan saldo Kantong Utama/ })).not.toBeInTheDocument();
  });

  it("shows a flagged amount-tracked product without offering a reconciliation it cannot take", async () => {
    const product = {
      ...investmentParent, id: "fund", name: "Reksa Contoh", investment_tracking_mode: "amount", reconciliation_required: true,
    };
    serve([product]);
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
      if (path === "/dashboard/net-worth") return EMPTY_NET_WORTH;
      if (path === "/review/queue") return { ok: true, awaiting: [{ id: "t1", account_id: "bank", amount: 50_000 }, { id: "t2", account_id: "bank", amount: 70_000 }] };
      return OTHER;
    });
    renderAccounts();
    expect(within(await row("Bank Utama")).getByText("2 pindah saldo belum lengkap")).toBeInTheDocument();
    expect(screen.queryByText(/Rp 120\.000|Rp 50\.000/)).not.toBeInTheDocument();
  });

  it("keeps stored account colours and gives default-blue accounts distinct hues for display only", async () => {
    const { displayColours } = await import("@/components/accounts/displayColours");
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
    serve([bank], { ...EMPTY_NET_WORTH, net_worth: 1000, total_assets: 1000, liquid: 1000, lines: [{ key: "liquid", label: "Dana likuid", amount: 1000 }] });
    realReplace(null, "", "/accounts?akun=bank");
    hidden.on = true;
    try {
      renderAccounts();
      const drawer = await screen.findByRole("dialog", { name: /^Bank Utama/ });
      expect(within(drawer).getByRole("button", { name: /Tambah kantong/ })).toBeInTheDocument();
      expect(screen.queryByText(/Rp 1\.000/)).not.toBeInTheDocument();
      // The rekening mark is decorative; the kind is written beside it.
      expect(within(await row("Bank Utama")).getAllByText("Bank").length).toBeGreaterThan(0);
    } finally {
      hidden.on = false;
    }
  });
});

/* A synthetic owner shaped like the accepted preview (no personal data): BCA with cash, an RDN and three products,
   Bank Jago with a daily and a savings pocket, Stockbit with two shares, and SPayLater. */
const base = { type: "bank", initial_balance: 0, is_archived: false, children: [], created_at: "2026-09-01T00:00:00.000Z" };
const product = (id: string, parent: string, name: string, kind: string, instrument: string, balance: number, gain: number | null) => ({
  ...base, id, parent_id: parent, name, type: "investment", instrument_type: instrument, role: "investasi", kind, balance,
  cost_basis: gain == null ? null : balance - gain, capital_gain: gain, capital_gain_pct: null,
});
const bcaPockets = [
  { ...base, id: "bca-atm", parent_id: "bca", name: "Tabungan BCA (ATM)", role: "harian", balance: 5_400_000 },
  { ...base, id: "bca-rdn", parent_id: "bca", name: "RDN BCA", role: "dana_investasi", balance: 1_200_000 },
  product("bca-emas", "bca", "Tabungan Emas", "emas", "gold", 1_300_000, 51_200),
  product("bca-rdpu", "bca", "RD Pasar Uang", "reksa_dana", "mutual_fund", 2_500_000, 18_400),
  product("bca-goals", "bca", "Investment Goals", "reksa_dana", "mutual_fund", 4_000_000, 47_400),
];
const jagoPockets = [
  { ...base, id: "jago-main", parent_id: "jago", name: "Main Pocket", role: "harian", balance: 1_150_000 },
  { ...base, id: "jago-darurat", parent_id: "jago", name: "Dana Darurat", role: "tabungan", balance: 5_700_000 },
];
const stockbitProducts = [
  product("sb-bbri", "stockbit", "BBRI", "saham", "stock", 3_300_000, -67_300),
  product("sb-bbca", "stockbit", "BBCA", "saham", "stock", 1_800_000, 203_300),
];
const owner = [
  { ...base, id: "bca", name: "BCA", role: "harian", balance: 14_400_000, own_balance: 0, children: bcaPockets },
  { ...base, id: "jago", name: "Bank Jago", role: "harian", balance: 6_850_000, own_balance: 0, children: jagoPockets },
  { ...base, id: "stockbit", name: "Stockbit", type: "investment", role: "investasi", balance: 5_100_000, own_balance: 0, children: stockbitProducts },
  { ...base, id: "spaylater", name: "SPayLater", type: "credit", role: "utang", balance: -470_000, credit_owed: 470_000, credit_limit: 2_000_000, payment_day: 11 },
];
const ownerNetWorth = {
  ok: true, net_worth: 25_880_000, total_assets: 26_350_000, total_liabilities: 470_000, accounts: [], obligations: [],
  liquid: 12_250_000, dana_investasi: 1_200_000, investment: 12_900_000, reconciliation_pending: 0, credit_owed: 470_000,
  lines: [{ key: "liquid", label: "Dana likuid", amount: 12_250_000 }, { key: "dana_investasi", label: "Dana investasi", amount: 1_200_000 },
    { key: "investment", label: "Investasi", amount: 12_900_000 }, { key: "credit:spaylater", label: "SPayLater", amount: -470_000 }],
};
const USE_ROWS = ["harian", "tabungan", "dana_investasi", "investasi", "utang"];

describe("Rekening & Kantong by use and by rekening", () => {
  afterEach(restorePage);

  beforeEach(() => {
    resetPage();
    serve(owner, ownerNetWorth);
    vi.mocked(api.put).mockImplementation(async (_path, body) => body);
  });

  it("opens on Kartu grouped Per kegunaan, with a row per use, Investasi sub-totals and the server's figures", async () => {
    renderAccounts();
    const summary = await screen.findByRole("region", { name: "Ringkasan" });
    expect(await within(summary).findByRole("button", { name: "Kekayaan bersih Rp 25.880.000: lihat rincian" })).toBeInTheDocument();
    expect(summary).toHaveTextContent(/Saldo tersedia\s*Rp 12\.250\.000/);
    expect(summary).toHaveTextContent(/Dana investasi\s*Rp 1\.200\.000/);
    expect(summary).toHaveTextContent(/Investasi\s*Rp 12\.900\.000/);
    expect(summary).toHaveTextContent(/Untung\/Rugi\s*▲Rp 253\.000 \(2,0%\) untung/);
    expect(summary).toHaveTextContent(/Utang\s*−Rp 470\.000/);
    expect(screen.getByRole("tab", { name: "Kartu" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("button", { name: "Per kegunaan" })).toHaveAttribute("aria-pressed", "true");

    expect(railOrder()).toEqual(USE_ROWS);
    expect(rail("harian")).toHaveTextContent(/Uang harian\s*2 kantong.*Rp 6\.550\.000/);
    expect(rail("tabungan")).toHaveTextContent("Rp 5.700.000");
    expect(rail("dana_investasi")).toHaveTextContent(/Dana investasi\s*belum dibelikan/);
    expect(rail("utang")).toHaveTextContent("−Rp 470.000");
    expect(cardsOf("Uang harian")).toEqual(["Tabungan BCA (ATM), kantong di BCA", "Main Pocket, kantong di Bank Jago"]);

    const kinds = within(rail("investasi")).getByRole("list", { name: "Investasi per jenis" });
    expect(within(kinds).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "Emas Rp 1.300.000▲Rp 51.200 (4,1%) untung",
      "Reksa dana Rp 6.500.000▲Rp 65.800 (1,0%) untung",
      "Saham Rp 5.100.000▲Rp 136.000 (2,7%) untung",
    ]);
    // Each product card shows its own gain, the debt card what it owes and its limit.
    expect(await row("BBRI, produk di Stockbit")).toHaveTextContent("▼Rp 67.300 (2,0%) rugi");
    const debt = await row("SPayLater");
    expect(debt).toHaveTextContent("−Rp 470.000");
    expect(within(debt).getByRole("progressbar", { name: "Limit terpakai SPayLater" })).toHaveAttribute("aria-valuenow", "24");

    const ring = within(summary).getByRole("group", { name: "Porsi aset per kegunaan" });
    expect(within(ring).getAllByRole("button").map((segment) => segment.getAttribute("aria-label"))).toEqual([
      "Uang harian Rp 6.550.000, 25% aset, lompat ke baris",
      "Tabungan Rp 5.700.000, 22% aset, lompat ke baris",
      "Dana investasi Rp 1.200.000, 5% aset, lompat ke baris",
      "Investasi Rp 12.900.000, 49% aset, lompat ke baris",
    ]);
    expect((await axe(screen.getByRole("tabpanel", { name: "Kartu" }))).violations).toHaveLength(0);
  });

  it("jumps from a ring segment to its row", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const segment = await screen.findByRole("button", { name: /^Investasi Rp 12\.900\.000, 49% aset/ });
    segment.focus();
    await user.keyboard("{Enter}");
    expect(screen.getByRole("heading", { name: /^Investasi/, level: 2 })).toHaveFocus();
  });

  it("shows each rekening as a row with its composition by use, the use on every card and the gains on BCA", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await perRekening(user);
    expect(railOrder()).toEqual(["bca", "jago", "stockbit", "spaylater"]);
    const bca = rail("bca");
    expect(bca).toHaveTextContent(/▲Rp 117\.000 \(1,5%\) untung\s*Rp 14\.400\.000/);
    expect(within(within(bca).getByRole("list", { name: "Isi BCA" })).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "Uang harian Rp 5.400.000", "Dana investasi Rp 1.200.000", "Investasi Rp 7.800.000",
    ]);
    expect(await row("Tabungan Emas, produk di BCA")).toHaveTextContent(/Investasi.*Tabungan Emas.*Emas.*▲Rp 51\.200 \(4,1%\) untung/);
    expect(await row("RDN BCA, kantong di BCA")).toHaveTextContent("Dana investasi");
    expect(await row("Main Pocket, kantong di Bank Jago")).toHaveTextContent("Uang harian");

    const compo = within(screen.getByRole("region", { name: "Ringkasan" })).getByRole("list", { name: "Isi tiap rekening" });
    expect(within(compo).getByRole("img", { name: "Isi BCA: Uang harian 38%, Dana investasi 8%, Investasi 54%" })).toBeInTheDocument();
    expect(within(compo).queryByText("SPayLater")).not.toBeInTheDocument();
    expect((await axe(screen.getByRole("tabpanel", { name: "Kartu" }))).violations).toHaveLength(0);
  });

  it("remembers the grouping in this browser", async () => {
    const user = userEvent.setup();
    const first = renderAccounts();
    await perRekening(user);
    expect(window.localStorage.getItem("rekening-grouping")).toBe("rekening");
    first.unmount();

    renderAccounts();
    await waitFor(() => expect(screen.getByRole("button", { name: "Per rekening" })).toHaveAttribute("aria-pressed", "true"));
    expect(railOrder()).toEqual(["bca", "jago", "stockbit", "spaylater"]);
  });

  it("opens on Per kegunaan when browser storage is blocked", async () => {
    const blocked = () => {
      throw new Error("blocked");
    };
    installStorage({ getItem: blocked, setItem: blocked });
    const user = userEvent.setup();
    renderAccounts();
    expect(await screen.findByRole("button", { name: "Per kegunaan" })).toHaveAttribute("aria-pressed", "true");
    // Without storage the choice still works for this visit.
    await perRekening(user);
    expect(railOrder()).toEqual(["bca", "jago", "stockbit", "spaylater"]);
  });

  it("says Modal belum diisi instead of a partial gain when one product has no cost basis", async () => {
    const bibit = {
      ...base, id: "bibit", name: "Bibit", type: "investment", role: "investasi", balance: 11_400_000, own_balance: 0,
      children: [product("bibit-a", "bibit", "RD Pasar Uang A", "reksa_dana", "mutual_fund", 5_700_000, 112_400),
        product("bibit-b", "bibit", "RD Kas B", "reksa_dana", "mutual_fund", 5_700_000, null)],
    };
    serve([...owner, bibit], ownerNetWorth);
    const user = userEvent.setup();
    renderAccounts();
    const summary = await screen.findByRole("region", { name: "Ringkasan" });
    expect(await within(summary).findByText("Modal belum diisi")).toBeInTheDocument();
    expect(summary).not.toHaveTextContent("untung");
    expect(within(rail("investasi")).getAllByText("Modal belum diisi").length).toBeGreaterThan(0);
    expect(await row("RD Kas B, produk di Bibit")).toHaveTextContent("Modal belum diisi");
    expect(await row("RD Pasar Uang A, produk di Bibit")).toHaveTextContent("▲Rp 112.400 (2,0%) untung");

    await perRekening(user);
    const head = rail("bibit").querySelector("h2")!.parentElement!.parentElement!;
    expect(head).toHaveTextContent("Modal belum diisi");
    expect(head).not.toHaveTextContent("▲");
    expect(rail("bca")).toHaveTextContent("▲Rp 117.000 (1,5%) untung");
  });

  it("shows Grafik as two rings beside the chosen use's list, by kind for Investasi and by rekening Per rekening", async () => {
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("tab", { name: "Kartu" }));
    await user.keyboard("{ArrowRight}");
    const tab = screen.getByRole("tab", { name: "Grafik" });
    expect(tab).toHaveAttribute("aria-selected", "true");
    expect(tab).toHaveFocus();
    const panel = screen.getByRole("tabpanel", { name: "Grafik" });

    const rings = within(panel).getByRole("group", { name: "Porsi aset, pilih kegunaan" });
    expect(within(rings).getAllByRole("button")).toHaveLength(4);
    expect(within(rings).getByRole("button", { name: /^Investasi/ })).toHaveAttribute("aria-pressed", "true");
    const focus = within(panel).getByRole("region", { name: /^Investasi/ });
    expect(within(focus).getAllByRole("heading", { level: 3 }).map((heading) => heading.textContent)).toEqual([
      "Emas▲Rp 51.200 (4,1%) untungRp 1.300.000",
      "Reksa dana▲Rp 65.800 (1,0%) untungRp 6.500.000",
      "Saham▲Rp 136.000 (2,7%) untungRp 5.100.000",
    ]);
    expect(within(focus).getByRole("button", { name: /^BBRI · Stockbit Rp 3\.300\.000/ })).toBeInTheDocument();
    expect((await axe(panel)).violations).toHaveLength(0);

    await user.click(within(within(panel).getByRole("group", { name: "Pilih kegunaan" })).getByRole("button", { name: /^Tabungan/ }));
    const savings = within(panel).getByRole("region", { name: /^Tabungan/ });
    expect(within(savings).getByRole("button", { name: /^Dana Darurat · Bank Jago/ })).toBeInTheDocument();

    await perRekening(user);
    await user.click(within(rings).getByRole("button", { name: /^Investasi/ }));
    const byRekening = within(panel).getByRole("region", { name: /^Investasi/ });
    expect(byRekening).toHaveTextContent("per rekening");
    expect(within(byRekening).getAllByRole("heading", { level: 3 }).map((heading) => heading.textContent)).toEqual([
      "BCBCA▲Rp 117.000 (1,5%) untungRp 7.800.000",
      "STStockbit▲Rp 136.000 (2,7%) untungRp 5.100.000",
    ]);
    // Opening an item opens the same detail as a card.
    await user.click(within(byRekening).getByRole("button", { name: /^BBCA · Stockbit/ }));
    expect(await screen.findByRole("dialog", { name: /^BBCA/ })).toBeInTheDocument();
  });

  it("reorders cards and rows with Alt and the arrows, saves each grouping's layout and keeps it after a reload", async () => {
    const saved: Record<string, unknown> = {};
    vi.mocked(api.put).mockImplementation(async (path, body) => {
      saved[(body as { grouping: string }).grouping] = body;
      return body;
    });
    const user = userEvent.setup();
    const first = renderAccounts();

    const main = await screen.findByRole("button", { name: "Main Pocket, kantong di Bank Jago" });
    expect(main).toHaveAccessibleDescription(/Alt dan panah kiri atau kanan untuk memindah/);
    main.focus();
    await user.keyboard("{Alt>}{ArrowLeft}{/Alt}");
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/accounts/layout?grouping=use", expect.objectContaining({
      grouping: "use",
      sections: USE_ROWS,
      cards: expect.objectContaining({ harian: ["jago-main", "bca-atm"], investasi: ["bca-emas", "bca-rdpu", "bca-goals", "sb-bbri", "sb-bbca"] }),
    })));
    expect(cardsOf("Uang harian")).toEqual(["Main Pocket, kantong di Bank Jago", "Tabungan BCA (ATM), kantong di BCA"]);
    expect(screen.getByRole("button", { name: "Main Pocket, kantong di Bank Jago" })).toHaveFocus();
    expect(screen.getByRole("status")).toHaveTextContent("Main Pocket dipindah ke posisi 1 dari 2.");

    const handle = screen.getByRole("button", { name: "Pindah baris Investasi. Alt dan panah atas atau bawah." });
    handle.focus();
    await user.keyboard("{Alt>}{ArrowUp}{/Alt}");
    await user.keyboard("{Alt>}{ArrowUp}{/Alt}");
    await user.keyboard("{Alt>}{ArrowUp}{/Alt}");
    await waitFor(() => expect(railOrder()).toEqual(["investasi", "harian", "tabungan", "dana_investasi", "utang"]));
    expect(screen.getByRole("button", { name: /^Pindah baris Investasi/ })).toHaveFocus();
    expect(screen.getByRole("status")).toHaveTextContent("Baris Investasi dipindah ke posisi 1 dari 5.");
    expect(saved.use).toMatchObject({ sections: ["investasi", "harian", "tabungan", "dana_investasi", "utang"] });
    first.unmount();

    // Another device reads the same order from the server; Per rekening keeps its own.
    serve(owner, ownerNetWorth, { use: saved.use, rekening: { grouping: "rekening", sections: ["jago", "bca", "stockbit", "spaylater"], cards: {} } });
    renderAccounts();
    await waitFor(() => expect(railOrder()).toEqual(["investasi", "harian", "tabungan", "dana_investasi", "utang"]));
    expect(cardsOf("Uang harian")).toEqual(["Main Pocket, kantong di Bank Jago", "Tabungan BCA (ATM), kantong di BCA"]);
    await perRekening(user);
    await waitFor(() => expect(railOrder()).toEqual(["jago", "bca", "stockbit", "spaylater"]));
    expect(api.get).toHaveBeenCalledWith("/accounts/layout?grouping=rekening");
  });

  it("restores the previous order and says Urutan gagal disimpan when the save fails", async () => {
    vi.mocked(api.put).mockRejectedValue(new Error("Network unavailable"));
    const user = userEvent.setup();
    renderAccounts();
    const main = await screen.findByRole("button", { name: "Main Pocket, kantong di Bank Jago" });
    main.focus();
    await user.keyboard("{Alt>}{ArrowLeft}{/Alt}");
    expect(await screen.findByRole("alert")).toHaveTextContent("Urutan gagal disimpan");
    expect(cardsOf("Uang harian")).toEqual(["Tabungan BCA (ATM), kantong di BCA", "Main Pocket, kantong di Bank Jago"]);
    expect(railOrder()).toEqual(USE_ROWS);
  });

  it("drags cards within a row and rows by their handle on desktop", async () => {
    renderAccounts();
    const dragData = { setData: vi.fn(), effectAllowed: "move", dropEffect: "move" };
    const mainCard = await row("Main Pocket, kantong di Bank Jago");
    const atmCard = await row("Tabungan BCA (ATM), kantong di BCA");
    expect(mainCard).toHaveAttribute("draggable", "true");

    fireEvent.dragStart(mainCard, { dataTransfer: dragData });
    fireEvent.dragOver(atmCard, { dataTransfer: dragData });
    fireEvent.drop(atmCard, { dataTransfer: dragData });
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/accounts/layout?grouping=use", expect.objectContaining({
      cards: expect.objectContaining({ harian: ["jago-main", "bca-atm"] }),
    })));

    // A card does not move into another row.
    fireEvent.dragStart(await row("Dana Darurat, kantong di Bank Jago"), { dataTransfer: dragData });
    fireEvent.drop(await row("Main Pocket, kantong di Bank Jago"), { dataTransfer: dragData });
    fireEvent.dragEnd(await row("Dana Darurat, kantong di Bank Jago"));
    expect(api.put).toHaveBeenCalledTimes(1);

    fireEvent.dragStart(screen.getByRole("button", { name: /^Pindah baris Utang/ }), { dataTransfer: dragData });
    fireEvent.dragOver(rail("harian"), { dataTransfer: dragData });
    fireEvent.drop(rail("harian"), { dataTransfer: dragData });
    await waitFor(() => expect(railOrder()).toEqual(["utang", "harian", "tabungan", "dana_investasi", "investasi"]));
    expect(api.put).toHaveBeenLastCalledWith("/accounts/layout?grouping=use", expect.objectContaining({
      sections: ["utang", "harian", "tabungan", "dana_investasi", "investasi"],
    }));
  });

  it("switches phones to an Ubah urutan mode with move buttons for rows and cards, and returns focus when done", async () => {
    class PhoneWidth {
      constructor(private readonly callback: ResizeObserverCallback) {}
      observe() { this.callback([{ contentRect: { width: 390 } } as ResizeObserverEntry], this as unknown as ResizeObserver); }
      unobserve() {}
      disconnect() {}
    }
    vi.stubGlobal("ResizeObserver", PhoneWidth);
    try {
      const user = userEvent.setup();
      renderAccounts();
      await user.click(await screen.findByRole("button", { name: "Ubah urutan" }));
      expect(document.querySelector('[draggable="true"]')).toBeNull();
      const panel = screen.getByRole("region", { name: "Ubah urutan" });
      expect((await axe(panel)).violations).toHaveLength(0);
      await user.click(within(panel).getByRole("button", { name: "Turunkan baris Uang harian" }));
      await waitFor(() => expect(api.put).toHaveBeenCalledWith("/accounts/layout?grouping=use", expect.objectContaining({
        sections: ["tabungan", "harian", "dana_investasi", "investasi", "utang"],
      })));
      await user.click(within(panel).getByRole("button", { name: "Geser ke belakang Tabungan BCA (ATM)" }));
      await waitFor(() => expect(api.put).toHaveBeenLastCalledWith("/accounts/layout?grouping=use", expect.objectContaining({
        cards: expect.objectContaining({ harian: ["jago-main", "bca-atm"] }),
      })));
      await user.click(screen.getByRole("button", { name: "Selesai" }));
      await waitFor(() => expect(screen.getByRole("button", { name: "Ubah urutan" })).toHaveFocus());
      expect(railOrder()).toEqual(["tabungan", "harian", "dana_investasi", "investasi", "utang"]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("sets Kegunaan on a pocket, and explains a Kegunaan the server refuses for the type", async () => {
    vi.mocked(api.patch).mockResolvedValueOnce({ ok: true });
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "Main Pocket, kantong di Bank Jago", "Main Pocket");
    expect(detail).toHaveTextContent(/Kegunaan\s*Uang harian/);
    await user.click(within(detail).getByRole("button", { name: "Ubah kantong" }));
    const dialog = screen.getByRole("dialog", { name: "Ubah Kantong: Main Pocket" });
    const role = within(dialog).getByRole("combobox", { name: "Kegunaan" });
    expect(role).toHaveValue("harian");
    expect(within(role).getAllByRole("option").map((option) => option.textContent)).toEqual(["Uang harian", "Tabungan", "Dana investasi"]);
    await user.selectOptions(role, "tabungan");
    expect((await axe(dialog)).violations).toHaveLength(0);
    await user.click(within(dialog).getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/accounts/jago-main", expect.objectContaining({ type: "bank", role: "tabungan" })));

    const message = "Kegunaan tidak sesuai jenis rekening. Rekening kredit selalu Utang, produk investasi selalu Investasi.";
    vi.mocked(api.patch).mockRejectedValueOnce(new Error(message));
    await user.click(within(screen.getByRole("dialog", { name: /^Main Pocket/ })).getByRole("button", { name: "Ubah kantong" }));
    const again = screen.getByRole("dialog", { name: "Ubah Kantong: Main Pocket" });
    await user.click(within(again).getByRole("button", { name: "Simpan Perubahan" }));
    const refused = within(again).getByRole("combobox", { name: "Kegunaan" });
    await waitFor(() => expect(refused).toHaveAttribute("aria-invalid", "true"));
    expect(refused).toHaveFocus();
    expect(refused).toHaveAccessibleDescription(expect.stringContaining(message));
  });

  it("offers Kegunaan for cash accounts and fixes it for credit and investment", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const user = userEvent.setup();
    renderAccounts();
    await user.click(await screen.findByRole("button", { name: "Tambah rekening" }));
    const dialog = screen.getByRole("dialog", { name: "Tambah rekening" });
    const type = within(dialog).getByRole("combobox", { name: "Jenis rekening" });
    await user.selectOptions(type, "credit");
    expect(within(dialog).queryByRole("combobox", { name: "Kegunaan" })).not.toBeInTheDocument();
    expect(dialog).toHaveTextContent(/Kegunaan\s*Utang\s*Ditentukan oleh jenis rekening\./);
    await user.selectOptions(type, "investment");
    expect(dialog).toHaveTextContent(/Kegunaan\s*Investasi\s*Ditentukan oleh jenis rekening\./);
    await user.selectOptions(type, "bank");
    await user.type(within(dialog).getByRole("textbox", { name: "Nama Rekening" }), "Bank Contoh");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Kegunaan" }), "dana_investasi");
    await user.click(within(dialog).getByRole("button", { name: "Tambah Rekening" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({ name: "Bank Contoh", type: "bank", role: "dana_investasi" })));
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
    serve(accounts, netWorth);
  }

  afterEach(restorePage);

  beforeEach(() => {
    resetPage();
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
      name: "SPayLater", type: "credit", role: "utang", initial_balance: -350_000, credit_limit: 2_000_000, payment_day: 11, color: "#475569",
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
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/accounts", expect.objectContaining({ type: "bank", role: "harian" })));
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
    expect(within(dialog).queryByRole("combobox", { name: "Pindahkan ke rekening" })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("combobox", { name: "Kegunaan" })).not.toBeInTheDocument();
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
      name: "SPayLater", type: "credit", role: "utang", credit_limit: 3_000_000, payment_day: 25,
    })));
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("parent_id");
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("initial_balance");
  });

  it("does not offer a credit account as a rekening to move under", async () => {
    const gopay = { ...jago, id: "gopay", name: "GoPay", type: "wallet" };
    mockAccounts([jago, gopay, spaylater]);
    const user = userEvent.setup();
    renderAccounts();
    const detail = await openDrawer(user, "GoPay", "GoPay");
    await user.click(within(detail).getByRole("button", { name: "Ubah rekening" }));
    const parent = within(screen.getByRole("dialog", { name: "Ubah Rekening: GoPay" })).getByRole("combobox", { name: "Pindahkan ke rekening" });
    expect(within(parent).getByRole("option", { name: "Jago" })).toBeInTheDocument();
    expect(within(parent).queryByRole("option", { name: "SPayLater" })).not.toBeInTheDocument();
  });

  it("shows what is owed, the limit used and the payment day on the Utang card, with no share in the assets", async () => {
    const user = userEvent.setup();
    renderAccounts();
    const card = await row("SPayLater");
    expect(card).toHaveTextContent("−Rp 470.000");
    expect(card).toHaveTextContent("dibayar tiap tgl 11");
    expect(card).toHaveTextContent("24% dari limit Rp 2.000.000");
    expect(card).not.toHaveTextContent("-Rp 470.000");
    expect(card).not.toHaveTextContent("Melewati limit");
    const bar = within(card).getByRole("progressbar", { name: "Limit terpakai SPayLater" });
    expect(bar).toHaveAttribute("aria-valuenow", "24");
    expect(bar).toHaveAttribute("aria-valuetext", "Rp 470.000 dari Rp 2.000.000, 24%");
    expect(within(card).queryByRole("button", { name: /Tambah kantong/ })).not.toBeInTheDocument();
    expect(card.closest("section")).toHaveTextContent(/Utang\s*1 rekening\s*−Rp 470\.000/);
    expect((await axe(card)).violations).toHaveLength(0);
    // Credit is utang, so it has no share in the assets.
    expect(within(screen.getByRole("list", { name: "Porsi aset per kegunaan" })).queryByText(/Utang/)).not.toBeInTheDocument();
    await perRekening(user);
    expect(within(screen.getByRole("list", { name: "Isi tiap rekening" })).queryByText("SPayLater")).not.toBeInTheDocument();
  });

  it("shows active obligations as Tagihan debt cards, so the Utang rows add up to the summary's Utang", async () => {
    const cicilan = { id: "cicilan", name: "Cicilan Homegym", total_amount: 3_000_000, remaining_amount: 2_200_000, due_date: null, minimum_payment: 400_000, notes: null };
    serve([jago, spaylater], {
      ...netWorth, net_worth: -2_170_000, total_liabilities: 2_670_000, obligations: [cicilan],
      lines: [...netWorth.lines, { key: "debt:cicilan", label: "Cicilan Homegym", amount: -2_200_000 }],
    });
    const user = userEvent.setup();
    renderAccounts();
    const summary = await screen.findByRole("region", { name: "Ringkasan" });
    expect(await within(summary).findByText("−Rp 2.670.000")).toBeInTheDocument();

    const debts = rail("utang");
    expect(debts).toHaveTextContent(/Utang\s*1 rekening · 1 tagihan\s*−Rp 2\.670\.000/);
    expect(cardsOf("Utang")).toEqual(["SPayLater"]);
    const bill = within(debts).getByRole("link", { name: "Cicilan Homegym, tagihan. Buka Target & Tagihan" });
    expect(bill).toHaveAttribute("href", "/goals");
    expect(bill).toHaveTextContent(/Tagihan\s*Cicilan Homegym.*−Rp 2\.200\.000/);
    const billCard = bill.closest("li")!;
    expect(billCard).not.toHaveAttribute("draggable");
    // The bill follows the credit card it cannot be reordered with.
    expect(within(debts).getAllByRole("listitem").at(-1)).toBe(billCard);
    expect((await axe(debts)).violations).toHaveLength(0);

    await perRekening(user);
    expect(railOrder()).toEqual(["jago", "spaylater", "tagihan"]);
    const bills = rail("tagihan");
    expect(bills).toHaveTextContent(/Tagihan\s*1 tagihan\s*−Rp 2\.200\.000/);
    expect(rail("spaylater")).toHaveTextContent("−Rp 470.000");
    expect(within(bills).queryByRole("button", { name: /^Pindah baris/ })).not.toBeInTheDocument();
    expect(within(bills).getByRole("link", { name: /^Cicilan Homegym, tagihan/ })).toHaveAttribute("href", "/goals");
    expect((await axe(bills)).violations).toHaveLength(0);
  });

  it("marks a credit account over its limit in words, and shows an overpayment as Lebih bayar", async () => {
    const over = { ...spaylater, balance: -2_100_000, credit_owed: 2_100_000, over_limit: true };
    const overpaid = { ...spaylater, id: "kredivo", name: "Kredivo", balance: 25_000, credit_owed: 0, credit_limit: null, payment_day: null };
    mockAccounts([jago, over, overpaid]);
    renderAccounts();
    const overCard = await row("SPayLater");
    expect(overCard).toHaveTextContent("−Rp 2.100.000");
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
    const summary = await screen.findByRole("region", { name: "Ringkasan" });
    expect(await within(summary).findByText(/−Rp 470\.000/)).toBeInTheDocument();
    await user.click(within(summary).getByRole("button", { name: "Kekayaan bersih Rp 30.000: lihat rincian" }));
    const explanation = await screen.findByRole("dialog", { name: "Kekayaan bersih" });
    expect(explanation).toHaveTextContent("Utang SPayLater");
  });
});
