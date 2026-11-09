import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { WorkspaceContext, type WorkspaceValue } from "@/components/workspace/context";
import { PHONE_QUERY } from "@/components/workspace/layers";
import { ToastProvider } from "@/components/workspace/overlays";
import { AccountDetailDialog } from "@/components/accounts/AccountDetailDialog";
import type { AccountItem } from "@/components/accounts/types";
import AccountsPage from "./page";

// Next.js keeps `useSearchParams` in sync with the history API; the mock does the same.
vi.mock("next/navigation", async () => {
  const react = await import("react");
  return {
    usePathname: () => window.location.pathname,
    useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
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

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), put: vi.fn(), del: vi.fn() },
}));
const hidden = vi.hoisted(() => ({ on: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: hidden.on, bal: (amount: number) => (hidden.on ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`), openMovement: () => {} }),
}));

const base = { initial_balance: 0, is_archived: false, children: [], created_at: "2026-09-01T00:00:00.000Z" };
const pocket = (fields: Partial<AccountItem> & { id: string; name: string; balance: number; parent_id: string }): AccountItem =>
  ({ ...base, type: "bank", role: "harian", ...fields }) as AccountItem;

const jago: AccountItem = {
  ...base, id: "jago", name: "Jago", type: "bank", balance: 6_589_000, own_balance: 100_000, role: "harian", default_pocket_id: "makan",
  children: [
    pocket({ id: "makan", name: "Makan & Jajan", balance: 1_489_000, parent_id: "jago" }),
    pocket({ id: "darurat", name: "Darurat", balance: 5_000_000, parent_id: "jago", role: "tabungan", reconciliation_required: true }),
  ],
};
const gopay: AccountItem = { ...base, id: "gopay", name: "GoPay", type: "wallet", balance: 300_000, role: "harian" };
const bca: AccountItem = { ...base, id: "bca", name: "BCA", type: "bank", balance: 2_000_000, role: "harian" };
const rdn: AccountItem = { ...base, id: "rdn", name: "RDN", type: "bank", balance: 771_060, role: "dana_investasi" };
const stockbit: AccountItem = {
  ...base, id: "stockbit", name: "Stockbit", type: "investment", balance: 1_800_000, role: "investasi", group_capital_gain: -100_000, group_cost_basis: 1_900_000,
  children: [
    { ...base, id: "bbca", parent_id: "stockbit", name: "BBCA", type: "investment", instrument_type: "stock", units: 1_200, last_price: 833, balance: 1_000_000, cost_basis: 900_000, capital_gain: 100_000, capital_gain_pct: 11.11, role: "investasi", kind: "saham" },
    { ...base, id: "bbri", parent_id: "stockbit", name: "BBRI", type: "investment", instrument_type: "stock", units: 500, balance: 800_000, cost_basis: 1_000_000, capital_gain: -200_000, capital_gain_pct: -20, role: "investasi", kind: "saham" },
  ],
};
const bibit: AccountItem = {
  ...base, id: "bibit", name: "Bibit", type: "investment", balance: 500_000, role: "investasi", group_capital_gain: null,
  children: [{ ...base, id: "rdpu", parent_id: "bibit", name: "RDPU Contoh", type: "investment", instrument_type: "mutual_fund", balance: 500_000, cost_basis: null, capital_gain: null, role: "investasi", kind: "reksa_dana" }],
};
const spaylater: AccountItem = {
  ...base, id: "spaylater", name: "SPayLater", type: "credit", balance: -893_000, role: "utang", credit_limit: 3_000_000, credit_owed: 893_000,
  limit_left: 2_107_000, over_limit: false, payment_day: 8, statement_day: 25,
  next_due: { due_date: "2026-11-08", amount: 434_000, left_to_pay: 434_000, status: "due_today" },
};
const ACCOUNTS = [jago, gopay, bca, rdn, stockbit, bibit, spaylater];

const NET_WORTH = {
  ok: true, net_worth: 25_000_000, total_assets: 11_960_060, total_liabilities: 13_143_000, credit_owed: 893_000, accounts: [], runway: null,
  liquid: 8_889_000, dana_investasi: 771_060, investment: 2_300_000, reconciliation_pending: 0,
  obligations: [
    { id: "kos", name: "Kos", total_amount: 10_000_000, remaining_amount: 10_000_000, due_date: null, minimum_payment: null, notes: null },
    { id: "laptop", name: "Cicilan laptop", total_amount: 3_000_000, remaining_amount: 2_250_000, due_date: null, minimum_payment: null, notes: null },
  ],
  lines: [
    { key: "liquid", label: "Dana likuid", amount: 8_889_000 },
    { key: "dana_investasi", label: "Dana investasi", amount: 771_060 },
    { key: "investment", label: "Investasi", amount: 2_300_000 },
    { key: "debt:kos", label: "Kos", amount: -10_000_000 },
    { key: "debt:laptop", label: "Cicilan laptop", amount: -2_250_000 },
    { key: "credit:spaylater", label: "SPayLater", amount: -893_000 },
  ],
};

const CREDIT_OVERVIEW = {
  ok: true, due_dates_unavailable: false, next_due: spaylater.next_due,
  bills: [
    {
      due_date: "2026-11-08", statement_date: "2026-10-25", amount: 434_000, paid: 0, left_to_pay: 434_000, status: "due_today", settled_on: null,
      items: [
        { kind: "purchase", transaction_id: "t1", date: "2026-10-03", due_date: "2026-11-08", installment: { number: 2, count: 3 }, amount: 334_000, left_to_pay: 334_000, settled_on: null, days_to_settle: null, settled_on_time: null },
        { kind: "fee", transaction_id: "t2", date: "2026-10-03", due_date: "2026-11-08", installment: null, amount: 100_000, left_to_pay: 100_000, settled_on: null, days_to_settle: null, settled_on_time: null },
      ],
    },
    {
      due_date: "2026-10-08", statement_date: "2026-09-25", amount: 334_000, paid: 334_000, left_to_pay: 0, status: "paid_late", settled_on: "2026-11-07",
      items: [
        { kind: "purchase", transaction_id: "t1", date: "2026-10-03", due_date: "2026-10-08", installment: { number: 1, count: 3 }, amount: 334_000, left_to_pay: 0, settled_on: "2026-11-07", days_to_settle: 35, settled_on_time: false },
      ],
    },
  ],
  totals: { spent_on_credit: 1_002_000, paid_to_credit: 334_000, items_settled: 1, items_settled_on_time: 0 },
};

const realReplace = window.history.replaceState.bind(window.history);
let storage: Map<string, string>;
let phone = false;
let layoutFails = false;

function serve({ accounts = ACCOUNTS, netWorth = NET_WORTH as unknown, accountsFail = false, netWorthFail = false } = {}) {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path === "/accounts") {
      if (accountsFail) throw new Error("down");
      return { ok: true, accounts, total_balance: 0 };
    }
    if (path === "/dashboard/net-worth") {
      if (netWorthFail) throw new Error("down");
      return netWorth;
    }
    if (path.startsWith("/credit/accounts/")) return CREDIT_OVERVIEW;
    const layout = /^\/accounts\/layout\?grouping=(\w+)$/.exec(path);
    if (layout) return { grouping: layout[1], sections: [], cards: {} };
    if (path.startsWith("/transactions?account_id=")) {
      return { transactions: [{ id: "tx1", date: "2026-11-07T05:30:00.000Z", amount: 30_000, type: "expense", description: "Makan siang", category_name: "Makan" }] };
    }
    return { ok: true, links: [], awaiting: [{ account_id: "gopay" }], rules: [], results: [], transactions: [] };
  });
  vi.mocked(api.put).mockImplementation(async (_path: string, body: unknown) => {
    if (layoutFails) throw new Error("down");
    return body;
  });
}

const openMovementCapture = vi.fn();

function Shell({ children }: { children: ReactNode }) {
  const value = { openMovementCapture, actionsSlot: document.getElementById("slot"), setActionsSlot: () => {} } as unknown as WorkspaceValue;
  return <WorkspaceContext.Provider value={value}>{children}</WorkspaceContext.Provider>;
}

function renderPage(language: "id" | "en" = "id", ui: ReactNode = <AccountsPage />) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const user = userEvent.setup();
  const view = renderWithIntl(
    <QueryClientProvider client={client}>
      <ToastProvider>
        <Shell>{ui}</Shell>
      </ToastProvider>
    </QueryClientProvider>,
    { language },
  );
  return { user, ...view };
}

const group = (key: string) => document.querySelector(`section[data-group="${key}"]`) as HTMLElement;
const groupOrder = () => [...document.querySelectorAll("section[data-group]")].map((section) => section.getAttribute("data-group"));
const tileNames = (key: string) => [...group(key).querySelectorAll("[data-account-row] .tl-name")].map((name) => name.textContent);

beforeEach(() => {
  vi.clearAllMocks();
  hidden.on = false;
  phone = false;
  layoutFails = false;
  storage = new Map();
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: { getItem: (key: string) => storage.get(key) ?? null, setItem: (key: string, value: string) => void storage.set(key, value), removeItem: (key: string) => void storage.delete(key) },
  });
  vi.stubGlobal("matchMedia", vi.fn((query: string) => ({ matches: phone && query === PHONE_QUERY, media: query, addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  const slot = document.createElement("div");
  slot.id = "slot";
  document.body.appendChild(slot);
  realReplace(null, "", "/accounts");
  window.history.replaceState = (state, title, url) => {
    realReplace(state, title, url);
    window.dispatchEvent(new Event("test-history"));
  };
  serve();
});

afterEach(() => {
  window.history.replaceState = realReplace;
  realReplace(null, "", "/accounts");
  document.getElementById("slot")?.remove();
  vi.unstubAllGlobals();
});

describe("Rekening hero and Rincian", () => {
  it("reads the server's net worth with its assets, the split by use and exactly one Rincian", async () => {
    const { user } = renderPage();
    await screen.findByText("Rp 25.000.000");
    const hero = screen.getByRole("region", { name: "Kekayaan bersih" });
    expect(hero).toHaveTextContent("dari aset Rp 11.960.060");
    expect(within(hero).getByRole("img", { name: /^Pembagian aset: Uang harian \d+%/ })).toBeInTheDocument();
    expect(within(hero).getByRole("list", { name: "Porsi aset per kegunaan" })).toHaveTextContent("Tabungan");
    expect(screen.getAllByRole("button", { name: "Rincian" })).toHaveLength(1);

    await user.click(screen.getByRole("button", { name: "Rincian" }));
    const dialog = await screen.findByRole("dialog", { name: "Kekayaan bersih" });
    for (const label of ["Saldo tersedia", "Dana investasi", "Investasi", "Tagihan · Kos", "Tagihan · Cicilan laptop", "Utang · SPayLater"]) {
      expect(within(dialog).getByText(label)).toBeInTheDocument();
    }
    expect(within(dialog).getByText("−Rp 893.000")).toBeInTheDocument();
    expect(within(dialog).getByRole("link", { name: "Buka Target & Tagihan" })).toHaveAttribute("href", "/goals");
    await user.click(within(dialog).getByRole("button", { name: "Tutup rincian" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Rincian" })).toHaveFocus());
  });

  it("shows an error box instead of Rp 0 when net worth fails, and the groups still show", async () => {
    serve({ netWorthFail: true });
    renderPage();
    expect(await screen.findByText("Kekayaan bersih belum dapat dihitung.")).toBeInTheDocument();
    const hero = screen.getByRole("region", { name: "Kekayaan bersih" });
    expect(hero).not.toHaveTextContent("Rp 0");
    expect(group("harian")).toBeInTheDocument();
  });

  it("shows one error box when the accounts fail, and the empty state without accounts", async () => {
    serve({ accountsFail: true });
    const first = renderPage();
    expect(await screen.findByText("Daftar rekening gagal dimuat.")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Kekayaan bersih" })).toBeNull();
    first.unmount();
    serve({ accounts: [] });
    renderPage();
    expect(await screen.findByRole("heading", { name: "Belum ada rekening" })).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Tambah rekening" }).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "Kartu" })).toBeNull();
  });
});

describe("Kartu groups and tiles", () => {
  it("groups by use with rails, counts, gains and totals, and Utang without obligation tiles", async () => {
    const { container } = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    expect(groupOrder()).toEqual(["harian", "tabungan", "dana_investasi", "investasi", "utang"]);
    expect(tileNames("harian")).toEqual(["Jago", "Makan & Jajan", "GoPay", "BCA"]);
    expect(group("harian")).toHaveTextContent("saldo di luar kantong");
    expect(within(group("harian")).getByText("Kantong di Jago · bawaan")).toBeInTheDocument();
    // A parent's own balance and its pockets are counted once.
    expect(within(group("harian")).getByText("Rp 3.889.000")).toBeInTheDocument();
    expect(group("dana_investasi")).toHaveTextContent("1 rekening · belum dibelikan");

    const investasi = group("investasi");
    expect(within(investasi).getByRole("list", { name: "Investasi per jenis" })).toHaveTextContent("Reksa dana");
    expect(within(investasi).getAllByText("Modal belum diisi").length).toBeGreaterThan(0);
    expect(investasi).toHaveTextContent("▲ +Rp 100.000 · +11,1% untung");
    expect(investasi).toHaveTextContent("▼ −Rp 200.000 · −20,0% rugi");

    const utang = group("utang");
    expect(tileNames("utang")).toEqual(["SPayLater"]);
    expect(within(utang).getByText("−Rp 893.000", { selector: ".total" })).toBeInTheDocument();
    expect(within(utang).getByRole("link", { name: "Tagihan −Rp 12.250.000 di Target & Tagihan" })).toHaveAttribute("href", "/goals");
    expect(utang).toHaveTextContent("2 tagihan");
    expect(screen.queryByText("Kos")).toBeNull();
    expect(container.querySelector("[style*='card-accent']")).toBeNull();
    expect((await axe(container)).violations).toEqual([]);
  });

  it("shows the credit tile's debt, limit left and next bill, over limit and a missing due day", async () => {
    const over: AccountItem = { ...spaylater, id: "akulaku", name: "Akulaku", over_limit: true, limit_left: -150_000, credit_owed: 1_150_000, balance: -1_150_000, credit_limit: 1_000_000, payment_day: null, next_due: null };
    serve({ accounts: [spaylater, over] });
    renderPage();
    await screen.findByRole("region", { name: "Utang" });
    const tile = within(group("utang")).getByText("SPayLater").closest("[data-account-row]") as HTMLElement;
    expect(tile).toHaveTextContent("Sisa limit Rp 2.107.000");
    expect(tile).toHaveTextContent("Utang −Rp 893.000");
    expect(tile).toHaveTextContent("jatuh tempo hari ini · Rp 434.000");
    const overTile = within(group("utang")).getByText("Akulaku").closest("[data-account-row]") as HTMLElement;
    expect(overTile).toHaveTextContent("Melewati limit");
    expect(overTile).toHaveTextContent("Tanggal jatuh tempo belum diisi");
    expect(overTile).not.toHaveTextContent("Sisa limit");
  });

  it("shows one note per tile and opens Sesuaikan saldo from Perlu dicocokkan", async () => {
    const { user } = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    expect(within(group("harian")).getByText("1 pindah saldo belum lengkap")).toBeInTheDocument();
    await user.click(within(group("tabungan")).getByRole("button", { name: "Perlu dicocokkan: sesuaikan saldo Darurat" }));
    expect(await screen.findByRole("dialog", { name: /Sesuaikan saldo/ })).toBeInTheDocument();
  });

  it("groups by rekening with per-use subtotals in words, the platform gain and the Tagihan row after the last group", async () => {
    const { user } = renderPage();
    await user.click(await screen.findByRole("button", { name: "Per rekening" }));
    expect(storage.get("rekening-grouping")).toBe("rekening");
    const jagoGroup = group("jago");
    expect(jagoGroup).toHaveTextContent("3 kantong · Uang harian Rp 1.589.000 · Tabungan Rp 5.000.000");
    expect(jagoGroup).toHaveTextContent("1 kantong perlu dicocokkan");
    expect(group("bibit")).toHaveTextContent("Modal belum lengkap");
    expect(within(group("jago")).getByText("Uang harian · Kantong di Jago · bawaan")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Utang lain" })).toBeNull();
    const rows = document.querySelector(".rk-bills.is-last") as HTMLElement;
    expect(within(rows).getByRole("link", { name: "Tagihan −Rp 12.250.000 di Target & Tagihan" })).toBeInTheDocument();
  });

  it("opens on Per kegunaan when browser storage is blocked", async () => {
    Object.defineProperty(window, "localStorage", { configurable: true, value: { getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } } });
    renderPage();
    expect(await screen.findByRole("button", { name: "Per kegunaan" })).toHaveAttribute("aria-pressed", "true");
  });

  it("masks every amount with hidden balances and keeps the percentages", async () => {
    hidden.on = true;
    renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    expect(document.body).not.toHaveTextContent("Rp 893.000");
    expect(document.body).not.toHaveTextContent("Rp 25.000.000");
    expect(screen.getAllByRole("img", { name: "Disembunyikan" }).length).toBeGreaterThan(10);
    expect(group("investasi")).toHaveTextContent("+11,1%");
  });
});

describe("Grafik", () => {
  it("keeps Grafik in the address, hides the grouping and lets the legend choose a use", async () => {
    const { user } = renderPage();
    await user.click(await screen.findByRole("button", { name: "Grafik" }));
    expect(window.location.search).toBe("?tampilan=grafik");
    expect(screen.queryByRole("button", { name: "Per rekening" })).toBeNull();
    const legend = screen.getByRole("group", { name: "Pilih kegunaan" });
    expect(within(legend).getByRole("button", { name: /^Investasi/ })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("region", { name: "Investasi per jenis" })).toHaveTextContent("Saham");
    await user.click(within(legend).getByRole("button", { name: /^Tabungan/ }));
    expect(within(legend).getByRole("button", { name: /^Tabungan/ })).toHaveAttribute("aria-pressed", "true");
    expect(within(legend).getByRole("button", { name: /^Tabungan/ })).toHaveClass("ws-select-row");
    const contents = screen.getByRole("region", { name: "Isi Tabungan" });
    expect(contents).toHaveTextContent("Darurat");
    expect(contents).toHaveTextContent("aset");
  });

  it("opens a product's detail from its bar and reopens Grafik from the address", async () => {
    realReplace(null, "", "/accounts?tampilan=grafik");
    const { user } = renderPage();
    const products = await screen.findByRole("region", { name: "Per produk" });
    await user.click(within(products).getByRole("button", { name: /BBCA/ }));
    expect(await screen.findByRole("dialog", { name: "BBCA" })).toBeInTheDocument();
  });
});

describe("Atur urutan", () => {
  it("moves nothing outside the mode, and moves tiles and rows by keyboard inside it", async () => {
    const { user } = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    expect(screen.queryByText(/Seret kartu dalam baris/)).toBeNull();
    const gopayTile = within(group("harian")).getByText("GoPay").closest("button") as HTMLElement;
    gopayTile.focus();
    await user.keyboard("{Alt>}{ArrowRight}{/Alt}");
    expect(api.put).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Tindakan lain untuk Rekening & Kantong" }));
    await user.click(screen.getByRole("menuitem", { name: "Atur urutan" }));
    expect(screen.getByText(/Seret kartu dalam baris/)).toBeInTheDocument();
    const grip = document.getElementById("rk-tile-gopay") as HTMLElement;
    grip.focus();
    await user.keyboard("{Alt>}{ArrowRight}{/Alt}");
    await waitFor(() => expect(api.put).toHaveBeenCalledWith("/accounts/layout?grouping=use", expect.objectContaining({ grouping: "use" })));
    expect(tileNames("harian")).toEqual(["Jago", "Makan & Jajan", "BCA", "GoPay"]);
    expect(screen.getByText("GoPay dipindah ke posisi 4 dari 4.")).toHaveAttribute("aria-live", "polite");
    await waitFor(() => expect(document.getElementById("rk-tile-gopay")).toHaveFocus());

    const handle = screen.getByRole("button", { name: "Pindah baris Investasi. Alt dan panah atas atau bawah." });
    handle.focus();
    await user.keyboard("{Alt>}{ArrowUp}{/Alt}");
    await waitFor(() => expect(groupOrder()).toEqual(["harian", "tabungan", "investasi", "dana_investasi", "utang"]));
    await user.click(screen.getByRole("button", { name: "Selesai" }));
    expect(screen.queryByText(/Seret kartu dalam baris/)).toBeNull();
  });

  it("restores the order and says so when the save fails", async () => {
    layoutFails = true;
    const { user } = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    await user.click(screen.getByRole("button", { name: "Tindakan lain untuk Rekening & Kantong" }));
    await user.click(screen.getByRole("menuitem", { name: "Atur urutan" }));
    (document.getElementById("rk-tile-gopay") as HTMLElement).focus();
    await user.keyboard("{Alt>}{ArrowRight}{/Alt}");
    expect(await screen.findByText("Urutan gagal disimpan.")).toBeInTheDocument();
    await waitFor(() => expect(tileNames("harian")).toEqual(["Jago", "Makan & Jajan", "GoPay", "BCA"]));
  });

  it("uses move buttons on a phone, with the first move disabled, and shows Uang harian as a wallet", async () => {
    phone = true;
    const { user } = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    expect(screen.getByRole("list", { name: "Kartu Uang harian, geser ke samping" })).toHaveClass("wallet");
    await user.click(screen.getByRole("button", { name: "Tindakan lain untuk Rekening & Kantong" }));
    expect(screen.getByRole("menuitem", { name: "Perbarui harga" })).toBeInTheDocument();
    await user.click(screen.getByRole("menuitem", { name: "Atur urutan" }));
    expect(screen.getByText("Pakai tombol naik dan turun. Urutan langsung tersimpan.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Geser ke depan Jago" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Naikkan baris Uang harian" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Geser ke belakang GoPay" }));
    await waitFor(() => expect(api.put).toHaveBeenCalled());
  });
});

describe("Account detail dialog", () => {
  it("opens a liquid account with Cocokkan saldo first, the rest under Lainnya, and returns focus", async () => {
    const { user } = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    const tile = within(group("harian")).getByText("BCA").closest("button") as HTMLElement;
    await user.click(tile);
    expect(window.location.search).toBe("?akun=bca");
    const dialog = await screen.findByRole("dialog", { name: "BCA" });
    expect(within(dialog).getByText("Rp 2.000.000")).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Cocokkan saldo" })).toHaveClass("ws-btn-accent");
    expect(within(dialog).getByRole("button", { name: "Ubah rekening" })).toBeInTheDocument();
    const recent = await within(dialog).findByText("−Rp 30.000");
    expect(recent).toHaveClass("c-expense");
    await user.click(within(dialog).getByRole("button", { name: "Tindakan lain untuk BCA" }));
    const menu = screen.getByRole("menu", { name: "Lainnya" });
    expect(within(menu).getAllByRole("menuitem").map((item) => item.textContent)).toEqual(["Sesuaikan saldo", "Tambah kantong", "Arsipkan"]);
    await user.click(within(dialog).getByRole("button", { name: "Tindakan lain untuk BCA" }));
    await user.click(within(dialog).getByRole("button", { name: "Tutup" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "BCA" })).toBeNull());
    expect(window.location.search).toBe("");
    await waitFor(() => expect(tile).toHaveFocus());
  });

  it("offers Beli and Jual for a stock and keeps the rest under Lainnya", async () => {
    realReplace(null, "", "/accounts?akun=bbca");
    const { user } = renderPage();
    const dialog = await screen.findByRole("dialog", { name: "BBCA" });
    expect(within(dialog).getByRole("button", { name: "Beli" })).toHaveClass("ws-btn-accent");
    expect(within(dialog).getByRole("button", { name: "Jual" })).toBeInTheDocument();
    expect(within(dialog).getByText("Produk di Stockbit")).toBeInTheDocument();
    expect(dialog).toHaveTextContent("▲ +Rp 100.000 · +11,1%");
    await user.click(within(dialog).getByRole("button", { name: "Tindakan lain untuk BBCA" }));
    expect(within(screen.getByRole("menu")).getAllByRole("menuitem").map((item) => item.textContent)).toEqual(["Perbarui nilai", "Sesuaikan saldo", "Ubah kantong", "Buka Stockbit", "Arsipkan"]);
  });

  it("lists a credit account's bills with their items and pays the next bill through Perpindahan", async () => {
    realReplace(null, "", "/accounts?akun=spaylater");
    const { user } = renderPage();
    const dialog = await screen.findByRole("dialog", { name: "SPayLater" });
    expect(within(dialog).queryByRole("button", { name: "Cocokkan saldo" })).toBeNull();
    expect(dialog).toHaveTextContent("Tagihan terbit tiap tgl");
    expect(dialog).toHaveTextContent("tiap tanggal 25");
    expect(dialog).toHaveTextContent("Pengeluaran pakai kredit");
    const bills = await within(dialog).findByRole("region", { name: "Tagihan" });
    expect(bills).toHaveTextContent("Jatuh tempo hari ini");
    expect(bills).toHaveTextContent("Lunas terlambat");
    await user.click(within(bills).getByRole("button", { name: /Jatuh tempo 8 Okt/ }));
    expect(bills).toHaveTextContent("cicilan 1 dari 3");
    expect(bills).toHaveTextContent("lunas 7 Nov · 35 hari · terlambat");
    expect(within(bills).getByRole("link", { name: "Lihat di Transaksi" })).toHaveAttribute("href", "/ledger?account=spaylater");
    await user.click(within(dialog).getByRole("button", { name: "Bayar tagihan" }));
    expect(openMovementCapture).toHaveBeenCalledWith({ targetAccountId: "spaylater", amount: 434_000 });
  });

  it("clears an unknown ?akun= and opens outside the Rekening page", async () => {
    realReplace(null, "", "/accounts?akun=missing");
    const first = renderPage();
    await screen.findByRole("region", { name: "Uang harian" });
    await waitFor(() => expect(window.location.search).toBe(""));
    expect(screen.queryByRole("dialog")).toBeNull();
    first.unmount();
    const onClose = vi.fn();
    renderPage("id", <AccountDetailDialog accountId="gopay" onClose={onClose} />);
    expect(await screen.findByRole("dialog", { name: "GoPay" })).toBeInTheDocument();
  });
});

describe("Rekening in English", () => {
  it("reads in plain English with the same figure strings", async () => {
    const { container } = renderPage("en");
    await screen.findByRole("region", { name: "Everyday money" });
    for (const name of ["Savings", "Investment cash", "Investments", "Debt"]) expect(screen.getByRole("region", { name })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Breakdown" })).toBeInTheDocument();
    expect(container).toHaveTextContent("of Rp 11.960.060 in assets");
    expect(within(group("utang")).getByRole("link", { name: "Bills −Rp 12.250.000 in Goals & Bills" })).toBeInTheDocument();
    expect(group("investasi")).toHaveTextContent("▲ +Rp 100.000 · +11,1% gain");
    expect(container).toHaveTextContent("Limit left Rp 2.107.000");
    expect(container).not.toHaveTextContent("This page is in Indonesian for now.");
    await act(async () => undefined);
  });
});
