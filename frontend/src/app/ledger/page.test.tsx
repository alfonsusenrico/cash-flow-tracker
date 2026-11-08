import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, api } from "@/lib/api";
import { RegionProvider } from "@/lib/format";
import { renderWithIntl } from "@/lib/i18n/testing";
import type { Language } from "@/i18n/language";
import { ToastProvider } from "@/components/workspace/overlays";
import { ShortcutProvider } from "@/components/workspace/shortcuts";
import { LEDGER_TYPES } from "@/lib/ledgerFilters";
import { LedgerTypeControl } from "./LedgerRail";
import LedgerPage from "./page";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  uploadTransactionReceipt: vi.fn(),
  updateMovement: vi.fn(),
  splitMovement: vi.fn(),
  ApiError: class ApiError extends Error {
    constructor(readonly status: number, readonly detail: unknown) {
      super(typeof detail === "string" ? detail : "Request failed");
    }
  },
}));

// Next.js keeps `useSearchParams` in sync with the history API; the mock does the same.
const navigation = vi.hoisted(() => ({ push: vi.fn(), replace: vi.fn() }));
vi.mock("next/navigation", async () => {
  const react = await import("react");
  return {
    useRouter: () => ({ push: navigation.push, replace: navigation.replace }),
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

const app = vi.hoisted(() => ({ setTimeframe: vi.fn(), setCycleOffset: vi.fn(), hidden: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    hideBalances: app.hidden,
    bal: (amount: number) => (app.hidden ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`),
    setTimeframe: app.setTimeframe,
    setCycleOffset: app.setCycleOffset,
  }),
}));
vi.mock("@/components/ui/InvestmentTopupModal", () => ({ InvestmentTopupModal: () => null }));

type Row = Record<string, unknown> & { id: string; date: string; local_date: string; review_status: string };

function row(id: string, local: string, time: string, extra: Record<string, unknown> = {}): Row {
  return {
    id, account_id: "acc-dd", account_name: "Dompet Digital", category_id: "cat-makan", category_name: "Makan di Luar",
    goal_id: null, goal_name: null, obligation_id: null, obligation_name: null, obligation_allocations: [], type: "expense",
    kakeibo_type: "want", amount: 30_000, notes: null, merchant: "Kedai Awan", date: `${local}T${time}:00+07:00`, local_date: local,
    receipt_path: null, created_at: `${local}T${time}:00+07:00`, source: "notification", review_status: "reviewed", classified_by: "ai",
    movement_id: null, investment_topup_id: null, saving_flow: null, credit: null, ...extra,
  };
}

const KEDAI = row("00000000-0000-4000-8000-00000000000a", "2026-11-02", "16:05", { review_status: "auto" });
const QRIS = row("00000000-0000-4000-8000-00000000000b", "2026-11-02", "12:40", { merchant: null, category_name: null, kakeibo_type: null, amount: 45_000, review_status: "needs_review" });
const BAKSO = row("00000000-0000-4000-8000-00000000000c", "2026-11-01", "18:40", { merchant: "Bakso Mas Joko", amount: 45_000 });
const TOPUP = row("00000000-0000-4000-8000-00000000000d", "2026-11-01", "10:31", {
  merchant: null, notes: "Top up Investasi", movement_id: "mv-1", movement_role: "outbound", investment_topup_id: "topup-1",
  movement_kind: "investment_topup", transfer_target_account_name: "Reksa Dana A", amount: 150_000, category_name: "Internal Movement",
  category_system_key: "internal_movement", saving_flow: "invested",
});
const ROWS = [KEDAI, QRIS, BAKSO, TOPUP];
const TOTALS: Record<string, Record<string, unknown>> = {
  "2026-11-02": { date: "2026-11-02", count: 2, inflow: 0, outflow: 75_000, spending: 75_000, movements: 0, waiting_count: 1, waiting_amount: 45_000 },
  "2026-11-01": { date: "2026-11-01", count: 2, inflow: 0, outflow: 45_000, spending: 45_000, movements: 150_000, waiting_count: 0, waiting_amount: 0 },
};
const PERIOD = {
  inflow: 9_800_000, outflow: 110_000, net: 9_690_000, spending: 90_000, saving: 20_000, spending_net: 9_710_000, gross_outflow: 120_000,
  reimbursement_offset: 10_000, saving_net: null, basis: "cycle", label: "Siklus sejak 25 Okt", start: "2026-10-25T01:02:00Z", end: "2026-11-24T16:59:59Z",
};
const CUMULATIVE = { inflow: 9_800_000, outflow: 120_000, net: 9_680_000, spending: 120_000, saving: 0, spending_net: 9_680_000, saving_net: null };
const COUNTS = { all: 4, expense: 3, income: 0, movement: 0, investment: 1, saving: 1, credit: 0 };

let pages: Row[][];
let withTotals = true;
let failMore = false;
let failFirst = false;
let holdFirst = false;
let summaryPatch: Record<string, unknown> = {};

function mockApi() {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path.startsWith("/transactions?")) {
      const offset = Number(new URLSearchParams(path.split("?")[1]).get("offset"));
      const index = offset === 0 ? 0 : 1;
      if (index === 0 && failFirst) throw new Error("network");
      if (index === 0 && holdFirst) return new Promise(() => undefined);
      if (index === 1 && failMore) throw new Error("network");
      const rows = pages[index] ?? [];
      const days = [...new Set(rows.map((item) => item.local_date))];
      return {
        ok: true, total: pages.flat().length, transactions: rows,
        summary: { cycle: CUMULATIVE, cumulative: { ...CUMULATIVE, ...summaryPatch }, period: { ...PERIOD, ...summaryPatch } },
        ...(withTotals ? { daily_totals: days.map((day) => TOTALS[day] ?? { date: day, count: rows.filter((item) => item.local_date === day).length, inflow: 0, outflow: 0, spending: 0, movements: 0, waiting_count: 0, waiting_amount: 0 }), type_counts: COUNTS } : {}),
      };
    }
    if (path.startsWith("/credit/accounts/")) {
      return {
        ok: true, account: { id: "acc-spl", name: "SPayLater", owed: 470_000 }, next_due: { due_date: "2026-12-11", amount: 470_000, left_to_pay: 470_000, status: "upcoming" },
        period: { from: "2026-10-25", to: "2026-11-24" }, totals: { spent_on_credit: 120_000, paid_to_credit: 350_000 },
      };
    }
    const detail = /^\/transactions\/([^/?]+)$/.exec(path);
    if (detail) {
      const found = [...ROWS, ...pages.flat()].find((item) => item.id === detail[1]);
      if (!found) throw new ApiError(404, "Transaction not found");
      return { transaction: { ...found, waiting: found.review_status === "needs_review", origin: null } };
    }
    if (path === "/accounts") {
      return { accounts: [
        { id: "00000000-0000-4000-8000-0000000000dd", name: "Dompet Digital", type: "ewallet", balance: 500_000 },
        { id: "00000000-0000-4000-8000-00000000c0de", name: "SPayLater", type: "credit", balance: -470_000, credit_owed: 470_000 },
      ] };
    }
    if (path === "/categories") return { categories: [{ id: "cat-makan", name: "Makan di Luar", kind: "expense", kakeibo_type: "want" }] };
    if (path === "/goals") return { goals: [] };
    if (path === "/obligations") return { obligations: [] };
    throw new Error(`Unexpected GET ${path}`);
  });
}

function renderPage(url = "/ledger?period=cycle", language: Language = "id") {
  window.history.replaceState(null, "", url);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = renderWithIntl(
    <QueryClientProvider client={queryClient}>
      <RegionProvider value={{ region: "ID", currency: "IDR", rate: 16_000 }}>
        <ShortcutProvider>
          <ToastProvider>
            <LedgerPage />
          </ToastProvider>
        </ShortcutProvider>
      </RegionProvider>
    </QueryClientProvider>,
    { language },
  );
  return { ...view, user: userEvent.setup() };
}

const realReplace = window.history.replaceState.bind(window.history);
const realPush = window.history.pushState.bind(window.history);

beforeEach(() => {
  pages = [[KEDAI, QRIS, BAKSO], [TOPUP]];
  withTotals = true;
  failMore = false;
  failFirst = false;
  holdFirst = false;
  summaryPatch = {};
  app.hidden = false;
  window.history.replaceState = (state, title, url) => {
    realReplace(state, title, url);
    window.dispatchEvent(new Event("test-history"));
  };
  window.history.pushState = (state, title, url) => {
    realPush(state, title, url);
    window.dispatchEvent(new Event("test-history"));
  };
  mockApi();
});

afterEach(() => {
  window.history.replaceState = realReplace;
  window.history.pushState = realPush;
  vi.clearAllMocks();
});

const here = () => `${window.location.pathname}${window.location.search}`;
const listCalls = () => vi.mocked(api.get).mock.calls.map(([path]) => path as string).filter((path) => path.startsWith("/transactions?"));
const rail = () => screen.getByRole("navigation", { name: "Jenis dan periode" });
const rowButton = (name: RegExp) => screen.getByRole("button", { name });
const findRow = (name: RegExp) => screen.findByRole("button", { name });

describe("Transaksi list", () => {
  it("groups rows by day under headings with the server's muted day totals and the waiting count in warning", async () => {
    const { container } = renderPage();
    await findRow(/Kedai Awan/);
    const headings = container.querySelectorAll(".day-h");
    expect(headings).toHaveLength(2);
    expect(headings[0]).toHaveTextContent(/^Senin, 2 November/);
    expect(headings[0]).toHaveTextContent("Pengeluaran Rp 75.000 · 1 menunggu jawaban");
    expect(within(headings[0] as HTMLElement).getByText("1 menunggu jawaban")).toHaveClass("c-warning");
    expect(headings[0].querySelector(".c-expense")).toBeNull();
    expect(headings[1]).toHaveTextContent("Pengeluaran Rp 45.000 · Pindah saldo Rp 150.000");
    expect(listCalls().at(-1)).toContain("include=daily_totals%2Ctype_counts");
    expect(listCalls().at(-1)).toContain("logical_movements=true");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows one-line rows: time, title and meta, category, pillar and the signed amount in its kind's colour", async () => {
    renderPage();
    const kedai = await findRow(/Kedai Awan/);
    expect(kedai).toHaveAttribute("data-row", KEDAI.id);
    expect(kedai.querySelector(".r-time")).toHaveTextContent("16.05");
    expect(kedai.querySelector(".r-meta")).toHaveTextContent("Dompet Digital");
    expect(kedai.querySelector(".r-cat")).toHaveTextContent("Makan di Luar");
    expect(kedai.querySelector(".r-pillar")).toHaveTextContent("Keinginan");
    expect(within(kedai).getByText("−Rp 30.000")).toHaveClass("c-expense");
    const qris = rowButton(/Tanpa keterangan/);
    expect(qris.querySelector(".r-cat")).toHaveTextContent("Belum ada kategori");
    expect(within(qris).getByText("Menunggu jawaban", { selector: ".r-pillar *" })).toHaveClass("c-warning");
    expect(document.querySelector(".ws-ava, .ws-pill")).toBeNull();
  });

  it("says how many rows of a split day are still to load, and keeps one heading after loading more", async () => {
    const { user, container } = renderPage();
    const more = await screen.findByRole("button", { name: "1 lagi di hari ini · Muat lagi" });
    expect(more).toHaveClass("ws-btn-quiet");
    await user.click(screen.getByRole("button", { name: "Muat 1 lagi" }));
    expect(await findRow(/Top up Reksa Dana A/)).toBeInTheDocument();
    expect(container.querySelectorAll(".day-h")).toHaveLength(2);
    expect(screen.queryByText(/lagi di hari ini/)).not.toBeInTheDocument();
    expect(screen.getByText("1 transaksi lagi dimuat.")).toBeInTheDocument();
    expect(screen.getByText("4 transaksi · akhir daftar")).toBeInTheDocument();
  });

  it("keeps the loaded rows and says so when loading more fails", async () => {
    failMore = true;
    const { user } = renderPage();
    await user.click(await screen.findByRole("button", { name: "Muat 1 lagi" }));
    expect(await screen.findByText("Gagal memuat transaksi berikutnya.")).toBeInTheDocument();
    expect(rowButton(/Kedai Awan/)).toBeInTheDocument();
  });

  it("shows only the date on a day heading when the server sends no day totals", async () => {
    withTotals = false;
    const { container } = renderPage();
    await findRow(/Kedai Awan/);
    for (const heading of container.querySelectorAll(".day-h")) expect(heading.textContent).not.toMatch(/Rp/);
  });

  it("keeps the desktop rows at a laptop width: the layout no longer follows the list's own width", async () => {
    renderPage();
    const kedai = await findRow(/Kedai Awan/);
    // The old page switched to the phone feed when the list region was under 767 px (456 px at 1024 with a rail).
    expect(kedai.querySelector(".r-cat")).not.toBeNull();
    expect(kedai.querySelector(".r-pillar")).not.toBeNull();
  });

  it("hides every amount with Sembunyikan saldo and names it as hidden", async () => {
    app.hidden = true;
    const { container } = renderPage();
    await findRow(/Kedai Awan/);
    expect(container.textContent).not.toMatch(/Rp \d/);
    expect(screen.getAllByRole("img", { name: "Disembunyikan" }).length).toBeGreaterThan(5);
  });
});

describe("Transaksi hero", () => {
  it("shows Pengeluaran with its reimbursement line, Masuk with a plus and Bersih with its sign", async () => {
    renderPage();
    const hero = await screen.findByRole("region", { name: "Pengeluaran" });
    expect(await within(hero).findByText("Rp 90.000")).toBeInTheDocument();
    expect(hero).toHaveTextContent("sudah dikurangi penggantian Rp 10.000");
    expect(within(hero).getByText("+Rp 9.800.000")).toBeInTheDocument();
    expect(within(hero).getByText("Bersih · masuk − pengeluaran")).toBeInTheDocument();
    expect(within(hero).getByText("+Rp 9.710.000")).toBeInTheDocument();
  });

  it("names the saved part, the all-time window and an older API without spending in words", async () => {
    summaryPatch = { reimbursement_offset: 0, saving: 400_000 };
    const first = renderPage();
    expect(await screen.findByText("tanpa Rp 400.000 yang ditabung")).toBeInTheDocument();
    first.unmount();
    summaryPatch = { spending: undefined, spending_net: undefined };
    renderPage("/ledger");
    const hero = await screen.findByRole("region", { name: "Pengeluaran · semua waktu" });
    await waitFor(() => expect(within(hero).getAllByText("Belum tersedia")).toHaveLength(2));
  });

  it("shows Ditabung & investasi from the server for the savings type, and no number with another filter", async () => {
    summaryPatch = { saving_net: 850_000 };
    const first = renderPage("/ledger?period=cycle&type=saving");
    const hero = await screen.findByRole("region", { name: "Ditabung & investasi" });
    expect(await within(hero).findByText("Rp 850.000")).toBeInTheDocument();
    expect(hero).toHaveTextContent("Tabungan bertambah Rp 850.000");
    expect(within(hero).queryByText("Bersih · masuk − pengeluaran")).not.toBeInTheDocument();
    expect(listCalls().at(-1)).toContain("type=saving");
    expect(listCalls().at(-1)).toContain("logical_movements=true");
    first.unmount();
    summaryPatch = { saving_net: null };
    renderPage("/ledger?period=cycle&type=saving&account=00000000-0000-4000-8000-0000000000a1");
    expect(await screen.findByText("Tidak tersedia dengan filter lain")).toBeInTheDocument();
  });

  it("shows skeleton figures and no counts while the first page loads", async () => {
    holdFirst = true;
    renderPage();
    const status = await screen.findByRole("status", { name: "Memuat transaksi" });
    expect(status).toHaveAttribute("aria-busy", "true");
    expect(screen.getByRole("region", { name: "Pengeluaran" })).toHaveAttribute("aria-busy", "true");
    expect(rail()).not.toHaveTextContent(/\d/);
  });
});

describe("Type and period rail", () => {
  it("lists seven types with the server's counts and two period groups", async () => {
    renderPage();
    await findRow(/Kedai Awan/);
    const items = within(rail()).getAllByRole("button");
    expect(items.map((item) => item.querySelector(".lbl")?.textContent)).toEqual([
      "Semua", "Pengeluaran", "Masuk", "Pindah saldo", "Investasi", "Tabungan & investasi", "Kredit", "Siklus gaji", "Bulan kalender", "Semua waktu",
    ]);
    expect(within(rail()).getByRole("button", { name: /^Investasi/ })).toHaveTextContent("1");
    expect(within(rail()).getByRole("button", { name: /^Siklus gaji/ })).toHaveAttribute("aria-pressed", "true");
    expect(within(screen.getByRole("group", { name: "Jenis" })).getAllByRole("button")).toHaveLength(7);
  });

  it("writes the type to the URL without a navigation and keeps focus on the pressed item", async () => {
    const { user } = renderPage();
    await findRow(/Kedai Awan/);
    const spending = within(rail()).getByRole("button", { name: /^Pengeluaran/ });
    await user.click(spending);
    expect(here()).toBe("/ledger?period=cycle&type=expense");
    expect(spending).toHaveFocus();
    expect(spending).toHaveAttribute("aria-pressed", "true");
    expect(navigation.replace).not.toHaveBeenCalled();
  });

  it("switches the period mode, resets the offset and shares it with Analisis", async () => {
    const { user } = renderPage("/ledger?period=cycle&offset=-2");
    await findRow(/Kedai Awan/);
    await user.click(within(rail()).getByRole("button", { name: "Bulan kalender" }));
    expect(here()).toBe("/ledger?period=month");
    expect(app.setTimeframe).toHaveBeenCalledWith("month");
    expect(app.setCycleOffset).toHaveBeenCalledWith(0);
  });

  it("takes another type from the one ordered list without other changes", () => {
    renderWithIntl(<LedgerTypeControl types={[...LEDGER_TYPES, "credit"]} type="all" counts={null} format={{ integer: String } as never} onType={vi.fn()} />);
    expect(within(screen.getByRole("group", { name: "Jenis" })).getAllByRole("button")).toHaveLength(LEDGER_TYPES.length + 1);
  });
});

describe("Toolbar, chips and the Filter dialog", () => {
  it("applies several filters at once, counts them, shows them as chips and returns focus to Filter", async () => {
    const { user } = renderPage();
    await findRow(/Kedai Awan/);
    await user.click(screen.getByRole("button", { name: "Filter" }));
    const dialog = await screen.findByRole("dialog", { name: "Filter transaksi" });
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Rekening" }), "00000000-0000-4000-8000-0000000000dd");
    await user.click(within(dialog).getByRole("radio", { name: "Belum dicek" }));
    await user.click(within(dialog).getByRole("radio", { name: "Dari HP" }));
    expect(here()).toBe("/ledger?period=cycle");
    await user.click(within(dialog).getByRole("button", { name: "Terapkan" }));
    expect(here()).toBe("/ledger?period=cycle&account=00000000-0000-4000-8000-0000000000dd&review=auto&source=notification");
    const filter = screen.getByRole("button", { name: "Filter, 3 aktif" });
    expect(filter).toHaveTextContent("Filter · 3");
    await waitFor(() => expect(filter).toHaveFocus());
    const chips = screen.getByRole("group", { name: "Filter aktif" });
    expect(within(chips).getAllByRole("button").map((button) => button.getAttribute("aria-label"))).toEqual([
      "Hapus filter Dompet Digital", "Hapus filter Belum dicek", "Hapus filter Dari HP",
    ]);
  });

  it("keeps the filters when the dialog is cancelled", async () => {
    const { user } = renderPage("/ledger?period=cycle&kakeibo=want");
    await findRow(/Kedai Awan/);
    await user.click(screen.getByRole("button", { name: "Filter, 1 aktif" }));
    const dialog = await screen.findByRole("dialog", { name: "Filter transaksi" });
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Kategori" }), "cat-makan");
    await user.keyboard("{Escape}");
    expect(here()).toBe("/ledger?period=cycle&kakeibo=want");
  });

  it("clears a duplicate filter from Laporan and changes the period mode below the rail", async () => {
    const { user } = renderPage("/ledger?dup=1");
    await findRow(/Kedai Awan/);
    await user.click(screen.getByRole("button", { name: "Filter, 1 aktif" }));
    const dialog = await screen.findByRole("dialog", { name: "Filter transaksi" });
    const duplicates = within(dialog).getByRole("checkbox", { name: "Kemungkinan duplikat" });
    expect(duplicates).toBeChecked();
    await user.click(duplicates);
    await user.click(within(dialog).getByRole("radio", { name: "Siklus gaji" }));
    await user.click(within(dialog).getByRole("button", { name: "Terapkan" }));
    expect(here()).toBe("/ledger?period=cycle");
  });

  it("removes a chip by keyboard and moves focus to the next chip, then to the search field", async () => {
    const { user } = renderPage("/ledger?period=cycle&waiting=1&kakeibo=want");
    await findRow(/Kedai Awan/);
    const chips = screen.getByRole("group", { name: "Filter aktif" });
    within(chips).getByRole("button", { name: "Hapus filter Keinginan" }).focus();
    await user.keyboard("{Enter}");
    expect(here()).toBe("/ledger?period=cycle&waiting=1");
    await waitFor(() => expect(screen.getByRole("button", { name: "Hapus filter Menunggu jawaban" })).toHaveFocus());
    await user.keyboard("{Enter}");
    await waitFor(() => expect(screen.getByRole("searchbox", { name: "Cari catatan, merchant, kategori" })).toHaveFocus());
  });

  it("shows a link's search and range as chips and searches by note", async () => {
    const { user } = renderPage("/ledger?from=2026-11-01T00:00:00%2B07:00&to=2026-11-05T23:59:59%2B07:00&q=kopi");
    await findRow(/Kedai Awan/);
    const chips = screen.getByRole("group", { name: "Filter aktif" });
    expect(chips).toHaveTextContent("1 Nov – 5 Nov");
    expect(chips).toHaveTextContent("“kopi”");
    expect(listCalls().at(-1)).toContain("q=kopi");
    await user.click(within(chips).getByRole("button", { name: "Hapus filter “kopi”" }));
    expect(here()).not.toContain("q=");
  });
});

describe("States", () => {
  it("says nothing matches and clears every filter but the period", async () => {
    pages = [[]];
    const { user } = renderPage("/ledger?period=cycle&account=00000000-0000-4000-8000-000000000001");
    expect(await screen.findByText("Tidak ada transaksi yang cocok")).toBeInTheDocument();
    expect(screen.getByText("Ubah pencarian atau filter.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Hapus semua filter" }));
    expect(here()).toBe("/ledger?period=cycle");
  });

  it("says nothing is recorded yet without a filter, search or period", async () => {
    pages = [[]];
    renderPage("/ledger");
    expect(await screen.findByText("Belum ada transaksi")).toBeInTheDocument();
    expect(screen.getByText("Transaksi dari HP dan yang dicatat manual muncul di sini.")).toBeInTheDocument();
  });

  it("shows the failure box without figures, and retries", async () => {
    failFirst = true;
    const { user } = renderPage();
    const title = await screen.findByText("Transaksi belum bisa dimuat");
    const alert = title.closest("[role=alert]") as HTMLElement;
    expect(screen.queryByRole("region", { name: "Pengeluaran" })).not.toBeInTheDocument();
    failFirst = false;
    await user.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    expect(await findRow(/Kedai Awan/)).toBeInTheDocument();
  });
});

describe("Merge into a transfer", () => {
  const OUT = row("00000000-0000-4000-8000-0000000000f1", "2026-11-02", "09:00", { merchant: "Transfer keluar", amount: 50_000, account_id: "acc-a" });
  const IN = row("00000000-0000-4000-8000-0000000000f2", "2026-11-02", "09:01", { merchant: "Transfer masuk", type: "income", amount: 50_000, account_id: "acc-b" });

  it("selects two rows from the menu, keeps movements out, and merges with today's request", async () => {
    pages = [[OUT, IN, { ...TOPUP, local_date: "2026-11-02" }]];
    withTotals = false;
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    const { user } = renderPage();
    await findRow(/Transfer keluar/);
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    expect(screen.queryByRole("menuitem", { name: "Rutin" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("menuitem", { name: "Gabungkan dua transaksi" }));
    const bar = screen.getByRole("region", { name: "Gabungkan dua transaksi" });
    expect(bar).toHaveTextContent("0 dari 2 dipilih");
    expect(screen.getByRole("checkbox", { name: /Top up Reksa Dana A/ })).toBeDisabled();
    await user.click(screen.getByRole("checkbox", { name: /Transfer keluar/ }));
    await user.click(screen.getByRole("checkbox", { name: /Transfer masuk/ }));
    expect(bar).toHaveTextContent("2 dari 2 dipilih");
    await user.click(within(bar).getByRole("button", { name: "Gabungkan jadi pindah saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", { expense_transaction_id: OUT.id, income_transaction_id: IN.id }));
    expect(await screen.findByText("Digabung sebagai pindah saldo.")).toBeInTheDocument();
  });

  it("walks the checkboxes with J while selecting, skipping rows that cannot join, and toggles with Space", async () => {
    pages = [[OUT, { ...TOPUP, local_date: "2026-11-02" }, IN]];
    withTotals = false;
    const { user } = renderPage();
    await findRow(/Transfer keluar/);
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    await user.click(screen.getByRole("menuitem", { name: "Gabungkan dua transaksi" }));
    const first = screen.getByRole("checkbox", { name: /Transfer keluar/ });
    first.focus();
    await user.keyboard(" ");
    await user.keyboard("j");
    const second = screen.getByRole("checkbox", { name: /Transfer masuk/ });
    expect(second).toHaveFocus();
    await user.keyboard(" ");
    expect(screen.getByRole("region", { name: "Gabungkan dua transaksi" })).toHaveTextContent("2 dari 2 dipilih");
  });

  it("rejects unequal amounts before converting", async () => {
    pages = [[OUT, { ...IN, amount: 60_000 }]];
    withTotals = false;
    const { user } = renderPage();
    await findRow(/Transfer keluar/);
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    await user.click(screen.getByRole("menuitem", { name: "Gabungkan dua transaksi" }));
    await user.click(screen.getByRole("checkbox", { name: /Transfer keluar/ }));
    await user.click(screen.getByRole("checkbox", { name: /Transfer masuk/ }));
    await user.click(screen.getByRole("button", { name: "Gabungkan jadi pindah saldo" }));
    const dialog = await screen.findByRole("dialog", { name: "Nominal tidak sama" });
    expect(dialog).toHaveTextContent("Nominal keluar (Rp 50.000) berbeda dengan nominal masuk (Rp 60.000).");
    expect(api.post).not.toHaveBeenCalled();
  });
});

describe("Transaction dialog", () => {
  it("opens from a row with provenance and marks it checked with an undo", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string) => {
      if (path === "/review/bulk") return { ok: true, undo_token: "t".repeat(43), expires_at: new Date(Date.now() + 600_000).toISOString() };
      if (path === "/review/undo") return { ok: true, kind: "review_accept", already_undone: false, transaction_ids: [KEDAI.id], removed_transaction_ids: [], restored: [{ id: KEDAI.id }] };
      throw new Error(path);
    });
    const { user } = renderPage();
    await user.click(await findRow(/Kedai Awan/));
    expect(here()).toBe(`/ledger?period=cycle&edit=${KEDAI.id}`);
    const dialog = await screen.findByRole("dialog", { name: "Kedai Awan" });
    expect(await within(dialog).findByText("Notifikasi bank")).toBeInTheDocument();
    expect(within(dialog).getByText("−Rp 30.000")).toHaveClass("c-expense");
    expect(document.querySelector(`[data-row="${KEDAI.id}"]`)).toHaveAttribute("data-selected");
    await user.click(within(dialog).getByRole("button", { name: "Tandai sudah dicek" }));
    expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: [KEDAI.id] });
    expect(await screen.findByText("Ditandai sudah dicek.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/undo", { undo_token: "t".repeat(43) }));
  });

  it("points a waiting row to Tinjau instead of offering the check", async () => {
    renderPage(`/ledger?period=cycle&edit=${QRIS.id}`);
    const dialog = await screen.findByRole("dialog", { name: "Tanpa keterangan" });
    expect(await within(dialog).findByRole("link", { name: "Jawab di Tinjau" })).toHaveAttribute("href", `/review?item=${QRIS.id}`);
    expect(within(dialog).queryByRole("button", { name: "Tandai sudah dicek" })).not.toBeInTheDocument();
  });

  it("asks before deleting a top-up, names its effects, and deletes it through its own lifecycle", async () => {
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const { user } = renderPage(`/ledger?period=cycle&edit=${TOPUP.id}`);
    const dialog = await screen.findByRole("dialog", { name: "Top up Reksa Dana A" });
    expect(await within(dialog).findByText("Tabungan & investasi")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Hapus" }));
    expect(dialog).toHaveTextContent("Hapus transaksi Rp 150.000?");
    expect(dialog).toHaveTextContent("Nilai produk berkurang Rp 150.000.");
    expect(dialog).toHaveTextContent("Penghapusan tidak dapat dibatalkan.");
    expect(within(dialog).getByRole("button", { name: "Batal" })).toHaveFocus();
    expect(api.del).not.toHaveBeenCalled();
    await user.click(within(dialog).getByRole("button", { name: "Hapus" }));
    expect(api.del).toHaveBeenCalledWith("/investment-topups/topup-1");
    expect(await screen.findByText("Transaksi dihapus.")).toBeInTheDocument();
  });

  it("deletes an ordinary row through the transaction path and names the balance effect", async () => {
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const { user } = renderPage(`/ledger?period=cycle&edit=${BAKSO.id}`);
    const dialog = await screen.findByRole("dialog", { name: "Bakso Mas Joko" });
    await user.click(await within(dialog).findByRole("button", { name: "Hapus" }));
    expect(dialog).toHaveTextContent("Saldo Dompet Digital bertambah kembali Rp 45.000.");
    await user.click(within(dialog).getByRole("button", { name: "Hapus" }));
    expect(api.del).toHaveBeenCalledWith(`/transactions/${BAKSO.id}`);
  });

  it("opens Ubah transaksi in place of the detail, saves today's PATCH and returns to the detail", async () => {
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    const { user } = renderPage(`/ledger?period=cycle&edit=${BAKSO.id}`);
    const detail = await screen.findByRole("dialog", { name: "Bakso Mas Joko" });
    await user.click(await within(detail).findByRole("button", { name: "Ubah" }));
    const edit = await screen.findByRole("dialog", { name: "Ubah transaksi" });
    expect(screen.queryByRole("dialog", { name: "Bakso Mas Joko" })).not.toBeInTheDocument();
    expect(within(edit).getByRole("radiogroup", { name: "Pilar Kakeibo" })).toBeInTheDocument();
    const merchant = within(edit).getByLabelText("Merchant atau tempat (opsional)");
    await user.clear(merchant);
    await user.type(merchant, "Bakso Contoh");
    await user.click(within(edit).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith(`/transactions/${BAKSO.id}`, expect.objectContaining({ merchant: "Bakso Contoh", type: "expense", amount: 45_000 })));
    expect(await screen.findByText("Perubahan disimpan.")).toBeInTheDocument();
    const back = await screen.findByRole("dialog", { name: "Bakso Mas Joko" });
    await waitFor(() => expect(within(back).getByRole("button", { name: "Ubah" })).toHaveFocus());
  });

  it("returns to the same Tinjau item and folder on close, and drops a tampered folder", async () => {
    const first = renderPage(`/ledger?edit=${KEDAI.id}&return=review&folder=jawab`);
    await first.user.click(within(await screen.findByRole("dialog", { name: "Kedai Awan" })).getByRole("button", { name: "Tutup detail" }));
    expect(navigation.push).toHaveBeenCalledWith(`/review?item=${KEDAI.id}&folder=jawab`);
    first.unmount();
    const second = renderPage(`/ledger?edit=${KEDAI.id}&return=review&folder=evil`);
    await second.user.click(within(await screen.findByRole("dialog", { name: "Kedai Awan" })).getByRole("button", { name: "Tutup detail" }));
    expect(navigation.push).toHaveBeenLastCalledWith(`/review?item=${KEDAI.id}`);
  });

  it("says a deleted row is gone", async () => {
    renderPage("/ledger?period=cycle&edit=00000000-0000-4000-8000-000000000000");
    expect(await screen.findByText(/Transaksi ini sudah tidak ada/)).toBeInTheDocument();
  });

  it("has no axe violation with the dialog open", async () => {
    renderPage(`/ledger?period=cycle&edit=${BAKSO.id}`);
    const dialog = await screen.findByRole("dialog", { name: "Bakso Mas Joko" });
    await within(dialog).findByText("Notifikasi bank");
    expect((await axe(document.body)).violations).toHaveLength(0);
  });
});

describe("Transaksi keys and focus", () => {
  it("walks the list with J and K inside it, and never while typing in search", async () => {
    const { user } = renderPage();
    const first = await findRow(/Kedai Awan/);
    first.focus();
    await user.keyboard("j");
    expect(rowButton(/Tanpa keterangan/)).toHaveFocus();
    await user.keyboard("k");
    expect(first).toHaveFocus();
    const search = screen.getByRole("searchbox", { name: "Cari catatan, merchant, kategori" });
    await user.click(search);
    await user.keyboard("j");
    expect(search).toHaveValue("j");
  });

  it("focuses the page's search once for fokus=cari and removes the parameter from the address", async () => {
    renderPage("/ledger?period=cycle&fokus=cari");
    await waitFor(() => expect(document.getElementById("ledger-search")).toHaveFocus());
    expect(window.location.search).toBe("?period=cycle");
  });
});

describe("Movement routes and saving directions", () => {
  const sent = row("00000000-0000-4000-8000-0000000000e1", "2026-11-04", "01:39", {
    merchant: null, notes: "Isi GoPay", type: "expense", category_name: "Internal Movement", category_system_key: "internal_movement", kakeibo_type: null,
    account_id: "00000000-0000-4000-8000-0000000000a1", account_name: "ATM", account_parent_name: "BCA", movement_id: "mv-a", movement_role: "outbound",
    partner_id: "00000000-0000-4000-8000-0000000000e2", transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
  });
  const saved = row("00000000-0000-4000-8000-0000000000e3", "2026-11-04", "01:41", {
    merchant: null, notes: "Internal Movement", type: "income", category_name: "Internal Movement", category_system_key: "internal_movement", kakeibo_type: null,
    account_id: "00000000-0000-4000-8000-0000000000a2", account_name: "Tabungan", account_parent_name: "Jago", movement_id: "mv-b", movement_role: "inbound",
    partner_id: "00000000-0000-4000-8000-0000000000e4", transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null, saving_flow: "saved",
  });
  const unpaired = row("00000000-0000-4000-8000-0000000000e5", "2026-11-04", "01:50", {
    merchant: null, notes: null, type: "income", category_name: "Internal Movement", category_system_key: "internal_movement", kakeibo_type: null,
    account_id: "00000000-0000-4000-8000-0000000000a3", account_name: "Main Pocket", account_parent_name: "Jago", amount: 120_000,
  });

  beforeEach(() => {
    withTotals = false;
  });

  it("titles a transfer by its note with the route as meta, else by its route, and names its saving direction", async () => {
    pages = [[sent, saved, unpaired]];
    renderPage();
    const noted = await findRow(/Isi GoPay/);
    expect(noted.querySelector(".r-meta")).toHaveTextContent("BCA · ATM → Gopay");
    expect(noted.querySelector(".r-cat")).toHaveTextContent("Pindah saldo");
    expect(noted.querySelector(".r-pillar")).toHaveTextContent("—");
    expect(within(noted).getByText("Rp 30.000")).toHaveClass("c-transfer");
    const routed = rowButton(/Gopay → Jago · Tabungan/);
    expect(routed.querySelector(".r-pillar")).toHaveTextContent("Ditabung");
    const lone = rowButton(/\? → Jago · Main Pocket/);
    expect(lone.querySelector(".r-meta .c-warning")).toHaveTextContent("Pasangan belum ditemukan");
    expect(document.body).not.toHaveTextContent("Tidak dihitung");
  });

  it("says Masuk dari in a list filtered to one account", async () => {
    pages = [[saved]];
    renderPage("/ledger?period=cycle&account=00000000-0000-4000-8000-0000000000a2");
    const routed = await findRow(/Gopay → Jago · Tabungan/);
    expect(routed.querySelector(".r-meta")).toHaveTextContent("Masuk dari Gopay");
  });

  it("gives the detail Dari and Ke with the missing side named, and the saving direction", async () => {
    pages = [[unpaired, saved]];
    const first = renderPage(`/ledger?period=cycle&edit=${unpaired.id}`);
    const detail = await screen.findByRole("dialog", { name: "? → Jago · Main Pocket" });
    const facts = (await within(detail).findByText("Dari")).closest("dl") as HTMLElement;
    expect(within(facts).getByText("Dari").nextElementSibling).toHaveTextContent("Pasangan belum ditemukan");
    expect(within(facts).getByText("Ke").nextElementSibling).toHaveTextContent("Jago · Main Pocket");
    first.unmount();
    renderPage(`/ledger?period=cycle&edit=${saved.id}`);
    const savedDetail = await screen.findByRole("dialog", { name: "Gopay → Jago · Tabungan" });
    expect((await within(savedDetail).findByText("Tabungan & investasi")).nextElementSibling).toHaveTextContent("Ditabung");
    expect(within(savedDetail).getByText("Status pasangan").nextElementSibling).toHaveTextContent("Lengkap");
  });
});

describe("Credit (H5)", () => {
  const SPL = "00000000-0000-4000-8000-00000000c0de";
  const purchase = row("00000000-0000-4000-8000-0000000000c1", "2026-11-05", "19:20", {
    merchant: "Warung Contoh", account_id: SPL, account_name: "SPayLater", amount: 120_000,
    credit: { role: "purchase", account_id: SPL, account_name: "SPayLater", installment_count: 1, installments_settled: 0, next_due_date: "2026-12-11", settled_on: null },
  });
  const payment = row("00000000-0000-4000-8000-0000000000c2", "2026-11-05", "08:00", {
    merchant: null, notes: null, category_name: "Internal Movement", category_system_key: "internal_movement", kakeibo_type: null, account_id: "acc-jago",
    account_name: "Jago", movement_id: "mv-c", movement_role: "outbound", transfer_target_account_name: "SPayLater", amount: 470_000, saving_flow: "debt_payment",
    credit: { role: "payment", account_id: SPL, account_name: "SPayLater" },
  });

  beforeEach(() => {
    withTotals = false;
  });

  it("says a purchase is paid later in red, and a payment is a bill payment", async () => {
    pages = [[purchase, payment]];
    renderPage();
    const bought = await findRow(/Warung Contoh/);
    expect(bought.querySelector(".r-meta")).toHaveTextContent("dibayar nanti · SPayLater · jatuh tempo 11 Des");
    expect(within(bought).getByText("−Rp 120.000")).toHaveClass("c-expense");
    const paid = rowButton(/Jago → SPayLater/);
    expect(paid.querySelector(".r-meta")).toHaveTextContent("bayar tagihan");
    expect(paid.querySelector(".r-pillar")).toHaveTextContent("Bayar utang");
  });

  it("lists the providers under Kredit and shows one provider's header from the credit overview", async () => {
    pages = [[purchase]];
    const { user } = renderPage("/ledger?period=cycle&type=credit");
    await findRow(/Warung Contoh/);
    expect(listCalls().at(-1)).toContain("type=credit");
    expect(screen.getByRole("region", { name: "Pengeluaran" })).toHaveTextContent("Dibayar nanti: uang keluar saat tagihan dibayar.");
    const providers = await screen.findByRole("group", { name: "Penyedia" });
    await user.click(within(providers).getByRole("button", { name: "SPayLater" }));
    expect(here()).toBe(`/ledger?period=cycle&type=credit&account=${SPL}`);
    const header = await screen.findByRole("region", { name: "SPayLater · 25 Okt – 24 Nov" });
    expect(vi.mocked(api.get)).toHaveBeenCalledWith(`/credit/accounts/${SPL}?from=2026-10-25&to=2026-11-24`);
    expect(header).toHaveTextContent("UtangRp 470.000");
    expect(header).toHaveTextContent("Tagihan berikutnyaRp 470.000jatuh tempo 11 Des");
    expect(header).toHaveTextContent("Pengeluaran pakai kreditRp 120.000");
    expect(header).toHaveTextContent("Tagihan kredit dibayarRp 350.000");
  });

  it("shows the terms in the dialog and changes the instalments through the credit-terms request", async () => {
    pages = [[purchase]];
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(api.put).mockResolvedValue({ ok: true });
    const { user } = renderPage(`/ledger?period=cycle&edit=${purchase.id}`);
    const detail = await screen.findByRole("dialog", { name: "Warung Contoh" });
    expect(await within(detail).findByText("Pembayaran")).toBeInTheDocument();
    expect(within(detail).getByText("Sekali bayar")).toBeInTheDocument();
    expect(within(detail).getByText("Jatuh tempo").nextElementSibling).toHaveTextContent("11 Des");
    await user.click(within(detail).getByRole("button", { name: "Ubah" }));
    const edit = await screen.findByRole("dialog", { name: "Ubah transaksi" });
    await user.click(await within(edit).findByRole("radio", { name: "3x" }));
    await user.click(within(edit).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.put).toHaveBeenCalledWith(`/transactions/${purchase.id}/credit-terms`, { installment_count: 3, first_due_date: null }));
    expect(api.patch).toHaveBeenCalled();
  });
});

describe("Transaksi in English", () => {
  it("reads Spending, Income, Net and Transfer, with no Indonesian region", async () => {
    const { container } = renderPage("/ledger?period=cycle", "en");
    await findRow(/Kedai Awan/);
    expect(screen.getByRole("region", { name: "Spending" })).toHaveTextContent("Net · income − spending");
    expect(screen.getByRole("navigation", { name: "Type and period" })).toHaveTextContent("Calendar month");
    expect(container.querySelector("[lang='id']")).toBeNull();
    expect(container.textContent).not.toMatch(/Expense|Ledger|Outflow|Inflow/);
    expect(container.querySelectorAll(".day-h")[0]).toHaveTextContent("Spending Rp 75.000 · 1 needs an answer");
  });
});
