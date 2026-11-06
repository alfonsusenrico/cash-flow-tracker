import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, api } from "@/lib/api";
import { ToastProvider } from "@/components/workspace/overlays";
import { ShortcutProvider } from "@/components/workspace/shortcuts";
import LedgerPage from "./page";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
  uploadTransactionReceipt: vi.fn(),
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

const app = vi.hoisted(() => ({ setTimeframe: vi.fn(), setCycleOffset: vi.fn() }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`, ...app }),
}));
vi.mock("@/components/recurring/PendingScheduledBanner", () => ({ PendingScheduledBanner: () => <p>Detail rencana</p> }));
vi.mock("@/components/recurring/RecurringRulesModal", () => ({ RecurringRulesModal: () => null }));

type Row = Record<string, unknown> & { id: string; date: string; local_date: string; review_status: string };

function row(id: string, local: string, time: string, extra: Record<string, unknown> = {}): Row {
  return {
    id, account_id: "acc-dd", account_name: "Dompet Digital", category_id: "cat-makan", category_name: "Makan di Luar",
    goal_id: null, goal_name: null, obligation_id: null, obligation_name: null, obligation_allocations: [], type: "expense",
    kakeibo_type: "want", amount: 30_000, notes: null, merchant: "Kedai Awan", date: `${local}T${time}:00+07:00`, local_date: local,
    receipt_path: null, created_at: `${local}T${time}:00+07:00`, source: "notification", review_status: "reviewed", classified_by: "ai",
    movement_id: null, investment_topup_id: null, ...extra,
  };
}

const KEDAI = row("00000000-0000-4000-8000-00000000000a", "2026-11-02", "16:05", { review_status: "auto" });
const QRIS = row("00000000-0000-4000-8000-00000000000b", "2026-11-02", "12:40", { merchant: null, category_name: null, kakeibo_type: null, amount: 45_000, review_status: "needs_review" });
const BAKSO = row("00000000-0000-4000-8000-00000000000c", "2026-11-01", "18:40", { merchant: "Bakso Mas Joko", amount: 45_000 });
const TOPUP = row("00000000-0000-4000-8000-00000000000d", "2026-11-01", "10:31", {
  merchant: null, notes: "Top up Investasi", movement_id: "mv-1", movement_role: "outbound", investment_topup_id: "topup-1",
  movement_kind: "investment_topup", transfer_target_account_name: "Reksa Dana A", amount: 150_000, category_name: "Internal Movement",
});
const ROWS = [KEDAI, QRIS, BAKSO, TOPUP];
const TOTALS = {
  "2026-11-02": { date: "2026-11-02", count: 2, inflow: 0, outflow: 75_000, spending: 75_000, movements: 0, waiting_count: 1, waiting_amount: 45_000 },
  "2026-11-01": { date: "2026-11-01", count: 2, inflow: 0, outflow: 45_000, spending: 45_000, movements: 150_000, waiting_count: 0, waiting_amount: 0 },
};
const SUMMARY = {
  cycle: { inflow: 9_800_000, outflow: 110_000, net: 9_690_000, spending: 110_000, saving: 0, spending_net: 9_690_000 },
  cumulative: { inflow: 9_800_000, outflow: 120_000, net: 9_680_000, spending: 120_000, saving: 0, spending_net: 9_680_000 },
  period: { inflow: 9_800_000, outflow: 110_000, net: 9_690_000, spending: 90_000, saving: 20_000, spending_net: 9_710_000, gross_outflow: 120_000, reimbursement_offset: 10_000, basis: "cycle", label: "Siklus sejak 25 Okt", start: "2026-10-25T01:02:00Z", end: "2026-11-24T16:59:59Z" },
};

let pages: Row[][];
let withTotals = true;
let failMore = false;
let width = 1280;

function mockApi() {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path.startsWith("/transactions?")) {
      const offset = Number(new URLSearchParams(path.split("?")[1]).get("offset"));
      const index = offset === 0 ? 0 : 1;
      if (index === 1 && failMore) throw new Error("network");
      const rows = pages[index] ?? [];
      const days = [...new Set(rows.map((item) => item.local_date))] as (keyof typeof TOTALS)[];
      return {
        ok: true, total: pages.flat().length, transactions: rows, summary: SUMMARY,
        ...(withTotals ? { daily_totals: days.map((day) => TOTALS[day]), type_counts: { all: 4, expense: 3, income: 0, movement: 0, investment: 1 } } : {}),
      };
    }
    const detail = /^\/transactions\/([^/?]+)$/.exec(path);
    if (detail) {
      const found = [...ROWS, ...pages.flat()].find((item) => item.id === detail[1]);
      if (!found) throw new ApiError(404, "Transaction not found");
      return { transaction: { ...found, waiting: found.review_status === "needs_review", origin: null } };
    }
    if (path === "/accounts") return { accounts: [{ id: "acc-dd", name: "Dompet Digital", type: "ewallet", balance: 500_000 }] };
    if (path === "/categories") return { categories: [{ id: "cat-makan", name: "Makan di Luar", kind: "expense", kakeibo_type: "want" }] };
    if (path === "/goals") return { goals: [] };
    if (path === "/obligations") return { obligations: [] };
    if (path === "/recurring/pending") return { ok: true, rules: [{ id: "plan-1", name: "Streamio", type: "expense", amount: 65_000, next_due_date: "2026-11-03", match_state: "none", candidates: [] }], recent_auto_links: [] };
    throw new Error(`Unexpected GET ${path}`);
  });
}

class FakeResizeObserver {
  constructor(private readonly callback: ResizeObserverCallback) {}
  observe(target: Element) {
    this.callback([{ target, contentRect: { width } as DOMRectReadOnly } as ResizeObserverEntry], this as unknown as ResizeObserver);
  }
  unobserve() {}
  disconnect() {}
}

function renderPage(url = "/ledger?period=cycle") {
  window.history.replaceState(null, "", url);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <ShortcutProvider>
        <ToastProvider>
          <LedgerPage />
        </ToastProvider>
      </ShortcutProvider>
    </QueryClientProvider>,
  );
  return { ...view, user: userEvent.setup() };
}

const realReplace = window.history.replaceState.bind(window.history);
const realPush = window.history.pushState.bind(window.history);

beforeEach(() => {
  pages = [[KEDAI, QRIS, BAKSO], [TOPUP]];
  withTotals = true;
  failMore = false;
  width = 1280;
  vi.stubGlobal("ResizeObserver", FakeResizeObserver);
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
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

const here = () => `${window.location.pathname}${window.location.search}`;
const listCalls = () => vi.mocked(api.get).mock.calls.map(([path]) => path as string).filter((path) => path.startsWith("/transactions?"));

describe("Transaksi list", () => {
  it("groups rows by day with the server's day totals and tab counts", async () => {
    const { container } = renderPage();
    const table = await screen.findByRole("table", { name: /dikelompokkan per hari/ });
    const headings = within(table).getAllByRole("rowheader").map((cell) => cell.textContent ?? "");
    expect(headings[0]).toMatch(/^Senin, 2 November/);
    expect(headings[0]).toContain("Pengeluaran Rp 75.000");
    expect(headings[0]).toContain("1 menunggu jawaban");
    expect(headings[1]).toContain("Pindah saldo Rp 150.000");
    expect(table.querySelectorAll("tbody")).toHaveLength(2);
    expect(table.querySelectorAll("tbody th[scope=rowgroup]")).toHaveLength(2);
    expect(screen.getByRole("button", { name: /^Investasi/ })).toHaveTextContent("1");
    expect(listCalls().at(-1)).toContain("include=daily_totals%2Ctype_counts");
    expect(listCalls().at(-1)).toContain("logical_movements=true");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows pillar and category columns instead of provenance, and colours each day figure by its kind", async () => {
    renderPage();
    const table = await screen.findByRole("table", { name: /dikelompokkan per hari/ });
    const heads = within(table).getAllByRole("columnheader").map((cell) => cell.textContent);
    expect(heads).toEqual(["Transaksi", "Jam", "Jenis", "Pilar", "Kategori", "Jumlah"]);
    const kedai = within(table).getByRole("button", { name: "Kedai Awan" }).closest("tr") as HTMLElement;
    expect(within(kedai).getByText("Keinginan")).toBeInTheDocument();
    expect(within(kedai).getByText("Makan di Luar")).toHaveAttribute("title", "Makan di Luar");
    expect(kedai).not.toHaveTextContent("Notifikasi bank");
    const qris = within(table).getByText("Menunggu jawaban").closest("tr") as HTMLElement;
    expect(qris.querySelector(".ws-tx-category")).toHaveTextContent("—");
    expect(table).not.toHaveTextContent("Internal Movement");
    const [nov2, nov1] = within(table).getAllByRole("rowheader");
    expect(within(nov2).getByText("Rp 75.000")).toHaveClass("ws-amt-out");
    expect(within(nov1).getByText("Rp 150.000")).toHaveClass("ws-amt-tr");
  });

  it("says how many rows of a split day are still to load, and keeps one heading after loading more", async () => {
    const { user } = renderPage();
    expect(await screen.findByText(/1 lagi di hari ini/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Muat 1 lagi" }));
    expect(await screen.findByRole("button", { name: "Top up Investasi" })).toBeInTheDocument();
    expect(within(screen.getByRole("table")).getAllByRole("rowheader")).toHaveLength(2);
    expect(screen.queryByText(/lagi di hari ini/)).not.toBeInTheDocument();
    expect(screen.getByText("1 transaksi lagi dimuat.")).toBeInTheDocument();
  });

  it("keeps the loaded rows and says so when loading more fails", async () => {
    failMore = true;
    const { user } = renderPage();
    await user.click(await screen.findByRole("button", { name: "Muat 1 lagi" }));
    expect(await screen.findByText("Gagal memuat transaksi berikutnya. Coba lagi.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Kedai Awan" })).toBeInTheDocument();
  });

  it("shows only the date on a day heading when the server sends no day totals", async () => {
    withTotals = false;
    renderPage();
    const table = await screen.findByRole("table");
    for (const heading of within(table).getAllByRole("rowheader")) expect(heading.textContent).not.toMatch(/Belanja|Rp/);
  });

  it("shows Masuk, Pengeluaran without saving and Bersih from the server, with the reimbursement line", async () => {
    renderPage();
    const strip = await screen.findByRole("region", { name: "Total Siklus sejak 25 Okt" });
    expect(strip).toHaveTextContent("PengeluaranRp 90.000");
    expect(strip).toHaveTextContent("sudah dikurangi penggantian Rp 10.000");
    expect(strip).toHaveTextContent("BersihRp 9.710.000");
    expect(strip).not.toHaveTextContent("Belanja");
  });

  it("says nothing was found and offers to clear the filters", async () => {
    pages = [[]];
    const { user } = renderPage("/ledger?period=cycle&account=00000000-0000-4000-8000-000000000001");
    expect(await screen.findByRole("heading", { name: "Tidak ditemukan" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Hapus filter" }));
    expect(here()).toBe("/ledger?period=cycle");
  });

  it("uses a feed with a heading per day when the list is phone-width", async () => {
    width = 390;
    renderPage();
    expect(await screen.findByRole("heading", { name: /Senin, 2 November/ })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});

describe("Due plans on Transaksi", () => {
  it("Due plan from the bell on Transaksi: the page shows no due-plan banner (the bell carries the plans)", async () => {
    renderPage();
    await screen.findByRole("table");
    expect(screen.queryByRole("region", { name: "Rencana jatuh tempo" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Lihat rencana" })).not.toBeInTheDocument();
    expect(screen.queryByText("Detail rencana")).not.toBeInTheDocument();
  });
});

describe("Searching from the phone header", () => {
  it("focuses the page's search once for fokus=cari and removes the parameter from the address", async () => {
    renderPage("/ledger?period=cycle&fokus=cari");
    await waitFor(() => expect(document.getElementById("ledger-search")).toHaveFocus());
    expect(window.location.search).toBe("?period=cycle");
  });
});

describe("Transaksi filters in the URL", () => {
  it("writes tabs and chips with the history API, also back to the plain ledger", async () => {
    const { user } = renderPage("/ledger?type=income");
    await screen.findByRole("table");
    await user.click(within(screen.getByRole("group", { name: "Jenis transaksi" })).getByRole("button", { name: /^Semua/ }));
    expect(here()).toBe("/ledger");
    await user.click(within(screen.getByRole("group", { name: "Jenis transaksi" })).getByRole("button", { name: /^Pindah saldo/ }));
    expect(here()).toBe("/ledger?type=movement");
    await user.click(screen.getByRole("button", { name: "Filter" }));
    const panel = screen.getByRole("region", { name: "Filter transaksi" });
    await user.click(within(panel).getByRole("button", { name: "Menunggu jawaban" }));
    expect(here()).toBe("/ledger?type=movement&waiting=1");
    await waitFor(() => expect(listCalls().at(-1)).toContain("waiting=true"));
    await user.click(within(panel).getByRole("button", { name: "Belum dicek" }));
    expect(here()).toBe("/ledger?type=movement&review=auto");
    await user.click(within(panel).getByRole("button", { name: "Belum dicek" }));
    expect(here()).toBe("/ledger?type=movement");
    expect(navigation.replace).not.toHaveBeenCalled();
  });

  it("keeps the toolbar to one row: tabs, search, Filter and a menu with Gabungkan and Rutin", async () => {
    const { user } = renderPage("/ledger?period=cycle");
    await screen.findByRole("table");
    expect(within(screen.getByRole("group", { name: "Jenis transaksi" })).getAllByRole("button").map((tab) => tab.textContent?.replace(/\d+$/, ""))).toEqual([
      "Semua", "Pengeluaran", "Masuk", "Pindah saldo", "Investasi",
    ]);
    // The chips, the period mode and the selects live in the popover, not in the toolbar.
    expect(screen.queryByRole("button", { name: "Menunggu jawaban" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Siklus gaji" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    const menu = screen.getByRole("region", { name: "Tindakan lain" });
    expect(within(menu).getByRole("button", { name: "Gabungkan dua transaksi" })).toBeInTheDocument();
    expect(within(menu).getByRole("button", { name: "Rutin" })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.getByRole("button", { name: "Tindakan lain" })).toHaveFocus();
  });

  it("reaches every filter in the popover, counts the active ones and shows them as removable chips", async () => {
    const { user } = renderPage("/ledger?period=cycle&waiting=1&kakeibo=want");
    await screen.findByRole("table");
    const trigger = screen.getByRole("button", { name: "Filter, 2 aktif" });
    const active = screen.getByRole("group", { name: "Filter aktif" });
    expect(within(active).getByRole("button", { name: "Hapus filter Menunggu jawaban" })).toBeInTheDocument();
    expect(within(active).getByRole("button", { name: "Hapus filter Pilar: Keinginan" })).toBeInTheDocument();
    await user.click(trigger);
    const panel = screen.getByRole("region", { name: "Filter transaksi" });
    for (const name of ["Siklus gaji", "Bulan kalender", "Semua waktu", "Menunggu jawaban", "Belum dicek", "Dari HP", "Lewat Catat"]) {
      expect(within(panel).getByRole("button", { name })).toBeInTheDocument();
    }
    for (const name of ["Rekening", "Kategori", "Pilar Kakeibo", "Status"]) expect(within(panel).getByRole("combobox", { name })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(trigger).toHaveFocus();
    await user.click(within(active).getByRole("button", { name: "Hapus filter Pilar: Keinginan" }));
    expect(here()).toBe("/ledger?period=cycle&waiting=1");
  });

  it("switches the period in the URL and shares it with Analisis", async () => {
    const { user } = renderPage("/ledger?period=cycle");
    await screen.findByRole("table");
    await user.click(screen.getByRole("button", { name: "Filter" }));
    await user.click(within(screen.getByRole("region", { name: "Filter transaksi" })).getByRole("button", { name: "Bulan kalender" }));
    expect(here()).toBe("/ledger?period=month");
    expect(app.setTimeframe).toHaveBeenCalledWith("month");
  });
});

describe("Transaction drawer", () => {
  it("opens from a row with provenance and marks it checked with an undo", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string) => {
      if (path === "/review/bulk") return { ok: true, undo_token: "t".repeat(43), expires_at: new Date(Date.now() + 600_000).toISOString() };
      if (path === "/review/undo") return { ok: true, kind: "review_accept", already_undone: false, transaction_ids: ["00000000-0000-4000-8000-00000000000a"], removed_transaction_ids: [], restored: [{ id: "00000000-0000-4000-8000-00000000000a" }] };
      throw new Error(path);
    });
    const { user } = renderPage();
    await user.click(await screen.findByRole("button", { name: "Kedai Awan" }));
    expect(here()).toBe("/ledger?period=cycle&edit=00000000-0000-4000-8000-00000000000a");
    const drawer = await screen.findByRole("dialog", { name: /^Kedai Awan/ });
    expect(await within(drawer).findByText("Notifikasi bank")).toBeInTheDocument();
    expect(within(drawer).getByText("Belum dicek")).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "Tandai sudah dicek" }));
    expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: ["00000000-0000-4000-8000-00000000000a"] });
    expect(await screen.findByText("Ditandai sudah dicek.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/undo", { undo_token: "t".repeat(43) }));
    expect(await screen.findByText("Dibatalkan: Kedai Awan.")).toBeInTheDocument();
  });

  it("points a waiting row to Tinjau instead of offering the check", async () => {
    renderPage("/ledger?period=cycle&edit=00000000-0000-4000-8000-00000000000b");
    const drawer = await screen.findByRole("dialog", { name: /^Tanpa keterangan/ });
    expect(await within(drawer).findByRole("link", { name: "Jawab di Tinjau" })).toHaveAttribute("href", "/review?item=00000000-0000-4000-8000-00000000000b");
    expect(within(drawer).queryByRole("button", { name: "Tandai sudah dicek" })).not.toBeInTheDocument();
  });

  it("asks before deleting and deletes a top-up through its own lifecycle", async () => {
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const { user } = renderPage("/ledger?period=cycle&edit=00000000-0000-4000-8000-00000000000d");
    const drawer = await screen.findByRole("dialog", { name: /^Top up Investasi/ });
    await user.click(await within(drawer).findByRole("button", { name: "Hapus" }));
    const confirm = within(drawer).getByRole("group", { name: "Hapus transaksi Rp 150.000?" });
    expect(confirm).toHaveTextContent("Nilai produk berkurang Rp 150.000.");
    expect(within(confirm).getByRole("button", { name: "Batal" })).toHaveFocus();
    expect(api.del).not.toHaveBeenCalled();
    await user.click(within(confirm).getByRole("button", { name: "Hapus" }));
    expect(api.del).toHaveBeenCalledWith("/investment-topups/topup-1");
    expect(await screen.findByText("Transaksi dihapus.")).toBeInTheDocument();
  });

  it("deletes an ordinary row through the transaction path and names the balance effect", async () => {
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const { user } = renderPage("/ledger?period=cycle&edit=00000000-0000-4000-8000-00000000000c");
    const drawer = await screen.findByRole("dialog", { name: /^Bakso Mas Joko/ });
    await user.click(await within(drawer).findByRole("button", { name: "Hapus" }));
    const confirm = within(drawer).getByRole("group", { name: /^Hapus transaksi/ });
    expect(confirm).toHaveTextContent("Saldo Dompet Digital bertambah kembali Rp 45.000.");
    await user.click(within(confirm).getByRole("button", { name: "Hapus" }));
    expect(api.del).toHaveBeenCalledWith("/transactions/00000000-0000-4000-8000-00000000000c");
  });

  it("edits inline and saves the same PATCH as the old edit dialog", async () => {
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    const { user } = renderPage("/ledger?period=cycle&edit=00000000-0000-4000-8000-00000000000c");
    const drawer = await screen.findByRole("dialog", { name: /^Bakso Mas Joko/ });
    await user.click(await within(drawer).findByRole("button", { name: "Ubah" }));
    const merchant = await within(drawer).findByLabelText("Merchant atau tempat (opsional)");
    await user.clear(merchant);
    await user.type(merchant, "Bakso Contoh");
    await user.click(within(drawer).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/transactions/00000000-0000-4000-8000-00000000000c", expect.objectContaining({ merchant: "Bakso Contoh", type: "expense", amount: 45_000 })));
    expect(await screen.findByText("Perubahan disimpan.")).toBeInTheDocument();
  });

  it("returns to the same Tinjau item and folder on close, and drops a tampered folder", async () => {
    const first = renderPage("/ledger?edit=00000000-0000-4000-8000-00000000000a&return=review&folder=jawab");
    await first.user.click(within(await screen.findByRole("dialog", { name: /^Kedai Awan/ })).getByRole("button", { name: "Tutup detail" }));
    expect(navigation.push).toHaveBeenCalledWith("/review?item=00000000-0000-4000-8000-00000000000a&folder=jawab");
    first.unmount();
    const second = renderPage("/ledger?edit=00000000-0000-4000-8000-00000000000a&return=review&folder=evil");
    await second.user.click(within(await screen.findByRole("dialog", { name: /^Kedai Awan/ })).getByRole("button", { name: "Tutup detail" }));
    expect(navigation.push).toHaveBeenLastCalledWith("/review?item=00000000-0000-4000-8000-00000000000a");
  });

  it("says a deleted row is gone", async () => {
    renderPage("/ledger?period=cycle&edit=00000000-0000-4000-8000-000000000000");
    expect(await screen.findByText(/Transaksi ini sudah tidak ada/)).toBeInTheDocument();
  });
});

describe("Transaksi keys", () => {
  it("walks the list with J and K inside it, and never while typing in search", async () => {
    const { user } = renderPage();
    const first = await screen.findByRole("button", { name: "Kedai Awan" });
    first.focus();
    await user.keyboard("j");
    expect(screen.getByRole("button", { name: "Tanpa keterangan" })).toHaveFocus();
    await user.keyboard("k");
    expect(first).toHaveFocus();
    const search = screen.getByRole("searchbox", { name: "Cari transaksi" });
    await user.click(search);
    await user.keyboard("j");
    expect(search).toHaveValue("j");
  });
});

describe("Movement routes", () => {
  // Synthetic: BCA's ATM pocket sends to Gopay, Gopay sends on to Jago's Tabungan, and Jago's Main Pocket has an
  // income filed as a movement that nobody paired.
  const sent = row("00000000-0000-4000-8000-0000000000e1", "2026-11-04", "01:39", {
    merchant: null, notes: "Pindah saldo ke Gopay", type: "expense", category_name: "Internal Movement", kakeibo_type: null,
    account_id: "00000000-0000-4000-8000-0000000000a1", account_name: "ATM", account_parent_name: "BCA", movement_id: "mv-a", movement_role: "outbound",
    partner_id: "00000000-0000-4000-8000-0000000000e2", transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
  });
  const received = row("00000000-0000-4000-8000-0000000000e3", "2026-11-04", "01:41", {
    merchant: null, notes: "Dari GoPay", type: "income", category_name: "Internal Movement", kakeibo_type: null,
    account_id: "00000000-0000-4000-8000-0000000000a2", account_name: "Tabungan", account_parent_name: "Jago", movement_id: "mv-b", movement_role: "inbound",
    partner_id: "00000000-0000-4000-8000-0000000000e4", transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
  });
  const unpaired = row("00000000-0000-4000-8000-0000000000e5", "2026-11-04", "01:50", {
    merchant: null, notes: "Pindah uang dari BCA", type: "income", category_name: "Internal Movement", kakeibo_type: null,
    account_id: "00000000-0000-4000-8000-0000000000a3", account_name: "Main Pocket", account_parent_name: "Jago", amount: 120_000,
  });

  beforeEach(() => {
    withTotals = false;
  });

  it("titles every movement from where the money left to where it arrived, with the bank", async () => {
    pages = [[sent, received, unpaired]];
    renderPage();
    expect(await screen.findByRole("button", { name: "BCA · ATM → Gopay" })).toBeInTheDocument();
    // The inbound leg reads from its partner; it used to read "Tabungan → Gopay".
    expect(screen.getByRole("button", { name: "Gopay → Jago · Tabungan" })).toBeInTheDocument();
    const lone = screen.getByRole("button", { name: "? → Jago · Main Pocket" });
    expect(lone.closest("tr")).toHaveTextContent("Pasangan belum ditemukan · Pindah uang dari BCA");
  });

  it("says Masuk dari or Keluar ke in a list filtered to one account", async () => {
    pages = [[received]];
    const first = renderPage("/ledger?period=cycle&account=00000000-0000-4000-8000-0000000000a2");
    const title = await screen.findByRole("button", { name: "Gopay → Jago · Tabungan" });
    expect(title.closest("tr")).toHaveTextContent("Masuk dari Gopay · Dari GoPay");
    first.unmount();

    pages = [[sent]];
    width = 390;
    renderPage("/ledger?period=cycle&account=00000000-0000-4000-8000-0000000000a1");
    const feedRow = await screen.findByText("BCA · ATM → Gopay");
    expect(feedRow.closest("button")).toHaveTextContent("Keluar ke Gopay");
  });

  it("gives the detail the same Dari and Ke, with the missing side named", async () => {
    pages = [[unpaired]];
    renderPage(`/ledger?period=cycle&edit=${unpaired.id}`);
    const detail = await screen.findByRole("dialog", { name: /^\? → Jago · Main Pocket/ });
    const facts = within(detail).getByText("Dari").closest("dl") as HTMLElement;
    expect(within(facts).getByText("Dari").nextElementSibling).toHaveTextContent("Pasangan belum ditemukan");
    expect(within(facts).getByText("Ke").nextElementSibling).toHaveTextContent("Jago · Main Pocket");
  });
});
