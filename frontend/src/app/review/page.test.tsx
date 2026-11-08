import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "@/components/workspace/overlays";
import { ShortcutProvider, SHORTCUTS_STORAGE_KEY } from "@/components/workspace/shortcuts";
import { api } from "@/lib/api";
import type { SuggestionList } from "@/lib/historySuggestions";
import type { AwaitingItem, ReviewItem, ReviewQueue } from "@/lib/review";
import ReviewPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
const ctx = vi.hoisted(() => ({ hidden: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: ctx.hidden, bal: (amount: number) => (ctx.hidden ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`) }),
}));

/** Node's own experimental localStorage global shadows jsdom's, so each test gets an in-memory one. */
function installStorage() {
  const data = new Map<string, string>();
  const storage = {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => void data.set(key, String(value)),
    removeItem: (key: string) => void data.delete(key),
    clear: () => data.clear(),
    key: (index: number) => [...data.keys()][index] ?? null,
    get length() { return data.size; },
  };
  Object.defineProperty(window, "localStorage", { configurable: true, value: storage });
  Object.defineProperty(window, "sessionStorage", { configurable: true, value: storage });
  return data;
}

const item = (id: string, overrides: Partial<ReviewItem> = {}): ReviewItem => ({
  id, type: "expense", amount: 125_000, date: "2026-10-20T05:00:00Z", account_id: "account-bca", account_name: "BCA Harian",
  category_id: "category-food", category_name: "Makanan & Minuman", kakeibo_type: "need", merchant: "Kedai Awan",
  notes: "Kedai Awan", review_status: "auto", source: "notification", classified_by: "rule", movement_id: null,
  movement_role: null, fee_for_movement_id: null, reasons: [], ...overrides,
});

const EXPIRES = () => new Date(Date.now() + 10 * 60_000).toISOString();
const token = (value: string) => ({ undo_token: value.padEnd(43, "x"), expires_at: EXPIRES() });

/** One open question, a needs-review group of two, an automatic group, one awaiting send and one ambiguous credit. */
function queue(): ReviewQueue {
  return {
    ok: true,
    truncated: false,
    counts: { attention: 5, unreviewed: 2, questions: 1, needs_review: 2, auto: 2, groups: 2, awaiting: 1, ambiguous: 1 },
    questions: [{
      event_id: "event-1",
      post_time: "2026-10-21T11:00:00Z",
      question: {
        question_id: "question-1",
        prompt: "Apa ini? Rp187.500 dari BCA Harian",
        allows_text: true,
        text_max_length: 200,
        suggestions: [
          { id: "s1", kind: "history", label: "belanja dapur", category_name: "Belanja", kakeibo: "need" },
          { id: "s2", kind: "current", label: "Pengeluaran Groceries", category_name: null, kakeibo: null },
        ],
      },
      transaction: {
        id: "tx-question", type: "expense", amount: 187_500, date: "2026-10-21T11:00:00Z",
        account_name: "BCA Harian", notes: "Pengeluaran Groceries", merchant: null, category_name: null,
      },
    }],
    groups: [
      {
        key: "expense|merchant:toko serba ada", kind: "merchant", type: "expense", label: "Toko Serba Ada",
        count: 2, total: 400_000, needs_review_count: 2, auto_count: 0,
        first_date: "2026-10-19T05:00:00Z", last_date: "2026-10-20T05:00:00Z", reasons: ["low_confidence"], category: null,
        transactions: [
          item("tx-c", { merchant: "Toko Serba Ada", notes: "Toko Serba Ada", amount: 200_000, review_status: "needs_review", reasons: ["low_confidence"] }),
          item("tx-d", { merchant: "Toko Serba Ada", notes: "Toko Serba Ada", amount: 200_000, review_status: "needs_review" }),
        ],
      },
      {
        key: "expense|merchant:kedai awan", kind: "merchant", type: "expense", label: "Kedai Awan",
        count: 2, total: 250_000, needs_review_count: 0, auto_count: 2,
        first_date: "2026-10-18T05:00:00Z", last_date: "2026-10-20T05:00:00Z", reasons: [],
        category: { id: "category-food", name: "Makanan & Minuman" },
        transactions: [item("tx-a"), item("tx-b", { date: "2026-10-18T05:00:00Z" })],
      },
    ],
    awaiting: [{
      ...item("tx-await", {
        amount: 500_000, account_id: "account-gopay", account_name: "GoPay", notes: "Pindah saldo", merchant: null,
        category_name: "Internal Movement", review_status: "needs_review", reasons: ["awaiting_counterpart"],
      }),
      topup_suggestion: null,
    }],
    ambiguous: [{
      ...item("tx-ambiguous", { type: "income", amount: 500_000, notes: "Transfer masuk", merchant: null, review_status: "needs_review", reasons: ["ambiguous_movement"] }),
      candidates: [
        { id: "tx-send-1", type: "expense", amount: 500_000, date: "2026-10-20T04:59:00Z", account_id: "account-gopay", account_name: "GoPay" },
        { id: "tx-send-2", type: "expense", amount: 500_000, date: "2026-10-20T04:58:00Z", account_id: "account-ovo", account_name: "OVO" },
      ],
    }] as ReviewQueue["ambiguous"],
  };
}

const emptySuggestions: SuggestionList = { ok: true, truncated: false, pending: 0, groups: [] };
function suggestionList(): SuggestionList {
  return {
    ok: true, truncated: false, pending: 3,
    groups: [{
      key: "expense|kafe", type: "expense", label: "Kafe Senja Contoh", merchant: "Kafe Senja Contoh",
      category: { id: "category-snack", name: "Jajan & Nongkrong" }, kakeibo: "want", offset_category_names: [], sources: ["rule"],
      before: [{ label: "Kafe Senja Contoh", category_name: "Kopi & Jajan", count: 3 }], count: 3, total: 105_000,
      first_date: "2026-10-28T09:00:00Z", last_date: "2026-10-31T09:00:00Z",
      suggestions: ["a", "b", "c"].map((key) => ({
        id: `sg-${key}`, transaction_id: `tx-cafe-${key}`, type: "expense" as const, amount: 35_000, date: "2026-10-31T09:00:00Z",
        account_name: "BCA Harian", source: "rule" as const, confidence: null, reason: "Kafe masuk ke Jajan & Nongkrong",
        before: { merchant: "Kafe Senja Contoh", notes: "Kafe Senja Contoh", category_id: "c", category_name: "Kopi & Jajan", kakeibo: "want" as const },
        after: { merchant: "Kafe Senja Contoh", notes: "Kafe Senja Contoh", category_id: "category-snack", category_name: "Jajan & Nongkrong", kakeibo: "want" as const, offset_category_id: null, offset_category_name: null },
      })),
    }],
  };
}

const summary = {
  keluar: { total: 3_521_500, offsets_total: 0, lines: [{ key: "unclassified", label: "Belum diklasifikasi", amount: 357_000 }] },
  safe_to_spend: { status: "available", reason: null, detail: null, per_day: 145_000, days_left: 22, rounding: 1000, over_by: null },
  remaining: { total: 5_000_000, lines: [] },
};

let currentQueue: ReviewQueue;
let currentSuggestions: SuggestionList;
let summaryFails = false;
let width = 1280;

class FakeResizeObserver {
  constructor(private readonly callback: ResizeObserverCallback) {}
  observe(target: Element) {
    this.callback([{ target, contentRect: { width } as DOMRectReadOnly } as ResizeObserverEntry], this as unknown as ResizeObserver);
  }
  unobserve() {}
  disconnect() {}
}

function renderPage(url = "/review") {
  window.history.replaceState(null, "", url);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <ShortcutProvider>
        <ToastProvider>
          <button type="button">Cari transaksi</button>
          <ReviewPage />
        </ToastProvider>
      </ShortcutProvider>
    </QueryClientProvider>,
  );
  return { ...view, queryClient, user: userEvent.setup() };
}

const itemHeading = () => document.getElementById("triage-item-heading") as HTMLElement;
const pane = () => itemHeading().closest("section") as HTMLElement;
const row = (name: string) => {
  const rows = [...document.querySelectorAll<HTMLElement>("[data-row]")];
  const found = rows.find((button) => button.textContent?.includes(name));
  if (!found) throw new Error(`No row ${name}`);
  return found;
};
const status = () => screen.getAllByRole("status").map((node) => node.textContent ?? "").join(" ");

async function loaded() {
  await screen.findByRole("heading", { name: /Perlu ditinjau/ });
  await waitFor(() => expect(itemHeading()).toBeInTheDocument());
}

/** The answered item leaves the queue, as the server's next response would show. */
function removeFromQueue(ids: string[]) {
  const next = { ...currentQueue };
  next.questions = next.questions.filter((entry) => !ids.includes(entry.transaction.id));
  next.groups = next.groups.filter((group) => !group.transactions.some((tx) => ids.includes(tx.id)));
  next.awaiting = next.awaiting.filter((entry) => !ids.includes(entry.id));
  next.ambiguous = next.ambiguous.filter((entry) => !ids.includes(entry.id));
  currentQueue = next;
}

describe("Tinjau triage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    installStorage();
    currentQueue = queue();
    currentSuggestions = emptySuggestions;
    summaryFails = false;
    width = 1280;
    ctx.hidden = false;
    vi.stubGlobal("ResizeObserver", FakeResizeObserver);
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/review/queue") return currentQueue;
      if (path === "/categories") return { categories: [
        { id: "category-food", name: "Makanan & Minuman", kind: "expense", kakeibo_type: "need" },
        { id: "category-snack", name: "Jajan & Nongkrong", kind: "expense", kakeibo_type: "want" },
        // Renamed by the owner: the key, not the name, keeps it out of the spending choices.
        { id: "category-internal", name: "Transfer", kind: "expense", system_key: "internal_movement" },
        { id: "category-salary", name: "Gaji", kind: "income" },
      ] };
      if (path === "/accounts") return { accounts: [
        { id: "account-bca", name: "BCA Harian", type: "bank", parent_id: null },
        { id: "account-gopay", name: "GoPay", type: "ewallet", parent_id: null },
        { id: "account-rdn", name: "RDN Contoh", type: "bank", parent_id: null },
        { id: "account-bibit", name: "Bibit Contoh", type: "investment", instrument_type: "mutual_fund", parent_id: null, is_parent: true, children: [
          { id: "fund-a", name: "Reksa Dana A", type: "investment", instrument_type: "mutual_fund", units: null, parent_id: "account-bibit" },
          { id: "fund-b", name: "Reksa Dana B", type: "investment", instrument_type: "mutual_fund", units: null, parent_id: "account-bibit" },
          { id: "fund-unit", name: "Reksa Dana Unit", type: "investment", instrument_type: "mutual_fund", units: 12.5, parent_id: "account-bibit" },
        ] },
      ] };
      if (path === "/review/suggestions") return currentSuggestions;
      if (path === "/review/suggestions/runs/latest") return { ok: true, run: null };
      if (path === "/dashboard/cycle-summary") {
        if (summaryFails) throw new Error("summary down");
        return summary;
      }
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/undo") return { ok: true, kind: "review_accept", already_undone: false, transaction_ids: [], removed_transaction_ids: [], restored: [] };
      if (String(path).includes("answer-purpose")) return { ok: true, answer: { status: "applied", notes: "belanja dapur" }, ...token("answer") };
      return { ok: true, ...token("action") };
    });
  });
  afterEach(() => vi.unstubAllGlobals());

  // --- Folders, counts and layout ---------------------------------------------------------------

  it("Counts match the badge, with no axe violations", async () => {
    const { container } = renderPage();
    await loaded();
    const chips = screen.getByRole("group", { name: "Saring Tinjau" });
    expect(within(chips).getAllByRole("button").map((chip) => chip.textContent)).toEqual([
      "Semua5", "Belum dikenali1", "Perlu jawaban2", "Pindah saldo belum lengkap2",
    ]);
    expect(within(chips).getByRole("button", { name: /^Semua/ })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("heading", { name: "Perlu ditinjau 5 transaksi" })).toBeInTheDocument();
    expect(screen.getByText("2 dicatat otomatis · opsional")).toBeInTheDocument();
    const list = screen.getByRole("list", { name: /Semua: yang perlu dijawab/ });
    expect(within(list).getAllByRole("listitem")).toHaveLength(4);
    // The header shows no amount (phase plan D9: waiting rows are counted).
    expect(screen.getByRole("heading", { name: /Perlu ditinjau/ }).parentElement).not.toHaveTextContent("Rp");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Folder filter", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(screen.getByRole("button", { name: /^Pindah saldo belum lengkap/ }));
    expect(screen.getByRole("button", { name: /^Pindah saldo belum lengkap/ })).toHaveAttribute("aria-pressed", "true");
    const list = screen.getByRole("list", { name: /Pindah saldo belum lengkap: yang perlu dijawab/ });
    expect(within(list).getAllByRole("listitem").map((entry) => entry.textContent)).toEqual([
      expect.stringContaining("Keluar dari GoPay"), expect.stringContaining("Masuk ke BCA Harian"),
    ]);
    expect(window.location.search).toBe("?item=tx-await&folder=pindah");
  });

  it("Question rows follow the money's direction", async () => {
    const base = queue();
    const [entry] = base.questions;
    currentQueue = { ...base, counts: { ...base.counts, attention: 6, questions: 2 }, questions: [entry, {
      ...entry, event_id: "event-income", post_time: "2026-10-21T12:00:00Z",
      question: { ...entry.question, question_id: "question-income", prompt: "Apa ini? Rp10.000 masuk ke Kantong Utama" },
      transaction: {
        ...entry.transaction, id: "tx-income-question", type: "income", amount: 10_000, date: "2026-10-21T12:00:00Z",
        account_name: "Bank Jago · Kantong Utama", notes: "Uang masuk dari GoPay",
      },
    }] };
    renderPage();
    await loaded();
    const income = row("Uang masuk dari GoPay");
    expect(income).toHaveTextContent("Belum dikenali · Uang masuk");
    expect(income).not.toHaveTextContent("Pembayaran tanpa nama toko");
    expect(income).not.toHaveTextContent("−");
    expect(within(income).getByText("+Rp 10.000")).toHaveClass("ws-amt-in");
    const expense = row("Pengeluaran Groceries");
    expect(expense).toHaveTextContent("Belum dikenali · Pembayaran tanpa nama toko");
    expect(within(expense).getByText("−Rp 187.500")).not.toHaveClass("ws-amt-in");
  });

  it("Optional automatic rows", async () => {
    renderPage();
    await loaded();
    const fold = screen.getByText("Dicatat otomatis", { selector: "b" }).closest("details") as HTMLDetailsElement;
    expect(fold.open).toBe(false);
    expect(within(fold).getByText("opsional · boleh dilewati")).toBeInTheDocument();
    expect(within(fold).getByRole("button", { name: /Kedai Awan/, hidden: true })).toBeInTheDocument();
    expect(screen.getByRole("list", { name: /Semua/ })).not.toHaveTextContent("Kedai Awan");
  });

  it("Automatic rows inside a mixed group", async () => {
    const data = queue();
    data.groups[0] = { ...data.groups[0], count: 4, auto_count: 2, transactions: [...data.groups[0].transactions, item("tx-e", { merchant: "Toko Serba Ada" }), item("tx-f", { merchant: "Toko Serba Ada" })] };
    currentQueue = data;
    renderPage();
    await loaded();
    expect(screen.getByText(/2 lainnya ikut kelompok di Perlu jawaban/)).toBeInTheDocument();
  });

  it("Two panes on a wide screen", async () => {
    renderPage();
    await loaded();
    expect(screen.getByRole("region", { name: "Tinjau" })).toHaveAttribute("data-panes", "two");
    expect(itemHeading()).toHaveTextContent("Apa ini? Rp187.500 dari BCA Harian");
  });

  it("One pane at 768 px, and Back to the list", async () => {
    width = 660;
    const { user } = renderPage();
    await screen.findByRole("heading", { name: /Perlu ditinjau/ });
    expect(screen.getByRole("region", { name: "Tinjau" })).toHaveAttribute("data-panes", "one");
    expect(document.getElementById("triage-item-heading")).toBeNull();
    const length = window.history.length;
    await user.click(row("Toko Serba Ada"));
    expect(itemHeading()).toHaveTextContent("Toko Serba Ada");
    expect(window.history.length).toBe(length + 1);
    expect(screen.queryByRole("heading", { name: /Perlu ditinjau/ })).not.toBeInTheDocument();
    await waitFor(() => expect(itemHeading()).toHaveFocus());
    // The browser's back gesture returns to the list with focus on the item's row.
    act(() => {
      window.history.replaceState(null, "", "/review");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });
    await waitFor(() => expect(row("Toko Serba Ada")).toHaveFocus());
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: "Daftar" }));
    await waitFor(() => expect(screen.getByRole("heading", { name: /Perlu ditinjau/ })).toBeInTheDocument());
  });

  it("Hidden balances", async () => {
    ctx.hidden = true;
    renderPage();
    await loaded();
    expect(pane()).toHaveTextContent("−Rp ••••••");
    expect(pane()).not.toHaveTextContent("187.500");
  });

  // --- Answers --------------------------------------------------------------------------------

  it("answers a question with a numbered choice or with text", async () => {
    const { user } = renderPage();
    await loaded();
    const choice = within(pane()).getByRole("button", { name: /belanja dapur.*Saran/ });
    expect(choice).toHaveAttribute("data-primary-choice");
    await user.click(within(pane()).getByRole("button", { name: "Simpan jawaban" }));
    expect(within(pane()).getByRole("alert")).toHaveTextContent("Tulis jawaban singkat");
    await user.type(within(pane()).getByRole("textbox", { name: "Jawab sendiri" }), "sewa lapangan futsal{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/ingest/notifications/event-1/answer-purpose", expect.objectContaining({
      question_id: "question-1", text: "sewa lapangan futsal", reply_id: expect.any(String),
    })));
  });

  it("names the habit guess with its basis", async () => {
    const base = queue();
    const [entry] = base.questions;
    currentQueue = { ...base, questions: [{ ...entry, question: { ...entry.question, suggestions: [
      { id: "s1", kind: "habit", label: "Sepertinya Makanan & Minuman?", category_name: "Makanan & Minuman", kakeibo: "need", basis: { agreeing: 3, support: 4 } },
      ...entry.question.suggestions.map((suggestion, index) => ({ ...suggestion, id: `s${index + 2}` })),
    ] } }] };
    const { container } = renderPage();
    await loaded();
    expect(within(pane()).getByRole("button", { name: /^Sepertinya Makanan & Minuman\?.*Saran.*3 dari 4 jawaban sebelumnya/ })).toBeInTheDocument();
    expect(within(pane()).getAllByText(/jawaban sebelumnya/)).toHaveLength(1);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Keyboard group accept, Next item and Urungkan", async () => {
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/bulk") removeFromQueue(["tx-c"]);
      if (path === "/review/undo") currentQueue = queue();
      return { ok: true, ...token("bulk") };
    });
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    const accept = within(pane()).getByRole("button", { name: /Terima semua \(2\)/ });
    accept.focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: ["tx-c", "tx-d"] }));
    // The following item of the list as it was is selected and focused, and the message names it.
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay. Masuknya ke mana?"));
    await waitFor(() => expect(itemHeading()).toHaveFocus());
    expect(status()).toContain("Tersimpan: Toko Serba Ada, 2 transaksi ditandai sudah ditinjau. Berikutnya: Keluar dari GoPay.");
    await user.click(screen.getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/undo", { undo_token: expect.stringMatching(/^bulk/) }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Toko Serba Ada"));
    expect(status()).toContain("Dibatalkan. Toko Serba Ada kembali ke Perlu jawaban.");
  });

  it("recategorises and renames a group with one request each", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Ubah kategori/ }));
    expect(within(pane()).getByRole("button", { name: /Ubah kategori/ })).toHaveAttribute("aria-expanded", "true");
    const category = within(pane()).getByRole("combobox", { name: "Kategori" });
    expect(within(category).queryByRole("option", { name: "Gaji" })).not.toBeInTheDocument();
    await user.selectOptions(category, "category-snack");
    await user.selectOptions(within(pane()).getByRole("combobox", { name: "Pilar Kakeibo" }), "want");
    await user.click(within(pane()).getByRole("button", { name: "Simpan untuk 2" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", {
      action: "recategorise", transaction_ids: ["tx-c", "tx-d"], category_id: "category-snack", kakeibo_type: "want",
    }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Ganti nama/ }));
    const merchant = within(pane()).getByRole("textbox", { name: "Nama toko" });
    await user.clear(merchant);
    await user.type(merchant, "Toko Serba Ada Baru");
    await user.click(within(pane()).getByRole("button", { name: "Simpan untuk 2" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/bulk", {
      action: "rename", transaction_ids: ["tx-c", "tx-d"], merchant: "Toko Serba Ada Baru",
    }));
    expect(within(pane()).getAllByRole("link", { name: /Ubah transaksi Toko Serba Ada/ })[0]).toHaveAttribute("href", "/ledger?edit=tx-c&return=review&folder=jawab");
  });

  it("Failed bulk action", async () => {
    vi.mocked(api.post).mockRejectedValueOnce(Object.assign(new Error("x"), {
      status: 409, detail: { code: "movement_leg_not_editable", message: "Pindah saldo diubah lewat detail pindah saldo." },
    }));
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Terima semua/ }));
    expect(await within(pane()).findByRole("alert")).toHaveTextContent("Pindah saldo diubah lewat detail pindah saldo.");
    expect(status()).not.toContain("Tersimpan");
  });

  it("Partial failure and Batched undo of a group of 250 rows", async () => {
    const rows = Array.from({ length: 250 }, (_, index) => item(`tx-big-${index}`, { merchant: "Toko Serba Ada", review_status: "needs_review" }));
    currentQueue = { ...queue(), groups: [{ ...queue().groups[0], count: 250, needs_review_count: 250, transactions: rows }] };
    let call = 0;
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/bulk") {
        call += 1;
        if (call === 2) throw Object.assign(new Error("x"), { status: 500, detail: { message: "Server sibuk." } });
        return { ok: true, ...token(`batch${call}`) };
      }
      return { ok: true };
    });
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Terima semua \(250\)/ }));
    expect(await within(pane()).findByRole("alert")).toHaveTextContent("200 dari 250 transaksi tersimpan; sisanya gagal: Server sibuk.");

    // Both batches saved this time; the older batch's undo is refused.
    let saved = 10;
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      if (path === "/review/bulk") {
        saved += 1;
        return { ok: true, ...token(`b${saved}`) };
      }
      if ((body as { undo_token: string }).undo_token.startsWith("b11")) {
        throw Object.assign(new Error("x"), { status: 409, detail: { code: "undo_conflict", transaction_ids: ["tx-big-3"] } });
      }
      return { ok: true };
    });
    await user.click(within(pane()).getByRole("button", { name: /Terima semua \(250\)/ }));
    await waitFor(() => expect(status()).toContain("Tersimpan"));
    act(() => itemHeading().focus());
    await user.keyboard("z");
    expect(await screen.findByText(/50 dari 250 transaksi dibatalkan/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Buka di Transaksi" })).toHaveAttribute("href", "/ledger?edit=tx-big-3&return=review");
  });

  it("Failed undo", async () => {
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/undo") throw Object.assign(new Error("x"), { status: 410, detail: { code: "undo_expired" } });
      return { ok: true, ...token("bulk") };
    });
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Terima semua/ }));
    await user.click(await screen.findByRole("button", { name: "Urungkan" }));
    const alert = await within(pane()).findByRole("alert");
    expect(alert).toHaveTextContent("Sudah lewat 10 menit, jadi tidak dapat dibatalkan di sini.");
    expect(within(alert).getByRole("link", { name: "Buka di Transaksi" })).toHaveAttribute("href", "/ledger?edit=tx-c&return=review");
    expect(status()).not.toContain("Dibatalkan");
  });

  it("Response without a token, Nothing to undo and Tokens are not stored", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, undo_token: null, expires_at: null });
    const storage = installStorage();
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Terima semua/ }));
    await waitFor(() => expect(status()).toContain("Tersimpan"));
    expect(screen.queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
    act(() => itemHeading().focus());
    await user.keyboard("z");
    expect(await screen.findByText("Tidak ada yang dapat dibatalkan.")).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalledWith("/review/undo", expect.anything());
    vi.mocked(api.post).mockResolvedValue({ ok: true, ...token("secret") });
    await user.click(within(pane()).getByRole("button", { name: /Terima semua/ }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Urungkan" })).toBeInTheDocument());
    expect([...storage.values()].join(" ") + window.location.href).not.toContain("secret");
  });

  it("Undo by Z after the message closed", async () => {
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/bulk") removeFromQueue(["tx-c"]);
      if (path === "/review/undo") currentQueue = queue();
      return { ok: true, ...token("bulk") };
    });
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(within(pane()).getByRole("button", { name: /Terima semua/ }));
    const undoButton = await screen.findByRole("button", { name: "Urungkan" });
    // The message closes on its own after 6.5 s; the entry stays until the server expiry.
    await waitFor(() => expect(undoButton).not.toBeInTheDocument(), { timeout: 8000 });
    act(() => itemHeading().focus());
    await user.keyboard("z");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/undo", { undo_token: expect.any(String) }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Toko Serba Ada"));
    await waitFor(() => expect(itemHeading()).toHaveFocus());
  }, 15000);

  // --- Keys -------------------------------------------------------------------------------------

  it("Walk and accept", async () => {
    currentQueue = { ...queue(), questions: [], awaiting: [], ambiguous: [], groups: [
      ...["Satu", "Dua", "Tiga"].map((name, index) => ({ ...queue().groups[0], key: `g${index}`, label: `Toko ${name}`, transactions: [item(`tx-${name}`, { merchant: `Toko ${name}`, review_status: "needs_review" })] })),
    ] };
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      if (path === "/review/bulk") removeFromQueue((body as { transaction_ids: string[] }).transaction_ids);
      return { ok: true, ...token("walk") };
    });
    const { user } = renderPage("/review?folder=jawab");
    await screen.findByRole("heading", { name: /Perlu ditinjau/ });
    const list = await screen.findByRole("list", { name: /Perlu jawaban: yang perlu dijawab/ });
    within(list).getAllByRole("button")[0].focus();
    await user.keyboard("j");
    await waitFor(() => expect(within(list).getAllByRole("button")[1]).toHaveFocus());
    expect(itemHeading()).toHaveTextContent("Toko Dua");
    await user.keyboard("e");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: ["tx-Dua"] }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Toko Tiga"));
  });

  it("Outside the region and Typing an answer", async () => {
    const { user } = renderPage();
    await loaded();
    // Let any focus scheduled for the next frame happen first.
    await act(() => new Promise((resolve) => window.requestAnimationFrame(() => resolve(undefined))));
    screen.getByRole("button", { name: "Cari transaksi" }).focus();
    await user.keyboard("j");
    expect(itemHeading()).toHaveTextContent("Apa ini?");
    await user.type(within(pane()).getByRole("textbox", { name: "Jawab sendiri" }), "1 kopi");
    expect(api.post).not.toHaveBeenCalled();
    expect(itemHeading()).toHaveTextContent("Apa ini?");
  });

  it("Switched off", async () => {
    window.localStorage.setItem(SHORTCUTS_STORAGE_KEY, "off");
    const { user } = renderPage();
    await loaded();
    act(() => itemHeading().focus());
    await user.keyboard("e1j");
    expect(api.post).not.toHaveBeenCalled();
    expect(itemHeading()).toHaveTextContent("Apa ini?");
  });

  it("presses numbered choices, T and E inside the item", async () => {
    currentQueue = { ...queue(), questions: [] };
    const { user } = renderPage();
    await loaded();
    act(() => itemHeading().focus());
    await user.keyboard("2");
    expect(within(pane()).getByRole("button", { name: /Ubah kategori/ })).toHaveAttribute("aria-expanded", "true");
    await user.click(row("Keluar dari GoPay"));
    act(() => itemHeading().focus());
    await user.keyboard("t");
    expect(within(pane()).getByRole("group", { name: "Ini top up investasi. Ke produk mana?" })).toBeInTheDocument();
    await user.keyboard("1");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", { transaction_id: "tx-await", target_account_id: "fund-a" }));
    await user.click(row("Masuk ke BCA Harian"));
    act(() => itemHeading().focus());
    await user.keyboard("e");
    expect(await screen.findByText("Belum ada saran untuk diterima.")).toBeInTheDocument();
  });

  it("Help lists the page keys", async () => {
    const { user } = renderPage();
    await loaded();
    act(() => itemHeading().focus());
    await user.keyboard("?");
    const help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    for (const key of ["J / K", "Enter", "E", "T", "1–9", "Z"]) {
      const cap = within(help).getByText(key, { selector: "kbd" });
      expect(cap.closest("tr")).toHaveTextContent(/di Tinjau$/);
    }
    expect(help).toHaveTextContent("Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan.");
    expect((await axe(help)).violations).toHaveLength(0);
  });

  // --- Movements and top-ups ------------------------------------------------------------------

  it("completes an awaiting transfer with one press and resolves an ambiguous one", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    await user.click(within(pane()).getByRole("button", { name: /^BCA Harian/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", { transaction_id: "tx-await", target_account_id: "account-bca" }));
    await user.click(row("Masuk ke BCA Harian"));
    await user.click(within(pane()).getByRole("button", { name: /^OVO/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", { expense_transaction_id: "tx-send-2", income_transaction_id: "tx-ambiguous" }));
    await user.click(row("Masuk ke BCA Harian"));
    await user.click(within(pane()).getByRole("button", { name: /Biarkan terpisah/ }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/bulk", { action: "keep_separate", transaction_ids: ["tx-ambiguous"] }));
  });

  it("completes a one-sided debit as a credit payment from a Kredit choice", async () => {
    const getAccounts = vi.mocked(api.get).getMockImplementation()!;
    vi.mocked(api.get).mockImplementation(async (path, signal) => {
      if (path === "/accounts") {
        const listed = (await getAccounts(path, signal)) as { accounts: unknown[] };
        return { accounts: [...listed.accounts, { id: "account-spaylater", name: "SPayLater", type: "credit", parent_id: null, balance: -470_000, credit_owed: 470_000 }] };
      }
      return getAccounts(path, signal);
    });
    const { user, container } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    const choice = await within(pane()).findByRole("button", { name: /^SPayLater/ });
    expect(choice).toHaveTextContent("Kredit · bayar tagihan ke sini");
    expect(within(pane()).getByRole("button", { name: /^BCA Harian/ })).toHaveTextContent("Pindah saldo ke sini");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(choice);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", { transaction_id: "tx-await", target_account_id: "account-spaylater" }));
  });

  it("offers the existing leg instead of recording a transfer twice", async () => {
    const candidate = { id: "tx-existing-in", type: "income", amount: 500_000, date: "2026-10-20T05:37:00Z", account_id: "account-bca", category_name: "Internal Movement", notes: "Pindah saldo contoh" };
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      if (path === "/movements/complete" && !(body as { create_new?: boolean }).create_new) {
        throw Object.assign(new Error("conflict"), { status: 409, detail: { code: "counterpart_candidates", message: "x", candidates: [candidate] } });
      }
      return { ok: true, ...token("m") };
    });
    const { user, container } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    await user.click(within(pane()).getByRole("button", { name: /^BCA Harian/ }));
    const existing = await within(pane()).findByRole("group", { name: "Sudah ada di rekening ini" });
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(within(existing).getByRole("button", { name: /Hubungkan/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", { expense_transaction_id: "tx-await", income_transaction_id: "tx-existing-in" }));
    expect(api.post).not.toHaveBeenCalledWith("/movements/complete", expect.objectContaining({ create_new: true }));
  });

  it("creates a new leg only when the owner confirms it", async () => {
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      if (path === "/movements/complete" && !(body as { create_new?: boolean }).create_new) {
        throw Object.assign(new Error("conflict"), { status: 409, detail: { code: "counterpart_candidates", message: "x", candidates: [{
          id: "tx-other", type: "income", amount: 500_000, date: "2026-10-20T05:37:00Z", account_id: "account-bca", category_name: "Pendapatan Lain", notes: null,
        }] } });
      }
      return { ok: true };
    });
    const { user } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    await user.click(within(pane()).getByRole("button", { name: /^BCA Harian/ }));
    await user.click(await within(pane()).findByRole("button", { name: /Tetap catat baru/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", { transaction_id: "tx-await", target_account_id: "account-bca", create_new: true }));
  });

  it("Leg recorded as spending", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    await user.click(within(pane()).getByRole("button", { name: /Bukan pindah saldo, ini pengeluaran/ }));
    const picker = within(pane()).getByRole("group", { name: "Bukan pindah saldo, ini pengeluaran" });
    expect(within(within(picker).getByRole("combobox", { name: "Kategori" })).queryByRole("option", { name: "Transfer" })).not.toBeInTheDocument();
    await user.click(within(picker).getByRole("button", { name: "Simpan sebagai pengeluaran" }));
    expect(within(picker).getByRole("alert")).toHaveTextContent("Pilih kategori pengeluarannya.");
    await user.selectOptions(within(picker).getByRole("combobox", { name: "Kategori" }), "category-food");
    await user.click(within(picker).getByRole("button", { name: "Simpan sebagai pengeluaran" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledTimes(1));
    expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "recategorise", transaction_ids: ["tx-await"], category_id: "category-food" });
  });

  describe("top-up of an awaiting debit", () => {
    const rdnDebit = (suggestion: AwaitingItem["topup_suggestion"] = null): AwaitingItem => ({
      ...item("tx-rdn", {
        amount: 112_590, account_id: "account-rdn", account_name: "RDN Contoh", notes: "Pindah saldo keluar",
        category_name: "Internal Movement", review_status: "needs_review", reasons: ["awaiting_counterpart"], merchant: null,
      }),
      topup_suggestion: suggestion,
    });
    const productA = { id: "fund-a", name: "Reksa Dana A", label: "Bibit Contoh · Reksa Dana A" };
    const productB = { id: "fund-b", name: "Reksa Dana B", label: "Bibit Contoh · Reksa Dana B" };
    const openRdn = async (user: ReturnType<typeof userEvent.setup>) => {
      await loaded();
      await user.click(row("Keluar dari RDN Contoh"));
    };

    it("settles a debit as a top-up and explains unit-tracked funds", async () => {
      currentQueue = { ...queue(), awaiting: [rdnDebit()] };
      const { user, container } = renderPage();
      await openRdn(user);
      const openTopup = within(pane()).getByRole("button", { name: /Ini top up investasi/ });
      expect(openTopup).toHaveAttribute("aria-expanded", "false");
      await user.click(openTopup);
      const picker = within(pane()).getByRole("group", { name: "Ini top up investasi. Ke produk mana?" });
      expect(within(picker).getAllByRole("button").map((button) => button.textContent)).toEqual([
        "Bibit Contoh · Reksa Dana ADicatat per nominal1", "Bibit Contoh · Reksa Dana BDicatat per nominal2",
      ]);
      expect(within(picker).getByText(/Bibit Contoh · Reksa Dana Unit masih dicatat per unit/)).toBeInTheDocument();
      expect(within(picker).getByRole("link", { name: "Rekening" })).toHaveAttribute("href", "/accounts");
      expect((await axe(container)).violations).toHaveLength(0);
      await user.click(within(picker).getByRole("button", { name: /Reksa Dana A/ }));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", { transaction_id: "tx-rdn", target_account_id: "fund-a" }));
      await waitFor(() => expect(status()).toContain("debit dicatat sebagai top up investasi"));
    });

    it("does not offer a top-up for an incoming leg", async () => {
      currentQueue = { ...queue(), awaiting: [{ ...rdnDebit(), type: "income" }] };
      const { user } = renderPage();
      await loaded();
      await user.click(row("Masuk ke RDN Contoh"));
      expect(within(pane()).queryByRole("button", { name: /Ini top up investasi/ })).not.toBeInTheDocument();
      expect(within(pane()).queryByRole("button", { name: /Bukan pindah saldo/ })).not.toBeInTheDocument();
    });

    it("accepts, corrects or rejects a learned suggestion", async () => {
      currentQueue = { ...queue(), awaiting: [rdnDebit({
        target_account_id: "fund-a", target_label: productA.label, choices: [productA],
        prompt: "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?",
        explanation: "Dari 1 dari 1 jawaban sebelumnya untuk debit Rp112.590 dari RDN Contoh.",
        basis: { agreeing: 1, support: 1, agreement: 1 },
      })] };
      const { user, container } = renderPage();
      await openRdn(user);
      expect(within(pane()).getByText("Sepertinya top up ke Reksa Dana A, kayak bulan lalu?")).toBeInTheDocument();
      expect((await axe(container)).violations).toHaveLength(0);
      const yes = within(pane()).getByRole("button", { name: /Iya, top up Reksa Dana A/ });
      expect(yes).toHaveAttribute("data-primary-choice");
      await user.click(yes);
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", {
        transaction_id: "tx-rdn", target_account_id: "fund-a", suggested_target_account_id: "fund-a",
      }));
      await user.click(row("Keluar dari RDN Contoh"));
      await user.click(within(pane()).getByRole("button", { name: /Pilih lain/ }));
      await user.click(within(pane()).getByRole("button", { name: /Ini top up investasi/ }));
      const picker = within(pane()).getByRole("group", { name: "Ini top up investasi. Ke produk mana?" });
      expect(within(picker).queryByRole("button", { name: /Reksa Dana A/ })).not.toBeInTheDocument();
      await user.click(within(picker).getByRole("button", { name: /Reksa Dana B/ }));
      await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/investment-topups/from-transaction", {
        transaction_id: "tx-rdn", target_account_id: "fund-b", suggested_target_account_id: "fund-a",
      }));
      await user.click(row("Keluar dari RDN Contoh"));
      await user.click(within(pane()).getByRole("button", { name: /^Bukan top up/ }));
      await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/topup-suggestions/reject", {
        transaction_id: "tx-rdn", suggested_target_account_id: "fund-a",
      }));
    });

    it("asks without a default when the product cannot be assigned", async () => {
      currentQueue = { ...queue(), awaiting: [rdnDebit({
        target_account_id: null, target_label: null, choices: [productA, productB],
        prompt: "Debit Rp112.590 dari RDN Contoh biasanya jadi top up ke Reksa Dana A atau Reksa Dana B. Yang mana?",
        explanation: "Belum bisa dipastikan produknya, jadi kamu yang pilih.", basis: null,
      })] };
      const { user } = renderPage();
      await openRdn(user);
      expect(within(pane()).getAllByRole("button").filter((button) => button.hasAttribute("data-primary-choice"))).toHaveLength(0);
      await user.click(within(pane()).getByRole("button", { name: /Bibit Contoh · Reksa Dana B/ }));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", { transaction_id: "tx-rdn", target_account_id: "fund-b" }));
    });
  });

  // --- States and links -----------------------------------------------------------------------

  it("Answered elsewhere", async () => {
    const { user, queryClient } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    removeFromQueue(["tx-c"]);
    await act(async () => {
      await queryClient.invalidateQueries();
    });
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay"));
    expect(screen.getByText("Item ini sudah diselesaikan di tempat lain.")).toBeInTheDocument();
  });

  it("Nothing waiting, with the cycle figures and Cek yang otomatis", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [queue().groups[1]] };
    const { container, user } = renderPage();
    expect(await screen.findByRole("heading", { name: "Semua sudah ditinjau" })).toBeInTheDocument();
    const facts = await screen.findByLabelText("Siklus ini");
    expect(facts).toHaveTextContent("Pengeluaran siklus iniRp 3.521.500");
    expect(facts).toHaveTextContent("Aman dipakai sampai gajianRp 5.000.000");
    expect(facts).toHaveTextContent("Belum diklasifikasiRp 357.000");
    expect(screen.getByRole("link", { name: "Kembali ke Beranda" })).toHaveAttribute("href", "/");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Cek yang otomatis" }));
    expect(itemHeading()).toHaveTextContent("Kedai Awan");
    expect(within(pane()).getByRole("button", { name: /Sudah benar/ })).toHaveAttribute("data-primary-choice");
  });

  it("Summary unavailable", async () => {
    summaryFails = true;
    currentQueue = { ...queue(), questions: [], awaiting: [], ambiguous: [], groups: [] };
    renderPage();
    expect(await screen.findByRole("heading", { name: "Semua sudah ditinjau" })).toBeInTheDocument();
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/dashboard/cycle-summary"));
    expect(screen.queryByLabelText("Siklus ini")).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("Rp 0");
  });

  it("Not counted as waiting: suggestions are optional and decided as items", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [] };
    currentSuggestions = suggestionList();
    const { user } = renderPage();
    expect(await screen.findByRole("heading", { name: "Semua sudah ditinjau" })).toBeInTheDocument();
    const fold = screen.getByText("Saran dari riwayat", { selector: "b" }).closest("details") as HTMLElement;
    expect(within(fold).getByText("3")).toBeInTheDocument();
    expect(within(fold).getByText("opsional")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Perlu ditinjau 0 transaksi" })).toBeInTheDocument();
    await user.click(row("Kafe Senja Contoh"));
    await user.click(within(pane()).getAllByRole("button", { name: /^Tolak saran untuk/ })[0]);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/suggestions/reject", { suggestion_ids: ["sg-a"] }));
    await user.click(row("Kafe Senja Contoh"));
    await user.click(within(pane()).getByRole("button", { name: /Terima saran \(3\)/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/suggestions/accept", { suggestion_ids: ["sg-a", "sg-b", "sg-c"] }));
  });

  it("Terlihat dua kali: confirm one transaction, or record it on its own with Urungkan", async () => {
    currentQueue = {
      ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 1, seen_twice: 1 },
      questions: [], awaiting: [], ambiguous: [], groups: [],
      seen_twice: [{
        event_id: "event-jago", kind: "duplicate", app: "Bank Jago", type: "income", amount: 500_000,
        date: "2026-10-31T11:39:18Z", account_name: "Bank Jago · Kantong Utama",
        kept: {
          transaction_id: "tx-gopay", type: "income", amount: 500_000, date: "2026-10-31T11:39:18Z", account_name: "GoPay",
          movement: { source: "BCA Harian", target: "GoPay" },
        },
      }],
    };
    const { user, container } = renderPage();
    expect(await screen.findByRole("heading", { name: "Semua sudah ditinjau" })).toBeInTheDocument();
    const fold = screen.getByText("Terlihat dua kali", { selector: "b" }).closest("details") as HTMLElement;
    expect(within(fold).getByText("opsional")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Perlu ditinjau 0 transaksi" })).toBeInTheDocument();
    await user.click(row("Bank Jago: laporan kedua"));
    expect(itemHeading()).toHaveTextContent("Bank Jago melaporkan uang yang sudah tercatat");
    expect(within(pane()).getByText("BCA Harian → GoPay")).toBeInTheDocument();
    expect(within(pane()).getByText("Tidak dicatat lagi")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);

    await user.click(within(pane()).getByRole("button", { name: /Catat terpisah/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/seen-twice/event-jago/record-separately", {}));
    expect(status()).toMatch(/dicatat sebagai transaksi terpisah/);
    await user.click(within(screen.getAllByRole("status").find((node) => within(node).queryByRole("button", { name: "Urungkan" }))!).getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/undo", { undo_token: "action".padEnd(43, "x") }));

    await user.click(row("Bank Jago: laporan kedua"));
    await user.click(within(pane()).getByRole("button", { name: /Benar, satu transaksi/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/seen-twice/event-jago/acknowledge", {}));
  });

  it("Terlihat dua kali: Benar, satu transaksi links the account, and Batalkan stops the link", async () => {
    currentQueue = {
      ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 1, seen_twice: 1 },
      questions: [], awaiting: [], ambiguous: [], groups: [],
      seen_twice: [{
        event_id: "event-jago", kind: "duplicate", app: "Bank Jago", type: "income", amount: 500_000,
        date: "2026-10-31T11:39:18Z", account_name: "Bank Jago · Kantong Utama",
        kept: {
          transaction_id: "tx-gopay", type: "income", amount: 500_000, date: "2026-10-31T11:39:18Z", account_name: "GoPay",
          movement: { source: "BCA Harian", target: "GoPay" },
        },
      }],
    };
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (String(path).endsWith("/acknowledge")) {
        return { ok: true, event_id: "event-jago", link: { id: "link-1", account_id: "acc-gopay", account_label: "GoPay", institution_label: "Bank Jago", created: true } };
      }
      return { ok: true, ...token("action") };
    });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const { user } = renderPage();
    await screen.findByRole("heading", { name: "Semua sudah ditinjau" });
    await user.click(row("Bank Jago: laporan kedua"));
    expect(within(pane()).getByText("GoPay juga terlihat di Bank Jago")).toBeInTheDocument();
    expect(within(pane()).getByText("Rekening ditautkan; laporan serupa dilewati otomatis")).toBeInTheDocument();
    await user.click(within(pane()).getByRole("button", { name: /Benar, satu transaksi/ }));
    const toast = await screen.findByText("GoPay juga dilaporkan Bank Jago. Laporan serupa dilewati otomatis.");
    await user.click(within(toast.closest(".ws-toast") as HTMLElement).getByRole("button", { name: "Batalkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/ingest/account-links/link-1"));
    expect(await screen.findByText("Tautan dibatalkan. Laporan Bank Jago serupa akan ditanyakan lagi.")).toBeInTheDocument();
  });

  it("Dilewati otomatis: a skipped report shows its account link and can be recorded on its own", async () => {
    currentQueue = {
      ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 0, seen_twice: 0, skipped: 1 },
      questions: [], awaiting: [], ambiguous: [], groups: [],
      skipped: [{
        event_id: "event-skip", kind: "skipped", app: "Bank Jago", type: "income", amount: 500_000,
        date: "2026-11-01T11:39:18Z", account_name: "Bank Jago · Kantong Utama",
        kept: {
          transaction_id: "tx-gopay-2", type: "income", amount: 500_000, date: "2026-11-01T11:39:18Z", account_name: "GoPay",
          movement: { source: "BCA Harian", target: "GoPay" },
        },
        link: { id: "link-1", account_label: "GoPay", institution_label: "Bank Jago" },
      }],
    };
    const { user, container } = renderPage();
    await screen.findByRole("heading", { name: "Semua sudah ditinjau" });
    const fold = screen.getByText("Dilewati otomatis", { selector: "b" }).closest("details") as HTMLElement;
    expect(within(fold).getByText("1")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Perlu ditinjau 0 transaksi" })).toBeInTheDocument();
    await user.click(row("Bank Jago: dilewati"));
    expect(itemHeading()).toHaveTextContent("Bank Jago melaporkan uang yang sudah tercatat");
    expect(within(pane()).getByText("Dilewati, tidak dicatat")).toBeInTheDocument();
    expect(within(pane()).getByText("GoPay juga dilaporkan Bank Jago")).toBeInTheDocument();
    const choices = within(within(pane()).getByRole("group", { name: "Keputusan" })).getAllByRole("button");
    expect(choices.map((choice) => choice.querySelector(".ws-lbl")?.textContent)).toEqual(["Catat terpisah"]);
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(choices[0]);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/seen-twice/event-skip/record-separately", {}));
    expect(status()).toMatch(/dicatat sebagai transaksi terpisah/);
  });

  it("Failure of a suggestion decision, and skipped rows reported truthfully", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [] };
    currentSuggestions = suggestionList();
    vi.mocked(api.post)
      .mockResolvedValueOnce({ ok: true, accepted: 2, skipped: ["sg-c"], corrections: 2, ...token("sugg") })
      .mockRejectedValueOnce(Object.assign(new Error("x"), { status: 404, detail: { code: "suggestions_not_found", message: "Sebagian saran tidak ditemukan." } }));
    const { user } = renderPage();
    await screen.findByRole("heading", { name: "Semua sudah ditinjau" });
    await user.click(row("Kafe Senja Contoh"));
    await user.click(within(pane()).getByRole("button", { name: /Terima saran \(3\)/ }));
    await waitFor(() => expect(status()).toContain("Kafe Senja Contoh, 2 saran diterima; 1 dilewati"));
    await user.click(row("Kafe Senja Contoh"));
    await user.click(within(pane()).getByRole("button", { name: /^Tolak\s*Saran ini tidak/ }));
    expect(await within(pane()).findByRole("alert")).toHaveTextContent("Sebagian saran tidak ditemukan.");
    expect(status()).not.toContain("ditolak");
  });

  it("Link from Perlu tindakan", async () => {
    renderPage("/review?item=tx-await#review-awaiting");
    await loaded();
    expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay");
    expect(screen.getByRole("button", { name: /^Pindah saldo belum lengkap/ })).toHaveAttribute("aria-pressed", "true");
    await waitFor(() => expect(itemHeading()).toHaveFocus());
    // The legacy hash is read once and dropped; the place is kept in the query.
    expect(window.location.hash).toBe("");
    expect(window.location.search).toBe("?item=tx-await&folder=pindah");
  });

  it.each([
    ["#review-questions", "tx-question", "Apa ini?"],
    ["#review-groups", "tx-c", "Toko Serba Ada"],
    ["#review-ambiguous", "tx-ambiguous", "Masuk ke BCA Harian"],
  ])("Beranda link %s lands on its item", async (hash, id, title) => {
    renderPage(`/review?item=${id}${hash}`);
    await loaded();
    expect(itemHeading()).toHaveTextContent(title);
  });

  it("Resolved meanwhile", async () => {
    renderPage("/review?item=tx-gone#review-questions");
    await loaded();
    expect(screen.getByRole("button", { name: /^Belum dikenali/ })).toHaveAttribute("aria-pressed", "true");
    expect(itemHeading()).toHaveTextContent("Apa ini? Rp187.500");
    expect(screen.getByText("Item yang dibuka sudah selesai.")).toBeInTheDocument();
  });

  it("shows ErrorState with Coba lagi when the queue fails", async () => {
    vi.mocked(api.get).mockRejectedValueOnce(new Error("down"));
    const { user } = renderPage();
    expect(await screen.findByRole("alert")).toHaveTextContent("Daftar Tinjau gagal dimuat.");
    await user.click(screen.getByRole("button", { name: "Coba lagi" }));
    await loaded();
  });
});
