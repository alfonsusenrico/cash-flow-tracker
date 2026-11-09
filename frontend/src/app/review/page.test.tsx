import { readFileSync } from "node:fs";
import path from "node:path";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "@/components/workspace/overlays";
import { ShortcutProvider, SHORTCUTS_STORAGE_KEY } from "@/components/workspace/shortcuts";
import type { Language } from "@/i18n/language";
import { api } from "@/lib/api";
import { RegionProvider } from "@/lib/format";
import type { SuggestionList } from "@/lib/historySuggestions";
import { renderWithIntl } from "@/lib/i18n/testing";
import type { AwaitingItem, ReviewItem, ReviewQueue, SeenTwiceItem } from "@/lib/review";
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

/** The phone layout (below 768 px) is chosen by `matchMedia`, as in the browser. */
let viewport = 1440;
function installMatchMedia() {
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: (query: string) => {
      const max = Number(/max-width:\s*(\d+)px/.exec(query)?.[1] ?? Infinity);
      return { matches: viewport <= max, media: query, addEventListener: () => {}, removeEventListener: () => {} };
    },
  });
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

const seenTwice = (): SeenTwiceItem => ({
  event_id: "event-jago", kind: "duplicate", app: "Bank Jago", type: "income", amount: 500_000,
  date: "2026-10-31T11:39:18Z", account_name: "Bank Jago · Kantong Utama",
  kept: {
    transaction_id: "tx-gopay", type: "income", amount: 500_000, date: "2026-10-31T11:39:18Z", account_name: "GoPay",
    movement: { source: "BCA Harian", target: "GoPay" },
  },
});

let currentQueue: ReviewQueue;
let currentSuggestions: SuggestionList;
let categoriesFail = false;
let queueFailures = 0;

function renderPage(url = "/review", language: Language = "id") {
  window.history.replaceState(null, "", url);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = renderWithIntl(
    <QueryClientProvider client={queryClient}>
      <RegionProvider value={{ region: "ID", currency: "IDR", rate: 16_000 }}>
        <ShortcutProvider>
          <ToastProvider>
            <button type="button">Cari transaksi</button>
            <ReviewPage />
          </ToastProvider>
        </ShortcutProvider>
      </RegionProvider>
    </QueryClientProvider>,
    { language },
  );
  return { ...view, queryClient, user: userEvent.setup() };
}

const itemHeading = () => document.getElementById("triage-item-heading") as HTMLElement;
const pane = () => itemHeading().closest(".tj-detail") as HTMLElement;
const rail = () => screen.getByRole("navigation", { name: /^(Folder|Folders)$/ });
const railItem = (name: RegExp) => within(rail()).getByRole("button", { name });
const row = (name: string) => {
  const rows = [...document.querySelectorAll<HTMLElement>("[data-row]")];
  const found = rows.find((button) => button.textContent?.includes(name));
  if (!found) throw new Error(`No row ${name}`);
  return found;
};
const answer = (name: RegExp) => within(pane()).getByRole("button", { name });
const answerLabels = () =>
  within(pane()).getAllByRole("button").filter((button) => button.classList.contains("answer")).map((button) => button.querySelector(".t-strong")?.textContent);
const status = () => screen.getAllByRole("status").map((node) => node.textContent ?? "").join(" ");
const nextFrame = () => act(() => new Promise((resolve) => window.requestAnimationFrame(() => resolve(undefined))));

async function loaded() {
  await screen.findByRole("region", { name: /^(Tinjau|Review)$/ });
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

describe("Tinjau", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    installStorage();
    viewport = 1440;
    installMatchMedia();
    currentQueue = queue();
    currentSuggestions = emptySuggestions;
    categoriesFail = false;
    queueFailures = 0;
    ctx.hidden = false;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/review/queue") {
        if (queueFailures > 0) {
          queueFailures -= 1;
          throw new Error("down");
        }
        return currentQueue;
      }
      if (path === "/categories") {
        if (categoriesFail) throw new Error("categories down");
        return { categories: [
          { id: "category-food", name: "Makanan & Minuman", kind: "expense", kakeibo_type: "need" },
          { id: "category-snack", name: "Jajan & Nongkrong", kind: "expense", kakeibo_type: "want" },
          // Renamed by the owner: the key, not the name, keeps it out of the spending choices.
          { id: "category-internal", name: "Transfer", kind: "expense", system_key: "internal_movement" },
          { id: "category-salary", name: "Gaji", kind: "income" },
        ] };
      }
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
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/undo") return { ok: true, kind: "review_accept", already_undone: false, transaction_ids: [], removed_transaction_ids: [], restored: [] };
      if (String(path).includes("answer-purpose")) return { ok: true, answer: { status: "applied", notes: "belanja dapur" }, ...token("answer") };
      return { ok: true, ...token("action") };
    });
  });
  afterEach(() => vi.unstubAllGlobals());

  // --- Page, hero and actions --------------------------------------------------------------------

  it("Hero equals the badge, in DOM order hero → rail → list → item, with no axe violations", async () => {
    const { container } = renderPage();
    await loaded();
    const hero = screen.getByRole("region", { name: "Perlu ditinjau" });
    expect(within(hero).getByText("5")).toHaveClass("t-hero");
    expect(hero).toHaveTextContent("2 dicatat otomatis, opsional");
    expect(hero).not.toHaveTextContent("Rp");
    expect(hero.querySelector(".sw-bar")).toBeNull();
    const region = screen.getByRole("region", { name: "Tinjau" });
    const order = [hero, rail(), region.querySelector(".tj-list"), screen.getByRole("region", { name: "Detail" })];
    for (let index = 1; index < order.length; index += 1) {
      expect(order[index - 1]!.compareDocumentPosition(order[index]!) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    }
    expect(container.querySelectorAll(".t-hero")).toHaveLength(1);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Truncated queue", async () => {
    currentQueue = { ...queue(), truncated: true };
    renderPage();
    await loaded();
    expect(screen.getByRole("region", { name: "Perlu ditinjau" })).toHaveTextContent(
      "Hanya 1.000 transaksi terbaru yang ditampilkan. Selesaikan yang ada untuk melihat sisanya.",
    );
  });

  it("Open the suggestion dialog by keyboard, and the menu returns focus to its button", async () => {
    const { user } = renderPage();
    await loaded();
    const more = screen.getByRole("button", { name: "Tindakan lain untuk Tinjau" });
    more.focus();
    await user.keyboard("{Enter}");
    const menu = await screen.findByRole("menu");
    expect(within(menu).getAllByRole("menuitem").map((entry) => entry.textContent)).toEqual(["Cari saran dari riwayat", "Bantuan & pintasan"]);
    await waitFor(() => expect(within(menu).getByRole("menuitem", { name: "Cari saran dari riwayat" })).toHaveFocus());
    await user.keyboard("{ArrowDown}");
    expect(within(menu).getByRole("menuitem", { name: "Bantuan & pintasan" })).toHaveFocus();
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("menu")).not.toBeInTheDocument());
    expect(more).toHaveFocus();
    await user.keyboard("{Enter}");
    await screen.findByRole("menu");
    await waitFor(() => expect(screen.getByRole("menuitem", { name: "Cari saran dari riwayat" })).toHaveFocus());
    await user.keyboard("{Enter}");
    const dialog = await screen.findByRole("dialog", { name: "Cari saran dari riwayat" });
    expect(dialog.style.getPropertyValue("--dw")).toBe("520px");
    await waitFor(() => expect(dialog.contains(document.activeElement)).toBe(true));
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Cari saran dari riwayat" })).not.toBeInTheDocument());
    expect(more).toHaveFocus();
  });

  it("Nothing in the pane and No hint strip: key caps only on the numbered answers", async () => {
    renderPage();
    await loaded();
    const region = screen.getByRole("region", { name: "Tinjau" });
    expect(within(region).queryByText("Cari saran dari riwayat")).not.toBeInTheDocument();
    const caps = [...region.querySelectorAll("kbd")];
    expect(caps.map((cap) => cap.textContent)).toEqual(["1", "2"]);
    expect(caps.every((cap) => cap.closest(".answer"))).toBe(true);
  });

  // --- Folders and counts --------------------------------------------------------------------------

  it("Counts match the badge", async () => {
    renderPage();
    await loaded();
    const items = within(rail()).getAllByRole("button");
    expect(items.map((entry) => entry.querySelector(".lbl")?.textContent)).toEqual([
      "Semua", "Belum dikenali", "Perlu jawaban", "Pindah saldo belum lengkap", "Dicatat otomatis",
    ]);
    expect(items.map((entry) => entry.querySelector(".count")?.textContent)).toEqual(["5", "1", "2", "2", "2"]);
    expect(railItem(/^Semua/)).toHaveAccessibleName("Semua 5 item");
    expect(railItem(/^Semua/)).toHaveAttribute("aria-current", "true");
    expect(railItem(/^Semua/)).toHaveClass("ws-select-bar");
    expect(rail().querySelectorAll(".rail-sep")).toHaveLength(1);
    expect(within(screen.getByRole("list", { name: "Daftar Semua" })).getAllByRole("listitem")).toHaveLength(4);
  });

  it("Folder filter", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(railItem(/^Pindah saldo belum lengkap/));
    expect(railItem(/^Pindah saldo belum lengkap/)).toHaveAttribute("aria-current", "true");
    expect(railItem(/^Semua/)).not.toHaveAttribute("aria-current");
    const list = screen.getByRole("list", { name: "Daftar Pindah saldo belum lengkap" });
    expect(within(list).getAllByRole("listitem").map((entry) => entry.querySelector(".r-title")?.textContent)).toEqual([
      "Keluar dari GoPay", "Masuk ke BCA Harian",
    ]);
    expect(window.location.search).toBe("?item=tx-await&folder=pindah");
  });

  it("Empty folders hidden", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 2 }, questions: [], awaiting: [], ambiguous: [], groups: [queue().groups[0]] };
    renderPage();
    await loaded();
    expect(within(rail()).getAllByRole("button").map((entry) => entry.querySelector(".lbl")?.textContent)).toEqual(["Semua", "Perlu jawaban"]);
    expect(rail().querySelector(".rail-sep")).toBeNull();
  });

  it("Optional folder chosen, and Not counted as waiting", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [] };
    currentSuggestions = suggestionList();
    const { user } = renderPage();
    expect(await screen.findByText("Semua sudah ditinjau")).toBeInTheDocument();
    expect(within(screen.getByRole("region", { name: "Perlu ditinjau" })).getByText("0")).toBeInTheDocument();
    expect(railItem(/^Saran/).querySelector(".count")).toHaveTextContent("3");
    await user.click(railItem(/^Saran/));
    expect(railItem(/^Saran/)).toHaveAttribute("aria-current", "true");
    expect(screen.getByText("Usulan untuk transaksi lama. Tidak dihitung sebagai yang perlu dijawab.")).toBeInTheDocument();
    expect(itemHeading()).toHaveTextContent("Kafe Senja Contoh: jadikan Jajan & Nongkrong untuk 3 transaksi lama?");
  });

  it("Optional automatic rows", async () => {
    const { user } = renderPage();
    await loaded();
    expect(screen.getByRole("list", { name: "Daftar Semua" })).not.toHaveTextContent("Kedai Awan");
    const separator = rail().querySelector(".rail-sep")!;
    expect(separator.compareDocumentPosition(railItem(/^Dicatat otomatis/)) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    await user.click(railItem(/^Dicatat otomatis/));
    expect(screen.getByText("Dicatat dari aturan atau AI. Pengecekan opsional.")).toBeInTheDocument();
    expect(row("Kedai Awan")).toBeInTheDocument();
    expect(itemHeading()).toHaveTextContent("Kategori Kedai Awan sudah benar?");
    expect(answer(/Sudah benar/)).toHaveAttribute("data-primary-choice");
    expect(pane()).toHaveTextContent("Opsional. Transaksi ini sudah tercatat dan tetap tersimpan meskipun dilewati.");
  });

  it("Automatic rows inside a mixed group", async () => {
    const data = queue();
    data.groups[0] = { ...data.groups[0], count: 4, auto_count: 2, transactions: [...data.groups[0].transactions, item("tx-e", { merchant: "Toko Serba Ada" }), item("tx-f", { merchant: "Toko Serba Ada" })] };
    currentQueue = data;
    const { user } = renderPage();
    await loaded();
    await user.click(railItem(/^Dicatat otomatis/));
    expect(screen.getByText(/2 lainnya ikut kelompok di Perlu jawaban\./)).toBeInTheDocument();
  });

  // --- List rows -------------------------------------------------------------------------------------

  it("Question Row Direction, the meta line in Semua and elsewhere, and the amount variants", async () => {
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
    const { user } = renderPage();
    await loaded();
    const income = row("Uang masuk dari GoPay");
    expect(income.querySelector(".r-meta")).toHaveTextContent("Belum dikenali · 21 Okt 19.00");
    expect(within(income).getByText("+Rp 10.000")).toHaveClass("c-income");
    expect(income).not.toHaveTextContent("−");
    expect(within(row("Pengeluaran Groceries")).getByText("−Rp 187.500")).toHaveClass("c-expense");
    expect(within(row("Keluar dari GoPay")).getByText("Rp 500.000")).toHaveClass("c-transfer");
    expect(row("Keluar dari GoPay")).not.toHaveTextContent("⇄");
    await user.click(income);
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Uang masuk · +Rp 10.000 · 21 Okt 19.00 · Bank Jago · Kantong Utama");
    expect(pane()).not.toHaveTextContent("Pembayaran tanpa nama toko");
    await user.click(railItem(/^Belum dikenali/));
    expect(row("Uang masuk dari GoPay").querySelector(".r-meta")?.textContent).toBe("21 Okt 19.00");
  });

  it("Expense question meta, the selected row, and the notification text box", async () => {
    renderPage();
    await loaded();
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Pembayaran tanpa nama toko · −Rp 187.500 · 21 Okt 18.00 · BCA Harian");
    const selected = row("Pengeluaran Groceries");
    expect(selected).toHaveAttribute("aria-current", "true");
    expect(selected).toHaveAttribute("data-selected");
    expect(selected).toHaveClass("row", "is-stacked", "ws-select-row");
    const raw = pane().querySelector(".raw")!;
    expect(raw).toHaveTextContent("Teks notifikasi: Pengeluaran Groceries");
    expect(raw.querySelector(".sr-only")).toHaveTextContent("Teks notifikasi:");
    expect(raw).not.toHaveAttribute("lang");
  });

  it("Hidden balances", async () => {
    ctx.hidden = true;
    renderPage();
    await loaded();
    expect(itemHeading()).toHaveTextContent("Apa ini? Dari BCA Harian");
    expect(document.body).not.toHaveTextContent("187.500");
    expect(within(pane()).getAllByRole("img", { name: "Disembunyikan" }).length).toBeGreaterThan(0);
    expect(within(row("Toko Serba Ada")).getByRole("img", { name: "Disembunyikan" })).toHaveTextContent("Rp ••••••");
  });

  it("‹ › move within the folder, are disabled at the ends and name their keys", async () => {
    const { user } = renderPage();
    await loaded();
    const previous = within(pane()).getByRole("button", { name: "Sebelumnya" });
    const next = within(pane()).getByRole("button", { name: "Berikutnya" });
    expect(previous).toBeDisabled();
    expect(previous).toHaveAttribute("aria-keyshortcuts", "K");
    expect(next).toHaveAttribute("aria-keyshortcuts", "J");
    expect(within(pane()).getByText("1 dari 4")).toBeInTheDocument();
    expect(within(pane()).getByText("Belum dikenali", { selector: "p.t-label" })).toBeInTheDocument();
    await user.click(next);
    expect(itemHeading()).toHaveTextContent("Kategori untuk Toko Serba Ada?");
    expect(within(pane()).getByText("2 dari 4")).toBeInTheDocument();
  });

  // --- Each kind -----------------------------------------------------------------------------------

  it("Belum dikenali: numbered suggestions with Saran first, and the habit guess with its basis", async () => {
    const base = queue();
    const [entry] = base.questions;
    currentQueue = { ...base, questions: [{ ...entry, question: { ...entry.question, suggestions: [
      { id: "s1", kind: "habit", label: "Sepertinya Makanan & Minuman?", category_name: "Makanan & Minuman", kakeibo: "need", basis: { agreeing: 3, support: 4 } },
      ...entry.question.suggestions.map((suggestion, index) => ({ ...suggestion, id: `s${index + 2}` })),
    ] } }] };
    const { container } = renderPage();
    await loaded();
    const first = answer(/^Sepertinya Makanan & Minuman\?/);
    expect(first).toHaveTextContent("Saran · Makanan & Minuman · Kebutuhan · 3 dari 4 jawaban sebelumnya");
    expect(first).toHaveAttribute("data-primary-choice");
    expect(first).toHaveAttribute("aria-keyshortcuts", "1 E");
    expect(answer(/^belanja dapur/)).toHaveTextContent("Belanja · Kebutuhan");
    expect(answer(/^belanja dapur/)).not.toHaveTextContent("Saran");
    expect(within(pane()).getByRole("group", { name: entry.question.prompt })).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Perlu jawaban: the question, meta, three answers and the rows as links", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    expect(within(pane()).getByText("Perlu jawaban", { selector: "p.t-label" })).toBeInTheDocument();
    expect(itemHeading()).toHaveTextContent("Kategori untuk Toko Serba Ada?");
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Toko · −Rp 400.000 · 2 transaksi · 19 Okt – 20 Okt 2026");
    expect(pane()).toHaveTextContent("Keyakinan AI rendah");
    expect(answerLabels()).toEqual(["Terima semua (2)", "Ubah kategori", "Ganti nama"]);
    expect(within(pane()).getByText("Transaksi · 2")).toBeInTheDocument();
    const links = within(screen.getByRole("list", { name: "Transaksi Toko Serba Ada" })).getAllByRole("link");
    expect(links[0]).toHaveAttribute("href", "/ledger?edit=tx-c&return=review&folder=jawab");
    expect(links[0]).toHaveTextContent("20 Okt 12.00 · BCA Harian · Makanan & Minuman · Kebutuhan · perlu ditinjau");
  });

  it("Pindah saldo belum lengkap: the accounts, top-up and spending answers, and the warning", async () => {
    const { user, container } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    expect(within(pane()).getByText("Pindah saldo belum lengkap", { selector: "p.t-label" })).toBeInTheDocument();
    expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay. Masuknya ke mana?");
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Pasangan belum ditemukan · Rp 500.000 · 20 Okt 12.00 · GoPay");
    expect(within(pane()).getByText("Selama belum lengkap, ini belum dihitung sebagai pengeluaran.")).toHaveClass("c-warning");
    expect(answer(/^BCA Harian/)).toHaveTextContent("Pindah saldo ke sini");
    expect(answer(/Ini top up investasi/)).toHaveAttribute("data-topup");
    expect(answer(/Ini top up investasi/).getAttribute("aria-keyshortcuts")).toMatch(/^\d T$/);
    expect(within(pane()).getByRole("link", { name: "Ubah di Transaksi" })).toHaveAttribute("href", "/ledger?edit=tx-await&return=review&folder=pindah");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Complete an awaiting transfer, and resolve an ambiguous one", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    await user.click(answer(/^BCA Harian/));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", { transaction_id: "tx-await", target_account_id: "account-bca" }));
    await user.click(row("Masuk ke BCA Harian"));
    expect(itemHeading()).toHaveTextContent("Masuk ke BCA Harian. Asalnya yang mana?");
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("2 kemungkinan pasangan");
    expect(answer(/^OVO/)).toHaveTextContent("20 Okt 11.58 · Rp 500.000 · gabungkan sebagai pindah saldo");
    await user.click(answer(/^OVO/));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", { expense_transaction_id: "tx-send-2", income_transaction_id: "tx-ambiguous" }));
    await user.click(row("Masuk ke BCA Harian"));
    await user.click(answer(/Biarkan terpisah/));
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
    const { user } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    const choice = await within(pane()).findByRole("button", { name: /SPayLater/ });
    expect(choice).toHaveTextContent("Kredit · bayar tagihan ke sini");
    await user.click(choice);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", { transaction_id: "tx-await", target_account_id: "account-spaylater" }));
  });

  it("offers the existing leg instead of recording a transfer twice, and records new only when confirmed", async () => {
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
    await user.click(answer(/^BCA Harian/));
    const existing = await within(pane()).findByRole("group", { name: "Sudah ada di rekening ini" });
    expect(within(existing).getByRole("button", { name: /Hubungkan: Rp 500.000/ })).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
    act(() => itemHeading().focus());
    await user.keyboard("1");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", { expense_transaction_id: "tx-await", income_transaction_id: "tx-existing-in" }));
    expect(api.post).not.toHaveBeenCalledWith("/movements/complete", expect.objectContaining({ create_new: true }));
    await user.click(row("Keluar dari GoPay"));
    await user.click(answer(/^BCA Harian/));
    await user.click(await within(pane()).findByRole("button", { name: /Tetap catat baru/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", { transaction_id: "tx-await", target_account_id: "account-bca", create_new: true }));
  });

  it("Awaiting leg as spending: the dialog sends one recategorise", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Keluar dari GoPay"));
    await user.click(answer(/Bukan pindah saldo, ini pengeluaran/));
    const dialog = await screen.findByRole("dialog", { name: "Catat sebagai pengeluaran" });
    expect(within(within(dialog).getByRole("combobox", { name: "Kategori" })).queryByRole("option", { name: "Transfer" })).not.toBeInTheDocument();
    expect(dialog).toHaveTextContent("Setelah disimpan, ini dihitung sebagai pengeluaran dan tidak dipasangkan lagi.");
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
    expect(within(dialog).getByText("Pilih kategori pengeluarannya.")).toBeInTheDocument();
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Kategori" }), "category-food");
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
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

    it("settles a debit as a top-up and explains unit-tracked funds", async () => {
      currentQueue = { ...queue(), awaiting: [rdnDebit()] };
      const { user, container } = renderPage();
      await loaded();
      await user.click(row("Keluar dari RDN Contoh"));
      const openTopup = answer(/Ini top up investasi/);
      expect(openTopup).toHaveAttribute("aria-expanded", "false");
      await user.click(openTopup);
      const picker = within(pane()).getByRole("group", { name: "Ini top up investasi. Ke produk mana?" });
      expect(within(picker).getAllByRole("button").map((button) => button.textContent)).toEqual([
        "1Bibit Contoh · Reksa Dana ADicatat per nominal", "2Bibit Contoh · Reksa Dana BDicatat per nominal",
      ]);
      expect(pane()).toHaveTextContent("Bibit Contoh · Reksa Dana Unit masih dicatat per unit");
      expect(within(pane()).getByRole("link", { name: "Rekening" })).toHaveAttribute("href", "/accounts");
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

    it("accepts by T, corrects or rejects a learned suggestion", async () => {
      currentQueue = { ...queue(), awaiting: [rdnDebit({
        target_account_id: "fund-a", target_label: productA.label, choices: [productA],
        prompt: "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?",
        explanation: "Dari 1 dari 1 jawaban sebelumnya untuk debit Rp112.590 dari RDN Contoh.",
        basis: { agreeing: 1, support: 1, agreement: 1 },
      })] };
      const { user, container } = renderPage();
      await loaded();
      await user.click(row("Keluar dari RDN Contoh"));
      expect(pane().querySelector(".tj-meta")).toHaveTextContent(/^Top up investasi · Rp 112\.590/);
      expect((await axe(container)).violations).toHaveLength(0);
      const yes = answer(/^Ya, top up Reksa Dana A/);
      expect(yes).toHaveTextContent("Dari 1 dari 1 jawaban sebelumnya untuk debit Rp112.590 dari RDN Contoh.");
      expect(yes).toHaveAttribute("aria-keyshortcuts", "1 E T");
      expect(answer(/^Bukan top up/)).toHaveTextContent("Diingat untuk debit seperti ini");
      act(() => itemHeading().focus());
      await user.keyboard("t");
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", {
        transaction_id: "tx-rdn", target_account_id: "fund-a", suggested_target_account_id: "fund-a",
      }));
      await user.click(row("Keluar dari RDN Contoh"));
      await user.click(answer(/Pilih lain/));
      await user.click(answer(/Ini top up investasi/));
      const picker = within(pane()).getByRole("group", { name: "Ini top up investasi. Ke produk mana?" });
      expect(within(picker).queryByRole("button", { name: /Reksa Dana A/ })).not.toBeInTheDocument();
      await user.click(within(picker).getByRole("button", { name: /Reksa Dana B/ }));
      await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/investment-topups/from-transaction", {
        transaction_id: "tx-rdn", target_account_id: "fund-b", suggested_target_account_id: "fund-a",
      }));
      await user.click(row("Keluar dari RDN Contoh"));
      await user.click(answer(/^Bukan top up/));
      await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/topup-suggestions/reject", {
        transaction_id: "tx-rdn", suggested_target_account_id: "fund-a",
      }));
    });

    it("asks without a default when the product cannot be assigned", async () => {
      currentQueue = { ...queue(), awaiting: [rdnDebit({
        target_account_id: null, target_label: null, choices: [productA, productB],
        prompt: "Debit Rp112.590 dari RDN Contoh biasanya jadi top up ke Reksa Dana A atau Reksa Dana B. Yang mana?",
        explanation: "Belum bisa dipastikan produknya.", basis: null,
      })] };
      const { user } = renderPage();
      await loaded();
      await user.click(row("Keluar dari RDN Contoh"));
      expect(itemHeading()).toHaveTextContent("Yang mana?");
      expect(within(pane()).getAllByRole("button").filter((button) => button.hasAttribute("data-primary-choice"))).toHaveLength(0);
      await user.click(answer(/Bibit Contoh · Reksa Dana B/));
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/investment-topups/from-transaction", { transaction_id: "tx-rdn", target_account_id: "fund-b" }));
    });
  });

  it("Saran dari riwayat: decided as an item, a row on its own, and failures reported truthfully", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [] };
    currentSuggestions = suggestionList();
    vi.mocked(api.post)
      .mockResolvedValueOnce({ ok: true, rejected: 1, skipped: [], ...token("one") })
      .mockResolvedValueOnce({ ok: true, accepted: 2, skipped: ["sg-c"], corrections: 2, ...token("sugg") })
      .mockRejectedValueOnce(Object.assign(new Error("x"), { status: 404, detail: { code: "suggestions_not_found", message: "Sebagian saran tidak ditemukan." } }));
    const { user, container } = renderPage();
    await screen.findByText("Semua sudah ditinjau");
    await user.click(railItem(/^Saran/));
    expect(within(pane()).getByText("Saran dari riwayat", { selector: "p.t-label" })).toBeInTheDocument();
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Saran dari riwayat · −Rp 105.000 · 3 transaksi · 28 Okt – 31 Okt 2026");
    expect(pane()).toHaveTextContent("Opsional. Tidak ada yang berubah sebelum diterima.");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(within(pane()).getAllByRole("button", { name: /^Tolak saran untuk/ })[0]);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/suggestions/reject", { suggestion_ids: ["sg-a"] }));
    await user.click(row("Kafe Senja Contoh"));
    await user.click(answer(/Terima saran \(3\)/));
    await waitFor(() => expect(status()).toContain("Kafe Senja Contoh, 2 saran diterima; 1 dilewati"));
    await user.click(row("Kafe Senja Contoh"));
    await user.click(answer(/^Tolak\s*Saran ini tidak/));
    expect(await within(pane()).findByRole("alert")).toHaveTextContent("Sebagian saran tidak ditemukan.");
    expect(status()).not.toContain("ditolak");
  });

  it("The pair on Tinjau: confirm one transaction, or record it on its own with Urungkan", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 1, seen_twice: 1 }, questions: [], awaiting: [], ambiguous: [], groups: [], seen_twice: [seenTwice()] };
    const { user, container } = renderPage();
    await screen.findByText("Semua sudah ditinjau");
    await user.click(railItem(/^Terlihat dua kali/));
    expect(itemHeading()).toHaveTextContent("Bank Jago melaporkan uang yang sudah tercatat. Satu transaksi yang sama?");
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Kemungkinan tercatat dua kali · +Rp 500.000 · 31 Okt 18.39 · Bank Jago · Kantong Utama");
    const recorded = within(pane()).getByRole("link", { name: /Sudah tercatat · BCA Harian → GoPay/ });
    expect(recorded).toHaveAttribute("href", "/ledger?edit=tx-gopay&return=review&folder=dua");
    expect(within(pane()).getByText("Laporan Bank Jago").closest(".row")).toHaveClass("is-plain");
    expect(pane()).toHaveTextContent("GoPay juga terlihat di Bank Jago");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(answer(/Catat terpisah/));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/seen-twice/event-jago/record-separately", {}));
    await waitFor(() => expect(status()).toMatch(/dicatat sebagai transaksi terpisah/));
    await user.click(within(screen.getAllByRole("status").find((node) => within(node).queryByRole("button", { name: "Urungkan" }))!).getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/undo", { undo_token: "action".padEnd(43, "x") }));
  });

  it("Terlihat dua kali: Benar, satu transaksi links the account, and Batalkan stops the link", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 1, seen_twice: 1 }, questions: [], awaiting: [], ambiguous: [], groups: [], seen_twice: [seenTwice()] };
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (String(path).endsWith("/acknowledge")) {
        return { ok: true, event_id: "event-jago", link: { id: "link-1", account_id: "acc-gopay", account_label: "GoPay", institution_label: "Bank Jago", created: true } };
      }
      return { ok: true, ...token("action") };
    });
    vi.mocked(api.del).mockResolvedValue({ ok: true });
    const { user } = renderPage();
    await screen.findByText("Semua sudah ditinjau");
    await user.click(railItem(/^Terlihat dua kali/));
    expect(answer(/Benar, satu transaksi/)).toHaveTextContent("Rekening ditautkan; laporan serupa dilewati otomatis");
    await user.click(answer(/Benar, satu transaksi/));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/seen-twice/event-jago/acknowledge", {}));
    const toast = await screen.findByText("GoPay juga dilaporkan Bank Jago. Laporan serupa dilewati otomatis.");
    await user.click(within(toast.closest(".ws-toast") as HTMLElement).getByRole("button", { name: "Batalkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/ingest/account-links/link-1"));
    expect(await screen.findByText("Tautan dibatalkan. Laporan Bank Jago serupa akan ditanyakan lagi.")).toBeInTheDocument();
  });

  it("Dilewati otomatis: a skipped report shows its account link and can be recorded on its own", async () => {
    currentQueue = {
      ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 0, seen_twice: 0, skipped: 1 },
      questions: [], awaiting: [], ambiguous: [], groups: [],
      skipped: [{ ...seenTwice(), event_id: "event-skip", kind: "skipped", link: { id: "link-1", account_label: "GoPay", institution_label: "Bank Jago" } }],
    };
    const { user, container } = renderPage();
    await screen.findByText("Semua sudah ditinjau");
    expect(railItem(/^Dilewati otomatis/).querySelector(".count")).toHaveTextContent("1");
    await user.click(railItem(/^Dilewati otomatis/));
    expect(screen.getByText("Laporan rekening yang juga dilaporkan aplikasi lain, sudah tercatat sekali. 30 hari terakhir.")).toBeInTheDocument();
    expect(row("Bank Jago: dilewati").querySelector(".r-ico path")?.getAttribute("d")).toBe("M3 4h18v4H3zM5 8v12h14V8M10 12h4");
    expect(itemHeading()).toHaveTextContent("Laporan Bank Jago dilewati karena sudah tercatat.");
    expect(pane().querySelector(".tj-meta")?.textContent).toMatch(/^Dilewati, tidak dicatat/);
    expect(pane()).toHaveTextContent("GoPay juga dilaporkan Bank Jago");
    expect(answerLabels()).toEqual(["Catat terpisah"]);
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(answer(/Catat terpisah/));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/seen-twice/event-skip/record-separately", {}));
  });

  // --- Answers, auto-advance and Urungkan -------------------------------------------------------------

  it("Keyboard group accept, Next item and Urungkan", async () => {
    vi.mocked(api.post).mockImplementation(async (path) => {
      if (path === "/review/bulk") removeFromQueue(["tx-c"]);
      if (path === "/review/undo") currentQueue = queue();
      return { ok: true, ...token("bulk") };
    });
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    answer(/Terima semua \(2\)/).focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: ["tx-c", "tx-d"] }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay. Masuknya ke mana?"));
    await waitFor(() => expect(itemHeading()).toHaveFocus());
    expect(status()).toContain("Tersimpan: Toko Serba Ada, 2 transaksi ditandai sudah ditinjau. Berikutnya: Keluar dari GoPay.");
    await user.click(screen.getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/undo", { undo_token: expect.stringMatching(/^bulk/) }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Kategori untuk Toko Serba Ada?"));
    expect(status()).toContain("Dibatalkan. Toko Serba Ada kembali ke Perlu jawaban.");
  });

  it("Answer by text, Empty answer and Typing an answer", async () => {
    const { user } = renderPage();
    await loaded();
    const opener = within(pane()).getByRole("button", { name: "Jawab sendiri" });
    expect(opener).toHaveClass("ws-btn-quiet");
    await user.click(opener);
    const dialog = await screen.findByRole("dialog", { name: "Jawab sendiri" });
    expect(dialog.style.getPropertyValue("--dw")).toBe("520px");
    expect(dialog).toHaveTextContent("Apa ini? Rp187.500 dari BCA Harian");
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
    expect(api.post).not.toHaveBeenCalled();
    const field = within(dialog).getByRole("textbox", { name: "Jawaban" });
    expect(field).toHaveAttribute("aria-invalid", "true");
    expect(field).toHaveAttribute("maxlength", "200");
    expect(within(dialog).getByText('Tulis jawaban singkat, misalnya "makan siang di kantin".')).toBeInTheDocument();
    await user.type(field, "1 kopi");
    expect(api.post).not.toHaveBeenCalled();
    await user.clear(field);
    await user.type(field, "sewa lapangan futsal{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/ingest/notifications/event-1/answer-purpose", expect.objectContaining({
      question_id: "question-1", text: "sewa lapangan futsal", reply_id: expect.any(String),
    })));
  });

  it("Recategorise a group, Rename without input, and the rename request", async () => {
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(answer(/Ubah kategori/));
    let dialog = await screen.findByRole("dialog", { name: "Ubah kategori" });
    expect(dialog.style.getPropertyValue("--dw")).toBe("560px");
    expect(dialog).toHaveTextContent("Toko Serba Ada · 2 transaksi");
    expect(dialog).toHaveTextContent("Diingat untuk transaksi berikutnya dari Toko Serba Ada.");
    const category = within(dialog).getByRole("combobox", { name: "Kategori" });
    expect(within(category).queryByRole("option", { name: "Gaji" })).not.toBeInTheDocument();
    const pillars = within(dialog).getByRole("group", { name: "Pilar Kakeibo" });
    expect(within(pillars).getAllByRole("button")[0]).toHaveTextContent("Ikuti kategori");
    expect(within(pillars).getByRole("button", { name: "Ikuti kategori" })).toHaveAttribute("aria-pressed", "true");
    await user.selectOptions(category, "category-food");
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", {
      action: "recategorise", transaction_ids: ["tx-c", "tx-d"], category_id: "category-food",
    }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Ubah kategori" })).not.toBeInTheDocument());
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Kategori untuk Toko Serba Ada?"));
    await user.click(answer(/Ganti nama/));
    dialog = await screen.findByRole("dialog", { name: "Ganti nama" });
    const merchant = within(dialog).getByRole("textbox", { name: "Nama toko" });
    expect(merchant).toHaveValue("Toko Serba Ada");
    await user.clear(merchant);
    vi.mocked(api.post).mockClear();
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
    expect(api.post).not.toHaveBeenCalled();
    expect(within(dialog).getByText("Isi nama toko atau catatan.")).toBeInTheDocument();
    await user.type(merchant, "Toko Serba Ada Baru");
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/bulk", {
      action: "rename", transaction_ids: ["tx-c", "tx-d"], merchant: "Toko Serba Ada Baru",
    }));
  });

  it("Focus return and Failed save in the category dialog", async () => {
    vi.mocked(api.post).mockRejectedValueOnce(Object.assign(new Error("x"), {
      status: 409, detail: { code: "movement_leg_not_editable", message: "Pindah saldo diubah lewat detail pindah saldo." },
    }));
    const { user, container } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    const opener = answer(/Ubah kategori/);
    await user.click(opener);
    await screen.findByRole("dialog", { name: "Ubah kategori" });
    expect((await axe(container)).violations).toHaveLength(0);
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Ubah kategori" })).not.toBeInTheDocument());
    expect(opener).toHaveFocus();
    expect(api.post).not.toHaveBeenCalled();
    await user.click(opener);
    const dialog = await screen.findByRole("dialog", { name: "Ubah kategori" });
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Kategori" }), "category-snack");
    await user.click(within(dialog).getByRole("button", { name: "Simpan jawaban" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent("Pindah saldo diubah lewat detail pindah saldo.");
    expect(screen.getByRole("dialog", { name: "Ubah kategori" })).toBeInTheDocument();
    expect(status()).not.toContain("Tersimpan");
  });

  it("names a categories failure with Coba lagi instead of an empty list", async () => {
    categoriesFail = true;
    const { user } = renderPage();
    await loaded();
    await user.click(row("Toko Serba Ada"));
    await user.click(answer(/Ubah kategori/));
    const dialog = await screen.findByRole("dialog", { name: "Ubah kategori" });
    expect(dialog).toHaveTextContent("Daftar kategori gagal dimuat.");
    expect(within(dialog).getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
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
    expect(within(pane()).getByRole("button", { name: "Lihat semua 250" })).toBeInTheDocument();
    await user.click(answer(/Terima semua \(250\)/));
    expect(await within(pane()).findByRole("alert")).toHaveTextContent("200 dari 250 transaksi tersimpan; sisanya gagal: Server sibuk.");

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
    await user.click(answer(/Terima semua \(250\)/));
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
    await user.click(answer(/Terima semua/));
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
    await user.click(answer(/Terima semua/));
    await waitFor(() => expect(status()).toContain("Tersimpan"));
    expect(screen.queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
    act(() => itemHeading().focus());
    await user.keyboard("z");
    expect(await screen.findByText("Tidak ada yang dapat dibatalkan.")).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalledWith("/review/undo", expect.anything());
    vi.mocked(api.post).mockResolvedValue({ ok: true, ...token("secret") });
    await user.click(answer(/Terima semua/));
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
    await user.click(answer(/Terima semua/));
    const undoButton = await screen.findByRole("button", { name: "Urungkan" });
    await waitFor(() => expect(undoButton).not.toBeInTheDocument(), { timeout: 8000 });
    act(() => itemHeading().focus());
    await user.keyboard("z");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/undo", { undo_token: expect.any(String) }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Kategori untuk Toko Serba Ada?"));
    await waitFor(() => expect(itemHeading()).toHaveFocus());
  }, 15000);

  // --- Keys --------------------------------------------------------------------------------------------

  it("Walk and accept", async () => {
    currentQueue = { ...queue(), questions: [], awaiting: [], ambiguous: [], groups: [
      ...["Satu", "Dua", "Tiga"].map((name, index) => ({ ...queue().groups[0], key: `g${index}`, label: `Toko ${name}`, transactions: [item(`tx-${name}`, { merchant: `Toko ${name}`, review_status: "needs_review" })] })),
    ] };
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      if (path === "/review/bulk") removeFromQueue((body as { transaction_ids: string[] }).transaction_ids);
      return { ok: true, ...token("walk") };
    });
    const { user } = renderPage("/review?folder=jawab");
    const list = await screen.findByRole("list", { name: "Daftar Perlu jawaban" });
    within(list).getAllByRole("button")[0].focus();
    await user.keyboard("j");
    await waitFor(() => expect(within(list).getAllByRole("button")[1]).toHaveFocus());
    expect(itemHeading()).toHaveTextContent("Toko Dua");
    await user.keyboard("e");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: ["tx-Dua"] }));
    await waitFor(() => expect(itemHeading()).toHaveTextContent("Toko Tiga"));
  });

  it("Outside the region, and no answer pressed from inside a dialog over the item", async () => {
    const { user } = renderPage();
    await loaded();
    await nextFrame();
    screen.getByRole("button", { name: "Cari transaksi" }).focus();
    await user.keyboard("j");
    expect(itemHeading()).toHaveTextContent("Apa ini?");
    await user.click(within(pane()).getByRole("button", { name: "Jawab sendiri" }));
    const dialog = await screen.findByRole("dialog", { name: "Jawab sendiri" });
    within(dialog).getByRole("button", { name: "Batal" }).focus();
    await user.keyboard("1");
    expect(api.post).not.toHaveBeenCalled();
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

  it("presses numbered answers, T and E inside the item", async () => {
    currentQueue = { ...queue(), questions: [] };
    const { user } = renderPage();
    await loaded();
    act(() => itemHeading().focus());
    await user.keyboard("2");
    expect(await screen.findByRole("dialog", { name: "Ubah kategori" })).toBeInTheDocument();
    await user.keyboard("{Escape}");
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
    await user.keyboard("8");
    expect(await screen.findByText("Tidak ada pilihan nomor 8.")).toBeInTheDocument();
  });

  it("Help lists the page keys, by ? and from the menu", async () => {
    const { user } = renderPage();
    await loaded();
    act(() => itemHeading().focus());
    await user.keyboard("?");
    let help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    for (const key of ["J / K", "Enter", "E", "T", "1–9", "Z"]) {
      const cap = within(help).getByText(key, { selector: "kbd" });
      expect(cap.closest("tr")).toHaveTextContent(/di Tinjau$/);
      expect(cap.closest("tr")?.querySelector("[lang]")).toBeNull();
    }
    expect((await axe(help)).violations).toHaveLength(0);
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Bantuan & pintasan" })).not.toBeInTheDocument());
    await user.click(screen.getByRole("button", { name: "Tindakan lain untuk Tinjau" }));
    await user.click(await screen.findByRole("menuitem", { name: "Bantuan & pintasan" }));
    help = await screen.findByRole("dialog", { name: "Bantuan & pintasan" });
    expect(within(help).getByText("1–9", { selector: "kbd" })).toBeInTheDocument();
  });

  // --- States and links ---------------------------------------------------------------------------------

  it("Loading: busy, without a hero, a count or an amount", async () => {
    vi.mocked(api.get).mockImplementation(() => new Promise(() => {}));
    renderPage();
    const region = await screen.findByRole("region", { name: "Memuat Tinjau" });
    expect(region).toHaveAttribute("aria-busy", "true");
    expect(region.querySelectorAll(".skel").length).toBeGreaterThan(6);
    expect(screen.queryByRole("region", { name: "Perlu ditinjau" })).not.toBeInTheDocument();
    expect(region).not.toHaveTextContent(/\d|Rp/);
  });

  it("Failure and retry", async () => {
    queueFailures = 1;
    const { user } = renderPage();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Daftar Tinjau gagal dimuat.");
    expect(screen.queryByRole("region", { name: "Perlu ditinjau" })).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("Semua sudah ditinjau");
    await user.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    await loaded();
    expect(screen.getByRole("region", { name: "Perlu ditinjau" })).toBeInTheDocument();
  });

  it("Nothing waiting and Summary unavailable: no cycle figure, Cek yang dicatat otomatis, and no summary request", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [queue().groups[1]] };
    const { container, user } = renderPage();
    expect(await screen.findByText("Semua sudah ditinjau")).toBeInTheDocument();
    expect(screen.getByText("Tidak ada yang perlu dijawab.")).toBeInTheDocument();
    expect(within(screen.getByRole("region", { name: "Perlu ditinjau" })).getByText("0")).toBeInTheDocument();
    for (const figure of ["Pengeluaran", "Aman dipakai", "Belum diklasifikasi", "Kembali ke Beranda", "Rp 0"]) expect(document.body).not.toHaveTextContent(figure);
    expect(api.get).not.toHaveBeenCalledWith("/dashboard/cycle-summary");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Cek yang dicatat otomatis" }));
    expect(itemHeading()).toHaveTextContent("Kategori Kedai Awan sudah benar?");
    expect(railItem(/^Dicatat otomatis/)).toHaveAttribute("aria-current", "true");
  });

  it("Nothing at all: the empty state only, without the hero, the rail or a button", async () => {
    currentQueue = { ...queue(), counts: { ...queue().counts, attention: 0, unreviewed: 0 }, questions: [], awaiting: [], ambiguous: [], groups: [] };
    renderPage();
    expect(await screen.findByText("Semua sudah ditinjau")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Perlu ditinjau" })).not.toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: "Folder" })).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Tinjau" }).querySelectorAll("button")).toHaveLength(0);
    expect(document.body).not.toHaveTextContent("Rp 0");
  });

  it("A failed suggestions request leaves the queue visible without Saran", async () => {
    const getImpl = vi.mocked(api.get).getMockImplementation()!;
    vi.mocked(api.get).mockImplementation(async (path, signal) => {
      if (path === "/review/suggestions") throw new Error("down");
      return getImpl(path, signal);
    });
    renderPage();
    await loaded();
    expect(within(rail()).queryByRole("button", { name: /^Saran/ })).not.toBeInTheDocument();
    expect(railItem(/^Semua/).querySelector(".count")).toHaveTextContent("5");
  });

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

  it("Link from Perlu tindakan, with the legacy hash dropped", async () => {
    renderPage("/review?item=tx-await#review-awaiting");
    await loaded();
    expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay");
    expect(railItem(/^Pindah saldo belum lengkap/)).toHaveAttribute("aria-current", "true");
    await waitFor(() => expect(itemHeading()).toHaveFocus());
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
    expect(railItem(/^Belum dikenali/)).toHaveAttribute("aria-current", "true");
    expect(itemHeading()).toHaveTextContent("Apa ini? Rp187.500");
    expect(screen.getByText("Item yang dibuka sudah selesai.")).toBeInTheDocument();
  });

  // --- Phone ---------------------------------------------------------------------------------------------

  describe("at 390 px", () => {
    beforeEach(() => {
      viewport = 390;
    });

    it("Phone folders and Back to the list, with focus on the item's row", async () => {
      const { user } = renderPage();
      await screen.findByRole("list", { name: "Daftar Semua" });
      expect(rail()).toHaveClass("rail", "is-chips");
      expect(document.getElementById("triage-item-heading")).toBeNull();
      // Nothing is marked current until an item opens, as in the prototype's phone list.
      expect(document.querySelector("[data-row][aria-current]")).toBeNull();
      const length = window.history.length;
      await user.click(row("Toko Serba Ada"));
      const sheet = await screen.findByRole("dialog", { name: "Perlu jawaban" });
      expect(sheet).toHaveClass("is-full", "tj-sheet");
      expect(window.history.length).toBe(length + 1);
      await waitFor(() => expect(itemHeading()).toHaveFocus());
      expect(within(sheet).getByRole("button", { name: "Kembali ke daftar" })).toBeInTheDocument();
      expect(within(sheet).queryByText("2 dari 4")).not.toBeInTheDocument();
      act(() => {
        window.history.replaceState(null, "", "/review");
        window.dispatchEvent(new PopStateEvent("popstate"));
      });
      await waitFor(() => expect(screen.queryByRole("dialog", { name: "Perlu jawaban" })).not.toBeInTheDocument());
      await waitFor(() => expect(row("Toko Serba Ada")).toHaveFocus());
      expect(window.location.pathname).toBe("/review");
    });

    it("Answer inside the sheet, and the keys work in it", async () => {
      vi.mocked(api.post).mockImplementation(async (path) => {
        if (path === "/review/bulk") removeFromQueue(["tx-c"]);
        return { ok: true, ...token("bulk") };
      });
      const { user } = renderPage();
      await screen.findByRole("list", { name: "Daftar Semua" });
      await user.click(row("Toko Serba Ada"));
      await waitFor(() => expect(itemHeading()).toHaveFocus());
      await user.keyboard("1");
      await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", { action: "accept", transaction_ids: ["tx-c", "tx-d"] }));
      await waitFor(() => expect(itemHeading()).toHaveTextContent("Uang keluar dari GoPay"));
      await waitFor(() => expect(itemHeading()).toHaveFocus());
      expect(screen.getByRole("dialog", { name: "Pindah saldo belum lengkap" })).toBeInTheDocument();
      await user.keyboard("j");
      await waitFor(() => expect(itemHeading()).toHaveTextContent("Masuk ke BCA Harian"));
    });

    it("a deep link opens the sheet with the heading focused, and the close button returns to the row", async () => {
      const { user } = renderPage("/review?item=tx-ambiguous&folder=pindah");
      const sheet = await screen.findByRole("dialog", { name: "Pindah saldo belum lengkap" });
      await waitFor(() => expect(itemHeading()).toHaveFocus());
      await user.click(within(sheet).getByRole("button", { name: "Kembali ke daftar" }));
      await waitFor(() => expect(screen.queryByRole("dialog", { name: "Pindah saldo belum lengkap" })).not.toBeInTheDocument());
      await waitFor(() => expect(row("Masuk ke BCA Harian")).toHaveFocus());
    });

    it("a deep link focuses the heading even when the next frame runs before the sheet renders", async () => {
      // Under load the frame can fire before React commits the sheet; the sheet must still land on the heading.
      const frame = vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) => {
        callback(0);
        return 0;
      });
      try {
        renderPage("/review?item=tx-ambiguous&folder=pindah");
        await screen.findByRole("dialog", { name: "Pindah saldo belum lengkap" });
        await waitFor(() => expect(itemHeading()).toHaveFocus());
      } finally {
        frame.mockRestore();
      }
    });
  });

  // --- Both languages --------------------------------------------------------------------------------------

  it("English interface: Review, Transfer not complete, the question and the same figures", async () => {
    currentQueue = { ...queue(), questions: [], groups: [], ambiguous: [], counts: { ...queue().counts, attention: 1, unreviewed: 0 },
      awaiting: [{ ...queue().awaiting[0], account_name: "BCA Utama", amount: 300_000 }] };
    const { container } = renderPage("/review", "en");
    await loaded();
    expect(screen.getByRole("region", { name: "Review" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Needs review" })).toHaveTextContent("0 recorded automatically, optional");
    expect(railItem(/^Transfer not complete/)).toHaveAccessibleName("Transfer not complete 1 item");
    expect(itemHeading()).toHaveTextContent("Money out of BCA Utama. Where did it go?");
    expect(pane().querySelector(".tj-meta")).toHaveTextContent("Other side not found yet · Rp 300.000 · 20 Oct 12.00 · BCA Utama");
    expect(pane()).toHaveTextContent("Until it is complete, this is not counted as spending.");
    expect(answer(/Not a transfer, this is spending/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "More actions for Review" })).toBeInTheDocument();
    expect(container.querySelector("[lang='id']")).toBeNull();
    expect(document.body).not.toHaveTextContent(/\b(Expense|Spent|Outflow|ledger)\b/);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Indonesian interface: every count, amount and date equals the English page's", async () => {
    const figures = async (language: Language) => {
      const view = renderPage("/review", language);
      await loaded();
      const text = [...document.querySelectorAll(".t-hero, .rail-item .count, .r-amt, .tj-meta")]
        .map((node) => node.textContent ?? "")
        .join(" | ");
      view.unmount();
      return text.match(/[−+]?Rp [\d.]+|\d+\.\d{2}|\b\d+\b/g);
    };
    const id = await figures("id");
    const en = await figures("en");
    expect(id).toEqual(en);
    expect(id!.length).toBeGreaterThan(10);
  });
});

describe("Tinjau stylesheet", () => {
  const css = readFileSync(path.join(__dirname, "..", "globals.css"), "utf8");
  const rule = (selector: string) => {
    const start = css.indexOf(`\n${selector} {`);
    expect(start, `${selector} is missing`).toBeGreaterThanOrEqual(0);
    return css.slice(start, css.indexOf("}", start));
  };

  it("lays the panes out 232 · 336 · 1fr with 24 px gaps on one top edge, the item below up to 1199 px and a block below 768 px", () => {
    expect(rule(".tj")).toMatch(/grid-template-columns: 232px 336px minmax\(0, 1fr\); align-items: start; gap: var\(--sp-24\)/);
    expect(rule(".tj-list")).toMatch(/border-right: 1px solid var\(--line\)/);
    expect(rule(".tj-detail")).toMatch(/max-width: 880px/);
    expect(css).toMatch(/@media \(max-width: 1199px\) \{\n {2}\.tj \{ grid-template-columns: 184px minmax\(0, 1fr\); \}\n {2}\.tj-detail \{ grid-column: 1 \/ -1; \}/);
    expect(css).toMatch(/@media \(max-width: 767px\) \{\n {2}\.tj \{ display: block; \}/);
  });

  it("draws rail items 36 px with the straight bar, phone chips 44 px without it, and keeps focus clear of the top bar", () => {
    expect(rule(".rail-item")).toMatch(/height: 36px;[\s\S]*border-radius: var\(--r-control\)/);
    expect(css).toContain(".rail.is-chips .rail-item { flex: none; width: auto; height: 44px; border: 1px solid var(--line); }");
    expect(css).toContain(".rail.is-chips .rail-item::before { content: none; }");
    expect(css).toContain(".tj .row, .tj-q { scroll-margin-top: calc(var(--topbar-h) + var(--sp-16)); }");
    expect(rule(".answer")).toMatch(/min-height: 56px;[\s\S]*padding: var\(--sp-8\) var\(--sp-12\)/);
    expect(css).toContain(".answer:disabled { cursor: not-allowed; opacity: 0.45; }");
    expect(css).toContain("dialog.shell-dialog.is-full { height: 100vh; max-height: 100vh; margin: 0; border-radius: 0; }");
  });
});
