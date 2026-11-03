import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CycleSummary } from "@/components/beranda/types";
import { ToastProvider } from "@/components/workspace/overlays";
import { ShortcutProvider, useShortcuts } from "@/components/workspace/shortcuts";
import { api } from "@/lib/api";
import { bindThreadUser, clearThread } from "@/lib/tanya/thread";
import { TanyaView } from "./TanyaView";
import { billLines } from "./cards";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class ApiError extends Error {
    constructor(readonly status: number, readonly detail: unknown) {
      super(typeof detail === "string" ? detail : (detail as { message?: string })?.message ?? "Request failed");
    }
  },
}));
vi.mock("next/navigation", () => ({ usePathname: () => "/tanya", useRouter: () => ({ push: vi.fn(), replace: vi.fn() }) }));
vi.mock("@/components/transactions/TransactionDrawer", () => ({
  TransactionDrawer: ({ transactionId }: { transactionId: string | null }) =>
    transactionId ? <div role="dialog" aria-label="Detail transaksi">{transactionId}</div> : null,
}));

const ctx = { user: { id: "user-1", name: "Raka Contoh", username: "raka" } as Record<string, unknown> | null, hideBalances: false, reviewAttention: 3 as number | null };
const money = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ ...ctx, bal: (amount: number) => (ctx.hideBalances ? "Rp ••••••" : money(amount)) }),
}));

const SUMMARY: CycleSummary = {
  ok: true,
  as_of: "2026-11-03T06:00:00Z",
  period: { basis: "cycle", status: "open", cycle_status: "open", start_date: "2026-10-25", today: "2026-11-03", day_index: 10, label: "Siklus sejak 25 Okt", cycle_offset_basis: "cycle" },
  expected_salary: { status: "estimated", date: "2026-11-25", basis: "regular_receipts", receipt_dates: [], reason: null },
  keluar: {
    total: 3_521_500,
    offsets_total: 50_000,
    lines: [
      { key: "need", label: "Kebutuhan", amount: 1_420_000 },
      { key: "want", label: "Keinginan", amount: 610_000 },
      { key: "culture", label: "Budaya", amount: 245_000 },
      { key: "unexpected", label: "Tak Terduga", amount: 89_500 },
      { key: "saving", label: "Tabungan", amount: 800_000, note: "Termasuk uang yang dipindah ke kantong tabungan" },
      { key: "unclassified", label: "Belum diklasifikasi", amount: 357_000 },
    ],
  },
  masuk: { total: 9_800_000, lines: [] },
  waiting: { count: 4, total: 581_000, in_keluar: 357_000, not_in_keluar: 224_000, lines: [] },
  scheduled: {
    total: 1_665_000,
    window_start: "2026-11-03",
    window_end: "2026-11-25",
    window_basis: "expected_salary",
    lines: [
      { kind: "plan", rule_id: "r-1", label: "Streamio", due_date: "2026-11-03", overdue: false, amount: 65_000 },
      { kind: "plan", rule_id: "r-2", label: "Cicilan Laptop", due_date: "2026-11-05", overdue: false, amount: 600_000 },
      { kind: "plan", rule_id: "r-3", label: "Listrik", due_date: "2026-11-20", overdue: false, amount: 400_000 },
      { kind: "goal", goal_id: "g-1", label: "Dana Darurat", target_date: "2027-06-01", months_left: 7, set_aside: 0, funded: 0, amount: 600_000 },
    ],
  },
  remaining: { total: 5_283_500, lines: [] },
  safe_to_spend: { status: "available", reason: null, detail: null, per_day: 240_000, days_left: 22, rounding: 1000, over_by: null },
  net_worth: { total: 46_330_000, liquid: 20_000_000, investment: 28_730_000, debts: 2_400_000, lines: [] },
  comparison: {
    status: "available", compared_days: 10, label: "Siklus ini", previous_label: "Siklus 25 Sep – 25 Okt",
    keluar: { current: 3_521_500, previous: 3_000_000, change: 521_500 },
    masuk: { current: 9_800_000, previous: 9_800_000, change: 0 },
    belanja: { current: 2_721_500, previous: 2_410_000, change: 311_500 },
  },
};

const QUEUE = {
  questions: [{
    event_id: "e-1", question_id: "q-1", prompt: "Apa ini?", options: [], suggestions: [],
    transaction: { id: "tx-q", amount: 45_000, merchant: "QRIS 0812-xxxx", notes: null, date: "2026-11-02T09:05:00Z", account_name: "Dompet Digital", type: "expense" },
  }],
  groups: [
    { key: "g-1", label: "Kedai Awan", total: 115_000, needs_review_count: 2, auto_count: 2, transactions: [{ id: "tx-g1", review_status: "needs_review", amount: 30_000 }, { id: "tx-g2", review_status: "needs_review", amount: 25_000 }] },
    { key: "g-2", label: "Toko Serba Ada", total: 312_000, needs_review_count: 1, auto_count: 0, transactions: [{ id: "tx-g3", review_status: "needs_review", amount: 312_000 }] },
  ],
  awaiting: [{ id: "tx-a", type: "expense", account_name: "RDN Contoh", amount: 112_590, review_status: "needs_review" }],
  ambiguous: [],
  truncated: false,
  counts: { attention: 4, unreviewed: 2, questions: 1 },
};

const REPORT = { ready: true, period: { label: "Siklus 25 Sep – 25 Okt", end_date: "2026-10-25" }, url: "/laporan?period=2026-09-25" };

const preview = (text: string) => /\d/.test(text)
  ? { amount: 5000, problems: [], kind: "expense", description: "Parkir", occurred_at: "2026-11-03T07:00:00Z", time_phrase: "jam 2", time_options: [], account_hint: null, category_hint: null }
  : { amount: null, problems: ["missing_amount"], kind: "expense", description: text, occurred_at: null, time_phrase: text.includes("tadi") ? "tadi" : null, time_options: [], account_hint: null, category_hint: null };

const recorded = (over: Record<string, unknown> = {}) => ({
  id: "cap-1", status: "recorded", question: null,
  summary: { amount: 5000, description: "Parkir", time_label: "hari ini 14.00" },
  transaction: { id: "tx-new", type: "expense", amount: 5000, date: "2026-11-03T07:00:00Z", account_name: "Tunai", merchant: null, notes: "Parkir", category_name: "Transportasi", kakeibo: "need", review_status: "reviewed" },
  undo_until: new Date(Date.now() + 10_000).toISOString(),
  ...over,
});

let routes: Record<string, (body?: unknown) => unknown>;

function defaultRoutes(): Record<string, (body?: unknown) => unknown> {
  return {
    "GET /dashboard/cycle-summary": () => SUMMARY,
    "GET /review/summary": () => ({ ok: true, attention: 4 }),
    "GET /review/queue": () => QUEUE,
    "GET /reports/monthly/latest": () => REPORT,
    "POST /capture/parse": (body) => ({ ok: true, preview: preview((body as { text: string }).text) }),
    "POST /capture/text": () => ({ ok: true, capture: recorded() }),
    "POST /capture/cap-1/undo": () => ({ ok: true, capture: recorded({ status: "undone", undo_until: null }) }),
  };
}

function call(method: string, path: string, body?: unknown) {
  const handler = routes[`${method} ${path}`];
  if (!handler) return Promise.reject(new Error(`unexpected ${method} ${path}`));
  try {
    return Promise.resolve(handler(body));
  } catch (failure) {
    return Promise.reject(failure);
  }
}

function PageGroups() {
  const { pageGroups } = useShortcuts();
  return <p data-testid="groups">{pageGroups.map((group) => group.name).join(",")}</p>;
}

function setup() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <ShortcutProvider>
        <ToastProvider>
          <main>
            <TanyaView />
          </main>
          <PageGroups />
        </ToastProvider>
      </ShortcutProvider>
    </QueryClientProvider>,
  );
  return { ...view, user: userEvent.setup(), composer: () => screen.getByRole("textbox", { name: "Tulis transaksi atau pertanyaan" }) };
}

const log = () => screen.getByRole("log", { name: "Percakapan" });
const posts = (path: string) => vi.mocked(api.post).mock.calls.filter(([p]) => p === path);

beforeEach(() => {
  routes = defaultRoutes();
  ctx.user = { id: "user-1", name: "Raka Contoh", username: "raka" };
  ctx.hideBalances = false;
  ctx.reviewAttention = 3;
  bindThreadUser("user-1");
  clearThread();
  vi.mocked(api.get).mockImplementation((path: string) => call("GET", path) as never);
  vi.mocked(api.post).mockImplementation((path: string, body?: unknown) => call("POST", path, body) as never);
  vi.stubGlobal("IntersectionObserver", undefined);
});

afterEach(() => {
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});

async function ask(user: ReturnType<typeof userEvent.setup>, composer: HTMLElement, text: string) {
  await user.type(composer, `${text}{Enter}`);
}

describe("Tanya page", () => {
  it("greets by first name and leads with safe-to-spend and the waiting count", async () => {
    setup();
    expect(screen.getByRole("heading", { level: 2 })).toHaveTextContent(/^(Pagi|Siang|Sore|Malam), Raka\.$/);
    expect(await screen.findByText("Aman dipakai hari ini Rp 240.000 · 3 hal nunggu jawabanmu.")).toBeInTheDocument();
  });

  it("falls back to the username, then to no name", () => {
    ctx.user = { id: "user-1", username: "raka" };
    const { unmount } = setup();
    // The shared greeting rule capitalises a username, as Beranda does.
    expect(screen.getByRole("heading", { level: 2 })).toHaveTextContent(/, Raka\.$/);
    unmount();
    ctx.user = { id: "user-1" };
    setup();
    expect(screen.getByRole("heading", { level: 2 })).toHaveTextContent(/^(Pagi|Siang|Sore|Malam)\.$/);
  });

  it("names the remaining amount when safe-to-spend is unavailable, never 0", async () => {
    routes["GET /dashboard/cycle-summary"] = () => ({ ...SUMMARY, safe_to_spend: { ...SUMMARY.safe_to_spend, status: "unavailable", reason: "no_expected_salary", detail: "too_few_receipts", per_day: null, days_left: null } });
    setup();
    expect(await screen.findByText(/^Sisa uang periode ini Rp 5\.283\.500/)).toBeInTheDocument();
    expect(screen.queryByText(/Rp 0\b/)).not.toBeInTheDocument();
  });

  it("leaves an unloaded count out of the suggestion label instead of showing 0", () => {
    ctx.reviewAttention = null;
    setup();
    const suggestions = screen.getByRole("list", { name: "Saran pertanyaan" });
    expect(within(suggestions).getByRole("button", { name: /^Menunggu jawaban Transaksi/ })).toBeInTheDocument();
    expect(within(suggestions).queryByText(/\(0\)/)).not.toBeInTheDocument();
  });

  it("answers a suggestion with a snapshot card whose figures are the server's", async () => {
    const { user } = setup();
    await user.click(screen.getByRole("button", { name: /^Ringkasan siklus ini/ }));
    const card = await within(log()).findByRole("group", { name: /^Ringkasan periode:/ });
    for (const text of ["Rp 240.000", "Rp 3.521.500", "Rp 9.800.000", "Rp 46.330.000", "4 · Rp 581.000"]) expect(within(card).getByText(text)).toBeInTheDocument();
    expect(within(card).getByText("Rp 357.000 di antaranya sudah termasuk dalam Keluar")).toBeInTheDocument();
    expect(within(card).getByText(/^Per \d{2}\.\d{2}$/)).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/belum dihitung/i);
  });

  it("lists Kakeibo lines in Beranda's order that add up to Keluar", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "kakeibo");
    const card = await within(log()).findByRole("group", { name: /^Keluar per pilar Kakeibo/ });
    const rows = within(card).getAllByRole("definition").map((cell) => Number(cell.textContent!.replace(/\D/g, "")));
    const total = rows.pop();
    expect(rows.reduce((sum, amount) => sum + amount, 0)).toBe(total);
    expect(total).toBe(SUMMARY.keluar.total);
    expect(within(card).getAllByRole("term").map((cell) => cell.textContent?.split("Termasuk")[0])).toEqual([
      "Kebutuhan", "Keinginan", "Budaya", "Tak Terduga", "Tabungan", "Belum diklasifikasi", "Total keluar",
    ]);
    expect(within(card).getByText("Sudah dikurangi ganti uang Rp 50.000.")).toBeInTheDocument();
    expect(within(card).getByText(/Belanja Rp 2\.721\.500 dalam 10 hari pertama/)).toBeInTheDocument();
  });

  it("shows only plans due in the next 7 days for this week, with their sum", async () => {
    expect(billLines(SUMMARY, "week").map((line) => line.label)).toEqual(["Streamio", "Cicilan Laptop"]);
    expect(billLines(SUMMARY, "window")).toHaveLength(4);
    const { user, composer } = setup();
    await ask(user, composer(), "tagihan minggu ini");
    const card = await within(log()).findByRole("group", { name: /^Tagihan dan setoran:/ });
    expect(within(card).getByText("7 hari ke depan")).toBeInTheDocument();
    expect(within(card).queryByText("Listrik")).not.toBeInTheDocument();
    expect(within(card).getByText("Rp 665.000")).toBeInTheDocument();
  });

  it("names the cycle window for bills without a range", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "tagihan");
    const card = await within(log()).findByRole("group", { name: /^Tagihan dan setoran:/ });
    expect(within(card).getByText("Sampai gaji berikutnya")).toBeInTheDocument();
    expect(within(card).getByText("Rp 1.665.000")).toBeInTheDocument();
  });

  it("says so when no bill falls in the period", async () => {
    routes["GET /dashboard/cycle-summary"] = () => ({ ...SUMMARY, scheduled: { ...SUMMARY.scheduled, total: 0, lines: [] } });
    const { user, composer } = setup();
    await ask(user, composer(), "tagihan");
    expect(await within(log()).findByText("Tidak ada tagihan atau setoran dalam periode ini.")).toBeInTheDocument();
  });

  it("counts waiting rows as the badge does and links at most 3 items to their exact Tinjau place", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "menunggu jawaban");
    const card = await within(log()).findByRole("group", { name: /^Menunggu jawaban: 4 perlu ditinjau/ });
    expect(within(card).getByText("4 perlu ditinjau")).toBeInTheDocument();
    const links = within(card).getAllByRole("link").filter((link) => link.getAttribute("href")?.startsWith("/review?"));
    expect(links.map((link) => link.getAttribute("href"))).toEqual([
      "/review?item=tx-q&folder=apa", "/review?item=tx-g1&folder=jawab", "/review?item=tx-g3&folder=jawab",
    ]);
    expect(within(card).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
  });

  it("says nothing waits when the badge is 0", async () => {
    routes["GET /review/summary"] = () => ({ ok: true, attention: 0 });
    routes["GET /review/queue"] = () => ({ questions: [], groups: [], awaiting: [], ambiguous: [], truncated: false, counts: { attention: 0, unreviewed: 0, questions: 0 } });
    const { user, composer } = setup();
    await ask(user, composer(), "tinjau");
    expect(await within(log()).findByText("Tidak ada yang perlu ditinjau.")).toBeInTheDocument();
  });

  it("links the latest report when ready and says so when not", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "laporan terbaru");
    const card = await within(log()).findByRole("group", { name: /^Laporan terbaru: Laporan Siklus 25 Sep/ });
    expect(within(card).getByRole("link", { name: "Buka laporan" })).toHaveAttribute("href", REPORT.url);
    routes["GET /reports/monthly/latest"] = () => ({ ready: false, period: null, url: null });
    await user.click(within(card).getByRole("button", { name: "Perbarui" }));
    expect(await within(card).findByText("Laporan berikutnya belum siap.")).toBeInTheDocument();
  });

  it("hides every amount, in cards and in the assistant's words, when balances are hidden", async () => {
    ctx.hideBalances = true;
    const { user } = setup();
    expect(await screen.findByText("Aman dipakai hari ini Rp •••••• · 3 hal nunggu jawabanmu.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /^Ringkasan siklus ini/ }));
    await within(log()).findByRole("group", { name: /^Ringkasan periode:/ });
    expect(document.body.textContent).not.toMatch(/Rp\s?\d/);
  });

  it("shows an error with retry when a card cannot load, never a 0, and rebuilds it in place", async () => {
    routes["GET /dashboard/cycle-summary"] = () => { throw new TypeError("Failed to fetch"); };
    const { user, composer } = setup();
    await ask(user, composer(), "ringkasan");
    const card = await within(log()).findByRole("group", { name: /^Ringkasan periode: Data ini tidak dapat dimuat\./ });
    expect(within(card).getByRole("alert")).toHaveTextContent("Data ini tidak dapat dimuat.");
    expect(within(card).queryByText(/Rp/)).not.toBeInTheDocument();
    routes["GET /dashboard/cycle-summary"] = () => SUMMARY;
    await user.click(within(card).getByRole("button", { name: "Coba lagi" }));
    expect(await within(log()).findByText("Rp 46.330.000")).toBeInTheDocument();
  });
});

describe("Tanya recording", () => {
  it("records on send, shows the capture card and keeps focus in the composer", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb jam 2");
    const card = await within(log()).findByRole("group", { name: /^Catatan: Parkir, Rp 5\.000, Dicatat/ });
    expect(within(card).getByText("Tunai")).toBeInTheDocument();
    expect(within(card).getByText("Transportasi · Kebutuhan")).toBeInTheDocument();
    expect(within(card).getByText("hari ini 14.00")).toBeInTheDocument();
    expect(composer()).toHaveFocus();
    expect(composer()).toHaveValue("");
    expect(posts("/capture/text")).toHaveLength(1);
    expect(posts("/capture/text")[0][1]).toMatchObject({ text: "parkir 5rb jam 2" });
  });

  it("undoes inside the window, announces it and returns focus to the composer", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    const card = await within(log()).findByRole("group", { name: /^Catatan:/ });
    await user.click(within(card).getByRole("button", { name: "Urungkan" }));
    expect(await within(log()).findByText("Dibatalkan")).toBeInTheDocument();
    expect(screen.getAllByRole("status").some((region) => region.textContent === "Catatan dibatalkan.")).toBe(true);
    expect(composer()).toHaveFocus();
    expect(within(card).queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
  });

  it("shows the server's reason when undo is refused", async () => {
    const { ApiError } = await import("@/lib/api");
    routes["POST /capture/cap-1/undo"] = () => { throw new ApiError(410, "Waktu untuk membatalkan sudah lewat."); };
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    const card = await within(log()).findByRole("group", { name: /^Catatan:/ });
    await user.click(within(card).getByRole("button", { name: "Urungkan" }));
    expect(await within(log()).findByText("Waktu untuk membatalkan sudah lewat.")).toBeInTheDocument();
  });

  it("offers no Urungkan once the window has closed", async () => {
    routes["POST /capture/text"] = () => ({ ok: true, capture: recorded({ undo_until: new Date(Date.now() - 1000).toISOString() }) });
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    const card = await within(log()).findByRole("group", { name: /^Catatan:/ });
    expect(within(card).queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
    expect(within(card).getByRole("button", { name: "Ubah" })).toBeInTheDocument();
  });

  it("opens the shared drawer on the recorded row from Ubah", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    const card = await within(log()).findByRole("group", { name: /^Catatan:/ });
    await user.click(within(card).getByRole("button", { name: "Ubah" }));
    expect(screen.getByRole("dialog", { name: "Detail transaksi" })).toHaveTextContent("tx-new");
  });

  it("asks the server's follow-up in the thread and answers it with a fresh reply id", async () => {
    routes["POST /capture/text"] = () => ({ ok: true, capture: recorded({ status: "needs_detail", transaction: null, question: { question_id: "q-9", type: "account", prompt: "Dari rekening mana?", options: [{ id: "acc-1", label: "Tunai" }, { id: "acc-2", label: "Dompet Digital" }], allows_text: false } }) });
    routes["POST /capture/cap-1/answer"] = () => ({ ok: true, capture: recorded() });
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    const question = await within(log()).findByRole("group", { name: "Dari rekening mana?" });
    await user.click(within(question).getByRole("button", { name: "Tunai" }));
    expect(await within(log()).findByRole("group", { name: /^Catatan: Parkir, Rp 5\.000, Dicatat/ })).toBeInTheDocument();
    const body = posts("/capture/cap-1/answer")[0][1] as Record<string, string>;
    expect(body).toMatchObject({ question_id: "q-9", option_id: "acc-1" });
    expect(body.reply_id).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("keeps an unanswered question open and says the row is not recorded yet", async () => {
    routes["POST /capture/text"] = () => ({ ok: true, capture: recorded({ status: "needs_detail", transaction: null, question: { question_id: "q-9", type: "account", prompt: "Dari rekening mana?", options: [{ id: "acc-1", label: "Tunai" }], allows_text: true } }) });
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    await within(log()).findByRole("group", { name: "Dari rekening mana?" });
    await ask(user, composer(), "bantuan");
    expect(await within(log()).findByText("Belum dicatat sampai pertanyaan ini dijawab.")).toBeInTheDocument();
    expect(within(log()).getByRole("textbox", { name: "Jawaban" })).toBeInTheDocument();
  });

  it("keeps the text in the composer and records nothing when capture is rate limited", async () => {
    const { ApiError } = await import("@/lib/api");
    routes["POST /capture/text"] = () => { throw new ApiError(429, { code: "capture_rate_limited" }); };
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    expect(await within(log()).findByText("Terlalu banyak catatan dalam satu menit. Coba lagi sebentar lagi.")).toBeInTheDocument();
    expect(composer()).toHaveValue("parkir 5rb");
  });

  it("marks a message that did not reach the server and resends it with the same id", async () => {
    let attempts = 0;
    routes["POST /capture/text"] = () => {
      attempts += 1;
      if (attempts === 1) throw new TypeError("Failed to fetch");
      return { ok: true, capture: recorded() };
    };
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    expect(await within(log()).findByText("Gagal terkirim.")).toBeInTheDocument();
    await user.click(within(log()).getByRole("button", { name: "Kirim ulang" }));
    expect(await within(log()).findByRole("group", { name: /^Catatan:/ })).toBeInTheDocument();
    const ids = posts("/capture/text").map(([, body]) => (body as { client_capture_id: string }).client_capture_id);
    expect(ids).toHaveLength(2);
    expect(ids[0]).toBe(ids[1]);
    expect(within(log()).queryByText("Gagal terkirim.")).not.toBeInTheDocument();
  });

  it("keeps the text when the parse fails and records nothing", async () => {
    routes["POST /capture/parse"] = () => { throw new TypeError("Failed to fetch"); };
    const { user, composer } = setup();
    await ask(user, composer(), "parkir 5rb");
    expect(await within(log()).findByText("Pesan ini tidak dapat diproses. Coba lagi.")).toBeInTheDocument();
    expect(composer()).toHaveValue("parkir 5rb");
    expect(posts("/capture/text")).toHaveLength(0);
  });

  it("never records a question that holds a number, and answers unknown text with the chips", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "berapa keluar 2 hari ini?");
    expect(await within(log()).findByText(/^Aku belum bisa jawab itu\./)).toBeInTheDocument();
    await ask(user, composer(), "halo");
    await waitFor(() => expect(within(log()).getAllByText(/^Aku belum bisa jawab itu\./)).toHaveLength(2));
    expect(posts("/capture/text")).toHaveLength(0);
    expect(posts("/capture/parse")).toHaveLength(1);
    expect(within(log()).getAllByRole("button", { name: "Laporan terbaru" }).length).toBeGreaterThan(0);
  });

  it("asks for the amount on a transaction without one and records nothing", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "parkir tadi");
    expect(await within(log()).findByText(/^Jumlahnya berapa\? Aku nggak nebak\./)).toBeInTheDocument();
    expect(posts("/capture/text")).toHaveLength(0);
  });

  it("records a question with a number on purpose through Catat sebagai transaksi", async () => {
    const { user, composer } = setup();
    await ask(user, composer(), "tagihan listrik 200rb");
    await user.click(await within(log()).findByRole("button", { name: "Catat sebagai transaksi" }));
    expect(await within(log()).findByRole("group", { name: /^Catatan:/ })).toBeInTheDocument();
    expect(posts("/capture/text")[0][1]).toMatchObject({ text: "tagihan listrik 200rb" });
  });
});

describe("Tanya accessibility", () => {
  it("is a labelled log of list items with speaker labels and has no axe violations", async () => {
    const { user, composer, container } = setup();
    await ask(user, composer(), "ringkasan");
    await within(log()).findByRole("group", { name: /^Ringkasan periode:/ });
    const items = within(log()).getAllByRole("listitem");
    expect(items[0]).toHaveTextContent(/^Pesan terkirim, \d{2}\.\d{2}:ringkasan/);
    expect(items[1]).toHaveTextContent(/^CashFlow, \d{2}\.\d{2}:/);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("sends on Enter, adds a line on Shift+Enter, and never sends empty text", async () => {
    const { user, composer } = setup();
    await user.type(composer(), "{Enter}");
    expect(api.post).not.toHaveBeenCalled();
    await user.type(composer(), "baris satu{Shift>}{Enter}{/Shift}baris dua");
    expect(composer()).toHaveValue("baris satu\nbaris dua");
    expect(api.post).not.toHaveBeenCalled();
    await user.type(composer(), "{Enter}");
    await waitFor(() => expect(posts("/capture/parse")).toHaveLength(1));
  });

  it("lists its keys in the help under Di Tanya", () => {
    setup();
    expect(screen.getByTestId("groups")).toHaveTextContent("Di Tanya");
  });

  it("offers Hapus percakapan after the first message and empties the thread on confirm", async () => {
    const { user, composer } = setup();
    expect(screen.queryByRole("button", { name: "Hapus percakapan" })).not.toBeInTheDocument();
    await ask(user, composer(), "bantuan");
    await within(log()).findByText(/^Aku bisa catat transaksi/);
    await user.click(screen.getByRole("button", { name: "Hapus percakapan" }));
    await user.click(screen.getByRole("button", { name: "Ya, lanjutkan" }));
    expect(within(log()).queryAllByRole("listitem")).toHaveLength(0);
    expect(screen.getByRole("list", { name: "Saran pertanyaan" })).toBeInTheDocument();
  });

  it("shows another user nothing of the first user's thread", async () => {
    const { user, composer, unmount } = setup();
    await ask(user, composer(), "bantuan");
    await within(log()).findByText(/^Aku bisa catat transaksi/);
    unmount();
    ctx.user = { id: "user-2", name: "Pemilik Lain" };
    setup();
    await act(async () => {});
    expect(within(log()).queryAllByRole("listitem")).toHaveLength(0);
  });
});
