import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { ExplainProvider } from "@/components/workspace/explain";
import { headerFor } from "@/components/beranda/BerandaView";
import type { CycleSummary } from "@/components/beranda/types";
import BerandaPage from "./page";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() },
  getDevices: vi.fn().mockResolvedValue({ ok: true, devices: [] }),
  ApiError: class ApiError extends Error {
    constructor(readonly status: number, readonly detail: unknown) {
      super(typeof detail === "string" ? detail : "Request failed");
    }
  },
}));
vi.mock("next/navigation", () => ({ usePathname: () => "/", useRouter: () => ({ push: vi.fn() }) }));
vi.mock("@/components/recurring/PendingScheduledBanner", () => ({ PendingScheduledBanner: () => null }));
vi.mock("@/components/recurring/RecurringRulesModal", () => ({ RecurringRulesModal: () => null }));

const NOW = new Date("2026-11-02T09:20:00Z"); // 16.20 in Jakarta, day 9 of a cycle that started 25 October

/** The approved preview's numbers (2 November, day 9 of the cycle since 25 October). */
function previewSummary(): CycleSummary {
  return {
    ok: true,
    as_of: "2026-11-02T16:20:00+07:00",
    period: { basis: "cycle", status: "open", cycle_status: "open", start_date: "2026-10-25", today: "2026-11-02", day_index: 9,
      label: "Siklus sejak 25 Okt", cycle_offset_basis: "cycle" },
    expected_salary: { status: "estimated", date: "2026-11-25", basis: "regular_receipts",
      receipt_dates: ["2026-08-25", "2026-09-25", "2026-10-25"], reason: null },
    keluar: {
      total: 3_164_500,
      offsets_total: 0,
      lines: [
        { key: "need", label: "Kebutuhan", amount: 1_420_000, note: null, href: "/ledger?period=cycle&kakeibo=need" },
        { key: "want", label: "Keinginan", amount: 610_000, note: null, href: "/ledger?period=cycle&kakeibo=want" },
        { key: "culture", label: "Budaya", amount: 245_000, note: null, href: "/ledger?period=cycle&kakeibo=culture" },
        { key: "unexpected", label: "Tak Terduga", amount: 89_500, note: null, href: "/ledger?period=cycle&kakeibo=unexpected" },
        { key: "saving", label: "Tabungan", amount: 800_000, note: "Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan",
          href: "/ledger?period=cycle&kakeibo=saving" },
        { key: "unclassified", label: "Belum diklasifikasi", amount: 0, note: null, href: "/ledger?period=cycle&kakeibo=unclassified" },
      ],
    },
    spending: { amount: 2_364_500, lines: [
      { key: "need", label: "Kebutuhan", amount: 1_420_000, note: null, href: "/ledger?period=cycle&kakeibo=need" },
      { key: "want", label: "Keinginan", amount: 610_000, note: null, href: "/ledger?period=cycle&kakeibo=want" },
      { key: "culture", label: "Budaya", amount: 245_000, note: null, href: "/ledger?period=cycle&kakeibo=culture" },
      { key: "unexpected", label: "Tak Terduga", amount: 89_500, note: null, href: "/ledger?period=cycle&kakeibo=unexpected" },
      { key: "unclassified", label: "Belum diklasifikasi", amount: 0, note: null, href: "/ledger?period=cycle&kakeibo=unclassified" },
    ] },
    fresh_saving: { amount: 800_000, lines: [
      { key: "saving", label: "Tabungan", amount: 800_000, note: "Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan",
        href: "/ledger?period=cycle&kakeibo=saving" },
    ] },
    masuk: { total: 9_800_000, lines: [
      { key: "gaji", label: "Gaji", amount: 9_500_000, href: "/ledger?period=cycle&type=income&category=gaji" },
      { key: "refund", label: "Refund", amount: 300_000, href: "/ledger?period=cycle&type=income&category=refund" },
    ] },
    waiting: { count: 4, total: 581_000, in_keluar: 357_000, not_in_keluar: 224_000, lines: [
      { section: "questions", transaction_id: "qris", label: "QRIS 0812-xxxx", account_name: "Dompet Digital", type: "expense",
        date: "2026-11-02T09:05:00Z", amount: 45_000, in_keluar: true, in_keluar_as: "unclassified" },
      { section: "awaiting", transaction_id: "rdn-1", label: "RDN Contoh keluar", account_name: "RDN Contoh", type: "expense",
        date: "2026-10-29T03:30:00Z", amount: 112_000, in_keluar: false, in_keluar_as: null },
      { section: "awaiting", transaction_id: "rdn-2", label: "RDN Contoh keluar", account_name: "RDN Contoh", type: "expense",
        date: "2026-11-01T03:30:00Z", amount: 112_000, in_keluar: false, in_keluar_as: null },
      { section: "groups", transaction_id: "toko", label: "Toko Serba Ada", account_name: "Bank Utama", type: "expense",
        date: "2026-11-01T12:20:00Z", amount: 312_000, in_keluar: true, in_keluar_as: "unclassified" },
    ] },
    scheduled: { total: 1_713_500, window_start: "2026-10-25", window_end: "2026-11-24", window_basis: "expected_salary", lines: [
      { kind: "plan", rule_id: "streamio", label: "Streamio", due_date: "2026-11-03", overdue: false, amount: 65_000 },
      { kind: "plan", rule_id: "laptop", label: "Cicilan Laptop", due_date: "2026-11-05", overdue: false, amount: 600_000 },
      { kind: "plan", rule_id: "internet", label: "Internet Rumah", due_date: "2026-11-10", overdue: false, amount: 330_000 },
      { kind: "goal", goal_id: "lombok", label: "Liburan Lombok", target_date: "2027-06-30", months_left: 8, set_aside: 600_000, funded: 0, amount: 600_000 },
      { kind: "goal", goal_id: "dd", label: "Dana Darurat", target_date: "2027-12-31", months_left: 14, set_aside: 118_500, funded: 0, amount: 118_500 },
    ] },
    remaining: { total: 4_922_000, lines: [
      { key: "masuk", label: "Masuk", amount: 9_800_000 },
      { key: "keluar", label: "Keluar", amount: -3_164_500 },
      { key: "scheduled", label: "Terjadwal", amount: -1_713_500 },
    ] },
    safe_to_spend: { status: "available", reason: null, detail: null, per_day: 214_000, days_left: 23, rounding: 1000, over_by: null },
    net_worth: { total: 46_330_000, liquid: 21_480_000, investment: 27_250_000, debts: 2_400_000, lines: [
      { key: "liquid", label: "Dana likuid", amount: 21_480_000 },
      { key: "investment", label: "Investasi", amount: 27_250_000 },
      { key: "debt:laptop", label: "Cicilan Laptop", amount: -2_400_000 },
    ] },
    comparison: { status: "available", compared_days: 9, label: "Siklus sejak 25 Okt", previous_label: "Siklus 25 Sep – 24 Okt",
      keluar: { current: 3_164_500, previous: 3_010_000, change: 154_500 },
      masuk: { current: 9_800_000, previous: 9_500_000, change: 300_000 },
      belanja: { current: 2_364_500, previous: 2_410_000, change: -45_500 } },
  };
}

const queue = {
  ok: true, truncated: false,
  counts: { attention: 4, unreviewed: 3, questions: 1, needs_review: 1, auto: 0, groups: 1, awaiting: 2, ambiguous: 0 },
  questions: [{ event_id: "e1", post_time: "2026-11-02T09:05:00Z",
    question: { question_id: "q1", prompt: "Apa ini?", suggestions: [] },
    transaction: { id: "qris", type: "expense", amount: 45_000, date: "2026-11-02T09:05:00Z", account_name: "Dompet Digital",
      notes: "QRIS 0812-xxxx", merchant: null, category_name: null } }],
  groups: [{ key: "toko", kind: "merchant", type: "expense", label: "Toko Serba Ada", count: 1, total: 312_000, needs_review_count: 1, auto_count: 0,
    first_date: "2026-11-01T12:20:00Z", last_date: "2026-11-01T12:20:00Z", reasons: [], category: null,
    transactions: [{ id: "toko", date: "2026-11-01T12:20:00Z" }] }],
  awaiting: [
    { id: "rdn-1", account_name: "RDN Contoh", amount: 112_000, date: "2026-10-29T03:30:00Z" },
    { id: "rdn-2", account_name: "RDN Contoh", amount: 112_000, date: "2026-11-01T03:30:00Z" },
  ],
  ambiguous: [],
};

const plan = { id: "streamio", name: "Streamio", type: "expense", amount: 65_000, next_due_date: "2026-11-03", is_active: true,
  source_account_name: "Dompet Digital" };

const recent = [
  { id: "qris", type: "expense", amount: 45_000, notes: null, merchant: "QRIS 0812-xxxx", account_id: "dw", account_name: "Dompet Digital",
    date: "2026-11-02T09:05:00Z", review_status: "needs_review", source: "notification", classified_by: "ai", category_name: null },
  { id: "topup-out", type: "expense", amount: 250_000, notes: "", account_id: "rdn", account_name: "RDN Contoh", date: "2026-11-02T03:00:00Z",
    movement_id: "t1", movement_role: "outbound", investment_topup_id: "topup-1", transfer_target_account_name: "Reksa Dana A",
    category_name: "Internal Movement", review_status: "reviewed" },
  { id: "topup-in", type: "income", amount: 250_000, notes: "", account_id: "rda", account_name: "Reksa Dana A", date: "2026-11-02T03:00:00Z",
    movement_id: "t1", movement_role: "inbound", investment_topup_id: "topup-1", partner_id: "topup-out", category_name: "Internal Movement",
    review_status: "reviewed" },
  { id: "move-out", type: "expense", amount: 500_000, notes: "", account_id: "bu", account_name: "Bank Utama", date: "2026-11-02T02:12:00Z",
    movement_id: "m1", movement_role: "outbound", transfer_target_account_name: "Dompet Digital", category_name: "Internal Movement",
    review_status: "auto" },
  { id: "move-in", type: "income", amount: 500_000, notes: "", account_id: "dw", account_name: "Dompet Digital", date: "2026-11-02T02:12:00Z",
    movement_id: "m1", movement_role: "inbound", partner_id: "move-out", category_name: "Internal Movement", review_status: "auto" },
  { id: "kopi", type: "expense", amount: 32_000, notes: null, merchant: "Kopi Pagi", account_id: "dw", account_name: "Dompet Digital",
    date: "2026-11-02T00:45:00Z", kakeibo_type: "want", review_status: "auto", source: "notification", classified_by: "habit" },
];

let summary: CycleSummary | Error = previewSummary();
let reviewQueue: typeof queue = queue;

function mockApi() {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path === "/dashboard/cycle-summary") {
      if (summary instanceof Error) throw summary;
      return summary;
    }
    if (path.startsWith("/dashboard/overview")) return { kpis: { runway_status: "unknown", coverage_months: null, runway_months: null,
      runway_days: null, emergency_fund_balance: 0, monthly_primary_expense: null, essential_cost: { method: "unknown" } } };
    if (path === "/review/queue") return reviewQueue;
    if (path === "/recurring") return { rules: [plan] };
    if (path === "/recurring/pending") return { rules: [], recent_auto_links: [] };
    if (path.startsWith("/transactions?")) return { transactions: recent };
    const detail = /^\/transactions\/([^/?]+)$/.exec(path);
    if (detail) return { transaction: { ...recent.find((row) => row.id === detail[1]), waiting: false, origin: null } };
    if (path === "/accounts") return { accounts: [] };
    throw new Error(`Unexpected GET ${path}`);
  });
}

function renderBeranda() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <ExplainProvider>
        <BerandaPage />
      </ExplainProvider>
    </QueryClientProvider>,
  );
}

const kpis = async () => {
  renderBeranda();
  return (await screen.findByRole("region", { name: "Ringkasan siklus ini" })) as HTMLElement;
};
const region = (name: string | RegExp) => screen.getByRole("region", { name });
const figureNames = (scope: HTMLElement) => within(scope).getAllByRole("button").map((button) => button.getAttribute("aria-label"));

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
  summary = previewSummary();
  reviewQueue = queue;
  vi.clearAllMocks();
  mockApi();
});
afterEach(() => vi.useRealTimers());

describe("Beranda KPI cards", () => {
  it("leads with the period's safe amount, then Masuk, Pengeluaran, Ditabung and Kekayaan bersih, each a server field", async () => {
    const strip = await kpis();
    expect(figureNames(strip)).toEqual([
      "Aman dipakai sampai gajian Rp 4.922.000: lihat rincian",
      "Masuk siklus ini Rp 9.800.000: lihat rincian",
      "Pengeluaran siklus ini Rp 2.364.500: lihat rincian",
      "Ditabung & investasi Rp 800.000: lihat rincian",
      "Kekayaan bersih Rp 46.330.000: lihat rincian",
    ]);
    expect(strip).toHaveTextContent("23 hari lagi · gaji ± 25 Nov");
    expect(strip).toHaveTextContent("sejak gaji 25 Okt");
    // No per-day figure, no same-date comparison and no percentage.
    expect(strip).not.toHaveTextContent("Rp 214.000");
    expect(document.body).not.toHaveTextContent(/hari pertama|\/hari/);
    expect(strip.textContent).not.toMatch(/%/);
  });

  it("shows no written summary and no insight banner", async () => {
    await kpis();
    expect(document.querySelector(".ws-lede")).toBeNull();
    expect(screen.queryByRole("region", { name: "Wawasan periode ini" })).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent(/siklus gajimu|nunggu jawabanmu/);
  });

  it("explains the safe amount as Masuk minus Pengeluaran, Ditabung and Terjadwal", async () => {
    const user = userEvent.setup();
    await user.click(within(await kpis()).getByRole("button", { name: /^Aman dipakai sampai gajian/ }));
    const dialog = await screen.findByRole("dialog", { name: "Aman dipakai sampai gajian" });
    const cells = within(dialog).getAllByRole("row").map((row) => row.textContent);
    expect(cells).toEqual([
      "Masuk+Rp 9.800.000", "Pengeluaran−Rp 2.364.500", "Ditabung & investasi−Rp 800.000", "Terjadwal−Rp 1.713.500", "SisaRp 4.922.000",
    ]);
  });

  it("explains Pengeluaran with the five spending buckets and no saving", async () => {
    const user = userEvent.setup();
    await user.click(within(await kpis()).getByRole("button", { name: /^Pengeluaran siklus ini/ }));
    const dialog = await screen.findByRole("dialog", { name: "Pengeluaran siklus ini" });
    const rows = within(dialog).getAllByRole("row");
    expect(rows).toHaveLength(6);
    expect(rows[5]).toHaveTextContent("Total pengeluaranRp 2.364.500");
    expect(dialog).not.toHaveTextContent("Tabungan");
    expect(within(dialog).getByRole("link", { name: "Keinginan" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=want");
  });

  it("says the next salary date is unknown, still with the period amount", async () => {
    const base = previewSummary();
    summary = { ...base,
      expected_salary: { ...base.expected_salary, status: "unknown", date: null, basis: null, reason: "irregular_receipts" },
      safe_to_spend: { status: "unavailable", reason: "no_expected_salary", detail: "irregular_receipts", per_day: null, days_left: null, rounding: 1000, over_by: null } };
    const strip = await kpis();
    expect(strip).toHaveTextContent("Tanggal gaji berikutnya belum diketahui");
    expect(within(strip).getByRole("button", { name: /^Aman dipakai sampai gajian Rp 4\.922\.000/ })).toBeInTheDocument();
  });

  it("counts what credit accounts owe in the net-worth utang, next to the debts", async () => {
    const base = previewSummary();
    summary = { ...base, net_worth: { ...base.net_worth, total: 45_860_000, credit_owed: 470_000, lines: [
      ...base.net_worth.lines,
      { key: "credit:spaylater", label: "SPayLater", amount: -470_000 },
    ] } };
    const user = userEvent.setup();
    const strip = await kpis();
    expect(strip).toHaveTextContent("aset Rp 48.730.000 · utang Rp 2.870.000");
    await user.click(within(strip).getByRole("button", { name: "Kekayaan bersih Rp 45.860.000: lihat rincian" }));
    const dialog = await screen.findByRole("dialog", { name: "Kekayaan bersih" });
    expect(dialog).toHaveTextContent("Utang SPayLater");
    expect(dialog).toHaveTextContent("Cicilan Laptop");
  });

  it("states the savings change in words, with what went in, came out and was spent from savings", async () => {
    const base = previewSummary();
    summary = {
      ...base,
      fresh_saving: { ...base.fresh_saving, amount: -500_000 },
      keluar: { ...base.keluar, total: base.spending.amount - 500_000 },
      tabungan: { total: -500_000, masuk: 300_000, diambil: 600_000, belanja: 200_000, pilar_tabungan: 0 },
      net_worth: { ...base.net_worth, dana_investasi: 1_200_000 },
    };
    const user = userEvent.setup();
    const strip = await kpis();
    expect(strip).toHaveTextContent("Tabungan berkurang Rp 500.000");
    expect(within(strip).getByRole("button", { name: "Ditabung & investasi Rp 500.000: lihat rincian" })).toBeInTheDocument();
    // Dana investasi is an asset outside Saldo tersedia, so the caption's assets include it.
    const assets = base.net_worth.liquid + 1_200_000 + base.net_worth.investment - (base.net_worth.reconciliation_pending ?? 0);
    expect(strip).toHaveTextContent(`aset Rp ${assets.toLocaleString("id-ID")}`);
    await user.click(within(strip).getByRole("button", { name: "Ditabung & investasi Rp 500.000: lihat rincian" }));
    const dialog = await screen.findByRole("dialog", { name: "Ditabung & investasi" });
    expect(within(dialog).getAllByRole("row").map((row) => row.textContent)).toEqual([
      "Masuk ke tabungan+Rp 300.000",
      "Diambil dari tabungan−Rp 600.000",
      "Pengeluaran dari tabungantetap dihitung sebagai pengeluaran−Rp 200.000",
      "Tabungan berkurangRp 500.000",
    ]);
    expect((await axe(document.body)).violations).toHaveLength(0);
  });

  it("shows Rp 0 and by how much the period is over, never a negative safe amount", async () => {
    const base = previewSummary();
    summary = { ...base, remaining: { ...base.remaining, total: -300_000 }, safe_to_spend: { ...base.safe_to_spend, per_day: 0, over_by: 300_000 } };
    const strip = await kpis();
    expect(strip).toHaveTextContent("lebih Rp 300.000 dari uang masuk");
    expect(strip).not.toHaveTextContent("−Rp 300.000");
    expect(region(/^Uang siklus ini/)).toHaveTextContent("Siklus ini sudah lebih Rp 300.000 dari uang masuk.");
  });
});

describe("Beranda period and header", () => {
  it("names the cycle start and day", () => {
    expect(headerFor(previewSummary(), NOW)).toEqual({ title: "Senin, 2 November", subtitle: "Siklus sejak 25 Okt · hari ke-9" });
  });

  it("falls back to the calendar month without a cycle and links to the salary settings", async () => {
    const base = previewSummary();
    const month: CycleSummary = { ...base, period: { ...base.period, basis: "month", cycle_status: "not_configured", start_date: "2026-11-01", day_index: 2,
      cycle_offset_basis: "month", label: "November 2026" },
      safe_to_spend: { ...base.safe_to_spend, status: "unavailable", reason: "no_cycle", per_day: null, days_left: null } };
    summary = month;
    expect(headerFor(month, NOW).subtitle).toBe("November · siklus gaji belum tersedia");
    renderBeranda();
    expect(await screen.findByRole("region", { name: "Ringkasan bulan ini" })).toHaveTextContent("sejak 1 November");
    expect(screen.getByRole("link", { name: "Atur gaji di Pengaturan" })).toHaveAttribute("href", "/settings/gaji");
  });
});

describe("Beranda cards", () => {
  it("splits Uang siklus ini into four amounts that add up to Masuk, with one formula line", async () => {
    await kpis();
    const card = region(/^Uang siklus ini/);
    const list = within(card).getByRole("list", { name: "Pembagian uang siklus ini" });
    expect(within(list).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "PengeluaranRp 2.364.500", "Ditabung & investasiRp 800.000", "TerjadwalRp 1.713.500", "Aman dipakaiRp 4.922.000",
    ]);
    expect(card.querySelector(".ws-moneybar")).toHaveAttribute("aria-hidden", "true");
    expect(card).toHaveTextContent("Aman dipakai = masuk − pengeluaran − ditabung − terjadwal.");
    expect(card).toHaveTextContent("dari Rp 9.800.000 masuk");
  });

  it("lists Kakeibo's spending buckets under the Pengeluaran total and Tabungan apart", async () => {
    await kpis();
    const card = region(/^Kakeibo siklus ini/);
    const [spending, saving] = within(card).getAllByRole("list");
    expect(within(spending).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "KebutuhanRp 1.420.000", "KeinginanRp 610.000", "BudayaRp 245.000", "Tak TerdugaRp 89.500", "Belum diklasifikasiRp 0",
    ]);
    expect(saving).toHaveAccessibleName("Ditabung");
    expect(saving).toHaveTextContent("TabunganRp 800.000");
    expect(within(card).getByRole("button", { name: "Pengeluaran siklus ini Rp 2.364.500: lihat rincian" })).toBeInTheDocument();
    expect(within(card).getByRole("link", { name: "Belum diklasifikasi" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=unclassified");
  });

  it("keeps an unknown Ketahanan Dana unknown, without a sentinel", async () => {
    await kpis();
    const card = region("Ketahanan Dana");
    await waitFor(() => expect(card).toHaveTextContent("Belum bisa dihitung"));
    expect(card.textContent).not.toMatch(/\d+ bulan|999/);
  });

  it("shows the months of Ketahanan Dana in large type with the fund and its target", async () => {
    const base = vi.mocked(api.get).getMockImplementation()!;
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/overview")) return { kpis: { runway_status: "ok", coverage_months: 6.1, runway_months: 6.1, runway_days: 183,
        emergency_fund_balance: 20_412_400, monthly_primary_expense: 3_346_000, target_amount: 19_963_800, coverage_pct: 100, essential_cost: { method: "median" } } };
      return base(path);
    });
    await kpis();
    const card = region("Ketahanan Dana");
    await waitFor(() => expect(card).toHaveTextContent("6,1 bulan biaya pokok"));
    expect(card).toHaveTextContent("Rp 20.412.400 dana darurat");
    expect(card).toHaveTextContent("target Rp 19.963.800");
  });
});

describe("Perlu tindakan", () => {
  it("comes first when the grid is one column, and leads the side column otherwise", async () => {
    const precedes = (first: HTMLElement, second: HTMLElement) =>
      Boolean(first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING);
    const phoneOrder = () => {
      const order = [region(/Perlu tindakan/), region(/^Uang siklus ini/), region(/Aktivitas terbaru/), region(/^Kakeibo siklus ini/), region("Ketahanan Dana")];
      return order.every((card, index) => index === 0 || precedes(order[index - 1], card));
    };

    await kpis();
    expect(phoneOrder()).toBe(false);
    expect(precedes(region(/Aktivitas terbaru/), region(/Perlu tindakan/))).toBe(true);
    cleanup();

    let width = 820;
    class FixedWidthObserver {
      constructor(private readonly callback: ResizeObserverCallback) {}
      observe() {
        this.callback([{ contentRect: { width } } as ResizeObserverEntry], this as unknown as ResizeObserver);
      }
      unobserve() {}
      disconnect() {}
    }
    vi.stubGlobal("ResizeObserver", FixedWidthObserver);
    try {
      await kpis();
      await waitFor(() => expect(phoneOrder()).toBe(true));
      cleanup();

      width = 901;
      await kpis();
      expect(phoneOrder()).toBe(false);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("lists the question first and the plan last, with the waiting amount beside the title", async () => {
    await kpis();
    const card = region(/Perlu tindakan/);
    await waitFor(() => expect(within(card).getAllByRole("listitem")).toHaveLength(4));
    const items = within(card).getAllByRole("listitem");
    expect(items[0]).toHaveTextContent("Belum dikenali: QRIS 0812-xxxx Rp 45.000");
    expect(items[1]).toHaveTextContent("Pasangkan 2 pindah saldo");
    expect(items[2]).toHaveTextContent("Toko Serba Ada: perlu jawaban");
    expect(items[3]).toHaveTextContent("Streamio Rp 65.000 besok");
    expect(within(items[0]).getByRole("link", { name: "Jawab: QRIS 0812-xxxx" })).toHaveAttribute("href", "/review?item=qris#review-questions");
    expect(within(card).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
    expect(card).toHaveTextContent("Rp 581.000 menunggu jawaban");
    expect(within(card).getByRole("heading", { name: /Perlu tindakan.*4 perlu ditinjau/ })).toBeInTheDocument();
  });

  it("says Tinjau is settled and offers the automatic rows of today", async () => {
    reviewQueue = { ...queue, counts: { ...queue.counts, attention: 0 }, questions: [], groups: [], awaiting: [] };
    await kpis();
    const card = region(/Perlu tindakan/);
    await waitFor(() => expect(card).toHaveTextContent("Semua sudah ditinjau"));
    await waitFor(() => expect(card).toHaveTextContent("2 dicatat otomatis hari ini."));
  });
});

describe("Beranda recent activity", () => {
  it("names a top-up once in lavender, a movement once in sky, and a waiting row as waiting, each a whole-row button", async () => {
    await kpis();
    const card = region(/Aktivitas terbaru/);
    await waitFor(() => expect(within(card).getAllByRole("listitem")).toHaveLength(4));
    const topup = within(card).getByRole("button", { name: /^Top up Investasi/ });
    expect(topup.querySelector(".ws-amt-inv")).toHaveTextContent("Rp 250.000");
    expect(topup.querySelector(".ws-ava")).toHaveClass("ws-tone-inv");
    const movement = within(card).getByRole("button", { name: /^Bank Utama → Dompet Digital/ });
    expect(movement.querySelector(".ws-amt-tr")).toHaveTextContent("Rp 500.000");
    expect(movement).toHaveTextContent("Pindah saldo");
    const waiting = within(card).getByRole("button", { name: /^QRIS 0812-xxxx/ });
    expect(waiting).toHaveTextContent("Dompet Digital · menunggu jawaban");
    expect(waiting).toHaveTextContent("−Rp 45.000");
    expect(waiting).toHaveTextContent("2 Nov · 16.05");
  });

  it("opens the shared transaction drawer for a row", async () => {
    const user = userEvent.setup();
    await kpis();
    const card = region(/Aktivitas terbaru/);
    const opener = await within(card).findByRole("button", { name: /^Kopi Pagi/ });
    await user.click(opener);
    const drawer = await screen.findByRole("dialog", { name: /^Kopi Pagi/ });
    expect(await within(drawer).findByText("Diklasifikasikan")).toBeInTheDocument();
    expect(within(drawer).getByRole("button", { name: "Tandai sudah dicek" })).toBeInTheDocument();
    expect(within(drawer).getByRole("button", { name: "Hapus" })).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "Tutup detail" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: /^Kopi Pagi/ })).not.toBeInTheDocument());
    expect(opener).toHaveFocus();
  });

  it("titles movements in the direction the money moved, with each pocket's bank", async () => {
    const base = vi.mocked(api.get).getMockImplementation()!;
    const movement = { type: "expense", amount: 300_000, notes: "", category_name: "Internal Movement", review_status: "reviewed", date: "2026-11-02T01:00:00Z" };
    const rows = [
      { ...movement, id: "pair-out", account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago", movement_id: "p1",
        movement_role: "outbound", partner_id: "pair-in", transfer_target_account_name: "ATM", transfer_target_account_parent_name: "BCA" },
      { ...movement, id: "pair-in", type: "income", account_id: "atm", account_name: "ATM", account_parent_name: "BCA", movement_id: "p1",
        movement_role: "inbound", partner_id: "pair-out", transfer_target_account_name: "Main Pocket", transfer_target_account_parent_name: "Jago" },
      // The sending leg is outside Beranda's window, so only the receiving leg is listed.
      { ...movement, id: "lone-in", type: "income", amount: 500_000, account_id: "tab", account_name: "Tabungan", account_parent_name: "Jago",
        movement_id: "p2", movement_role: "inbound", partner_id: "elsewhere", transfer_target_account_name: "Gopay", transfer_target_account_parent_name: null,
        date: "2026-11-02T01:30:00Z" },
      { ...movement, id: "unpaired", type: "income", amount: 120_000, account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago",
        notes: "Pindah uang dari BCA", date: "2026-11-02T02:00:00Z" },
    ];
    vi.mocked(api.get).mockImplementation(async (path: string) => (path.startsWith("/transactions?") ? { transactions: rows } : base(path)));
    await kpis();
    const card = region(/Aktivitas terbaru/);
    await waitFor(() => expect(within(card).getAllByRole("listitem")).toHaveLength(3));
    expect(within(card).getByRole("button", { name: /^Jago · Main Pocket → BCA · ATM/ })).toBeInTheDocument();
    expect(within(card).getByRole("button", { name: /^Gopay → Jago · Tabungan/ })).toBeInTheDocument();
    expect(within(card).getByRole("button", { name: /^\? → Jago · Main Pocket/ })).toHaveTextContent("Pindah saldo · Pasangan belum ditemukan");
  });

  it("opens Rencana jatuh tempo as a centered dialog, not a side panel", async () => {
    const base = vi.mocked(api.get).getMockImplementation()!;
    const linked = { rule_id: "plan-auto", scheduled_for: "2026-11-01", linked_at: "2026-11-01T03:00:00Z", name: "Streaming Contoh",
      amount: 65_000, type: "expense", transaction_id: "kopi", transaction_date: "2026-11-01T02:00:00Z", account_name: "Dompet Digital" };
    vi.mocked(api.get).mockImplementation(async (path: string) => (
      path === "/recurring/pending" ? { rules: [], recent_auto_links: [linked] } : base(path)
    ));
    const user = userEvent.setup();
    await kpis();
    await user.click(await within(region(/Perlu tindakan/)).findByRole("button", { name: "Cek" }));
    const dialog = await screen.findByRole("dialog", { name: /^Rencana jatuh tempo/ });
    expect(dialog).toHaveAttribute("data-mode", "dialog");
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(document.querySelector(".ws-drawer-slot")).not.toBeInTheDocument();
  });

  it("says a row deleted elsewhere is gone", async () => {
    const user = userEvent.setup();
    await kpis();
    const base = vi.mocked(api.get).getMockImplementation()!;
    const { ApiError } = await import("@/lib/api");
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/transactions/kopi") throw new ApiError(404, "Transaction not found");
      return base(path);
    });
    const card = region(/Aktivitas terbaru/);
    await user.click(await within(card).findByRole("button", { name: /^Kopi Pagi/ }));
    expect(await screen.findByText(/Transaksi ini sudah tidak ada/)).toBeInTheDocument();
  });
});

describe("Beranda loading and failure", () => {
  it("shows placeholders of the final layout while loading", async () => {
    vi.mocked(api.get).mockImplementation(() => new Promise(() => {}));
    renderBeranda();
    expect(document.querySelector('[aria-busy="true"]')).toBeInTheDocument();
  });

  it("says the figures could not be loaded, offers a retry, shows no Rp 0, and keeps the other cards", async () => {
    summary = new Error("boom");
    const user = userEvent.setup();
    renderBeranda();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Angka Beranda belum dapat dimuat.");
    expect(document.body).not.toHaveTextContent("Rp 0");
    expect(screen.queryByRole("region", { name: /Ringkasan/ })).not.toBeInTheDocument();
    expect(region(/Perlu tindakan/)).toBeInTheDocument();
    expect(region(/Aktivitas terbaru/)).toBeInTheDocument();
    summary = previewSummary();
    await user.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    expect(await screen.findByRole("region", { name: "Ringkasan siklus ini" })).toHaveTextContent("Rp 2.364.500");
  });

  it("has no axe violations in the normal state", async () => {
    const { container } = renderBeranda();
    await screen.findByRole("region", { name: "Ringkasan siklus ini" });
    await waitFor(() => expect(within(region(/Aktivitas terbaru/)).getAllByRole("listitem").length).toBe(4));
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
