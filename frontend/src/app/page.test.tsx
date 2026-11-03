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

const comparison = {
  compared_days: 9, previous_label: "Siklus 25 Sep – 24 Okt", previous_has_data: true, current_to_date: { outflow: 2_364_500 },
  categories: { increases: [{ name: "Belanja Campur", change: 312_000 }], decreases: [{ name: "Kopi & Jajan", change: -88_000 }] },
};

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
    if (path.startsWith("/dashboard/period-comparison")) return comparison;
    if (path === "/review/queue") return reviewQueue;
    if (path === "/recurring") return { rules: [plan] };
    if (path === "/recurring/pending") return { rules: [], recent_auto_links: [] };
    if (path.startsWith("/transactions?")) return { transactions: recent };
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

/** The desktop composition; the phone one is rendered next to it and hidden by CSS. */
const desk = () => document.querySelector(".ws-only-desk") as HTMLElement;
const phone = () => document.querySelector(".ws-only-phone") as HTMLElement;
const findLede = async () => (await screen.findByText((_, element) => element?.classList.contains("ws-lede") ?? false)) as HTMLElement;
const lede = async () => {
  renderBeranda();
  return findLede();
};

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
  summary = previewSummary();
  reviewQueue = queue;
  vi.clearAllMocks();
  mockApi();
});
afterEach(() => vi.useRealTimers());

describe("Beranda summary sentences", () => {
  it("contains exactly the three explainable amounts and no other amount", async () => {
    const text = await lede();
    const figures = within(text).getAllByRole("button");
    expect(figures.map((figure) => figure.getAttribute("aria-label"))).toEqual([
      "Keluar siklus ini Rp 3.164.500: kok segini?",
      "Aman dipakai hari ini Rp 214.000: kok segini?",
      "Nunggu jawabanmu Rp 581.000: kok segini?",
    ]);
    const outside = figures.reduce((rest, figure) => rest.replace(figure.textContent ?? "", ""), text.textContent ?? "");
    expect(outside).not.toMatch(/Rp/);
    expect(text).toHaveTextContent("4 hal nunggu jawabanmu (Rp 581.000).");
    expect(text).toHaveTextContent("Hari ke-9 siklus gajimu.");
  });

  it("explains Keluar with the six buckets, the saving note and links filtered by pillar", async () => {
    const user = userEvent.setup();
    await user.click(within(await lede()).getByRole("button", { name: /^Keluar siklus ini/ }));
    const dialog = await screen.findByRole("dialog", { name: "Keluar siklus ini" });
    const rows = within(dialog).getAllByRole("row");
    expect(rows).toHaveLength(7);
    expect(rows[6]).toHaveTextContent("Total keluarRp 3.164.500");
    expect(dialog).toHaveTextContent("Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan");
    expect(dialog).toHaveTextContent("Pindah saldo antar rekeningmu sendiri nggak dihitung keluar.");
    expect(within(dialog).getByRole("link", { name: "Keinginan" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=want");
    expect(document.activeElement).toBe(within(dialog).getByRole("heading", { name: "Keluar siklus ini" }));
  });

  it("explains the safe figure line by line, ending with the figure and the basis of the salary date", async () => {
    const user = userEvent.setup();
    await user.click(within(await lede()).getByRole("button", { name: /^Aman dipakai/ }));
    const dialog = await screen.findByRole("dialog", { name: "Aman dipakai hari ini" });
    const cells = within(dialog).getAllByRole("row").map((row) => row.textContent);
    expect(cells[0]).toBe("Masuk+Rp 9.800.000");
    expect(cells[1]).toBe("Keluar−Rp 3.164.500");
    expect(cells).toContain("Tagihan: Cicilan Laptopjatuh tempo 5 Nov−Rp 600.000");
    expect(cells).toContain("Setoran target: Liburan LombokRp 600.000 per bulan, sudah masuk Rp 0−Rp 600.000");
    expect(cells).toContain("SisaRp 4.922.000");
    expect(cells.some((cell) => cell?.startsWith("÷ 23 hari (2 Nov – 24 Nov)"))).toBe(true);
    expect(cells.at(-1)).toBe("Aman hari iniRp 214.000");
    expect(dialog).toHaveTextContent("Gajian berikutnya diperkirakan 25 November, dari gaji yang kamu konfirmasi tanggal 25 Agu, 25 Sep, 25 Okt.");
  });

  it("explains what is waiting and what is already inside Keluar", async () => {
    const user = userEvent.setup();
    await user.click(within(await lede()).getByRole("button", { name: /^Nunggu jawabanmu/ }));
    const dialog = await screen.findByRole("dialog", { name: "Nunggu jawabanmu" });
    expect(dialog).toHaveTextContent("Apa ini? · sudah di Keluar sebagai Belum diklasifikasi");
    expect(dialog).toHaveTextContent("Pindah saldo belum lengkap · belum masuk Keluar");
    expect(dialog).toHaveTextContent("Rp 357.000 sudah ikut dihitung di Keluar, Rp 224.000 belum.");
    expect(within(dialog).getByRole("link", { name: /Jawab di Tinjau/ })).toHaveAttribute("href", "/review");
  });

  it("says everything is settled, without an amount, when nothing waits", async () => {
    summary = { ...previewSummary(), waiting: { count: 0, total: 0, in_keluar: 0, not_in_keluar: 0, lines: [] } };
    const text = await lede();
    expect(text).toHaveTextContent("Semua transaksi udah beres, nggak ada yang nunggu jawaban.");
    expect(within(text).getAllByRole("button")).toHaveLength(2);
  });

  it("says why the safe figure is missing when the salary date is irregular, and still explains it", async () => {
    const base = previewSummary();
    summary = { ...base,
      expected_salary: { ...base.expected_salary, status: "unknown", date: null, basis: null, reason: "irregular_receipts" },
      safe_to_spend: { status: "unavailable", reason: "no_expected_salary", detail: "irregular_receipts", per_day: null, days_left: null, rounding: 1000, over_by: null },
      scheduled: { ...base.scheduled, window_basis: "next_30_days", window_end: "2026-12-02" } };
    const user = userEvent.setup();
    const text = await lede();
    expect(text).toHaveTextContent("Angka aman per hari belum bisa aku hitung: tanggal gajian belum jelas.");
    expect(text).not.toHaveTextContent("Rp 214.000");
    await user.click(within(text).getByRole("button", { name: "Aman dipakai hari ini belum tersedia: kok segini?" }));
    const dialog = await screen.findByRole("dialog", { name: "Aman dipakai hari ini" });
    expect(dialog).toHaveTextContent("tanggal tiga gaji terakhirmu nggak teratur");
    expect(dialog).toHaveTextContent("SisaRp 4.922.000");
    expect(within(dialog).queryByText("Aman hari ini")).not.toBeInTheDocument();
    expect(within(desk()).getByText("Belum bisa dihitung")).toBeInTheDocument();
  });

  it("says the salary is late without using a zero or negative day count", async () => {
    const base = previewSummary();
    summary = { ...base,
      expected_salary: { ...base.expected_salary, status: "overdue", date: "2026-11-01" },
      safe_to_spend: { status: "unavailable", reason: "salary_overdue", detail: null, per_day: null, days_left: null, rounding: 1000, over_by: null } };
    const text = await lede();
    expect(text).toHaveTextContent("belum bisa aku hitung: gaji baru belum tercatat.");
    expect(desk()).not.toHaveTextContent(/0 hari lagi/);
  });

  it("shows no negative daily amount when the period is over, and explains by how much", async () => {
    const base = previewSummary();
    summary = { ...base, remaining: { ...base.remaining, total: -300_000 },
      safe_to_spend: { ...base.safe_to_spend, per_day: 0, days_left: 10, over_by: 300_000 } };
    const user = userEvent.setup();
    const text = await lede();
    expect(text).toHaveTextContent("Hari ini belum ada yang aman dipakai");
    expect(desk()).toHaveTextContent("Siklus ini sudah lebih Rp 300.000 dari uang masuk.");
    await user.click(within(text).getByRole("button", { name: /^Aman dipakai/ }));
    expect(await screen.findByRole("dialog", { name: "Aman dipakai hari ini" })).toHaveTextContent("sudah lebih Rp 300.000 dari uang masuk");
  });
});

describe("Beranda period and header", () => {
  it("names the cycle day and start date", () => {
    expect(headerFor(previewSummary(), NOW)).toEqual({
      title: "Senin, 2 November",
      subtitle: "Siklus gaji hari ke-9 · sejak 25 Okt · gajian berikutnya ± 25 Nov",
    });
  });

  it("falls back to the calendar month without a cycle and links to the salary settings", async () => {
    const base = previewSummary();
    const month: CycleSummary = { ...base, period: { ...base.period, basis: "month", cycle_status: "not_configured", start_date: "2026-11-01", day_index: 2,
      cycle_offset_basis: "month", label: "November 2026" },
      safe_to_spend: { ...base.safe_to_spend, status: "unavailable", reason: "no_cycle", per_day: null, days_left: null } };
    summary = month;
    expect(headerFor(month, NOW).subtitle).toBe("November · siklus gaji belum tersedia");
    const text = await lede();
    expect(text).toHaveTextContent("Hari ke-2 bulan November. Bulan ini kamu keluar");
    expect(within(desk()).getByRole("link", { name: "Atur gaji di Pengaturan" })).toHaveAttribute("href", "/settings/gaji");
  });
});

describe("Beranda KPI strip and cards", () => {
  it("shows four figures, signed-amount chips and no percentage", async () => {
    await lede();
    const strip = within(desk()).getByRole("region", { name: "Ringkasan siklus ini" });
    expect(strip).toHaveTextContent("Rp 214.000");
    expect(strip).toHaveTextContent("23 hari lagi sampai gajian");
    expect(strip).toHaveTextContent("+Rp 154.500");
    expect(strip).toHaveTextContent("+Rp 300.000");
    expect(strip).toHaveTextContent("Rp 46.330.000");
    expect(strip.textContent).not.toMatch(/%/);
  });

  it("omits the chips before three compared days", async () => {
    const base = previewSummary();
    summary = { ...base, comparison: { ...base.comparison, status: "too_short", compared_days: 2,
      keluar: { current: 1, previous: null, change: null }, masuk: { current: 1, previous: null, change: null }, belanja: { current: 1, previous: null, change: null } } };
    await lede();
    const strip = within(desk()).getByRole("region", { name: "Ringkasan siklus ini" });
    expect(strip).not.toHaveTextContent("+Rp");
    expect(strip).toHaveTextContent("perbandingan muncul setelah 3 hari");
    expect(within(desk()).getByRole("region", { name: "Wawasan periode ini" })).toHaveTextContent("Perbandingan dengan periode lalu muncul setelah 3 hari.");
  });

  it("splits Uang siklus ini into three labelled amounts that add up to Masuk, with the bar hidden", async () => {
    await lede();
    const card = within(desk()).getByRole("region", { name: "Uang siklus ini" });
    const list = within(card).getByRole("list", { name: "Pembagian uang siklus ini" });
    expect(within(list).getAllByRole("listitem").map((item) => item.querySelector(".ws-legend-v")?.textContent)).toEqual([
      "Rp 3.164.500", "Rp 1.713.500", "Rp 4.922.000",
    ]);
    expect(card.querySelector(".ws-moneybar")).toHaveAttribute("aria-hidden", "true");
    expect(card).toHaveTextContent("3 tagihan + 2 target");
  });

  it("lists Kakeibo's six buckets with the total equal to Keluar and ledger links", async () => {
    await lede();
    const card = within(desk()).getByRole("region", { name: "Kakeibo siklus ini" });
    const items = within(card).getAllByRole("listitem");
    expect(items).toHaveLength(6);
    expect(items[5]).toHaveTextContent("Belum diklasifikasiRp 00%");
    expect(within(card).getByRole("link", { name: "Belum diklasifikasi" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=unclassified");
    expect(within(card).getByRole("button", { name: "Keluar siklus ini Rp 3.164.500: kok segini?" })).toBeInTheDocument();
  });

  it("keeps an unknown Ketahanan Dana unknown, without a sentinel", async () => {
    await lede();
    const card = within(desk()).getByRole("region", { name: "Ketahanan Dana" });
    await waitFor(() => expect(card).toHaveTextContent("Belum bisa dihitung"));
    expect(card.textContent).not.toMatch(/\d+ bulan|999/);
  });

  it("words the insight as belanja with amounts and no percentage", async () => {
    await lede();
    const insight = within(desk()).getByRole("region", { name: "Wawasan periode ini" });
    await waitFor(() => expect(insight).toHaveTextContent("Naik paling besar: Belanja Campur +Rp 312.000."));
    expect(insight).toHaveTextContent("Belanja 9 hari pertama Rp 45.500 lebih hemat dari periode lalu di hari yang sama.");
    expect(insight.textContent).not.toMatch(/%|Keluar/);
  });
});

describe("Kerjakan dulu", () => {
  it("follows Uang siklus ini when the grid is one column, and leads the side column otherwise", async () => {
    const region = (name: string | RegExp) => within(desk()).getByRole("region", { name });
    const precedes = (first: HTMLElement, second: HTMLElement) =>
      Boolean(first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING);
    const todoFollowsMoney = () => {
      const money = region("Uang siklus ini");
      const todo = region(/Kerjakan dulu/);
      const activity = region(/Aktivitas terbaru/);
      return precedes(money, todo) && precedes(todo, activity);
    };

    await lede();
    expect(todoFollowsMoney()).toBe(false);
    expect(precedes(region(/Aktivitas terbaru/), region(/Kerjakan dulu/))).toBe(true);
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
      await lede();
      await waitFor(() => expect(todoFollowsMoney()).toBe(true));
      cleanup();

      width = 901;
      await lede();
      expect(todoFollowsMoney()).toBe(false);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("lists the question first and the plan last, each opening its place", async () => {
    await lede();
    const card = within(desk()).getByRole("region", { name: /Kerjakan dulu/ });
    await waitFor(() => expect(within(card).getAllByRole("listitem")).toHaveLength(4));
    const items = within(card).getAllByRole("listitem");
    expect(items[0]).toHaveTextContent("Apa ini? QRIS 0812-xxxx Rp 45.000");
    expect(items[1]).toHaveTextContent("Pasangkan 2 pindah saldo");
    expect(items[2]).toHaveTextContent("Toko Serba Ada: perlu jawaban");
    expect(items[3]).toHaveTextContent("Streamio Rp 65.000 besok");
    expect(within(items[0]).getByRole("link", { name: "Jawab: QRIS 0812-xxxx" })).toHaveAttribute("href", "/review?item=qris#review-questions");
    expect(within(card).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
    expect(within(card).getByRole("heading", { name: /Kerjakan dulu.*4 perlu ditinjau/ })).toBeInTheDocument();
  });

  it("says Tinjau is settled and offers the automatic rows of today", async () => {
    reviewQueue = { ...queue, counts: { ...queue.counts, attention: 0 }, questions: [], groups: [], awaiting: [] };
    await lede();
    const card = within(desk()).getByRole("region", { name: /Kerjakan dulu/ });
    await waitFor(() => expect(card).toHaveTextContent("Tinjau udah beres"));
    await waitFor(() => expect(card).toHaveTextContent("2 dicatat otomatis hari ini."));
    expect(within(card).getByRole("link", { name: "cek kalau mau" })).toHaveAttribute("href", "/ledger?review=auto");
  });
});

describe("Beranda recent activity", () => {
  it("names a top-up once in lavender, a movement once in sky, and a waiting row as waiting", async () => {
    await lede();
    const card = within(desk()).getByRole("region", { name: /Aktivitas terbaru/ });
    await waitFor(() => expect(within(card).getAllByRole("row")).toHaveLength(5));
    const topup = within(card).getByRole("button", { name: "Top up Investasi" }).closest("tr")!;
    expect(topup.querySelector(".ws-amt-inv")).toHaveTextContent("Rp 250.000");
    expect(topup.querySelector(".ws-ava")).toHaveClass("ws-tone-inv");
    const movement = within(card).getByRole("button", { name: "Bank Utama → Dompet Digital" }).closest("tr")!;
    expect(movement.querySelector(".ws-amt-tr")).toHaveTextContent("Rp 500.000");
    expect(movement).toHaveTextContent("Nggak dihitung");
    const waiting = within(card).getByRole("button", { name: "QRIS 0812-xxxx" }).closest("tr")!;
    expect(waiting).toHaveTextContent("Nunggu jawaban");
    expect(waiting).toHaveTextContent("−Rp 45.000");
  });

  it("opens the existing detail dialog for a row", async () => {
    const user = userEvent.setup();
    await lede();
    const card = within(desk()).getByRole("region", { name: /Aktivitas terbaru/ });
    await user.click(await within(card).findByRole("button", { name: "Top up Investasi" }));
    const dialog = await screen.findByRole("dialog", { name: "Detail Transaksi" });
    expect(dialog).toHaveTextContent("Top up Investasi");
  });
});

describe("Beranda on phones", () => {
  it("shows the same figures as the desktop layout, safe-to-spend first", async () => {
    await lede();
    const hero = within(phone()).getByRole("region", { name: "Aman dipakai hari ini" });
    expect(phone().querySelector("section")).toBe(hero);
    const name = (scope: HTMLElement, label: RegExp) => within(scope).getAllByRole("button", { name: label })[0].getAttribute("aria-label");
    for (const label of [/^Aman dipakai hari ini/, /^Keluar siklus ini/, /^Masuk siklus ini/, /^Nunggu jawabanmu/, /^Sisa aman/]) {
      expect(name(phone(), label)).toBe(name(desk(), label));
    }
    const headings = within(phone()).getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent);
    expect(headings).toEqual(["Aman dipakai hari ini", "Kerjakan dulu", "Siklus ini", "Hari ini"]);
  });
});

describe("Beranda loading and failure", () => {
  it("shows placeholders of the final layout while loading", async () => {
    vi.mocked(api.get).mockImplementation(() => new Promise(() => {}));
    renderBeranda();
    expect(desk().querySelector('[aria-busy="true"]')).toBeInTheDocument();
  });

  it("says the figures could not be loaded, offers a retry, shows no Rp 0, and keeps the other cards", async () => {
    summary = new Error("boom");
    const user = userEvent.setup();
    renderBeranda();
    const alert = await within(desk()).findByRole("alert");
    expect(alert).toHaveTextContent("Angka Beranda belum bisa dimuat.");
    expect(desk()).not.toHaveTextContent("Rp 0");
    expect(within(desk()).queryByRole("region", { name: /Ringkasan/ })).not.toBeInTheDocument();
    expect(within(desk()).getByRole("region", { name: /Kerjakan dulu/ })).toBeInTheDocument();
    expect(within(desk()).getByRole("region", { name: /Aktivitas terbaru/ })).toBeInTheDocument();
    summary = previewSummary();
    await user.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    expect(await findLede()).toHaveTextContent("Rp 3.164.500");
  });

  it("has no axe violations in the normal state", async () => {
    // Without the stylesheet both compositions would be visible; the browser shows one per width.
    const style = document.createElement("style");
    style.textContent = ".ws-only-phone { display: none; }";
    document.head.appendChild(style);
    const { container } = renderBeranda();
    await findLede();
    await waitFor(() => expect(within(desk()).getAllByRole("listitem").length).toBeGreaterThan(5));
    expect((await axe(container)).violations).toHaveLength(0);
    style.remove();
  });
});
