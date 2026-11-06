import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { headerFor } from "@/components/beranda/BerandaView";
import type { CycleSummary } from "@/components/beranda/types";
import { ShortcutProvider } from "@/components/workspace/shortcuts";
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
// "Sembunyikan saldo" for the hidden-balance cases; the real context otherwise.
const display = vi.hoisted(() => ({ hidden: false }));
vi.mock("@/components/layout/AppLayout", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/components/layout/AppLayout")>();
  return {
    ...actual,
    useAppCtx: () => {
      const context = actual.useAppCtx();
      return display.hidden ? { ...context, hideBalances: true, bal: () => "Rp ••••••" } : context;
    },
  };
});

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
let plans: (typeof plan)[] = [plan];
let overview: unknown = { kpis: { runway_status: "unknown", coverage_months: null, runway_months: null, runway_days: null, emergency_fund_balance: 0,
  monthly_primary_expense: null, essential_cost: { method: "unknown" } } };
let activityRows: unknown[] = recent;

function mockApi() {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path === "/dashboard/cycle-summary") {
      if (summary instanceof Error) throw summary;
      return summary;
    }
    if (path.startsWith("/dashboard/overview")) {
      if (overview instanceof Error) throw overview;
      return overview;
    }
    if (path === "/review/queue") {
      if (reviewQueue instanceof Error) throw reviewQueue;
      return reviewQueue;
    }
    if (path === "/recurring") return { rules: plans };
    if (path === "/recurring/pending") return { rules: [], recent_auto_links: [] };
    if (path.startsWith("/transactions?")) return { transactions: activityRows };
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
      <ShortcutProvider>
        <BerandaPage />
      </ShortcutProvider>
    </QueryClientProvider>,
  );
}

/** Beranda once its figures and lists are in. */
async function beranda() {
  const view = renderBeranda();
  const hero = await screen.findByRole("region", { name: "Aman dipakai sampai gajian" });
  await screen.findByRole("region", { name: "Aktivitas terbaru" });
  await screen.findByRole("region", { name: "Perlu tindakan" });
  return { ...view, hero };
}
const region = (name: string) => screen.getByRole("region", { name });
const panel = () => screen.getByRole("complementary", { name: "Ringkasan simpanan" });
const figureOf = (hero: HTMLElement, label: string) =>
  [...hero.querySelectorAll(".fig-block")].find((block) => block.querySelector(".fb-label")?.textContent === label) as HTMLElement;
const rowTexts = (scope: HTMLElement) => [...scope.querySelectorAll(".list > li > .row")].map((row) => row.textContent);

async function openRincian(hero: HTMLElement) {
  const user = userEvent.setup();
  await user.click(within(hero).getByRole("button", { name: "Rincian aman dipakai sampai gajian" }));
  return { user, dialog: await screen.findByRole("dialog", { name: "Aman dipakai sampai gajian" }) };
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
  summary = previewSummary();
  reviewQueue = queue;
  plans = [plan];
  overview = { kpis: { runway_status: "unknown", coverage_months: null, runway_months: null, runway_days: null, emergency_fund_balance: 0,
    monthly_primary_expense: null, essential_cost: { method: "unknown" } } };
  activityRows = recent;
  display.hidden = false;
  vi.clearAllMocks();
  mockApi();
});
afterEach(() => vi.useRealTimers());

describe("Beranda period", () => {
  it("names the cycle's span to the day before the expected salary, and its day", () => {
    expect(headerFor(previewSummary(), NOW)).toEqual({ title: "Senin, 2 November", subtitle: "Siklus gaji 25 Okt – 24 Nov · hari ke-9" });
  });

  it("names only the cycle's start when the expected salary date is unknown", () => {
    const base = previewSummary();
    const unknown = { ...base, scheduled: { ...base.scheduled, window_basis: "next_30_days" as const, window_end: "2026-12-01" } };
    expect(headerFor(unknown, NOW).subtitle).toBe("Siklus gaji sejak 25 Okt · hari ke-9");
  });

  it("falls back to the calendar month without a cycle, links to the salary settings and adds no heading of its own", async () => {
    const base = previewSummary();
    const month: CycleSummary = { ...base, period: { ...base.period, basis: "month", cycle_status: "not_configured", start_date: "2026-11-01", day_index: 2,
      cycle_offset_basis: "month", label: "November 2026" },
      safe_to_spend: { ...base.safe_to_spend, status: "unavailable", reason: "no_cycle", per_day: null, days_left: null } };
    summary = month;
    expect(headerFor(month, NOW).subtitle).toBe("November · siklus gaji belum tersedia");
    const { hero } = await beranda();
    expect(screen.getByRole("link", { name: "Atur gaji di Pengaturan" })).toHaveAttribute("href", "/settings/gaji");
    expect(figureOf(hero, "Pengeluaran")).toHaveTextContent("sejak 1 November");
    expect(figureOf(hero, "Masuk")).toHaveTextContent("sejak 1 November");
    expect(hero.querySelector(".hero-sub")).toHaveTextContent("Siklus gaji belum ada");
    // The shell's title block holds the page's one h1.
    expect(document.querySelectorAll("h1")).toHaveLength(0);
  });
});

describe("Beranda hero", () => {
  it("shows the safe amount once at 44 px with its line, the split bar and four supporting figures, each a server field", async () => {
    const { hero } = await beranda();
    expect(hero.querySelector(".t-hero")).toHaveTextContent("Rp 4.922.000");
    expect(hero.querySelector(".hero-sub")).toHaveTextContent(/^23 hari lagi sampai gajian 25 Nov$/);
    expect(document.body).not.toHaveTextContent(/per hari|Rp 214\.000/);
    expect(figureOf(hero, "Masuk")).toHaveTextContent("MasukRp 9.800.000Gaji 25 Okt");
    expect(figureOf(hero, "Pengeluaran")).toHaveTextContent("PengeluaranRp 2.364.500sejak gaji 25 Okt");
    expect(figureOf(hero, "Ditabung & investasi")).toHaveTextContent("Ditabung & investasiRp 800.000Tabungan bertambah");
    expect(figureOf(hero, "Terjadwal")).toHaveTextContent("TerjadwalRp 1.713.500tagihan & setoran sampai 24 Nov");
    expect(figureOf(hero, "Masuk")).toHaveClass("is-phone-hide");
    expect(figureOf(hero, "Terjadwal")).toHaveClass("is-phone-hide");
    expect(within(hero).getByRole("img")).toHaveAccessibleName(
      "Uang siklus ini dari Rp 9.800.000 masuk: Pengeluaran 24%, Ditabung & investasi 8%, Terjadwal 17%, Aman dipakai 50%",
    );
    expect(document.querySelectorAll(".t-hero")).toHaveLength(1);
  });

  it("has exactly one Rincian and no dotted figure", async () => {
    await beranda();
    const rincian = screen.getAllByRole("button").filter((button) => button.textContent === "Rincian");
    expect(rincian).toHaveLength(1);
    expect(rincian[0]).toHaveAccessibleName("Rincian aman dipakai sampai gajian");
    expect(rincian[0]).toHaveAttribute("aria-haspopup", "dialog");
    expect(document.querySelector(".ws-xn")).toBeNull();
    expect(screen.queryByRole("button", { name: /lihat rincian/ })).not.toBeInTheDocument();
  });

  it("says only 'tagihan' when no goal set-aside is scheduled", async () => {
    const base = previewSummary();
    summary = { ...base, scheduled: { ...base.scheduled, lines: base.scheduled.lines.filter((line) => line.kind === "plan") } };
    const { hero } = await beranda();
    expect(figureOf(hero, "Terjadwal")).toHaveTextContent("tagihan sampai 24 Nov");
  });

  const unavailable = (detail: "irregular_receipts" | "no_salary_source", reason: "no_expected_salary" = "no_expected_salary") => {
    const base = previewSummary();
    return { ...base,
      expected_salary: { ...base.expected_salary, status: "unknown" as const, date: null, basis: null, reason: detail },
      safe_to_spend: { ...base.safe_to_spend, status: "unavailable" as const, reason, detail, per_day: null, days_left: null } };
  };
  it.each([
    ["the next salary date is unknown", () => unavailable("irregular_receipts"), "Tanggal gaji berikutnya belum diketahui"],
    ["no salary source is set", () => unavailable("no_salary_source"), "Sumber gaji belum diatur"],
    ["the salary is late", () => {
      const base = previewSummary();
      return { ...base, expected_salary: { ...base.expected_salary, status: "overdue" as const, date: "2026-11-01" },
        safe_to_spend: { ...base.safe_to_spend, status: "unavailable" as const, reason: "salary_overdue" as const, per_day: null, days_left: null } };
    }, "Gaji baru belum tercatat"],
  ])("keeps the period amount and says why when %s", async (_case, make, line) => {
    summary = make();
    const { hero } = await beranda();
    expect(hero.querySelector(".t-hero")).toHaveTextContent("Rp 4.922.000");
    expect(hero.querySelector(".hero-sub")).toHaveTextContent(new RegExp(`^${line}$`));
    expect(hero.querySelector(".hero-sub")!.textContent).not.toMatch(/\d+ hari/);
  });

  it("shows Rp 0 and by how much the period is over, never a negative safe amount", async () => {
    const base = previewSummary();
    summary = { ...base, remaining: { ...base.remaining, total: -300_000 }, safe_to_spend: { ...base.safe_to_spend, per_day: 0, over_by: 300_000 } };
    const { hero } = await beranda();
    expect(hero.querySelector(".t-hero")).toHaveTextContent(/^Rp 0$/);
    expect(hero.querySelector(".hero-sub")).toHaveTextContent("Siklus ini sudah lebih Rp 300.000 dari uang masuk");
    expect(hero).not.toHaveTextContent("−Rp 300.000");
    const { dialog } = await openRincian(hero);
    expect(dialog).toHaveTextContent("Periode ini sudah lebih Rp 300.000 dari uang masuk, jadi belum ada yang aman dipakai.");
  });

  it("lists the Rincian as today's calculation: Masuk less Pengeluaran, Ditabung and Terjadwal, and its total", async () => {
    const { hero } = await beranda();
    const { dialog } = await openRincian(hero);
    expect(dialog).toHaveTextContent("Rincian hitungan");
    expect(dialog).toHaveTextContent("Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir siklus.");
    expect(rowTexts(dialog)).toEqual([
      "MasukRp 9.800.000", "Pengeluaran−Rp 2.364.500", "Ditabung & investasi−Rp 800.000", "Terjadwal sampai 24 Nov−Rp 1.713.500",
      "Aman dipakaiRp 4.922.000", "Menunggu jawaban · Rp 581.000",
    ]);
    expect(dialog).toHaveTextContent("Gaji berikutnya diperkirakan 25 November, dari gaji yang dikonfirmasi tanggal 25 Agu, 25 Sep, 25 Okt.");
    expect(dialog).toHaveTextContent("4 hal (Rp 581.000) masih menunggu jawaban; Rp 357.000 di antaranya sudah termasuk.");
    expect(within(dialog).getByRole("link", { name: "Lihat transaksi siklus ini" })).toHaveAttribute("href", "/ledger?period=cycle");
  });

  it("opens each line's server lines in place, and the waiting rows by section and bucket", async () => {
    const { hero } = await beranda();
    const { user, dialog } = await openRincian(hero);
    const parts = async (name: RegExp) => {
      const line = within(dialog).getByRole("button", { name });
      await user.click(line);
      expect(line).toHaveAttribute("aria-expanded", "true");
      return [...document.getElementById(line.getAttribute("aria-controls")!)!.querySelectorAll(".part")].map((part) => part.textContent);
    };
    expect(await parts(/^Masuk/)).toEqual(["GajiRp 9.500.000", "RefundRp 300.000"]);
    expect(await parts(/^Pengeluaran/)).toEqual([
      "KebutuhanRp 1.420.000", "KeinginanRp 610.000", "BudayaRp 245.000", "Tak TerdugaRp 89.500", "Belum diklasifikasiRp 0",
    ]);
    expect(within(dialog).getByRole("link", { name: "Keinginan" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=want");
    expect(await parts(/^Ditabung/)).toEqual(["TabunganTermasuk uang yang dipindah ke kantong tabungan atau diinvestasikanRp 800.000"]);
    expect(await parts(/^Terjadwal/)).toEqual([
      "Tagihan: Streamiojatuh tempo 3 NovRp 65.000", "Tagihan: Cicilan Laptopjatuh tempo 5 NovRp 600.000",
      "Tagihan: Internet Rumahjatuh tempo 10 NovRp 330.000",
      "Setoran target: Liburan LombokRp 600.000 per bulan, sudah masuk Rp 0Rp 600.000",
      "Setoran target: Dana DaruratRp 118.500 per bulan, sudah masuk Rp 0Rp 118.500",
    ]);
    expect(dialog).toHaveTextContent("Sampai sehari sebelum perkiraan gaji berikutnya (24 Nov).");
    expect(await parts(/^Menunggu jawaban/)).toEqual([
      "QRIS 0812-xxxxBelum dikenali · sudah dihitung di Pengeluaran sebagai Belum diklasifikasiRp 45.000",
      "RDN Contoh keluarPindah saldo belum lengkap · belum dihitungRp 112.000",
      "RDN Contoh keluarPindah saldo belum lengkap · belum dihitungRp 112.000",
      "Toko Serba AdaPerlu jawaban · sudah dihitung di Pengeluaran sebagai Belum diklasifikasiRp 312.000",
    ]);
    expect(within(dialog).getByRole("link", { name: "Jawab di Tinjau" })).toHaveAttribute("href", "/review");
  });

  it("names a savings decrease in words, takes no bar width for it and opens what went in, came out and was spent", async () => {
    const base = previewSummary();
    summary = {
      ...base,
      fresh_saving: { ...base.fresh_saving, amount: -500_000 },
      keluar: { ...base.keluar, total: base.spending.amount - 500_000 },
      tabungan: { total: -500_000, masuk: 300_000, diambil: 600_000, belanja: 200_000, pilar_tabungan: 0 },
    };
    const { hero } = await beranda();
    expect(figureOf(hero, "Ditabung & investasi")).toHaveTextContent("Ditabung & investasiRp 500.000Tabungan berkurang");
    expect(hero.querySelector('.split [data-segment="saving"]')).toBeNull();
    expect(within(hero).getByRole("img")).toHaveAccessibleName(/Ditabung & investasi 0%/);
    const { user, dialog } = await openRincian(hero);
    expect(rowTexts(dialog)[2]).toBe("Tabungan berkurangRp 500.000");
    const line = within(dialog).getByRole("button", { name: /^Tabungan berkurang/ });
    await user.click(line);
    expect([...document.getElementById(line.getAttribute("aria-controls")!)!.querySelectorAll(".part")].map((part) => part.textContent)).toEqual([
      "Masuk ke tabungan+Rp 300.000", "Diambil dari tabungan−Rp 600.000", "Pengeluaran dari tabungantetap dihitung sebagai pengeluaran−Rp 200.000",
    ]);
  });
});

describe("Perlu tindakan", () => {
  it("lists at most three whole-row items: the question, the awaiting transfers, then the plan, with Buka Tinjau and the count", async () => {
    await beranda();
    const section = region("Perlu tindakan");
    await waitFor(() => expect(rowTexts(section)).toHaveLength(3));
    expect(rowTexts(section)).toEqual([
      "QRIS 0812-xxxxBelum dikenali · hari ini 16.05−Rp 45.000",
      "Pasangkan 2 pindah saldoPindah saldo belum lengkap · RDN Contoh",
      "StreamioBesok · RencanaRp 65.000",
    ]);
    const rows = within(section).getAllByRole("link").filter((link) => link.classList.contains("row"));
    expect(rows.map((row) => row.getAttribute("href"))).toEqual(["/review?item=qris#review-questions", "/review?item=rdn-1#review-awaiting", "/goals"]);
    expect(within(section).getByRole("link", { name: "Buka Tinjau · 4" })).toHaveAttribute("href", "/review");
    expect(section).not.toHaveTextContent(/menunggu jawaban|dicatat otomatis|Jawab|Pasangkan$|Selesai/);
    expect(section.querySelector(".ws-act, .ws-pill")).toBeNull();
    expect(section.querySelectorAll(".r-ico svg[aria-hidden=true]")).toHaveLength(3);
  });

  it("fills free places with review items when fewer plans are due", async () => {
    plans = [];
    await beranda();
    const section = region("Perlu tindakan");
    await waitFor(() => expect(rowTexts(section)).toHaveLength(3));
    expect(rowTexts(section)[2]).toBe("Toko Serba AdaPerlu jawaban · 1 transaksi · 1 Nov 19.20−Rp 312.000");
  });

  it("opens Rencana jatuh tempo from a plan due today, as a centered dialog", async () => {
    plans = [{ ...plan, next_due_date: "2026-11-02" }];
    reviewQueue = { ...queue, counts: { ...queue.counts, attention: 0 }, questions: [], groups: [], awaiting: [] };
    const user = userEvent.setup();
    await beranda();
    const section = region("Perlu tindakan");
    const row = await within(section).findByRole("button", { name: /^Streamio/ });
    expect(row).toHaveTextContent("StreamioJatuh tempo hari ini · RencanaRp 65.000");
    expect(row.querySelector(".c-warning")).toHaveTextContent("Jatuh tempo hari ini");
    await user.click(row);
    const dialog = await screen.findByRole("dialog", { name: /^Rencana jatuh tempo/ });
    expect(dialog).toHaveAttribute("data-mode", "dialog");
    expect(dialog).toHaveAttribute("aria-modal", "true");
  });

  it("says nothing waits in one line, with Buka Tinjau without a count", async () => {
    plans = [];
    reviewQueue = { ...queue, counts: { ...queue.counts, attention: 0 }, questions: [], groups: [], awaiting: [] };
    await beranda();
    const section = region("Perlu tindakan");
    expect(await within(section).findByText("Tidak ada yang perlu dijawab.")).toHaveClass("t-body");
    expect(within(section).getByRole("link", { name: "Buka Tinjau" })).toHaveAttribute("href", "/review");
    expect(section.querySelector(".list")).toBeNull();
  });

  it("says when the Tinjau list cannot be loaded", async () => {
    plans = [];
    reviewQueue = new Error("boom") as unknown as typeof queue;
    await beranda();
    expect(await within(region("Perlu tindakan")).findByText("Daftar Tinjau belum bisa dimuat.")).toBeInTheDocument();
  });
});

describe("Aktivitas terbaru", () => {
  it("lists today's answered rows under a day heading, one line each, with the time, status and signed amount", async () => {
    await beranda();
    const section = region("Aktivitas terbaru");
    await waitFor(() => expect(rowTexts(section)).toHaveLength(3));
    expect(within(section).getByRole("heading", { level: 3, name: "Senin, 2 November · hari ini" })).toBeInTheDocument();
    expect(section.querySelector(".sec-h .aside")).toHaveTextContent("hari ini dan kemarin · 3");
    // The waiting QRIS row is in Perlu tindakan, not here.
    expect(section).not.toHaveTextContent("QRIS");
    const topup = within(section).getByRole("button", { name: /Top up Investasi/ });
    expect(topup.querySelector(".r-amt .t-amt")).toHaveClass("c-invest");
    expect(topup.querySelector(".r-amt")).toHaveTextContent(/^Rp 250\.000$/);
    expect(topup.querySelector(".r-pillar")).toHaveTextContent("Tabungan");
    expect(topup.querySelector(".ws-ava")).toBeNull();
    const movement = within(section).getByRole("button", { name: /Bank Utama → Dompet Digital/ });
    expect(movement.querySelector(".r-amt .t-amt")).toHaveClass("c-transfer");
    expect(movement.querySelector(".r-pillar")).toHaveTextContent("Tidak dihitung");
    expect(movement.querySelector(".ph-only")).toHaveTextContent("09.12 · Pindah saldo");
    const kopi = within(section).getByRole("button", { name: /Kopi Pagi/ });
    expect(kopi.querySelector(".r-time")).toHaveTextContent("07.45");
    expect(kopi.querySelector(".r-meta.dt-only")).toHaveTextContent("Dompet Digital");
    expect(kopi.querySelector(".ph-only")).toHaveTextContent("07.45 · Keinginan");
    expect(kopi.querySelector(".r-pillar")).toHaveTextContent("Keinginan");
    const amount = kopi.querySelector(".r-amt .t-amt")!;
    expect(amount).toHaveTextContent("−Rp 32.000");
    expect(amount.className).toBe("t-amt");
    expect(within(section).getByRole("link", { name: "Semua transaksi" })).toHaveAttribute("href", "/ledger");
  });

  it("puts yesterday's rows under their own heading and keeps eight rows at most", async () => {
    const yesterday = Array.from({ length: 9 }, (_, index) => ({
      id: `y${index}`, type: "expense", amount: 10_000 + index, notes: null, merchant: `Warung ${index}`, account_id: "dw", account_name: "Pocket",
      account_parent_name: "Jago", date: `2026-11-01T1${index}:00:00Z`, kakeibo_type: "need", review_status: "reviewed",
    }));
    activityRows = [...recent, ...yesterday.reverse()];
    await beranda();
    const section = region("Aktivitas terbaru");
    await waitFor(() => expect(rowTexts(section)).toHaveLength(8));
    expect(within(section).getAllByRole("heading", { level: 3 }).map((heading) => heading.textContent)).toEqual([
      "Senin, 2 November · hari ini", "Minggu, 1 November · kemarin",
    ]);
    expect(within(section).getByRole("button", { name: /Warung 8/ }).querySelector(".r-meta.dt-only")).toHaveTextContent("Jago · Pocket");
  });

  it("opens the shared transaction dialog for a row and returns focus to it", async () => {
    const user = userEvent.setup();
    await beranda();
    const opener = await within(region("Aktivitas terbaru")).findByRole("button", { name: /Kopi Pagi/ });
    await user.click(opener);
    const drawer = await screen.findByRole("dialog", { name: /^Kopi Pagi/ });
    expect(await within(drawer).findByText("Diklasifikasikan")).toBeInTheDocument();
    expect(within(drawer).getByRole("button", { name: "Tandai sudah dicek" })).toBeInTheDocument();
    expect(within(drawer).getByRole("button", { name: "Hapus" })).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "Tutup detail" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: /^Kopi Pagi/ })).not.toBeInTheDocument());
    expect(opener).toHaveFocus();
  });

  it("says a row deleted elsewhere is gone", async () => {
    const user = userEvent.setup();
    await beranda();
    const base = vi.mocked(api.get).getMockImplementation()!;
    const { ApiError } = await import("@/lib/api");
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/transactions/kopi") throw new ApiError(404, "Transaction not found");
      return base(path);
    });
    await user.click(await within(region("Aktivitas terbaru")).findByRole("button", { name: /Kopi Pagi/ }));
    expect(await screen.findByText(/Transaksi ini sudah tidak ada/)).toBeInTheDocument();
  });

  it("titles movements in the direction the money moved, with each pocket's bank, and marks an unpaired leg", async () => {
    const movement = { type: "expense", amount: 300_000, notes: "", category_name: "Internal Movement", review_status: "reviewed", date: "2026-11-02T01:00:00Z" };
    activityRows = [
      { ...movement, id: "pair-out", account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago", movement_id: "p1",
        movement_role: "outbound", partner_id: "pair-in", transfer_target_account_name: "ATM", transfer_target_account_parent_name: "BCA" },
      { ...movement, id: "pair-in", type: "income", account_id: "atm", account_name: "ATM", account_parent_name: "BCA", movement_id: "p1",
        movement_role: "inbound", partner_id: "pair-out", transfer_target_account_name: "Main Pocket", transfer_target_account_parent_name: "Jago" },
      { ...movement, id: "unpaired", type: "income", amount: 120_000, account_id: "main", account_name: "Main Pocket", account_parent_name: "Jago",
        notes: "Pindah uang dari BCA", date: "2026-11-02T02:00:00Z" },
    ];
    await beranda();
    const section = region("Aktivitas terbaru");
    await waitFor(() => expect(rowTexts(section)).toHaveLength(2));
    expect(within(section).getByRole("button", { name: /Jago · Main Pocket → BCA · ATM/ })).toBeInTheDocument();
    const unpaired = within(section).getByRole("button", { name: /\? → Jago · Main Pocket/ });
    expect(unpaired.querySelector(".r-meta.dt-only .c-warning")).toHaveTextContent("Pasangan belum ditemukan");
    expect(unpaired.querySelector(".ph-only .c-warning")).toHaveTextContent("Pasangan belum ditemukan");
  });

  it("says when there is no activity", async () => {
    activityRows = [];
    await beranda();
    expect(await within(region("Aktivitas terbaru")).findByText("Belum ada transaksi hari ini dan kemarin.")).toBeInTheDocument();
  });
});

describe("Beranda context panel", () => {
  it("shows Kekayaan bersih with aset and utang, credit owed included, as one link to Rekening & Kantong", async () => {
    const base = previewSummary();
    summary = { ...base, net_worth: { ...base.net_worth, total: 45_860_000, credit_owed: 470_000, dana_investasi: 1_200_000 } };
    await beranda();
    const net = within(panel()).getByRole("link", { name: /^Kekayaan bersih/ });
    expect(net).toHaveAttribute("href", "/accounts");
    expect(net.querySelector(".t-fig")).toHaveTextContent("Rp 45.860.000");
    const assets = base.net_worth.liquid + 1_200_000 + base.net_worth.investment;
    expect(net).toHaveTextContent(`aset Rp ${assets.toLocaleString("id-ID")} · utang Rp 2.870.000`);
    expect(within(net).queryByRole("button")).not.toBeInTheDocument();
  });

  it("keeps an unknown Ketahanan Dana unknown with its reason, without a number of months or a sentinel", async () => {
    await beranda();
    const dana = within(panel()).getByRole("link", { name: /^Ketahanan Dana/ });
    await waitFor(() => expect(dana).toHaveTextContent("Belum bisa dihitung"));
    expect(dana).toHaveTextContent("Belum diketahui: belum ada periode lengkap");
    expect(dana.textContent).not.toMatch(/\d+ bulan|999/);
    expect(dana).toHaveAttribute("href", "/goals");
  });

  it.each([
    ["dana darurat", undefined],
    ["saldo kas", "all_liquid"],
  ])("states Ketahanan Dana in one line with %s and the target in months", async (word, basis) => {
    overview = { kpis: { runway_status: "healthy", coverage_months: 6.1, runway_months: 6.1, runway_days: 183, emergency_fund_balance: 20_412_400,
      monthly_primary_expense: 3_346_000, target_months: 6, target_amount: 19_963_800, emergency_balance_basis: basis, essential_cost: { method: "median" } } };
    await beranda();
    const dana = within(panel()).getByRole("link", { name: /^Ketahanan Dana/ });
    await waitFor(() => expect(dana).toHaveTextContent("6,1 bulan biaya pokok"));
    expect(dana).toHaveTextContent(`${word} Rp 20.412.400 · target 6 bulan`);
    expect(dana).not.toHaveTextContent("Rp 19.963.800");
    expect(dana.querySelector(".ws-bar, .prog")).toBeNull();
  });

  it("says when Ketahanan Dana cannot be loaded", async () => {
    overview = new Error("boom");
    await beranda();
    expect(await within(panel()).findByText("Ketahanan Dana belum bisa dimuat.")).toBeInTheDocument();
  });

  it("lists the five Kakeibo buckets with ledger links, no total and no Tabungan row", async () => {
    await beranda();
    const rows = [...panel().querySelectorAll(".kk-row")];
    expect(rows.map((row) => row.textContent)).toEqual([
      "KebutuhanRp 1.420.000", "KeinginanRp 610.000", "BudayaRp 245.000", "Tak TerdugaRp 89.500", "Belum diklasifikasiRp 0",
    ]);
    expect(within(panel()).getByRole("heading", { name: "Kakeibo siklus ini" })).toBeInTheDocument();
    expect(within(panel()).getByRole("link", { name: "Analisis" })).toHaveAttribute("href", "/insights");
    expect(within(panel()).getByRole("link", { name: "Belum diklasifikasi" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=unclassified");
    expect(panel()).not.toHaveTextContent(/Tabungan|Pengeluaran|Perlu dipilih di Tinjau/);
  });

  it("asks to choose a pillar in Tinjau when unclassified spending exists", async () => {
    const base = previewSummary();
    summary = { ...base, spending: { ...base.spending, lines: base.spending.lines.map((line) => (line.key === "unclassified" ? { ...line, amount: 40_000 } : line)) } };
    await beranda();
    const row = [...panel().querySelectorAll(".kk-row")][4] as HTMLElement;
    expect(row).toHaveTextContent("Belum diklasifikasiRp 40.000Perlu dipilih di Tinjau");
    expect(within(row).getByRole("link", { name: "Perlu dipilih di Tinjau" })).toHaveAttribute("href", "/review");
    expect(within(row).getByRole("link", { name: "Belum diklasifikasi" })).toHaveAttribute("href", "/ledger?period=cycle&kakeibo=unclassified");
  });
});

describe("Beranda composition, loading and failure", () => {
  it("keeps the DOM in the reading order at every width: hero, Perlu tindakan, Aktivitas terbaru, then the panel", async () => {
    const { hero } = await beranda();
    const order = [hero, region("Perlu tindakan"), region("Aktivitas terbaru"), panel()];
    order.slice(1).forEach((element, index) => {
      expect(order[index].compareDocumentPosition(element) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });
    expect(document.querySelector(".cols > .stack")!.children).toHaveLength(3);
  });

  it("shows busy placeholders of the final layout without any amount while loading", async () => {
    vi.mocked(api.get).mockImplementation(() => new Promise(() => {}));
    renderBeranda();
    const busy = document.querySelector('[aria-busy="true"]')!;
    expect(busy).toHaveClass("cols");
    expect(busy.querySelectorAll(".skel").length).toBeGreaterThan(10);
    expect(busy.querySelector(".hero-skel .legend")).not.toBeNull();
    expect(busy.querySelectorAll(".panel .panel-block")).toHaveLength(3);
    expect(document.body).not.toHaveTextContent(/Rp/);
    expect(document.querySelector(".animate-pulse")).toBeNull();
  });

  it("says the figures could not be loaded in the hero's place, shows no Rp 0, and keeps the blocks with their own data", async () => {
    summary = new Error("boom");
    const user = userEvent.setup();
    renderBeranda();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveClass("error-box");
    expect(alert).toHaveTextContent("Angka Beranda belum dapat dimuat");
    expect(alert).toHaveTextContent("Saldo dan transaksi tetap aman; coba muat lagi.");
    expect(await screen.findByRole("region", { name: "Perlu tindakan" })).toBeInTheDocument();
    expect(await screen.findByRole("region", { name: "Aktivitas terbaru" })).toBeInTheDocument();
    expect(within(panel()).getByRole("link", { name: /^Ketahanan Dana/ })).toBeInTheDocument();
    expect(panel()).not.toHaveTextContent(/Kekayaan bersih|Kakeibo/);
    expect(screen.queryByRole("region", { name: "Aman dipakai sampai gajian" })).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("Rp 0");
    expect((await axe(document.body)).violations).toHaveLength(0);
    summary = previewSummary();
    await user.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    expect(await screen.findByRole("region", { name: "Aman dipakai sampai gajian" })).toHaveTextContent("Rp 4.922.000");
  });

  it("has no axe violations in the normal state", async () => {
    const { container } = await beranda();
    await waitFor(() => expect(rowTexts(region("Aktivitas terbaru"))).toHaveLength(3));
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("hides every Beranda figure behind Rp •••••• named Disembunyikan, also in the Rincian, with no axe violation", async () => {
    display.hidden = true;
    const { container, hero } = await beranda();
    await waitFor(() => expect(rowTexts(region("Aktivitas terbaru"))).toHaveLength(3));
    const figures = [...document.querySelectorAll(".t-hero, .t-fig, .t-amt")];
    expect(figures.length).toBeGreaterThan(15);
    for (const figure of figures) {
      expect(figure.querySelector('[role="img"][aria-label="Disembunyikan"]') ?? figure.closest('[role="img"][aria-label="Disembunyikan"]') ?? figure).toHaveTextContent("Rp ••••••");
    }
    expect(within(hero).getAllByRole("img", { name: "Disembunyikan" }).length).toBe(5);
    expect(within(hero).getByRole("img", { name: /^Uang siklus ini/ })).toHaveAccessibleName(/^Uang siklus ini dari Rp •••••• masuk: Pengeluaran 24%/);
    for (const row of [...document.querySelectorAll(".row")]) expect(row.querySelector(".r-amt")?.textContent ?? "").not.toMatch(/\d/);
    expect((await axe(container)).violations).toHaveLength(0);
    const { dialog } = await openRincian(hero);
    expect(within(dialog).getAllByRole("img", { name: "Disembunyikan" }).length).toBeGreaterThanOrEqual(6);
    expect((await axe(document.body)).violations).toHaveLength(0);
  });
});
