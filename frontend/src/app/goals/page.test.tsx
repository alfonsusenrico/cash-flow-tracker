import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import type { Language } from "@/i18n/language";
import GoalsAndDebtsPage from "./page";

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
const ctx = vi.hoisted(() => ({ hidden: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    hideBalances: ctx.hidden,
    bal: (amount: number) => (ctx.hidden ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`),
    currency: "IDR",
    openMovement: () => {},
  }),
}));
// The plans dialog and the account detail have their own tests; here only what opens them matters.
vi.mock("@/components/recurring/PlansDialog", () => ({
  PlansDialog: ({ planId, expenseDraft, onClose }: { planId?: string | null; expenseDraft?: { name: string } | null; onClose: () => void }) => (
    <div role="dialog" aria-label="Rencana & Pengingat">
      <p>plan:{planId ?? "none"}</p>
      <p>draft:{expenseDraft?.name ?? "none"}</p>
      <button type="button" onClick={onClose}>
        Tutup rencana
      </button>
    </div>
  ),
}));
vi.mock("@/components/accounts/AccountDetailDialog", () => ({
  AccountDetailDialog: ({ accountId, onClose }: { accountId: string | null; onClose: () => void }) =>
    accountId ? (
      <div role="dialog" aria-label={`Detail ${accountId}`}>
        <button type="button" onClick={onClose}>
          Tutup detail
        </button>
      </div>
    ) : null,
}));

const realReplace = window.history.replaceState.bind(window.history);
const jakarta = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Jakarta" });
const TODAY = jakarta.format(new Date());
const day = (offset: number) => jakarta.format(new Date(Date.now() + offset * 86_400_000));

const goal = (fields: Record<string, unknown>) => ({
  id: "goal", name: "Goal", target_amount: 5_000_000, current_amount: 1_000_000, remaining_amount: 4_000_000, percentage_completed: 20,
  target_date: null, monthly_target_pace: null, color: "#3b82f6", icon: "plane", is_emergency: false, is_archived: false,
  account_ids: [], linked_accounts: [], created_at: "2026-09-01T00:00:00.000Z", ...fields,
});
const standaloneGoal = goal({ id: "goal-standalone", name: "Laptop" });
const linkedGoal = goal({ id: "goal-linked", name: "Liburan", account_ids: ["bca"], linked_accounts: [{ id: "bca", name: "BCA", type: "bank", balance: 1_000_000 }] });
const pacedGoal = goal({
  id: "goal-paced", name: "Motor", target_date: "2027-04-01", monthly_target_pace: 2_580_000, current_amount: 5_100_000, target_amount: 18_000_000,
  remaining_amount: 12_900_000, percentage_completed: 28.3, account_ids: ["bca"],
});
const reachedGoal = goal({ id: "goal-reached", name: "Sepeda", current_amount: 2_000_000, target_amount: 2_000_000, remaining_amount: 0, percentage_completed: 100 });
const emergencyGoal = goal({
  id: "goal-emergency", name: "Dana Darurat", is_emergency: true, account_ids: ["mandiri", "belanja"], current_amount: 9_000_000, target_amount: 14_000_000,
  remaining_amount: 5_000_000, percentage_completed: 64,
});

const debt = (fields: Record<string, unknown>) => ({
  id: "debt", name: "Debt", total_amount: 15_000_000, remaining_amount: 12_000_000, paid_amount: 3_000_000, payoff_percentage: 20, due_date: null,
  minimum_payment: null, estimated_payoff_months: null, notes: null, is_archived: false, created_at: "2026-09-01T00:00:00.000Z", ...fields,
});
const kartu = debt({ id: "kartu", name: "Kartu Kredit Lama", due_date: "2026-12-05", minimum_payment: 500_000 });
const paidOff = debt({ id: "lunas", name: "Pinjaman Lunas", remaining_amount: 0, paid_amount: 15_000_000, payoff_percentage: 100, is_archived: true });

const account = (fields: Record<string, unknown>) => ({ initial_balance: 0, is_archived: false, children: [], created_at: "2026-09-01T00:00:00.000Z", ...fields });
const ACCOUNTS = [
  account({ id: "bca", name: "BCA", type: "bank", balance: 1_000_000 }),
  account({
    id: "mandiri", name: "Mandiri", type: "bank", balance: 3_000_000,
    children: [account({ id: "belanja", name: "Belanja Bulanan", type: "bank", balance: 1_000_000, parent_id: "mandiri" })],
  }),
  account({
    id: "spaylater", name: "SPayLater", type: "credit", balance: -893_000, credit_limit: 3_000_000, credit_owed: 893_000, limit_left: 2_107_000,
    over_limit: false, payment_day: 8, statement_day: 25, next_due: { due_date: TODAY, amount: 434_000, left_to_pay: 434_000, status: "due_today" },
  }),
  account({ id: "kredivo", name: "Kredivo", type: "credit", balance: -150_000, credit_limit: 100_000, credit_owed: 150_000, limit_left: -50_000, over_limit: true, next_due: null }),
];

const RUNWAY = {
  runway_days: 162, runway_months: 5.4, coverage_months: 5.4, status: "moderate", emergency_fund_balance: 9_000_000, balance_basis: "emergency_goals",
  emergency_goal_names: ["Dana Darurat"], monthly_primary_expense: 1_666_667, target_months: 6, target_amount: 10_000_002, coverage_pct: 90,
  essential_cost: {
    amount: 1_666_667, living_amount: 1_666_667, debt_minimum: 0, method: "median_need_periods", basis: "history", period_count: 3, limited_history: false,
    periods_used: [{
      kind: "month", label: "Oktober 2026", start: "2026-10-01T00:00:00+07:00", end: "2026-10-31T23:59:59+07:00", start_date: "2026-10-01",
      end_date: "2026-10-31", days: 31, gross_need: 1_700_000, reimbursement_offset: 0, amount: 1_700_000,
    }],
  },
};

const planLine = (rule: string, label: string, due: string, amount: number, extra: Record<string, unknown> = {}) => ({
  kind: "plan", rule_id: rule, label, due_date: due, overdue: due < TODAY, amount, ...extra,
});
const PLAN_LINES = [
  planLine("r-yoga", "Kelas Yoga", day(-2), 150_000),
  planLine("r-iuran", "Iuran Kebersihan", TODAY, 50_000),
  planLine("r-spay", "Bayar SPayLater", TODAY, 310_000, { plan_amount: 434_000, counted_elsewhere: 124_000, credit_account_id: "spaylater" }),
  planLine("r-net", "Internet Rumah", day(3), 330_000),
  planLine("r-musik", "Musik", day(5), 55_000),
  planLine("r-air", "Air", day(6), 90_000),
  planLine("r-gym", "Gym", day(9), 200_000),
];

interface Fixture {
  goals: unknown[];
  obligations: unknown[];
  runway: unknown;
  lines: unknown[];
  charges: unknown[];
  bills: unknown[];
  creditFails: boolean;
  failing: string[];
}
let fixture: Fixture;

function freshFixture(): Fixture {
  return {
    goals: [standaloneGoal, linkedGoal, pacedGoal, reachedGoal, emergencyGoal],
    obligations: [kartu, paidOff],
    runway: RUNWAY,
    lines: PLAN_LINES,
    charges: [{
      key: "video", merchant: "Langganan Video", cadence: "monthly", typical_amount: 65_000, next_expected_date: day(4), existing_plan: null,
      plan_draft: { name: "Langganan Video", type: "expense", amount: 65_000, source_account_id: "bca", category_id: null, schedule_type: "monthly_day", schedule_day: 12 },
    }],
    bills: [
      { account_id: "spaylater", account_name: "SPayLater", due_date: TODAY, statement_date: day(-14), amount: 434_000, paid: 0, left_to_pay: 434_000, status: "due_today", settled_on: null },
      { account_id: "kartu-cc", account_name: "Kartu Kredit Contoh", due_date: day(-3), statement_date: day(-20), amount: 15_000, paid: 0, left_to_pay: 15_000, status: "overdue", settled_on: null },
    ],
    creditFails: false,
    failing: [],
  };
}

async function get(path: string): Promise<unknown> {
  if (fixture.failing.includes(path)) throw new Error("down");
  if (path === "/goals") return { ok: true, goals: fixture.goals };
  if (path === "/obligations?include_archived=true") return { ok: true, obligations: fixture.obligations };
  if (path === "/accounts") return { ok: true, accounts: ACCOUNTS, total_balance: 0 };
  if (path === "/dashboard/net-worth") return { ok: true, runway: fixture.runway };
  if (path === "/dashboard/cycle-summary") return { scheduled: { total: 2_000_000, window_basis: "next_30_days", window_start: TODAY, window_end: day(30), lines: fixture.lines } };
  if (path === "/dashboard/recurring-charges") return { ok: true, charges: fixture.charges };
  if (path === "/recurring") return { ok: true, rules: [{ id: "r-yoga", schedule_type: "weekly" }, { id: "r-net", schedule_type: "monthly_day" }] };
  if (path.startsWith("/credit/bills?")) {
    if (fixture.creditFails) throw new Error("down");
    return { ok: true, bills: fixture.bills, due_dates_unavailable: [] };
  }
  if (path.endsWith("/payment-candidates")) {
    return {
      match_state: "ambiguous", window_days: 45,
      candidates: ["tx-1", "tx-2"].map((id) => ({ id, amount: 500_000, date: "2026-10-01T03:00:00+00:00", notes: null, merchant: "Bank Contoh", account_name: "BCA", exact_amount: true })),
    };
  }
  throw new Error(`Unexpected GET ${path}`);
}

function renderGoals(language: Language = "id") {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  return renderWithIntl(<GoalsAndDebtsPage />, { language, wrapper });
}

/** A section once it has loaded: its skeleton is a busy region of the same name. */
async function loaded(name: string | RegExp) {
  return waitFor(() => {
    const region = screen.getByRole("region", { name });
    expect(region).not.toHaveAttribute("aria-busy");
    return region;
  });
}

async function openGoal(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(await screen.findByRole("button", { name: new RegExp(`^${name}`) }));
  return screen.findByRole("dialog", { name: "Ubah target tabungan" });
}
async function openDebt(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(await screen.findByRole("button", { name: new RegExp(`^${name}`) }));
  return screen.findByRole("dialog", { name: "Ubah utang" });
}

beforeEach(() => {
  vi.clearAllMocks();
  ctx.hidden = false;
  fixture = freshFixture();
  vi.mocked(api.get).mockImplementation(get);
  vi.mocked(api.post).mockResolvedValue({ ok: true });
  vi.mocked(api.patch).mockResolvedValue({ ok: true });
  vi.mocked(api.del).mockResolvedValue({ ok: true });
  realReplace(null, "", "/goals");
  window.history.replaceState = (state, title, url) => {
    realReplace(state, title, url);
    window.dispatchEvent(new Event("test-history"));
  };
});
afterEach(() => {
  window.history.replaceState = realReplace;
  realReplace(null, "", "/goals");
});

describe("Target & Tagihan layout", () => {
  it("reads hero, Tagihan, Target tabungan, then Utang & kredit, with one Rincian and no axe violations", async () => {
    const { container } = renderGoals();
    await screen.findByText("Tagihan Kartu Kredit Contoh");
    await screen.findByText("Kartu Kredit Lama");
    const headings = screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent);
    expect(headings).toEqual(["Ketahanan Dana", "Tagihan 30 hari ke depan", "Target tabungan", "Utang & kredit"]);
    expect(screen.getAllByRole("button", { name: "Rincian" })).toHaveLength(1);
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect((await axe(container)).violations).toEqual([]);
  });

  it("offers Tambah utang and Ubah target dana darurat in the more menu for one flagged goal", async () => {
    const user = userEvent.setup();
    renderGoals();
    await screen.findByText("Laptop");
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    const menu = await screen.findByRole("menu");
    expect(within(menu).getAllByRole("menuitem").map((item) => item.textContent)).toEqual(["Tambah utang", "Ubah target dana darurat"]);
    await user.click(within(menu).getByRole("menuitem", { name: "Ubah target dana darurat" }));
    const dialog = await screen.findByRole("dialog", { name: "Ubah target tabungan" });
    expect(within(dialog).getByRole("checkbox", { name: "Tandai sebagai dana darurat" })).toBeChecked();
    expect(window.location.search).toBe("?target=goal-emergency");
  });

  it("offers Buat target dana darurat without a flagged goal", async () => {
    const user = userEvent.setup();
    fixture.goals = [standaloneGoal];
    renderGoals();
    await screen.findByText("Laptop");
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    await user.click(await screen.findByRole("menuitem", { name: "Buat target dana darurat" }));
    const dialog = await screen.findByRole("dialog", { name: "Buat target tabungan baru" });
    expect(within(dialog).getByRole("checkbox", { name: "Tandai sebagai dana darurat" })).toBeChecked();
  });

  it("keeps several flagged goals listed with Dana darurat and leaves the menu item out", async () => {
    const user = userEvent.setup();
    fixture.goals = [emergencyGoal, { ...standaloneGoal, is_emergency: true }];
    renderGoals();
    const goals = await loaded("Target tabungan");
    await within(goals).findByText("Laptop");
    expect(within(goals).getAllByText(/Dana darurat$/)).toHaveLength(2);
    await user.click(screen.getByRole("button", { name: "Tindakan lain" }));
    expect(within(await screen.findByRole("menu")).getAllByRole("menuitem").map((item) => item.textContent)).toEqual(["Tambah utang"]);
  });
});

describe("Ketahanan Dana hero", () => {
  it("shows the server's months, a bar of coverage_pct, four figures, and the basis in the Rincian", async () => {
    const user = userEvent.setup();
    renderGoals();
    const hero = await loaded("Ketahanan Dana");
    await within(hero).findByText("5,4 bulan");
    expect(within(hero).getByText("biaya pokok tertutup dana darurat · target 6 bulan")).toBeInTheDocument();
    const bar = within(hero).getByRole("img", { name: "90% dari target dana darurat" });
    expect(bar.querySelector(".prog > span")).toHaveStyle({ width: "90%" });
    expect(hero).toHaveTextContent("Dana daruratRp 9.000.00090% dari target");
    expect(hero).toHaveTextContent("TargetRp 10.000.0026 × biaya pokok");
    expect(hero).toHaveTextContent("Biaya pokok / bulanRp 1.666.667rata-rata 3 bulan");
    expect(hero).toHaveTextContent("Jika pemasukan berhenti162 harisampai dana darurat habis");

    const opener = within(hero).getByRole("button", { name: "Rincian" });
    await user.click(opener);
    const rincian = await screen.findByRole("dialog", { name: "Ketahanan Dana" });
    expect(rincian).toHaveTextContent("5,4x biaya pokok · 162 hari");
    await user.click(within(rincian).getByRole("button", { name: /Saldo cadangan/ }));
    expect(within(rincian).getByText("Dana Darurat")).toBeInTheDocument();
    await user.click(within(rincian).getByRole("button", { name: /Biaya hidup pokok bulanan/ }));
    const period = within(rincian).getByRole("link", { name: /Oktober 2026/ });
    const href = new URL(period.getAttribute("href") ?? "", "https://example.test");
    expect(Object.fromEntries(href.searchParams)).toMatchObject({ kakeibo: "need", type: "expense" });
    await user.click(within(rincian).getByRole("button", { name: "Tutup rincian" }));
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it("names the all-cash basis and shows no months, bar or days while the cost is unknown", async () => {
    fixture.runway = {
      ...RUNWAY, runway_days: null, runway_months: null, coverage_months: null, status: "unknown", balance_basis: "all_liquid", emergency_goal_names: [],
      monthly_primary_expense: null, target_amount: null, coverage_pct: null,
      essential_cost: { amount: null, living_amount: null, debt_minimum: 0, method: "unknown", basis: "unknown", period_count: 0, limited_history: false, periods_used: [] },
    };
    renderGoals();
    const hero = await loaded("Ketahanan Dana");
    await within(hero).findByText("Belum dapat dihitung");
    expect(within(hero).queryByRole("img")).not.toBeInTheDocument();
    expect(hero).not.toHaveTextContent(/\d bulan/);
    expect(hero).not.toHaveTextContent(/\d hari/);
    expect(hero).toHaveTextContent("Saldo kas");
    expect(hero).toHaveTextContent("Biaya pokok / bulanBelum diketahui");
  });

  it("shows zero coverage as 0 bulan, an empty bar and 0 hari", async () => {
    fixture.runway = { ...RUNWAY, runway_days: 0, runway_months: 0, coverage_months: 0, status: "zero", emergency_fund_balance: 0, coverage_pct: 0 };
    renderGoals();
    const hero = await loaded("Ketahanan Dana");
    await within(hero).findByText("0 bulan");
    expect(within(hero).getByRole("img", { name: "0% dari target dana darurat" })).toBeInTheDocument();
    expect(hero).toHaveTextContent("0 hari");
  });
});

describe("Tagihan 30 hari ke depan", () => {
  it("lists five rows and a toggle, keeps credit bills outside the count and total, and lists a credit-bill plan once", async () => {
    const user = userEvent.setup();
    renderGoals();
    const section = await loaded("Tagihan 30 hari ke depan");
    await within(section).findByText("Tagihan Kartu Kredit Contoh");
    // Seven plan lines and one detected charge: the total adds each line's counted amount and no credit bill.
    expect(section).toHaveTextContent("8 · Rp 1.250.000 · 1 tagihan kredit");
    const list = within(section).getByRole("list", { name: "Tagihan berikutnya" });
    expect(within(list).getAllByRole("listitem")).toHaveLength(5);
    expect(within(section).queryByText("Tagihan SPayLater")).not.toBeInTheDocument();
    const spay = within(section).getByRole("button", { name: /Bayar SPayLater/ });
    expect(spay).toHaveTextContent("Rp 124.000 sudah dihitung di Pengeluaran");
    expect(spay).toHaveTextContent("Rp 434.000");
    expect(within(section).getByRole("button", { name: /Tagihan Kartu Kredit Contoh/ })).toHaveTextContent("lewat jatuh tempo · sudah dihitung di Pengeluaran");
    expect(within(section).getByRole("button", { name: /Kelas Yoga/ })).toHaveTextContent("terlewat · mingguan");

    const toggle = within(section).getByRole("button", { name: "dan 4 tagihan lain" });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    await user.click(toggle);
    expect(within(list).getAllByRole("listitem")).toHaveLength(9);
    expect(within(section).getByRole("button", { name: "Tampilkan lebih sedikit" })).toHaveFocus();
    expect(section).toHaveTextContent("bulanan · perkiraan");
  });

  it("opens a plan, a detected charge's draft and a credit bill's account from their rows", async () => {
    const user = userEvent.setup();
    renderGoals();
    const section = await loaded("Tagihan 30 hari ke depan");
    await user.click(await within(section).findByRole("button", { name: /Kelas Yoga/ }));
    expect(await screen.findByText("plan:r-yoga")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Tutup rencana" }));
    await user.click(within(section).getByRole("button", { name: "dan 4 tagihan lain" }));
    await user.click(within(section).getByRole("button", { name: /Langganan Video/ }));
    expect(await screen.findByText("draft:Langganan Video")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Tutup rencana" }));
    await user.click(within(section).getByRole("button", { name: /Tagihan Kartu Kredit Contoh/ }));
    expect(await screen.findByRole("dialog", { name: "Detail kartu-cc" })).toBeInTheDocument();
  });

  it("keeps the plan rows when credit bills fail and offers a retry", async () => {
    fixture.creditFails = true;
    renderGoals();
    const section = await loaded("Tagihan 30 hari ke depan");
    expect(await within(section).findByText("Tagihan kredit belum dapat dimuat.")).toBeInTheDocument();
    expect(section).toHaveTextContent("Kelas Yoga");
    expect(within(section).getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
  });
});

describe("Target tabungan", () => {
  it("states each goal in words with its amounts, without the one emergency goal", async () => {
    renderGoals();
    const section = await loaded("Target tabungan");
    await within(section).findByText("Laptop");
    expect(section).toHaveTextContent("4 target");
    expect(within(section).queryByText("Dana Darurat")).not.toBeInTheDocument();
    const motor = within(section).getByRole("button", { name: /^Motor/ });
    expect(motor).toHaveTextContent("sampai 1 Apr 2027 · Rp 2.580.000/bln · 1 kantong");
    expect(motor).toHaveTextContent("Rp 5.100.000dari Rp 18.000.000 · 28%");
    expect(within(section).getByRole("button", { name: /^Sepeda/ })).toHaveTextContent("Tercapai · dicatat manual");
    expect(within(section).getByRole("button", { name: /^Laptop/ })).toHaveTextContent("Tanpa tanggal · dicatat manual");
  });

  it("offers Tambah target when there is no goal", async () => {
    fixture.goals = [];
    renderGoals();
    const section = await loaded("Target tabungan");
    expect(await within(section).findByText("Belum ada target tabungan.")).toBeInTheDocument();
    expect(within(section).getByRole("button", { name: "Tambah target" })).toBeInTheDocument();
  });
});

describe("Utang & kredit", () => {
  it("lists each debt and credit account, reads paid-off debts with include_archived and opens them", async () => {
    const user = userEvent.setup();
    renderGoals();
    const panel = await loaded("Utang & kredit");
    await within(panel).findByText("Kartu Kredit Lama");
    expect(api.get).toHaveBeenCalledWith("/obligations?include_archived=true");
    expect(api.get).not.toHaveBeenCalledWith("/obligations");
    expect(panel).toHaveTextContent("3 aktif");
    expect(within(panel).getByRole("button", { name: /^Kartu Kredit Lama/ })).toHaveTextContent("Utang −Rp 12.000.0005 Dessisa · 20% terbayar");
    const spay = within(panel).getByRole("button", { name: /^SPayLater · kredit/ });
    expect(spay).toHaveTextContent("Jatuh tempo hari ini · sisa Rp 434.000");
    expect(spay).toHaveTextContent("Sisa limit Rp 2.107.000");
    expect(within(panel).getByRole("button", { name: /^Kredivo · kredit/ })).toHaveTextContent("Melewati limit");

    const toggle = within(panel).getByRole("button", { name: "Sudah lunas · 1" });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    await user.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    await user.click(within(panel).getByRole("button", { name: /^Pinjaman Lunas/ }));
    const dialog = await screen.findByRole("dialog", { name: "Ubah utang" });
    expect(dialog).toHaveTextContent("Diarsipkan otomatis saat lunas; aktif lagi jika pembayarannya dibatalkan.");
    expect(within(dialog).queryByRole("button", { name: "Kaitkan atau catat pembayaran" })).not.toBeInTheDocument();
  });

  it("opens a credit account's detail in place and returns focus to its row", async () => {
    const user = userEvent.setup();
    renderGoals();
    const row = await screen.findByRole("button", { name: /^SPayLater · kredit/ });
    await user.click(row);
    await user.click(await screen.findByRole("button", { name: "Tutup detail" }));
    await waitFor(() => expect(row).toHaveFocus());
  });
});

describe("goal dialogs", () => {
  it("offers adjustment only for standalone goals and never posts a transaction", async () => {
    const user = userEvent.setup();
    renderGoals();
    const linked = await openGoal(user, "Liburan");
    expect(within(linked).queryByRole("button", { name: "Tambah progres" })).not.toBeInTheDocument();
    await user.click(within(linked).getByRole("button", { name: "Batal" }));
    const standalone = await openGoal(user, "Laptop");
    expect(within(standalone).getByText("Progres diatur manual.")).toBeInTheDocument();
    await user.click(within(standalone).getByRole("button", { name: "Tambah progres" }));
    const progress = await screen.findByRole("dialog", { name: "Tambah progres" });
    expect(progress).toHaveTextContent("Tidak ada transaksi dan tidak ada saldo rekening yang berubah.");
    expect(within(progress).queryByRole("combobox")).not.toBeInTheDocument();
    await user.type(within(progress).getByRole("textbox", { name: "Tambahan progres" }), "250000");
    await user.click(within(progress).getByRole("button", { name: "Simpan progres" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/goals/goal-standalone/adjust", { amount: 250_000 }));
    expect(api.post).not.toHaveBeenCalledWith("/transactions", expect.anything());
  });

  it("requires confirmation before changing a standalone goal to account-backed progress", async () => {
    const user = userEvent.setup();
    renderGoals();
    const dialog = await openGoal(user, "Laptop");
    await user.click(within(dialog).getByRole("radio", { name: "Otomatis dari saldo kantong" }));
    await user.click(within(dialog).getByRole("checkbox", { name: /BCA/ }));
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    expect(api.patch).not.toHaveBeenCalled();
    expect(dialog).toHaveTextContent(/progres akan mengikuti saldo rekening terhubung/);
    await user.click(within(dialog).getByRole("button", { name: "Konfirmasi & simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/goals/goal-standalone", expect.objectContaining({ account_ids: ["bca"] })));
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("current_amount");
  });

  it("changes the target of an account-backed goal without manual progress and keeps the stored colour", async () => {
    const user = userEvent.setup();
    renderGoals();
    const dialog = await openGoal(user, "Liburan");
    expect(within(dialog).queryByRole("group", { name: /warna/i })).not.toBeInTheDocument();
    const target = within(dialog).getByRole("textbox", { name: "Target" });
    await user.clear(target);
    await user.type(target, "2500000");
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/goals/goal-linked", expect.objectContaining({
      target_amount: 2_500_000, account_ids: ["bca"], color: "#3b82f6", icon: "plane",
    })));
    expect(vi.mocked(api.patch).mock.calls[0][1]).not.toHaveProperty("current_amount");
  });

  it("counts a pocket and its parent once in the linked total", async () => {
    realReplace(null, "", "/goals?target=goal-emergency");
    renderGoals();
    const dialog = await screen.findByRole("dialog", { name: "Ubah target tabungan" });
    expect(within(dialog).getByText("Total saldo terhubung").closest(".box")).toHaveTextContent("Rp 3.000.000");
  });

  it("opens a goal from ?target= and confirms archiving in the footer without touching an account", async () => {
    realReplace(null, "", "/goals?target=goal-standalone");
    const user = userEvent.setup();
    renderGoals();
    const dialog = await screen.findByRole("dialog", { name: "Ubah target tabungan" });
    await user.click(within(dialog).getByRole("button", { name: "Arsipkan" }));
    expect(api.del).not.toHaveBeenCalled();
    expect(within(dialog).getByRole("alert")).toHaveTextContent('Arsipkan target "Laptop"? Saldo rekening tidak berubah.');
    await user.click(within(dialog).getByRole("button", { name: "Batal" }));
    await waitFor(() => expect(within(dialog).getByRole("button", { name: "Arsipkan" })).toHaveFocus());
    await user.click(within(dialog).getByRole("button", { name: "Arsipkan" }));
    await user.click(within(dialog).getByRole("button", { name: "Ya, arsipkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/goals/goal-standalone"));
  });
});

describe("debt dialogs", () => {
  it("validates a debt against the final total and remaining amounts", async () => {
    const user = userEvent.setup();
    renderGoals();
    const dialog = await openDebt(user, "Kartu Kredit Lama");
    expect(dialog).toHaveTextContent("Jatuh tempo 5 Des 2026 · minimal Rp 500.000 per bulan");
    const total = within(dialog).getByRole("textbox", { name: "Total" });
    const remaining = within(dialog).getByRole("textbox", { name: "Sisa" });
    await user.clear(total);
    await user.type(total, "10000000");
    expect(within(dialog).getByText("Sisa tagihan tidak boleh melebihi total tagihan.")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(remaining).toHaveFocus());
    expect(api.patch).not.toHaveBeenCalled();
    await user.clear(remaining);
    await user.type(remaining, "8000000");
    await user.click(within(dialog).getByRole("button", { name: "Simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/obligations/kartu", expect.objectContaining({ total_amount: 10_000_000, remaining_amount: 8_000_000 })));
  });

  it("links an already recorded payment first and shows no payoff-time estimate", async () => {
    const user = userEvent.setup();
    renderGoals();
    const dialog = await openDebt(user, "Kartu Kredit Lama");
    expect(dialog).not.toHaveTextContent(/bulan lagi|bln lagi/);
    await user.click(within(dialog).getByRole("button", { name: "Kaitkan atau catat pembayaran" }));
    const pay = await screen.findByRole("dialog", { name: "Pembayaran: Kartu Kredit Lama" });
    expect(await within(pay).findByText(/Pilih yang benar/)).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();
    const choices = within(pay).getAllByRole("button", { name: /^Kaitkan 1 Okt/ });
    await user.click(choices[1]);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/obligations/kartu/payments", { transaction_id: "tx-2" }));
    expect(api.post).toHaveBeenCalledTimes(1);
  });

  it("records a manual payment with today's request", async () => {
    const user = userEvent.setup();
    renderGoals();
    const dialog = await openDebt(user, "Kartu Kredit Lama");
    await user.click(within(dialog).getByRole("button", { name: "Kaitkan atau catat pembayaran" }));
    const pay = await screen.findByRole("dialog", { name: "Pembayaran: Kartu Kredit Lama" });
    expect(within(pay).getByRole("textbox", { name: "Nominal" })).toHaveValue("500.000");
    await user.click(within(pay).getByRole("button", { name: "Catat pembayaran manual" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      account_id: "bca", type: "expense", amount: 500_000, obligation_id: "kartu", notes: "Bayar tagihan: Kartu Kredit Lama",
    })));
  });

  it("confirms archiving a debt in the footer", async () => {
    const user = userEvent.setup();
    renderGoals();
    const dialog = await openDebt(user, "Kartu Kredit Lama");
    await user.click(within(dialog).getByRole("button", { name: "Arsipkan" }));
    expect(within(dialog).getByRole("alert")).toHaveTextContent('Arsipkan utang "Kartu Kredit Lama"?');
    await user.click(within(dialog).getByRole("button", { name: "Ya, arsipkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/obligations/kartu"));
  });
});

describe("states and languages", () => {
  it("shows each section's own error and keeps the others", async () => {
    fixture.failing = ["/dashboard/net-worth", "/goals"];
    renderGoals();
    expect(await screen.findByText("Ketahanan Dana belum dapat dihitung.")).toBeInTheDocument();
    expect(await screen.findByText("Target belum dapat dimuat.")).toBeInTheDocument();
    expect(await screen.findByText("Kartu Kredit Lama")).toBeInTheDocument();
    expect(await screen.findByText("Kelas Yoga")).toBeInTheDocument();
  });

  it("hides every amount and keeps month and day counts while balances are hidden", async () => {
    ctx.hidden = true;
    renderGoals();
    const hero = await loaded("Ketahanan Dana");
    await within(hero).findByText("5,4 bulan");
    expect(hero).toHaveTextContent("162 hari");
    expect(hero).not.toHaveTextContent("9.000.000");
    expect(within(hero).getAllByRole("img", { name: "Disembunyikan" }).length).toBeGreaterThan(0);
    const panel = await loaded("Utang & kredit");
    await within(panel).findByText("Kartu Kredit Lama");
    expect(panel).not.toHaveTextContent("12.000.000");
  });

  it("reads in plain English with the same figures", async () => {
    const { container } = renderGoals("en");
    const hero = await loaded("Safety net");
    await within(hero).findByText("5,4 months");
    expect(hero).toHaveTextContent("Rp 9.000.000");
    await screen.findByText("Kartu Kredit Contoh bill");
    const headings = screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent);
    expect(headings).toEqual(["Safety net", "Bills in the next 30 days", "Savings goals", "Debts & credit"]);
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(container.querySelector('[lang="id"]')).toBeNull();
  });
});
