import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import OverviewPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    timeframe: "cycle",
    cycleOffset: 0,
    openQuickAdd: vi.fn(),
    openMovement: vi.fn(),
    user: { full_name: "Sari Contoh" },
    bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`,
  }),
}));
// The cards around the feed have their own tests and data; only "Aktivitas Terkini" is under test here.
vi.mock("@/components/dashboard/HeroWalletCard", () => ({ HeroWalletCard: () => null }));
vi.mock("@/components/dashboard/MetricMatrixGrid", () => ({ MetricMatrixGrid: () => null }));
vi.mock("@/components/dashboard/KakeiboBenchmark", () => ({ KakeiboBenchmark: () => null }));
vi.mock("@/components/dashboard/ObservedCashFlowCard", () => ({ ObservedCashFlowCard: () => null }));
vi.mock("@/components/dashboard/NarrativeInsightCard", () => ({ NarrativeInsightCard: () => null }));
vi.mock("@/components/dashboard/CategoryDonutChart", () => ({ CategoryDonutChart: () => null }));
vi.mock("@/components/recurring/PendingScheduledBanner", () => ({ PendingScheduledBanner: () => null }));
vi.mock("@/components/report/ReportReadyNotice", () => ({ ReportReadyNotice: () => null }));
vi.mock("@/components/recurring/RecurringRulesModal", () => ({ RecurringRulesModal: () => null }));

const recent = [
  { id: "topup-out", type: "expense", amount: 250_000, notes: "", account_name: "Rekening Contoh", date: "2026-11-02T03:00:00Z", is_excluded_from_budget: true, investment_topup_id: "topup-1" },
  { id: "move-out", type: "expense", amount: 200_000, notes: "", account_name: "Rekening Contoh", date: "2026-11-02T02:00:00Z", is_excluded_from_budget: true, investment_topup_id: null },
  { id: "coffee", type: "expense", amount: 15_000, notes: "", category_name: "Kopi Contoh", account_name: "Tunai Contoh", date: "2026-11-02T01:00:00Z", is_excluded_from_budget: false, investment_topup_id: null },
];

function renderOverview() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}><OverviewPage /></QueryClientProvider>);
}

/** The desktop "Aktivitas Terkini" card; the phone-width view renders the same rows in its own list. */
async function desktopFeed() {
  const headings = await screen.findAllByRole("heading", { name: /Aktivitas Terkini/ });
  const card = headings.map((heading) => heading.closest(".card-squircle")).find(Boolean);
  expect(card).toBeTruthy();
  return card as HTMLElement;
}

describe("Beranda recent activity", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path.startsWith("/dashboard/overview")) return { ok: true, recent_transactions: recent, accounts_liquidity: [], categories: [] };
      if (path === "/accounts") return { accounts: [] };
      return {};
    });
  });

  it("paints an investment top-up lavender and names it, keeping transfers sky", async () => {
    renderOverview();
    const feed = await desktopFeed();

    const topupAmount = within(feed).getByText("-Rp 250.000");
    expect(topupAmount).toHaveClass("text-investment");
    expect(topupAmount).not.toHaveClass("text-sky-500");
    expect(within(feed).getByText("Top up Investasi")).toBeVisible();

    expect(within(feed).getByText("-Rp 200.000")).toHaveClass("text-sky-500");
    expect(within(feed).getByText("Pindah Saldo")).toBeVisible();
    expect(within(feed).getByText("-Rp 15.000")).toHaveClass("text-rose-500");
  });

  it("shows a top-up's detail as an investment top-up", async () => {
    const user = userEvent.setup();
    renderOverview();
    const feed = await desktopFeed();

    await user.click(within(feed).getByText("Top up Investasi"));
    const dialog = await screen.findByRole("dialog", { name: "Detail Transaksi" });
    expect(within(dialog).getAllByText("Top up Investasi")).toHaveLength(2);
    expect(within(dialog).getByText("-Rp 250.000")).toHaveClass("text-investment");
    expect(within(dialog).queryByText(/Pindah Saldo/)).not.toBeInTheDocument();
  });
});
