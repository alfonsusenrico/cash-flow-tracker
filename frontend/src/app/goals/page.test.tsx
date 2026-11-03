import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import GoalsAndDebtsPage from "./page";

// Next.js keeps `useSearchParams` in sync with the history API; the mock does the same.
vi.mock("next/navigation", async () => {
  const react = await import("react");
  return {
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
const realReplace = window.history.replaceState.bind(window.history);

async function openDrawer(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(await screen.findByRole("button", { name }));
  return screen.findByRole("dialog", { name: new RegExp(`^${name}`) });
}

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`, currency: "IDR" }),
}));

const standaloneGoal = {
  id: "goal-standalone",
  name: "Laptop",
  target_amount: 5_000_000,
  current_amount: 1_000_000,
  remaining_amount: 4_000_000,
  percentage_completed: 20,
  target_date: null,
  monthly_target_pace: null,
  color: "#10b981",
  icon: "target",
  is_emergency: false,
  is_archived: false,
  account_ids: [],
  linked_accounts: [],
  created_at: "2026-09-01T00:00:00.000Z",
};

const linkedGoal = {
  ...standaloneGoal,
  id: "goal-linked",
  name: "Dana Darurat",
  account_ids: ["account-1"],
  linked_accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 1_000_000 }],
};

const obligation = {
  id: "obligation-1",
  name: "Kartu Kredit",
  total_amount: 15_000_000,
  remaining_amount: 12_000_000,
  paid_amount: 3_000_000,
  payoff_percentage: 20,
  due_date: null,
  minimum_payment: null as number | null,
  estimated_payoff_months: null as number | null,
  notes: null,
  is_archived: false,
  created_at: "2026-09-01T00:00:00.000Z",
};
let currentObligations: typeof obligation[] = [];

function renderGoals() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(<QueryClientProvider client={queryClient}><GoalsAndDebtsPage /></QueryClientProvider>);
}

describe("goal backing forms", () => {
  afterEach(() => {
    window.history.replaceState = realReplace;
    realReplace(null, "", "/goals");
  });

  beforeEach(() => {
    vi.clearAllMocks();
    realReplace(null, "", "/goals");
    window.history.replaceState = (state, title, url) => {
      realReplace(state, title, url);
      window.dispatchEvent(new Event("test-history"));
    };
    currentObligations = [];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/goals") return { ok: true, goals: [standaloneGoal, linkedGoal], summary: {} };
      if (path === "/obligations") return { ok: true, obligations: currentObligations, summary: {} };
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 1_000_000 }] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("offers adjustment only for standalone goals and never posts a transaction", async () => {
    const user = userEvent.setup();
    renderGoals();

    const linked = await openDrawer(user, "Dana Darurat");
    expect(within(linked).queryByRole("button", { name: "Tambah progres" })).not.toBeInTheDocument();
    await user.click(within(linked).getByRole("button", { name: "Tutup detail" }));
    const standalone = await openDrawer(user, "Laptop");
    expect(within(standalone).getByText("Progres manual tidak membuat transaksi di rekening.")).toBeInTheDocument();
    await user.click(within(standalone).getByRole("button", { name: "Tambah progres" }));
    expect(screen.getByRole("dialog", { name: "Catat Progres: Laptop" })).toBeVisible();
    expect(screen.queryByRole("combobox", { name: /rekening/i })).not.toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Tambahan Progres" }), "250000");
    await user.click(screen.getByRole("button", { name: "Simpan progres" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/goals/goal-standalone/adjust", { amount: 250_000 }));
    expect(api.post).not.toHaveBeenCalledWith("/transactions", expect.anything());
  });

  it("requires confirmation before changing a standalone goal to account-backed progress", async () => {
    const user = userEvent.setup();
    renderGoals();
    await user.click(within(await openDrawer(user, "Laptop")).getByRole("button", { name: "Ubah target" }));
    await user.click(screen.getByRole("checkbox", { name: /BCA/ }));
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    expect(api.patch).not.toHaveBeenCalled();
    expect(screen.getByText(/progres akan mengikuti saldo rekening terhubung/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Konfirmasi & simpan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/goals/goal-standalone", expect.objectContaining({
      account_ids: ["account-1"],
      current_amount: 0,
    })));
  });

  it("validates an obligation against the final total and remaining amounts", async () => {
    currentObligations = [obligation];
    const user = userEvent.setup();
    renderGoals();
    await user.click(within(await openDrawer(user, "Kartu Kredit")).getByRole("button", { name: "Ubah utang" }));

    expect(screen.getByText(/otomatis diarsipkan saat sisa mencapai nol/)).toBeInTheDocument();
    const total = screen.getByRole("textbox", { name: "Total Tagihan / Pinjaman (IDR)" });
    const remaining = screen.getByRole("textbox", { name: "Sisa Tagihan Saat Ini (IDR)" });
    await user.clear(total);
    await user.type(total, "10000000");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));

    expect(await screen.findByText("Sisa tagihan tidak boleh melebihi total tagihan.")).toBeInTheDocument();
    expect(remaining).toHaveFocus();
    expect(api.patch).not.toHaveBeenCalled();

    await user.clear(remaining);
    await user.type(remaining, "8000000");
    await user.click(screen.getByRole("button", { name: "Simpan Perubahan" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/obligations/obligation-1", expect.objectContaining({
      total_amount: 10_000_000,
      remaining_amount: 8_000_000,
    })));
  });

  it("links an already recorded payment first and shows no payoff-time estimate", async () => {
    currentObligations = [{ ...obligation, minimum_payment: 500_000, estimated_payoff_months: 24 }];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/goals") return { ok: true, goals: [], summary: {} };
      if (path === "/obligations") return { ok: true, obligations: currentObligations, summary: {} };
      if (path === "/accounts") return { accounts: [{ id: "account-1", name: "BCA", type: "bank", balance: 1_000_000 }] };
      if (path === "/obligations/obligation-1/payment-candidates") {
        return {
          match_state: "ambiguous", window_days: 45,
          candidates: ["tx-1", "tx-2"].map((id) => ({
            id, amount: 500_000, date: "2026-10-01T03:00:00+00:00", notes: null, merchant: "Bank Contoh",
            account_name: "BCA", exact_amount: true,
          })),
        };
      }
      throw new Error(`Unexpected GET ${path}`);
    });
    const user = userEvent.setup();
    renderGoals();

    expect(await screen.findByText("Terbayar Rp 3.000.000 dari Rp 15.000.000 · 20%")).toBeInTheDocument();
    const drawer = await openDrawer(user, "Kartu Kredit");
    expect(within(drawer).queryByText(/bulan lagi|bln lagi/)).not.toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "Kaitkan atau catat pembayaran" }));
    expect(await screen.findByText(/Pilih yang benar/)).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();
    const choices = screen.getAllByRole("button", { name: /^Kaitkan 1 Okt/ });
    await user.click(choices[1]);
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/obligations/obligation-1/payments", { transaction_id: "tx-2" }));
    expect(api.post).toHaveBeenCalledTimes(1);
  });

  it("states each goal in words, lists the bills from the plan lines with the server's total and shows an unknown runway", async () => {
    const reached = { ...standaloneGoal, id: "g-reached", name: "Sepeda", current_amount: 2_000_000, target_amount: 2_000_000, remaining_amount: 0, percentage_completed: 100 };
    const paced = { ...standaloneGoal, id: "g-paced", name: "Liburan", target_date: "2027-06-30", monthly_target_pace: 750_000 };
    currentObligations = [obligation, { ...obligation, id: "paid", name: "Pinjaman Lunas", remaining_amount: 0, payoff_percentage: 100, is_archived: true }];
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/goals") return { ok: true, goals: [standaloneGoal, reached, paced], summary: {} };
      if (path === "/obligations") return { ok: true, obligations: currentObligations, summary: {} };
      if (path === "/accounts") return { accounts: [] };
      if (path === "/dashboard/cycle-summary") return {
        scheduled: { total: 1_500_000, plans_total: 395_000, window_basis: "expected_salary", window_start: "2026-10-25", window_end: "2026-11-24",
          lines: [
            { kind: "plan", rule_id: "r1", label: "Streamio", due_date: "2026-11-03", overdue: false, amount: 65_000 },
            { kind: "plan", rule_id: "r2", label: "Internet Rumah", due_date: "2026-11-10", overdue: false, amount: 330_000 },
            { kind: "goal", goal_id: "g-paced", label: "Liburan", target_date: "2027-06-30", months_left: 8, set_aside: 750_000, funded: 0, amount: 1_105_000 },
          ] },
      };
      if (path === "/dashboard/net-worth") return {
        runway: { runway_days: null, runway_months: null, coverage_months: null, status: "unknown", emergency_fund_balance: 3_500_000, balance_basis: "goal",
          essential_cost: { amount: null, method: "unknown", basis: "unknown" } },
      };
      throw new Error(`Unexpected GET ${path}`);
    });
    renderGoals();
    expect(await screen.findByText("Tercapai")).toBeInTheDocument();
    expect(screen.getByText("Tanpa tanggal")).toBeInTheDocument();
    expect(screen.getByText("Perlu Rp 750.000/bulan")).toBeInTheDocument();
    const bills = await screen.findByRole("table", { name: "Tagihan sampai gaji berikutnya" });
    expect(within(bills).getAllByRole("row")).toHaveLength(4);
    expect(within(bills).queryByText("Liburan")).not.toBeInTheDocument();
    expect(within(bills).getByRole("row", { name: /Total/ })).toHaveTextContent("Rp 395.000");
    expect(await screen.findByText("Belum dapat dihitung")).toBeInTheDocument();
    expect(screen.queryByText(/^0 bulan/)).not.toBeInTheDocument();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Tampilkan 1 utang lunas" }));
    expect(screen.getByText("Pinjaman Lunas")).toBeInTheDocument();
    expect(screen.queryByText(/sudah mencapai|bebas utang/i)).not.toBeInTheDocument();
  });

  it("opens a goal from ?target= and confirms before archiving without touching an account", async () => {
    realReplace(null, "", "/goals?target=goal-standalone");
    const user = userEvent.setup();
    renderGoals();
    const drawer = await screen.findByRole("dialog", { name: /^Laptop/ });
    await user.click(within(drawer).getByRole("button", { name: "Arsipkan target Laptop" }));
    expect(api.del).not.toHaveBeenCalled();
    expect(within(drawer).getByRole("group", { name: /Arsipkan target "Laptop"\? Saldo rekening tidak berubah\./ })).toBeInTheDocument();
    await user.click(within(drawer).getByRole("button", { name: "Ya, lanjutkan" }));
    await waitFor(() => expect(api.del).toHaveBeenCalledWith("/goals/goal-standalone"));
  });
});

