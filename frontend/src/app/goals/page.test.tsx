import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import GoalsAndDebtsPage from "./page";

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
  minimum_payment: null,
  estimated_payoff_months: null,
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
  beforeEach(() => {
    vi.clearAllMocks();
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

    const progressActions = await screen.findAllByRole("button", { name: "+ Catat Progres" });
    expect(progressActions).toHaveLength(1);
    await user.click(progressActions[0]);
    expect(screen.getByRole("dialog", { name: "Catat Progres: Laptop" })).toBeVisible();
    expect(screen.queryByRole("combobox", { name: /rekening/i })).not.toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Tambahan Progres" }), "250000");
    await user.click(screen.getByRole("button", { name: "Simpan Progres" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/goals/goal-standalone/adjust", { amount: 250_000 }));
    expect(api.post).not.toHaveBeenCalledWith("/transactions", expect.anything());
  });

  it("requires confirmation before changing a standalone goal to account-backed progress", async () => {
    const user = userEvent.setup();
    renderGoals();
    await user.click(await screen.findByRole("button", { name: "Ubah target Laptop" }));
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
    await user.click(await screen.findByRole("button", { name: "Ubah tagihan Kartu Kredit" }));

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
});
