import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { BudgetSettings } from "./BudgetSettings";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), patch: vi.fn() } }));

const owner = {
  username: "owner", name: "Owner", name_aliases: ["Owner Full Name"], currency: "IDR",
  emergency_fund_multiplier: 6, monthly_spending_budget: null,
};

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const invalidate = vi.spyOn(client, "invalidateQueries");
  const view = render(<QueryClientProvider client={client}><BudgetSettings /></QueryClientProvider>);
  return { ...view, invalidate, user: userEvent.setup() };
}

describe("Mata uang & anggaran settings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: owner };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16_000 };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("saves only currency, emergency months and the monthly limit and refreshes money views", async () => {
    const { user, invalidate, container } = show();
    const currency = await screen.findByRole("combobox", { name: "Mata uang tampilan" });
    await waitFor(() => expect(currency).toHaveValue("IDR"));
    expect(await screen.findByText(/1 USD ≈ Rp 16\.000/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);

    await user.selectOptions(currency, "USD");
    await user.type(screen.getByRole("textbox", { name: "Batas belanja operasional bulanan (IDR)" }), "10000000");
    expect(screen.getByRole("textbox", { name: "Batas belanja operasional bulanan (IDR)" })).toHaveValue("10.000.000");
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledTimes(1));
    expect(api.patch).toHaveBeenCalledWith("/auth/settings", {
      currency: "USD", emergency_fund_multiplier: 6, monthly_spending_budget: 10_000_000,
    });
    for (const queryKey of [queryKeys.auth.all, queryKeys.pulse, queryKeys.dashboard.all, queryKeys.goals, queryKeys.recurring.all]) {
      expect(invalidate).toHaveBeenCalledWith({ queryKey });
    }
  });

  it("clears the monthly limit with an explicit null", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: { ...owner, monthly_spending_budget: 7_500_000 } };
      return { ok: true, usdidr: 16_000 };
    });
    const { user } = show();
    const limit = await screen.findByRole("textbox", { name: "Batas belanja operasional bulanan (IDR)" });
    await waitFor(() => expect(limit).toHaveValue("7.500.000"));
    await user.clear(limit);
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/auth/settings", {
      currency: "IDR", emergency_fund_multiplier: 6, monthly_spending_budget: null,
    }));
  });

  it("refuses an emergency target outside 1–36 months without saving", async () => {
    const { user } = show();
    const months = await screen.findByRole("spinbutton", { name: "Target dana darurat (bulan)" });
    await waitFor(() => expect(months).toHaveValue(6));
    expect(months).toHaveAccessibleDescription(/1–36 bulan/);
    await user.clear(months);
    await user.type(months, "40");
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Target dana darurat harus antara 1 dan 36 bulan");
    expect(api.patch).not.toHaveBeenCalled();
  });
});
