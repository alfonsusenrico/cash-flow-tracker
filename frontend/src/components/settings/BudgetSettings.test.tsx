import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { queryKeys } from "@/lib/queryKeys";
import { BudgetSettings } from "./BudgetSettings";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), patch: vi.fn() } }));
const ctx = vi.hoisted(() => ({ hide: false }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ hideBalances: ctx.hide, bal: () => (ctx.hide ? "Rp ••••••" : "Rp 0") }),
}));

const owner = {
  username: "owner", name: "Owner", name_aliases: ["Owner Full Name"], currency: "IDR",
  emergency_fund_multiplier: 6, monthly_spending_budget: null,
};

function show(language: "en" | "id" = "id") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const invalidate = vi.spyOn(client, "invalidateQueries");
  const view = renderWithIntl(<QueryClientProvider client={client}><BudgetSettings /></QueryClientProvider>, { language });
  return { ...view, invalidate, user: userEvent.setup() };
}

describe("Mata uang & anggaran settings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    ctx.hide = false;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: owner };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16_000 };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("saves only region, currency, emergency months and the monthly limit and refreshes money views", async () => {
    const { user, invalidate, container } = show();
    const currency = await screen.findByRole("radiogroup", { name: "Mata uang tampilan" });
    await waitFor(() => expect(screen.getByRole("radio", { name: "IDR" })).toHaveAttribute("aria-checked", "true"));
    expect(await screen.findByText(/Kurs hari ini 1 USD ≈ Rp 16\.000\. Data tetap tersimpan dalam rupiah\./)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);

    await user.click(screen.getByRole("radio", { name: "USD" }));
    expect(currency).toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Batas pengeluaran bulanan" }), "10000000");
    expect(screen.getByRole("textbox", { name: "Batas pengeluaran bulanan" })).toHaveValue("10.000.000");
    expect(screen.getByRole("textbox", { name: "Batas pengeluaran bulanan" })).toHaveAccessibleDescription("Kosongkan untuk memakai total anggaran kategori.");
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledTimes(1));
    expect(api.patch).toHaveBeenCalledWith("/auth/settings", {
      currency: "USD", region: "ID", emergency_fund_multiplier: 6, monthly_spending_budget: 10_000_000,
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
    const limit = await screen.findByRole("textbox", { name: "Batas pengeluaran bulanan" });
    await waitFor(() => expect(limit).toHaveValue("7.500.000"));
    await user.clear(limit);
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/auth/settings", {
      currency: "IDR", region: "ID", emergency_fund_multiplier: 6, monthly_spending_budget: null,
    }));
  });

  it("refuses an emergency target outside 1–36 months without saving", async () => {
    const { user } = show();
    const months = await screen.findByRole("spinbutton", { name: "Target dana darurat (bulan)" });
    await waitFor(() => expect(months).toHaveValue(6));
    await user.clear(months);
    await user.type(months, "40");
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Target dana darurat harus antara 1 dan 36 bulan");
    expect(api.patch).not.toHaveBeenCalled();
  });

  it("saves the stored limit unchanged while balances are hidden (spec \"Saving with balances hidden\")", async () => {
    ctx.hide = true;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: { ...owner, monthly_spending_budget: 4_800_000 } };
      return { ok: true, usdidr: 16_000 };
    });
    const { user } = show();
    const limit = await screen.findByRole("textbox", { name: "Batas pengeluaran bulanan" });
    await waitFor(() => expect(limit).toHaveValue("Rp ••••••"));
    expect(limit).toHaveAttribute("readonly");
    expect(limit).toHaveAccessibleDescription("Saldo disembunyikan. Matikan Sembunyikan saldo untuk mengubah.");
    expect(screen.queryByText(/16\.000/)).not.toBeInTheDocument();
    const months = screen.getByRole("spinbutton", { name: "Target dana darurat (bulan)" });
    await user.clear(months);
    await user.type(months, "9");
    await user.click(screen.getByRole("button", { name: "Simpan anggaran" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/auth/settings", {
      currency: "IDR", region: "ID", emergency_fund_multiplier: 9, monthly_spending_budget: 4_800_000,
    }));
  });

  it("reads English (spec \"English account\")", async () => {
    show("en");
    expect(await screen.findByRole("heading", { level: 2, name: "Currency & budget" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Monthly spending limit" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save budget" })).toBeInTheDocument();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
  });
});
