import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, getApiKeyInfo } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { SettingsModal } from "./SettingsModal";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), patch: vi.fn() },
  getApiKeyInfo: vi.fn(),
  rotateApiKey: vi.fn(),
}));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    user: { username: "owner", name: "Owner", payday_day: 25, currency: "IDR", emergency_fund_multiplier: 6 },
  }),
}));

describe("settings form", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") {
        return { ok: true, user: { username: "owner", name: "Owner", payday_day: 25, currency: "IDR", emergency_fund_multiplier: 6, monthly_spending_budget: null } };
      }
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16_000 };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(getApiKeyInfo).mockResolvedValue({ ok: true, api_key: null });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
  });

  it("saves supported profile settings and refreshes canonical consumers", async () => {
    const user = userEvent.setup();
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    render(
      <QueryClientProvider client={queryClient}>
        <SettingsModal open onClose={() => {}} />
      </QueryClientProvider>,
    );

    const name = await screen.findByRole("textbox", { name: /nama tampilan/i });
    await waitFor(() => expect(name).toHaveValue("Owner"));
    expect((await axe(screen.getByRole("dialog", { name: "Pengaturan Akun & Keuangan" }))).violations).toHaveLength(0);
    await user.clear(name);
    await user.type(name, "Updated Owner");
    await user.selectOptions(screen.getByRole("combobox", { name: "Mata Uang Utama" }), "USD");
    await user.click(screen.getByRole("button", { name: "Simpan pengaturan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/auth/settings", expect.objectContaining({
      name: "Updated Owner",
      currency: "USD",
      payday_day: 25,
      emergency_fund_multiplier: 6,
    })));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.auth.all });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.pulse });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.dashboard.all });
  });
});
