import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, deleteNotificationAlias, getApiKeyInfo, getNotificationAliases } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { SettingsModal } from "./SettingsModal";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), patch: vi.fn() },
  getApiKeyInfo: vi.fn(),
  rotateApiKey: vi.fn(),
  getNotificationAliases: vi.fn(),
  deleteNotificationAlias: vi.fn(),
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
    vi.mocked(getNotificationAliases).mockResolvedValue({ ok: true, aliases: [] });
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

  it("saves bank-notification name aliases as a trimmed list", async () => {
    const user = userEvent.setup();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") {
        return { ok: true, user: { username: "owner", name: "Owner", name_aliases: ["Owner Full Name"], payday_day: 25, currency: "IDR", emergency_fund_multiplier: 6, monthly_spending_budget: null } };
      }
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16_000 };
      throw new Error(`Unexpected GET ${path}`);
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <SettingsModal open onClose={() => {}} />
      </QueryClientProvider>,
    );

    const aliases = await screen.findByRole("textbox", { name: /nama di notifikasi bank/i });
    await waitFor(() => expect(aliases).toHaveValue("Owner Full Name"));
    expect(aliases).toHaveAccessibleDescription(/pisahkan dengan koma/i);
    await user.clear(aliases);
    await user.type(aliases, " Owner Full Name ,  OWNER F N,, ");
    await user.click(screen.getByRole("button", { name: "Simpan pengaturan" }));

    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/auth/settings", expect.objectContaining({
      name_aliases: ["Owner Full Name", "OWNER F N"],
    })));
  });

  it("rejects aliases shorter than three characters without saving", async () => {
    const user = userEvent.setup();
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <SettingsModal open onClose={() => {}} />
      </QueryClientProvider>,
    );

    const aliases = await screen.findByRole("textbox", { name: /nama di notifikasi bank/i });
    await user.type(aliases, "AB");
    await user.click(screen.getByRole("button", { name: "Simpan pengaturan" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Setiap nama alias harus 3–150 karakter");
    expect(api.patch).not.toHaveBeenCalled();
  });

  it("lists learned bank-notification names and removes a wrong one", async () => {
    const user = userEvent.setup();
    vi.mocked(getNotificationAliases).mockResolvedValue({
      ok: true,
      aliases: [
        { id: "alias-1", institution: "jago", name: "GoPay Tabungan", account_id: "acc-1", account: "GoPay", source: "ai", created_at: "2026-09-29T10:00:00Z" },
        { id: "alias-2", institution: "jago", name: "Tabungan Liburan", account_id: "acc-2", account: "Bank Jago · Liburan", source: "owner", created_at: "2026-09-29T10:00:00Z" },
      ],
    });
    vi.mocked(deleteNotificationAlias).mockResolvedValue({ ok: true });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <SettingsModal open onClose={() => {}} />
      </QueryClientProvider>,
    );

    const section = await screen.findByRole("region", { name: "Nama dari Notifikasi Bank" });
    expect(await within(section).findByText("Dipetakan otomatis")).toBeInTheDocument();
    expect(within(section).getByText("Dikonfirmasi Anda")).toBeInTheDocument();
    expect((await axe(section)).violations).toHaveLength(0);
    await user.click(within(section).getByRole("button", { name: "Hapus pemetaan GoPay Tabungan" }));
    await waitFor(() => expect(deleteNotificationAlias).toHaveBeenCalledWith("alias-1"));
    await waitFor(() => expect(getNotificationAliases).toHaveBeenCalledTimes(2));
  });
});
