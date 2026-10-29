import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  api, createPairingCode, deleteNotificationAlias, getApiKeyInfo, getDevices, getNotificationAliases, getSenderAliases,
  rotateApiKey,
} from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { SettingsModal } from "./SettingsModal";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), patch: vi.fn() },
  getApiKeyInfo: vi.fn(),
  rotateApiKey: vi.fn(),
  getNotificationAliases: vi.fn(),
  deleteNotificationAlias: vi.fn(),
  getSenderAliases: vi.fn(),
  updateSenderAlias: vi.fn(),
  deleteSenderAlias: vi.fn(),
  getDevices: vi.fn(),
  createPairingCode: vi.fn(),
  revokeDevice: vi.fn(),
}));
vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn().mockResolvedValue(undefined) } }));
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
    vi.mocked(getSenderAliases).mockResolvedValue({ ok: true, aliases: [] });
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
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

  it("labels the manual key as the fallback and rotates it without disconnecting paired phones", async () => {
    const user = userEvent.setup();
    vi.mocked(getApiKeyInfo).mockResolvedValue({
      ok: true,
      api_key: { id: "key-1", key_prefix: "cfk_abcd", created_at: "2026-10-01T00:00:00Z", last_used_at: null },
    });
    vi.mocked(rotateApiKey).mockResolvedValue({ ok: true, api_key: "cfk_synthetic-new-key", key_prefix: "cfk_synt", message: "" });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <SettingsModal open onClose={() => {}} />
      </QueryClientProvider>,
    );

    const manual = await screen.findByRole("region", { name: "API key manual (cadangan)" });
    const devices = screen.getByRole("region", { name: "Perangkat terhubung" });
    expect(devices.compareDocumentPosition(manual) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(within(manual).getByText(/ponsel yang dipasangkan lewat QR tetap terhubung/)).toBeInTheDocument();
    await user.click(within(manual).getByRole("button", { name: "Buat Kunci Baru (Reset)" }));
    expect(within(manual).getByText(/Kunci manual lama langsung dicabut/)).toBeInTheDocument();
    await user.click(within(manual).getByRole("button", { name: "Ya, Buat Kunci Baru" }));

    expect(await within(manual).findByRole("textbox", { name: "Kunci API baru" })).toHaveValue("cfk_synthetic-new-key");
  });

  it("closes only the pairing dialog on Escape and returns focus to its trigger", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    vi.mocked(createPairingCode).mockResolvedValue({
      ok: true, code: "ABCDE12345", expires_at: new Date(Date.now() + 300_000).toISOString(), expires_in_seconds: 300,
      qr_payload: "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=ABCDE12345",
    });
    const onClose = vi.fn();
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <SettingsModal open onClose={onClose} />
      </QueryClientProvider>,
    );

    const trigger = await screen.findByRole("button", { name: "Hubungkan ponsel" });
    await user.click(trigger);
    const pairing = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    expect(await within(pairing).findByText("ABCDE 12345")).toBeInTheDocument();
    await user.tab();
    expect(pairing).toContainElement(document.activeElement as HTMLElement);
    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog", { name: "Hubungkan ponsel" })).not.toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Pengaturan Akun & Keuangan" })).toBeInTheDocument();
    expect(onClose).not.toHaveBeenCalled();
    expect(trigger).toHaveFocus();
    vi.unstubAllGlobals();
  });
});
