import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getApiKeyInfo, getDevices, rotateApiKey } from "@/lib/api";
import { DevicesSettings } from "./DevicesSettings";

vi.mock("@/lib/api", () => ({
  getApiKeyInfo: vi.fn(),
  rotateApiKey: vi.fn(),
  getDevices: vi.fn(),
  createPairingCode: vi.fn(),
  revokeDevice: vi.fn(),
}));
vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn().mockResolvedValue(undefined) } }));

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = render(<QueryClientProvider client={client}><DevicesSettings /></QueryClientProvider>);
  return { ...view, user: userEvent.setup() };
}

describe("Perangkat & akses", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
    vi.mocked(getApiKeyInfo).mockResolvedValue({
      ok: true,
      api_key: { id: "key-1", key_prefix: "cfk_abcd", created_at: "2026-10-01T00:00:00Z", last_used_at: null },
    });
  });

  it("puts the collapsed manual key after the devices and rotates it without disconnecting paired phones", async () => {
    vi.mocked(rotateApiKey).mockResolvedValue({ ok: true, api_key: "cfk_synthetic-new-key", key_prefix: "cfk_synt", message: "" });
    const { user, container } = show();
    const devices = await screen.findByRole("region", { name: "Perangkat terhubung" });
    const disclosure = screen.getByText("Kunci API manual (cadangan)").closest("details")!;
    expect(disclosure).not.toHaveAttribute("open");
    expect(devices.compareDocumentPosition(disclosure) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect((await axe(container)).violations).toHaveLength(0);

    await user.click(screen.getByText("Kunci API manual (cadangan)"));
    expect(disclosure).toHaveAttribute("open");
    const manual = screen.getByRole("region", { name: "Kunci API manual (cadangan)" });
    expect(within(manual).getByText(/ponsel yang dipasangkan lewat QR tetap terhubung/)).toBeInTheDocument();
    expect(await within(manual).findByText("cfk_abcd••••••••••••")).toBeInTheDocument();
    await user.click(within(manual).getByRole("button", { name: "Buat Kunci Baru (Reset)" }));
    expect(within(manual).getByText(/Kunci manual lama langsung dicabut/)).toBeInTheDocument();
    await user.click(within(manual).getByRole("button", { name: "Ya, Buat Kunci Baru" }));
    expect(await within(manual).findByRole("textbox", { name: "Kunci API baru" })).toHaveValue("cfk_synthetic-new-key");
  });

  it("closes only the pairing dialog on Escape and returns focus to its trigger", async () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    vi.mocked(createPairingCode).mockResolvedValue({
      ok: true, code: "ABCDE12345", expires_at: new Date(Date.now() + 300_000).toISOString(), expires_in_seconds: 300,
      qr_payload: "cashflow://pair?v=1&server=https%3A%2F%2Fexample.test&code=ABCDE12345",
    });
    const { user } = show();
    const trigger = await screen.findByRole("button", { name: "Hubungkan ponsel" });
    await user.click(trigger);
    const pairing = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    expect(await within(pairing).findByText("ABCDE 12345")).toBeInTheDocument();
    await user.tab();
    expect(pairing).toContainElement(document.activeElement as HTMLElement);
    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Perangkat & akses" })).toBeInTheDocument();
    expect(trigger).toHaveFocus();
    vi.unstubAllGlobals();
  });
});
