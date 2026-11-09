import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getApiKeyInfo, getDevices, rotateApiKey } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { DevicesSettings } from "./DevicesSettings";

vi.mock("@/lib/api", () => ({
  getApiKeyInfo: vi.fn(),
  rotateApiKey: vi.fn(),
  getDevices: vi.fn(),
  createPairingCode: vi.fn(),
  revokeDevice: vi.fn(),
}));
vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn().mockResolvedValue(undefined) } }));

function show(language: "en" | "id" = "id") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = language === "id"
    ? render(<QueryClientProvider client={client}><DevicesSettings /></QueryClientProvider>)
    : renderWithIntl(<QueryClientProvider client={client}><DevicesSettings /></QueryClientProvider>, { language });
  return { ...view, user: userEvent.setup() };
}

/** Spec "API Key Section in Settings", "Help and Phone Status in Pengaturan" (apply-markas-pengaturan D6 to D8). */
describe("HP & perangkat", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
    vi.mocked(getApiKeyInfo).mockResolvedValue({
      ok: true,
      api_key: { id: "key-1", key_prefix: "cfk_abcd", created_at: "2026-10-01T00:00:00Z", last_used_at: null },
    });
  });

  afterEach(() => vi.unstubAllGlobals());

  it("orders Status HP, the download, the devices and the open manual key, and rotates it without disconnecting phones", async () => {
    vi.mocked(rotateApiKey).mockResolvedValue({ ok: true, api_key: "cfk_synthetic-new-key", key_prefix: "cfk_synt", message: "" });
    const log = vi.spyOn(console, "log");
    const { user, container } = show();
    const section = screen.getByRole("region", { name: "HP & perangkat" });
    const parts = ["Status HP", "Unduh aplikasi pendamping", "Perangkat terhubung", "Kunci API manual (cadangan)"].map((name) =>
      within(section).getByRole("region", { name }),
    );
    for (let index = 1; index < parts.length; index += 1) {
      expect(parts[index - 1].compareDocumentPosition(parts[index]) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    }
    const manual = parts[3];
    expect(manual.closest("details")).toBeNull();
    expect(await within(manual).findByText("Kunci cadangan · cfk_abcd••••••••••••")).toBeInTheDocument();
    expect(within(manual).getByText("dibuat 1 Okt 2026 · terakhir dipakai belum pernah")).toBeInTheDocument();
    expect(within(manual).getByText(/Ponsel yang dipasangkan lewat QR tetap terhubung/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);

    await user.click(within(manual).getByRole("button", { name: "Buat kunci baru (reset)" }));
    expect(within(manual).getByText(/Kunci manual lama langsung dicabut/)).toBeInTheDocument();
    expect(within(manual).getByRole("button", { name: "Batal" })).toHaveFocus();
    await user.click(within(manual).getByRole("button", { name: "Ya, buat kunci baru" }));
    const key = await within(manual).findByRole("textbox", { name: "Kunci API baru" });
    expect(key).toHaveValue("cfk_synthetic-new-key");
    expect(within(manual).getByText("Salin sekarang. Kunci ini hanya ditampilkan sekali.")).toBeInTheDocument();
    expect(rotateApiKey).toHaveBeenCalledTimes(1);

    // The key is shown once: "Tutup" clears it from the page, and it never reaches the console.
    await user.click(within(manual).getByRole("button", { name: "Tutup" }));
    expect(within(manual).queryByRole("textbox")).not.toBeInTheDocument();
    expect(container).not.toHaveTextContent("cfk_synthetic-new-key");
    expect(log.mock.calls.flat().join(" ")).not.toContain("cfk_synthetic-new-key");
    expect(within(manual).getByRole("button", { name: "Buat kunci baru (reset)" })).toHaveFocus();
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
    fireEvent(pairing, new Event("cancel", { cancelable: true }));

    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.getByRole("region", { name: "HP & perangkat" })).toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it("says a failed load in Status HP and the manual key instead of showing them empty (D21)", async () => {
    vi.mocked(getDevices).mockRejectedValue(new Error("offline"));
    vi.mocked(getApiKeyInfo).mockRejectedValue(new Error("offline"));
    show();
    const status = screen.getByRole("region", { name: "Status HP" });
    expect(await within(status).findByText("Daftar perangkat gagal dimuat.")).toBeInTheDocument();
    const manual = screen.getByRole("region", { name: "Kunci API manual (cadangan)" });
    expect(await within(manual).findByText("Info kunci API gagal dimuat.")).toBeInTheDocument();
    expect(within(manual).queryByText("Belum ada kunci")).not.toBeInTheDocument();
    expect(within(manual).getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
  });

  it("reads the section in English", async () => {
    show("en");
    const section = screen.getByRole("region", { name: "Phone & devices" });
    expect(within(section).getByRole("region", { name: "Phone status" })).toBeInTheDocument();
    expect(within(section).getByRole("region", { name: "Connected devices" })).toBeInTheDocument();
    expect(await within(section).findByText("No phone is paired yet.")).toBeInTheDocument();
    expect(within(section).getByRole("button", { name: "Create a new key (reset)" })).toBeInTheDocument();
    expect(section.querySelector("[lang='id']")).toBeNull();
  });
});
