import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import QRCode from "qrcode";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getDevices } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import {
  COMPANION_DOWNLOAD_URL,
  COMPANION_RELEASE_URL,
  CompanionDownloadSection,
} from "./CompanionDownload";
import { DevicePairingDialog } from "./DevicePairingDialog";

vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn() } }));
vi.mock("@/lib/api", () => ({
  createPairingCode: vi.fn(),
  getDevices: vi.fn(),
}));

const PAIRING_PAYLOAD = "cashflow://pair?v=1&server=https%3A%2F%2Ftracker.example.invalid&code=ABCDE12345";

function renderDialog() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <DevicePairingDialog onClose={() => {}} />
    </QueryClientProvider>,
  );
}

describe("companion app download", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(QRCode.toCanvas).mockResolvedValue(undefined as never);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("offers the stable latest-release APK with a QR, the checksum page and the minimum Android version", async () => {
    const { container } = render(<CompanionDownloadSection />);

    const section = screen.getByRole("region", { name: "Unduh aplikasi pendamping" });
    expect(COMPANION_DOWNLOAD_URL).toBe(
      "https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk",
    );
    expect(within(section).getByRole("link", { name: "Unduh cashflow-companion.apk" })).toHaveAttribute(
      "href",
      COMPANION_DOWNLOAD_URL,
    );
    expect(within(section).getByRole("link", { name: "halaman rilis" })).toHaveAttribute("href", COMPANION_RELEASE_URL);
    expect(within(section).getByText(/Butuh Android 8\.0 atau lebih baru\. Cocokkan SHA-256 berkas di/)).toHaveTextContent(
      "Butuh Android 8.0 atau lebih baru. Cocokkan SHA-256 berkas di halaman rilis.",
    );
    expect(within(section).getByRole("img", { name: "Kode QR tautan unduhan aplikasi pendamping" })).toBeInTheDocument();
    await waitFor(() =>
      expect(QRCode.toCanvas).toHaveBeenCalledWith(
        expect.any(HTMLCanvasElement),
        COMPANION_DOWNLOAD_URL,
        expect.objectContaining({ errorCorrectionLevel: "M", color: { dark: "#000000", light: "#ffffff" } }),
      ),
    );
    // The download QR fills the prototype's 136 px box: 118 css px, drawn at the device pixel ratio.
    const canvas = within(section).getByRole("img", { name: "Kode QR tautan unduhan aplikasi pendamping" });
    expect(canvas).toHaveStyle({ width: "118px", height: "118px" });
    expect(canvas.closest(".qr")).not.toBeNull();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("reads the download in English", () => {
    renderWithIntl(<CompanionDownloadSection />, { language: "en" });
    const section = screen.getByRole("region", { name: "Download the companion app" });
    expect(section).toHaveTextContent("Needs Android 8.0 or later. Check the file's SHA-256 on the release page.");
    expect(within(section).getByRole("link", { name: "release page" })).toHaveAttribute("href", COMPANION_RELEASE_URL);
  });

  it("keeps the download link usable when the QR cannot be drawn", async () => {
    vi.mocked(QRCode.toCanvas).mockRejectedValue(new Error("canvas unavailable"));
    render(<CompanionDownloadSection />);

    expect(await screen.findByRole("alert")).toHaveTextContent("Kode QR gagal ditampilkan. Pakai tautan unduhan.");
    expect(screen.getByRole("link", { name: "Unduh cashflow-companion.apk" })).toHaveAttribute("href", COMPANION_DOWNLOAD_URL);
  });

  it("keeps the download collapsed in the pairing dialog so its QR is not mistaken for the pairing code", async () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
    vi.mocked(createPairingCode).mockResolvedValue({
      ok: true,
      code: "ABCDE12345",
      expires_at: new Date(Date.now() + 300_000).toISOString(),
      expires_in_seconds: 300,
      qr_payload: PAIRING_PAYLOAD,
    });
    const user = userEvent.setup();
    renderDialog();

    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    expect(await within(dialog).findByRole("img", { name: /Kode QR pemasangan ponsel/ })).toBeInTheDocument();
    const summary = within(dialog).getByText("Belum punya aplikasinya? Unduh aplikasi pendamping");
    const disclosure = summary.closest("details")!;
    expect(disclosure).not.toHaveAttribute("open");

    await user.click(summary);
    expect(disclosure).toHaveAttribute("open");
    const download = within(dialog).getByRole("region", { name: "Belum punya aplikasinya? Unduh aplikasi pendamping" });
    expect(within(download).getByRole("link", { name: "Unduh cashflow-companion.apk" })).toHaveAttribute(
      "href",
      COMPANION_DOWNLOAD_URL,
    );
    expect(within(download).getByText(/Butuh Android 8\.0 atau lebih baru\./)).toBeInTheDocument();
    await waitFor(() => {
      const payloads = vi.mocked(QRCode.toCanvas).mock.calls.map((call) => call[1]);
      expect(payloads).toContain(PAIRING_PAYLOAD);
      expect(payloads).toContain(COMPANION_DOWNLOAD_URL);
    });
    expect((await axe(dialog)).violations).toHaveLength(0);
  });
});
