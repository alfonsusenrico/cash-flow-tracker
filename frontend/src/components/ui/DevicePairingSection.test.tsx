import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import QRCode from "qrcode";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getDevices, revokeDevice, type PairedDevice } from "@/lib/api";
import { DevicePairingSection } from "./DevicePairingSection";

vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn() } }));
vi.mock("@/lib/api", () => ({
  createPairingCode: vi.fn(),
  getDevices: vi.fn(),
  revokeDevice: vi.fn(),
}));

const NOW = Date.parse("2026-10-29T03:00:00Z");
const manualKey: PairedDevice = {
  id: "key-manual", name: "API key manual", created_via: "manual", key_prefix: "cfk_abcd",
  created_at: "2026-09-01T02:00:00Z", last_used_at: null,
};
const phone: PairedDevice = {
  id: "key-phone", name: "TECNO CN7c", created_via: "pairing", key_prefix: "cfk_wxyz",
  created_at: "2026-10-20T02:00:00Z", last_used_at: "2026-10-28T09:30:00Z",
};

function code(value: string) {
  return {
    ok: true,
    code: value,
    expires_at: new Date(Date.now() + 300_000).toISOString(),
    expires_in_seconds: 300,
    qr_payload: `cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=${value}`,
  };
}

function renderSection() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <DevicePairingSection enabled />
    </QueryClientProvider>,
  );
}

async function tick(ms: number) {
  await act(async () => {
    vi.advanceTimersByTime(ms);
  });
}

describe("paired devices in Settings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true, now: NOW });
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [phone, manualKey] });
    vi.mocked(QRCode.toCanvas).mockResolvedValue(undefined as never);
    vi.mocked(createPairingCode)
      .mockReset()
      .mockResolvedValueOnce(code("ABCDE12345"))
      .mockResolvedValueOnce(code("FGHJK67890"));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("lists paired phones with paired and last-active dates, without the manual key", async () => {
    renderSection();

    const section = await screen.findByRole("region", { name: "Perangkat terhubung" });
    expect(await within(section).findByText("TECNO CN7c")).toBeInTheDocument();
    expect(within(section).getByText(/Dipasangkan 20 Okt 2026/)).toBeInTheDocument();
    expect(within(section).getByText(/Terakhir aktif 28 Okt 2026/)).toBeInTheDocument();
    expect(within(section).queryByText("API key manual")).not.toBeInTheDocument();
    expect((await axe(section)).violations).toHaveLength(0);
  });

  it("shows an empty state when no phone is paired", async () => {
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [manualKey] });
    renderSection();

    expect(await screen.findByText("Belum ada ponsel yang dipasangkan.")).toBeInTheDocument();
  });

  it("revokes a phone only after confirming in a dialog", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    vi.mocked(revokeDevice).mockResolvedValue({ ok: true });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Cabut TECNO CN7c" }));
    const confirm = await screen.findByRole("dialog", { name: "Cabut TECNO CN7c?" });
    expect((await axe(confirm)).violations).toHaveLength(0);
    await user.click(within(confirm).getByRole("button", { name: "Batal" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(revokeDevice).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Cabut TECNO CN7c" })).toHaveFocus();

    await user.click(screen.getByRole("button", { name: "Cabut TECNO CN7c" }));
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(revokeDevice).not.toHaveBeenCalled();

    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [manualKey] });
    await user.click(screen.getByRole("button", { name: "Cabut TECNO CN7c" }));
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Cabut perangkat" }));

    await waitFor(() => expect(revokeDevice).toHaveBeenCalledWith("key-phone"));
    expect(await screen.findByRole("status")).toHaveTextContent("TECNO CN7c dicabut. Ponsel itu harus dipasangkan ulang.");
    expect(await screen.findByText("Belum ada ponsel yang dipasangkan.")).toBeInTheDocument();
  });

  it("opens a pairing dialog with the QR code, the code as text and a countdown", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderSection();

    const trigger = await screen.findByRole("button", { name: "Hubungkan ponsel" });
    await user.click(trigger);
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    const qr = await within(dialog).findByRole("img", { name: "Kode QR pemasangan ponsel, kode ABCDE 12345" });

    expect(createPairingCode).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(QRCode.toCanvas).toHaveBeenCalledWith(
      qr,
      "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=ABCDE12345",
      expect.objectContaining({ width: 240 }),
    ));
    expect(qr).toHaveAttribute("width", "240");
    expect(within(dialog).getByText("ABCDE 12345")).toBeInTheDocument();
    expect(within(dialog).getByRole("timer")).toHaveTextContent("Berlaku 05:00");
    expect(within(dialog).getByText(/hanya bisa dipakai sekali/)).toBeInTheDocument();
    expect(dialog).toContainElement(document.activeElement as HTMLElement);
    expect((await axe(dialog)).violations).toHaveLength(0);

    await tick(61_000);
    expect(within(dialog).getByRole("timer")).toHaveTextContent("Berlaku 03:59");

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it("says the code expired at zero and issues a new one on request", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    await within(dialog).findByRole("img");
    await tick(300_000);

    expect(within(dialog).getByRole("status")).toHaveTextContent("Kode kedaluwarsa. Buat kode baru untuk memasangkan ponsel.");
    expect(within(dialog).queryByRole("img")).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("timer")).not.toBeInTheDocument();
    expect((await axe(dialog)).violations).toHaveLength(0);

    await user.click(within(dialog).getByRole("button", { name: "Buat kode baru" }));
    expect(await within(dialog).findByRole("img", { name: /FGHJK 67890/ })).toBeInTheDocument();
    expect(createPairingCode).toHaveBeenCalledTimes(2);
    expect(within(dialog).getByRole("timer")).toHaveTextContent("Berlaku 05:00");
    expect(within(dialog).getByRole("status")).toHaveTextContent("");
  });

  it("regenerates before expiry and shows only the newest code", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    await within(dialog).findByText("ABCDE 12345");
    await tick(120_000);
    await user.click(within(dialog).getByRole("button", { name: "Buat kode baru" }));

    expect(await within(dialog).findByText("FGHJK 67890")).toBeInTheDocument();
    expect(within(dialog).queryByText("ABCDE 12345")).not.toBeInTheDocument();
    expect(within(dialog).getByRole("timer")).toHaveTextContent("Berlaku 05:00");
  });

  it("confirms when the phone pairs while the dialog is open", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [manualKey] });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    await within(dialog).findByRole("img");
    vi.mocked(getDevices).mockResolvedValue({
      ok: true,
      devices: [{ ...phone, id: "key-new", created_at: new Date(Date.now() + 1000).toISOString(), last_used_at: null }, manualKey],
    });
    await tick(3_500);

    await waitFor(() => expect(within(dialog).getByRole("status")).toHaveTextContent("TECNO CN7c terhubung."));
    expect(within(dialog).queryByRole("img")).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Selesai" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("reports a code that could not be created", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    vi.mocked(createPairingCode).mockReset().mockRejectedValue(new Error("Pemasangan ponsel belum dikonfigurasi di server."));
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });

    expect(await within(dialog).findByRole("alert")).toHaveTextContent("Pemasangan ponsel belum dikonfigurasi di server.");
    expect(within(dialog).queryByRole("img")).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Buat kode baru" })).toBeEnabled();
  });
});
