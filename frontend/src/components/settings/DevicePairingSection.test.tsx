import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import QRCode from "qrcode";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getDevices, revokeDevice, type PairedDevice } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { healthReport, pairedPhone } from "@/test/deviceHealthFixtures";
import { DevicePairingSection } from "./DevicePairingSection";
import { PhoneStatus } from "./PhoneStatus";

vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn() } }));
vi.mock("@/lib/api", () => ({
  createPairingCode: vi.fn(),
  getDevices: vi.fn(),
  revokeDevice: vi.fn(),
}));

const NOW = Date.parse("2026-10-29T03:00:00Z");
const manualKey: PairedDevice = {
  id: "key-manual", name: "API key manual", created_via: "manual", key_prefix: "cfk_abcd",
  created_at: "2026-09-01T02:00:00Z", last_used_at: null, health: null,
};
const phone: PairedDevice = {
  id: "key-phone", name: "TECNO CN7c", created_via: "pairing", key_prefix: "cfk_wxyz",
  created_at: "2026-10-20T02:00:00Z", last_used_at: "2026-10-28T09:30:00Z", health: null,
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
      <DevicePairingSection />
    </QueryClientProvider>,
  );
}

/** Escape on a modal <dialog> fires its `cancel` event in a browser; jsdom has no such default, so it is sent here. */
function pressEscape() {
  const dialog = screen.getByRole("dialog");
  fireEvent(dialog, new Event("cancel", { cancelable: true }));
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
    expect(within(section).getByText("dipasangkan 20 Okt 2026 · terakhir aktif 28 Okt 2026 16.30")).toBeInTheDocument();
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
    pressEscape();
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
      expect.objectContaining({ width: 200 }),
    ));
    expect(qr).toHaveStyle({ width: "200px" });
    expect(within(dialog).getByText("ABCDE 12345")).toBeInTheDocument();
    expect(within(dialog).getByText("Pindai kode ini. Berlaku 05:00.")).toBeInTheDocument();
    expect(within(dialog).getByText(/hanya dapat dipakai sekali/)).toBeInTheDocument();
    expect(dialog).toContainElement(document.activeElement as HTMLElement);
    expect((await axe(dialog)).violations).toHaveLength(0);

    await tick(61_000);
    expect(within(dialog).getByText("Pindai kode ini. Berlaku 03:59.")).toBeInTheDocument();

    pressEscape();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it("says the code expired at zero and issues a new one on request", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    await within(dialog).findByRole("img", { name: /Kode QR pemasangan ponsel/ });
    await tick(300_000);

    expect(within(dialog).getByRole("status")).toHaveTextContent("Kode sudah kedaluwarsa. Buat kode baru untuk menyambungkan HP.");
    expect(within(dialog).queryByRole("img", { name: /Kode QR pemasangan ponsel/ })).not.toBeInTheDocument();
    expect(within(dialog).queryByText(/Pindai kode ini/)).not.toBeInTheDocument();
    expect(within(dialog).getByText("Kode sudah kedaluwarsa.")).toBeInTheDocument();
    expect((await axe(dialog)).violations).toHaveLength(0);

    await user.click(within(dialog).getByRole("button", { name: "Buat kode baru" }));
    expect(await within(dialog).findByRole("img", { name: /FGHJK 67890/ })).toBeInTheDocument();
    expect(createPairingCode).toHaveBeenCalledTimes(2);
    expect(within(dialog).getByText("Pindai kode ini. Berlaku 05:00.")).toBeInTheDocument();
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
    expect(within(dialog).getByText("Pindai kode ini. Berlaku 05:00.")).toBeInTheDocument();
  });

  it("switches to the checklist when the phone pairs while the dialog is open", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [manualKey] });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
    await within(dialog).findByRole("img", { name: /Kode QR pemasangan ponsel/ });
    vi.mocked(getDevices).mockResolvedValue({
      ok: true,
      devices: [{ ...phone, id: "key-new", created_at: new Date(Date.now() + 1000).toISOString(), last_used_at: null }, manualKey],
    });
    await tick(3_500);

    await waitFor(() => expect(within(dialog).getByRole("status")).toHaveTextContent("HP terhubung. Lanjutkan pengaturan di HP."));
    expect(within(dialog).queryByRole("img", { name: /Kode QR pemasangan ponsel/ })).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Selesai" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Hubungkan ponsel" })).toHaveFocus();
  });

  it("reports a code that could not be created", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    vi.mocked(createPairingCode).mockReset().mockRejectedValue(new Error("Pemasangan ponsel belum dikonfigurasi di server."));
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Hubungkan ponsel" }));
    const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });

    expect(await within(dialog).findByRole("alert")).toHaveTextContent("Pemasangan ponsel belum dikonfigurasi di server.");
    expect(within(dialog).queryByRole("img", { name: /Kode QR pemasangan ponsel/ })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Buat kode baru" })).toBeEnabled();
  });
});

describe("Status HP (apply-markas-pengaturan D7)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true, now: NOW });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function renderStatus(devices: PairedDevice[], language: "en" | "id" = "id") {
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [...devices, manualKey] });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return renderWithIntl(
      <QueryClientProvider client={queryClient}>
        <PhoneStatus />
      </QueryClientProvider>,
      { language },
    );
  }

  async function checks(name: string) {
    const list = await screen.findByRole("list", { name: `Pemeriksaan ${name}` });
    return within(list).getAllByRole("listitem").map((item) => item.textContent);
  }

  it("shows a ready phone with its last report, app version and every check as Baik", async () => {
    const { container } = renderStatus([
      pairedPhone(healthReport(NOW, { reported_at: new Date(NOW - 3600_000).toISOString(), app_version: "2.4.0" })),
    ]);
    const box = await screen.findByRole("region", { name: "Status HP" });
    expect(await within(box).findByText(/TECNO CN7c ·/)).toHaveTextContent("TECNO CN7c · siap");
    expect(within(box).getByText(/Laporan terakhir 29 Okt 2026/)).toHaveTextContent("aplikasi 2.4.0");
    expect(await checks("TECNO CN7c")).toEqual([
      "Izin baca notifikasiBaik", "Jalan di latar belakangBaik", "Tes tangkap notifikasiBaik", "Mengirim kabarBaik",
    ]);
    // The companion line comes first, as text.
    expect(within(box).getByText("HP terhubung").compareDocumentPosition(within(box).getByText(/TECNO CN7c ·/)) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(container.querySelector("[class*='in-bg'], [class*='warn-bg'], [class*='out-bg']")).toBeNull();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("lists each problem of a phone that needs attention beside its check", async () => {
    renderStatus([pairedPhone(healthReport(NOW, {
      notification_access: false,
      listener_connected: false,
      self_test: { status: "failed", failed_step: "capture" },
    }))]);
    expect(await screen.findByText(/TECNO CN7c ·/)).toHaveTextContent("TECNO CN7c · perlu dicek");
    expect(await checks("TECNO CN7c")).toEqual([
      "Izin baca notifikasiIzin notifikasi mati.",
      "Jalan di latar belakangAplikasi tidak berjalan di latar belakang.",
      "Tes tangkap notifikasiTes tangkap notifikasi gagal. Notifikasi tes tidak tertangkap oleh aplikasi.",
      "Mengirim kabarBaik",
    ]);
  });

  it("names a self-test that never ran", async () => {
    renderStatus([pairedPhone(healthReport(NOW, { self_test: { status: "not_run", at: null } }))]);
    expect((await checks("TECNO CN7c"))[2]).toBe("Tes tangkap notifikasiTes tangkap notifikasi belum dijalankan.");
  });

  it("flags a ready phone that has been silent for more than 24 hours", async () => {
    renderStatus([pairedPhone(healthReport(NOW, { reported_at: new Date(NOW - 25 * 3600_000).toISOString() }))]);
    expect(await screen.findByText(/TECNO CN7c ·/)).toHaveTextContent("perlu dicek");
    expect((await checks("TECNO CN7c"))[3]).toBe("Mengirim kabarLebih dari 24 jam tidak ada kabar dari HP ini.");
  });

  it("shows one block per phone, with the access problem only on the second", async () => {
    renderStatus([
      pairedPhone(healthReport(NOW), { id: "phone-a", name: "Ponsel A" }),
      pairedPhone(healthReport(NOW, { notification_access: false }), { id: "phone-b", name: "Ponsel B" }),
    ]);
    expect((await checks("Ponsel A"))[0]).toBe("Izin baca notifikasiBaik");
    expect((await checks("Ponsel B"))[0]).toBe("Izin baca notifikasiIzin notifikasi mati.");
  });

  it("says when a phone has never reported", async () => {
    renderStatus([pairedPhone(null)]);
    expect(await screen.findByText("Belum pernah mengirim status. Buka aplikasinya di HP.")).toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
  });

  it("reads the box in English", async () => {
    renderStatus([pairedPhone(healthReport(NOW, { notification_access: false }))], "en");
    const box = await screen.findByRole("region", { name: "Phone status" });
    expect(await within(box).findByText(/TECNO CN7c ·/)).toHaveTextContent("TECNO CN7c · needs a check");
    const list = within(box).getByRole("list", { name: "Checks on TECNO CN7c" });
    expect(within(list).getAllByRole("listitem")[0]).toHaveTextContent("Notification accessNotification access is off.");
    expect(within(list).getAllByRole("listitem")[1]).toHaveTextContent("Runs in the backgroundGood");
  });
});
