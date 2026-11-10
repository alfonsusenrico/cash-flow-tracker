import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import QRCode from "qrcode";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getDevices, type DeviceHealth, type PairedDevice } from "@/lib/api";
import { healthReport, pairedPhone } from "@/test/deviceHealthFixtures";
import { renderWithIntl } from "@/lib/i18n/testing";
import { DevicePairingDialog } from "./DevicePairingDialog";

vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn() } }));
vi.mock("@/lib/api", () => ({
  createPairingCode: vi.fn(),
  getDevices: vi.fn(),
  revokeDevice: vi.fn(),
}));

const NOW = Date.parse("2026-11-02T03:00:00Z");
let devices: PairedDevice[] = [];

function Harness() {
  const [open, setOpen] = useState(true);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>Buka lagi</button>
      {open && <DevicePairingDialog onClose={() => setOpen(false)} />}
    </>
  );
}

function renderDialog() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <Harness />
    </QueryClientProvider>,
  );
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

/** The phone pairs after the code was issued, optionally with a health report. */
function phonePairs(health: DeviceHealth | null) {
  devices = [pairedPhone(health, { created_at: new Date(Date.now() + 1000).toISOString() })];
}

function report(overrides: Parameters<typeof healthReport>[1] = {}) {
  return healthReport(Date.now(), overrides);
}

function stepStates(dialog: HTMLElement) {
  const list = within(dialog).getByRole("list", { name: "Langkah pengaturan HP" });
  return within(list).getAllByRole("listitem").map((item) => [
    item.querySelector("span.t-body")?.textContent,
    item.getAttribute("data-state"),
  ]);
}

async function openDialog() {
  renderDialog();
  const dialog = await screen.findByRole("dialog", { name: "Hubungkan ponsel" });
  await within(dialog).findByRole("img", { name: /Kode QR pemasangan ponsel/ });
  return dialog;
}

function statusCard(dialog: HTMLElement) {
  return within(within(dialog).getByRole("region", { name: "Status HP" }));
}

async function expectNoAxeViolations(dialog: HTMLElement) {
  expect((await axe(dialog)).violations).toHaveLength(0);
}

describe("live pairing checklist", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true, now: NOW });
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    devices = [];
    vi.mocked(getDevices).mockImplementation(async () => ({ ok: true, devices }));
    vi.mocked(QRCode.toCanvas).mockResolvedValue(undefined as never);
    vi.mocked(createPairingCode).mockImplementation(async () => ({
      ok: true,
      code: "ABCDE12345",
      expires_at: new Date(Date.now() + 300_000).toISOString(),
      expires_in_seconds: 300,
      qr_payload: "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.example.test&code=ABCDE12345",
    }));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("shows the QR at 200 px or more, three numbered steps, the code and the countdown until a phone pairs", async () => {
    const dialog = await openDialog();

    // The owner kept the pairing QR at 200 px or more (2026-11-08) for a reliable scan on a real phone.
    const qr = within(dialog).getByRole("img", { name: "Kode QR pemasangan ponsel, kode ABCDE 12345" });
    expect(parseInt(qr.style.width, 10)).toBeGreaterThanOrEqual(200);
    expect(QRCode.toCanvas).toHaveBeenCalledWith(
      expect.any(HTMLCanvasElement),
      expect.stringContaining("code=ABCDE12345"),
      expect.objectContaining({ margin: 2, color: { dark: "#000000", light: "#ffffff" } }),
    );
    const steps = within(dialog).getByRole("list", { name: "Langkah" });
    expect(within(steps).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "Buka aplikasi pendamping di HP.", "Ketuk Mulai, lalu Scan QR.", "Pindai kode ini. Berlaku 05:00.",
    ]);
    expect(within(dialog).getByText("Atau ketik kode")).toBeInTheDocument();
    expect(within(dialog).getByText("ABCDE 12345")).toBeInTheDocument();
    expect(within(dialog).queryByRole("list", { name: "Langkah pengaturan HP" })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Nanti saja" })).toBeInTheDocument();
    const live = within(dialog).getByRole("status");
    expect(live).toHaveAttribute("aria-live", "polite");
    expect(live).toHaveTextContent("");
    await expectNoAxeViolations(dialog);
  });

  it("goes from connected through each step to ready, announcing each change", async () => {
    const dialog = await openDialog();
    const live = within(dialog).getByRole("status");
    const focused = document.activeElement;

    phonePairs(null);
    await advance(3_000);
    expect(within(dialog).getByText("Terhubung. Lanjutkan pengaturan di HP.")).toBeInTheDocument();
    expect(within(dialog).getByText("TECNO CN7c sedang menampilkan langkah-langkahnya.")).toBeInTheDocument();
    expect(within(dialog).queryByRole("img", { name: /Kode QR pemasangan ponsel/ })).not.toBeInTheDocument();
    expect(stepStates(dialog).map(([, state]) => state)).toEqual(["ok", "wait", "todo", "todo"]);
    expect(within(dialog).getByText("Sedang diatur di HP")).toBeInTheDocument();
    expect(live).toHaveTextContent("HP terhubung. Lanjutkan pengaturan di HP.");
    expect(within(dialog).getByRole("button", { name: "Selesai" })).toBeInTheDocument();
    await expectNoAxeViolations(dialog);

    phonePairs(report({ listener_connected: false, self_test: { status: "not_run", at: null } }));
    await advance(3_000);
    expect(stepStates(dialog).map(([, state]) => state)).toEqual(["ok", "ok", "wait", "todo"]);
    expect(live).toHaveTextContent("Izin notifikasi selesai.");
    await expectNoAxeViolations(dialog);

    phonePairs(report({ self_test: { status: "not_run", at: null } }));
    await advance(3_000);
    expect(stepStates(dialog).map(([, state]) => state)).toEqual(["ok", "ok", "ok", "wait"]);
    expect(live).toHaveTextContent("Jalan di latar belakang selesai.");

    phonePairs(report({ app_version: "2.4.0" }));
    await advance(3_000);
    expect(stepStates(dialog).map(([, state]) => state)).toEqual(["ok", "ok", "ok", "ok"]);
    expect(statusCard(dialog).getByText("HP siap. Transaksi dari notifikasi bank kini tercatat otomatis.")).toBeInTheDocument();
    expect(within(dialog).getByText(/TECNO CN7c · aplikasi pendamping v2\.4\.0/)).toBeInTheDocument();
    expect(live).toHaveTextContent("HP siap. Transaksi dari notifikasi bank kini tercatat otomatis.");
    expect(document.activeElement).toBe(focused);
    await expectNoAxeViolations(dialog);

    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    await user.click(within(dialog).getByRole("button", { name: "Selesai" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shows the battery hint on the background step without blocking readiness", async () => {
    const dialog = await openDialog();

    phonePairs(report({ battery_unrestricted: false, self_test: { status: "not_run", at: null } }));
    await advance(3_000);
    const background = within(dialog).getByText("Jalan di latar belakang").closest("li") as HTMLElement;
    expect(within(background).getByText(/TECNO\/Infinix \(HiOS\) atau Xiaomi \(MIUI\)/)).toBeInTheDocument();
    await expectNoAxeViolations(dialog);

    phonePairs(report({ battery_unrestricted: false }));
    await advance(3_000);
    expect(statusCard(dialog).getByText("HP siap. Transaksi dari notifikasi bank kini tercatat otomatis.")).toBeInTheDocument();
    expect(within(background).getByText(/izinkan aplikasinya berjalan tanpa batasan baterai/)).toBeInTheDocument();
  });

  it.each([
    ["post", "HP tidak dapat menampilkan notifikasi tes."],
    ["capture", "Notifikasi tes tidak tertangkap oleh aplikasi."],
    ["server", "HP belum dapat mengirim ke server."],
  ] as const)("shows a self-test that failed at %s", async (step, text) => {
    const dialog = await openDialog();

    phonePairs(report({ self_test: { status: "failed", failed_step: step } }));
    await advance(3_000);

    expect(within(dialog).getByText("Ada yang belum siap: Tes tangkap notifikasi")).toBeInTheDocument();
    const selfTest = within(dialog).getByText("Tes tangkap notifikasi").closest("li") as HTMLElement;
    expect(selfTest).toHaveAttribute("data-state", "fail");
    expect(within(selfTest).getByText("Belum berjalan")).toBeInTheDocument();
    expect(within(selfTest).getByText(text)).toBeInTheDocument();
    expect(within(dialog).getByText("Aplikasi di HP menunjukkan cara memperbaikinya.")).toBeInTheDocument();
    expect(within(dialog).getByRole("status")).toHaveTextContent(`Tes tangkap notifikasi gagal. ${text}`);
    await expectNoAxeViolations(dialog);
  });

  it.each([
    ["notification_access", "Izin notifikasi"],
    ["listener_connected", "Jalan di latar belakang"],
  ] as const)("shows %s turned off after the self-test as failed", async (field, label) => {
    const dialog = await openDialog();

    phonePairs(report({ [field]: false }));
    await advance(3_000);

    expect(within(dialog).getByText(`Ada yang belum siap: ${label}`)).toBeInTheDocument();
    expect(within(dialog).getByText(label).closest("li")).toHaveAttribute("data-state", "fail");
    expect(within(dialog).getByRole("status")).toHaveTextContent(`${label} belum berjalan.`);
    await expectNoAxeViolations(dialog);
  });
});

describe("pairing dialog polling", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true, now: NOW });
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    devices = [];
    vi.mocked(getDevices).mockImplementation(async () => ({ ok: true, devices }));
    vi.mocked(QRCode.toCanvas).mockResolvedValue(undefined as never);
    vi.mocked(createPairingCode).mockImplementation(async () => ({
      ok: true,
      code: "ABCDE12345",
      expires_at: new Date(Date.now() + 300_000).toISOString(),
      expires_in_seconds: 300,
      qr_payload: "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.example.test&code=ABCDE12345",
    }));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("polls every 3 seconds and stops once the phone is ready", async () => {
    await openDialog();
    const start = vi.mocked(getDevices).mock.calls.length;

    await advance(9_000);
    const whileWaiting = vi.mocked(getDevices).mock.calls.length - start;
    expect(whileWaiting).toBeGreaterThanOrEqual(3);
    expect(whileWaiting).toBeLessThanOrEqual(4);

    phonePairs(report());
    await advance(3_000);
    expect(statusCard(screen.getByRole("dialog")).getByText("HP siap. Transaksi dari notifikasi bank kini tercatat otomatis.")).toBeInTheDocument();
    const atReady = vi.mocked(getDevices).mock.calls.length;

    await advance(60_000);
    expect(vi.mocked(getDevices).mock.calls.length).toBe(atReady);
  });

  it("stops polling when the dialog closes", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    const dialog = await openDialog();
    await advance(3_000);

    await user.click(within(dialog).getByRole("button", { name: "Nanti saja" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    const atClose = vi.mocked(getDevices).mock.calls.length;

    await advance(60_000);
    expect(vi.mocked(getDevices).mock.calls.length).toBe(atClose);
  });

  it("gives up after 10 minutes when the phone never gets ready", async () => {
    const dialog = await openDialog();
    phonePairs(report({ listener_connected: false, self_test: { status: "not_run", at: null } }));

    await advance(10 * 60_000);
    const atWindowEnd = vi.mocked(getDevices).mock.calls.length;
    expect(within(dialog).getByText("Pengecekan dihentikan. Lanjutkan di aplikasi HP; statusnya tetap tampil di Status HP.")).toBeInTheDocument();
    await expectNoAxeViolations(dialog);

    await advance(60_000);
    expect(vi.mocked(getDevices).mock.calls.length).toBe(atWindowEnd);
  });
});

describe("pairing dialog in English", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    devices = [];
    vi.mocked(getDevices).mockImplementation(async () => ({ ok: true, devices }));
    vi.mocked(QRCode.toCanvas).mockResolvedValue(undefined as never);
    vi.mocked(createPairingCode).mockResolvedValue({
      ok: true,
      code: "ABCDE12345",
      expires_at: new Date(Date.now() + 300_000).toISOString(),
      expires_in_seconds: 300,
      qr_payload: "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.example.test&code=ABCDE12345",
    });
  });

  afterEach(() => vi.unstubAllGlobals());

  it("reads the steps, the code box and the actions in plain English", async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    renderWithIntl(
      <QueryClientProvider client={queryClient}>
        <DevicePairingDialog onClose={() => {}} />
      </QueryClientProvider>,
      { language: "en" },
    );
    const dialog = await screen.findByRole("dialog", { name: "Connect a phone" });
    expect(await within(dialog).findByText("Open the companion app on the phone.")).toBeInTheDocument();
    expect(within(dialog).getByText("Or type the code")).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Not now" })).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Create a new code" })).toBeInTheDocument();
    expect(dialog.querySelector("[lang='id']")).toBeNull();
  });
});
