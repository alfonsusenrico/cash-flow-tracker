import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import QRCode from "qrcode";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPairingCode, getDevices, type DeviceHealth, type PairedDevice } from "@/lib/api";
import { healthReport, pairedPhone } from "@/test/deviceHealthFixtures";
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
    item.querySelector("span.text-sm")?.textContent,
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

  it("waits for the phone with every step open", async () => {
    const dialog = await openDialog();

    expect(within(dialog).getByText("Menunggu HP…")).toBeInTheDocument();
    expect(within(dialog).getByText(/Tidak perlu dimuat ulang/)).toBeInTheDocument();
    expect(stepStates(dialog)).toEqual([
      ["Terhubung", "todo"], ["Izin notifikasi", "todo"], ["Jalan di latar belakang", "todo"], ["Tes tangkap notifikasi", "todo"],
    ]);
    expect(within(dialog).getAllByText("Belum")).toHaveLength(4);
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
    expect(within(dialog).getByRole("button", { name: "Nanti saja" })).toBeInTheDocument();
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
    expect(statusCard(dialog).getByText("HP-mu siap! Mulai sekarang transaksi tercatat sendiri.")).toBeInTheDocument();
    expect(within(dialog).getByText(/TECNO CN7c · aplikasi pendamping v2\.4\.0/)).toBeInTheDocument();
    expect(live).toHaveTextContent("HP-mu siap! Mulai sekarang transaksi tercatat sendiri.");
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
    expect(statusCard(dialog).getByText("HP-mu siap! Mulai sekarang transaksi tercatat sendiri.")).toBeInTheDocument();
    expect(within(background).getByText(/izinkan aplikasinya jalan tanpa batasan baterai/)).toBeInTheDocument();
  });

  it.each([
    ["post", "HP tidak dapat menampilkan notifikasi tes."],
    ["capture", "Notifikasi tes tidak tertangkap oleh aplikasi."],
    ["server", "HP belum bisa ngirim ke server."],
  ] as const)("shows a self-test that failed at %s", async (step, text) => {
    const dialog = await openDialog();

    phonePairs(report({ self_test: { status: "failed", failed_step: step } }));
    await advance(3_000);

    expect(within(dialog).getByText("Ada yang belum siap: Tes tangkap notifikasi")).toBeInTheDocument();
    const selfTest = within(dialog).getByText("Tes tangkap notifikasi").closest("li") as HTMLElement;
    expect(selfTest).toHaveAttribute("data-state", "fail");
    expect(within(selfTest).getByText("Belum berjalan")).toBeInTheDocument();
    expect(within(selfTest).getByText(text)).toBeInTheDocument();
    expect(within(dialog).getByText("Tenang, aplikasi di HP-mu bakal nunjukin cara benerinnya.")).toBeInTheDocument();
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
    expect(within(dialog).getByRole("status")).toHaveTextContent(`${label} belum jalan.`);
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
    expect(statusCard(screen.getByRole("dialog")).getByText("HP-mu siap! Mulai sekarang transaksi tercatat sendiri.")).toBeInTheDocument();
    const atReady = vi.mocked(getDevices).mock.calls.length;

    await advance(60_000);
    expect(vi.mocked(getDevices).mock.calls.length).toBe(atReady);
  });

  it("stops polling when the dialog closes", async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    const dialog = await openDialog();
    await advance(3_000);

    // The footer button; the header's ✕ shares the name.
    await user.click(within(dialog).getAllByRole("button", { name: "Tutup" }).at(-1) as HTMLElement);
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
    expect(within(dialog).getByText(/Aku berhenti ngecek dulu/)).toBeInTheDocument();
    await expectNoAxeViolations(dialog);

    await advance(60_000);
    expect(vi.mocked(getDevices).mock.calls.length).toBe(atWindowEnd);
  });
});
