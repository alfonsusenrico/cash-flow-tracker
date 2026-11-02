import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getDevices, type DeviceHealth, type PairedDevice } from "@/lib/api";
import { CompanionStatus, companionState } from "./SideNav";

vi.mock("next/navigation", () => ({ usePathname: () => "/" }));
vi.mock("@/lib/api", () => ({ getDevices: vi.fn() }));

const NOW = new Date("2026-11-02T03:00:00Z"); // 10:00 in Asia/Jakarta

function health(overrides: Partial<DeviceHealth> = {}): DeviceHealth {
  return {
    reported_at: "2026-11-02T02:55:00Z",
    ready: true,
    app_version: "1.4.0",
    listener_connected: true,
    notification_access: true,
    post_notifications: true,
    battery_unrestricted: true,
    self_test: { status: "passed", failed_step: null, at: "2026-11-01T01:00:00Z" },
    last_capture_at: "2026-11-02T02:15:00Z",
    queue_pending: 0,
    ...overrides,
  };
}

function device(overrides: Partial<PairedDevice> = {}): PairedDevice {
  return {
    id: "device-1",
    name: "TECNO",
    created_via: "pairing",
    key_prefix: "cfk_test",
    created_at: "2026-10-01T00:00:00Z",
    last_used_at: "2026-11-02T02:15:00Z",
    health: health(),
    ...overrides,
  };
}

describe("companionState", () => {
  it("is ready with the time of the last capture", () => {
    expect(companionState([device()], NOW)).toEqual({ tone: "ok", title: "HP-mu siap", detail: "terakhir nangkep 09.15" });
  });

  it("says yesterday for a capture on the previous Jakarta day", () => {
    const state = companionState([device({ health: health({ last_capture_at: "2026-11-01T13:30:00Z" }) })], NOW);
    expect(state.detail).toBe("terakhir nangkep kemarin 20.30");
  });

  it("uses the newest health report when several phones are paired", () => {
    const old = device({ id: "old", health: health({ reported_at: "2026-11-02T01:00:00Z", ready: false, notification_access: false }) });
    const fresh = device({ id: "fresh" });
    expect(companionState([old, fresh], NOW).tone).toBe("ok");
  });

  it("needs attention with the first problem as the reason", () => {
    const state = companionState([device({ health: health({ ready: false, notification_access: false }) })], NOW);
    expect(state).toEqual({ tone: "warn", title: "HP perlu dicek", detail: "Izin notifikasi mati." });
  });

  it("needs attention when a ready phone has been quiet for more than a day", () => {
    const state = companionState([device({ health: health({ reported_at: "2026-11-01T02:00:00Z" }) })], NOW);
    expect(state.tone).toBe("warn");
    expect(state.detail).toBe("Udah lebih dari 24 jam nggak ada kabar dari HP ini.");
  });

  it("needs attention when a paired phone never reported its health", () => {
    expect(companionState([device({ health: null })], NOW)).toEqual({
      tone: "warn",
      title: "HP perlu dicek",
      detail: "HP terhubung tapi belum kirim kabar",
    });
  });

  it("has no phone when only a manual key exists", () => {
    expect(companionState([device({ created_via: "manual" })], NOW).tone).toBe("none");
    expect(companionState([], NOW)).toEqual({ tone: "none", title: "Belum ada HP terhubung", detail: "Sambungkan HP-mu di Pengaturan" });
  });
});

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <CompanionStatus />
    </QueryClientProvider>,
  );
}

describe("CompanionStatus", () => {
  beforeEach(() => vi.clearAllMocks());

  it("shows a ready phone with its last contact as text, name and tooltip", async () => {
    const minutesAgo = (minutes: number) => new Date(Date.now() - minutes * 60_000).toISOString();
    vi.mocked(getDevices).mockResolvedValue({
      ok: true,
      devices: [device({ health: health({ reported_at: minutesAgo(2), last_capture_at: minutesAgo(5) }) })],
    });
    const { container } = show();
    const link = await screen.findByRole("link", { name: /^HP-mu siap, terakhir nangkep \d{2}\.\d{2}$/ });
    expect(link).toHaveAttribute("href", "/settings/perangkat");
    expect(link.querySelector(".ws-tip")).toHaveTextContent(link.getAttribute("aria-label")!);
    expect(link).toHaveTextContent("HP-mu siap");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows the reason when the phone needs attention", async () => {
    vi.mocked(getDevices).mockResolvedValue({
      ok: true,
      devices: [device({ health: health({ reported_at: new Date().toISOString(), ready: false, listener_connected: false }) })],
    });
    show();
    expect(
      await screen.findByRole("link", { name: "HP perlu dicek, Aplikasinya nggak jalan di latar belakang." }),
    ).toHaveTextContent("Aplikasinya nggak jalan di latar belakang.");
  });

  it("links to the device settings when no phone is paired", async () => {
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
    show();
    const link = await screen.findByRole("link", { name: "Belum ada HP terhubung, Sambungkan HP-mu di Pengaturan" });
    expect(link).toHaveAttribute("href", "/settings/perangkat");
  });

  it("renders nothing when the request fails", async () => {
    vi.mocked(getDevices).mockRejectedValue(new Error("network down"));
    const { container } = show();
    await vi.waitFor(() => expect(getDevices).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(container).toBeEmptyDOMElement();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });
});
