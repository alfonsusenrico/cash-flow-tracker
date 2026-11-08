import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getDevices, type DeviceHealth, type PairedDevice } from "@/lib/api";
import { createTranslator } from "next-intl";
import { createFormat } from "@/lib/format";
import { CATALOGUES } from "@/lib/i18n/testing";
import { CompanionStatus, companionState, type CompanionText } from "./CompanionStatus";

vi.mock("@/lib/api", () => ({ getDevices: vi.fn() }));

const NOW = new Date("2026-11-02T03:00:00Z"); // 10:00 in Asia/Jakarta

function text(language: "en" | "id" = "id"): CompanionText {
  const t = createTranslator({ locale: language, messages: CATALOGUES[language], namespace: "shell.companion" });
  return { t: t as unknown as CompanionText["t"], format: createFormat({ language, region: "ID" }) };
}

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
    expect(companionState([device()], NOW, text())).toEqual({ tone: "ok", title: "HP terhubung", detail: "Terakhir menangkap 09.15" });
  });

  it("says yesterday for a capture on the previous Jakarta day", () => {
    const state = companionState([device({ health: health({ last_capture_at: "2026-11-01T13:30:00Z" }) })], NOW, text());
    expect(state.detail).toBe("Terakhir menangkap kemarin 20.30");
  });

  it("uses the newest health report when several phones are paired", () => {
    const old = device({ id: "old", health: health({ reported_at: "2026-11-02T01:00:00Z", ready: false, notification_access: false }) });
    const fresh = device({ id: "fresh" });
    expect(companionState([old, fresh], NOW, text()).tone).toBe("ok");
  });

  it("needs attention with the first problem as the reason", () => {
    const state = companionState([device({ health: health({ ready: false, notification_access: false }) })], NOW, text());
    expect(state).toEqual({ tone: "warn", title: "HP perlu dicek", detail: "Izin notifikasi mati." });
  });

  it("needs attention when a ready phone has been quiet for more than a day", () => {
    const state = companionState([device({ health: health({ reported_at: "2026-11-01T02:00:00Z" }) })], NOW, text());
    expect(state.tone).toBe("warn");
    expect(state.detail).toBe("Lebih dari 24 jam tidak ada kabar dari HP ini.");
  });

  it("needs attention when a paired phone never reported its health", () => {
    expect(companionState([device({ health: null })], NOW, text())).toEqual({
      tone: "warn",
      title: "HP perlu dicek",
      detail: "HP terhubung tapi belum kirim kabar",
    });
  });

  it("has no phone when only a manual key exists", () => {
    expect(companionState([device({ created_via: "manual" })], NOW, text()).tone).toBe("none");
    expect(companionState([], NOW, text())).toEqual({ tone: "none", title: "Belum ada HP terhubung", detail: "Sambungkan HP dengan kode di bawah ini" });
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

  it("shows a ready phone with its last contact as text in Pengaturan", async () => {
    const minutesAgo = (minutes: number) => new Date(Date.now() - minutes * 60_000).toISOString();
    vi.mocked(getDevices).mockResolvedValue({
      ok: true,
      devices: [device({ health: health({ reported_at: minutesAgo(2), last_capture_at: minutesAgo(5) }) })],
    });
    const { container } = show();
    expect(await screen.findByText("HP terhubung")).toBeInTheDocument();
    expect(container).toHaveTextContent(/Terakhir menangkap \d{2}\.\d{2}/);
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("shows the reason when the phone needs attention", async () => {
    vi.mocked(getDevices).mockResolvedValue({
      ok: true,
      devices: [device({ health: health({ reported_at: new Date().toISOString(), ready: false, listener_connected: false }) })],
    });
    const { container } = show();
    expect(await screen.findByText("HP perlu dicek")).toBeInTheDocument();
    expect(container).toHaveTextContent("Aplikasi tidak berjalan di latar belakang.");
  });

  it("points to the pairing below when no phone is paired", async () => {
    vi.mocked(getDevices).mockResolvedValue({ ok: true, devices: [] });
    const { container } = show();
    expect(await screen.findByText("Belum ada HP terhubung")).toBeInTheDocument();
    expect(container).toHaveTextContent("Sambungkan HP dengan kode di bawah ini");
  });

  it("renders nothing when the request fails", async () => {
    vi.mocked(getDevices).mockRejectedValue(new Error("network down"));
    const { container } = show();
    await vi.waitFor(() => expect(getDevices).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(container).toBeEmptyDOMElement();
  });
});

describe("companionState in English (add-language-system)", () => {
  it("words the phone's status in English with the same clock", () => {
    expect(companionState([device()], NOW, text("en"))).toEqual({ tone: "ok", title: "Phone connected", detail: "Last capture 09.15" });
    const yesterday = companionState([device({ health: health({ last_capture_at: "2026-11-01T13:30:00Z" }) })], NOW, text("en"));
    expect(yesterday.detail).toBe("Last capture yesterday 20.30");
    expect(companionState([], NOW, text("en"))).toEqual({ tone: "none", title: "No phone connected yet", detail: "Connect a phone with the code below" });
    expect(companionState([device({ health: null })], NOW, text("en")).detail).toBe("Phone connected but not reporting yet");
  });

  it("keeps a device problem from Pengaturan in Indonesian and marks it", () => {
    const state = companionState([device({ health: health({ ready: false, notification_access: false }) })], NOW, text("en"));
    expect(state).toEqual({ tone: "warn", title: "Phone needs a check", detail: "Izin notifikasi mati.", detailLang: "id" });
  });
});
