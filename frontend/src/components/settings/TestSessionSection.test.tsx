import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  ApiError, deleteTestSession, endTestSession, getTestSessionEvents, getTestSessions, labelTestSessionEvent,
  startTestSession, type TestSession, type TestSessionEvent,
} from "@/lib/api";
import { TestSessionSection, durationLabel } from "./TestSessionSection";

vi.mock("@/lib/api", () => ({
  ApiError: class ApiError extends Error {
    constructor(public status: number, public detail: unknown) {
      super("api");
    }
  },
  getTestSessions: vi.fn(), startTestSession: vi.fn(), endTestSession: vi.fn(), getTestSessionEvents: vi.fn(),
  labelTestSessionEvent: vi.fn(), deleteTestSession: vi.fn(),
  testSessionCsvUrl: (id: string) => `/api/ingest/test-sessions/${id}/export.csv`,
}));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

// Synthetic sessions: 1 Nov 2026, 09.00–11.00 WIB.
const ended: TestSession = {
  id: "sesi-lalu", status: "ended", started_at: "2026-11-01T02:00:00Z", planned_end_at: "2026-11-01T04:00:00Z",
  ended_at: "2026-11-01T03:15:00Z", event_count: 2, labelled_count: 1,
};
const running: TestSession = {
  id: "sesi-jalan", status: "open", started_at: "2026-11-04T09:30:00Z", planned_end_at: "2026-11-04T11:30:00Z",
  ended_at: null, event_count: 0, labelled_count: 0,
};
const jagoCredit: TestSessionEvent = {
  event_id: "event-1", test_case: null, label_notes: null, post_time: "2026-11-01T02:05:00Z",
  received_at: "2026-11-01T02:05:02Z", gap_seconds: 2, package_name: "com.jago.digitalBanking", app_label: "Bank Jago",
  direction: "income", amount: 5000, parse_status: "candidate", preview: "You've received Rp5.000 from GoPay.",
};

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const view = render(<QueryClientProvider client={client}><TestSessionSection /></QueryClientProvider>);
  return { ...view, user: userEvent.setup() };
}

describe("Sesi uji", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getTestSessions).mockResolvedValue({ ok: true, sessions: [ended] });
    vi.mocked(startTestSession).mockResolvedValue({ ok: true, session: running });
    vi.mocked(endTestSession).mockResolvedValue({ ok: true, session: { ...running, status: "ended" } });
    vi.mocked(getTestSessionEvents).mockResolvedValue({ ok: true, events: [jagoCredit] });
    vi.mocked(labelTestSessionEvent).mockResolvedValue({ ok: true, event: { ...jagoCredit, test_case: "5" } });
    vi.mocked(deleteTestSession).mockResolvedValue({ ok: true });
  });

  it("names durations in hours and minutes", () => {
    expect([15, 60, 120, 90].map(durationLabel)).toEqual(["15 menit", "1 jam", "2 jam", "1 jam 30 menit"]);
  });

  it("starts a session with the default two hours", async () => {
    const { user, container } = show();
    const duration = await screen.findByRole("combobox", { name: "Durasi" });
    expect(duration).toHaveValue("120");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.selectOptions(duration, "30");
    await user.click(screen.getByRole("button", { name: "Mulai sesi" }));
    expect(startTestSession).toHaveBeenCalledWith(30);
  });

  it("says another session is still running when starting is refused", async () => {
    vi.mocked(startTestSession).mockRejectedValue(new ApiError(409, { code: "test_session_open" }));
    const { user } = show();
    await user.click(await screen.findByRole("button", { name: "Mulai sesi" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Sesi lain masih berjalan. Akhiri sesi itu dulu.");
  });

  it("shows the running session and ends it", async () => {
    vi.mocked(getTestSessions).mockResolvedValue({ ok: true, sessions: [running, ended] });
    const { user } = show();
    expect(await screen.findByText(/Berjalan sampai/)).toHaveTextContent("Berjalan sampai 18.30 · 0 notifikasi");
    expect(screen.queryByRole("button", { name: "Mulai sesi" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Akhiri sesi" }));
    expect(endTestSession).toHaveBeenCalledWith("sesi-jalan");
  });

  it("opens a past session's report and saves a case number and note", async () => {
    const { user, container } = show();
    const list = await screen.findByRole("list", { name: "Sesi uji sebelumnya" });
    expect(within(list).getByText("1 Nov 09.00 – 10.15")).toBeInTheDocument();
    expect(within(list).getByText("2 notifikasi · 1 bernomor kasus")).toBeInTheDocument();
    expect(within(list).getByRole("link", { name: /Unduh CSV/ })).toHaveAttribute("href", "/api/ingest/test-sessions/sesi-lalu/export.csv");
    await user.click(within(list).getByRole("button", { name: /Lihat laporan/ }));
    const report = await screen.findByRole("list", { name: "Notifikasi sesi" });
    expect(report).toHaveTextContent("Bank Jago · Uang masuk Rp 5.000");
    expect(report).toHaveTextContent("diterima +2 dtk");
    const save = within(report).getByRole("button", { name: "Simpan" });
    expect(save).toBeDisabled();
    await user.type(within(report).getByRole("textbox", { name: "Nomor kasus" }), "5");
    await user.type(within(report).getByRole("textbox", { name: "Catatan" }), "BCA ke Jago");
    await user.click(save);
    expect(labelTestSessionEvent).toHaveBeenCalledWith("sesi-lalu", "event-1", { test_case: "5", label_notes: "BCA ke Jago" });
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("deletes a session's data only after confirming", async () => {
    const { user } = show();
    const list = await screen.findByRole("list", { name: "Sesi uji sebelumnya" });
    await user.click(within(list).getByRole("button", { name: /Hapus data sesi/ }));
    expect(deleteTestSession).not.toHaveBeenCalled();
    await user.click(within(list).getByRole("button", { name: "Ya, hapus data" }));
    await waitFor(() => expect(deleteTestSession).toHaveBeenCalledWith("sesi-lalu"));
  });

  it("says when the sessions cannot be loaded", async () => {
    vi.mocked(getTestSessions).mockRejectedValue(new Error("offline"));
    show();
    expect(await screen.findByRole("alert")).toHaveTextContent("Sesi uji gagal dimuat.");
  });
});
