import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { SuggestionRun } from "@/lib/historySuggestions";
import { HistorySuggestionsSection } from "./HistorySuggestionsSection";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const completedRun: SuggestionRun = {
  id: "run-1", status: "completed", from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 40, selected_count: 40,
  processed_count: 40, memory_count: 2, rule_count: 1, ai_count: 0, unchanged_count: 3, rejected_before_count: 0,
  ai_calls: 0, error_code: "ai_disabled", model: null, prompt_version: null,
  created_at: "2026-10-28T05:00:00Z", finished_at: "2026-10-28T05:00:05Z",
};

let currentRun: SuggestionRun | null;

function renderSection() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}><HistorySuggestionsSection /></QueryClientProvider>);
}

describe("Cari saran dari riwayat (run panel)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentRun = completedRun;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/review/suggestions/runs/latest") return { ok: true, run: currentRun };
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("shows the last run and no axe violations", async () => {
    const { container } = renderSection();
    expect(await screen.findByText(/40 transaksi diperiksa · 3 saran baru \(2 dari ingatan, 1 dari aturan, 0 dari AI\)/)).toBeInTheDocument();
    expect(screen.getByText("AI tidak aktif, jadi hanya saran dari ingatan dan aturan yang dibuat.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Estimate then confirm by keyboard", async () => {
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      const payload = body as { confirm?: boolean };
      if (path === "/review/suggestions/runs" && !payload.confirm) {
        return { ok: true, estimate: {
          from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 420, selected_count: 300, truncated: true, max_rows: 300,
          memory_count: 30, rule_count: 5, ai_rows: 265, ai_calls: 14, batch_size: 20, ai_available: true, ai_unavailable_reason: null,
        } };
      }
      return { ok: true, run: { ...completedRun, status: "running" } };
    });
    const user = userEvent.setup();
    const { container } = renderSection();
    const from = await screen.findByLabelText("Dari tanggal");
    await user.clear(from);
    await user.type(from, "2026-08-01");
    screen.getByRole("button", { name: "Hitung perkiraan" }).focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/suggestions/runs", expect.objectContaining({ from_date: "2026-08-01" })));
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("confirm");
    const estimate = await screen.findByRole("group", { name: /Perkiraan: 300 transaksi akan diperiksa/ });
    expect(within(estimate).getByText("265 lewat AI dalam 14 panggilan (maks. 20 per panggilan)")).toBeInTheDocument();
    expect(within(estimate).getByText(/Ada 420 transaksi; hanya 300 terbaru/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);

    await user.tab();
    expect(within(estimate).getByRole("button", { name: "Mulai buat saran" })).toHaveFocus();
    currentRun = { ...completedRun, status: "running", processed_count: 35, selected_count: 300, error_code: null };
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/suggestions/runs", {
      from_date: "2026-08-01", to_date: "2026-10-28", confirm: true,
    }));
    expect(await screen.findByText(/Pembuatan saran dimulai/)).toBeInTheDocument();
    const progress = await screen.findByRole("progressbar", { name: /35 dari 300 transaksi diperiksa/ });
    expect(progress).toHaveAttribute("value", "35");
  });

  it("cancels an estimate and returns focus, and rejects an invalid window without a request", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, estimate: {
      from_date: "2026-10-01", to_date: "2026-10-28", eligible_count: 0, selected_count: 0, truncated: false, max_rows: 300,
      memory_count: 0, rule_count: 0, ai_rows: 0, ai_calls: 0, batch_size: 20, ai_available: false, ai_unavailable_reason: "ai_disabled",
    } });
    const user = userEvent.setup();
    renderSection();
    const to = await screen.findByLabelText("Sampai tanggal");
    const from = screen.getByLabelText("Dari tanggal");
    await user.clear(from);
    await user.type(from, "2026-10-20");
    await user.clear(to);
    await user.type(to, "2026-10-01");
    await user.click(screen.getByRole("button", { name: "Hitung perkiraan" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Tanggal akhir tidak boleh sebelum tanggal awal.");
    expect(api.post).not.toHaveBeenCalled();

    await user.clear(from);
    await user.type(from, "2026-10-01");
    await user.clear(to);
    await user.type(to, "2026-10-28");
    await user.click(screen.getByRole("button", { name: "Hitung perkiraan" }));
    expect(await screen.findByText("Tidak ada transaksi otomatis yang bisa diperbaiki di periode ini.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Tutup" }));
    expect(screen.getByRole("button", { name: "Hitung perkiraan" })).toHaveFocus();
  });

  it("starts without a run and shows no figures", async () => {
    currentRun = null;
    const { container } = renderSection();
    expect(await screen.findByRole("button", { name: "Hitung perkiraan" })).toBeInTheDocument();
    expect(screen.queryByText(/transaksi diperiksa/)).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
