import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ShortcutProvider } from "@/components/workspace/shortcuts";
import { api } from "@/lib/api";
import type { SuggestionEstimate, SuggestionRun } from "@/lib/historySuggestions";
import { renderWithIntl } from "@/lib/i18n/testing";
import { HistorySuggestionsDialog } from "./HistorySuggestionsDialog";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));

const completedRun: SuggestionRun = {
  id: "run-1", status: "completed", from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 40, selected_count: 40,
  processed_count: 40, memory_count: 2, rule_count: 1, ai_count: 0, unchanged_count: 3, rejected_before_count: 0,
  ai_calls: 0, error_code: "ai_disabled", model: null, prompt_version: null,
  created_at: "2026-10-28T05:00:00Z", finished_at: "2026-10-28T05:00:05Z",
};

const estimate = (overrides: Partial<SuggestionEstimate> = {}): SuggestionEstimate => ({
  from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 420, selected_count: 300, truncated: true, max_rows: 300,
  memory_count: 30, rule_count: 5, ai_rows: 265, ai_calls: 14, batch_size: 20, ai_available: true, ai_unavailable_reason: null,
  ...overrides,
});

let currentRun: SuggestionRun | null;

function Opener() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        Buka saran
      </button>
      <HistorySuggestionsDialog open={open} onClose={() => setOpen(false)} />
    </>
  );
}

async function openDialog() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const user = userEvent.setup();
  const view = renderWithIntl(
    <QueryClientProvider client={queryClient}>
      <ShortcutProvider>
        <Opener />
      </ShortcutProvider>
    </QueryClientProvider>,
  );
  await user.click(screen.getByRole("button", { name: "Buka saran" }));
  const dialog = await screen.findByRole("dialog", { name: "Cari saran dari riwayat" });
  return { ...view, user, dialog };
}

describe("Cari saran dari riwayat (apply-markas-tinjau D12)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentRun = completedRun;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/review/suggestions/runs/latest") return { ok: true, run: currentRun };
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("Model unavailable: the last run's window, counts and why model suggestions were skipped, without axe violations", async () => {
    const { container, dialog } = await openDialog();
    expect(dialog.style.getPropertyValue("--dw")).toBe("520px");
    expect(dialog).toHaveTextContent("Nama, kategori dan pilar untuk transaksi lama yang dicatat otomatis. Tidak ada yang berubah sebelum diterima.");
    expect(await within(dialog).findByText(
      "Terakhir: 1 Agu 2026 – 28 Okt 2026 · 40 transaksi diperiksa · 3 saran baru (2 dari ingatan, 1 dari aturan, 0 dari AI)",
    )).toBeInTheDocument();
    expect(within(dialog).getByText("AI tidak aktif, jadi hanya saran dari ingatan dan aturan yang dibuat.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("Start disabled before an estimate, then Estimate then confirm by keyboard", async () => {
    vi.mocked(api.post).mockImplementation(async (path, body) => {
      if (path === "/review/suggestions/runs" && !(body as { confirm?: boolean }).confirm) return { ok: true, estimate: estimate() };
      return { ok: true, run: { ...completedRun, status: "running" } };
    });
    const { user, dialog, container } = await openDialog();
    const start = within(dialog).getByRole("button", { name: "Mulai buat saran" });
    expect(start).toBeDisabled();
    const from = within(dialog).getByLabelText("Dari tanggal");
    await user.clear(from);
    await user.type(from, "2026-08-01");
    within(dialog).getByRole("button", { name: "Hitung perkiraan" }).focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/suggestions/runs", expect.objectContaining({ from_date: "2026-08-01" })));
    expect(vi.mocked(api.post).mock.calls[0][1]).not.toHaveProperty("confirm");
    const lines = await within(dialog).findByRole("group", { name: "Perkiraan: 300 transaksi akan diperiksa" });
    expect(lines).toHaveTextContent("30 cocok dengan riwayat jawaban, 5 dengan aturan");
    expect(lines).toHaveTextContent("265 lewat AI dalam 14 panggilan (maks. 20 per panggilan)");
    expect(lines).toHaveTextContent("Ada 420 transaksi; hanya 300 terbaru yang diperiksa sekali jalan.");
    expect((await axe(container)).violations).toHaveLength(0);
    await user.tab();
    expect(start).toHaveFocus();
    expect(start).toBeEnabled();
    currentRun = { ...completedRun, status: "running", processed_count: 35, selected_count: 300, error_code: null };
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/suggestions/runs", {
      from_date: "2026-08-01", to_date: "2026-10-28", confirm: true,
    }));
    expect(await within(dialog).findByText('Pembuatan saran dimulai. Saran muncul di "Saran" setelah selesai.')).toBeInTheDocument();
    const progress = await within(dialog).findByRole("progressbar", { name: "Sedang membuat saran: 35 dari 300 transaksi diperiksa" });
    expect(progress).toHaveAttribute("value", "35");
  });

  it("disables Mulai buat saran again after a date changes", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, estimate: estimate() });
    const { user, dialog } = await openDialog();
    await user.click(within(dialog).getByRole("button", { name: "Hitung perkiraan" }));
    const start = within(dialog).getByRole("button", { name: "Mulai buat saran" });
    await waitFor(() => expect(start).toBeEnabled());
    const from = within(dialog).getByLabelText("Dari tanggal");
    await user.clear(from);
    await user.type(from, "2026-08-02");
    expect(start).toBeDisabled();
  });

  it("Cancel the estimate, and Invalid period without a request", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, estimate: estimate({ eligible_count: 0, selected_count: 0, truncated: false, ai_available: false, ai_unavailable_reason: "ai_disabled" }) });
    const { user, dialog } = await openDialog();
    const from = within(dialog).getByLabelText("Dari tanggal");
    const to = within(dialog).getByLabelText("Sampai tanggal");
    await user.clear(from);
    await user.type(from, "2026-10-20");
    await user.clear(to);
    await user.type(to, "2026-10-01");
    await user.click(within(dialog).getByRole("button", { name: "Hitung perkiraan" }));
    expect(api.post).not.toHaveBeenCalled();
    expect(to).toHaveAttribute("aria-invalid", "true");
    expect(within(dialog).getByText("Tanggal akhir tidak boleh sebelum tanggal awal.")).toBeInTheDocument();

    await user.clear(from);
    await user.type(from, "2026-10-01");
    await user.clear(to);
    await user.type(to, "2026-10-28");
    await user.click(within(dialog).getByRole("button", { name: "Hitung perkiraan" }));
    expect(await within(dialog).findByText("Tidak ada transaksi otomatis yang bisa diperbaiki di periode ini.")).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "Mulai buat saran" })).toBeDisabled();
    await user.click(within(dialog).getByRole("button", { name: "Batal" }));
    expect(within(dialog).getByRole("button", { name: "Hitung perkiraan" })).toHaveFocus();
    expect(within(dialog).queryByText("Tidak ada transaksi otomatis yang bisa diperbaiki di periode ini.")).not.toBeInTheDocument();
    expect(api.post).toHaveBeenCalledTimes(1);
  });

  it("Closed while running: no new run, the same run's progress on reopening, and focus back on the opener", async () => {
    currentRun = { ...completedRun, status: "running", processed_count: 12, selected_count: 40, error_code: null };
    const { user, dialog } = await openDialog();
    expect(await within(dialog).findByRole("progressbar", { name: "Sedang membuat saran: 12 dari 40 transaksi diperiksa" })).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Batal" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Cari saran dari riwayat" })).not.toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Buka saran" })).toHaveFocus();
    await user.click(screen.getByRole("button", { name: "Buka saran" }));
    const reopened = await screen.findByRole("dialog", { name: "Cari saran dari riwayat" });
    expect(await within(reopened).findByRole("progressbar", { name: "Sedang membuat saran: 12 dari 40 transaksi diperiksa" })).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();
  });
});
