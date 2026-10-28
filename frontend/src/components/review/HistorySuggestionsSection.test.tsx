import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { HistorySuggestion, SuggestionList, SuggestionRun } from "@/lib/historySuggestions";
import { HistorySuggestionsSection } from "./HistorySuggestionsSection";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const suggestion = (id: string, date: string): HistorySuggestion => ({
  id,
  transaction_id: `tx-${id}`,
  type: "expense",
  amount: 50_000,
  date,
  account_name: "BCA Harian",
  source: "memory",
  confidence: null,
  reason: "Sama dengan pilihan Anda untuk Kedai Awan",
  before: { merchant: "Kedai Awan", notes: "Pengeluaran makanan dan minuman", category_id: "category-food", category_name: "Makanan & Minuman", kakeibo: "need" },
  after: {
    merchant: "Kedai Awan", notes: "Ngopi di Kedai Awan", category_id: "category-snack", category_name: "Jajan & Nongkrong",
    kakeibo: "want", offset_category_id: null, offset_category_name: null,
  },
});

function list(): SuggestionList {
  return {
    ok: true,
    truncated: false,
    pending: 3,
    groups: [
      {
        key: "expense|merchant:kedai awan|category-snack", type: "expense", label: "Kedai Awan", merchant: "Kedai Awan",
        category: { id: "category-snack", name: "Jajan & Nongkrong" }, kakeibo: "want", offset_category_names: [],
        sources: ["memory"], before: [{ label: "Pengeluaran makanan dan minuman", category_name: "Makanan & Minuman", count: 2 }],
        count: 2, total: 100_000, first_date: "2026-10-01T05:00:00Z", last_date: "2026-10-03T05:00:00Z",
        suggestions: [suggestion("s-1", "2026-10-03T05:00:00Z"), suggestion("s-2", "2026-10-01T05:00:00Z")],
      },
      {
        key: "income|notes:ganti uang patungan|category-repay", type: "income", label: "Ganti uang patungan", merchant: null,
        category: { id: "category-repay", name: "Patungan & Ganti Uang" }, kakeibo: null, offset_category_names: ["Makanan & Minuman"],
        sources: ["rule"], before: [{ label: "Transfer masuk", category_name: "Pendapatan Lain", count: 1 }],
        count: 1, total: 57_000, first_date: "2026-10-02T05:00:00Z", last_date: "2026-10-02T05:00:00Z",
        suggestions: [{
          ...suggestion("s-3", "2026-10-02T05:00:00Z"), type: "income", amount: 57_000, source: "rule",
          reason: "Transfer masuk menyebut patungan atau ganti uang: teman membayar kembali, bukan pemasukan",
          before: { merchant: null, notes: "Transfer masuk", category_id: "category-other", category_name: "Pendapatan Lain", kakeibo: null },
          after: {
            merchant: null, notes: "Ganti uang patungan", category_id: "category-repay", category_name: "Patungan & Ganti Uang",
            kakeibo: null, offset_category_id: "category-food", offset_category_name: "Makanan & Minuman",
          },
        }],
      },
    ],
  };
}

const completedRun: SuggestionRun = {
  id: "run-1", status: "completed", from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 40, selected_count: 40,
  processed_count: 40, memory_count: 2, rule_count: 1, ai_count: 0, unchanged_count: 3, rejected_before_count: 0,
  ai_calls: 0, error_code: "ai_disabled", model: null, prompt_version: null,
  created_at: "2026-10-28T05:00:00Z", finished_at: "2026-10-28T05:00:05Z",
};

let currentList: SuggestionList;
let currentRun: SuggestionRun | null;

function renderSection() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}><HistorySuggestionsSection /></QueryClientProvider>);
}

describe("history suggestions section", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentList = list();
    currentRun = completedRun;
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/review/suggestions") return currentList;
      if (path === "/review/suggestions/runs/latest") return { ok: true, run: currentRun };
      throw new Error(`Unexpected GET ${path}`);
    });
  });

  it("shows grouped before and after values, the last run and no axe violations", async () => {
    const { container } = renderSection();
    expect(await screen.findByRole("heading", { name: /Saran perbaikan riwayat \(3\)/ })).toBeInTheDocument();
    const kedai = screen.getByRole("heading", { name: "Kedai Awan" }).closest("article") as HTMLElement;
    expect(within(kedai).getByText("Pengeluaran makanan dan minuman · Makanan & Minuman")).toBeInTheDocument();
    expect(within(kedai).getByText("Kedai Awan · Ngopi di Kedai Awan · Jajan & Nongkrong · Keinginan", { selector: "dd" })).toBeInTheDocument();
    expect(within(kedai).getByText("Dari ingatan Anda")).toBeInTheDocument();
    const repayment = screen.getByRole("heading", { name: "Ganti uang patungan" }).closest("article") as HTMLElement;
    expect(within(repayment).getByText("Ganti uang patungan · Patungan & Ganti Uang · mengurangi Makanan & Minuman", { selector: "dd" })).toBeInTheDocument();
    expect(screen.getByText(/40 transaksi diperiksa · 3 saran baru \(2 dari ingatan, 1 dari aturan, 0 dari AI\)/)).toBeInTheDocument();
    expect(screen.getByText("AI tidak aktif, jadi hanya saran dari ingatan dan aturan yang dibuat.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("estimates first and starts only on confirmation, by keyboard", async () => {
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

  it("accepts a group by keyboard and rejects a single row", async () => {
    vi.mocked(api.post).mockImplementation(async (path) => (
      path.endsWith("/accept") ? { ok: true, accepted: 2, skipped: [], corrections: 2 } : { ok: true, rejected: 1, skipped: [] }
    ));
    const user = userEvent.setup();
    renderSection();
    const accept = await screen.findByRole("button", { name: "Terima semua (2)" });
    accept.focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/suggestions/accept", { suggestion_ids: ["s-1", "s-2"] }));
    expect(await screen.findByText("Kedai Awan: 2 saran diterima.")).toBeInTheDocument();

    const repayment = screen.getByRole("heading", { name: "Ganti uang patungan" }).closest("article") as HTMLElement;
    await user.click(within(repayment).getByText("Lihat 1 saran"));
    await user.click(within(repayment).getByRole("button", { name: /Tolak saran untuk Transfer masuk/ }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/suggestions/reject", { suggestion_ids: ["s-3"] }));
    expect(await screen.findByText("Ganti uang patungan: 1 saran ditolak.")).toBeInTheDocument();
  });

  it("reports skipped and failed decisions truthfully", async () => {
    vi.mocked(api.post)
      .mockResolvedValueOnce({ ok: true, accepted: 1, skipped: ["s-2"], corrections: 1 })
      .mockRejectedValueOnce(Object.assign(new Error("x"), { detail: { code: "suggestions_not_found", message: "Sebagian saran tidak ditemukan." } }));
    const user = userEvent.setup();
    renderSection();
    await user.click(await screen.findByRole("button", { name: "Terima semua (2)" }));
    expect(await screen.findByText(/Kedai Awan: 1 saran diterima\. 1 dilewati/)).toBeInTheDocument();
    const kedai = screen.getByRole("heading", { name: "Kedai Awan" }).closest("article") as HTMLElement;
    await user.click(within(kedai).getByRole("button", { name: "Tolak semua" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Sebagian saran tidak ditemukan.");
    expect(screen.queryByText(/saran ditolak/)).not.toBeInTheDocument();
  });

  it("shows an honest empty state before the first run", async () => {
    currentList = { ok: true, truncated: false, pending: 0, groups: [] };
    currentRun = null;
    const { container } = renderSection();
    expect(await screen.findByText("Belum ada saran. Pilih periode lalu hitung perkiraan.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
