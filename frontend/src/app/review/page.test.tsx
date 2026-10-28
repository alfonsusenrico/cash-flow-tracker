import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import type { ReviewItem, ReviewQueue } from "@/lib/review";
import ReviewPage from "./page";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), del: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }),
}));

const item = (id: string, overrides: Partial<ReviewItem> = {}): ReviewItem => ({
  id,
  type: "expense",
  amount: 125_000,
  date: "2026-10-20T05:00:00Z",
  account_id: "account-bca",
  account_name: "BCA Harian",
  category_id: "category-food",
  category_name: "Makanan & Minuman",
  kakeibo_type: "need",
  merchant: "Kedai Awan",
  notes: "Kedai Awan",
  review_status: "auto",
  source: "notification",
  classified_by: "rule",
  movement_id: null,
  movement_role: null,
  fee_for_movement_id: null,
  reasons: [],
  ...overrides,
});

function queue(): ReviewQueue {
  return {
    ok: true,
    truncated: false,
    counts: { questions: 1, needs_review: 1, auto: 2, groups: 2, awaiting: 1, ambiguous: 1 },
    questions: [{
      event_id: "event-1",
      post_time: "2026-10-21T11:00:00Z",
      question: {
        question_id: "question-1",
        prompt: "Apa ini? Rp187.500 dari BCA Harian · kategori bank: Groceries",
        allows_text: true,
        text_max_length: 200,
        suggestions: [
          { id: "s1", kind: "history", label: "belanja dapur", category_name: "Belanja", kakeibo: "need" },
          { id: "s2", kind: "current", label: "Pengeluaran Groceries", category_name: null, kakeibo: null },
        ],
      },
      transaction: {
        id: "tx-question", type: "expense", amount: 187_500, date: "2026-10-21T11:00:00Z",
        account_name: "BCA Harian", notes: "Pengeluaran Groceries", merchant: null, category_name: null,
      },
    }],
    groups: [
      {
        key: "expense|merchant:kedai awan", kind: "merchant", type: "expense", label: "Kedai Awan",
        count: 2, total: 250_000, needs_review_count: 0, auto_count: 2,
        first_date: "2026-10-18T05:00:00Z", last_date: "2026-10-20T05:00:00Z", reasons: [],
        category: { id: "category-food", name: "Makanan & Minuman" }, editable: true,
        transactions: [item("tx-a"), item("tx-b", { date: "2026-10-18T05:00:00Z" })],
      },
      {
        key: "income|notes:transfer masuk", kind: "notes", type: "income", label: "Transfer masuk",
        count: 1, total: 57_000, needs_review_count: 1, auto_count: 0,
        first_date: "2026-10-19T05:00:00Z", last_date: "2026-10-19T05:00:00Z", reasons: ["low_confidence"],
        category: null, editable: true,
        transactions: [item("tx-c", { type: "income", amount: 57_000, notes: "Transfer masuk", merchant: null, review_status: "needs_review", reasons: ["low_confidence"] })],
      },
    ],
    awaiting: [item("tx-await", {
      amount: 500_000, account_id: "account-gopay", account_name: "GoPay", notes: "Pindah saldo",
      category_name: "Internal Movement", review_status: "needs_review", reasons: ["awaiting_counterpart"],
    })],
    ambiguous: [{
      ...item("tx-ambiguous", { type: "income", amount: 500_000, notes: "Transfer masuk", review_status: "needs_review", reasons: ["ambiguous_movement"] }),
      candidates: [
        { id: "tx-send-1", type: "expense", amount: 500_000, date: "2026-10-20T04:59:00Z", account_id: "account-gopay", account_name: "GoPay" },
        { id: "tx-send-2", type: "expense", amount: 500_000, date: "2026-10-20T04:58:00Z", account_id: "account-ovo", account_name: "OVO" },
      ],
    }] as ReviewQueue["ambiguous"],
  };
}

let currentQueue: ReviewQueue;

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}><ReviewPage /></QueryClientProvider>);
}

describe("review page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentQueue = queue();
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/review/queue") return currentQueue;
      if (path === "/categories") return { categories: [
        { id: "category-food", name: "Makanan & Minuman", kind: "expense", kakeibo_type: "need" },
        { id: "category-snack", name: "Jajan & Nongkrong", kind: "expense", kakeibo_type: "want" },
        { id: "category-salary", name: "Gaji", kind: "income" },
      ] };
      if (path === "/accounts") return { accounts: [
        { id: "account-bca", name: "BCA Harian", type: "bank", parent_id: null },
        { id: "account-gopay", name: "GoPay", type: "ewallet", parent_id: null },
      ] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.post).mockResolvedValue({ ok: true, answer: { status: "applied", notes: "belanja dapur" } });
  });

  it("shows every section with Indonesian reasons and no axe violations", async () => {
    const { container } = renderPage();
    expect(await screen.findByRole("heading", { name: /Apa ini\? \(1\)/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Per toko \(3\)/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Pindah saldo menunggu pasangan \(1\)/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Pasangan pindah saldo belum pasti \(1\)/ })).toBeInTheDocument();
    expect(screen.getByText("Keyakinan AI rendah")).toBeInTheDocument();
    expect(screen.getByText("2 otomatis, belum ditinjau")).toBeInTheDocument();
    expect(screen.getByText(/Menunggu rekening pasangan/)).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: /Ubah transaksi Kedai Awan/ })[0]).toHaveAttribute("href", "/ledger?edit=tx-a&return=review");
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("answers a question with a chip or with text", async () => {
    const user = userEvent.setup();
    renderPage();
    const card = (await screen.findByRole("heading", { name: /Apa ini\? Rp187\.500/ })).closest("article") as HTMLElement;
    await user.click(within(card).getByRole("button", { name: /belanja dapur/ }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/ingest/notifications/event-1/answer-purpose", expect.objectContaining({
      question_id: "question-1", suggestion_id: "s1", reply_id: expect.any(String),
    })));
    expect(await screen.findByText("Jawaban tersimpan: belanja dapur.")).toBeInTheDocument();

    await user.click(within(card).getByRole("button", { name: "Simpan jawaban" }));
    expect(within(card).getByRole("alert")).toHaveTextContent("Tulis jawaban singkat");
    await user.type(within(card).getByRole("textbox", { name: "Jawab sendiri" }), "sewa lapangan futsal{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/ingest/notifications/event-1/answer-purpose", expect.objectContaining({
      question_id: "question-1", text: "sewa lapangan futsal",
    })));
  });

  it("accepts a whole group from the keyboard", async () => {
    const user = userEvent.setup();
    renderPage();
    const accept = await screen.findByRole("button", { name: "Terima semua (2)" });
    accept.focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", {
      action: "accept", transaction_ids: ["tx-a", "tx-b"],
    }));
    expect(await screen.findByText("Kedai Awan: 2 transaksi ditandai sudah ditinjau.")).toBeInTheDocument();
  });

  it("recategorises and renames a group with one request each", async () => {
    const user = userEvent.setup();
    renderPage();
    const card = (await screen.findByRole("heading", { name: "Kedai Awan" })).closest("article") as HTMLElement;
    await user.click(within(card).getByRole("button", { name: "Ubah kategori semua" }));
    expect(within(card).getByRole("button", { name: "Ubah kategori semua" })).toHaveAttribute("aria-expanded", "true");
    const category = within(card).getByRole("combobox", { name: "Kategori" });
    expect(within(category).queryByRole("option", { name: "Gaji" })).not.toBeInTheDocument();
    await user.selectOptions(category, "category-snack");
    await user.selectOptions(within(card).getByRole("combobox", { name: "Pilar Kakeibo" }), "want");
    await user.click(within(card).getByRole("button", { name: "Simpan untuk 2" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/review/bulk", {
      action: "recategorise", transaction_ids: ["tx-a", "tx-b"], category_id: "category-snack", kakeibo_type: "want",
    }));

    await user.click(within(card).getByRole("button", { name: "Ganti nama" }));
    const merchant = within(card).getByRole("textbox", { name: "Nama toko" });
    await user.clear(merchant);
    await user.type(merchant, "Kedai Awan Senja");
    await user.click(within(card).getByRole("button", { name: "Simpan untuk 2" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/bulk", {
      action: "rename", transaction_ids: ["tx-a", "tx-b"], merchant: "Kedai Awan Senja",
    }));
  });

  it("completes an awaiting transfer and resolves an ambiguous one", async () => {
    const user = userEvent.setup();
    renderPage();
    const awaiting = (await screen.findByRole("heading", { name: "Keluar dari GoPay, ke rekening mana?" })).closest("article") as HTMLElement;
    await user.click(within(awaiting).getByRole("button", { name: "Lengkapi pindah saldo" }));
    expect(within(awaiting).getByRole("alert")).toHaveTextContent("Pilih rekening tujuan.");
    await user.selectOptions(within(awaiting).getByRole("combobox", { name: "Rekening tujuan" }), "account-bca");
    await user.click(within(awaiting).getByRole("button", { name: "Lengkapi pindah saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/complete", {
      transaction_id: "tx-await", target_account_id: "account-bca",
    }));

    const ambiguous = screen.getByRole("heading", { name: "Masuk ke BCA Harian" }).closest("article") as HTMLElement;
    await user.click(within(ambiguous).getByRole("radio", { name: /OVO/ }));
    await user.click(within(ambiguous).getByRole("button", { name: "Gabungkan sebagai pindah saldo" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/movements/merge", {
      expense_transaction_id: "tx-send-2", income_transaction_id: "tx-ambiguous",
    }));
    await user.click(within(ambiguous).getByRole("button", { name: "Tetap terpisah" }));
    await waitFor(() => expect(api.post).toHaveBeenLastCalledWith("/review/bulk", {
      action: "keep_separate", transaction_ids: ["tx-ambiguous"],
    }));
  });

  it("reports a failed group action without claiming success", async () => {
    vi.mocked(api.post).mockRejectedValueOnce(Object.assign(new Error("x"), {
      detail: { code: "movement_leg_not_editable", message: "Pindah saldo diubah lewat detail pindah saldo." },
    }));
    const user = userEvent.setup();
    renderPage();
    await user.click(await screen.findByRole("button", { name: "Terima semua (2)" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Pindah saldo diubah lewat detail pindah saldo.");
    expect(screen.queryByText(/ditandai sudah ditinjau/)).not.toBeInTheDocument();
  });

  it("shows a calm empty state when nothing is left", async () => {
    currentQueue = { ...queue(), counts: { questions: 0, needs_review: 0, auto: 0, groups: 0, awaiting: 0, ambiguous: 0 }, questions: [], groups: [], awaiting: [], ambiguous: [] };
    const { container } = renderPage();
    expect(await screen.findByText("Semua transaksi sudah ditinjau.")).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });
});
