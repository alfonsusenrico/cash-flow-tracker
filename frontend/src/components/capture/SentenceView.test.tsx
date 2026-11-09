import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { renderWithIntl } from "@/lib/i18n/testing";
import { ToastProvider } from "@/components/workspace/overlays";
import CaptureDialog from "./CaptureDialog";

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: { get: vi.fn(), post: vi.fn() },
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
}));
const appCtx = { hideBalances: false, bal: (amount: number) => (appCtx.hideBalances ? "Rp ••••••" : `Rp ${amount.toLocaleString("id-ID")}`) };
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => appCtx }));

const preview = (text: string) => {
  if (/5rb|5k/.test(text)) {
    return { amount: 5000, problems: [], kind: "expense", description: "Parkir", occurred_at: new Date().toISOString(), time_phrase: "jam 2",
      time_options: [], account_hint: { account_id: "cash", label: "Tunai" }, category_hint: { category_id: "parkir", label: "Parkir", pillar: "need" } };
  }
  if (/spaylater/.test(text)) {
    return { amount: 120_000, problems: [], kind: "expense", description: "Sepatu", occurred_at: new Date().toISOString(), time_phrase: null,
      time_options: [], account_hint: { account_id: "spaylater", label: "SPayLater" }, category_hint: null };
  }
  if (/gaji/.test(text)) {
    return { amount: 8_000_000, problems: [], kind: "income", description: "Gaji", occurred_at: null, time_phrase: null,
      time_options: [], account_hint: null, category_hint: null };
  }
  if (/dua/.test(text)) {
    return { amount: null, problems: ["several_amounts"], kind: "expense", description: null, occurred_at: null, time_phrase: null,
      time_options: [], account_hint: null, category_hint: null };
  }
  return { amount: null, problems: ["missing_amount"], kind: "expense", description: "Bayar parkir", occurred_at: null, time_phrase: null,
    time_options: [], account_hint: null, category_hint: null };
};

const recorded = { id: "cap-1", status: "recorded", question: null, undo_until: new Date(Date.now() + 10_000).toISOString(),
  summary: { amount: 5000, description: "Parkir", time_label: "hari ini 14.00" } };

function wrapper({ children }: { children: React.ReactNode }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return (
    <QueryClientProvider client={queryClient}>
      <ToastProvider>{children}</ToastProvider>
    </QueryClientProvider>
  );
}

function setup(language: "id" | "en" = "id") {
  const onClose = vi.fn();
  const ui = <CaptureDialog request={{ view: "sentence" }} onClose={onClose} />;
  const view = language === "id" ? render(ui, { wrapper }) : renderWithIntl(ui, { wrapper, language });
  return { ...view, onClose, user: userEvent.setup() };
}

const posts = () => vi.mocked(api.post).mock.calls.map(([path]) => path);
const field = () => screen.getByRole("textbox", { name: "Kalimat transaksi" });

beforeEach(() => {
  vi.mocked(api.get).mockImplementation(async (path: string) => {
    if (path === "/accounts") return { accounts: [{ id: "spaylater", name: "SPayLater", type: "credit", balance: -100_000 }] };
    return {};
  });
  vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
    if (path === "/capture/parse") return { ok: true, preview: preview((body as { text: string }).text) };
    throw new Error(path);
  });
});
afterEach(() => {
  vi.clearAllMocks();
  vi.unstubAllGlobals();
  appCtx.hideBalances = false;
});

describe("Web Catat dialog: Tulis transaksi", () => {
  it("previews what it reads while typing, without recording anything", async () => {
    const { user, baseElement } = setup();
    expect(screen.getByRole("dialog", { name: "Tulis transaksi" })).toBeInTheDocument();
    expect(field()).toHaveFocus();
    expect(field()).toHaveAttribute("placeholder", "makan siang 35rb pakai gopay");
    expect(screen.getByText("Hasil terbaca muncul di sini sebelum disimpan.")).toBeInTheDocument();
    await user.type(field(), "parkir 5rb jam 2");
    const box = await screen.findByText("Terbaca");
    const read = box.closest(".capture-read") as HTMLElement;
    expect(within(read).getByText("−Rp 5.000")).toHaveClass("c-expense", "t-fig");
    const terms = within(read).getAllByRole("term").map((item) => item.textContent);
    const values = within(read).getAllByRole("definition").map((item) => item.textContent);
    expect(terms).toEqual(["Jenis", "Rekening", "Kategori", "Pilar", "Tanggal"]);
    expect(values.slice(0, 4)).toEqual(["Pengeluaran", "Tunai", "Parkir", "Kebutuhan"]);
    expect(values[4]).toMatch(/^Hari ini, \d{2}\.\d{2}$/);
    expect(new Set(posts())).toEqual(new Set(["/capture/parse"]));
    expect((await axe(baseElement)).violations).toHaveLength(0);
  });

  it("refuses text without an amount, or with several, and says why", async () => {
    const { user } = setup();
    await user.type(field(), "bayar parkir");
    expect(await screen.findByText("Jumlah belum ada. Tulis jumlahnya, misalnya 5rb.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Catat" })).toBeDisabled();
    expect(field()).toHaveAttribute("aria-invalid", "true");
    await user.clear(field());
    await user.type(field(), "dua angka");
    expect(await screen.findByText("Ada lebih dari satu jumlah. Tulis satu jumlah saja.")).toBeInTheDocument();
  });

  it("says when the preview is unavailable and keeps Catat", async () => {
    vi.mocked(api.post).mockRejectedValue(new Error("down"));
    const { user } = setup();
    await user.type(field(), "parkir 5rb");
    expect(await screen.findByText("Belum bisa membaca sekarang. Teks tetap bisa dicatat.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Catat" })).toBeEnabled();
  });

  it("shows income in green with +, and a credit account as paid later", async () => {
    const { user } = setup();
    await user.type(field(), "gaji masuk 8jt");
    expect(await screen.findByText("+Rp 8.000.000")).toHaveClass("c-income");
    expect(screen.getByText("ditentukan saat dicatat")).toHaveClass("c-warning");
    expect(screen.getByText("ditanyakan saat dicatat")).toHaveClass("c-warning");
    await user.clear(field());
    await user.type(field(), "sepatu 120rb spaylater");
    expect(await screen.findByText("SPayLater · dibayar nanti")).toBeInTheDocument();
  });

  it("fills an example and previews it", async () => {
    const { user } = setup();
    await user.click(screen.getByRole("button", { name: "parkir 5rb jam 2" }));
    expect(field()).toHaveValue("parkir 5rb jam 2");
    expect(field()).toHaveFocus();
    expect(await screen.findByText("−Rp 5.000")).toBeInTheDocument();
  });

  it("records through the capture path, closes and offers Urungkan", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
      if (path === "/capture/parse") return { ok: true, preview: preview((body as { text: string }).text) };
      if (path === "/capture/text") return { ok: true, capture: recorded };
      if (path === "/capture/cap-1/undo") return { ok: true, capture: { ...recorded, status: "undone" } };
      throw new Error(path);
    });
    const { user, onClose } = setup();
    await user.type(field(), "parkir 5rb jam 2");
    await screen.findByText("Terbaca");
    await user.click(screen.getByRole("button", { name: "Catat" }));
    const sent = vi.mocked(api.post).mock.calls.find(([path]) => path === "/capture/text")![1] as Record<string, unknown>;
    expect(Object.keys(sent).sort()).toEqual(["client_capture_id", "device_time", "question_types", "text"]);
    expect(sent).toMatchObject({ text: "parkir 5rb jam 2", question_types: ["amount", "time", "kind", "account", "category"] });
    expect(String(sent.device_time)).toMatch(/[+-]\d{2}:\d{2}$/);
    expect(onClose).toHaveBeenCalled();
    expect(await screen.findByText("Dicatat: Parkir Rp 5.000, hari ini 14.00.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(posts()).toContain("/capture/cap-1/undo"));
    expect(await screen.findByText("Catatan dibatalkan.")).toBeInTheDocument();
  });

  it("records with Enter in the field", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
      if (path === "/capture/parse") return { ok: true, preview: preview((body as { text: string }).text) };
      if (path === "/capture/text") return { ok: true, capture: recorded };
      throw new Error(path);
    });
    const { user, onClose } = setup();
    await user.type(field(), "parkir 5rb{Enter}");
    await waitFor(() => expect(posts()).toContain("/capture/text"));
    expect(onClose).toHaveBeenCalled();
  });

  it("asks the capture's follow-up question inline and records after the answer", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
      if (path === "/capture/parse") return { ok: true, preview: preview("5rb") };
      if (path === "/capture/text") return { ok: true, capture: { ...recorded, status: "needs_detail", question: {
        question_id: "q1", type: "account", prompt: "Dari rekening mana?", options: [{ id: "a1", label: "Tunai" }], allows_text: false } } };
      if (path === "/capture/cap-1/answer") {
        expect(body).toMatchObject({ question_id: "q1", option_id: "a1" });
        return { ok: true, capture: recorded };
      }
      throw new Error(path);
    });
    const { user } = setup();
    await user.type(field(), "beli pulsa 5rb");
    await user.click(screen.getByRole("button", { name: "Catat" }));
    const question = await screen.findByRole("group", { name: "Dari rekening mana?" });
    await waitFor(() => expect(within(question).getByRole("button", { name: "Tunai" })).toHaveFocus());
    expect(screen.queryByRole("button", { name: "Catat" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Batal" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Isi manual" })).not.toBeInTheDocument();
    await user.click(within(question).getByRole("button", { name: "Tunai" }));
    expect(await screen.findByText(/^Dicatat: Parkir/)).toBeInTheDocument();
  });

  it("resends the same client capture id after a failure and keeps the text", async () => {
    let attempts = 0;
    vi.mocked(api.post).mockImplementation(async (path: string) => {
      if (path === "/capture/parse") return { ok: true, preview: preview("5rb") };
      attempts += 1;
      if (attempts === 1) throw new Error("Server sedang sibuk.");
      return { ok: true, capture: recorded };
    });
    const { user } = setup();
    await user.type(field(), "parkir 5rb");
    await user.click(screen.getByRole("button", { name: "Catat" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Server sedang sibuk.");
    expect(field()).toHaveValue("parkir 5rb");
    await user.click(screen.getByRole("button", { name: "Catat" }));
    await waitFor(() => expect(attempts).toBe(2));
    const ids = vi.mocked(api.post).mock.calls.filter(([path]) => path === "/capture/text").map(([, body]) => (body as { client_capture_id: string }).client_capture_id);
    expect(ids[0]).toBe(ids[1]);
  });

  it("Isi manual turns the same dialog into the form with the amount read, and Tulis kalimat turns it back", async () => {
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/accounts") return { accounts: [{ id: "cash", name: "Tunai", type: "cash", balance: 10_000 }] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      return {};
    });
    const { user } = setup();
    await user.type(field(), "parkir 5rb");
    await screen.findByText("Terbaca");
    await user.click(screen.getByRole("button", { name: "Isi manual" }));
    expect(screen.getAllByRole("dialog")).toHaveLength(1);
    expect(screen.getByRole("dialog", { name: "Catat transaksi" })).toBeInTheDocument();
    const amount = screen.getByRole("textbox", { name: "Nominal (IDR)" });
    expect(amount).toHaveValue("5.000");
    await waitFor(() => expect(amount).toHaveFocus());
    await user.click(screen.getByRole("button", { name: "Tulis kalimat" }));
    expect(screen.getByRole("dialog", { name: "Tulis transaksi" })).toBeInTheDocument();
    expect(field()).toHaveValue("parkir 5rb");
    await user.click(screen.getByRole("button", { name: "Isi manual" }));
    expect(screen.getByRole("textbox", { name: "Nominal (IDR)" })).toHaveValue("5.000");
  });

  it("hides the read amount with Sembunyikan saldo", async () => {
    appCtx.hideBalances = true;
    const { user } = setup();
    await user.type(field(), "parkir 5rb");
    expect(await screen.findByRole("img", { name: "Disembunyikan" })).toHaveTextContent("Rp ••••••");
  });

  it("reads in English with no untranslated notice or Indonesian part", async () => {
    const { user, baseElement } = setup("en");
    expect(screen.getByRole("dialog", { name: "Write a transaction" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "lunch 35k" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Fill in manually" })).toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Transaction sentence" }), "parking 5k");
    expect(await screen.findByText("Read as")).toBeInTheDocument();
    expect(screen.getByText("Spending")).toBeInTheDocument();
    expect(screen.queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
    expect(baseElement.querySelector("[lang='id']")).toBeNull();
  });
});
