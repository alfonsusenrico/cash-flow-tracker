import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { ToastProvider } from "@/components/workspace/overlays";
import CatatDialog from "./CatatDialog";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class ApiError extends Error {
    constructor(readonly status: number, readonly detail: unknown) {
      super(typeof detail === "string" ? detail : "Request failed");
    }
  },
}));
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ bal: (amount: number) => `Rp ${amount.toLocaleString("id-ID")}` }) }));

const preview = (text: string) => text.includes("5rb")
  ? { amount: 5000, problems: [], kind: "expense", description: "Parkir", occurred_at: new Date().toISOString(), time_phrase: "jam 2",
      time_options: [], account_hint: { account_id: "cash", label: "Tunai" }, category_hint: { category_id: "parkir", label: "Parkir", pillar: "need" } }
  : { amount: null, problems: ["missing_amount"], kind: "expense", description: "Bayar parkir", occurred_at: null, time_phrase: null,
      time_options: [], account_hint: null, category_hint: null };

const recorded = { id: "cap-1", status: "recorded", question: null, undo_until: new Date(Date.now() + 10_000).toISOString(),
  summary: { amount: 5000, description: "Parkir", time_label: "hari ini 14.00" } };

function setup(onManual = vi.fn()) {
  const onClose = vi.fn();
  const queryClient = new QueryClient();
  const view = render(
    <QueryClientProvider client={queryClient}>
      <ToastProvider>
        <CatatDialog open onClose={onClose} onManual={onManual} />
      </ToastProvider>
    </QueryClientProvider>,
  );
  return { ...view, onClose, onManual, user: userEvent.setup() };
}

const posts = () => vi.mocked(api.post).mock.calls.map(([path]) => path);

afterEach(() => vi.clearAllMocks());

describe("Web Catat dialog", () => {
  it("previews what it reads while typing, without recording anything", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
      if (path === "/capture/parse") return { ok: true, preview: preview((body as { text: string }).text) };
      throw new Error(path);
    });
    const { user, container } = setup();
    const field = screen.getByLabelText("Tulis transaksi");
    expect(field).toHaveFocus();
    expect(field).toHaveAttribute("placeholder", "Contoh: parkir 5rb jam 2");
    await user.type(field, "parkir 5rb jam 2");
    const read = await screen.findByRole("list", { name: "Yang terbaca" });
    expect(within(read).getAllByRole("listitem").map((item) => item.textContent)).toEqual(["Rp 5.000", expect.stringMatching(/^Jam /), "Tunai", "Parkir"]);
    expect(new Set(posts())).toEqual(new Set(["/capture/parse"]));
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("refuses text without an amount and says why", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, preview: preview("bayar parkir") });
    const { user } = setup();
    await user.type(screen.getByLabelText("Tulis transaksi"), "bayar parkir");
    expect(await screen.findByText("Jumlah belum ada. Tulis jumlahnya, misalnya 5rb.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Catat" })).toBeDisabled();
  });

  it("records through the capture path, closes and offers Urungkan", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
      if (path === "/capture/parse") return { ok: true, preview: preview((body as { text: string }).text) };
      if (path === "/capture/text") return { ok: true, capture: recorded };
      if (path === "/capture/cap-1/undo") return { ok: true, capture: { ...recorded, status: "undone" } };
      throw new Error(path);
    });
    const { user, onClose } = setup();
    await user.type(screen.getByLabelText("Tulis transaksi"), "parkir 5rb jam 2");
    await screen.findByRole("list", { name: "Yang terbaca" });
    await user.click(screen.getByRole("button", { name: "Catat" }));
    const sent = vi.mocked(api.post).mock.calls.find(([path]) => path === "/capture/text")![1] as Record<string, unknown>;
    expect(sent).toMatchObject({ text: "parkir 5rb jam 2", question_types: ["amount", "time", "kind", "account", "category"] });
    expect(String(sent.device_time)).toMatch(/[+-]\d{2}:\d{2}$/);
    expect(onClose).toHaveBeenCalled();
    expect(await screen.findByText("Dicatat: Parkir Rp 5.000, hari ini 14.00.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Urungkan" }));
    await waitFor(() => expect(posts()).toContain("/capture/cap-1/undo"));
    expect(await screen.findByText("Catatan dibatalkan.")).toBeInTheDocument();
  });

  it("asks the capture's follow-up question inline and records after the answer", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string, body?: unknown) => {
      if (path === "/capture/parse") return { ok: true, preview: preview("5rb") };
      if (path === "/capture/text") return { ok: true, capture: { ...recorded, status: "needs_detail", question: {
        question_id: "q1", type: "account", prompt: "Pakai rekening mana?", options: [{ id: "a1", label: "Tunai" }], allows_text: false } } };
      if (path === "/capture/cap-1/answer") {
        expect(body).toMatchObject({ question_id: "q1", option_id: "a1" });
        return { ok: true, capture: recorded };
      }
      throw new Error(path);
    });
    const { user } = setup();
    await user.type(screen.getByLabelText("Tulis transaksi"), "parkir 5rb");
    await user.click(screen.getByRole("button", { name: "Catat" }));
    const question = await screen.findByRole("group", { name: "Pakai rekening mana?" });
    await user.click(within(question).getByRole("button", { name: "Tunai" }));
    expect(await screen.findByText(/^Dicatat: Parkir/)).toBeInTheDocument();
  });

  it("opens the manual form with the amount it read", async () => {
    vi.mocked(api.post).mockResolvedValue({ ok: true, preview: preview("5rb") });
    const { user, onManual } = setup();
    await user.type(screen.getByLabelText("Tulis transaksi"), "parkir 5rb");
    await screen.findByRole("list", { name: "Yang terbaca" });
    await user.click(screen.getByRole("button", { name: "Isi manual" }));
    expect(onManual).toHaveBeenCalledWith(5000);
  });

  it("keeps the text and says so when recording fails", async () => {
    vi.mocked(api.post).mockImplementation(async (path: string) => {
      if (path === "/capture/parse") return { ok: true, preview: preview("5rb") };
      throw new Error("Server sedang sibuk.");
    });
    const { user } = setup();
    await user.type(screen.getByLabelText("Tulis transaksi"), "parkir 5rb");
    await user.click(screen.getByRole("button", { name: "Catat" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Server sedang sibuk.");
    expect(screen.getByLabelText("Tulis transaksi")).toHaveValue("parkir 5rb");
  });
});
