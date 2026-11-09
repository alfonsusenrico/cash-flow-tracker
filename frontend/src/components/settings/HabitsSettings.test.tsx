import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { HabitsSettings } from "./HabitsSettings";
import { AppearanceSettings } from "./AppearanceSettings";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn() } }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ refresh: vi.fn() }) }));
const ctx = vi.hoisted(() => ({ hide: false, setHide: vi.fn(), theme: "system", setTheme: vi.fn(), keys: true, setKeys: vi.fn() }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ bal: (amount: number) => (ctx.hide ? "••••" : `Rp ${amount.toLocaleString("id-ID")}`), hideBalances: ctx.hide, setHideBalances: ctx.setHide }),
}));
vi.mock("@/components/workspace/context", () => ({ useWorkspace: () => ({ themeChoice: ctx.theme, setThemeChoice: ctx.setTheme }) }));
vi.mock("@/components/workspace/shortcuts", () => ({ useShortcuts: () => ({ singleKeysEnabled: ctx.keys, setSingleKeysEnabled: ctx.setKeys }) }));

const listing = {
  ok: true,
  auto_record: { habits: true, topups: true },
  thresholds: { min_confirmations: 3, min_agreement: 0.9 },
  habits: [
    { key: "h_1", label: "Kedai Awan", account: { id: "a", name: "Dompet Digital" }, category: { id: "c", name: "Makanan & Minuman" }, kakeibo: "want",
      amount_anchor: 30000, confirmations: 3, agreeing: 3, agreement: 1, state: "ready", last_confirmed_at: "2026-11-02T05:40:00Z" },
    { key: "h_2", label: "Tanpa nama toko · BCA", account: { id: "b", name: "Bank Utama" }, category: { id: "d", name: "Belanja" }, kakeibo: "need",
      amount_anchor: 12000, confirmations: 4, agreeing: 2, agreement: 0.5, state: "conflicting", last_confirmed_at: "2026-11-01T05:40:00Z" },
  ],
  topups: [{ key: "t_1", source_account: { id: "r", name: "RDN Contoh" }, amount: 112590, product: { id: "p", name: "Reksa Dana A", parent_name: "Bibit Contoh" },
    confirmations: 2, state: "learning", last_confirmed_at: "2026-10-01T02:15:00Z" }],
  truncated: false,
};

const questionRate = {
  has_data: true,
  weeks: [{ week_start: "2026-11-02", week_end: "2026-11-08", in_progress: true, recorded: 20, asked: 3, answered: 3, by_memory: 6, by_habit: 5, by_ai: 4, by_other: 2, corrections: 1, habit_corrections: 0 }],
};

/** The habit list for `/habits` and the weekly counts for the moved "Seberapa sering ditanya" panel. */
function serve(habits: unknown) {
  vi.mocked(api.get).mockImplementation(async (path: string) => (path.startsWith("/dashboard/question-rate") ? questionRate : habits));
}

function renderWith(node: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{node}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
  ctx.hide = false;
  serve(listing);
});

describe("Kebiasaan", () => {
  it("reads habits and top-ups in words, with no edit or forget control", async () => {
    const view = renderWith(<HabitsSettings />);
    const habits = await screen.findByRole("table", { name: "Kebiasaan pembayaran" });
    const ready = within(habits).getByRole("row", { name: /Kedai Awan/ });
    expect(ready).toHaveTextContent("Kedai Awan · Dompet Digital");
    expect(ready).toHaveTextContent("Makanan & Minuman · Keinginan");
    expect(ready).toHaveTextContent("3×");
    expect(ready).toHaveTextContent("Dicatat otomatis");
    expect(within(habits).getByRole("row", { name: /Tanpa nama toko/ })).toHaveTextContent("Jawaban berbeda, tetap ditanya");
    const topup = within(screen.getByRole("table", { name: "Top up investasi" })).getByRole("row", { name: /RDN Contoh/ });
    expect(topup).toHaveTextContent("Rp 112.590 dari RDN Contoh");
    expect(topup).toHaveTextContent("Bibit Contoh · Reksa Dana A");
    expect(topup).toHaveTextContent("Masih belajar · 2 dari 3");
    expect(screen.getByText(/Diatur oleh pengaturan server/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /hapus|lupakan|ubah/i })).not.toBeInTheDocument();
    expect((await axe(view.container)).violations).toHaveLength(0);
  });

  it("says when the server switch is off and masks hidden amounts", async () => {
    ctx.hide = true;
    serve({ ...listing, auto_record: { habits: false, topups: true } });
    renderWith(<HabitsSettings />);
    expect(await screen.findByText("Siap, pencatatan otomatis nonaktif")).toBeInTheDocument();
    expect(screen.getByText("Nonaktif")).toBeInTheDocument();
    expect(screen.queryByText(/112\.590/)).not.toBeInTheDocument();
  });

  it("explains an empty list and shows an error with retry instead of an empty table", async () => {
    serve({ ...listing, habits: [], topups: [] });
    const view = renderWith(<HabitsSettings />);
    expect(await screen.findByText("Belum ada kebiasaan yang dipelajari.")).toBeInTheDocument();
    view.unmount();
    vi.mocked(api.get).mockRejectedValue(new Error("down"));
    renderWith(<HabitsSettings />);
    expect(await screen.findByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});

describe("Seberapa sering ditanya in Kebiasaan", () => {
  it("shows the weekly panel after the habit and top-up tables (apply-markas-analisis D18)", async () => {
    renderWith(<HabitsSettings />);
    const tables = await screen.findAllByRole("table");
    const panel = await screen.findByRole("heading", { name: "Seberapa sering ditanya" });
    expect(tables.every((table) => table.compareDocumentPosition(panel) & Node.DOCUMENT_POSITION_FOLLOWING)).toBe(true);
    expect(await screen.findByTestId("question-rate-headline")).toHaveTextContent("15% minggu ini · 3 dari 20");
    expect(api.get).toHaveBeenCalledWith("/dashboard/question-rate?weeks=8");
  });
});

describe("Tampilan", () => {
  it("drives the shared theme, shortcut and hidden-balance state", async () => {
    const view = render(
      <QueryClientProvider client={new QueryClient()}>
        <AppearanceSettings />
      </QueryClientProvider>,
    );
    screen.getByRole("radio", { name: "Gelap" }).click();
    expect(ctx.setTheme).toHaveBeenCalledWith("dark");
    screen.getByRole("switch", { name: "Pintasan satu tombol" }).click();
    expect(ctx.setKeys).toHaveBeenCalledWith(false);
    screen.getByRole("switch", { name: "Sembunyikan saldo" }).click();
    expect(ctx.setHide).toHaveBeenCalledWith(true);
    expect(screen.getByRole("switch", { name: "Pintasan satu tombol" })).toHaveAccessibleDescription(/⌘K dan Esc tetap berfungsi/);
    expect((await axe(view.container)).violations).toHaveLength(0);
  });
});
