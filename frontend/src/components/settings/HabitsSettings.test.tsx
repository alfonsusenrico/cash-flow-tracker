import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { HabitsSettings } from "./HabitsSettings";
import { AppearanceSettings } from "./AppearanceSettings";
import { HabitsLearnedCard } from "@/components/analysis/HabitsLearnedCard";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn() } }));
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

function renderWith(node: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{node}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
  ctx.hide = false;
  vi.mocked(api.get).mockResolvedValue(listing);
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
    vi.mocked(api.get).mockResolvedValue({ ...listing, auto_record: { habits: false, topups: true } });
    renderWith(<HabitsSettings />);
    expect(await screen.findByText("Siap, pencatatan otomatis nonaktif")).toBeInTheDocument();
    expect(screen.getByText("Nonaktif")).toBeInTheDocument();
    expect(screen.queryByText(/112\.590/)).not.toBeInTheDocument();
  });

  it("explains an empty list and shows an error with retry instead of an empty table", async () => {
    vi.mocked(api.get).mockResolvedValue({ ...listing, habits: [], topups: [] });
    const view = renderWith(<HabitsSettings />);
    expect(await screen.findByText("Belum ada kebiasaan yang dipelajari.")).toBeInTheDocument();
    view.unmount();
    vi.mocked(api.get).mockRejectedValue(new Error("down"));
    renderWith(<HabitsSettings />);
    expect(await screen.findByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});

describe("Kebiasaan dihafal on Ringkasan", () => {
  it("counts ready habits, links to Pengaturan and never shows 0 on failure", async () => {
    const view = renderWith(<HabitsLearnedCard />);
    expect(await screen.findByText("Kedai Awan")).toBeInTheDocument();
    expect(screen.queryByText("Tanpa nama toko · BCA")).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Lihat semua" })).toHaveAttribute("href", "/settings/kebiasaan");
    view.unmount();
    vi.mocked(api.get).mockResolvedValue({ ...listing, habits: [] });
    const empty = renderWith(<HabitsLearnedCard />);
    expect(await screen.findByText(/Belum ada kebiasaan/)).toBeInTheDocument();
    empty.unmount();
    vi.mocked(api.get).mockRejectedValue(new Error("down"));
    renderWith(<HabitsLearnedCard />);
    expect(await screen.findByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    expect(screen.queryByText("0")).not.toBeInTheDocument();
  });
});

describe("Tampilan", () => {
  it("drives the shared theme, shortcut and hidden-balance state", async () => {
    const view = render(<AppearanceSettings />);
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
