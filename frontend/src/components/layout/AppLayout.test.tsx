import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, createMovement } from "@/lib/api";
import AppLayout, { useAppCtx } from "./AppLayout";

vi.mock("next/navigation", () => ({ usePathname: () => "/", useRouter: () => ({ push: vi.fn() }) }));
vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class extends Error {},
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
  getDevices: vi.fn().mockResolvedValue({ ok: true, devices: [] }),
}));

function MovementTrigger() {
  const { openMovement } = useAppCtx();
  return <button type="button" onClick={() => openMovement()}>Global movement</button>;
}

function renderLayout(children: React.ReactNode = <MovementTrigger />) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <AppLayout>{children}</AppLayout>
    </QueryClientProvider>,
  );
}

describe("global movement entry point", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn().mockReturnValue({ matches: false }),
    });
    const stored = new Map<string, string>();
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: {
        getItem: vi.fn((key: string) => stored.get(key) ?? null),
        setItem: vi.fn((key: string, value: string) => void stored.set(key, value)),
        removeItem: vi.fn((key: string) => void stored.delete(key)),
      },
    });
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/auth/me") return { ok: true, user: { id: "qa-user", currency: "IDR" } };
      if (path === "/auth/currency/rates") return { ok: true, usdidr: 16000 };
      if (path === "/accounts") return { accounts: [
        { id: "cash-a", name: "Cash A", type: "cash", balance: 1000 },
        { id: "cash-b", name: "Cash B", type: "bank", balance: 0 },
        { id: "stock", name: "Stock", type: "investment", balance: 5000 },
      ] };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      if (path === "/review/summary") return { ok: true, attention: 4, unreviewed: 3 };
      if (path === "/reports/monthly/latest") return { ready: false, period: null, url: null };
      if (path === "/recurring") return { rules: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(createMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-1",
      expense_transaction_id: "movement-out",
      income_transaction_id: "movement-in",
    });
  });

  it("opens Quick Capture in movement mode from shared navigation context", async () => {
    const user = userEvent.setup();
    renderLayout();

    await user.click(await screen.findByRole("button", { name: "Global movement" }));
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Perpindahan" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.queryByRole("option", { name: /Stock/ })).not.toBeInTheDocument();
    expect(await screen.findByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash-a");
    expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("cash-b");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "300");
    await user.click(screen.getByRole("button", { name: "Pindahkan Saldo" }));

    await waitFor(() => expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "cash-a",
      target_account_id: "cash-b",
      amount: 300,
    })));
    expect(api.post).not.toHaveBeenCalled();
  });

  it("puts every page inside the workspace: skip link, sidebar, main and the phone tab bar", async () => {
    renderLayout();
    expect(await screen.findByRole("link", { name: "Langsung ke konten" })).toHaveAttribute("href", "#main");
    expect(screen.getByRole("complementary", { name: "Navigasi samping" })).toBeInTheDocument();
    expect(screen.getByRole("main")).toHaveAttribute("id", "main");
    expect(screen.getByRole("navigation", { name: "Navigasi utama" })).toBeInTheDocument();
    // Sidebar and tab bar both name the badge in words (CSS shows one of them per width).
    expect(await screen.findAllByRole("link", { name: "Tinjau, 4 perlu ditinjau" })).toHaveLength(2);
    expect(screen.queryByRole("button", { name: /Buka menu/ })).not.toBeInTheDocument();
  });

  it("remembers the collapsed sidebar", async () => {
    const user = userEvent.setup();
    renderLayout();
    await user.click(await screen.findByRole("button", { name: "Ciutkan menu samping" }));
    expect(localStorage.setItem).toHaveBeenCalledWith("sidebarCollapsed", "1");
    expect(document.querySelector(".ws-window")).toHaveAttribute("data-collapsed", "true");
  });

  it("opens capture with N and not while typing", async () => {
    const user = userEvent.setup();
    renderLayout(<input aria-label="Ketik" />);
    await screen.findByRole("main");
    await user.type(screen.getByRole("textbox", { name: "Ketik" }), "n");
    expect(screen.queryByRole("dialog", { name: "Catat transaksi" })).not.toBeInTheDocument();
    (document.activeElement as HTMLElement).blur();
    await user.keyboard("n");
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
  });
});
