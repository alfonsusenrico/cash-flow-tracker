import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, createMovement } from "@/lib/api";
import AppLayout, { useAppCtx } from "./AppLayout";

vi.mock("next/navigation", () => ({ usePathname: () => "/" }));
vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class extends Error {},
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
}));
vi.mock("./Sidebar", () => ({ Sidebar: () => null, MobileDrawer: () => null }));
vi.mock("./TopBar", () => ({ TopBar: () => null }));
vi.mock("./BottomNav", () => ({ BottomNav: () => null }));
vi.mock("@/components/ui/SettingsModal", () => ({ SettingsModal: () => null }));

function MovementTrigger() {
  const { openMovement } = useAppCtx();
  return <button type="button" onClick={() => openMovement()}>Global movement</button>;
}

describe("global movement entry point", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn().mockReturnValue({ matches: false }),
    });
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: { getItem: vi.fn().mockReturnValue(null), setItem: vi.fn(), removeItem: vi.fn() },
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
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={queryClient}>
        <AppLayout><MovementTrigger /></AppLayout>
      </QueryClientProvider>,
    );

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
});
