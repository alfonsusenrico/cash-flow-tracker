import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { api, createMovement } from "@/lib/api";
import { QuickCaptureModal } from "@/components/ui/QuickCaptureModal";
import { MobileHomeView } from "./MobileHomeView";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  createMovement: vi.fn(),
  uploadTransactionReceipt: vi.fn(),
}));

const accounts = [
  { id: "cash", name: "Cash", type: "cash", balance: 1000 },
  { id: "bank", name: "Bank", type: "bank", balance: 2000 },
  { id: "stock", name: "Stock", type: "investment", balance: 3000 },
];

function renderHome(canTransfer: boolean, onOpenTransfer = vi.fn()) {
  render(
    <MobileHomeView
      dashboard={{ accounts_liquidity: accounts, recent_transactions: [] }}
      accounts={accounts}
      canTransfer={canTransfer}
      onOpenCapture={vi.fn()}
      onOpenTransfer={onOpenTransfer}
      onOpenPayroll={vi.fn()}
      onSelectTx={vi.fn()}
      bal={(amount) => `Rp ${amount}`}
    />,
  );
  return onOpenTransfer;
}

describe("mobile Home transfer entry points", () => {
  it("opens a liquid account but never an investment position", async () => {
    const user = userEvent.setup();
    const openTransfer = renderHome(true);

    await user.click(screen.getByRole("button", { name: "Transfer dari Cash" }));
    expect(openTransfer).toHaveBeenCalledWith("cash");
    expect(screen.getByRole("button", { name: "Transfer dari Stock" })).toBeDisabled();
  });

  it("disables transfer entry points without two liquid accounts", () => {
    renderHome(false);

    expect(screen.getByRole("button", { name: "Transfer" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Transfer dari Cash" })).toBeDisabled();
  });

  it("uses the canonical movement payload from the mobile Home action", async () => {
    vi.mocked(api.get).mockImplementation(async (path) => {
      if (path === "/accounts") return { accounts };
      if (path === "/categories") return { categories: [] };
      if (path === "/goals") return { goals: [] };
      if (path === "/obligations") return { obligations: [] };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(createMovement).mockResolvedValue({
      ok: true,
      movement_id: "movement-home",
      expense_transaction_id: "movement-out",
      income_transaction_id: "movement-in",
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    function HomeMovement() {
      const [sourceId, setSourceId] = useState<string | null>(null);
      return <>
        <MobileHomeView
          dashboard={{ accounts_liquidity: accounts, recent_transactions: [] }}
          accounts={accounts}
          canTransfer
          onOpenCapture={vi.fn()}
          onOpenTransfer={(id) => setSourceId(id ?? "cash")}
          onOpenPayroll={vi.fn()}
          onSelectTx={vi.fn()}
          bal={(amount) => `Rp ${amount}`}
        />
        <QuickCaptureModal
          open={sourceId !== null}
          onClose={() => setSourceId(null)}
          defaultType="movement"
          defaultSourceAccountId={sourceId ?? undefined}
        />
      </>;
    }
    const user = userEvent.setup();
    render(<QueryClientProvider client={queryClient}><HomeMovement /></QueryClientProvider>);

    await user.click(screen.getByRole("button", { name: "Transfer dari Cash" }));
    expect(await screen.findByRole("dialog", { name: "Catat transaksi" })).toBeVisible();
    expect(screen.getByRole("combobox", { name: "Dari rekening" })).toHaveValue("cash");
    expect(screen.getByRole("combobox", { name: "Ke rekening" })).toHaveValue("bank");
    await user.type(screen.getByRole("textbox", { name: "Nominal (IDR)" }), "300");
    await user.click(screen.getByRole("button", { name: "Pindahkan Saldo" }));

    expect(createMovement).toHaveBeenCalledWith(expect.objectContaining({
      source_account_id: "cash",
      target_account_id: "bank",
      amount: 300,
    }));
  });
});
