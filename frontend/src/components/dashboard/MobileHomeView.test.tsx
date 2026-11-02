import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, within } from "@testing-library/react";
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
      onOpenRecurring={vi.fn()}
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
          onOpenRecurring={vi.fn()}
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

describe("mobile Home recent activity", () => {
  const recent = [
    { id: "topup-out", type: "expense", amount: 250000, notes: "", account_name: "Bank", date: "2026-11-02T03:00:00Z", is_excluded_from_budget: true, investment_topup_id: "topup-1" },
    { id: "move-out", type: "expense", amount: 200000, notes: "", account_name: "Bank", date: "2026-11-02T02:00:00Z", is_excluded_from_budget: true, investment_topup_id: null },
    { id: "coffee", type: "expense", amount: 15000, notes: "", category_name: "Kopi", account_name: "Cash", date: "2026-11-02T01:00:00Z", is_excluded_from_budget: false, investment_topup_id: null },
    { id: "topup-noted", type: "income", amount: 100000, notes: "Setoran bulanan", account_name: "Stock", date: "2026-11-02T00:30:00Z", is_excluded_from_budget: true, investment_topup_id: "topup-2" },
  ];

  function renderRecent(rows: typeof recent = recent) {
    render(
      <MobileHomeView
        dashboard={{ accounts_liquidity: accounts, recent_transactions: rows }}
        accounts={accounts}
        canTransfer
        onOpenCapture={vi.fn()}
        onOpenTransfer={vi.fn()}
        onOpenRecurring={vi.fn()}
        onSelectTx={vi.fn()}
        bal={(amount) => `Rp ${amount}`}
      />,
    );
  }

  // The row whose title reads `text`; a noted top-up repeats "Top up Investasi" in its smaller detail line.
  const rowOf = (text: string) =>
    screen.getAllByText(text).find((node) => node.classList.contains("font-bold"))!.closest("div.p-3\\.5") as HTMLElement;

  it("paints an investment top-up lavender and names it, keeping transfers sky", () => {
    renderRecent();

    const topup = rowOf("Top up Investasi");
    expect(within(topup).getByText("Rp 250000")).toHaveClass("text-investment");
    expect(within(topup).getByText("Rp 250000")).not.toHaveClass("text-sky-500");
    expect(topup.querySelector(".h-10")).toHaveClass("bg-violet-500/15", "text-investment");

    const transfer = rowOf("Pindah Saldo");
    expect(within(transfer).getByText("Rp 200000")).toHaveClass("text-sky-500");
    expect(transfer.querySelector(".h-10")).toHaveClass("bg-sky-500/15");

    expect(within(rowOf("Kopi")).getByText("-Rp 15000")).toHaveClass("text-rose-500");
  });

  it("still names a top-up when the row shows its own note, without repeating the default note", () => {
    renderRecent();

    const noted = rowOf("Setoran bulanan");
    expect(within(noted).getByText("Top up Investasi")).toBeVisible();
    expect(within(noted).getByText("Rp 100000")).toHaveClass("text-investment");

    // The API stores "Top up Investasi" as the note of a top-up recorded without one.
    cleanup();
    renderRecent([{ ...recent[0], notes: "Top up Investasi" }]);
    expect(within(rowOf("Top up Investasi")).getAllByText("Top up Investasi")).toHaveLength(1);
  });
});
