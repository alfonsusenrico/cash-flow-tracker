import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, ApiError } from "@/lib/api";
import { InvestmentTradeModal } from "./InvestmentTradeModal";

vi.mock("@/lib/api", () => ({
  api: { get: vi.fn(), post: vi.fn() },
  ApiError: class ApiError extends Error {
    constructor(public status: number, public detail: { code: string; message: string }) {
      super(detail.message);
    }
  },
}));

const position = {
  id: "position-1",
  name: "Test Stock",
  type: "investment" as const,
  instrument_type: "stock" as const,
  instrument_symbol: "TEST.JK",
  units: 500,
  avg_buy_price: 100,
  balance: 50_000,
};

const accounts = [
  { id: "funding-1", name: "BCA", type: "bank" as const, balance: 20_000 },
  { ...position, parent_id: "account-parent" },
];

function renderTrade(
  defaultAction: "buy" | "sell" = "buy",
  allAccounts: React.ComponentProps<typeof InvestmentTradeModal>["allAccounts"] = accounts,
) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <InvestmentTradeModal
        open
        onClose={() => {}}
        pocket={position}
        allAccounts={allAccounts}
        defaultAction={defaultAction}
      />
    </QueryClientProvider>,
  );
}

describe("InvestmentTradeModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.post).mockResolvedValue({ ok: true });
  });

  it("previews funding cash and position effects for a buy", async () => {
    const user = userEvent.setup();
    renderTrade();

    await user.type(screen.getByRole("spinbutton", { name: "Jumlah Lot" }), "1");
    await user.type(screen.getByRole("textbox", { name: "Harga / Lembar (IDR)" }), "100");

    const cashProjection = screen.getByText("Saldo rekening setelah transaksi").parentElement;
    expect(cashProjection).not.toBeNull();
    expect(within(cashProjection!).getByText("Rp 10.000")).toBeInTheDocument();
    expect(screen.getByText("Unit setelah transaksi")).toBeInTheDocument();
    expect(screen.getByText("600 lembar (6 lot)")).toBeInTheDocument();
  });

  it("prevents selling more shares than the owned position", async () => {
    const user = userEvent.setup();
    renderTrade("sell");

    await user.type(screen.getByRole("spinbutton", { name: "Jumlah Lot" }), "6");

    expect(screen.getByRole("alert")).toHaveTextContent("Jumlah jual melebihi unit yang dimiliki");
    expect(screen.getByRole("button", { name: "Catat Penjualan" })).toBeDisabled();
    expect(api.post).not.toHaveBeenCalled();
  });

  it("shows the funding choice before the effect and blocks an unaffordable buy", async () => {
    const user = userEvent.setup();
    renderTrade();

    await user.type(screen.getByRole("spinbutton", { name: "Jumlah Lot" }), "3");
    await user.type(screen.getByRole("textbox", { name: "Harga / Lembar (IDR)" }), "100");

    const funding = screen.getByRole("combobox", { name: "Dana dari Rekening" });
    const effect = screen.getByText("Dampak pembelian");
    expect(funding.compareDocumentPosition(effect) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByRole("alert")).toHaveTextContent("Saldo rekening yang ditampilkan tidak cukup");
    expect(screen.getByRole("button", { name: "Catat Pembelian" })).toBeDisabled();
    expect(api.post).not.toHaveBeenCalled();
  });

  it("allows exact liquidation and settles proceeds into the selected liquid pocket", async () => {
    const user = userEvent.setup();
    renderTrade("sell");

    await user.type(screen.getByRole("spinbutton", { name: "Jumlah Lot" }), "5");
    await user.type(screen.getByRole("textbox", { name: "Harga / Lembar (IDR)" }), "100");

    expect(screen.getByText("Seluruh unit dijual; posisi menjadi kosong.")).toBeInTheDocument();
    expect(screen.getByText("0 lembar (0 lot)")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Catat Penjualan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/transactions", expect.objectContaining({
      investment_action: "sell",
      account_id: "position-1",
      target_account_id: "funding-1",
      units: 500,
      amount: 50_000,
    })));
  });

  it("maps server balance rejection to the funding account", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValueOnce(new ApiError(409, {
      code: "insufficient_funds",
      message: "Saldo tidak mencukupi.",
    }));
    renderTrade();

    await user.type(screen.getByRole("spinbutton", { name: "Jumlah Lot" }), "1");
    await user.type(screen.getByRole("textbox", { name: "Harga / Lembar (IDR)" }), "100");
    await user.click(screen.getByRole("button", { name: "Catat Pembelian" }));

    await waitFor(() => expect(screen.getByRole("combobox", { name: "Dana dari Rekening" })).toHaveFocus());
    expect(screen.getByRole("combobox", { name: "Dana dari Rekening" })).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByRole("alert")).toHaveTextContent("Saldo tidak mencukupi");
  });

  it("maps a stale owned-unit rejection to the unit input", async () => {
    const user = userEvent.setup();
    vi.mocked(api.post).mockRejectedValueOnce(new ApiError(409, {
      code: "insufficient_units",
      message: "Unit investasi tidak mencukupi.",
    }));
    renderTrade("sell");

    await user.type(screen.getByRole("spinbutton", { name: "Jumlah Lot" }), "1");
    await user.type(screen.getByRole("textbox", { name: "Harga / Lembar (IDR)" }), "100");
    await user.click(screen.getByRole("button", { name: "Catat Penjualan" }));

    await waitFor(() => expect(screen.getByRole("spinbutton", { name: "Jumlah Lot" })).toHaveFocus());
    expect(screen.getByRole("spinbutton", { name: "Jumlah Lot" })).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByRole("alert")).toHaveTextContent("Unit investasi tidak mencukupi");
  });

  it("offers a liquid funding pocket under an investment platform without offering its position", () => {
    renderTrade("buy", [{
      id: "platform",
      name: "Broker",
      type: "investment",
      balance: 20_000,
      children: [
        { id: "rdn", parent_id: "platform", name: "RDN", type: "bank", balance: 20_000 },
        { ...position, parent_id: "platform" },
      ],
    }]);

    expect(screen.getByRole("option", { name: "RDN" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Test Stock" })).not.toBeInTheDocument();
  });
});
